from __future__ import annotations

import gc
import os
import time
from typing import Any, Dict, Optional

from fastapi import BackgroundTasks

from api.schemas.models import FileDownloadRequest
from api.services.task_facade import TaskFacade
from modules.accounts.accounts_sql_manager import get_accounts_sql_manager
from modules.shared.db_path_manager import get_db_path_manager
from modules.zsxq.zsxq_file_downloader import ZSXQFileDownloader
from modules.zsxq.zsxq_interactive_crawler import load_config


class FileService:
    def __init__(self):
        self.tasks = TaskFacade()
        self.file_downloader_instances: Dict[str, Any] = {}

    def _resolve_cookie_for_group(self, group_id: str) -> str:
        manager = get_accounts_sql_manager()
        account = manager.get_account_for_group(group_id, mask_cookie=False)
        if account and account.get("cookie"):
            return str(account["cookie"]).strip()

        first = manager.get_first_account(mask_cookie=False)
        if first and first.get("cookie"):
            return str(first["cookie"]).strip()

        cfg = load_config() or {}
        return str((cfg.get("auth", {}) or {}).get("cookie", "")).strip()

    def _build_downloader(self, group_id: str, log_callback=None, stop_check=None, **kwargs) -> ZSXQFileDownloader:
        cookie = self._resolve_cookie_for_group(group_id)
        db_path = get_db_path_manager().get_files_db_path(group_id)
        downloader = ZSXQFileDownloader(cookie=cookie, group_id=group_id, db_path=db_path, **kwargs)
        downloader.log_callback = log_callback
        downloader.stop_check_func = stop_check
        return downloader

    def start_collect_files(self, group_id: str, background_tasks: BackgroundTasks) -> Dict[str, Any]:
        task_id = self.tasks.create_task("collect_files", "收集文件列表")
        background_tasks.add_task(self._run_collect_files_task, task_id, group_id)
        return {"task_id": task_id, "message": "任务已创建，正在后台执行"}

    def _run_collect_files_task(self, task_id: str, group_id: str) -> None:
        try:
            self.tasks.update_task(task_id, "running", "开始收集文件列表...")

            def log_callback(message: str):
                self.tasks.append_log(task_id, message)

            downloader = self._build_downloader(group_id, log_callback=log_callback, stop_check=lambda: self.tasks.is_task_stopped(task_id))
            self.file_downloader_instances[task_id] = downloader

            if self.tasks.is_task_stopped(task_id):
                self.tasks.append_log(task_id, "🛑 任务在初始化过程中被停止")
                return

            self.tasks.append_log(task_id, "📡 连接到知识星球API...")
            result = downloader.collect_incremental_files()
            if self.tasks.is_task_stopped(task_id):
                return
            self.tasks.append_log(task_id, "✅ 文件列表收集完成！")
            self.tasks.update_task(task_id, "completed", "文件列表收集完成", result)
        except Exception as e:
            if not self.tasks.is_task_stopped(task_id):
                self.tasks.append_log(task_id, f"❌ 文件列表收集失败: {e}")
                self.tasks.update_task(task_id, "failed", f"文件列表收集失败: {e}")
        finally:
            self.file_downloader_instances.pop(task_id, None)

    def start_download_files(self, group_id: str, request: FileDownloadRequest, background_tasks: BackgroundTasks) -> Dict[str, Any]:
        task_id = self.tasks.create_task("download_files", f"下载文件 (排序: {request.sort_by})")
        background_tasks.add_task(self._run_file_download_task, task_id, group_id, request)
        return {"task_id": task_id, "message": "任务已创建，正在后台执行"}

    def _run_file_download_task(self, task_id: str, group_id: str, request: FileDownloadRequest) -> None:
        try:
            self.tasks.update_task(task_id, "running", "开始文件下载...")

            def log_callback(message: str):
                self.tasks.append_log(task_id, message)

            downloader = self._build_downloader(
                group_id,
                log_callback=log_callback,
                stop_check=lambda: self.tasks.is_task_stopped(task_id),
                download_interval=request.download_interval,
                long_sleep_interval=request.long_sleep_interval,
                files_per_batch=request.files_per_batch,
                download_interval_min=request.download_interval_min,
                download_interval_max=request.download_interval_max,
                long_sleep_interval_min=request.long_sleep_interval_min,
                long_sleep_interval_max=request.long_sleep_interval_max,
            )
            self.file_downloader_instances[task_id] = downloader

            self.tasks.append_log(task_id, "⚙️ 下载配置:")
            self.tasks.append_log(task_id, f"   ⏱️ 单次下载间隔: {request.download_interval}秒")
            self.tasks.append_log(task_id, f"   😴 长休眠间隔: {request.long_sleep_interval}秒")
            self.tasks.append_log(task_id, f"   📦 批次大小: {request.files_per_batch}个文件")

            if self.tasks.is_task_stopped(task_id):
                self.tasks.append_log(task_id, "🛑 任务在初始化过程中被停止")
                return

            self.tasks.append_log(task_id, "📡 连接到知识星球API...")
            self.tasks.append_log(task_id, "🔍 开始收集文件列表...")
            collect_result = downloader.collect_incremental_files()
            if self.tasks.is_task_stopped(task_id):
                return
            self.tasks.append_log(task_id, f"📊 文件收集完成: {collect_result}")
            self.tasks.append_log(task_id, "🚀 开始下载文件...")

            if request.sort_by == "download_count":
                result = downloader.download_files_from_database(max_files=request.max_files, status_filter="pending", order_by="download_count DESC")
            else:
                result = downloader.download_files_from_database(max_files=request.max_files, status_filter="pending", order_by="create_time DESC")

            if self.tasks.is_task_stopped(task_id):
                return
            self.tasks.append_log(task_id, "✅ 文件下载完成！")
            self.tasks.update_task(task_id, "completed", "文件下载完成", {"downloaded_files": result})
        except Exception as e:
            if not self.tasks.is_task_stopped(task_id):
                self.tasks.append_log(task_id, f"❌ 文件下载失败: {e}")
                self.tasks.update_task(task_id, "failed", f"文件下载失败: {e}")
        finally:
            self.file_downloader_instances.pop(task_id, None)

    def start_download_single_file(
        self,
        group_id: str,
        file_id: int,
        background_tasks: BackgroundTasks,
        file_name: Optional[str] = None,
        file_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        task_id = self.tasks.create_task("download_single_file", f"下载单个文件 (ID: {file_id})")
        background_tasks.add_task(self._run_single_file_download_task, task_id, group_id, file_id, file_name, file_size)
        return {"task_id": task_id, "message": "单个文件下载任务已创建"}

    def _run_single_file_download_task(
        self,
        task_id: str,
        group_id: str,
        file_id: int,
        file_name: Optional[str],
        file_size: Optional[int],
    ) -> None:
        try:
            self.tasks.update_task(task_id, "running", f"开始下载文件 (ID: {file_id})...")

            def log_callback(message: str):
                self.tasks.append_log(task_id, message)

            downloader = self._build_downloader(group_id, log_callback=log_callback, stop_check=lambda: self.tasks.is_task_stopped(task_id))
            self.file_downloader_instances[task_id] = downloader

            if self.tasks.is_task_stopped(task_id):
                self.tasks.append_log(task_id, "🛑 任务在初始化过程中被停止")
                return

            if file_name and file_size is not None:
                self.tasks.append_log(task_id, f"📄 使用提供的文件信息: {file_name} ({file_size} bytes)")
                file_info = {"file": {"id": file_id, "name": file_name, "size": file_size, "download_count": 0}}
            else:
                downloader.file_db.cursor.execute(
                    """
                    SELECT file_id, name, size, download_count
                    FROM files
                    WHERE file_id = ?
                    """,
                    (file_id,),
                )
                result = downloader.file_db.cursor.fetchone()
                if result:
                    _, db_file_name, db_file_size, download_count = result
                    self.tasks.append_log(task_id, f"📄 从数据库获取文件信息: {db_file_name} ({db_file_size} bytes)")
                    file_info = {"file": {"id": file_id, "name": db_file_name, "size": db_file_size, "download_count": download_count}}
                else:
                    self.tasks.append_log(task_id, f"📄 直接下载文件 ID: {file_id}")
                    file_info = {"file": {"id": file_id, "name": f"file_{file_id}", "size": 0, "download_count": 0}}

            result = downloader.download_file(file_info)
            if result == "skipped":
                self.tasks.append_log(task_id, "✅ 文件已存在，跳过下载")
                self.tasks.update_task(task_id, "completed", "文件已存在")
                return
            if not result:
                self.tasks.append_log(task_id, "❌ 文件下载失败")
                self.tasks.update_task(task_id, "failed", "下载失败")
                return

            self.tasks.append_log(task_id, "✅ 文件下载成功")
            actual_file_info = file_info["file"]
            actual_file_name = actual_file_info.get("name", f"file_{file_id}")
            actual_file_size = actual_file_info.get("size", 0)
            safe_filename = "".join(c for c in actual_file_name if c.isalnum() or c in "._-（）()[]{}") or f"file_{file_id}"
            local_path = os.path.join(downloader.download_dir, safe_filename)
            if os.path.exists(local_path):
                actual_file_size = os.path.getsize(local_path)

            downloader.file_db.cursor.execute(
                """
                INSERT OR REPLACE INTO files
                (file_id, name, size, download_status, local_path, download_time, download_count)
                VALUES (?, ?, ?, 'downloaded', ?, CURRENT_TIMESTAMP, ?)
                """,
                (file_id, actual_file_name, actual_file_size, local_path, actual_file_info.get("download_count", 0)),
            )
            downloader.file_db.conn.commit()
            self.tasks.update_task(task_id, "completed", "下载成功")
        except Exception as e:
            if not self.tasks.is_task_stopped(task_id):
                self.tasks.append_log(task_id, f"❌ 任务执行失败: {e}")
                self.tasks.update_task(task_id, "failed", f"任务失败: {e}")
        finally:
            self.file_downloader_instances.pop(task_id, None)

    def get_file_status(self, group_id: str, file_id: int) -> Dict[str, Any]:
        downloader = self._build_downloader(group_id)
        try:
            downloader.file_db.cursor.execute(
                """
                SELECT name, size, download_status
                FROM files
                WHERE file_id = ?
                """,
                (file_id,),
            )
            result = downloader.file_db.cursor.fetchone()
            if not result:
                return {
                    "file_id": file_id,
                    "name": f"file_{file_id}",
                    "size": 0,
                    "download_status": "not_collected",
                    "local_exists": False,
                    "local_size": 0,
                    "local_path": None,
                    "is_complete": False,
                    "message": "文件信息未收集，请先运行文件收集任务",
                }

            file_name, file_size, download_status = result
            safe_filename = "".join(c for c in file_name if c.isalnum() or c in "._-（）()[]{}") or f"file_{file_id}"
            file_path = os.path.join(downloader.download_dir, safe_filename)
            local_exists = os.path.exists(file_path)
            local_size = os.path.getsize(file_path) if local_exists else 0
            return {
                "file_id": file_id,
                "name": file_name,
                "size": file_size,
                "download_status": download_status or "pending",
                "local_exists": local_exists,
                "local_size": local_size,
                "local_path": file_path if local_exists else None,
                "is_complete": local_exists and local_size == file_size,
            }
        finally:
            try:
                downloader.file_db.close()
            except Exception:
                pass

    def check_local_file_status(self, group_id: str, file_name: str, file_size: int) -> Dict[str, Any]:
        downloader = self._build_downloader(group_id)
        try:
            safe_filename = "".join(c for c in file_name if c.isalnum() or c in "._-（）()[]{}") or file_name
            file_path = os.path.join(downloader.download_dir, safe_filename)
            local_exists = os.path.exists(file_path)
            local_size = os.path.getsize(file_path) if local_exists else 0
            return {
                "file_name": file_name,
                "safe_filename": safe_filename,
                "expected_size": file_size,
                "local_exists": local_exists,
                "local_size": local_size,
                "local_path": file_path if local_exists else None,
                "is_complete": local_exists and (file_size == 0 or local_size == file_size),
                "download_dir": downloader.download_dir,
            }
        finally:
            try:
                downloader.file_db.close()
            except Exception:
                pass

    def get_file_stats(self, group_id: str) -> Dict[str, Any]:
        downloader = self._build_downloader(group_id)
        try:
            stats = downloader.file_db.get_database_stats()
            downloader.file_db.cursor.execute("PRAGMA table_info(files)")
            columns = [col[1] for col in downloader.file_db.cursor.fetchall()]
            if "download_status" in columns:
                downloader.file_db.cursor.execute(
                    """
                    SELECT
                        COUNT(*) as total_files,
                        COUNT(CASE WHEN download_status = 'completed' THEN 1 END) as downloaded,
                        COUNT(CASE WHEN download_status = 'pending' THEN 1 END) as pending,
                        COUNT(CASE WHEN download_status = 'failed' THEN 1 END) as failed
                    FROM files
                    """
                )
                download_stats = downloader.file_db.cursor.fetchone()
            else:
                downloader.file_db.cursor.execute("SELECT COUNT(*) FROM files")
                total_files = downloader.file_db.cursor.fetchone()[0]
                download_stats = (total_files, 0, 0, 0)

            return {
                "database_stats": stats,
                "download_stats": {
                    "total_files": download_stats[0] if download_stats else 0,
                    "downloaded": download_stats[1] if download_stats else 0,
                    "pending": download_stats[2] if download_stats else 0,
                    "failed": download_stats[3] if download_stats else 0,
                },
            }
        finally:
            try:
                downloader.file_db.close()
            except Exception:
                pass

    def clear_file_database(self, group_id: str) -> Dict[str, Any]:
        path_manager = get_db_path_manager()
        db_path = path_manager.get_files_db_path(group_id)
        if not os.path.exists(db_path):
            return {"message": f"群组 {group_id} 的文件数据库不存在"}

        try:
            downloader = self._build_downloader(group_id)
            downloader.file_db.close()
        except Exception:
            pass

        gc.collect()
        time.sleep(0.5)

        try:
            os.remove(db_path)
        except PermissionError as pe:
            raise RuntimeError(f"文件被占用，无法删除数据库文件。请稍后重试。 {pe}")

        try:
            from app.runtime.image_cache_manager import clear_group_cache_manager, get_image_cache_manager

            cache_manager = get_image_cache_manager(group_id)
            cache_manager.clear_cache()
            clear_group_cache_manager(group_id)
        except Exception:
            pass

        return {"message": f"群组 {group_id} 的文件数据库和图片缓存已删除"}

    def list_files(self, group_id: str, page: int = 1, per_page: int = 20, status: Optional[str] = None) -> Dict[str, Any]:
        downloader = self._build_downloader(group_id)
        try:
            offset = (page - 1) * per_page
            if status:
                query = """
                    SELECT file_id, name, size, download_count, create_time, download_status
                    FROM files
                    WHERE download_status = ?
                    ORDER BY create_time DESC
                    LIMIT ? OFFSET ?
                """
                params = (status, per_page, offset)
            else:
                query = """
                    SELECT file_id, name, size, download_count, create_time, download_status
                    FROM files
                    ORDER BY create_time DESC
                    LIMIT ? OFFSET ?
                """
                params = (per_page, offset)

            downloader.file_db.cursor.execute(query, params)
            files = downloader.file_db.cursor.fetchall()

            if status:
                downloader.file_db.cursor.execute("SELECT COUNT(*) FROM files WHERE download_status = ?", (status,))
            else:
                downloader.file_db.cursor.execute("SELECT COUNT(*) FROM files")
            total = downloader.file_db.cursor.fetchone()[0]

            return {
                "files": [
                    {
                        "file_id": file[0],
                        "name": file[1],
                        "size": file[2],
                        "download_count": file[3],
                        "create_time": file[4],
                        "download_status": file[5] if len(file) > 5 else "unknown",
                    }
                    for file in files
                ],
                "pagination": {
                    "page": page,
                    "per_page": per_page,
                    "total": total,
                    "pages": (total + per_page - 1) // per_page,
                },
            }
        finally:
            try:
                downloader.file_db.close()
            except Exception:
                pass

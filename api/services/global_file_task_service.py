import random
import time
from typing import Any, Callable, Dict, List

from modules.shared.db_path_manager import get_db_path_manager
from modules.zsxq.zsxq_file_downloader import ZSXQFileDownloader
from api.services.group_filter_service import apply_group_scan_filter, format_group_filter_summary


class GlobalFileTaskService:
    """全区文件收集/下载服务（从 main.py 拆出业务流程）。"""

    def run_collect(
        self,
        task_id: str,
        add_task_log: Callable[[str, str], None],
        update_task: Callable[..., Any],
        is_task_stopped: Callable[[str], bool],
        get_cookie_for_group: Callable[[str], str],
        file_downloader_instances: Dict[str, Any],
    ) -> None:
        """执行全区文件列表收集流程。"""
        try:
            update_task(task_id, "running", "准备开始全区文件收集...")
            add_task_log(task_id, "🚀 开始全区文件列表收集")

            manager = get_db_path_manager()
            all_groups = manager.list_all_groups()
            filtered = apply_group_scan_filter(all_groups)
            groups = filtered["included_groups"]
            excluded_groups = filtered["excluded_groups"]
            reason_counts = filtered["reason_counts"]
            default_action = filtered["default_action"]
            for line in format_group_filter_summary(
                all_groups,
                groups,
                excluded_groups,
                reason_counts,
                default_action,
            ):
                add_task_log(task_id, line)

            if not groups:
                update_task(task_id, "completed", "全区收集完成: 过滤后无可扫描群组")
                return

            processed_groups = 0
            for i, group in enumerate(groups, 1):
                if is_task_stopped(task_id):
                    add_task_log(task_id, "🛑 任务已被用户停止")
                    break

                group_id = str(group["group_id"])
                add_task_log(task_id, "")
                add_task_log(task_id, f"👉 [{i}/{len(groups)}] 正在收集群组 {group_id} 的文件列表...")

                try:
                    cookie = get_cookie_for_group(group_id)
                    db_path = manager.get_files_db_path(group_id)

                    downloader = ZSXQFileDownloader(cookie, group_id, db_path)
                    downloader.log_callback = lambda msg: add_task_log(task_id, f"   {msg}")
                    downloader.stop_check_func = lambda: is_task_stopped(task_id)

                    file_downloader_instances[task_id] = downloader
                    res = downloader.collect_incremental_files()

                    add_task_log(
                        task_id,
                        f"   ✅ 群组 {group_id} 文件收集完成! 新增待下载: {res.get('new_files', 0) if isinstance(res, dict) else res}",
                    )
                    processed_groups += 1
                except Exception as ge:
                    add_task_log(task_id, f"   ❌ 群组 {group_id} 收集异常: {ge}")
                finally:
                    if task_id in file_downloader_instances:
                        del file_downloader_instances[task_id]

                if i < len(groups) and not is_task_stopped(task_id):
                    sleep_time = random.uniform(1.0, 3.0)
                    add_task_log(task_id, f"⏳ 等待 {sleep_time:.1f} 秒...")
                    time.sleep(sleep_time)

            if is_task_stopped(task_id):
                update_task(task_id, "cancelled", "全区收集已停止")
            else:
                add_task_log(task_id, "")
                add_task_log(task_id, "=" * 50)
                add_task_log(task_id, f"🎉 全区文件列表收集完成！共处理 {processed_groups}/{len(groups)} 个群组")
                update_task(task_id, "completed", f"全区收集完成: {processed_groups} 个群组")
        except Exception as e:
            add_task_log(task_id, f"❌ 全区收集异常: {e}")
            update_task(task_id, "failed", f"全区收集失败: {e}")

    def run_download(
        self,
        task_id: str,
        request: Any,
        add_task_log: Callable[[str, str], None],
        update_task: Callable[..., Any],
        is_task_stopped: Callable[[str], bool],
        get_cookie_for_group: Callable[[str], str],
        file_downloader_instances: Dict[str, Any],
    ) -> None:
        """执行全区文件下载流程。"""
        try:
            update_task(task_id, "running", "准备开始全区下载...")
            add_task_log(task_id, "🚀 开始全区文件下载")

            manager = get_db_path_manager()
            all_groups = manager.list_all_groups()
            filtered = apply_group_scan_filter(all_groups)
            groups = filtered["included_groups"]
            excluded_groups = filtered["excluded_groups"]
            reason_counts = filtered["reason_counts"]
            default_action = filtered["default_action"]
            for line in format_group_filter_summary(
                all_groups,
                groups,
                excluded_groups,
                reason_counts,
                default_action,
            ):
                add_task_log(task_id, line)

            if not groups:
                update_task(task_id, "completed", "全区下载完成: 过滤后无可扫描群组")
                return

            processed_groups = 0
            for i, group in enumerate(groups, 1):
                if is_task_stopped(task_id):
                    add_task_log(task_id, "🛑 任务已被用户停止")
                    break

                group_id = str(group["group_id"])
                add_task_log(task_id, "")
                add_task_log(task_id, f"👉 [{i}/{len(groups)}] 正在下载群组 {group_id} 的文件...")

                try:
                    cookie = get_cookie_for_group(group_id)
                    db_path = manager.get_files_db_path(group_id)

                    downloader = ZSXQFileDownloader(
                        cookie=cookie,
                        group_id=group_id,
                        db_path=db_path,
                        download_interval=request.download_interval,
                        long_sleep_interval=request.long_sleep_interval,
                        files_per_batch=request.files_per_batch,
                        download_interval_min=request.download_interval_min,
                        download_interval_max=request.download_interval_max,
                        long_sleep_interval_min=request.long_sleep_interval_min,
                        long_sleep_interval_max=request.long_sleep_interval_max,
                    )
                    downloader.log_callback = lambda msg: add_task_log(task_id, f"   {msg}")
                    downloader.stop_check_func = lambda: is_task_stopped(task_id)

                    file_downloader_instances[task_id] = downloader
                    res = downloader.download_files(request.max_files, sort_by=request.sort_by)

                    dl_success = res.get("downloaded", 0) if isinstance(res, dict) else res
                    add_task_log(task_id, f"   ✅ 群组 {group_id} 下载完成! 成功: {dl_success}")
                    processed_groups += 1
                except Exception as ge:
                    add_task_log(task_id, f"   ❌ 群组 {group_id} 下载异常: {ge}")
                finally:
                    if task_id in file_downloader_instances:
                        del file_downloader_instances[task_id]

            if is_task_stopped(task_id):
                update_task(task_id, "cancelled", "全区下载已停止")
            else:
                add_task_log(task_id, "")
                add_task_log(task_id, "=" * 50)
                add_task_log(task_id, f"🎉 全区文件下载完成！共处理 {processed_groups}/{len(groups)} 个群组")
                update_task(task_id, "completed", f"全区下载完成: {processed_groups} 个群组")
        except Exception as e:
            add_task_log(task_id, f"❌ 全区下载异常: {e}")
            update_task(task_id, "failed", f"全区下载失败: {e}")

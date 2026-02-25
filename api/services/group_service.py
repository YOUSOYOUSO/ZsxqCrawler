from __future__ import annotations

import gc
import os
import shutil
import time
from typing import Any, Dict

import requests
from fastapi import HTTPException

from api.services.account_resolution_service import get_account_summary_for_group_auto, get_cookie_for_group
from modules.shared.db_path_manager import get_db_path_manager
from modules.zsxq.zsxq_interactive_crawler import ZSXQInteractiveCrawler


class GroupService:
    def _build_crawler_for_group(self, group_id: str, log_callback=None) -> ZSXQInteractiveCrawler:
        cookie = get_cookie_for_group(group_id)
        if not cookie or cookie == "your_cookie_here":
            raise HTTPException(status_code=400, detail="未找到可用Cookie，请先在账号管理或 config/app.toml 中配置")
        db_path = get_db_path_manager().get_topics_db_path(group_id)
        return ZSXQInteractiveCrawler(cookie, group_id, db_path, log_callback)

    def get_group_info(self, group_id: str) -> Dict[str, Any]:
        cookie = get_cookie_for_group(group_id)

        def build_fallback(source: str = "fallback", note: str | None = None) -> Dict[str, Any]:
            files_count = 0
            try:
                crawler = self._build_crawler_for_group(group_id)
                downloader = crawler.get_file_downloader()
                try:
                    downloader.file_db.cursor.execute("SELECT COUNT(*) FROM files")
                    row = downloader.file_db.cursor.fetchone()
                    files_count = (row[0] or 0) if row else 0
                except Exception:
                    files_count = 0
            except Exception:
                files_count = 0

            try:
                gid = int(group_id)
            except Exception:
                gid = group_id

            result = {
                "group_id": gid,
                "name": f"群组 {group_id}",
                "description": "",
                "statistics": {"files": {"count": files_count}},
                "background_url": None,
                "account": get_account_summary_for_group_auto(group_id),
                "source": source,
            }
            if note:
                result["note"] = note
            return result

        if not cookie:
            return build_fallback(note="no_cookie")

        try:
            url = f"https://api.zsxq.com/v2/groups/{group_id}"
            headers = {
                "Cookie": cookie,
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            }
            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code == 200:
                data = response.json()
                if data.get("succeeded"):
                    group_data = data.get("resp_data", {}).get("group", {})
                    return {
                        "group_id": group_data.get("group_id"),
                        "name": group_data.get("name"),
                        "description": group_data.get("description"),
                        "statistics": group_data.get("statistics", {}),
                        "background_url": group_data.get("background_url"),
                        "account": get_account_summary_for_group_auto(group_id),
                        "source": "remote",
                    }
                return build_fallback(note="remote_response_failed")

            if response.status_code in (401, 403):
                return build_fallback(note=f"remote_api_{response.status_code}")
            return build_fallback(note=f"remote_api_{response.status_code}")
        except Exception:
            return build_fallback(note="exception_fallback")

    def delete_group_local(self, group_id: str) -> Dict[str, Any]:
        details = {
            "topics_db_removed": False,
            "files_db_removed": False,
            "downloads_dir_removed": False,
            "images_cache_removed": False,
            "group_dir_removed": False,
        }

        try:
            crawler = self._build_crawler_for_group(group_id)
            try:
                if hasattr(crawler, "file_downloader") and crawler.file_downloader:
                    if hasattr(crawler.file_downloader, "file_db") and crawler.file_downloader.file_db:
                        crawler.file_downloader.file_db.close()
            except Exception:
                pass
            try:
                if hasattr(crawler, "db") and crawler.db:
                    crawler.db.close()
            except Exception:
                pass
        except Exception:
            pass

        gc.collect()
        time.sleep(0.3)

        path_manager = get_db_path_manager()
        group_dir = path_manager.get_group_dir(group_id)
        topics_db = path_manager.get_topics_db_path(group_id)
        files_db = path_manager.get_files_db_path(group_id)

        try:
            if os.path.exists(topics_db):
                os.remove(topics_db)
                details["topics_db_removed"] = True
        except PermissionError as pe:
            raise HTTPException(status_code=500, detail=f"话题数据库被占用，无法删除: {pe}")

        try:
            if os.path.exists(files_db):
                os.remove(files_db)
                details["files_db_removed"] = True
        except PermissionError as pe:
            raise HTTPException(status_code=500, detail=f"文件数据库被占用，无法删除: {pe}")

        downloads_dir = os.path.join(group_dir, "downloads")
        if os.path.exists(downloads_dir):
            try:
                shutil.rmtree(downloads_dir, ignore_errors=False)
                details["downloads_dir_removed"] = True
            except Exception:
                pass

        try:
            from app.runtime.image_cache_manager import clear_group_cache_manager, get_image_cache_manager

            cache_manager = get_image_cache_manager(group_id)
            ok, _ = cache_manager.clear_cache()
            if ok:
                details["images_cache_removed"] = True
            images_dir = os.path.join(group_dir, "images")
            if os.path.exists(images_dir):
                shutil.rmtree(images_dir, ignore_errors=True)
            clear_group_cache_manager(group_id)
        except Exception:
            pass

        try:
            if os.path.exists(group_dir) and len(os.listdir(group_dir)) == 0:
                os.rmdir(group_dir)
                details["group_dir_removed"] = True
        except Exception:
            pass

        any_removed = any(details.values())
        return {
            "success": True,
            "message": f"群组 {group_id} 本地数据" + ("已删除" if any_removed else "不存在"),
            "details": details,
        }

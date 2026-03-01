from __future__ import annotations

import gc
import json
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from fastapi import HTTPException

from api.services.account_resolution_service import (
    build_account_group_detection,
    fetch_groups_from_api,
    get_account_summary_for_group_auto,
    get_cookie_for_group,
)
from modules.accounts.accounts_sql_manager import get_accounts_sql_manager
from modules.shared.db_path_manager import get_db_path_manager
from modules.zsxq.zsxq_database import ZSXQDatabase
from modules.zsxq.zsxq_interactive_crawler import ZSXQInteractiveCrawler, load_config


class GroupService:
    def __init__(self):
        self.local_output_dir = os.environ.get("OUTPUT_DIR", "output")
        try:
            self.local_scan_limit = int(os.environ.get("LOCAL_GROUPS_SCAN_LIMIT", "10000"))
        except Exception:
            self.local_scan_limit = 10000
        self._local_groups_cache = {"ids": set(), "scanned_at": 0.0}

    def _build_crawler_for_group(self, group_id: str, log_callback=None) -> ZSXQInteractiveCrawler:
        cookie = get_cookie_for_group(group_id)
        if not cookie or cookie == "your_cookie_here":
            raise HTTPException(status_code=400, detail="未找到可用Cookie，请先在账号管理或 config/app.toml 中配置")
        db_path = get_db_path_manager().get_topics_db_path(group_id)
        return ZSXQInteractiveCrawler(cookie, group_id, db_path, log_callback)

    def _get_primary_cookie(self) -> Optional[str]:
        try:
            sql_mgr = get_accounts_sql_manager()
            first_acc = sql_mgr.get_first_account(mask_cookie=False)
            if first_acc:
                cookie = (first_acc.get("cookie") or "").strip()
                if cookie:
                    return cookie
        except Exception:
            pass

        try:
            config = load_config()
            if not config:
                return None
            auth_config = config.get("auth", {}) or {}
            cookie = (auth_config.get("cookie") or "").strip()
            if cookie and cookie != "your_cookie_here":
                return cookie
        except Exception:
            return None

        return None

    def _safe_listdir(self, path: str):
        try:
            return os.listdir(path)
        except Exception:
            return []

    def _collect_numeric_dirs(self, base: str, limit: int) -> set:
        ids = set()
        if not base:
            return ids

        base_abs = os.path.abspath(base)
        if not (os.path.exists(base_abs) and os.path.isdir(base_abs)):
            return ids

        processed = 0
        for name in self._safe_listdir(base_abs):
            if not name or name.startswith("."):
                continue

            path = os.path.join(base_abs, name)
            try:
                if os.path.islink(path) or not os.path.isdir(path):
                    continue
                if name.isdigit():
                    ids.add(int(name))
                    processed += 1
                    if processed >= limit:
                        break
            except Exception:
                continue

        return ids

    def scan_local_groups(self, output_dir: str | None = None, limit: int | None = None) -> set:
        try:
            odir = output_dir or self.local_output_dir
            lim = int(limit or self.local_scan_limit)
            ids_primary = self._collect_numeric_dirs(odir, lim)
            ids_secondary = self._collect_numeric_dirs(os.path.join(odir, "databases"), lim)
            ids = set(ids_primary) | set(ids_secondary)
            self._local_groups_cache["ids"] = ids
            self._local_groups_cache["scanned_at"] = time.time()
            return ids
        except Exception:
            return self._local_groups_cache.get("ids", set())

    def get_cached_local_group_ids(self, force_refresh: bool = False) -> set:
        if force_refresh or not self._local_groups_cache.get("ids"):
            return self.scan_local_groups()
        return self._local_groups_cache.get("ids", set())

    def refresh_local_groups(self) -> Dict[str, Any]:
        try:
            ids = self.scan_local_groups()
            try:
                from modules.analyzers.global_analyzer import get_global_analyzer

                get_global_analyzer().invalidate_cache()
            except Exception:
                pass
            return {"success": True, "count": len(ids), "groups": sorted(list(ids))}
        except Exception as e:
            cached = self.get_cached_local_group_ids(force_refresh=False) or set()
            return {"success": False, "count": len(cached), "groups": sorted(list(cached)), "error": str(e)}

    def _persist_group_meta_local(self, group_id: int, info: Dict[str, Any]) -> None:
        try:
            path_manager = get_db_path_manager()
            group_dir = path_manager.get_group_data_dir(str(group_id))
            meta_path = Path(group_dir) / "group_meta.json"
            meta = {
                "group_id": group_id,
                "name": info.get("name") or f"本地群（{group_id}）",
                "type": info.get("type", ""),
                "background_url": info.get("background_url", ""),
                "owner": info.get("owner", {}) or {},
                "statistics": info.get("statistics", {}) or {},
                "create_time": info.get("create_time"),
                "subscription_time": info.get("subscription_time"),
                "expiry_time": info.get("expiry_time"),
                "join_time": info.get("join_time"),
                "last_active_time": info.get("last_active_time"),
                "description": info.get("description", ""),
                "is_trial": info.get("is_trial", False),
                "trial_end_time": info.get("trial_end_time"),
                "membership_end_time": info.get("membership_end_time"),
            }
            with meta_path.open("w", encoding="utf-8") as f:
                json.dump(meta, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def get_groups(self) -> Dict[str, Any]:
        group_account_map = build_account_group_detection()
        local_ids = self.get_cached_local_group_ids(force_refresh=False)

        groups_data: List[dict] = []
        try:
            primary_cookie = self._get_primary_cookie()
            if primary_cookie:
                groups_data = fetch_groups_from_api(primary_cookie)
        except Exception:
            groups_data = []

        by_id: Dict[int, dict] = {}

        for group in groups_data or []:
            user_specific = group.get("user_specific", {}) or {}
            validity = user_specific.get("validity", {}) or {}
            trial = user_specific.get("trial", {}) or {}

            actual_expiry_time = trial.get("end_time") or validity.get("end_time")
            is_trial = bool(trial.get("end_time"))

            status = None
            if actual_expiry_time:
                try:
                    end_time = datetime.fromisoformat(actual_expiry_time.replace("Z", "+00:00"))
                    now = datetime.now(timezone.utc)
                    days_until_expiry = (end_time - now).days
                    if days_until_expiry < 0:
                        status = "expired"
                    elif days_until_expiry <= 7:
                        status = "expiring_soon"
                    else:
                        status = "active"
                except Exception:
                    pass

            gid = group.get("group_id")
            try:
                gid = int(gid)
            except Exception:
                continue

            by_id[gid] = {
                "group_id": gid,
                "name": group.get("name", ""),
                "type": group.get("type", ""),
                "background_url": group.get("background_url", ""),
                "owner": group.get("owner", {}) or {},
                "statistics": group.get("statistics", {}) or {},
                "status": status,
                "create_time": group.get("create_time"),
                "subscription_time": validity.get("begin_time"),
                "expiry_time": actual_expiry_time,
                "join_time": user_specific.get("join_time"),
                "last_active_time": user_specific.get("last_active_time"),
                "description": group.get("description", ""),
                "is_trial": is_trial,
                "trial_end_time": trial.get("end_time"),
                "membership_end_time": validity.get("end_time"),
                "account": group_account_map.get(str(gid)),
                "source": "account",
            }

        for gid in local_ids or []:
            try:
                gid_int = int(gid)
            except Exception:
                continue

            if gid_int in by_id:
                src = by_id[gid_int].get("source", "account")
                if "local" not in src:
                    by_id[gid_int]["source"] = "account|local"
                self._persist_group_meta_local(gid_int, by_id[gid_int])
                continue

            local_name = f"本地群（{gid_int}）"
            local_type = "local"
            local_bg = ""
            owner: Dict[str, Any] = {}
            join_time = None
            expiry_time = None
            last_active_time = None
            description = ""
            statistics: Dict[str, Any] = {}

            try:
                path_manager = get_db_path_manager()
                group_dir = path_manager.get_group_data_dir(str(gid_int))
                meta_path = Path(group_dir) / "group_meta.json"
                if meta_path.exists():
                    with meta_path.open("r", encoding="utf-8") as f:
                        meta = json.load(f)
                    local_name = meta.get("name", local_name)
                    local_type = meta.get("type", local_type)
                    local_bg = meta.get("background_url", local_bg)
                    owner = meta.get("owner", {}) or owner
                    statistics = meta.get("statistics", {}) or statistics
                    join_time = meta.get("join_time", join_time)
                    expiry_time = meta.get("expiry_time", expiry_time)
                    last_active_time = meta.get("last_active_time", last_active_time)
                    description = meta.get("description", description)
            except Exception:
                pass

            try:
                path_manager = get_db_path_manager()
                db_paths = path_manager.list_group_databases(str(gid_int))
                topics_db = db_paths.get("topics")
                if topics_db and os.path.exists(topics_db):
                    db = ZSXQDatabase(topics_db)
                    try:
                        cur = db.cursor
                        if not local_bg or local_name.startswith("本地群（"):
                            cur.execute(
                                "SELECT name, type, background_url FROM groups WHERE group_id = ? LIMIT 1",
                                (gid_int,),
                            )
                            row = cur.fetchone()
                            if row:
                                if row[0]:
                                    local_name = row[0]
                                if row[1]:
                                    local_type = row[1]
                                if row[2]:
                                    local_bg = row[2]

                        if not join_time or not expiry_time:
                            cur.execute(
                                """
                                SELECT MIN(create_time), MAX(create_time)
                                FROM topics
                                WHERE group_id = ? AND create_time IS NOT NULL AND create_time != ''
                                """,
                                (gid_int,),
                            )
                            trow = cur.fetchone()
                            if trow:
                                if not join_time:
                                    join_time = trow[0]
                                if not expiry_time:
                                    expiry_time = trow[1]
                                if not last_active_time:
                                    last_active_time = trow[1]

                        if not statistics:
                            cur.execute("SELECT COUNT(*) FROM topics WHERE group_id = ?", (gid_int,))
                            topics_count = cur.fetchone()[0] or 0
                            statistics = {"topics": {"topics_count": topics_count, "answers_count": 0, "digests_count": 0}}
                    finally:
                        db.close()
            except Exception:
                pass

            by_id[gid_int] = {
                "group_id": gid_int,
                "name": local_name,
                "type": local_type,
                "background_url": local_bg,
                "owner": owner,
                "statistics": statistics,
                "status": None,
                "create_time": join_time,
                "subscription_time": None,
                "expiry_time": expiry_time,
                "join_time": join_time,
                "last_active_time": last_active_time,
                "description": description,
                "is_trial": False,
                "trial_end_time": None,
                "membership_end_time": None,
                "account": None,
                "source": "local",
            }

        merged = [by_id[k] for k in sorted(by_id.keys())]
        return {"groups": merged, "total": len(merged)}

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

        try:
            gid_int = int(group_id)
            if gid_int in self._local_groups_cache.get("ids", set()):
                self._local_groups_cache["ids"].discard(gid_int)
                self._local_groups_cache["scanned_at"] = time.time()
        except Exception:
            pass

        any_removed = any(details.values())
        return {
            "success": True,
            "message": f"群组 {group_id} 本地数据" + ("已删除" if any_removed else "不存在"),
            "details": details,
        }

    def delete_all_groups_local(self) -> Dict[str, Any]:
        group_ids = sorted(list(self.scan_local_groups()))

        results: List[Dict[str, Any]] = []
        deleted_count = 0
        failed_count = 0

        for gid in group_ids:
            gid_str = str(gid)
            try:
                res = self.delete_group_local(gid_str)
                details = res.get("details", {}) if isinstance(res, dict) else {}
                removed = any(bool(v) for v in details.values()) if isinstance(details, dict) else False
                if removed:
                    deleted_count += 1
                results.append(
                    {
                        "group_id": gid_str,
                        "success": True,
                        "deleted": removed,
                        "message": res.get("message") if isinstance(res, dict) else "",
                    }
                )
            except Exception as e:
                failed_count += 1
                results.append(
                    {
                        "group_id": gid_str,
                        "success": False,
                        "deleted": False,
                        "message": str(e),
                    }
                )

        self._local_groups_cache["ids"] = set()
        self._local_groups_cache["scanned_at"] = time.time()

        try:
            from modules.analyzers.global_analyzer import get_global_analyzer

            get_global_analyzer().invalidate_cache()
        except Exception:
            pass

        return {
            "success": failed_count == 0,
            "total_groups": len(group_ids),
            "deleted_groups": deleted_count,
            "failed_groups": failed_count,
            "results": results,
            "message": f"全量删除完成: 总计 {len(group_ids)} 个群组，成功删除 {deleted_count} 个，失败 {failed_count} 个",
        }

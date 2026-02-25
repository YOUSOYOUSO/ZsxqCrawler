from __future__ import annotations

from typing import Any, Dict, Optional

from modules.accounts.accounts_sql_manager import get_accounts_sql_manager
from modules.shared.db_path_manager import get_db_path_manager
from modules.zsxq.zsxq_database import ZSXQDatabase
from modules.zsxq.zsxq_file_database import ZSXQFileDatabase
from modules.zsxq.zsxq_interactive_crawler import load_config


class DatabaseStatsService:
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

    def _is_configured(self) -> bool:
        return self._get_primary_cookie() is not None

    def get_database_stats(self) -> Dict[str, Any]:
        configured = self._is_configured()
        if not configured:
            return {
                "configured": False,
                "topic_database": {
                    "stats": {},
                    "timestamp_info": {
                        "total_topics": 0,
                        "oldest_timestamp": "",
                        "newest_timestamp": "",
                        "has_data": False,
                    },
                },
                "file_database": {"stats": {}},
            }

        path_manager = get_db_path_manager()
        groups_info = path_manager.list_all_groups()
        if not groups_info:
            return {
                "configured": True,
                "topic_database": {
                    "stats": {},
                    "timestamp_info": {
                        "total_topics": 0,
                        "oldest_timestamp": "",
                        "newest_timestamp": "",
                        "has_data": False,
                    },
                },
                "file_database": {"stats": {}},
            }

        aggregated_topic_stats: Dict[str, int] = {}
        aggregated_file_stats: Dict[str, int] = {}
        oldest_ts: Optional[str] = None
        newest_ts: Optional[str] = None
        total_topics = 0
        has_data = False

        for gi in groups_info:
            group_id = gi.get("group_id")
            topics_db_path = gi.get("topics_db")
            if not topics_db_path:
                continue

            db = ZSXQDatabase(topics_db_path)
            try:
                topic_stats = db.get_database_stats()
                ts_info = db.get_timestamp_range_info()
            finally:
                db.close()

            for table, count in (topic_stats or {}).items():
                aggregated_topic_stats[table] = aggregated_topic_stats.get(table, 0) + int(count or 0)

            if ts_info.get("has_data"):
                has_data = True
                ot = ts_info.get("oldest_timestamp")
                nt = ts_info.get("newest_timestamp")
                if ot and (oldest_ts is None or ot < oldest_ts):
                    oldest_ts = ot
                if nt and (newest_ts is None or nt > newest_ts):
                    newest_ts = nt
                total_topics += int(ts_info.get("total_topics") or 0)

            db_paths = path_manager.list_group_databases(str(group_id))
            files_db_path = db_paths.get("files")
            if files_db_path:
                fdb = ZSXQFileDatabase(files_db_path)
                try:
                    file_stats = fdb.get_database_stats()
                finally:
                    fdb.close()

                for table, count in (file_stats or {}).items():
                    aggregated_file_stats[table] = aggregated_file_stats.get(table, 0) + int(count or 0)

        return {
            "configured": True,
            "topic_database": {
                "stats": aggregated_topic_stats,
                "timestamp_info": {
                    "total_topics": total_topics,
                    "oldest_timestamp": oldest_ts or "",
                    "newest_timestamp": newest_ts or "",
                    "has_data": has_data,
                },
            },
            "file_database": {"stats": aggregated_file_stats},
        }

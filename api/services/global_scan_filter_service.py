from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any, Dict

from modules.shared.db_path_manager import get_db_path_manager
from modules.shared.group_scan_filter import CONFIG_FILE, decide_group, get_filter_config, save_filter_config


class GlobalScanFilterService:
    def get_config(self) -> Dict[str, Any]:
        data = get_filter_config()
        data["source_file"] = CONFIG_FILE
        return data

    def update_config(self, default_action: str, whitelist_group_ids: list[str], blacklist_group_ids: list[str]) -> Dict[str, Any]:
        data = save_filter_config(
            default_action=default_action,
            whitelist_group_ids=whitelist_group_ids,
            blacklist_group_ids=blacklist_group_ids,
        )
        return {
            **data,
            "effective_counts": {
                "whitelist": len(data.get("whitelist_group_ids", [])),
                "blacklist": len(data.get("blacklist_group_ids", [])),
            },
        }

    def _get_group_name(self, group_id: str, topics_db_path: str | None) -> str:
        if topics_db_path and os.path.exists(topics_db_path):
            try:
                conn = sqlite3.connect(topics_db_path, timeout=10)
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM groups WHERE group_id = ? LIMIT 1", (int(group_id),))
                row = cursor.fetchone()
                conn.close()
                if row and row[0]:
                    return str(row[0])
            except Exception:
                pass

        try:
            path_manager = get_db_path_manager()
            group_dir = path_manager.get_group_data_dir(str(group_id))
            meta_path = Path(group_dir) / "group_meta.json"
            if meta_path.exists():
                import json

                with meta_path.open("r", encoding="utf-8") as f:
                    meta = json.load(f)
                if meta.get("name"):
                    return str(meta["name"])
        except Exception:
            pass

        return ""

    def preview(self, exclude_non_stock: bool = True) -> Dict[str, Any]:
        manager = get_db_path_manager()
        groups = manager.list_all_groups()

        included_groups = []
        excluded_groups = []
        reason_counts: Dict[str, int] = {}

        for g in groups:
            gid = str(g.get("group_id"))
            gname = self._get_group_name(gid, g.get("topics_db"))
            decision, reason = decide_group(gid)

            item = {
                "group_id": gid,
                "group_name": gname or gid,
                "decision": decision,
                "reason": reason,
            }
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
            if decision == "included":
                included_groups.append(item)
            else:
                excluded_groups.append(item)

        return {
            "total_groups": len(groups),
            "included_groups": included_groups,
            "excluded_groups": excluded_groups,
            "reason_counts": reason_counts,
            "compat_note": (
                "exclude_non_stock 参数已兼容保留，当前版本始终应用白黑名单规则"
                if exclude_non_stock is False
                else None
            ),
        }

    def preview_blacklist_cleanup(self) -> Dict[str, Any]:
        cfg = get_filter_config()
        blacklist_ids = {str(v).strip() for v in cfg.get("blacklist_group_ids", []) if str(v).strip()}
        manager = get_db_path_manager()
        groups = manager.list_all_groups()

        details = []
        total_mentions = 0
        total_performance = 0

        for g in groups:
            gid = str(g.get("group_id", "")).strip()
            if not gid or gid not in blacklist_ids:
                continue

            db_path = g.get("topics_db")
            mentions_count = 0
            perf_count = 0
            if db_path and os.path.exists(db_path):
                conn = None
                try:
                    conn = sqlite3.connect(db_path, timeout=30)
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='stock_mentions'")
                    if (cursor.fetchone() or [0])[0]:
                        cursor.execute("SELECT COUNT(*) FROM stock_mentions")
                        mentions_count = int((cursor.fetchone() or [0])[0] or 0)
                        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='mention_performance'")
                        if (cursor.fetchone() or [0])[0]:
                            cursor.execute("SELECT COUNT(*) FROM mention_performance")
                            perf_count = int((cursor.fetchone() or [0])[0] or 0)
                except Exception:
                    pass
                finally:
                    if conn:
                        conn.close()

            total_mentions += mentions_count
            total_performance += perf_count
            details.append(
                {
                    "group_id": gid,
                    "group_name": self._get_group_name(gid, db_path),
                    "stock_mentions_count": mentions_count,
                    "mention_performance_count": perf_count,
                }
            )

        return {
            "blacklist_group_count": len(blacklist_ids),
            "matched_group_count": len(details),
            "total_stock_mentions": total_mentions,
            "total_mention_performance": total_performance,
            "groups": details,
        }

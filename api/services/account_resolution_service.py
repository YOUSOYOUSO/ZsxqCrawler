from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import requests

from modules.accounts.accounts_sql_manager import get_accounts_sql_manager
from modules.zsxq.zsxq_interactive_crawler import load_config

ACCOUNT_DETECT_TTL_SECONDS = 300

_account_detect_cache: Dict[str, Any] = {
    "built_at": 0,
    "group_to_account": {},
    "cookie_by_account": {},
}


def _build_headers(cookie: str) -> Dict[str, str]:
    return {
        "Cookie": cookie,
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "https://wx.zsxq.com/",
        "Origin": "https://wx.zsxq.com",
        "Accept": "application/json, text/plain, */*",
    }


def fetch_groups_from_api(cookie: str) -> List[Dict[str, Any]]:
    if not cookie:
        return []

    resp = requests.get("https://api.zsxq.com/v2/groups", headers=_build_headers(cookie), timeout=30)
    resp.raise_for_status()
    payload = resp.json() if resp.content else {}
    if not payload.get("succeeded"):
        return []
    resp_data = payload.get("resp_data", {}) or {}
    groups = resp_data.get("groups", [])
    return groups if isinstance(groups, list) else []


def clear_account_detect_cache() -> None:
    _account_detect_cache["built_at"] = 0


def _get_all_account_sources() -> List[Dict[str, Any]]:
    sources: List[Dict[str, Any]] = []
    try:
        sql_mgr = get_accounts_sql_manager()
        accounts = sql_mgr.get_accounts(mask_cookie=False)
        if accounts:
            sources.extend(accounts)
    except Exception:
        pass
    return sources


def build_account_group_detection(force_refresh: bool = False) -> Dict[str, Dict[str, Any]]:
    now = time.time()
    cache = _account_detect_cache
    if (
        not force_refresh
        and cache.get("group_to_account")
        and now - cache.get("built_at", 0) < ACCOUNT_DETECT_TTL_SECONDS
    ):
        return cache["group_to_account"]

    group_to_account: Dict[str, Dict[str, Any]] = {}
    cookie_by_account: Dict[str, str] = {}

    for src in _get_all_account_sources():
        cookie = src.get("cookie", "")
        acc_id = src.get("id")
        if not cookie or cookie == "your_cookie_here" or not acc_id:
            continue

        cookie_by_account[acc_id] = cookie

        try:
            groups = fetch_groups_from_api(cookie)
            for g in groups:
                gid = str(g.get("group_id"))
                if gid and gid not in group_to_account:
                    group_to_account[gid] = {
                        "id": acc_id,
                        "name": src.get("name") or acc_id,
                        "created_at": src.get("created_at"),
                        "cookie": "***",
                    }
        except Exception:
            continue

    cache["group_to_account"] = group_to_account
    cache["cookie_by_account"] = cookie_by_account
    cache["built_at"] = now
    return group_to_account


def get_cookie_for_group(group_id: str) -> str:
    mapping = build_account_group_detection(force_refresh=False)
    summary = mapping.get(str(group_id))
    cookie = None
    if summary:
        cookie = _account_detect_cache.get("cookie_by_account", {}).get(summary["id"])
    if not cookie:
        cfg = load_config()
        auth = cfg.get("auth", {}) if cfg else {}
        cookie = auth.get("cookie", "")
    return cookie


def get_account_summary_for_group_auto(group_id: str) -> Optional[Dict[str, Any]]:
    mapping = build_account_group_detection(force_refresh=False)
    summary = mapping.get(str(group_id))
    if summary:
        return summary

    try:
        sql_mgr = get_accounts_sql_manager()
        first_acc = sql_mgr.get_first_account(mask_cookie=True)
        if first_acc:
            return {
                "id": first_acc["id"],
                "name": first_acc["name"],
                "created_at": first_acc["created_at"],
                "cookie": first_acc["cookie"],
            }
    except Exception:
        pass
    return None

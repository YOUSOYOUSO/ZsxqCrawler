#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Group scan filter config (manual whitelist/blacklist only)."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Tuple
from modules.shared.paths import get_config_path

CONFIG_FILE = str(get_config_path("group_scan_filter.json"))
BJ_TZ = timezone(timedelta(hours=8))

_DEFAULT = {
    "version": 1,
    "updated_at": None,
    "default_action": "include",
    "whitelist_group_ids": [],
    "blacklist_group_ids": [],
}

_cache_mtime: float = -1.0
_cache_data: Dict[str, Any] | None = None


def _normalize_group_ids(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []
    normalized: List[str] = []
    for v in values:
        gid = str(v).strip()
        if gid and gid not in normalized:
            normalized.append(gid)
    return normalized


def _normalize_config(payload: Any) -> Dict[str, Any]:
    data = dict(_DEFAULT)
    if isinstance(payload, dict):
        data["version"] = int(payload.get("version", 1) or 1)
        data["updated_at"] = payload.get("updated_at") or None
        default_action = str(payload.get("default_action", "include")).strip().lower()
        data["default_action"] = default_action if default_action in {"include", "exclude"} else "include"
        data["whitelist_group_ids"] = _normalize_group_ids(payload.get("whitelist_group_ids"))
        data["blacklist_group_ids"] = _normalize_group_ids(payload.get("blacklist_group_ids"))

    overlap = sorted(set(data["whitelist_group_ids"]) & set(data["blacklist_group_ids"]))
    if overlap:
        raise ValueError(f"group_id 同时存在白名单与黑名单: {', '.join(overlap)}")

    return data


def _now_iso() -> str:
    return datetime.now(BJ_TZ).isoformat()


def _write_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    payload = _normalize_config(cfg)
    payload["updated_at"] = _now_iso()
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return payload


def _ensure_file() -> None:
    if not os.path.exists(CONFIG_FILE):
        _write_config(dict(_DEFAULT))


def get_filter_config() -> Dict[str, Any]:
    global _cache_mtime, _cache_data
    _ensure_file()
    mtime = os.path.getmtime(CONFIG_FILE)
    if _cache_data is not None and mtime == _cache_mtime:
        return dict(_cache_data)

    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        raw = json.load(f)
    cfg = _normalize_config(raw)
    _cache_data = cfg
    _cache_mtime = mtime
    return dict(cfg)


def save_filter_config(
    whitelist_group_ids: List[str],
    blacklist_group_ids: List[str],
    default_action: str = "include",
) -> Dict[str, Any]:
    global _cache_mtime, _cache_data
    payload = {
        "version": 1,
        "updated_at": _now_iso(),
        "default_action": str(default_action).strip().lower() or "include",
        "whitelist_group_ids": _normalize_group_ids(whitelist_group_ids),
        "blacklist_group_ids": _normalize_group_ids(blacklist_group_ids),
    }
    cfg = _write_config(payload)
    _cache_data = cfg
    _cache_mtime = os.path.getmtime(CONFIG_FILE)
    return dict(cfg)


def decide_group(group_id: str) -> Tuple[str, str]:
    """Return (decision, reason): included/excluded."""
    cfg = get_filter_config()
    gid = str(group_id)

    if gid in cfg["blacklist_group_ids"]:
        return "excluded", "blacklisted"
    if gid in cfg["whitelist_group_ids"]:
        return "included", "whitelisted"
    if cfg.get("default_action") == "exclude":
        return "excluded", "default_excluded"
    return "included", "default_included"


def filter_groups(groups: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Apply whitelist/blacklist/default policy to a group list."""
    cfg = get_filter_config()
    included_groups: List[Dict[str, Any]] = []
    excluded_groups: List[Dict[str, Any]] = []
    reason_counts: Dict[str, int] = {}

    for group in groups or []:
        gid = str((group or {}).get("group_id", "")).strip()
        if not gid:
            continue
        decision, reason = decide_group(gid)
        reason_counts[reason] = reason_counts.get(reason, 0) + 1

        item = dict(group)
        item["group_id"] = gid
        item["scan_filter_decision"] = decision
        item["scan_filter_reason"] = reason
        if decision == "included":
            included_groups.append(item)
        else:
            excluded_groups.append(item)

    return {
        "config": cfg,
        "included_groups": included_groups,
        "excluded_groups": excluded_groups,
        "reason_counts": reason_counts,
    }

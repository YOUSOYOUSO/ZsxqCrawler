#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票排除规则加载与匹配
支持两种格式：
1) ["机器人", ...] -> 作为关键词列表
2) {"keywords": [...], "stock_names": [...], "stock_codes": [...]}
"""

from __future__ import annotations

import json
import os
from typing import Dict, List, Tuple, Any
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXCLUDE_FILES = [
    PROJECT_ROOT / "config" / "stock_exclude.json",
    PROJECT_ROOT / "stock_exclude.json",  # legacy fallback
]

_cache_mtime: float = -1.0
_cache_path: str = ""
_cache_rules: Dict[str, set] = {
    "keywords": set(),
    "stock_names": set(),
    "stock_codes": set(),
}


def _normalize_rules(payload: Any) -> Dict[str, set]:
    rules = {
        "keywords": set(),
        "stock_names": set(),
        "stock_codes": set(),
    }

    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, str) and item.strip():
                rules["keywords"].add(item.strip().lower())
        return rules

    if isinstance(payload, dict):
        for item in payload.get("keywords", []):
            if isinstance(item, str) and item.strip():
                rules["keywords"].add(item.strip().lower())
        for item in payload.get("stock_names", []):
            if isinstance(item, str) and item.strip():
                rules["stock_names"].add(item.strip().lower())
        for item in payload.get("stock_codes", []):
            if isinstance(item, str) and item.strip():
                rules["stock_codes"].add(item.strip().upper())
    return rules


def _load_rules() -> Dict[str, set]:
    global _cache_mtime, _cache_path, _cache_rules
    path_obj = None
    for candidate in EXCLUDE_FILES:
        if candidate.exists():
            path_obj = candidate
            break

    if path_obj is None:
        _cache_mtime = -1.0
        _cache_path = ""
        _cache_rules = {"keywords": set(), "stock_names": set(), "stock_codes": set()}
        return _cache_rules

    path = str(path_obj)
    mtime = os.path.getmtime(path)
    if path == _cache_path and mtime == _cache_mtime:
        return _cache_rules

    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        _cache_rules = _normalize_rules(payload)
        _cache_mtime = mtime
        _cache_path = path
    except Exception:
        _cache_rules = {"keywords": set(), "stock_names": set(), "stock_codes": set()}
        _cache_mtime = mtime
        _cache_path = path
    return _cache_rules


def get_exclusion_rules() -> Dict[str, List[str]]:
    rules = _load_rules()
    return {
        "keywords": sorted(rules["keywords"]),
        "stock_names": sorted(rules["stock_names"]),
        "stock_codes": sorted(rules["stock_codes"]),
    }


def is_excluded_stock(stock_code: str | None, stock_name: str | None) -> bool:
    rules = _load_rules()
    code = (stock_code or "").strip().upper()
    name = (stock_name or "").strip().lower()

    if not code and not name:
        return False

    if code and code in rules["stock_codes"]:
        return True
    if name and name in rules["stock_names"]:
        return True
    if name:
        for kw in rules["keywords"]:
            if kw and kw in name:
                return True
    return False


def build_sql_exclusion_clause(code_col: str, name_col: str) -> Tuple[str, List[Any]]:
    """
    返回适用于 SQL WHERE 的排除子句（前置带 AND）
    """
    rules = _load_rules()
    parts: List[str] = []
    params: List[Any] = []

    if rules["stock_codes"]:
        placeholders = ",".join(["?"] * len(rules["stock_codes"]))
        parts.append(f"UPPER({code_col}) NOT IN ({placeholders})")
        params.extend(sorted(rules["stock_codes"]))

    if rules["stock_names"]:
        placeholders = ",".join(["?"] * len(rules["stock_names"]))
        parts.append(f"LOWER({name_col}) NOT IN ({placeholders})")
        params.extend(sorted(rules["stock_names"]))

    for kw in sorted(rules["keywords"]):
        parts.append(f"LOWER({name_col}) NOT LIKE ?")
        params.append(f"%{kw}%")

    if not parts:
        return "", []
    return " AND " + " AND ".join(parts), params

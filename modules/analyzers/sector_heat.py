#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
板块热度共享计算工具：
1. 统一时间过滤边界（end_date 按“次日零点前”处理，包含结束日当天）
2. 统一文本关键词命中与板块聚合逻辑（按帖子计数）
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


def _normalize_date_str(day: Optional[str]) -> Optional[str]:
    if not day:
        return None
    value = str(day).strip()
    if not value:
        return None
    return value[:10]


def _build_exclusive_end(day: Optional[str]) -> Optional[str]:
    normalized = _normalize_date_str(day)
    if not normalized:
        return None
    try:
        dt = datetime.strptime(normalized, "%Y-%m-%d")
        return (dt + timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        return None


def build_topic_time_filter(
    start_date: Optional[str],
    end_date: Optional[str],
    column: str = "t.create_time",
) -> Tuple[str, List[str]]:
    """构造 topics.create_time 的 SQL 过滤条件。

    - start_date: 包含当天（>= start_date）
    - end_date: 包含当天（< end_date + 1day）
    - 若 end_date 格式不合法，回退为 <= end_date（保持兼容）
    """
    clauses: List[str] = []
    params: List[str] = []

    start = _normalize_date_str(start_date)
    if start:
        clauses.append(f"{column} >= ?")
        params.append(start)

    end_exclusive = _build_exclusive_end(end_date)
    if end_exclusive:
        clauses.append(f"{column} < ?")
        params.append(end_exclusive)
    else:
        end_raw = _normalize_date_str(end_date)
        if end_raw:
            clauses.append(f"{column} <= ?")
            params.append(end_raw)

    if not clauses:
        return "", params
    return f"AND {' AND '.join(clauses)}", params


def match_sector_keywords(
    text: str,
    sector_keywords: Dict[str, Sequence[str]],
) -> Dict[str, List[str]]:
    """返回文本命中的板块及其关键词列表。"""
    text_lower = (text or "").lower()
    if not text_lower:
        return {}

    hits: Dict[str, List[str]] = {}
    for sector, keywords in sector_keywords.items():
        matched = [kw for kw in keywords if kw in text_lower]
        if matched:
            hits[sector] = matched
    return hits


def aggregate_sector_heat(
    topics: Iterable[Tuple[str, Any]],
    sector_keywords: Dict[str, Sequence[str]],
) -> List[Dict[str, Any]]:
    """按帖子聚合板块热度。每条帖子命中某板块计 1 次。"""
    totals: Dict[str, int] = defaultdict(int)
    daily: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for text, create_time in topics:
        if not text:
            continue
        date_key = str(create_time or "")[:10]
        if not date_key:
            continue
        matched = match_sector_keywords(text, sector_keywords)
        if not matched:
            continue

        for sector in matched.keys():
            totals[sector] += 1
            daily[sector][date_key] += 1

    results: List[Dict[str, Any]] = []
    for sector, total in totals.items():
        daily_map = dict(sorted(daily[sector].items()))
        peak_date = None
        peak_count = 0
        if daily_map:
            peak_date, peak_count = max(daily_map.items(), key=lambda kv: kv[1])

        results.append(
            {
                "sector": sector,
                "total_mentions": int(total),
                "daily_mentions": daily_map,
                "peak_date": peak_date,
                "peak_count": int(peak_count),
            }
        )

    results.sort(key=lambda x: (-int(x["total_mentions"]), str(x["sector"])))
    return results

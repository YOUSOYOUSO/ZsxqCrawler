#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Global pipeline orchestration shared by scheduler and global actions."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from modules.shared.db_path_manager import get_db_path_manager


LogCallback = Optional[Callable[[str], None]]


def _log(log_callback: LogCallback, message: str) -> None:
    if log_callback:
        log_callback(message)


def list_groups(apply_scan_filter: bool = True) -> List[Dict[str, Any]]:
    manager = get_db_path_manager()
    groups = manager.list_all_groups()
    if not apply_scan_filter:
        return groups

    try:
        from modules.shared.group_scan_filter import filter_groups
        return filter_groups(groups).get("included_groups", [])
    except Exception:
        # Fail-open to avoid scheduler hard stop on transient config read failure.
        return groups


def run_group_incremental_pipeline(
    group_id: str,
    *,
    pages: int = 2,
    per_page: int = 20,
    calc_window_days: int = 365,
    do_analysis: bool = True,
    log_callback: LogCallback = None,
) -> Dict[str, Any]:
    """Run incremental crawl -> extract -> optional analyze for a single group."""
    from api.services.account_resolution_service import get_cookie_for_group
    from modules.analyzers.stock_analyzer import StockAnalyzer
    from modules.zsxq.zsxq_interactive_crawler import ZSXQInteractiveCrawler

    result: Dict[str, Any] = {
        "group_id": group_id,
        "crawl": {},
        "extract": {},
        "analyze": {},
        "started_at": datetime.now().isoformat(),
        "ok": False,
    }

    cookie = get_cookie_for_group(group_id)
    if not cookie or cookie == "your_cookie_here":
        raise RuntimeError(f"group {group_id} missing valid cookie")
    db_path = get_db_path_manager().get_topics_db_path(group_id)
    crawler = ZSXQInteractiveCrawler(cookie, group_id, db_path, log_callback=lambda msg: _log(log_callback, f"  [{group_id}] {msg}"))
    crawl_res = crawler.crawl_incremental(pages=pages, per_page=per_page)
    result["crawl"] = crawl_res or {}

    analyzer = StockAnalyzer(group_id)
    extract_res = analyzer.extract_only()
    result["extract"] = extract_res or {}

    if do_analysis:
        analyze_res = analyzer.calc_pending_performance(calc_window_days=calc_window_days)
        result["analyze"] = analyze_res or {}

    result["finished_at"] = datetime.now().isoformat()
    result["ok"] = True
    return result


def run_serial_incremental_pipeline(
    *,
    groups: Optional[List[Dict[str, Any]]] = None,
    pages: int = 2,
    per_page: int = 20,
    calc_window_days: int = 365,
    do_analysis: bool = True,
    stop_check: Optional[Callable[[], bool]] = None,
    log_callback: LogCallback = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Run incremental pipeline serially for all groups."""
    group_rows = groups if groups is not None else list_groups()
    successes: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []

    for idx, group in enumerate(group_rows, 1):
        if stop_check and stop_check():
            _log(log_callback, "🛑 检测到停止信号，终止后续群组执行")
            break

        group_id = str(group.get("group_id"))
        _log(log_callback, f"👉 [{idx}/{len(group_rows)}] 开始处理群组 {group_id}")

        try:
            res = run_group_incremental_pipeline(
                group_id,
                pages=pages,
                per_page=per_page,
                calc_window_days=calc_window_days,
                do_analysis=do_analysis,
                log_callback=log_callback,
            )
            successes.append(res)

            mentions = (res.get("extract") or {}).get("mentions_extracted", 0)
            processed = (res.get("analyze") or {}).get("processed", 0)
            _log(log_callback, f"   ✅ 群组 {group_id} 完成：提及 {mentions}，收益计算 {processed}")
        except Exception as e:
            failures.append({"group_id": group_id, "error": str(e)})
            _log(log_callback, f"   ❌ 群组 {group_id} 失败: {e}")

    return successes, failures

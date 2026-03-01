import time
import os
from typing import Any, Callable, Dict, List, Set
from datetime import datetime, timedelta

from modules.shared.db_path_manager import get_db_path_manager
from modules.analyzers.global_analyzer import get_global_analyzer
from modules.analyzers.stock_analyzer import StockAnalyzer
from modules.analyzers.market_data_sync import MarketDataSyncService
from api.services.group_filter_service import apply_group_scan_filter, format_group_filter_summary
from modules.shared.logger_config import log_warning


class GlobalAnalyzePerformanceService:
    """全区收益计算服务（从 main.py 拆出业务流程）。"""

    def _collect_pending_stock_codes(
        self,
        analyzer: StockAnalyzer,
        calc_window_days: int,
    ) -> Set[str]:
        """收集群组中待计算收益的股票代码集合（仅本地 SQL 查询）。"""
        since_date = (datetime.now() - timedelta(days=calc_window_days)).strftime("%Y-%m-%d")
        conn = analyzer._get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT sm.stock_code
            FROM stock_mentions sm
            LEFT JOIN mention_performance mp ON sm.id = mp.mention_id
            WHERE (mp.mention_id IS NULL OR mp.freeze_level IS NULL OR mp.freeze_level < 3)
              AND sm.mention_date >= ?
            """,
            (since_date,),
        )
        codes = {str(row[0]) for row in cursor.fetchall()}
        conn.close()
        return codes

    def run(
        self,
        task_id: str,
        add_task_log: Callable[[str, str], None],
        update_task: Callable[..., Any],
        is_task_stopped: Callable[[str], bool],
        calc_window_days: int = 365,
    ) -> None:
        """执行全区收益计算主流程。"""
        try:
            update_task(task_id, "running", "准备开始全区收益计算...")
            add_task_log(task_id, "🚀 开始全区提及收益刷新")
            since_date = (datetime.now() - timedelta(days=max(int(calc_window_days or 1), 1))).strftime("%Y-%m-%d")
            add_task_log(task_id, f"🗓️ 收益计算窗口: since={since_date} (calc_window_days={int(calc_window_days or 1)})")

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
                update_task(task_id, "completed", "全区收益计算完成: 过滤后无可扫描群组")
                return

            processed_groups = 0
            groups_with_auto_extract = 0
            mentions_extracted_total = 0
            performance_processed_total = 0

            # ── Phase 1: 全群提取 + 收集待算股票代码 ──
            add_task_log(task_id, "")
            add_task_log(task_id, "═" * 40)
            add_task_log(task_id, "📋 Phase 1: 全群增量提取 & 收集待算股票")
            all_pending_stocks: Set[str] = set()
            group_analyzers: Dict[str, StockAnalyzer] = {}

            for i, group in enumerate(groups, 1):
                if is_task_stopped(task_id):
                    add_task_log(task_id, "🛑 任务已被用户停止")
                    break

                group_id = str(group["group_id"])
                analyzer = StockAnalyzer(
                    group_id,
                    stop_check=lambda: is_task_stopped(task_id),
                )
                group_analyzers[group_id] = analyzer

                # 增量提取
                extract_res = analyzer.extract_only()
                extracted_mentions = int(extract_res.get("mentions_extracted", 0) or 0)
                new_topics = int(extract_res.get("new_topics", 0) or 0)
                if new_topics > 0 or extracted_mentions > 0:
                    groups_with_auto_extract += 1
                mentions_extracted_total += extracted_mentions

                # 收集待算股票代码
                pending_codes = self._collect_pending_stock_codes(analyzer, calc_window_days)
                all_pending_stocks |= pending_codes

                add_task_log(
                    task_id,
                    f"   [{i}/{len(groups)}] 群 {group_id}: 提取 {extracted_mentions} 条, 待算股票 {len(pending_codes)} 只",
                )

            # ── Phase 1.5: 全局行情预热（一次性） ──
            if all_pending_stocks and not is_task_stopped(task_id):
                add_task_log(task_id, "")
                add_task_log(task_id, f"🧰 全局行情预热: 共 {len(all_pending_stocks)} 只唯一股票")
                prewarm_started = time.perf_counter()
                try:
                    market_sync = MarketDataSyncService()
                    history_days = max(20, int(calc_window_days) + 20)
                    end_date = datetime.now().strftime("%Y-%m-%d")
                    start_date = (datetime.now() - timedelta(days=history_days)).strftime("%Y-%m-%d")

                    # 优先尝试 tushare 按日期批量模式（~14次API vs ~1700次）
                    batch_res = market_sync.sync_daily_by_dates(
                        start_date=start_date,
                        end_date=end_date,
                        symbols=sorted(all_pending_stocks),
                        include_index=True,
                    )
                    if batch_res.get("success") or batch_res.get("upserted", 0) > 0:
                        prewarm_elapsed = time.perf_counter() - prewarm_started
                        add_task_log(
                            task_id,
                            f"🧰 全局预热完成 (批量日期模式): api_calls={batch_res.get('api_calls', 0)}, "
                            f"upserted={batch_res.get('upserted', 0)}, elapsed={prewarm_elapsed:.1f}s",
                        )
                    else:
                        # 回退到逐股分片模式
                        add_task_log(
                            task_id,
                            f"⚠️ 批量日期模式不可用 ({batch_res.get('message', '')}), 回退到逐股预热",
                        )
                        chunk_size = max(1, int(os.environ.get("PERF_PREWARM_CHUNK_SIZE", "200")))
                        symbols_sorted = sorted(all_pending_stocks)
                        prewarm_chunks = [
                            symbols_sorted[j : j + chunk_size]
                            for j in range(0, len(symbols_sorted), chunk_size)
                        ]
                        prewarm_ok = 0
                        prewarm_fail = 0
                        for idx, chunk in enumerate(prewarm_chunks, 1):
                            if is_task_stopped(task_id):
                                add_task_log(task_id, "🛑 预热阶段停止")
                                break
                            try:
                                res = market_sync.sync_daily_incremental(
                                    history_days=history_days,
                                    symbols=chunk,
                                    include_index=(idx == 1),
                                    finalize_today=False,
                                )
                                if res.get("success"):
                                    prewarm_ok += 1
                                else:
                                    prewarm_fail += 1
                            except Exception as e:
                                prewarm_fail += 1
                                log_warning(f"全局预热分片异常 chunk={idx}/{len(prewarm_chunks)}: {e}")
                        prewarm_elapsed = time.perf_counter() - prewarm_started
                        add_task_log(
                            task_id,
                            f"🧰 全局预热完成 (逐股模式): chunks={len(prewarm_chunks)}, ok={prewarm_ok}, "
                            f"fail={prewarm_fail}, elapsed={prewarm_elapsed:.1f}s",
                        )
                except Exception as e:
                    prewarm_elapsed = time.perf_counter() - prewarm_started
                    add_task_log(task_id, f"⚠️ 全局预热异常: {e} (elapsed={prewarm_elapsed:.1f}s)")

            # ── Phase 2: 逐群收益计算（跳过群内预热） ──
            add_task_log(task_id, "")
            add_task_log(task_id, "═" * 40)
            add_task_log(task_id, "📈 Phase 2: 逐群收益计算")

            for i, group in enumerate(groups, 1):
                if is_task_stopped(task_id):
                    add_task_log(task_id, "🛑 任务已被用户停止")
                    break

                group_id = str(group["group_id"])
                add_task_log(task_id, "")
                add_task_log(task_id, f"👉 [{i}/{len(groups)}] 正在计算群 {group_id} 的收益...")

                try:
                    analyzer = group_analyzers.get(group_id)
                    if analyzer is None:
                        analyzer = StockAnalyzer(
                            group_id,
                            stop_check=lambda: is_task_stopped(task_id),
                        )

                    backlog = analyzer._get_analysis_backlog_stats(calc_window_days=calc_window_days)
                    add_task_log(
                        task_id,
                        f"   🧩 预检查: mentions={backlog.get('mentions_total', 0)}, pending={backlog.get('pending_total', 0)}",
                    )

                    last_log_time = 0.0
                    last_log_percent = -1
                    progress_log_interval = max(
                        1.0, float(os.environ.get("PERF_PROGRESS_LOG_INTERVAL_SECONDS", "15"))
                    )

                    def progress_cb(current: int, total: int, status: str):
                        nonlocal last_log_time, last_log_percent
                        now = time.time()
                        percent = int((current * 100) / total) if total > 0 else 100
                        if (
                            current in {1, total}
                            or percent >= (last_log_percent + 1)
                            or (now - last_log_time) >= progress_log_interval
                        ):
                            add_task_log(task_id, f"   ⏳ 进度: {current}/{total} - {status}")
                            last_log_time = now
                            last_log_percent = percent

                    # 已全局预热，跳过群内预热
                    original_prewarm = analyzer.PERF_PREWARM_ENABLED
                    analyzer.PERF_PREWARM_ENABLED = False
                    try:
                        res = analyzer.calc_pending_performance(
                            calc_window_days=calc_window_days,
                            progress_callback=progress_cb,
                        )
                    finally:
                        analyzer.PERF_PREWARM_ENABLED = original_prewarm

                    if bool(res.get("aborted")) or is_task_stopped(task_id):
                        add_task_log(task_id, f"   🛑 群组 {group_id} 收益计算已停止")
                        break
                    processed_count = int(res.get("processed", 0) or 0)
                    skipped_count = int(res.get("skipped", 0) or 0)
                    error_count = int(res.get("errors", 0) or 0)
                    performance_processed_total += processed_count
                    add_task_log(
                        task_id,
                        f"   ✅ 群组 {group_id} 收益计算完成! processed={processed_count}, skipped={skipped_count}, errors={error_count}",
                    )
                    processed_groups += 1
                except Exception as ge:
                    add_task_log(task_id, f"   ❌ 群组 {group_id} 计算异常: {ge}")

            if is_task_stopped(task_id):
                update_task(task_id, "cancelled", "全区计算已停止")
            else:
                add_task_log(task_id, "")
                add_task_log(task_id, "=" * 50)
                add_task_log(task_id, f"🎉 全区收益计算完成！共处理 {processed_groups}/{len(groups)} 个群组")
                add_task_log(
                    task_id,
                    f"📊 自动提取群组: {groups_with_auto_extract}, 自动提取提及: {mentions_extracted_total}, 收益处理条数: {performance_processed_total}",
                )

                try:
                    get_global_analyzer().invalidate_cache()
                    add_task_log(task_id, "🔄 全局统计缓存已刷新")
                except Exception:
                    pass

                update_task(
                    task_id,
                    "completed",
                    f"全区收益计算完成: {processed_groups} 个群组",
                    {
                        "groups_processed": processed_groups,
                        "groups_total": len(groups),
                        "groups_with_auto_extract": groups_with_auto_extract,
                        "mentions_extracted_total": mentions_extracted_total,
                        "performance_processed_total": performance_processed_total,
                    },
                )

        except Exception as e:
            add_task_log(task_id, f"❌ 全区计算异常: {e}")
            update_task(task_id, "failed", f"全区计算失败: {e}")


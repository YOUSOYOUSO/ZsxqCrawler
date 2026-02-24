import time
from typing import Any, Callable, Dict, List

from modules.shared.db_path_manager import get_db_path_manager
from modules.analyzers.global_analyzer import get_global_analyzer
from modules.analyzers.stock_analyzer import StockAnalyzer
from api.services.group_filter_service import apply_group_scan_filter, format_group_filter_summary


class GlobalAnalyzePerformanceService:
    """全区收益计算服务（从 main.py 拆出业务流程）。"""

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

            for i, group in enumerate(groups, 1):
                if is_task_stopped(task_id):
                    add_task_log(task_id, "🛑 任务已被用户停止")
                    break

                group_id = str(group["group_id"])
                add_task_log(task_id, "")
                add_task_log(task_id, f"👉 [{i}/{len(groups)}] 正在计算群 {group_id} 的收益...")

                try:
                    analyzer = StockAnalyzer(group_id)
                    backlog = analyzer._get_analysis_backlog_stats(calc_window_days=calc_window_days)
                    add_task_log(
                        task_id,
                        f"   🧩 预检查: mentions={backlog.get('mentions_total', 0)}, pending={backlog.get('pending_total', 0)}",
                    )

                    # 每次收益计算前都先做一次增量提取，避免“已有待算任务时跳过提取”导致新话题漏算
                    extract_res = analyzer.extract_only()
                    extracted_mentions = int(extract_res.get("mentions_extracted", 0) or 0)
                    new_topics = int(extract_res.get("new_topics", 0) or 0)
                    if new_topics > 0 or extracted_mentions > 0:
                        groups_with_auto_extract += 1
                    mentions_extracted_total += extracted_mentions
                    add_task_log(
                        task_id,
                        f"   📝 自动提取: new_topics={new_topics}, mentions={extracted_mentions}, unique_stocks={extract_res.get('unique_stocks', 0)}",
                    )

                    last_log_time = 0.0

                    def progress_cb(current: int, total: int, status: str):
                        nonlocal last_log_time
                        now = time.time()
                        # 避免日志过多，只在任务启动或一定时间后打印
                        if now - last_log_time >= 5 or current == total or current == 1:
                            add_task_log(task_id, f"   ⏳ 进度: {current}/{total} - {status}")
                            last_log_time = now

                    res = analyzer.calc_pending_performance(
                        calc_window_days=calc_window_days,
                        progress_callback=progress_cb,
                    )
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

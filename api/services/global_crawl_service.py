import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List

from db_path_manager import get_db_path_manager
from zsxq_interactive_crawler import ZSXQInteractiveCrawler


class GlobalCrawlService:
    """全区话题采集服务（从 main.py 拆出业务流程）。"""

    @staticmethod
    def _apply_group_scan_filter_for_tasks(groups: List[Dict[str, Any]]) -> Dict[str, Any]:
        """统一应用白黑名单过滤，供全区任务复用。"""
        from group_scan_filter import filter_groups

        filtered = filter_groups(groups)
        cfg = filtered.get("config", {}) or {}
        return {
            "all_groups": groups,
            "included_groups": filtered.get("included_groups", []) or [],
            "excluded_groups": filtered.get("excluded_groups", []) or [],
            "reason_counts": filtered.get("reason_counts", {}) or {},
            "default_action": str(cfg.get("default_action", "include")),
        }

    @staticmethod
    def _log_group_filter_summary(
        task_id: str,
        add_task_log: Callable[[str, str], None],
        all_groups: List[Dict[str, Any]],
        groups: List[Dict[str, Any]],
        excluded_groups: List[Dict[str, Any]],
        reason_counts: Dict[str, Any],
        default_action: str,
    ) -> None:
        add_task_log(task_id, f"📋 共发现 {len(all_groups)} 个群组")
        add_task_log(task_id, f"⚙️ 过滤策略: 未配置群组默认{'纳入' if default_action == 'include' else '排除'}")
        add_task_log(task_id, f"🧹 过滤后纳入 {len(groups)}/{len(all_groups)} 个群组")
        if reason_counts:
            add_task_log(task_id, f"📌 命中统计: {reason_counts}")
        if excluded_groups:
            preview = "，".join(
                f"{g.get('group_id')}({g.get('scan_filter_reason', 'unknown')})"
                for g in excluded_groups[:20]
            )
            suffix = " ..." if len(excluded_groups) > 20 else ""
            add_task_log(task_id, f"🚫 已排除: {preview}{suffix}")

    def run(
        self,
        task_id: str,
        request: Any,
        add_task_log: Callable[[str, str], None],
        update_task: Callable[..., Any],
        is_task_stopped: Callable[[str], bool],
        get_cookie_for_group: Callable[[str], str],
    ) -> None:
        """执行全区话题采集主流程。"""
        try:
            update_task(task_id, "running", "准备开始全区采集...")
            add_task_log(task_id, f"🚀 开始全区话题采集 [模式: {request.mode}]")

            manager = get_db_path_manager()
            all_groups = manager.list_all_groups()
            filtered = self._apply_group_scan_filter_for_tasks(all_groups)
            groups = filtered["included_groups"]
            excluded_groups = filtered["excluded_groups"]
            reason_counts = filtered["reason_counts"]
            default_action = filtered["default_action"]
            self._log_group_filter_summary(
                task_id,
                add_task_log,
                all_groups,
                groups,
                excluded_groups,
                reason_counts,
                default_action,
            )

            if not groups:
                update_task(task_id, "completed", "全区采集完成: 过滤后无可扫描群组")
                return

            processed_groups = 0
            for i, group in enumerate(groups, 1):
                if is_task_stopped(task_id):
                    add_task_log(task_id, "🛑 任务已被用户停止")
                    break

                group_id = str(group["group_id"])
                add_task_log(task_id, "")
                add_task_log(task_id, f"👉 [{i}/{len(groups)}] 正在采集群组 {group_id}...")

                try:
                    cookie = get_cookie_for_group(group_id)
                    db_path = manager.get_topics_db_path(group_id)

                    def log_callback(msg: str):
                        add_task_log(task_id, f"   {msg}")

                    crawler = ZSXQInteractiveCrawler(cookie, group_id, db_path, log_callback)
                    crawler.stop_check_func = lambda: is_task_stopped(task_id)

                    crawler.set_custom_intervals(
                        crawl_interval_min=request.crawl_interval_min,
                        crawl_interval_max=request.crawl_interval_max,
                        long_sleep_interval_min=request.long_sleep_interval_min,
                        long_sleep_interval_max=request.long_sleep_interval_max,
                        pages_per_batch=request.pages_per_batch,
                    )

                    if request.mode == "latest":
                        res = crawler.crawl_latest_until_complete()
                    elif request.mode == "incremental":
                        res = crawler.crawl_incremental(pages=request.pages or 100, per_page=request.per_page or 20)
                    elif request.mode == "range":
                        if request.last_days is not None:
                            add_task_log(task_id, f"🧭 range 条件: 最近天数 = {request.last_days}")
                        else:
                            add_task_log(
                                task_id,
                                f"🧭 range 条件: 开始={request.start_time or '自动推导'}，结束={request.end_time or '当前时间'}",
                            )
                        range_start_time = request.start_time
                        range_end_time = request.end_time
                        if request.last_days and not range_start_time:
                            now_bj = datetime.now(timezone(timedelta(hours=8)))
                            range_start_time = (now_bj - timedelta(days=max(1, request.last_days))).isoformat()
                        safe_max_items = max(1, int(request.max_items or 500))
                        res = crawler.crawl_time_range(
                            start_time=range_start_time,
                            end_time=range_end_time,
                            max_items=safe_max_items,
                            per_page=request.per_page or 20,
                        )
                    elif request.mode == "all":
                        res = crawler.crawl_all_historical(per_page=request.per_page or 20, auto_confirm=True)
                    else:
                        add_task_log(task_id, f"   ⚠️ 未知的采集模式: {request.mode}")
                        continue

                    if not isinstance(res, dict):
                        res = {"new_topics": 0, "updated_topics": 0, "errors": 1}

                    if res.get("expired"):
                        add_task_log(task_id, f"   ❌ 群组 {group_id} 会员已过期: {res.get('message')}")
                    elif res.get("stopped"):
                        add_task_log(task_id, f"   🛑 群组 {group_id} 采集已停止")
                    else:
                        add_task_log(
                            task_id,
                            f"   ✅ 群组 {group_id} 采集完成! 新增: {res.get('new_topics', 0)}, 更新: {res.get('updated_topics', 0)}",
                        )
                        processed_groups += 1
                except Exception as ge:
                    add_task_log(task_id, f"   ❌ 群组 {group_id} 采集异常: {ge}")

                if i < len(groups) and not is_task_stopped(task_id):
                    sleep_time = random.uniform(2.0, 5.0)
                    add_task_log(task_id, f"⏳ 等待 {sleep_time:.1f} 秒后采集下一个群组...")
                    time.sleep(sleep_time)

            if is_task_stopped(task_id):
                update_task(task_id, "cancelled", "全区采集已停止")
            else:
                add_task_log(task_id, "")
                add_task_log(task_id, "=" * 50)
                add_task_log(task_id, f"🎉 全区采集完成！共处理 {processed_groups}/{len(groups)} 个群组")
                update_task(task_id, "completed", f"全区采集完成: {processed_groups} 个群组")
        except Exception as e:
            add_task_log(task_id, f"❌ 全区采集异常: {e}")
            update_task(task_id, "failed", f"全区采集失败: {e}")


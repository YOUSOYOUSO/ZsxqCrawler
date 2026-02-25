from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from fastapi import BackgroundTasks

from api.schemas.models import CrawlBehaviorSettingsRequest, CrawlHistoricalRequest, CrawlSettingsRequest, CrawlTimeRangeRequest
from api.services.task_facade import TaskFacade
from modules.accounts.accounts_sql_manager import get_accounts_sql_manager
from modules.shared.db_path_manager import get_db_path_manager
from modules.zsxq.zsxq_interactive_crawler import ZSXQInteractiveCrawler, load_config


class CrawlService:
    CRAWL_SETTINGS_DEFAULTS = {
        "crawl_interval_min": 2.0,
        "crawl_interval_max": 5.0,
        "long_sleep_interval_min": 180.0,
        "long_sleep_interval_max": 300.0,
        "pages_per_batch": 15,
    }

    def __init__(self):
        self.tasks = TaskFacade()
        self.app_settings_path = os.path.join(get_db_path_manager().base_dir, "app_settings.json")

    def _resolve_cookie_for_group(self, group_id: str) -> str:
        manager = get_accounts_sql_manager()
        account = manager.get_account_for_group(group_id, mask_cookie=False)
        if account and account.get("cookie"):
            return str(account["cookie"]).strip()

        first = manager.get_first_account(mask_cookie=False)
        if first and first.get("cookie"):
            return str(first["cookie"]).strip()

        cfg = load_config() or {}
        return str((cfg.get("auth", {}) or {}).get("cookie", "")).strip()

    def _load_app_settings(self) -> Dict[str, Any]:
        try:
            if not os.path.exists(self.app_settings_path):
                return {}
            with open(self.app_settings_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _save_app_settings(self, settings: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(self.app_settings_path), exist_ok=True)
        with open(self.app_settings_path, "w", encoding="utf-8") as f:
            json.dump(settings, f, ensure_ascii=False, indent=2)

    def get_crawl_settings(self) -> Dict[str, Any]:
        settings = self._load_app_settings()
        crawl_settings = settings.get("crawl", {}) if isinstance(settings, dict) else {}
        merged = dict(self.CRAWL_SETTINGS_DEFAULTS)
        if isinstance(crawl_settings, dict):
            merged.update({k: v for k, v in crawl_settings.items() if k in self.CRAWL_SETTINGS_DEFAULTS})
        return merged

    def update_crawl_settings(self, settings: CrawlBehaviorSettingsRequest) -> Dict[str, Any]:
        if settings.crawl_interval_min >= settings.crawl_interval_max:
            raise ValueError("crawl_interval_min must be less than crawl_interval_max")
        if settings.long_sleep_interval_min >= settings.long_sleep_interval_max:
            raise ValueError("long_sleep_interval_min must be less than long_sleep_interval_max")

        all_settings = self._load_app_settings()
        if not isinstance(all_settings, dict):
            all_settings = {}
        all_settings["crawl"] = settings.model_dump()
        self._save_app_settings(all_settings)
        return all_settings["crawl"]

    def _resolve_crawl_interval_values(self, request_obj: Optional[Any]) -> Dict[str, Any]:
        persisted = self.get_crawl_settings()
        return {
            "crawl_interval_min": getattr(request_obj, "crawlIntervalMin", None) or persisted["crawl_interval_min"],
            "crawl_interval_max": getattr(request_obj, "crawlIntervalMax", None) or persisted["crawl_interval_max"],
            "long_sleep_interval_min": getattr(request_obj, "longSleepIntervalMin", None) or persisted["long_sleep_interval_min"],
            "long_sleep_interval_max": getattr(request_obj, "longSleepIntervalMax", None) or persisted["long_sleep_interval_max"],
            "pages_per_batch": getattr(request_obj, "pagesPerBatch", None) or persisted["pages_per_batch"],
        }

    def _run_post_crawl_stock_analysis(self, task_id: str, group_id: str) -> Dict[str, Any]:
        from modules.analyzers.stock_analyzer import StockAnalyzer

        analyzer = StockAnalyzer(group_id)
        extract_res = analyzer.extract_only()
        calc_res = analyzer.calc_pending_performance()
        self.tasks.append_log(
            task_id,
            "📈 股票分析补跑完成: "
            f"new_topics={extract_res.get('new_topics', 0)}, "
            f"mentions={extract_res.get('mentions_extracted', 0)}, "
            f"perf_processed={calc_res.get('processed', 0)}, "
            f"perf_errors={calc_res.get('errors', 0)}",
        )
        return {"extract": extract_res, "performance": calc_res}

    def _build_crawler(self, group_id: str, log_callback, stop_check, request_obj: Optional[Any] = None) -> ZSXQInteractiveCrawler:
        cookie = self._resolve_cookie_for_group(group_id)
        db_path = get_db_path_manager().get_topics_db_path(group_id)
        crawler = ZSXQInteractiveCrawler(cookie, group_id, db_path, log_callback)
        crawler.stop_check_func = stop_check
        crawler.set_custom_intervals(**self._resolve_crawl_interval_values(request_obj))
        return crawler

    def start_crawl_historical(self, group_id: str, request: CrawlHistoricalRequest, background_tasks: BackgroundTasks) -> Dict[str, Any]:
        task_id = self.tasks.create_task("crawl_historical", f"爬取历史数据 {request.pages} 页 (群组: {group_id})")
        background_tasks.add_task(self._run_crawl_historical_task, task_id, group_id, request.pages, request.per_page, request)
        return {"task_id": task_id, "message": "任务已创建，正在后台执行"}

    def _run_crawl_historical_task(self, task_id: str, group_id: str, pages: int, per_page: int, crawl_settings: CrawlHistoricalRequest | None):
        try:
            if self.tasks.is_task_stopped(task_id):
                return
            self.tasks.update_task(task_id, "running", f"开始爬取历史数据 {pages} 页...")
            self.tasks.append_log(task_id, f"🚀 开始获取历史数据，{pages} 页，每页 {per_page} 条")

            def log_callback(message: str):
                self.tasks.append_log(task_id, message)

            crawler = self._build_crawler(group_id, log_callback, lambda: self.tasks.is_task_stopped(task_id), crawl_settings)
            if self.tasks.is_task_stopped(task_id):
                self.tasks.append_log(task_id, "🛑 任务在初始化过程中被停止")
                return

            self.tasks.append_log(task_id, "📡 连接到知识星球API...")
            self.tasks.append_log(task_id, "🔍 检查数据库状态...")
            result = crawler.crawl_incremental(pages, per_page)

            if self.tasks.is_task_stopped(task_id):
                return
            if result and result.get("expired"):
                self.tasks.append_log(task_id, f"❌ 会员已过期: {result.get('message', '成员体验已到期')}")
                self.tasks.update_task(task_id, "failed", "会员已过期", {"expired": True, "code": result.get("code"), "message": result.get("message")})
                return

            if (result.get("new_topics", 0) or 0) > 0 or (result.get("updated_topics", 0) or 0) > 0:
                if self.tasks.is_task_stopped(task_id):
                    return
                self.tasks.append_log(task_id, "🧠 检测到新数据，开始自动执行股票提取与收益刷新...")
                try:
                    result["stock_analysis"] = self._run_post_crawl_stock_analysis(task_id, group_id)
                except Exception as analysis_err:
                    self.tasks.append_log(task_id, f"⚠️ 自动股票分析失败（爬取结果已保留）: {analysis_err}")
            else:
                self.tasks.append_log(task_id, "ℹ️ 本次无新增/更新话题，跳过自动股票分析")

            self.tasks.append_log(task_id, f"✅ 获取完成！新增话题: {result.get('new_topics', 0)}, 更新话题: {result.get('updated_topics', 0)}")
            self.tasks.update_task(task_id, "completed", "历史数据爬取完成", result)
        except Exception as e:
            if not self.tasks.is_task_stopped(task_id):
                self.tasks.append_log(task_id, f"❌ 获取失败: {e}")
                self.tasks.update_task(task_id, "failed", f"爬取失败: {e}")

    def start_crawl_all(self, group_id: str, request: CrawlSettingsRequest, background_tasks: BackgroundTasks) -> Dict[str, Any]:
        task_id = self.tasks.create_task("crawl_all", f"全量爬取所有历史数据 (群组: {group_id})")
        background_tasks.add_task(self._run_crawl_all_task, task_id, group_id, request)
        return {"task_id": task_id, "message": "任务已创建，正在后台执行"}

    def _run_crawl_all_task(self, task_id: str, group_id: str, crawl_settings: CrawlSettingsRequest | None):
        try:
            self.tasks.update_task(task_id, "running", "开始全量爬取...")
            self.tasks.append_log(task_id, "🚀 开始全量爬取...")
            self.tasks.append_log(task_id, "⚠️ 警告：此模式将持续爬取直到没有数据，可能需要很长时间")

            def log_callback(message: str):
                self.tasks.append_log(task_id, message)

            crawler = self._build_crawler(group_id, log_callback, lambda: self.tasks.is_task_stopped(task_id), crawl_settings)
            if self.tasks.is_task_stopped(task_id):
                self.tasks.append_log(task_id, "🛑 任务在初始化过程中被停止")
                return

            self.tasks.append_log(task_id, "📡 连接到知识星球API...")
            self.tasks.append_log(task_id, "🔍 检查数据库状态...")
            db_stats = crawler.db.get_database_stats()
            self.tasks.append_log(task_id, f"📊 当前数据库状态: 话题: {db_stats.get('topics', 0)}, 用户: {db_stats.get('users', 0)}")
            result = crawler.crawl_all_historical(per_page=20, auto_confirm=True)

            if self.tasks.is_task_stopped(task_id):
                return
            if result and result.get("expired"):
                self.tasks.append_log(task_id, f"❌ 会员已过期: {result.get('message', '成员体验已到期')}")
                self.tasks.update_task(task_id, "failed", "会员已过期", {"expired": True, "code": result.get("code"), "message": result.get("message")})
                return

            if (result.get("new_topics", 0) or 0) > 0 or (result.get("updated_topics", 0) or 0) > 0:
                if self.tasks.is_task_stopped(task_id):
                    return
                self.tasks.append_log(task_id, "🧠 检测到新数据，开始自动执行股票提取与收益刷新...")
                try:
                    result["stock_analysis"] = self._run_post_crawl_stock_analysis(task_id, group_id)
                except Exception as analysis_err:
                    self.tasks.append_log(task_id, f"⚠️ 自动股票分析失败（爬取结果已保留）: {analysis_err}")
            else:
                self.tasks.append_log(task_id, "ℹ️ 本次无新增/更新话题，跳过自动股票分析")

            self.tasks.append_log(task_id, "🎉 全量爬取完成！")
            self.tasks.append_log(task_id, f"📊 最终统计: 新增话题: {result.get('new_topics', 0)}, 更新话题: {result.get('updated_topics', 0)}, 总页数: {result.get('pages', 0)}")
            self.tasks.update_task(task_id, "completed", "全量爬取完成", result)
        except Exception as e:
            self.tasks.append_log(task_id, f"❌ 全量爬取失败: {e}")
            self.tasks.update_task(task_id, "failed", f"全量爬取失败: {e}")

    def start_crawl_incremental(self, group_id: str, request: CrawlHistoricalRequest, background_tasks: BackgroundTasks) -> Dict[str, Any]:
        task_id = self.tasks.create_task("crawl_incremental", f"增量爬取历史数据 {request.pages} 页 (群组: {group_id})")
        background_tasks.add_task(self._run_crawl_incremental_task, task_id, group_id, request.pages, request.per_page, request)
        return {"task_id": task_id, "message": "任务已创建，正在后台执行"}

    def _run_crawl_incremental_task(self, task_id: str, group_id: str, pages: int, per_page: int, crawl_settings: CrawlHistoricalRequest | None):
        try:
            self.tasks.update_task(task_id, "running", "开始增量爬取...")

            def log_callback(message: str):
                self.tasks.append_log(task_id, message)

            crawler = self._build_crawler(group_id, log_callback, lambda: self.tasks.is_task_stopped(task_id), crawl_settings)
            if self.tasks.is_task_stopped(task_id):
                self.tasks.append_log(task_id, "🛑 任务在初始化过程中被停止")
                return

            self.tasks.append_log(task_id, "📡 连接到知识星球API...")
            self.tasks.append_log(task_id, "🔍 检查数据库状态...")
            result = crawler.crawl_incremental(pages, per_page)

            if self.tasks.is_task_stopped(task_id):
                return
            if (result.get("new_topics", 0) or 0) > 0 or (result.get("updated_topics", 0) or 0) > 0:
                if self.tasks.is_task_stopped(task_id):
                    return
                self.tasks.append_log(task_id, "🧠 检测到新数据，开始自动执行股票提取与收益刷新...")
                try:
                    result["stock_analysis"] = self._run_post_crawl_stock_analysis(task_id, group_id)
                except Exception as analysis_err:
                    self.tasks.append_log(task_id, f"⚠️ 自动股票分析失败（爬取结果已保留）: {analysis_err}")
            else:
                self.tasks.append_log(task_id, "ℹ️ 本次无新增/更新话题，跳过自动股票分析")

            self.tasks.append_log(task_id, f"✅ 增量爬取完成！新增话题: {result.get('new_topics', 0)}, 更新话题: {result.get('updated_topics', 0)}")
            self.tasks.update_task(task_id, "completed", "增量爬取完成", result)
        except Exception as e:
            if not self.tasks.is_task_stopped(task_id):
                self.tasks.append_log(task_id, f"❌ 增量爬取失败: {e}")
                self.tasks.update_task(task_id, "failed", f"增量爬取失败: {e}")

    def start_crawl_latest_until_complete(self, group_id: str, request: CrawlSettingsRequest, background_tasks: BackgroundTasks) -> Dict[str, Any]:
        task_id = self.tasks.create_task("crawl_latest_until_complete", f"获取最新记录 (群组: {group_id})")
        background_tasks.add_task(self._run_crawl_latest_task, task_id, group_id, request)
        return {"task_id": task_id, "message": "任务已创建，正在后台执行"}

    def _run_crawl_latest_task(self, task_id: str, group_id: str, crawl_settings: CrawlSettingsRequest | None):
        try:
            self.tasks.update_task(task_id, "running", "开始获取最新记录...")

            def log_callback(message: str):
                self.tasks.append_log(task_id, message)

            crawler = self._build_crawler(group_id, log_callback, lambda: self.tasks.is_task_stopped(task_id), crawl_settings)
            if self.tasks.is_task_stopped(task_id):
                self.tasks.append_log(task_id, "🛑 任务在初始化过程中被停止")
                return

            self.tasks.append_log(task_id, "📡 连接到知识星球API...")
            self.tasks.append_log(task_id, "🔍 检查数据库状态...")
            result = crawler.crawl_latest_until_complete()

            if self.tasks.is_task_stopped(task_id):
                return
            if result and result.get("expired"):
                self.tasks.append_log(task_id, f"❌ 会员已过期: {result.get('message', '成员体验已到期')}")
                self.tasks.update_task(task_id, "failed", "会员已过期", {"expired": True, "code": result.get("code"), "message": result.get("message")})
                return

            if (result.get("new_topics", 0) or 0) > 0 or (result.get("updated_topics", 0) or 0) > 0:
                if self.tasks.is_task_stopped(task_id):
                    return
                self.tasks.append_log(task_id, "🧠 检测到新数据，开始自动执行股票提取与收益刷新...")
                try:
                    result["stock_analysis"] = self._run_post_crawl_stock_analysis(task_id, group_id)
                except Exception as analysis_err:
                    self.tasks.append_log(task_id, f"⚠️ 自动股票分析失败（爬取结果已保留）: {analysis_err}")
            else:
                self.tasks.append_log(task_id, "ℹ️ 本次无新增/更新话题，跳过自动股票分析")

            self.tasks.append_log(task_id, f"✅ 获取最新记录完成！新增话题: {result.get('new_topics', 0)}, 更新话题: {result.get('updated_topics', 0)}")
            self.tasks.update_task(task_id, "completed", "获取最新记录完成", result)
        except Exception as e:
            if not self.tasks.is_task_stopped(task_id):
                self.tasks.append_log(task_id, f"❌ 获取最新记录失败: {e}")
                self.tasks.update_task(task_id, "failed", f"获取最新记录失败: {e}")

    def start_crawl_time_range(self, group_id: str, request: CrawlTimeRangeRequest, background_tasks: BackgroundTasks) -> Dict[str, Any]:
        task_id = self.tasks.create_task("crawl_time_range", f"按时间区间爬取 (群组: {group_id})")
        background_tasks.add_task(self._run_crawl_time_range_task, task_id, group_id, request)
        return {"task_id": task_id, "message": "任务已创建，正在后台执行"}

    def _run_crawl_time_range_task(self, task_id: str, group_id: str, request: CrawlTimeRangeRequest):
        try:
            def parse_user_time(s: Optional[str]) -> Optional[datetime]:
                if not s:
                    return None
                t = s.strip()
                try:
                    if len(t) == 10 and t[4] == "-" and t[7] == "-":
                        dt = datetime.strptime(t, "%Y-%m-%d")
                        return dt.replace(tzinfo=timezone(timedelta(hours=8)))
                    if "T" in t and len(t) == 16:
                        t = t + ":00"
                    if t.endswith("Z"):
                        t = t.replace("Z", "+00:00")
                    if len(t) >= 24 and (t[-5] in ["+", "-"]) and t[-3] != ":":
                        t = t[:-2] + ":" + t[-2:]
                    dt = datetime.fromisoformat(t)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
                    return dt
                except Exception:
                    return None

            bj_tz = timezone(timedelta(hours=8))
            now_bj = datetime.now(bj_tz)
            start_dt = parse_user_time(request.startTime)
            end_dt = parse_user_time(request.endTime) if request.endTime else None
            if request.lastDays and request.lastDays > 0:
                if end_dt is None:
                    end_dt = now_bj
                start_dt = end_dt - timedelta(days=request.lastDays)
            if end_dt is None:
                end_dt = now_bj
            if start_dt is None:
                start_dt = end_dt - timedelta(days=30)
            if start_dt > end_dt:
                start_dt, end_dt = end_dt, start_dt

            self.tasks.update_task(task_id, "running", "开始按时间区间爬取...")
            self.tasks.append_log(task_id, f"🗓️ 时间范围: {start_dt.isoformat()} ~ {end_dt.isoformat()}")

            def log_callback(message: str):
                self.tasks.append_log(task_id, message)

            crawler = self._build_crawler(group_id, log_callback, lambda: self.tasks.is_task_stopped(task_id), request)
            per_page = request.perPage or 20
            total_stats = crawler.crawl_time_range(
                start_time=start_dt.isoformat(),
                end_time=end_dt.isoformat(),
                max_items=500,
                per_page=per_page,
            ) or {}

            if total_stats.get("expired"):
                msg = str(total_stats.get("message") or "会员已过期")
                self.tasks.update_task(task_id, "failed", msg, total_stats)
                return
            if total_stats.get("stopped") or self.tasks.is_task_stopped(task_id):
                self.tasks.update_task(task_id, "cancelled", "时间区间爬取已停止", total_stats)
                return
            self.tasks.update_task(task_id, "completed", "时间区间爬取完成", total_stats)
        except Exception as e:
            if not self.tasks.is_task_stopped(task_id):
                self.tasks.append_log(task_id, f"❌ 时间区间爬取失败: {e}")
                self.tasks.update_task(task_id, "failed", f"时间区间爬取失败: {e}")

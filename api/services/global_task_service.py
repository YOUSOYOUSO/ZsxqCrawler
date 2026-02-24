from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import BackgroundTasks, HTTPException

from api.deps.container import get_task_runtime
from api.services.global_analyze_service import GlobalAnalyzePerformanceService
from api.services.global_crawl_service import GlobalCrawlService
from api.services.global_file_task_service import GlobalFileTaskService
from api.services.group_filter_service import apply_group_scan_filter, format_group_filter_summary
from api.services.task_runtime import TaskRuntime
from modules.accounts.accounts_sql_manager import get_accounts_sql_manager
from modules.analyzers.global_analyzer import get_global_analyzer
from modules.analyzers.global_pipeline import run_serial_incremental_pipeline
from modules.shared.db_path_manager import get_db_path_manager
from modules.shared.group_scan_filter import get_filter_config
from modules.shared.stock_exclusion import build_sql_exclusion_clause
from modules.zsxq.zsxq_interactive_crawler import load_config


class GlobalTaskService:
    """Global task orchestration service."""

    def __init__(self, runtime: TaskRuntime | None = None):
        self.runtime = runtime or get_task_runtime()

    def start(self, fn, background_tasks: BackgroundTasks, *args, **kwargs):
        return fn(*args, background_tasks=background_tasks, **kwargs)

    def start_global_crawl(self, request: Any, background_tasks: BackgroundTasks):
        self._validate_global_crawl_request(request)
        task_id = self._create_running_task(
            prefix="global_crawl",
            task_type="global_crawl",
            message="正在初始化全区话题采集...",
        )
        background_tasks.add_task(self._run_global_crawl, task_id, request)
        return {"task_id": task_id, "message": "全区采集任务已启动"}

    def start_global_files_collect(self, request: Any, background_tasks: BackgroundTasks):
        task_id = self._create_running_task(
            prefix="global_files_collect",
            task_type="global_files_collect",
            message="正在初始化全区文件列表收集...",
        )
        background_tasks.add_task(self._run_global_files_collect, task_id, request)
        return {"task_id": task_id, "message": "全区收集任务已启动"}

    def start_global_files_download(self, request: Any, background_tasks: BackgroundTasks):
        task_id = self._create_running_task(
            prefix="global_files_download",
            task_type="global_files_download",
            message="正在初始化全区文件下载...",
        )
        background_tasks.add_task(self._run_global_files_download, task_id, request)
        return {"task_id": task_id, "message": "全区下载任务已启动"}

    def start_global_analyze_performance(self, background_tasks: BackgroundTasks, force: bool = False):
        task_id = self._create_running_task(
            prefix="global_analyze_performance",
            task_type="global_analyze",
            message="正在初始化全区收益计算...",
        )
        background_tasks.add_task(self._run_global_analyze_performance, task_id, force)
        return {"task_id": task_id, "message": "全区计算任务已启动"}

    def cleanup_excluded_stocks(self, scope: str = "all", group_id: Optional[str] = None):
        if scope not in ("all", "group"):
            raise HTTPException(status_code=400, detail="scope 仅支持 all 或 group")
        if scope == "group" and not group_id:
            raise HTTPException(status_code=400, detail="scope=group 时必须提供 group_id")

        manager = get_db_path_manager()
        groups = manager.list_all_groups()
        if scope == "group":
            groups = [g for g in groups if str(g.get("group_id")) == str(group_id)]

        exclude_clause, exclude_params = build_sql_exclusion_clause("stock_code", "stock_name")
        if not exclude_clause:
            return {
                "groups_processed": 0,
                "mentions_deleted": 0,
                "performances_deleted": 0,
                "details": [],
                "message": "未配置排除规则，无需清理",
            }

        total_mentions_deleted = 0
        total_perf_deleted = 0
        details = []

        for group in groups:
            gid = str(group.get("group_id"))
            db_path = group.get("topics_db")
            if not db_path or not os.path.exists(db_path):
                continue

            mentions_deleted = 0
            perf_deleted = 0
            conn = None
            try:
                conn = sqlite3.connect(db_path, timeout=30)
                cursor = conn.cursor()

                cursor.execute(
                    """
                    SELECT 1 FROM sqlite_master
                    WHERE type = 'table' AND name = 'stock_mentions'
                    """
                )
                if cursor.fetchone() is None:
                    continue

                cursor.execute(
                    f"SELECT id FROM stock_mentions WHERE NOT (1=1 {exclude_clause})",
                    exclude_params,
                )
                mention_ids = [row[0] for row in cursor.fetchall()]

                if mention_ids:
                    placeholders = ",".join(["?"] * len(mention_ids))
                    cursor.execute(
                        """
                        SELECT 1 FROM sqlite_master
                        WHERE type = 'table' AND name = 'mention_performance'
                        """
                    )
                    if cursor.fetchone() is not None:
                        cursor.execute(
                            f"DELETE FROM mention_performance WHERE mention_id IN ({placeholders})",
                            mention_ids,
                        )
                        perf_deleted = cursor.rowcount or 0

                    cursor.execute(
                        f"DELETE FROM stock_mentions WHERE id IN ({placeholders})",
                        mention_ids,
                    )
                    mentions_deleted = cursor.rowcount or 0

                conn.commit()
            except Exception as e:
                if conn:
                    conn.rollback()
                details.append(
                    {
                        "group_id": gid,
                        "mentions_deleted": 0,
                        "performances_deleted": 0,
                        "error": str(e),
                    }
                )
                continue
            finally:
                if conn:
                    conn.close()

            total_mentions_deleted += mentions_deleted
            total_perf_deleted += perf_deleted
            details.append(
                {
                    "group_id": gid,
                    "mentions_deleted": mentions_deleted,
                    "performances_deleted": perf_deleted,
                }
            )

        try:
            get_global_analyzer().invalidate_cache()
        except Exception:
            pass

        return {
            "groups_processed": len(details),
            "mentions_deleted": total_mentions_deleted,
            "performances_deleted": total_perf_deleted,
            "details": details,
        }

    def start_cleanup_blacklist(self, background_tasks: BackgroundTasks):
        task_id = self._create_running_task(
            prefix="global_cleanup_blacklist",
            task_type="global_cleanup_blacklist",
            message="正在初始化黑名单数据清理...",
        )
        background_tasks.add_task(self._run_cleanup_blacklist, task_id)
        return {"task_id": task_id, "message": "黑名单清理任务已启动"}

    def start_scan_global(self, background_tasks: BackgroundTasks, force: bool = False, exclude_non_stock: bool = False):
        task_id = self._create_running_task(
            prefix="global_scan",
            task_type="global_scan",
            message="正在初始化全局扫描...",
        )
        background_tasks.add_task(self._run_global_scan, task_id, force, exclude_non_stock)
        return {"task_id": task_id, "message": "全局扫描任务已启动"}

    def _run_cleanup_blacklist(self, task_id: str):
        try:
            self._update(task_id, "running", "开始清理黑名单历史分析数据...")
            cfg = get_filter_config()
            blacklist_ids = set(str(v).strip() for v in cfg.get("blacklist_group_ids", []) if str(v).strip())
            manager = get_db_path_manager()
            groups = manager.list_all_groups()
            target_groups = [g for g in groups if str(g.get("group_id", "")).strip() in blacklist_ids]

            self._log(task_id, f"📋 黑名单群组总数: {len(blacklist_ids)}，本地匹配: {len(target_groups)}")
            if not target_groups:
                self._update(task_id, "completed", "黑名单清理完成: 无匹配本地群组")
                return

            total_mentions_deleted = 0
            total_perf_deleted = 0
            processed = 0

            for i, group in enumerate(target_groups, 1):
                if self._stopped(task_id):
                    self._log(task_id, "🛑 清理任务已停止")
                    break

                gid = str(group.get("group_id", "")).strip()
                db_path = group.get("topics_db")
                self._log(task_id, f"👉 [{i}/{len(target_groups)}] 清理群组 {gid}")

                if not db_path or not os.path.exists(db_path):
                    self._log(task_id, f"   ⚠️ 群组 {gid} 无可用 topics_db，跳过")
                    continue

                conn = None
                try:
                    conn = sqlite3.connect(db_path, timeout=30)
                    cursor = conn.cursor()

                    cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='stock_mentions'")
                    has_mentions = bool((cursor.fetchone() or [0])[0])
                    cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='mention_performance'")
                    has_perf = bool((cursor.fetchone() or [0])[0])
                    if not has_mentions:
                        self._log(task_id, f"   ℹ️ 群组 {gid} 无 stock_mentions，跳过")
                        continue

                    perf_deleted = 0
                    if has_perf:
                        cursor.execute(
                            "DELETE FROM mention_performance WHERE mention_id IN (SELECT id FROM stock_mentions)"
                        )
                        perf_deleted = cursor.rowcount or 0

                    cursor.execute("DELETE FROM stock_mentions")
                    mentions_deleted = cursor.rowcount or 0
                    conn.commit()

                    total_perf_deleted += perf_deleted
                    total_mentions_deleted += mentions_deleted
                    processed += 1
                    self._log(task_id, f"   ✅ 完成: 删除提及 {mentions_deleted}，收益 {perf_deleted}")
                except Exception as e:
                    if conn:
                        conn.rollback()
                    self._log(task_id, f"   ❌ 清理失败: {e}")
                finally:
                    if conn:
                        conn.close()

            try:
                get_global_analyzer().invalidate_cache()
                self._log(task_id, "🔄 全局统计缓存已刷新")
            except Exception:
                pass

            if self._stopped(task_id):
                self._update(task_id, "cancelled", "黑名单清理已停止")
            else:
                self._update(
                    task_id,
                    "completed",
                    f"黑名单清理完成: {processed}/{len(target_groups)} 个群组，删除提及 {total_mentions_deleted}，收益 {total_perf_deleted}",
                    {
                        "groups_processed": processed,
                        "groups_total": len(target_groups),
                        "mentions_deleted": total_mentions_deleted,
                        "performances_deleted": total_perf_deleted,
                    },
                )
        except Exception as e:
            self._log(task_id, f"❌ 黑名单清理异常: {e}")
            self._update(task_id, "failed", f"黑名单清理失败: {e}")

    def _run_global_crawl(self, task_id: str, request: Any):
        GlobalCrawlService().run(
            task_id=task_id,
            request=request,
            add_task_log=self._log,
            update_task=self._update,
            is_task_stopped=self._stopped,
            get_cookie_for_group=self._get_cookie_for_group,
        )

    def _run_global_files_collect(self, task_id: str, request: Any):
        GlobalFileTaskService().run_collect(
            task_id=task_id,
            add_task_log=self._log,
            update_task=self._update,
            is_task_stopped=self._stopped,
            get_cookie_for_group=self._get_cookie_for_group,
            file_downloader_instances={},
        )

    def _run_global_files_download(self, task_id: str, request: Any):
        GlobalFileTaskService().run_download(
            task_id=task_id,
            request=request,
            add_task_log=self._log,
            update_task=self._update,
            is_task_stopped=self._stopped,
            get_cookie_for_group=self._get_cookie_for_group,
            file_downloader_instances={},
        )

    def _run_global_analyze_performance(self, task_id: str, force: bool):
        _ = force
        GlobalAnalyzePerformanceService().run(
            task_id=task_id,
            add_task_log=self._log,
            update_task=self._update,
            is_task_stopped=self._stopped,
            calc_window_days=365,
        )

    def _run_global_scan(self, task_id: str, force: bool, exclude_non_stock: bool):
        try:
            self._update(task_id, "running", "准备开始全局扫描...")
            self._log(task_id, "🚀 开始全局股票提及扫描...")

            manager = get_db_path_manager()
            all_groups = manager.list_all_groups()
            self._log(task_id, f"📋 共发现 {len(all_groups)} 个群组")
            if force:
                self._log(task_id, "ℹ️ 当前全局扫描的编排模式不区分 force，按增量采集执行")
            if exclude_non_stock is False:
                self._log(task_id, "ℹ️ 参数 exclude_non_stock 已兼容保留，当前版本始终强制应用白黑名单规则")

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
                self._log(task_id, line)

            if not groups:
                self._log(task_id, "ℹ️ 过滤后无可扫描群组，任务结束")
                self._update(task_id, "completed", "全局扫描完成: 过滤后无可扫描群组")
                return

            successes, failures = run_serial_incremental_pipeline(
                groups=groups,
                pages=2,
                per_page=20,
                calc_window_days=365,
                do_analysis=False,
                stop_check=lambda: self._stopped(task_id),
                log_callback=lambda msg: self._log(task_id, msg),
            )
            total_mentions = sum((item.get("extract") or {}).get("mentions_extracted", 0) for item in successes)

            if self._stopped(task_id):
                self._update(task_id, "cancelled", "全局扫描已停止")
            else:
                self._log(task_id, "")
                self._log(task_id, "=" * 50)
                self._log(task_id, f"🎉 全局扫描完成！共处理 {len(successes)}/{len(groups)} 个群组")
                self._log(task_id, f"📊 本次累计提取提及: {total_mentions} 次")
                if failures:
                    self._log(task_id, f"⚠️ 失败群组: {len(failures)} 个")

                try:
                    get_global_analyzer().invalidate_cache()
                    self._log(task_id, "🔄 全局统计缓存已刷新")
                except Exception:
                    pass

                self._update(
                    task_id,
                    "completed",
                    f"全局扫描完成: {len(successes)} 个群组, {total_mentions} 次提及",
                    {
                        "groups_total": len(groups),
                        "groups_succeeded": len(successes),
                        "groups_failed": len(failures),
                        "mentions_extracted_total": total_mentions,
                    },
                )
        except Exception as e:
            self._log(task_id, f"❌ 全局扫描异常: {e}")
            self._update(task_id, "failed", f"全局扫描失败: {e}")

    def _create_running_task(self, prefix: str, task_type: str, message: str) -> str:
        task_id = self.runtime.next_id(prefix)
        self.runtime.create_task(task_type=task_type, message=message, task_id=task_id, status="running")
        return task_id

    def _log(self, task_id: str, message: str):
        self.runtime.append_log(task_id, message)

    def _update(self, task_id: str, status: str, message: str, result: Dict[str, Any] | None = None):
        self.runtime.update_task(task_id, status=status, message=message, result=result)

    def _stopped(self, task_id: str) -> bool:
        return self.runtime.is_stopped(task_id)

    def _validate_global_crawl_request(self, request: Any):
        if getattr(request, "mode", None) != "range":
            return
        has_last_days = getattr(request, "last_days", None) is not None
        has_time_range = bool((getattr(request, "start_time", "") or "").strip() or (getattr(request, "end_time", "") or "").strip())
        if has_last_days and int(request.last_days) < 1:
            raise HTTPException(status_code=422, detail="last_days 必须大于 0")
        if has_last_days and has_time_range:
            raise HTTPException(
                status_code=422,
                detail="range 模式下，“最近天数(last_days)”与“开始/结束时间(start_time/end_time)”必须二选一",
            )
        if getattr(request, "start_time", None):
            self._parse_global_crawl_time(request.start_time, "start_time")
        if getattr(request, "end_time", None):
            self._parse_global_crawl_time(request.end_time, "end_time")

    @staticmethod
    def _parse_global_crawl_time(raw: Optional[str], field_name: str) -> Optional[datetime]:
        if raw is None:
            return None
        text = str(raw).strip()
        if not text:
            return None
        try:
            if "T" in text and len(text) == 16:
                text = text + ":00"
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            if len(text) >= 24 and text[-5] in ["+", "-"] and text[-3] != ":":
                text = text[:-2] + ":" + text[-2:]
            return datetime.fromisoformat(text)
        except Exception:
            raise HTTPException(
                status_code=422,
                detail=f"{field_name} 格式无效，请使用 ISO8601（例如 2026-02-21T10:00:00+08:00）",
            )

    @staticmethod
    def _get_cookie_for_group(group_id: str) -> str:
        try:
            account = get_accounts_sql_manager().get_account_for_group(str(group_id), mask_cookie=False)
            cookie = (account or {}).get("cookie", "")
            if cookie:
                return cookie
        except Exception:
            pass

        cfg = load_config() or {}
        auth = cfg.get("auth", {}) if isinstance(cfg, dict) else {}
        return auth.get("cookie", "")

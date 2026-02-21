#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""自动调度器（固定时点串行增量流水线）"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from db_path_manager import get_db_path_manager
from logger_config import log_info


BEIJING_TZ = timezone(timedelta(hours=8))


class SchedulerState(str, Enum):
    STOPPED = "stopped"
    RUNNING = "running"
    PAUSED = "paused"


class AutoScheduler:
    """固定时点执行：按群串行执行增量采集->分析。"""

    def __init__(self):
        self.state = SchedulerState.STOPPED
        self._main_task: Optional[asyncio.Task] = None
        self._manual_calc_task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._run_lock = asyncio.Lock()

        self.config: Dict[str, Any] = {
            "timezone": "Asia/Shanghai",
            "schedule_times": ["09:00", "12:00", "15:00", "18:00", "21:00"],
            "run_mode": "serial_group_pipeline",
            "pipeline": ["crawl_incremental", "analyze_pending_performance"],
            "include_download": False,
            "pages_per_group": 2,
            "per_page": 20,
            "calc_window_days": 365,
        }

        self.stats: Dict[str, Any] = {
            "round_count": 0,
            "calc_rounds": 0,
            "last_round_start": None,
            "last_round_end": None,
            "last_calc_time": None,
            "groups_synced": {},
            "errors": [],
            "current_group": None,
            "is_crawling": False,
            "is_calculating": False,
            "skipped_due_to_busy": 0,
            "last_run_summary": None,
        }

        self._log_callback: Optional[Callable[[str], None]] = None
        self._status_callback: Optional[Callable[[str, str], None]] = None

    def set_log_callback(self, callback: Callable[[str], None]):
        self._log_callback = callback

    def set_status_callback(self, callback: Callable[[str, str], None]):
        self._status_callback = callback

    def _update_status(self, status: str, message: str):
        if self._status_callback:
            self._status_callback(status, message)

    def log(self, message: str):
        ts = datetime.now().strftime("%H:%M:%S")
        full = f"[调度器 {ts}] {message}"
        if self._log_callback:
            self._log_callback(full)
        log_info(full)

    def update_config(self, new_config: Dict[str, Any]):
        for key, value in (new_config or {}).items():
            if key == "schedule_times" and isinstance(value, list):
                cleaned = []
                for item in value:
                    if isinstance(item, str) and ":" in item:
                        cleaned.append(item.strip())
                if cleaned:
                    self.config[key] = cleaned
            elif key in self.config:
                self.config[key] = value
        self.log(f"⚙️ 配置已更新: {new_config}")

    def _now(self) -> datetime:
        return datetime.now(BEIJING_TZ)

    def _get_next_run_time(self, now: Optional[datetime] = None) -> Optional[datetime]:
        now = now or self._now()
        candidates: List[datetime] = []
        for time_str in self.config.get("schedule_times", []):
            try:
                hour, minute = map(int, time_str.split(":"))
            except Exception:
                continue
            today = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if today > now:
                candidates.append(today)
            candidates.append((now + timedelta(days=1)).replace(hour=hour, minute=minute, second=0, microsecond=0))
        return min(candidates) if candidates else None

    def get_next_runs(self, count: int = 5) -> Dict[str, Any]:
        count = max(1, min(count, 20))
        now = self._now()
        times = []
        cursor = now
        for _ in range(count):
            nxt = self._get_next_run_time(cursor)
            if not nxt:
                break
            times.append(nxt.isoformat())
            cursor = nxt + timedelta(seconds=1)

        return {
            "timezone": self.config.get("timezone", "Asia/Shanghai"),
            "next_runs": times,
            "last_run_summary": self.stats.get("last_run_summary"),
        }

    def get_status(self) -> Dict[str, Any]:
        errors_total = len(self.stats.get("errors", []))
        return {
            "state": self.state.value,
            "is_crawling": bool(self.stats.get("is_crawling")),
            "is_calculating": bool(self.stats.get("is_calculating")),
            "current_group": self.stats.get("current_group"),
            "errors_total": errors_total,
            "last_crawl": self.stats.get("last_round_end"),
            "last_calc": self.stats.get("last_calc_time"),
            "crawl_rounds": int(self.stats.get("round_count", 0)),
            "calc_rounds": int(self.stats.get("calc_rounds", 0)),
            "config": self.config,
            "stats": {
                "round_count": self.stats.get("round_count", 0),
                "calc_rounds": self.stats.get("calc_rounds", 0),
                "last_round_start": self.stats.get("last_round_start"),
                "last_round_end": self.stats.get("last_round_end"),
                "last_calc_time": self.stats.get("last_calc_time"),
                "current_group": self.stats.get("current_group"),
                "is_crawling": self.stats.get("is_crawling"),
                "is_calculating": self.stats.get("is_calculating"),
                "groups_synced": self.stats.get("groups_synced", {}),
                "recent_errors": self.stats.get("errors", [])[-10:],
                "skipped_due_to_busy": self.stats.get("skipped_due_to_busy", 0),
                "last_run_summary": self.stats.get("last_run_summary"),
            },
        }

    async def start(self):
        if self.state == SchedulerState.RUNNING:
            self.log("⚠️ 调度器已在运行中")
            return

        self.state = SchedulerState.RUNNING
        self._stop_event = asyncio.Event()
        self._main_task = asyncio.create_task(self._schedule_loop())
        self.log("🚀 调度器启动（固定时点模式）")
        self._update_status("running", "调度器运行中")

    async def stop(self):
        if self.state == SchedulerState.STOPPED:
            self.log("⚠️ 调度器已停止")
            return

        self.state = SchedulerState.STOPPED
        self._update_status("stopped", "调度器停止中")
        self.log("🛑 调度器正在停止...")

        if self._stop_event:
            self._stop_event.set()

        if self._main_task and not self._main_task.done():
            try:
                await asyncio.wait_for(self._main_task, timeout=3)
            except asyncio.TimeoutError:
                self._main_task.cancel()

        self._main_task = None
        self.stats["current_group"] = None
        self.stats["is_crawling"] = False
        self.stats["is_calculating"] = False
        self.log("✅ 调度器已停止")
        self._update_status("stopped", "调度器已停止")

    async def _schedule_loop(self):
        while self.state == SchedulerState.RUNNING:
            try:
                now = self._now()
                nxt = self._get_next_run_time(now)
                if not nxt:
                    self.log("⚠️ 未配置有效 schedule_times，60 秒后重试")
                    await self._sleep_with_check(60)
                    continue

                wait_seconds = max((nxt - now).total_seconds(), 0)
                self.log(f"⏰ 下次自动执行: {nxt.strftime('%m-%d %H:%M')}，等待 {int(wait_seconds)} 秒")
                await self._sleep_with_check(wait_seconds)

                if self.state != SchedulerState.RUNNING:
                    break

                await self._run_scheduled_round(triggered_at=nxt)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.stats["errors"].append({"time": self._now().isoformat(), "error": str(e)})
                self.log(f"❌ 调度循环异常: {e}")
                await self._sleep_with_check(10)

    async def _run_scheduled_round(self, triggered_at: datetime):
        if self._run_lock.locked():
            self.stats["skipped_due_to_busy"] = int(self.stats.get("skipped_due_to_busy", 0)) + 1
            self.log("⚠️ 上一轮仍在运行，本时点跳过")
            return

        async with self._run_lock:
            self.stats["round_count"] = int(self.stats.get("round_count", 0)) + 1
            self.stats["calc_rounds"] = int(self.stats.get("calc_rounds", 0)) + 1
            self.stats["last_round_start"] = self._now().isoformat()
            self.stats["is_crawling"] = True
            self.stats["is_calculating"] = True
            self._update_status("running", f"时点任务执行中: {triggered_at.strftime('%H:%M')}")
            self.log(f"📡 开始时点任务: {triggered_at.strftime('%H:%M')}")

            try:
                from global_pipeline import list_groups, run_serial_incremental_pipeline

                all_groups = list_groups(apply_scan_filter=False)
                groups, excluded_groups, reason_counts, default_action = self._apply_group_scan_filter(all_groups)
                self.log(f"📋 本轮群组总数: {len(all_groups)}")
                self.log(f"🧹 过滤后纳入: {len(groups)}，排除: {len(excluded_groups)}")
                self.log(f"⚙️ 过滤策略: 未配置群组默认{'纳入' if default_action == 'include' else '排除'}")
                if reason_counts:
                    self.log(f"📌 命中统计: {reason_counts}")
                if excluded_groups:
                    preview = "，".join(excluded_groups[:20])
                    suffix = " ..." if len(excluded_groups) > 20 else ""
                    self.log(f"🚫 已排除: {preview}{suffix}")

                if not groups:
                    self.stats["last_round_end"] = self._now().isoformat()
                    self.stats["last_calc_time"] = self.stats["last_round_end"]
                    self.stats["last_run_summary"] = {
                        "trigger_time": triggered_at.isoformat(),
                        "groups_total": len(all_groups),
                        "groups_success": 0,
                        "groups_failed": 0,
                        "groups_excluded": len(excluded_groups),
                        "duration_seconds": 0,
                        "failed_groups": [],
                    }
                    self.log("ℹ️ 过滤后无可执行群组，本轮结束")
                    self._update_status("running", "时点任务完成，等待下一轮")
                    return

                def _log(msg: str):
                    self.log(msg)

                def _stopped() -> bool:
                    return self.state != SchedulerState.RUNNING

                start = self._now()
                # 把同步重任务移出事件循环，避免阻塞 FastAPI 接口响应
                successes, failures = await asyncio.to_thread(
                    run_serial_incremental_pipeline,
                    groups=groups,
                    pages=int(self.config.get("pages_per_group", 2) or 2),
                    per_page=int(self.config.get("per_page", 20) or 20),
                    calc_window_days=int(self.config.get("calc_window_days", 365) or 365),
                    do_analysis=True,
                    stop_check=_stopped,
                    log_callback=_log,
                )

                for item in successes:
                    gid = str(item.get("group_id"))
                    self.stats["groups_synced"][gid] = self._now().isoformat()

                self.stats["last_round_end"] = self._now().isoformat()
                self.stats["last_calc_time"] = self.stats["last_round_end"]

                elapsed = int((self._now() - start).total_seconds())
                summary = {
                    "trigger_time": triggered_at.isoformat(),
                    "groups_total": len(all_groups),
                    "groups_success": len(successes),
                    "groups_failed": len(failures),
                    "groups_excluded": len(excluded_groups),
                    "duration_seconds": elapsed,
                    "failed_groups": failures[:20],
                }
                self.stats["last_run_summary"] = summary
                self.log(f"✅ 时点任务完成：成功 {len(successes)}/{len(groups)}，失败 {len(failures)}，耗时 {elapsed}s")

                try:
                    from global_analyzer import get_global_analyzer
                    get_global_analyzer().invalidate_cache()
                    self.log("🔄 全局缓存已刷新")
                except Exception as e:
                    self.log(f"⚠️ 全局缓存刷新失败: {e}")

                self._update_status("running", "时点任务完成，等待下一轮")
            except asyncio.CancelledError:
                self.log("🛑 时点任务被取消")
                self._update_status(self.state.value, "时点任务已停止")
                raise
            except Exception as e:
                self.stats["errors"].append({"time": self._now().isoformat(), "error": str(e)})
                self.log(f"❌ 时点任务失败: {e}")
                self._update_status(self.state.value, f"时点任务失败: {e}")
            finally:
                # 无论成功/失败，都要复位阶段状态，避免前端一直显示“运行中”
                self.stats["is_crawling"] = False
                self.stats["is_calculating"] = False
                self.stats["current_group"] = None

    async def trigger_manual_analysis_task(self):
        """手动触发分析：沿用全群收益计算。"""
        if self.stats.get("is_calculating"):
            self.log("⚠️ 分析任务正在运行中，忽略请求")
            return None

        async def _run_and_track():
            self.stats["is_calculating"] = True
            try:
                from stock_analyzer import StockAnalyzer
                groups = self._get_active_groups()
                for idx, group in enumerate(groups, 1):
                    if self.state == SchedulerState.STOPPED:
                        break
                    gid = str(group.get("group_id"))
                    self.stats["current_group"] = gid
                    self.log(f"⏳ [手动分析 {idx}/{len(groups)}] 群组 {gid}")
                    # 同步计算过程转为线程执行，避免阻塞主事件循环
                    await asyncio.to_thread(
                        lambda: StockAnalyzer(gid).calc_pending_performance(
                            calc_window_days=int(self.config.get("calc_window_days", 365) or 365)
                        )
                    )
                self.stats["last_calc_time"] = self._now().isoformat()
                self.stats["calc_rounds"] = int(self.stats.get("calc_rounds", 0)) + 1
                self.log("✅ 手动分析完成")
                self._update_status(self.state.value, "数据分析完成")
            except asyncio.CancelledError:
                self.log("🛑 数据分析被手动停止")
                self._update_status(self.state.value, "数据分析已停止")
            except Exception as e:
                self.stats["errors"].append({"time": self._now().isoformat(), "error": str(e)})
                self.log(f"❌ 数据分析失败: {e}")
                self._update_status(self.state.value, f"数据分析失败: {e}")
            finally:
                self.stats["current_group"] = None
                self.stats["is_calculating"] = False
                self._manual_calc_task = None

        self._manual_calc_task = asyncio.create_task(_run_and_track())
        return self._manual_calc_task

    async def stop_manual_analysis(self):
        if self._manual_calc_task and not self._manual_calc_task.done():
            self.log("🛑 正在停止数据分析任务...")
            self._manual_calc_task.cancel()
            return True
        if self.stats.get("is_calculating"):
            self.log("⚠️ 分析执行中但没有可取消任务句柄")
            return False
        self.log("⚠️ 没有正在运行的数据分析任务")
        return False

    def _get_active_groups(self) -> List[Dict[str, Any]]:
        db_manager = get_db_path_manager()
        all_groups = db_manager.list_all_groups()
        groups, excluded_groups, reason_counts, default_action = self._apply_group_scan_filter(all_groups)
        self.log(f"📋 手动分析群组总数: {len(all_groups)}")
        self.log(f"🧹 手动分析纳入: {len(groups)}，排除: {len(excluded_groups)}")
        self.log(f"⚙️ 过滤策略: 未配置群组默认{'纳入' if default_action == 'include' else '排除'}")
        if reason_counts:
            self.log(f"📌 命中统计: {reason_counts}")

        def sort_key(g: Dict[str, Any]):
            gid = str(g.get("group_id"))
            return self.stats["groups_synced"].get(gid, "")

        groups.sort(key=sort_key)
        return groups

    def _apply_group_scan_filter(self, groups: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], List[str], Dict[str, int], str]:
        try:
            from group_scan_filter import filter_groups
            filtered = filter_groups(groups)
            cfg = filtered.get("config", {}) or {}
            default_action = str(cfg.get("default_action", "include"))
            included = filtered.get("included_groups", []) or []
            excluded_rows = filtered.get("excluded_groups", []) or []
            reason_counts = filtered.get("reason_counts", {}) or {}
            excluded = [f"{g.get('group_id')}({g.get('scan_filter_reason', 'unknown')})" for g in excluded_rows]
            return included, excluded, reason_counts, default_action
        except Exception:
            return groups, [], {}, "include"

    async def _sleep_with_check(self, seconds: float):
        seconds = max(0, float(seconds))
        if self._stop_event is None:
            self._stop_event = asyncio.Event()
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            return


_scheduler_instance: Optional[AutoScheduler] = None


def get_scheduler() -> AutoScheduler:
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = AutoScheduler()
    return _scheduler_instance

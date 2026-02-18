#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动调度器模块
使用 asyncio 管理后台爬取、提取和收益计算任务
"""

import asyncio
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Callable
from enum import Enum

from db_path_manager import get_db_path_manager
from logger_config import log_info, log_warning, log_error


# 北京时区
BEIJING_TZ = timezone(timedelta(hours=8))


class SchedulerState(str, Enum):
    STOPPED = "stopped"
    RUNNING = "running"
    PAUSED = "paused"


class AutoScheduler:
    """
    自动爬取调度器

    两种调度循环：
    1. 高频循环 — 爬取+文本提取（30-60分钟一轮）
    2. 低频循环 — 收益计算（每日12:00 + 15:15）
    """

    def __init__(self):
        self.state = SchedulerState.STOPPED
        self._crawl_task: Optional[asyncio.Task] = None
        self._calc_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event() if asyncio.get_event_loop().is_running() else None

        # 配置参数
        self.config = {
            'group_interval_min': 180,    # 群间隔最小(秒)
            'group_interval_max': 300,    # 群间隔最大(秒)
            'round_sleep_min': 1800,      # 轮间隔最小(秒)
            'round_sleep_max': 3600,      # 轮间隔最大(秒)
            'pages_per_group': 2,         # 每群每次拉取页数
            'calc_window_days': 365,      # 收益计算窗口(天)
            'calc_times': ['12:00', '15:15'],  # 定时计算时间点
        }

        # 状态跟踪
        self.stats = {
            'round_count': 0,
            'last_round_start': None,
            'last_round_end': None,
            'last_calc_time': None,
            'groups_synced': {},       # group_id -> last_sync_time
            'errors': [],              # 最近的错误记录
            'current_group': None,
            'is_crawling': False,
            'is_calculating': False,
        }

        # 回调
        self._log_callback: Optional[Callable] = None
        self._backoff_multiplier = 1  # 退避倍数

    def set_log_callback(self, callback: Callable):
        self._log_callback = callback

    def log(self, message: str):
        timestamp = datetime.now().strftime('%H:%M:%S')
        full_msg = f"[调度器 {timestamp}] {message}"
        if self._log_callback:
            self._log_callback(full_msg)
        log_info(full_msg)

    def update_config(self, new_config: Dict):
        """更新调度器配置"""
        for key, value in new_config.items():
            if key in self.config:
                self.config[key] = value
        self.log(f"⚙️ 配置已更新: {new_config}")

    def get_status(self) -> Dict[str, Any]:
        """获取调度器完整状态"""
        return {
            'state': self.state.value,
            'config': self.config,
            'stats': {
                'round_count': self.stats['round_count'],
                'last_round_start': self.stats['last_round_start'],
                'last_round_end': self.stats['last_round_end'],
                'last_calc_time': self.stats['last_calc_time'],
                'current_group': self.stats['current_group'],
                'is_crawling': self.stats['is_crawling'],
                'is_calculating': self.stats['is_calculating'],
                'groups_synced': self.stats['groups_synced'],
                'recent_errors': self.stats['errors'][-10:],  # 最近10条错误
                'backoff_multiplier': self._backoff_multiplier,
            }
        }

    # ========== 启动/停止 ==========

    async def start(self):
        """启动调度器"""
        if self.state == SchedulerState.RUNNING:
            self.log("⚠️ 调度器已在运行中")
            return

        self.state = SchedulerState.RUNNING
        self._stop_event = asyncio.Event()
        self._backoff_multiplier = 1
        self.log("🚀 调度器启动")

        # 启动两个循环
        self._crawl_task = asyncio.create_task(self._crawl_loop())
        self._calc_task = asyncio.create_task(self._calc_loop())

    async def stop(self):
        """停止调度器"""
        if self.state == SchedulerState.STOPPED:
            return

        self.log("🛑 调度器停止中...")
        self.state = SchedulerState.STOPPED
        if self._stop_event:
            self._stop_event.set()

        # 取消任务
        for task in [self._crawl_task, self._calc_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._crawl_task = None
        self._calc_task = None
        self.log("✅ 调度器已停止")

    # ========== 高频循环：爬取 + 提取 ==========

    async def _crawl_loop(self):
        """高频循环：轮询所有群组，爬取最新 + 提取股票名称"""
        while self.state == SchedulerState.RUNNING:
            try:
                self.stats['round_count'] += 1
                self.stats['last_round_start'] = datetime.now().isoformat()
                self.stats['is_crawling'] = True
                round_num = self.stats['round_count']

                self.log(f"📡 开始第 {round_num} 轮爬取...")

                # 获取所有活跃群组
                groups = self._get_active_groups()
                if not groups:
                    self.log("⚠️ 没有可用群组")
                    await self._sleep_with_check(60)
                    continue

                self.log(f"📋 本轮处理 {len(groups)} 个群组")

                for i, group in enumerate(groups):
                    if self.state != SchedulerState.RUNNING:
                        break

                    group_id = group['group_id']
                    self.stats['current_group'] = group_id

                    try:
                        await self._process_group(group_id)
                        self.stats['groups_synced'][group_id] = datetime.now().isoformat()
                    except Exception as e:
                        error_msg = f"处理群组 {group_id} 失败: {e}"
                        self.log(f"❌ {error_msg}")
                        self.stats['errors'].append({
                            'time': datetime.now().isoformat(),
                            'group_id': group_id,
                            'error': str(e)
                        })

                        # 检查是否是限流错误
                        if self._is_rate_limit_error(e):
                            await self._handle_rate_limit()

                    # 群组间随机间隔
                    if i < len(groups) - 1:
                        interval = random.uniform(
                            self.config['group_interval_min'],
                            self.config['group_interval_max']
                        ) * self._backoff_multiplier
                        self.log(f"⏳ 等待 {int(interval)} 秒后处理下一个群组...")
                        await self._sleep_with_check(interval)

                self.stats['current_group'] = None
                self.stats['is_crawling'] = False
                self.stats['last_round_end'] = datetime.now().isoformat()

                # 一轮完成 → 刷新全局缓存
                try:
                    from global_analyzer import get_global_analyzer
                    get_global_analyzer().invalidate_cache()
                    self.log("🔄 全局缓存已刷新")
                except Exception as e:
                    self.log(f"⚠️ 刷新全局缓存失败: {e}")

                # 成功完成一轮，重置退避
                self._backoff_multiplier = max(1, self._backoff_multiplier * 0.8)

                # 轮间长休眠
                sleep_time = random.uniform(
                    self.config['round_sleep_min'],
                    self.config['round_sleep_max']
                ) * self._backoff_multiplier
                self.log(f"😴 第 {round_num} 轮完成，休眠 {int(sleep_time/60)} 分钟...")
                await self._sleep_with_check(sleep_time)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log(f"❌ 爬取循环异常: {e}")
                self.stats['is_crawling'] = False
                await self._sleep_with_check(60)

    async def _process_group(self, group_id: str):
        """处理单个群组：爬取最新 + 提取股票名称"""
        self.log(f"🔍 处理群组 {group_id}...")

        # 阶段1：爬取最新帖子
        try:
            await self._crawl_group(group_id)
        except Exception as e:
            self.log(f"⚠️ 群组 {group_id} 爬取失败: {e}")
            raise

        # 阶段2：提取股票名称（纯本地操作）
        try:
            from stock_analyzer import StockAnalyzer
            analyzer = StockAnalyzer(group_id)
            result = analyzer.extract_only()
            if result.get('mentions_extracted', 0) > 0:
                self.log(f"📝 群组 {group_id}: 提取 {result['mentions_extracted']} 条提及")
        except Exception as e:
            self.log(f"⚠️ 群组 {group_id} 提取失败: {e}")

    async def _crawl_group(self, group_id: str):
        """执行群组爬取（在线程池中运行同步代码）"""
        loop = asyncio.get_event_loop()

        def _sync_crawl():
            try:
                from main import get_crawler_for_group
                crawler = get_crawler_for_group(group_id, log_callback=lambda msg: self.log(f"  [{group_id}] {msg}"))
                crawler.crawl_latest_until_complete(per_page=20)
                return True
            except Exception as e:
                raise e

        await loop.run_in_executor(None, _sync_crawl)

    # ========== 低频循环：定时收益计算 ==========

    async def _calc_loop(self):
        """低频循环：每日 12:00 + 15:15 计算收益表现"""
        while self.state == SchedulerState.RUNNING:
            try:
                now = datetime.now(BEIJING_TZ)
                next_calc = self._get_next_calc_time(now)

                if next_calc:
                    wait_seconds = (next_calc - now).total_seconds()
                    if wait_seconds > 0:
                        self.log(f"⏰ 下次收益计算: {next_calc.strftime('%H:%M')}，等待 {int(wait_seconds/60)} 分钟")
                        await self._sleep_with_check(min(wait_seconds, 300))  # 最多等5分钟再检查
                        continue

                    # 到达计算时间
                    if wait_seconds > -300:  # 5分钟内的窗口
                        await self._run_performance_calc()

                # 等待1分钟后再检查
                await self._sleep_with_check(60)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log(f"❌ 计算循环异常: {e}")
                await self._sleep_with_check(60)

    def _get_next_calc_time(self, now: datetime) -> Optional[datetime]:
        """获取下一次计算时间点"""
        calc_times = self.config.get('calc_times', ['12:00', '15:15'])

        candidates = []
        for time_str in calc_times:
            hour, minute = map(int, time_str.split(':'))
            calc_dt = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if calc_dt > now:
                candidates.append(calc_dt)
            # 也添加明天的第一个时间点
            tomorrow_dt = (now + timedelta(days=1)).replace(hour=hour, minute=minute, second=0, microsecond=0)
            candidates.append(tomorrow_dt)

        return min(candidates) if candidates else None

    async def _run_performance_calc(self):
        """执行收益计算"""
        self.stats['is_calculating'] = True
        self.log("📈 开始定时收益计算...")

        loop = asyncio.get_event_loop()
        groups = self._get_active_groups()

        for group in groups:
            if self.state != SchedulerState.RUNNING:
                break

            group_id = group['group_id']
            try:
                def _sync_calc(gid):
                    from stock_analyzer import StockAnalyzer
                    analyzer = StockAnalyzer(gid)
                    return analyzer.calc_pending_performance(
                        calc_window_days=self.config['calc_window_days']
                    )

                result = await loop.run_in_executor(None, _sync_calc, group_id)
                processed = result.get('processed', 0)
                if processed > 0:
                    self.log(f"📊 群组 {group_id}: 计算 {processed} 条收益")

            except Exception as e:
                self.log(f"⚠️ 群组 {group_id} 收益计算失败: {e}")

            # 群组间短暂间隔
            await self._sleep_with_check(5)

        self.stats['is_calculating'] = False
        self.stats['last_calc_time'] = datetime.now().isoformat()

        # 刷新全局缓存
        try:
            from global_analyzer import get_global_analyzer
            get_global_analyzer().invalidate_cache()
        except Exception:
            pass

        self.log("✅ 收益计算完成")

    # ========== 辅助方法 ==========

    def _get_active_groups(self) -> List[Dict]:
        """获取所有活跃群组（跳过过期群）"""
        db_manager = get_db_path_manager()
        groups = db_manager.list_all_groups()

        # 按最后同步时间排序（最久未同步的优先）
        def sort_key(g):
            last_sync = self.stats['groups_synced'].get(g['group_id'], '')
            return last_sync  # 空字符串排最前

        groups.sort(key=sort_key)
        return groups

    def _is_rate_limit_error(self, error: Exception) -> bool:
        """检测是否为限流错误"""
        error_str = str(error).lower()
        return any(keyword in error_str for keyword in [
            '429', 'rate limit', 'too many', 'throttl', '频率', '限流'
        ])

    async def _handle_rate_limit(self):
        """限流退避处理"""
        self._backoff_multiplier = min(self._backoff_multiplier * 2, 10)
        wait = 60 * self._backoff_multiplier
        self.log(f"🚨 触发限流退避！等待 {int(wait)} 秒，退避倍数: {self._backoff_multiplier}x")
        await self._sleep_with_check(wait)

    async def _sleep_with_check(self, seconds: float):
        """可中断的睡眠"""
        if self._stop_event is None:
            self._stop_event = asyncio.Event()
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass  # 正常超时，继续运行


# 全局单例
_scheduler_instance = None


def get_scheduler() -> AutoScheduler:
    """获取调度器单例"""
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = AutoScheduler()
    return _scheduler_instance

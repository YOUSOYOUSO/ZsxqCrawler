#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全局分析模块
跨群组聚合股票提及、胜率、板块热度等数据
"""

import sqlite3
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from collections import defaultdict

from db_path_manager import get_db_path_manager
from logger_config import log_info, log_warning, log_error


class GlobalAnalyzer:
    """跨群组全局数据聚合引擎"""

    def __init__(self):
        self.db_path_manager = get_db_path_manager()
        self._cache = {}
        self._cache_time = {}
        self._cache_ttl = 300  # 缓存有效期5分钟

    def _is_cache_valid(self, key: str) -> bool:
        if key not in self._cache_time:
            return False
        return (datetime.now() - self._cache_time[key]).total_seconds() < self._cache_ttl

    def _set_cache(self, key: str, data: Any):
        self._cache[key] = data
        self._cache_time[key] = datetime.now()

    def invalidate_cache(self):
        """清除全部缓存（调度器每轮结束后调用）"""
        self._cache.clear()
        self._cache_time.clear()

    def _get_all_group_dbs(self) -> List[Dict]:
        """获取所有群组的数据库信息"""
        return self.db_path_manager.list_all_groups()

    def _get_conn(self, db_path: str):
        """获取带 WAL 模式和超时的数据库连接"""
        conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout=30000')
        return conn

    def _query_all_groups(self, query: str, params: tuple = ()) -> List[tuple]:
        """对所有群组数据库执行相同查询，合并结果"""
        all_results = []
        for group in self._get_all_group_dbs():
            db_path = group['topics_db']
            if not os.path.exists(db_path):
                continue
            try:
                conn = self._get_conn(db_path)
                cursor = conn.cursor()
                cursor.execute(query, params)
                rows = cursor.fetchall()
                # 给每行添加 group_id
                for row in rows:
                    all_results.append((group['group_id'],) + row)
                conn.close()
            except Exception as e:
                log_warning(f"查询群组 {group['group_id']} 失败: {e}")
        return all_results

    # ========== 全局统计 ==========

    def get_global_stats(self) -> Dict[str, Any]:
        """全局统计概览"""
        cache_key = 'global_stats'
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        groups = self._get_all_group_dbs()
        total_topics = 0
        total_mentions = 0
        total_stocks = set()
        total_performance = 0
        group_count = len(groups)

        for group in groups:
            db_path = group['topics_db']
            if not os.path.exists(db_path):
                continue
            try:
                conn = self._get_conn(db_path)
                cursor = conn.cursor()

                cursor.execute('SELECT COUNT(*) FROM topics')
                total_topics += cursor.fetchone()[0]

                try:
                    cursor.execute('SELECT COUNT(*) FROM stock_mentions')
                    total_mentions += cursor.fetchone()[0]

                    cursor.execute('SELECT DISTINCT stock_code FROM stock_mentions')
                    for row in cursor.fetchall():
                        total_stocks.add(row[0])

                    cursor.execute('SELECT COUNT(*) FROM mention_performance')
                    total_performance += cursor.fetchone()[0]
                except Exception:
                    pass  # stock_mentions 表可能不存在

                conn.close()
            except Exception as e:
                log_warning(f"统计群组 {group['group_id']} 失败: {e}")

        result = {
            'group_count': group_count,
            'total_topics': total_topics,
            'total_mentions': total_mentions,
            'unique_stocks': len(total_stocks),
            'total_performance': total_performance
        }
        self._set_cache(cache_key, result)
        return result

    # ========== 全局胜率排行 ==========

    def get_global_win_rate(self, min_mentions: int = 2,
                            return_period: str = 'return_5d',
                            limit: int = 50) -> List[Dict]:
        """
        跨群组胜率排行
        合并所有群组中同一股票的提及数据，计算综合胜率
        """
        cache_key = f'win_rate_{return_period}_{min_mentions}_{limit}'
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        # 聚合所有群组的 mention_performance 数据
        stock_data = defaultdict(lambda: {
            'total': 0, 'positive': 0, 'returns': [],
            'groups': set(), 'stock_name': ''
        })

        for group in self._get_all_group_dbs():
            db_path = group['topics_db']
            if not os.path.exists(db_path):
                continue
            try:
                conn = self._get_conn(db_path)
                cursor = conn.cursor()
                cursor.execute(f'''
                    SELECT sm.stock_code, sm.stock_name, mp.{return_period}
                    FROM stock_mentions sm
                    JOIN mention_performance mp ON sm.id = mp.mention_id
                    WHERE mp.{return_period} IS NOT NULL
                ''')
                for code, name, ret in cursor.fetchall():
                    d = stock_data[code]
                    d['total'] += 1
                    if ret > 0:
                        d['positive'] += 1
                    d['returns'].append(ret)
                    d['groups'].add(group['group_id'])
                    d['stock_name'] = name
                conn.close()
            except Exception as e:
                log_warning(f"胜率查询群组 {group['group_id']} 失败: {e}")

        # 计算胜率并排序
        results = []
        for code, d in stock_data.items():
            if d['total'] < min_mentions:
                continue
            win_rate = d['positive'] / d['total'] * 100 if d['total'] > 0 else 0
            avg_return = sum(d['returns']) / len(d['returns']) if d['returns'] else 0
            results.append({
                'stock_code': code,
                'stock_name': d['stock_name'],
                'mention_count': d['total'],
                'win_rate': round(win_rate, 1),
                'avg_return': round(avg_return, 2),
                'group_count': len(d['groups']),
                'groups': list(d['groups'])
            })

        results.sort(key=lambda x: (-x['win_rate'], -x['mention_count']))
        results = results[:limit]

        self._set_cache(cache_key, results)
        return results

    # ========== 全局板块热度 ==========

    def get_global_sector_heat(self) -> List[Dict]:
        """跨群组板块热度聚合"""
        cache_key = 'sector_heat'
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        from stock_analyzer import SECTOR_KEYWORDS

        sector_stats = defaultdict(lambda: {
            'mention_count': 0, 'stocks': set(), 'positive': 0, 'total_with_return': 0
        })

        for group in self._get_all_group_dbs():
            db_path = group['topics_db']
            if not os.path.exists(db_path):
                continue
            try:
                conn = self._get_conn(db_path)
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT sm.stock_name, sm.stock_code, mp.return_5d
                    FROM stock_mentions sm
                    LEFT JOIN mention_performance mp ON sm.id = mp.mention_id
                ''')
                for name, code, ret in cursor.fetchall():
                    name_lower = name.lower() if name else ''
                    for sector, keywords in SECTOR_KEYWORDS.items():
                        if any(kw in name_lower for kw in keywords):
                            s = sector_stats[sector]
                            s['mention_count'] += 1
                            s['stocks'].add(code)
                            if ret is not None:
                                s['total_with_return'] += 1
                                if ret > 0:
                                    s['positive'] += 1
                            break
                conn.close()
            except Exception:
                pass

        results = []
        for sector, s in sector_stats.items():
            win_rate = (s['positive'] / s['total_with_return'] * 100
                       if s['total_with_return'] > 0 else 0)
            results.append({
                'sector': sector,
                'mention_count': s['mention_count'],
                'stock_count': len(s['stocks']),
                'win_rate': round(win_rate, 1)
            })

        results.sort(key=lambda x: -x['mention_count'])
        self._set_cache(cache_key, results)
        return results

    # ========== 全局信号雷达 ==========

    def get_global_signals(self, lookback_days: int = 7,
                           min_mentions: int = 2) -> List[Dict]:
        """
        跨群共识信号：多个群同时提及的股票 = 更高权重
        """
        cache_key = f'signals_{lookback_days}_{min_mentions}'
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        since_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        stock_signals = defaultdict(lambda: {
            'mentions': 0, 'groups': set(), 'stock_name': '',
            'latest_date': '', 'returns': []
        })

        for group in self._get_all_group_dbs():
            db_path = group['topics_db']
            if not os.path.exists(db_path):
                continue
            try:
                conn = self._get_conn(db_path)
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT sm.stock_code, sm.stock_name, sm.mention_date, mp.return_5d
                    FROM stock_mentions sm
                    LEFT JOIN mention_performance mp ON sm.id = mp.mention_id
                    WHERE sm.mention_date >= ?
                    ORDER BY sm.mention_date DESC
                ''', (since_date,))
                for code, name, date, ret in cursor.fetchall():
                    s = stock_signals[code]
                    s['mentions'] += 1
                    s['groups'].add(group['group_id'])
                    s['stock_name'] = name
                    if date > s['latest_date']:
                        s['latest_date'] = date
                    if ret is not None:
                        s['returns'].append(ret)
                conn.close()
            except Exception:
                pass

        results = []
        for code, s in stock_signals.items():
            if s['mentions'] < min_mentions:
                continue
            avg_ret = sum(s['returns']) / len(s['returns']) if s['returns'] else None
            # 权重 = 提及次数 × 群组数（跨群共识加权）
            weight = s['mentions'] * len(s['groups'])
            results.append({
                'stock_code': code,
                'stock_name': s['stock_name'],
                'mention_count': s['mentions'],
                'group_count': len(s['groups']),
                'groups': list(s['groups']),
                'latest_date': s['latest_date'],
                'avg_return': round(avg_ret, 2) if avg_ret is not None else None,
                'consensus_weight': weight
            })

        results.sort(key=lambda x: -x['consensus_weight'])
        self._set_cache(cache_key, results)
        return results

    # ========== 群组概览 ==========

    def get_groups_overview(self) -> List[Dict]:
        """各群组摘要统计"""
        cache_key = 'groups_overview'
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        results = []
        for group in self._get_all_group_dbs():
            db_path = group['topics_db']
            if not os.path.exists(db_path):
                continue
            try:
                conn = self._get_conn(db_path)
                cursor = conn.cursor()

                # 话题数和最新更新
                cursor.execute('SELECT COUNT(*), MAX(create_time) FROM topics')
                topic_count, latest_time = cursor.fetchone()

                # 股票提及数
                mention_count = 0
                stock_count = 0
                try:
                    cursor.execute('SELECT COUNT(*), COUNT(DISTINCT stock_code) FROM stock_mentions')
                    mention_count, stock_count = cursor.fetchone()
                except Exception:
                    pass

                # 胜率
                win_rate = None
                try:
                    cursor.execute('''
                        SELECT COUNT(*), SUM(CASE WHEN return_5d > 0 THEN 1 ELSE 0 END)
                        FROM mention_performance WHERE return_5d IS NOT NULL
                    ''')
                    total, positive = cursor.fetchone()
                    if total and total > 0:
                        win_rate = round(positive / total * 100, 1)
                except Exception:
                    pass

                conn.close()

                results.append({
                    'group_id': group['group_id'],
                    'topic_count': topic_count or 0,
                    'latest_update': latest_time,
                    'mention_count': mention_count,
                    'stock_count': stock_count,
                    'win_rate': win_rate
                })
            except Exception as e:
                log_warning(f"概览群组 {group['group_id']} 失败: {e}")

        results.sort(key=lambda x: x.get('latest_update') or '', reverse=True)
        self._set_cache(cache_key, results)
        return results


# 全局单例
_global_analyzer_instance = None


def get_global_analyzer() -> GlobalAnalyzer:
    """获取全局分析器单例"""
    global _global_analyzer_instance
    if _global_analyzer_instance is None:
        _global_analyzer_instance = GlobalAnalyzer()
    return _global_analyzer_instance

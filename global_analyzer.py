#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全局分析模块
跨群组聚合股票提及、胜率、板块热度等数据
"""

import sqlite3
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Set, TypedDict
from collections import defaultdict
import concurrent.futures
import time

from db_path_manager import get_db_path_manager
from logger_config import log_info, log_warning, log_error


class SectorStat(TypedDict):
    mention_count: int
    stocks: Set[str]
    positive: int
    total_with_return: int

class StockSignal(TypedDict):
    mentions: int
    group_names: Set[str]
    stock_name: str
    latest_date: str
    returns: List[float]

class StockData(TypedDict):
    stock_name: str
    detail_returns: List[float]
    detail_dates: List[str]
    detail_groups: List[str]

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

    def _get_group_name(self, conn, group_id: str) -> str:
        """从数据库获取群组名称"""
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT name FROM groups WHERE group_id = ?', (group_id,))
            row = cursor.fetchone()
            if row:
                return row[0]
        except Exception:
            pass
        return str(group_id)

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
                    row = cursor.fetchone()
                    total_mentions += row[0] if row else 0

                    cursor.execute('SELECT DISTINCT stock_code FROM stock_mentions')
                    for row in cursor.fetchall():
                        total_stocks.add(row[0])

                    cursor.execute('SELECT COUNT(*) FROM mention_performance')
                    row = cursor.fetchone()
                    total_performance += row[0] if row else 0
                except Exception:
                    pass  # stock_mentions 表可能不存在

                conn.close()
            except Exception as e:
                log_warning(f"统计群组 {group['group_id']} 失败: {e}")

        result: Dict[str, Any] = {
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
                            limit: int = 1000,
                            start_date: Optional[str] = None,
                            sort_by: str = 'win_rate',
                            order: str = 'desc',
                            page: int = 1,
                            page_size: int = 20) -> Dict[str, Any]:
        """
        跨群组胜率排行 (支持过滤、排序、分页)
        优化：并行查询 + 内存过滤/排序
        """
        # 1. 获取全量数据 (仅按 min_mentions 缓存)
        # return_period 也作为 key，因为 SQL 查询依赖它
        raw_data = self._get_cached_raw_win_rate(min_mentions, return_period)

        # 2. 内存过滤 (Date)
        filtered_results = []
        for item in raw_data:
            # 检查日期过滤
            # raw_data item 结构: {'stock_code':..., 'returns': [r1, r2...], 'dates': [d1, d2...]}
            # 注意: 为了支持 date filter，我们需要在 raw data 里保留 mention_date
            # 如果 start_date 存在，我们需要重新计算该股票在范围内的 win_rate
            
            if not start_date:
                # 无时间过滤，直接使用预计算好的统计值
                filtered_results.append(item)
                continue

            # 有时间过滤：需要重新聚合该股票的 returns
            valid_returns = []
            mention_count = 0
            groups = set()
            
            for ret, date_str, gid in zip(item['detail_returns'], item['detail_dates'], item['detail_groups']):
                if date_str >= start_date:
                    valid_returns.append(ret)
                    mention_count += 1
                    groups.add(gid)
            
            if mention_count < min_mentions:
                continue

            positive = sum(1 for r in valid_returns if r > 0)
            win_rate = positive / mention_count * 100 if mention_count > 0 else 0.0
            avg_return = sum(valid_returns) / len(valid_returns) if valid_returns else 0.0

            filtered_results.append({
                'stock_code': item['stock_code'],
                'stock_name': item['stock_name'],
                'mention_count': mention_count,
                'win_rate': round(win_rate, 1),
                'avg_return': round(avg_return, 2),
                'group_count': len(groups),
                'groups': list(groups)
            })

        # 3. 排序
        reverse = (order == 'desc')
        key_map = {
            'win_rate': 'win_rate', 
            'avg_return': 'avg_return', 
            'mention_count': 'mention_count',
            'stock_code': 'stock_code'
        }
        sort_key = key_map.get(sort_by, 'win_rate')
        
        try:
            filtered_results.sort(key=lambda x: x.get(sort_key, 0), reverse=reverse)
        except Exception:
            filtered_results.sort(key=lambda x: x['win_rate'], reverse=True)

        # 4. 分页
        total_count = len(filtered_results)
        if page < 1: page = 1
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_data = filtered_results[start_idx:end_idx]

        return {
            'data': paginated_data,
            'total': total_count,
            'page': page,
            'page_size': page_size
        }

    def _get_cached_raw_win_rate(self, min_mentions: int, return_period: str) -> List[Dict]:
        """获取并缓存原始胜率数据（包含所有详细记录以便二次过滤）"""
        cache_key = f'raw_win_rate_{return_period}_{min_mentions}'
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        start_time = time.time()
        groups = self._get_all_group_dbs()
        
        # 并行查询所有群组
        all_rows = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_group = {
                executor.submit(self._fetch_group_win_rate_data, g, return_period): g  # type: ignore
                for g in groups
            }
            for future in concurrent.futures.as_completed(future_to_group):
                try:
                    rows = future.result()
                    all_rows.extend(rows)
                except Exception as e:
                    pass

        # Aggregate data and store details for in-memory processing
        stock_map: Dict[str, StockData] = {}
        
        for r in all_rows:
            # row: (code, name, return, date, group_id)
            if not r: continue
            code, name, ret, date_str, gid = r
            
            if code not in stock_map:
                stock_map[code] = {
                    'stock_name': '', 
                    'detail_returns': [], 
                    'detail_dates': [],
                    'detail_groups': []
                }
            d = stock_map[code]
            
            if not d['stock_name'] or (name and len(name) > len(d['stock_name'])):
                d['stock_name'] = name
            
            d['detail_returns'].append(ret)
            d['detail_dates'].append(date_str)
            d['detail_groups'].append(gid)

        # 转换为列表
        results = []
        for code, d in stock_map.items():
            # 这里先不做 min_mentions 过滤，因为 date filter 后 mention 可能减少
            # 但为了缓存不过大，可以先过滤掉极其冷门的（比如 total < min_mentions）
            # 不过为了准确性，全量保留最稳妥
            
            # 计算无过滤时的默认值
            total = len(d['detail_returns'])
            if total < min_mentions:
                continue
                
            positive = sum(1 for x in d['detail_returns'] if x > 0)
            win_rate = float(positive) / total * 100 if total > 0 else 0.0
            avg_return = float(sum(d['detail_returns'])) / len(d['detail_returns']) if total > 0 else 0.0
            
            results.append({
                'stock_code': code,
                'stock_name': d['stock_name'],
                'mention_count': total,
                'win_rate': round(float(win_rate), 1),
                'avg_return': round(float(avg_return), 2),
                'group_count': len(set(d['detail_groups'])),
                # 保留详情供后续过滤
                'detail_returns': d['detail_returns'],
                'detail_dates': d['detail_dates'],
                'detail_groups': d['detail_groups']
            })

        duration = time.time() - start_time
        log_info(f"Global Win Rate Raw Data Loaded: {len(results)} stocks in {duration:.2f}s")
        
        self._set_cache(cache_key, results)
        return results

    def _fetch_group_win_rate_data(self, group: Dict, return_period: str) -> List[Tuple]:
        """单个群组的数据获取函数 (供线程池调用)"""
        db_path = group['topics_db']
        if not os.path.exists(db_path):
            return []
        
        try:
            conn = self._get_conn(db_path)
            cursor = conn.cursor()
            # 增加 mention_date 查询
            query = f'''
                SELECT sm.stock_code, sm.stock_name, mp.{return_period}, sm.mention_date
                FROM stock_mentions sm
                JOIN mention_performance mp ON sm.id = mp.mention_id
                WHERE mp.{return_period} IS NOT NULL
            '''
            cursor.execute(query)
            rows = cursor.fetchall()
            conn.close()
            
            # 附加 group_id
            return [r + (group['group_id'],) for r in rows]
        except Exception:
            return []

    # ========== 全局股票事件详情 ==========

    def get_global_stock_events(self, stock_code: str) -> Dict[str, Any]:
        """获取某只股票在所有群组的提及事件"""
        # 此方法实时查询，不缓存或短缓存
        all_events = []
        stock_name = ""

        for group in self._get_all_group_dbs():
            db_path = group['topics_db']
            if not os.path.exists(db_path):
                continue
            try:
                conn = self._get_conn(db_path)
                # 获取群名
                group_name = self._get_group_name(conn, group['group_id'])
                
                # 查询事件
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT sm.context_snippet as context, sm.mention_date, sm.stock_name, 
                           mp.return_1d, mp.return_3d, mp.return_5d, mp.return_10d, mp.return_20d
                    FROM stock_mentions sm
                    LEFT JOIN mention_performance mp ON sm.id = mp.mention_id
                    WHERE sm.stock_code = ?
                    ORDER BY sm.mention_date DESC
                ''', (stock_code,))
                
                rows = cursor.fetchall()
                if rows:
                    if not stock_name:
                        stock_name = rows[0][2] # stock_name
                    
                    for row in rows:
                        all_events.append({
                            'group_id': group['group_id'],
                            'group_name': group_name,
                            'context': row[0],
                            'mention_date': row[1],
                            'stock_name': row[2],
                            'return_1d': row[3],
                            'return_3d': row[4],
                            'return_5d': row[5],
                            'return_10d': row[6],
                            'return_20d': row[7]
                        })
                conn.close()
            except Exception as e:
                log_warning(f"查询群组 {group['group_id']} 事件失败: {e}")

        # 按时间倒序排序
        all_events.sort(key=lambda x: x['mention_date'], reverse=True)

        return {
            'stock_code': stock_code,
            'stock_name': stock_name,
            'total_mentions': len(all_events),
            'events': all_events
        }

    # ========== 全局板块热度 ==========

    def get_global_sector_heat(self, start_date: Optional[str] = None) -> List[Dict]:
        """跨群组板块热度聚合 (并行 + 内存过滤)"""
        # 1. 获取Raw Data (只缓存全量)
        raw_data = self._get_cached_raw_sector_data()

        # 2. 内存过滤
        from stock_analyzer import SECTOR_KEYWORDS
        sector_stats: Dict[str, SectorStat] = {} # explicit typed dict

        for item in raw_data:
            # item: {'stock_name':..., 'stock_code':..., 'return_5d':..., 'mention_date':...}
            if start_date and item['mention_date'] < start_date:
                continue
            
            name = item['stock_name']
            code = item['stock_code']
            ret = item['return_5d']
            
            name_lower = name.lower() if name else ''
            
            # Use explicit typed dict for accumulation to avoid lint errors
            if name_lower:
                for sector, keywords in SECTOR_KEYWORDS.items():
                    if any(kw in name_lower for kw in keywords):
                        if sector not in sector_stats:
                            sector_stats[sector] = {
                                'mention_count': 0, 
                                'stocks': set(), 
                                'positive': 0, 
                                'total_with_return': 0
                            }
                        s: SectorStat = sector_stats[sector]
                        s['mention_count'] += 1
                        s['stocks'].add(code)
                            
                        if ret is not None:
                            s['total_with_return'] += 1
                            if ret > 0:
                                s['positive'] += 1
                        break
        
        results = []
        for sector, s in sector_stats.items():
            pos = int(s['positive'])
            total_ret = int(s['total_with_return'])
            win_rate = float(pos) / total_ret * 100 if total_ret > 0 else 0.0
            
            stock_count = len(s['stocks']) if s['stocks'] else 0

            results.append({
                'sector': sector,
                'mention_count': int(s['mention_count']),
                'stock_count': stock_count,
                'win_rate': round(float(win_rate), 1)
            })

        results.sort(key=lambda x: -x['mention_count'])
        return results

    def _get_cached_raw_sector_data(self) -> List[Dict]:
        """缓存全量板块基础数据"""
        cache_key = 'raw_sector_data'
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        start_time = time.time()
        groups = self._get_all_group_dbs()
        
        all_rows = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_group = {
                executor.submit(self._fetch_group_sector_data, g): g  # type: ignore
                for g in groups
            }
            for future in concurrent.futures.as_completed(future_to_group):
                try:
                    all_rows.extend(future.result())
                except Exception:
                    pass

        results = []
        for r in all_rows:
            # r: (name, code, ret, date)
            results.append({
                'stock_name': r[0],
                'stock_code': r[1],
                'return_5d': r[2],
                'mention_date': r[3]
            })

        duration = time.time() - start_time
        log_info(f"Global Sector Raw Data Loaded: {len(results)} mentions in {duration:.2f}s")
        
        self._set_cache(cache_key, results)
        return results

    def _fetch_group_sector_data(self, group: Dict) -> List[Tuple]:
        """获取单个群组板块数据"""
        db_path = group['topics_db']
        if not os.path.exists(db_path):
            return []
        try:
            conn = self._get_conn(db_path)
            cursor = conn.cursor()
            query = '''
                SELECT sm.stock_name, sm.stock_code, mp.return_5d, sm.mention_date
                FROM stock_mentions sm
                LEFT JOIN mention_performance mp ON sm.id = mp.mention_id
            '''
            cursor.execute(query)
            rows = cursor.fetchall()
            conn.close()
            return rows
        except Exception:
            return []

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

        stock_signals: Dict[str, StockSignal] = {} # explicit typed dict

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
                    if code not in stock_signals:
                        stock_signals[code] = {
                            'mentions': 0, 'group_names': set(), 'stock_name': '',
                            'latest_date': '', 'returns': []
                        }
                    s: StockSignal = stock_signals[code]
                    s['mentions'] += 1
                    
                    # 获取群名
                    group_name = self._get_group_name(conn, group['group_id'])
                    s['group_names'].add(group_name)
                    
                    if name:
                        s['stock_name'] = name
                    if str(date) > str(s['latest_date']):
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
            weight = s['mentions'] * len(s['group_names'])
            results.append({
                'stock_code': code,
                'stock_name': s['stock_name'],
                'mention_count': s['mentions'],
                'group_count': len(s['group_names']),
                'groups': list(s['group_names']),
                'latest_date': s['latest_date'],
                'avg_return': round(avg_ret, 2) if avg_ret is not None else None,
                'weight': weight
            })

        results.sort(key=lambda x: -x['weight'])
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

                # 获取群名
                group_name = self._get_group_name(conn, group['group_id'])

                conn.close()

                results.append({
                    'group_id': group['group_id'],
                    'group_name': group_name,
                    'total_topics': topic_count or 0,
                    'latest_topic': latest_time,
                    'total_mentions': mention_count,
                    'unique_stocks': stock_count,
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

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全局分析模块
跨群组聚合股票提及、胜率、板块热度等数据
"""

import sqlite3
import os
import json
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Tuple, Set, TypedDict
from collections import defaultdict
import concurrent.futures
import time

from modules.shared.db_path_manager import get_db_path_manager
from modules.shared.logger_config import log_info, log_warning, log_error
from modules.shared.stock_exclusion import is_excluded_stock
from modules.analyzers.sector_heat import build_topic_time_filter, aggregate_sector_heat, match_sector_keywords

BEIJING_TZ = timezone(timedelta(hours=8))


class StockSignal(TypedDict):
    mentions: int
    group_names: Set[str]
    stock_name: str
    latest_date: str
    returns: List[float]
    positive_returns: int

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
        self._alias_mtime: float = -1.0
        self._alias_to_std: Dict[str, str] = {}
        self._std_to_aliases: Dict[str, Set[str]] = {}

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

    def _get_scan_filter_fingerprint(self) -> str:
        """基于 group_scan_filter 配置生成口径指纹。"""
        try:
            from modules.shared.group_scan_filter import CONFIG_FILE
            if not os.path.exists(CONFIG_FILE):
                return "nofile"
            mtime = os.path.getmtime(CONFIG_FILE)
            with open(CONFIG_FILE, "rb") as f:
                digest = hashlib.sha1(f.read()).hexdigest()[:12]
            return f"{int(mtime)}-{digest}"
        except Exception:
            return "unknown"

    def _scoped_cache_key(self, base: str) -> str:
        return f"{base}|scope:{self._get_scan_filter_fingerprint()}"

    def _get_scoped_group_dbs(self) -> List[Dict]:
        """按白黑名单规则过滤后的群组列表。"""
        groups = self.db_path_manager.list_all_groups()
        try:
            from modules.shared.group_scan_filter import filter_groups
            return filter_groups(groups).get("included_groups", []) or []
        except Exception as e:
            log_warning(f"读取群组过滤规则失败，回退全量群组: {e}")
            return groups

    def _get_all_group_dbs(self) -> List[Dict]:
        """获取当前分析作用域群组信息（默认应用白黑名单）。"""
        return self._get_scoped_group_dbs()

    def _load_stock_aliases(self):
        """加载 stock_aliases.json，并构建别名/标准名双向索引。"""
        alias_file = os.path.join(self.db_path_manager.project_root, "stock_aliases.json")
        if not os.path.exists(alias_file):
            self._alias_mtime = -1.0
            self._alias_to_std = {}
            self._std_to_aliases = {}
            return

        mtime = os.path.getmtime(alias_file)
        if mtime == self._alias_mtime:
            return

        alias_to_std: Dict[str, str] = {}
        std_to_aliases: Dict[str, Set[str]] = defaultdict(set)
        try:
            with open(alias_file, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                for alias, std_name in raw.items():
                    alias_norm = str(alias or "").strip().lower()
                    std_norm = str(std_name or "").strip().lower()
                    if not alias_norm or not std_norm:
                        continue
                    alias_to_std[alias_norm] = std_norm
                    std_to_aliases[std_norm].add(alias_norm)
        except Exception as e:
            log_warning(f"加载股票别名失败: {e}")
            alias_to_std = {}
            std_to_aliases = defaultdict(set)

        self._alias_mtime = mtime
        self._alias_to_std = alias_to_std
        self._std_to_aliases = dict(std_to_aliases)

    def _expand_stock_search_terms(self, keyword: str) -> Set[str]:
        """若搜索词是股票标准名/别名，扩展出同义词集合。"""
        base = str(keyword or "").strip().lower()
        if not base:
            return set()

        self._load_stock_aliases()
        terms: Set[str] = {base}

        std_name = self._alias_to_std.get(base)
        if std_name:
            terms.add(std_name)
            terms.update(self._std_to_aliases.get(std_name, set()))

        if base in self._std_to_aliases:
            terms.update(self._std_to_aliases.get(base, set()))

        return {t for t in terms if t}

    def _get_whitelist_group_ids(self) -> List[str]:
        """读取全局扫描白名单群组 ID 列表"""
        try:
            from modules.shared.group_scan_filter import get_filter_config
            cfg = get_filter_config()
            values = cfg.get("whitelist_group_ids", []) if isinstance(cfg, dict) else []
            if not isinstance(values, list):
                return []
            return [str(v).strip() for v in values if str(v).strip()]
        except Exception:
            return []

    def _get_conn(self, db_path: str):
        """获取带 WAL 模式和超时的数据库连接"""
        conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout=30000')

        # 旧库可能缺少股票相关表，这里容错创建，避免查询报错
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_mentions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_id INTEGER NOT NULL,
                stock_code TEXT NOT NULL,
                stock_name TEXT NOT NULL,
                mention_date TEXT NOT NULL,
                mention_time TEXT NOT NULL,
                context_snippet TEXT,
                sentiment TEXT DEFAULT 'neutral',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS mention_performance (
                mention_id INTEGER PRIMARY KEY,
                stock_code TEXT NOT NULL,
                mention_date TEXT NOT NULL,
                price_at_mention REAL,
                return_1d REAL,
                return_3d REAL,
                return_5d REAL,
                return_10d REAL,
                return_20d REAL,
                return_60d REAL,
                return_120d REAL,
                return_250d REAL,
                excess_return_1d REAL,
                excess_return_3d REAL,
                excess_return_5d REAL,
                excess_return_10d REAL,
                excess_return_20d REAL,
                excess_return_60d REAL,
                excess_return_120d REAL,
                excess_return_250d REAL,
                max_return REAL,
                max_drawdown REAL,
                freeze_level INTEGER DEFAULT 0
            )
        ''')
        conn.commit()
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
        """获取群组名称（优先 topics DB，其次 group_meta.json）"""
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT name FROM groups WHERE group_id = ?', (group_id,))
            row = cursor.fetchone()
            if row and row[0]:
                name = str(row[0]).strip()
                if name and name != str(group_id):
                    return name
        except Exception:
            pass

        try:
            group_dir = self.db_path_manager.get_group_data_dir(str(group_id))
            meta_path = group_dir / "group_meta.json"
            if meta_path.exists():
                with meta_path.open("r", encoding="utf-8") as f:
                    meta = json.load(f)
                meta_name = str(meta.get("name", "")).strip()
                if meta_name and meta_name != str(group_id):
                    return meta_name
        except Exception:
            pass
        return str(group_id)

    # ========== 全局统计 ==========

    def _parse_mention_datetime(self, mention_time: Any, mention_date: Any) -> Optional[datetime]:
        """尽量将提及时间解析为北京时间 datetime，失败返回 None。"""
        if isinstance(mention_time, str) and mention_time.strip():
            raw = mention_time.strip()
            try:
                if raw.endswith('+0800'):
                    raw = raw[:-5] + '+08:00'
                dt = datetime.fromisoformat(raw)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=BEIJING_TZ)
                else:
                    dt = dt.astimezone(BEIJING_TZ)
                return dt
            except Exception:
                pass

        if isinstance(mention_date, str) and mention_date.strip():
            raw_date = mention_date.strip()[:10]
            try:
                d = datetime.strptime(raw_date, "%Y-%m-%d")
                return d.replace(tzinfo=BEIJING_TZ)
            except Exception:
                return None
        return None

    def _collect_hot_words_for_window(self, window_hours: int, limit: int, normalize: bool) -> Dict[str, Any]:
        now = datetime.now(BEIJING_TZ)
        cutoff = now - timedelta(hours=window_hours)
        cutoff_date = cutoff.strftime('%Y-%m-%d')

        word_counts: Dict[str, int] = defaultdict(int)
        # 同一群组内同一话题下同一股票仅计一次，避免重复提及抬高热度
        seen_topic_stock: set[tuple[str, Any, str]] = set()
        query = '''
            SELECT topic_id, stock_code, stock_name, mention_time, mention_date
            FROM stock_mentions
            WHERE mention_date >= ? AND stock_name != '' AND stock_name IS NOT NULL
        '''
        for group in self._get_all_group_dbs():
            group_id = str(group.get('group_id', '')).strip()
            db_path = group.get('topics_db')
            if not group_id or not db_path or not os.path.exists(db_path):
                continue

            conn = None
            try:
                conn = self._get_conn(db_path)
                cursor = conn.cursor()
                cursor.execute(query, (cutoff_date,))
                rows = cursor.fetchall()

                for row in rows:
                    topic_id = row[0] if len(row) > 0 else None
                    stock_code = row[1] if len(row) > 1 else None
                    stock_name = row[2] if len(row) > 2 else None
                    mention_time = row[3] if len(row) > 3 else None
                    mention_date = row[4] if len(row) > 4 else None

                    dt = self._parse_mention_datetime(mention_time, mention_date)
                    if dt is None or dt < cutoff:
                        continue
                    if is_excluded_stock(None, stock_name):
                        continue

                    dedup_key = (
                        group_id,
                        topic_id,
                        str(stock_code or stock_name or '').strip().upper(),
                    )
                    if dedup_key in seen_topic_stock:
                        continue
                    seen_topic_stock.add(dedup_key)
                    word_counts[str(stock_name)] += 1
            except Exception as e:
                log_warning(f"热词统计失败(group={group_id}): {e}")
            finally:
                if conn:
                    conn.close()

        words: List[Dict[str, Any]] = []
        all_points_total = int(sum(word_counts.values()))
        factor = (24.0 / float(window_hours)) if normalize and window_hours > 0 else 1.0
        for name, raw_count in word_counts.items():
            normalized_count = round(raw_count * factor, 2)
            value = normalized_count if normalize else raw_count
            words.append({
                "name": name,
                "value": value,
                "raw_count": int(raw_count),
                "normalized_count": normalized_count,
            })

        words.sort(key=lambda x: (x["value"], x["raw_count"]), reverse=True)
        words = words[:max(1, int(limit or 50))]

        return {
            "words": words,
            "data_points_total": all_points_total,
            "time_range": {
                "start_at": cutoff.isoformat(),
                "end_at": now.isoformat(),
            },
        }

    def get_global_hot_words(
        self,
        days: int = 1,
        limit: int = 50,
        force_refresh: bool = False,
        window_hours: Optional[int] = None,
        normalize: bool = True,
        fallback: bool = True,
        fallback_windows: Optional[List[int]] = None
    ) -> Dict[str, Any]:
        """按滑动小时窗口统计全局热词，支持归一化与自动回退。"""
        requested_window = int(window_hours or (int(days or 1) * 24))
        limit = max(1, int(limit or 50))
        normalize = bool(normalize)
        fallback = bool(fallback)
        fallback_windows = fallback_windows or [24, 36, 48, 168]

        windows_to_try: List[int] = [requested_window]
        if fallback:
            for w in fallback_windows:
                if w != requested_window:
                    windows_to_try.append(int(w))

        cache_key = self._scoped_cache_key(
            f"global_hot_words_wh_{requested_window}_limit_{limit}_norm_{int(normalize)}_fb_{int(fallback)}_fws_{','.join(str(w) for w in windows_to_try)}"
        )
        if force_refresh:
            if cache_key in self._cache:
                del self._cache[cache_key]
            if cache_key in self._cache_time:
                del self._cache_time[cache_key]

        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        chosen_window = requested_window
        chosen_payload: Optional[Dict[str, Any]] = None
        fallback_applied = False
        fallback_reason = ""

        for idx, wh in enumerate(windows_to_try):
            payload = self._collect_hot_words_for_window(wh, limit, normalize)
            if payload.get("words"):
                chosen_window = wh
                chosen_payload = payload
                fallback_applied = idx > 0
                if fallback_applied:
                    fallback_reason = f"窗口 {requested_window}h 无数据，自动回退到 {wh}h"
                break

        if chosen_payload is None:
            chosen_payload = self._collect_hot_words_for_window(requested_window, limit, normalize)
            fallback_reason = f"窗口 {requested_window}h 及候选窗口均无数据"

        result = {
            "words": chosen_payload.get("words", []),
            "window_hours_requested": requested_window,
            "window_hours_effective": chosen_window,
            "fallback_applied": fallback_applied,
            "fallback_reason": fallback_reason if fallback_reason else None,
            "data_points_total": chosen_payload.get("data_points_total", 0),
            "time_range": chosen_payload.get("time_range", {}),
        }
        self._set_cache(cache_key, result)
        return result


    def get_global_stats(self) -> Dict[str, Any]:
        """全局统计概览"""
        cache_key = self._scoped_cache_key('global_stats')
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
                            end_date: Optional[str] = None,
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
            if is_excluded_stock(item.get('stock_code'), item.get('stock_name')):
                continue
            # 检查日期过滤
            # raw_data item 结构: {'stock_code':..., 'returns': [r1, r2...], 'dates': [d1, d2...]}
            # 注意: 为了支持 date filter，我们需要在 raw data 里保留 mention_date
            # 如果 start_date 存在，我们需要重新计算该股票在范围内的 win_rate
            
            if not start_date and not end_date:
                # 无时间过滤，直接使用预计算好的统计值
                filtered_results.append(item)
                continue

            # 有时间过滤：需要重新聚合该股票的 returns
            valid_returns = []
            valid_benchmark_returns = []
            mention_count = 0
            groups = set()
            
            for ret, date_str, gid, benchmark_ret in zip(
                item.get('detail_returns', []),
                item.get('detail_dates', []),
                item.get('detail_groups', []),
                item.get('detail_benchmark_returns', []),
            ):
                if start_date and date_str < start_date:
                    continue
                if end_date and date_str > end_date:
                    continue
                if ret is None:
                    continue
                valid_returns.append(ret)
                mention_count += 1
                groups.add(gid)
                if benchmark_ret is not None:
                    valid_benchmark_returns.append(benchmark_ret)
            
            if mention_count < min_mentions:
                continue

            positive = sum(1 for r in valid_returns if r > 0)
            win_rate = positive / mention_count * 100 if mention_count > 0 else 0.0
            avg_return = sum(valid_returns) / len(valid_returns) if valid_returns else 0.0
            avg_benchmark_return = (
                sum(valid_benchmark_returns) / len(valid_benchmark_returns)
                if valid_benchmark_returns
                else 0.0
            )

            filtered_results.append({
                'stock_code': item['stock_code'],
                'stock_name': item['stock_name'],
                'mention_count': mention_count,
                'total_mentions': mention_count,
                'win_rate': round(win_rate, 1),
                'avg_return': round(avg_return, 2),
                'avg_benchmark_return': round(avg_benchmark_return, 2),
                'latest_mention': item['latest_mention'],
                'group_count': len(groups),
                'groups': list(groups)
            })

        # 3. 排序
        reverse = (order == 'desc')
        key_map = {
            'win_rate': 'win_rate', 
            'avg_return': 'avg_return', 
            'mention_count': 'mention_count',
            'stock_code': 'stock_code',
            'avg_benchmark_return': 'avg_benchmark_return',
            'latest_mention': 'latest_mention'
        }
        sort_key = key_map.get(sort_by, 'win_rate')
        
        try:
            filtered_results.sort(key=lambda x: x.get(sort_key, 0) if x.get(sort_key) is not None else 0, reverse=reverse)
        except Exception:
            filtered_results.sort(key=lambda x: x['win_rate'], reverse=True)

        # 4. 分页
        total_count = len(filtered_results)
        # 若设置了 limit，则对总数和分页窗口进行裁剪，避免翻页不生效
        if limit and limit > 0:
            total_count = min(total_count, limit)

        if page < 1:
            page = 1
        start_idx = (page - 1) * page_size
        if start_idx >= total_count:
            paginated_data = []
        else:
            end_idx = min(start_idx + page_size, total_count)
            paginated_data = filtered_results[start_idx:end_idx]

        return {
            'data': paginated_data,
            'total': total_count,
            'page': page,
            'page_size': page_size
        }

    def _get_cached_raw_win_rate(self, min_mentions: int, return_period: str) -> List[Dict]:
        """获取并缓存原始胜率数据（包含所有详细记录以便二次过滤）"""
        cache_key = self._scoped_cache_key(f'raw_win_rate_{return_period}_{min_mentions}')
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
                    g = future_to_group.get(future, {})
                    log_warning(f"读取群组胜率数据失败: group={g.get('group_id')}, error={e}")

        # Aggregate data and store details for in-memory processing
        stock_map: Dict[str, StockData] = {}
        
        for r in all_rows:
            # row: (code, name, return, date, excess_ret, group_id)
            if not r: continue
            code, name, ret, date_str, excess_ret, gid = r
            
            if code not in stock_map:
                stock_map[code] = {
                    'stock_name': '', 
                    'latest_mention': '',
                    'detail_returns': [], 
                    'detail_dates': [],
                    'detail_groups': [],
                    'detail_benchmark_returns': []
                } # type: ignore
            d = stock_map[code]
            
            if not d['stock_name'] or (name and len(name) > len(d['stock_name'])):
                d['stock_name'] = name
            
            if not d['latest_mention'] or date_str > d['latest_mention']: # type: ignore
                d['latest_mention'] = date_str # type: ignore
            
            # 仅保留数值收益，避免 None/脏数据污染后续聚合
            if ret is None:
                continue
            ret = float(ret)
            d['detail_returns'].append(ret)
            d['detail_dates'].append(date_str)
            d['detail_groups'].append(gid)
            d['detail_benchmark_returns'].append((ret - excess_ret) if (ret is not None and excess_ret is not None) else None) # type: ignore

        # 转换为列表
        results = []
        for code, d in stock_map.items():
            # 这里先不做 min_mentions 过滤，因为 date filter 后 mention 可能减少
            # 但为了缓存不过大，可以先过滤掉极其冷门的（比如 total < min_mentions）
            # 不过为了准确性，全量保留最稳妥
            
            # 计算无过滤时的默认值
            valid_returns = [x for x in d['detail_returns'] if x is not None]
            total = len(valid_returns)
            if total < min_mentions:
                continue
            if is_excluded_stock(code, d.get('stock_name')):
                continue
                
            positive = sum(1 for x in valid_returns if x > 0)
            win_rate = float(positive) / total * 100 if total > 0 else 0.0
            avg_return = float(sum(valid_returns)) / len(valid_returns) if total > 0 else 0.0
            avg_benchmark_return = float(sum([x for x in d['detail_benchmark_returns'] if x is not None])) / len([x for x in d['detail_benchmark_returns'] if x is not None]) if len([x for x in d['detail_benchmark_returns'] if x is not None]) > 0 else 0.0 # type: ignore
            
            results.append({
                'stock_code': code,
                'stock_name': d['stock_name'],
                'mention_count': total,
                'total_mentions': total,
                'win_rate': round(float(win_rate), 1),
                'avg_return': round(float(avg_return), 2),
                'avg_benchmark_return': round(float(avg_benchmark_return), 2),
                'latest_mention': d['latest_mention'], # type: ignore
                'group_count': len(set(d['detail_groups'])),
                # 保留详情供后续过滤
                'detail_returns': d['detail_returns'],
                'detail_dates': d['detail_dates'],
                'detail_groups': d['detail_groups'],
                'detail_benchmark_returns': d['detail_benchmark_returns'] # type: ignore
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
            excess_col = return_period.replace('return_', 'excess_return_')
            # 增加 mention_date, excess_col 查询
            query = f'''
                SELECT sm.stock_code, sm.stock_name, mp.{return_period}, sm.mention_date, mp.{excess_col}
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
        """获取某只股票在所有群组的提及事件（含完整话题文本 + 关联股票）"""
        # 此方法实时查询，不缓存或短缓存
        if is_excluded_stock(stock_code, None):
            return {
                'stock_code': stock_code,
                'stock_name': '',
                'total_mentions': 0,
                'events': []
            }

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
                
                # 查询事件 + 话题全文
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT sm.id, sm.topic_id, sm.context_snippet as context,
                           sm.mention_date, sm.mention_time, sm.stock_name,
                           mp.return_1d, mp.return_3d, mp.return_5d, mp.return_10d, mp.return_20d,
                           tk.text as full_text
                    FROM stock_mentions sm
                    LEFT JOIN mention_performance mp ON sm.id = mp.mention_id
                    LEFT JOIN talks tk ON sm.topic_id = tk.topic_id
                    WHERE sm.stock_code = ?
                    ORDER BY sm.mention_time DESC
                ''', (stock_code,))
                
                rows = cursor.fetchall()
                if rows:
                    if not stock_name:
                        stock_name = rows[0][5]  # stock_name

                    # 批量查询该群组中这些 topic 下的关联股票
                    topic_ids = list(set(row[1] for row in rows if row[1]))
                    stocks_by_topic: Dict[int, List[Dict[str, str]]] = {}
                    if topic_ids:
                        placeholders = ','.join('?' * len(topic_ids))
                        cursor.execute(f'''
                            SELECT topic_id, stock_code, stock_name
                            FROM stock_mentions
                            WHERE topic_id IN ({placeholders})
                            ORDER BY mention_time DESC
                        ''', topic_ids)
                        seen: Dict[int, set] = {}
                        for r in cursor.fetchall():
                            tid = r[0]
                            code = r[1]
                            if is_excluded_stock(code, r[2]):
                                continue
                            if tid not in stocks_by_topic:
                                stocks_by_topic[tid] = []
                                seen[tid] = set()
                            if code not in seen[tid]:
                                seen[tid].add(code)
                                stocks_by_topic[tid].append({
                                    'stock_code': code,
                                    'stock_name': r[2],
                                })

                    for row in rows:
                        if is_excluded_stock(stock_code, row[5]):
                            continue
                        full_text = row[11] or ''
                        text_snippet = full_text[:500] + ('...' if len(full_text) > 500 else '')
                        topic_id = row[1]
                        all_events.append({
                            'mention_id': row[0],
                            'topic_id': str(topic_id) if topic_id is not None else None,
                            'group_id': group['group_id'],
                            'group_name': group_name,
                            'context': row[2],
                            'full_text': full_text,
                            'text_snippet': text_snippet,
                            'stocks': stocks_by_topic.get(topic_id, []),
                            'mention_date': row[3],
                            'mention_time': row[4],
                            'stock_name': row[5],
                            'return_1d': row[6],
                            'return_3d': row[7],
                            'return_5d': row[8],
                            'return_10d': row[9],
                            'return_20d': row[10]
                        })
                conn.close()
            except Exception as e:
                log_warning(f"查询群组 {group['group_id']} 事件失败: {e}")

        # 按时间倒序排序
        all_events.sort(key=lambda x: x.get('mention_time') or x.get('mention_date') or '', reverse=True)

        return {
            'stock_code': stock_code,
            'stock_name': stock_name,
            'total_mentions': len(all_events),
            'events': all_events
        }

    # ========== 全局板块热度 ==========

    def get_global_sector_heat(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict]:
        """跨群组板块热度聚合（按帖子文本计数）。"""
        from modules.analyzers.stock_analyzer import SECTOR_KEYWORDS
        cache_key = self._scoped_cache_key(
            f'global_sector_heat_posts_v2_{start_date or ""}_{end_date or ""}'
        )
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        start_at = time.time()
        groups = self._get_all_group_dbs()
        merged_daily: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        merged_total: Dict[str, int] = defaultdict(int)
        scanned_topics = 0
        matched_mentions = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(
                    self._compute_group_sector_heat,
                    group,
                    start_date,
                    end_date,
                    SECTOR_KEYWORDS,
                )
                for group in groups
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    payload = future.result()
                    scanned_topics += payload['scanned_topics']
                    matched_mentions += payload['matched_mentions']
                    for sector, total in payload['sector_total'].items():
                        merged_total[sector] += int(total)
                    for sector, day_map in payload['sector_daily'].items():
                        for date_key, count in day_map.items():
                            merged_daily[sector][date_key] += int(count)
                except Exception as e:
                    log_warning(f"全局板块热度聚合任务失败: {e}")

        results: List[Dict[str, Any]] = []
        for sector, total in merged_total.items():
            daily_map = dict(sorted(merged_daily[sector].items()))
            peak_date = None
            peak_count = 0
            if daily_map:
                peak_date, peak_count = max(daily_map.items(), key=lambda kv: kv[1])
            results.append({
                'sector': sector,
                # aliases for legacy page and StockDashboard
                'count': int(total),
                'total_mentions': int(total),
                'mention_count': int(total),
                'stocks': [],
                'stock_count': 0,
                'peak_date': peak_date,
                'peak_count': int(peak_count),
                'daily_mentions': daily_map,
                'win_rate': 0.0,
            })

        results.sort(key=lambda x: (-x['mention_count'], x['sector']))
        duration = time.time() - start_at
        log_info(
            "Global sector heat computed "
            f"(groups={len(groups)}, topics={scanned_topics}, matched={matched_mentions}, "
            f"sectors={len(results)}, start={start_date or '-'}, end={end_date or '-'}, "
            f"duration={duration:.2f}s)"
        )
        self._set_cache(cache_key, results)
        return results

    def _compute_group_sector_heat(
        self,
        group: Dict[str, Any],
        start_date: Optional[str],
        end_date: Optional[str],
        sector_keywords: Dict[str, List[str]],
    ) -> Dict[str, Any]:
        db_path = group.get('topics_db')
        if not db_path or not os.path.exists(db_path):
            return {
                'scanned_topics': 0,
                'matched_mentions': 0,
                'sector_total': {},
                'sector_daily': {},
            }

        try:
            conn = self._get_conn(db_path)
            cursor = conn.cursor()
            date_clause, params = build_topic_time_filter(
                start_date=start_date,
                end_date=end_date,
                column='t.create_time',
            )
            cursor.execute(f'''
                SELECT tk.text, t.create_time
                FROM topics t
                JOIN talks tk ON t.topic_id = tk.topic_id
                WHERE tk.text IS NOT NULL AND tk.text != ''
                {date_clause}
            ''', params)
            rows = cursor.fetchall()
            conn.close()
        except Exception as e:
            log_warning(f"读取群组板块热度失败(group={group.get('group_id')}): {e}")
            return {
                'scanned_topics': 0,
                'matched_mentions': 0,
                'sector_total': {},
                'sector_daily': {},
            }

        group_heat = aggregate_sector_heat(rows, sector_keywords)
        sector_total = {item['sector']: int(item['total_mentions']) for item in group_heat}
        sector_daily = {item['sector']: item['daily_mentions'] for item in group_heat}
        matched_mentions = sum(sector_total.values())
        return {
            'scanned_topics': len(rows),
            'matched_mentions': matched_mentions,
            'sector_total': sector_total,
            'sector_daily': sector_daily,
        }

    # ========== 全局信号雷达 ==========

    def get_global_signals(self, lookback_days: int = 7,
                           min_mentions: int = 2,
                           start_date: Optional[str] = None,
                           end_date: Optional[str] = None) -> List[Dict]:
        """
        跨群共识信号：多个群同时提及的股票 = 更高权重
        """
        cache_key = self._scoped_cache_key(f'signals_{lookback_days}_{min_mentions}_{start_date or ""}_{end_date or ""}')
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        since_date = start_date or (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        stock_signals: Dict[str, StockSignal] = {} # explicit typed dict

        for group in self._get_all_group_dbs():
            db_path = group['topics_db']
            if not os.path.exists(db_path):
                continue
            try:
                conn = self._get_conn(db_path)
                cursor = conn.cursor()
                query = '''
                    SELECT sm.stock_code, sm.stock_name, sm.mention_date, mp.return_5d
                    FROM stock_mentions sm
                    LEFT JOIN mention_performance mp ON sm.id = mp.mention_id
                    WHERE sm.mention_date >= ?
                '''
                params: List[Any] = [since_date]
                if end_date:
                    query += ' AND sm.mention_date <= ?'
                    params.append(end_date)
                query += '''
                    ORDER BY sm.mention_date DESC
                '''
                cursor.execute(query, params)
                for code, name, date, ret in cursor.fetchall():
                    if is_excluded_stock(code, name):
                        continue
                    if code not in stock_signals:
                        stock_signals[code] = {
                            'mentions': 0, 'group_names': set(), 'stock_name': '',
                            'latest_date': '', 'returns': [], 'positive_returns': 0
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
                        if ret > 0:
                            s['positive_returns'] += 1
                conn.close()
            except Exception:
                pass

        results = []
        for code, s in stock_signals.items():
            if s['mentions'] < min_mentions:
                continue
            avg_ret = sum(s['returns']) / len(s['returns']) if s['returns'] else None
            historical_win_rate = (s['positive_returns'] / len(s['returns']) * 100) if s['returns'] else None
            # 权重 = 提及次数 × 群组数（跨群共识加权）
            weight = s['mentions'] * len(s['group_names'])
            results.append({
                'stock_code': code,
                'stock_name': s['stock_name'],
                'mention_count': s['mentions'],
                'recent_mentions': s['mentions'],
                'group_count': len(s['group_names']),
                'groups': list(s['group_names']),
                'latest_mention': s['latest_date'],
                'latest_date': s['latest_date'],
                'avg_return': round(avg_ret, 2) if avg_ret is not None else None,
                'historical_avg_return': round(avg_ret, 2) if avg_ret is not None else None,
                'historical_win_rate': round(historical_win_rate, 1) if historical_win_rate is not None else None,
                'weight': weight
            })

        results.sort(key=lambda x: -x['weight'])
        self._set_cache(cache_key, results)
        return results

    def get_global_sector_topics(self, sector: str, start_date: Optional[str] = None,
                                 end_date: Optional[str] = None, page: int = 1,
                                 page_size: int = 20) -> Dict[str, Any]:
        """全局板块话题明细（跨群组）"""
        from modules.analyzers.stock_analyzer import SECTOR_KEYWORDS

        if sector not in SECTOR_KEYWORDS:
            raise ValueError(f"未知板块: {sector}")

        page = max(1, int(page or 1))
        page_size = max(1, min(int(page_size or 20), 100))

        matched_topics: List[Dict[str, Any]] = []
        for group in self._get_all_group_dbs():
            db_path = group['topics_db']
            if not os.path.exists(db_path):
                continue

            try:
                conn = self._get_conn(db_path)
                cursor = conn.cursor()
                group_name = self._get_group_name(conn, group['group_id'])

                date_clause, params = build_topic_time_filter(
                    start_date=start_date,
                    end_date=end_date,
                    column='t.create_time',
                )
                cursor.execute(f'''
                    SELECT
                        t.topic_id,
                        t.create_time,
                        tk.text as talk_text
                    FROM topics t
                    JOIN talks tk ON t.topic_id = tk.topic_id
                    WHERE tk.text IS NOT NULL AND tk.text != ''
                    {date_clause}
                    ORDER BY t.create_time DESC
                ''', params)
                rows = cursor.fetchall()

                topic_ids: List[str] = []
                topic_texts: Dict[str, str] = {}
                for row in rows:
                    topic_id = str(row[0])
                    full_text = row[2] or ''
                    if not full_text:
                        continue
                    topic_ids.append(topic_id)
                    topic_texts[topic_id] = full_text

                stocks_by_topic: Dict[str, List[Dict[str, str]]] = {}
                if topic_ids:
                    placeholders = ','.join('?' * len(topic_ids))
                    cursor.execute(f'''
                        SELECT topic_id, stock_code, stock_name
                        FROM stock_mentions
                        WHERE topic_id IN ({placeholders})
                        ORDER BY mention_time DESC
                    ''', topic_ids)
                    for topic_id_raw, stock_code, stock_name in cursor.fetchall():
                        topic_id_str = str(topic_id_raw)
                        if is_excluded_stock(stock_code, stock_name):
                            continue
                        if topic_id_str not in stocks_by_topic:
                            stocks_by_topic[topic_id_str] = []
                        if stock_code not in [x['stock_code'] for x in stocks_by_topic[topic_id_str]]:
                            stocks_by_topic[topic_id_str].append({
                                'stock_code': stock_code,
                                'stock_name': stock_name
                            })

                for row in rows:
                    topic_id_str = str(row[0])
                    create_time = row[1]
                    full_text = topic_texts.get(topic_id_str, '')
                    if not full_text:
                        continue

                    matches = match_sector_keywords(full_text, SECTOR_KEYWORDS)
                    matched_keywords = matches.get(sector, [])
                    if not matched_keywords:
                        continue

                    matched_topics.append({
                        'group_id': int(group['group_id']),
                        'group_name': group_name,
                        'topic_id': topic_id_str,
                        'create_time': create_time,
                        'full_text': full_text,
                        'text_snippet': full_text[:280] + ('...' if len(full_text) > 280 else ''),
                        'matched_keywords': matched_keywords,
                        'stocks': stocks_by_topic.get(topic_id_str, []),
                    })
                conn.close()
            except Exception as e:
                log_warning(f"全局板块话题查询失败(group={group.get('group_id')}): {e}")

        matched_topics.sort(key=lambda x: x.get('create_time') or '', reverse=True)
        total = len(matched_topics)
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        return {
            'total': total,
            'page': page,
            'page_size': page_size,
            'items': matched_topics[start_idx:end_idx]
        }

    # ========== 群组概览 ==========

    def get_groups_overview(self) -> List[Dict]:
        """各群组摘要统计"""
        cache_key = self._scoped_cache_key('groups_overview')
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

        results.sort(key=lambda x: x.get('latest_topic') or '', reverse=True)
        self._set_cache(cache_key, results)
        return results

    def get_whitelist_topic_mentions(self, page: int = 1, per_page: int = 20, search: Optional[str] = None) -> Dict[str, Any]:
        """聚合白名单群组内的话题列表（不依赖股票分析结果）"""
        page = max(1, int(page or 1))
        per_page = max(1, min(100, int(per_page or 20)))
        search_text = (search or '').strip().lower()
        search_terms = self._expand_stock_search_terms(search_text) if search_text else set()
        if search_text and not search_terms:
            search_terms = {search_text}

        whitelist_ids = set(self._get_whitelist_group_ids())
        if not whitelist_ids:
            return {
                "total": 0,
                "page": page,
                "per_page": per_page,
                "scope": "whitelist",
                "whitelist_group_count": 0,
                "items": []
            }

        all_topics: List[Dict[str, Any]] = []
        for group in self._get_all_group_dbs():
            group_id = str(group.get("group_id", "")).strip()
            if not group_id or group_id not in whitelist_ids:
                continue

            db_path = group.get("topics_db")
            if not db_path or not os.path.exists(db_path):
                continue

            conn = None
            try:
                conn = self._get_conn(db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                group_name = self._get_group_name(conn, group_id)

                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                table_names = {str(row[0]) for row in cursor.fetchall()}
                has_talks = "talks" in table_names
                has_questions = "questions" in table_names
                has_answers = "answers" in table_names
                has_mentions = "stock_mentions" in table_names

                cursor.execute('''
                    SELECT topic_id, create_time, type, title, comments_count, likes_count, reading_count
                    FROM topics
                    ORDER BY create_time DESC
                ''')

                topic_rows = cursor.fetchall()
                for row in topic_rows:
                    topic_id = str(row["topic_id"])
                    topic_text = ""
                    if has_talks:
                        cursor.execute("SELECT text FROM talks WHERE topic_id = ? LIMIT 1", (row["topic_id"],))
                        talk_row = cursor.fetchone()
                        if talk_row and talk_row[0]:
                            topic_text = str(talk_row[0])

                    if not topic_text and has_questions:
                        cursor.execute("SELECT text FROM questions WHERE topic_id = ? LIMIT 1", (row["topic_id"],))
                        q_row = cursor.fetchone()
                        q_text = str(q_row[0]) if (q_row and q_row[0]) else ""
                        a_text = ""
                        if has_answers:
                            cursor.execute("SELECT text FROM answers WHERE topic_id = ? LIMIT 1", (row["topic_id"],))
                            a_row = cursor.fetchone()
                            a_text = str(a_row[0]) if (a_row and a_row[0]) else ""
                        topic_text = (q_text + ("\n" + a_text if a_text else "")).strip()

                    if not topic_text:
                        topic_text = str(row["title"] or "").strip()

                    mention_search_blob = ""
                    if search_terms and has_mentions:
                        cursor.execute('''
                            SELECT stock_code, stock_name
                            FROM stock_mentions
                            WHERE topic_id = ?
                        ''', (row["topic_id"],))
                        mention_search_blob = "\n".join(
                            f"{m[0] or ''} {m[1] or ''}" for m in cursor.fetchall() if not is_excluded_stock(m[0], m[1])
                        )

                    haystack = f"{topic_id}\n{group_name}\n{row['title'] or ''}\n{topic_text}\n{mention_search_blob}".lower()
                    if search_terms and not any(term in haystack for term in search_terms):
                        continue

                    mentions: List[Dict[str, Any]] = []
                    latest_mention = row["create_time"]
                    if has_mentions:
                        cursor.execute('''
                            SELECT sm.stock_code, sm.stock_name,
                                   mp.return_1d, mp.return_3d, mp.return_5d, mp.return_10d, mp.return_20d,
                                   mp.max_return, sm.mention_time
                            FROM stock_mentions sm
                            LEFT JOIN mention_performance mp ON sm.id = mp.mention_id
                            WHERE sm.topic_id = ?
                            ORDER BY sm.mention_time DESC
                        ''', (row["topic_id"],))
                        raw_mentions = [dict(m) for m in cursor.fetchall() if not is_excluded_stock(m[0], m[1])]
                        if raw_mentions:
                            latest_mention = raw_mentions[0].get("mention_time") or latest_mention
                            mentions = [
                                {k: v for k, v in m.items() if k != "mention_time"}
                                for m in raw_mentions
                            ]

                    all_topics.append({
                        "group_id": group_id,
                        "group_name": group_name,
                        "topic_id": str(topic_id) if topic_id is not None else None,
                        "create_time": row["create_time"],
                        "latest_mention": latest_mention,
                        "type": row["type"],
                        "title": row["title"],
                        "comments_count": row["comments_count"] or 0,
                        "likes_count": row["likes_count"] or 0,
                        "reading_count": row["reading_count"] or 0,
                        "text": topic_text,
                        "mentions": mentions,
                    })
            except Exception as e:
                log_warning(f"白名单话题聚合失败(group={group_id}): {e}")
            finally:
                if conn:
                    conn.close()

        all_topics.sort(key=lambda x: x.get("latest_mention") or x.get("create_time") or '', reverse=True)
        total = len(all_topics)
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page

        return {
            "total": total,
            "page": page,
            "per_page": per_page,
            "scope": "whitelist",
            "whitelist_group_count": len(whitelist_ids),
            "items": all_topics[start_idx:end_idx]
        }


# 全局单例
_global_analyzer_instance = None


def get_global_analyzer() -> GlobalAnalyzer:
    """获取全局分析器单例"""
    global _global_analyzer_instance
    if _global_analyzer_instance is None:
        _global_analyzer_instance = GlobalAnalyzer()
    return _global_analyzer_instance

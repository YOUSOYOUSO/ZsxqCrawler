#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票舆情分析模块
从知识星球帖子中提取股票名称，结合A股行情数据进行事件研究分析
"""

import re
import sqlite3
import time
import json
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

import ahocorasick
import akshare as ak

from db_path_manager import get_db_path_manager
from logger_config import log_info, log_warning, log_error, log_debug
from stock_exclusion import is_excluded_stock, build_sql_exclusion_clause
from sector_heat import build_topic_time_filter, aggregate_sector_heat


# ========== 常量 ==========

# 板块关键词映射
SECTOR_KEYWORDS = {
    "AI应用": ["ai应用", "大模型", "deepseek", "chatgpt", "gpt", "通义", "文心", "智谱", "glm"],
    "AI算力": ["算力", "光模块", "cpo", "中际旭创", "新易盛", "天孚通信", "光通信"],
    "商业航天": ["商业航天", "火箭", "卫星", "航天", "星链", "低轨"],
    "机器人": ["机器人", "人形机器人", "宇树", "特斯拉机器人", "optimus"],
    "半导体": ["半导体", "芯片", "晶圆", "封测", "光刻", "先进封装", "国产芯片"],
    "新能源": ["光伏", "锂电", "储能", "新能源", "风电", "氢能"],
    "涨价链": ["涨价", "提价", "涨价函", "涨价逻辑"],
    "军工": ["军工", "国防", "导弹", "无人机"],
    "医药": ["医药", "创新药", "cxo", "生物医药"],
    "消费": ["消费", "白酒", "食饮", "调味品", "预调酒"],
    "地产": ["地产", "房地产", "二手房", "新房", "保租房"],
}

# 需要过滤的常见误匹配词
EXCLUDE_WORDS = frozenset([
    "中国", "美国", "日本", "韩国", "欧洲", "全球", "香港",
    "上海", "北京", "深圳", "广州", "杭州",
    "公司", "集团", "科技", "股份", "电子", "信息", "通信",
    "银行", "证券", "保险",
    "市场", "行业", "板块",
    "大家", "今天", "明天", "昨天", "今年", "去年", "明年",
    "第一", "第二", "第三",
    "核心", "龙头", "趋势",
])

# 北京时区
BEIJING_TZ = timezone(timedelta(hours=8))


class StockAnalyzer:
    """股票舆情分析引擎"""
    # 进程级字典缓存，避免每次任务重复构建
    _dict_lock = threading.RLock()
    _global_automaton = None
    _global_stock_dict: Dict[str, str] = {}
    _global_name_to_code: Dict[str, str] = {}
    _global_built_at: float = 0.0

    # 进程级表初始化缓存，避免多个实例重复建表/漏建表
    _init_lock = threading.RLock()
    _initialized_dbs: set = set()

    # 本地缓存时效（秒），默认12小时
    DICT_CACHE_TTL_SECONDS = int(os.environ.get("STOCK_DICT_CACHE_TTL_SECONDS", "43200"))
    DICT_CACHE_FILE = "stock_dict_cache.json"

    def __init__(self, group_id: str, log_callback=None):
        self.group_id = group_id
        self.log_callback = log_callback
        self.db_path_manager = get_db_path_manager()

        # 话题数据库路径
        self.topics_db_path = self.db_path_manager.get_topics_db_path(group_id)

        # 初始化股票分析相关表（幂等、防遗漏）
        self._ensure_stock_tables()

        # 股票字典 (延迟加载)
        self._automaton = None
        self._stock_dict = {}  # code -> name
        self._name_to_code = {}  # name -> code
        self._dict_cache_path = Path(self.db_path_manager.base_dir) / self.DICT_CACHE_FILE

    def log(self, message: str):
        """统一日志"""
        if self.log_callback:
            self.log_callback(message)
        log_info(message)

    def _get_conn(self):
        """获取带 WAL 模式和超时的数据库连接，确保表已就绪"""
        # 确保表存在，避免旧数据库缺表导致查询失败
        self._ensure_stock_tables()

        conn = sqlite3.connect(self.topics_db_path, check_same_thread=False, timeout=30)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout=30000')
        return conn

    # ========== 数据库初始化 ==========

    def _ensure_stock_tables(self):
        """在话题数据库中创建股票分析相关表（幂等，容错旧库缺表/缺列）"""
        db_key = os.path.abspath(self.topics_db_path)

        with StockAnalyzer._init_lock:
            conn = sqlite3.connect(self.topics_db_path, check_same_thread=False, timeout=30)
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA busy_timeout=30000')
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
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (topic_id) REFERENCES topics (topic_id)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stock_price_cache (
                    stock_code TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    open REAL,
                    close REAL,
                    high REAL,
                    low REAL,
                    change_pct REAL,
                    volume REAL,
                    PRIMARY KEY (stock_code, trade_date)
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
                    freeze_level INTEGER DEFAULT 0,
                    FOREIGN KEY (mention_id) REFERENCES stock_mentions(id)
                )
            ''')

            # 兼容旧表：添加新列（如果不存在）
            for col in ['return_60d', 'return_120d', 'return_250d',
                        'excess_return_60d', 'excess_return_120d', 'excess_return_250d',
                        'freeze_level']:
                try:
                    cursor.execute(f'ALTER TABLE mention_performance ADD COLUMN {col} REAL')
                except Exception:
                    pass  # 列已存在

            # 创建索引
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sm_stock_code ON stock_mentions(stock_code)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sm_mention_date ON stock_mentions(mention_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sm_topic_id ON stock_mentions(topic_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_mp_stock_code ON mention_performance(stock_code)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_mp_mention_date ON mention_performance(mention_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_spc_date ON stock_price_cache(trade_date)')

            conn.commit()
            conn.close()
            StockAnalyzer._initialized_dbs.add(db_key)

    # ========== 股票字典构建 ==========

    def _build_stock_dictionary(self):
        """构建/复用 A 股股票字典，优先使用进程缓存和本地缓存"""
        self.log("正在准备A股股票字典")

        if self._automaton is not None:
            total = len(self._name_to_code)
            self.log("从AkShare获取清单（实例缓存命中，跳过）")
            self.log(f"字典处理进度 {total}/{total}")
            self.log(f"索引构建进度 {total}/{total}")
            self.log("股票字典就绪")
            return

        # 先复用进程级缓存
        if StockAnalyzer._global_automaton is not None:
            self._automaton = StockAnalyzer._global_automaton
            self._stock_dict = StockAnalyzer._global_stock_dict
            self._name_to_code = StockAnalyzer._global_name_to_code
            total = len(self._name_to_code)
            self.log("从AkShare获取清单（进程缓存命中，跳过）")
            self.log(f"字典处理进度 {total}/{total}")
            self.log(f"索引构建进度 {total}/{total}")
            self.log("股票字典就绪")
            return

        with StockAnalyzer._dict_lock:
            # 双重检查，避免并发重复构建
            if StockAnalyzer._global_automaton is not None:
                self._automaton = StockAnalyzer._global_automaton
                self._stock_dict = StockAnalyzer._global_stock_dict
                self._name_to_code = StockAnalyzer._global_name_to_code
                total = len(self._name_to_code)
                self.log("从AkShare获取清单（进程缓存命中，跳过）")
                self.log(f"字典处理进度 {total}/{total}")
                self.log(f"索引构建进度 {total}/{total}")
                self.log("股票字典就绪")
                return

            self.log("从AkShare获取清单")
            stock_dict, name_to_code = self._load_stock_dictionary_from_cache()

            if not stock_dict:
                stock_dict, name_to_code = self._fetch_stock_dictionary_from_akshare()
                self._save_stock_dictionary_cache(stock_dict, name_to_code)
            else:
                total = len(name_to_code)
                self.log(f"字典处理进度 {total}/{total}")

            self._load_and_apply_user_aliases(name_to_code, stock_dict)
            self.log("正在构建股票匹配索引")
            automaton = ahocorasick.Automaton()
            total = len(name_to_code)
            for idx, (name, code) in enumerate(name_to_code.items(), 1):
                if len(name) >= 2:
                    automaton.add_word(name, (code, name))
                if idx % 1000 == 0 or idx == total:
                    self.log(f"索引构建进度 {idx}/{total}")

            automaton.make_automaton()

            # 回写进程级缓存
            StockAnalyzer._global_automaton = automaton
            StockAnalyzer._global_stock_dict = stock_dict
            StockAnalyzer._global_name_to_code = name_to_code
            StockAnalyzer._global_built_at = time.time()

            self._automaton = automaton
            self._stock_dict = stock_dict
            self._name_to_code = name_to_code
            self.log("股票字典就绪")

    def _load_and_apply_user_aliases(self, name_to_code: Dict[str, str], stock_dict: Dict[str, str]):
        """
        加载用户自定义别名并应用到字典中
        stock_aliases.json 格式: {"别名": "标准股票名称"}
        """
        alias_file = Path("stock_aliases.json")
        if not alias_file.exists():
            return

        try:
            with open(alias_file, "r", encoding="utf-8") as f:
                aliases = json.load(f)
            
            count = 0
            # 建立反向查找表: Standard Name -> Code (stock_dict is Code -> Name)
            std_name_to_code = {v: k for k, v in stock_dict.items()}

            for alias, std_name in aliases.items():
                alias = alias.strip()
                std_name = std_name.strip()
                if not alias or not std_name:
                    continue
                
                # 查找标准名称对应的代码
                code = std_name_to_code.get(std_name)
                
                # 如果找不到，尝试 std_name 是否本身就是代码
                if not code and std_name in stock_dict:
                     code = std_name
                
                if code:
                    # 将别名映射到该代码
                    name_to_code[alias] = code
                    count += 1
                else:
                    msg = f"别名配置错误: 找不到股票 '{std_name}' (别名: {alias})"
                    self.log(msg)
                    log_warning(msg)

            if count > 0:
                self.log(f"已加载 {count} 个用户自定义股票别名")

        except Exception as e:
            msg = f"加载股票别名失败: {e}"
            self.log(msg)
            log_warning(msg)

    def _load_stock_dictionary_from_cache(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        """尝试从本地缓存读取股票字典"""
        try:
            if not self._dict_cache_path.exists():
                return {}, {}

            with open(self._dict_cache_path, "r", encoding="utf-8") as f:
                payload = json.load(f)

            built_at = float(payload.get("built_at", 0))
            age = time.time() - built_at
            if age > self.DICT_CACHE_TTL_SECONDS:
                self.log("♻️ 本地股票字典缓存已过期，准备刷新")
                return {}, {}

            stock_dict = payload.get("stock_dict", {})
            name_to_code = payload.get("name_to_code", {})
            if isinstance(stock_dict, dict) and isinstance(name_to_code, dict) and stock_dict and name_to_code:
                self.log(f"⚡ 已加载本地股票字典缓存（{len(name_to_code)}只）")
                return stock_dict, name_to_code
            return {}, {}
        except Exception as e:
            log_warning(f"读取股票字典缓存失败: {e}")
            return {}, {}

    def _save_stock_dictionary_cache(self, stock_dict: Dict[str, str], name_to_code: Dict[str, str]):
        """保存股票字典到本地缓存"""
        try:
            self._dict_cache_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "built_at": time.time(),
                "stock_dict": stock_dict,
                "name_to_code": name_to_code,
            }
            with open(self._dict_cache_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
        except Exception as e:
            log_warning(f"写入股票字典缓存失败: {e}")

    def _fetch_stock_dictionary_from_akshare(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        """从 AkShare 获取全量股票字典，并输出构建进度"""
        self.log("从AkShare获取清单")
        try:
            holder: Dict[str, Any] = {"df": None, "err": None}

            def _fetch():
                try:
                    holder["df"] = ak.stock_zh_a_spot_em()
                except Exception as e:
                    holder["err"] = e

            t = threading.Thread(target=_fetch, daemon=True)
            t.start()
            waited = 0
            while t.is_alive():
                t.join(timeout=5)
                waited += 5
                if t.is_alive():
                    self.log(f"从AkShare获取清单中...已等待 {waited} 秒")

            if holder["err"] is not None:
                raise holder["err"]

            df = holder["df"]
            total_rows = len(df)
            self.log(f"字典处理进度 0/{total_rows}")

            stock_dict: Dict[str, str] = {}
            name_to_code: Dict[str, str] = {}

            for idx, (_, row) in enumerate(df.iterrows(), 1):
                code = str(row['代码'])
                name = str(row['名称']).strip()

                if not name or len(name) < 2:
                    continue
                if name in EXCLUDE_WORDS:
                    continue

                if code.startswith('6'):
                    full_code = f"{code}.SH"
                elif code.startswith(('0', '3')):
                    full_code = f"{code}.SZ"
                elif code.startswith(('4', '8')):
                    full_code = f"{code}.BJ"
                else:
                    full_code = code

                stock_dict[full_code] = name
                name_to_code[name] = full_code

                if idx % 1000 == 0 or idx == total_rows:
                    self.log(f"字典处理进度 {idx}/{total_rows}")

            return stock_dict, name_to_code
        except Exception as e:
            log_error(f"构建股票字典失败: {e}")
            raise

    def extract_stocks(self, text: str) -> List[Dict[str, Any]]:
        """
        从文本中提取所有股票提及
        返回: [{code, name, position, context}]
        """
        if self._automaton is None:
            self._build_stock_dictionary()

        if not text or not self._automaton:
            return []

        # 清理文本中的 XML/HTML 标签
        clean_text = re.sub(r'<[^>]+>', '', text)

        results = []
        seen_codes = set()

        for end_pos, (code, name) in self._automaton.iter(clean_text):
            if code in seen_codes:
                continue

            start_pos = end_pos - len(name) + 1

            # 提取上下文片段 (前后50字符)
            ctx_start = max(0, start_pos - 50)
            ctx_end = min(len(clean_text), end_pos + 51)
            context = clean_text[ctx_start:ctx_end].strip()

            # Use full stock name from dictionary instead of matched alias text
            full_name = self._stock_dict.get(code, name)
            if is_excluded_stock(code, full_name):
                continue
            results.append({
                'code': code,
                'name': full_name,
                'position': start_pos,
                'context': context
            })
            seen_codes.add(code)

        return results

    # ========== 行情数据 ==========

    def fetch_price_range(self, stock_code: str, start_date: str, end_date: str) -> List[Dict]:
        """
        获取股票区间行情，按天级粒度缓存
        - T-2 之前历史数据视为稳定，直接复用缓存
        - T-1 / T 数据每次刷新（盘后修正 + 盘中变动）
        - 只对缺失日期调用 AkShare
        """
        pure_code = stock_code.split('.')[0]
        today_dt = datetime.now().date()
        refresh_from = (today_dt - timedelta(days=1)).strftime('%Y-%m-%d')  # T-1 起刷新

        # 阶段1：查询缓存（短连接，快速释放）
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT trade_date, open, close, high, low, change_pct, volume
            FROM stock_price_cache
            WHERE stock_code = ? AND trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date
        ''', (stock_code, start_date, end_date))
        cached_rows = cursor.fetchall()
        cached_dates = {r[0] for r in cached_rows}

        # 删除 T-1 / T 缓存（需实时刷新），T-2 之前保留
        volatile_dates = [d for d in cached_dates if d >= refresh_from]
        if volatile_dates:
            cursor.execute('''
                DELETE FROM stock_price_cache
                WHERE stock_code = ? AND trade_date >= ? AND trade_date <= ?
            ''', (stock_code, refresh_from, end_date))
            conn.commit()
            cached_dates = {d for d in cached_dates if d < refresh_from}
            cached_rows = [r for r in cached_rows if r[0] < refresh_from]

        conn.close()  # ★ 释放连接后再做网络请求

        # 构建缓存结果
        results_map = {}
        for r in cached_rows:
            results_map[r[0]] = {
                'trade_date': r[0], 'open': r[1], 'close': r[2],
                'high': r[3], 'low': r[4], 'change_pct': r[5], 'volume': r[6]
            }

        # 判断是否需要从 AkShare 拉取
        need_fetch = len(cached_dates) == 0
        if not need_fetch:
            # 区间触及 T-1 / T 时强制刷新
            if end_date >= refresh_from:
                need_fetch = True
            else:
                # 对稳定历史区间做轻量完整性校验：仅当首条数据明显晚于 start_date 时回补
                try:
                    first_cached = datetime.strptime(cached_rows[0][0], '%Y-%m-%d').date() if cached_rows else None
                    start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
                    if first_cached and (first_cached - start_dt).days > 3:
                        need_fetch = True
                except Exception:
                    need_fetch = True

        # 阶段2：网络请求（不持有数据库连接）
        new_records = []
        if need_fetch:
            try:
                df = ak.stock_zh_a_hist(
                    symbol=pure_code,
                    period="daily",
                    start_date=start_date.replace('-', ''),
                    end_date=end_date.replace('-', ''),
                    adjust="qfq"
                )

                if df is not None and not df.empty:
                    for _, row in df.iterrows():
                        trade_date = str(row['日期'])[:10]
                        # 历史稳定日已缓存则跳过；T-1/T 由上方删除后会重写
                        if trade_date in cached_dates and trade_date < refresh_from:
                            continue

                        record = {
                            'trade_date': trade_date,
                            'open': float(row['开盘']),
                            'close': float(row['收盘']),
                            'high': float(row['最高']),
                            'low': float(row['最低']),
                            'change_pct': float(row['涨跌幅']),
                            'volume': float(row['成交量']),
                        }
                        results_map[trade_date] = record
                        new_records.append((stock_code, record))

            except Exception as e:
                log_warning(f"获取 {stock_code} 行情失败: {e}")

        # 阶段3：批量写入缓存（重新打开短连接）
        if new_records:
            conn = self._get_conn()
            cursor = conn.cursor()
            for sc, rec in new_records:
                cursor.execute('''
                    INSERT OR REPLACE INTO stock_price_cache
                    (stock_code, trade_date, open, close, high, low, change_pct, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    sc, rec['trade_date'],
                    rec['open'], rec['close'], rec['high'], rec['low'],
                    rec['change_pct'], rec['volume']
                ))
            conn.commit()
            conn.close()

        # 按日期排序返回
        return [results_map[d] for d in sorted(results_map.keys())]

    def _fetch_index_price(self, start_date: str, end_date: str) -> Dict[str, float]:
        """获取沪深300指数行情（用于计算超额收益）"""
        index_code = "000300.SH"  # 沪深300
        today_dt = datetime.now().date()
        refresh_from = (today_dt - timedelta(days=1)).strftime('%Y-%m-%d')  # T-1 起刷新

        # 阶段1：查缓存（短连接）
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT trade_date, close FROM stock_price_cache
            WHERE stock_code = ? AND trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date
        ''', (index_code, start_date, end_date))
        cached = cursor.fetchall()
        cached_map = {r[0]: r[1] for r in cached}

        # 删除 T-1 / T 指数缓存，避免复权/收盘后修正不一致
        if any(d >= refresh_from for d in cached_map.keys()):
            cursor.execute('''
                DELETE FROM stock_price_cache
                WHERE stock_code = ? AND trade_date >= ? AND trade_date <= ?
            ''', (index_code, refresh_from, end_date))
            conn.commit()
            cached_map = {d: v for d, v in cached_map.items() if d < refresh_from}

        conn.close()  # ★ 释放连接

        need_fetch = len(cached_map) == 0 or end_date >= refresh_from
        if not need_fetch:
            try:
                first_cached = min(datetime.strptime(d, '%Y-%m-%d').date() for d in cached_map.keys())
                start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
                if (first_cached - start_dt).days > 3:
                    need_fetch = True
            except Exception:
                need_fetch = True

        if not need_fetch:
            return cached_map

        # 阶段2：网络请求（不持有数据库连接）
        try:
            df = ak.stock_zh_index_daily(symbol="sh000300")
            if df is None or df.empty:
                return cached_map

            result = {}
            cache_rows = []
            for _, row in df.iterrows():
                trade_date = str(row['date'])[:10]
                if trade_date < start_date or trade_date > end_date:
                    continue
                # 稳定历史日命中缓存直接复用，避免重复写
                if trade_date in cached_map and trade_date < refresh_from:
                    result[trade_date] = cached_map[trade_date]
                    continue
                close_val = float(row['close'])
                result[trade_date] = close_val
                cache_rows.append((
                    index_code, trade_date, float(row['open']), close_val,
                    float(row['high']), float(row['low']), 0, float(row['volume'])
                ))

            # 阶段3：批量写入缓存
            if cache_rows:
                conn = self._get_conn()
                cursor = conn.cursor()
                cursor.executemany('''
                    INSERT OR REPLACE INTO stock_price_cache
                    (stock_code, trade_date, open, close, high, low, change_pct, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', cache_rows)
                conn.commit()
                conn.close()

            # 合并保留的稳定历史缓存
            result.update({d: v for d, v in cached_map.items() if d not in result})
            return result

        except Exception as e:
            log_warning(f"获取沪深300指数失败: {e}")
            return cached_map

    # ========== 事件表现计算 ==========

    def _calc_mention_performance(self, mention_id: int, stock_code: str, mention_date: str) -> Tuple[bool, str]:
        """
        计算一次提及事件的后续表现
        T+1, T+3, T+5, T+10, T+20, T+60, T+120, T+250 收益率 & 超额收益率
        支持渐进式冻结：已冻结的字段不再重新拉取行情
        """
        ALL_PERIODS = [1, 3, 5, 10, 20, 60, 120, 250]

        # 检查当前 freeze_level，决定需要计算哪些周期
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT freeze_level FROM mention_performance WHERE mention_id = ?', (mention_id,))
        row = cursor.fetchone()
        current_freeze = row[0] if row and row[0] else 0
        conn.close()

        # 根据 freeze_level 确定需要计算的周期
        # 0: 所有都需要, 1: T+60/120/250, 2: T+120/250, 3: 全部冻结
        if current_freeze >= 3:
            return False, "freeze_level 已冻结"

        freeze_thresholds = {1: 20, 2: 60, 3: 120}
        periods_to_calc = [d for d in ALL_PERIODS if d > freeze_thresholds.get(current_freeze, 0)]
        if current_freeze == 0:
            periods_to_calc = ALL_PERIODS

        # 计算日期范围：提及日前5天 ~ 后足够天数
        dt = datetime.strptime(mention_date, '%Y-%m-%d')
        max_period = max(periods_to_calc)
        start = (dt - timedelta(days=10)).strftime('%Y-%m-%d')
        end = (dt + timedelta(days=int(max_period * 1.5) + 10)).strftime('%Y-%m-%d')

        prices = self.fetch_price_range(stock_code, start, end)
        if not prices:
            return False, "无可用行情数据"

        # 找到提及日或之后最近的交易日作为基准
        base_price = None
        base_idx = -1
        for i, p in enumerate(prices):
            if p['trade_date'] >= mention_date:
                base_price = p['close']
                base_idx = i
                break

        if base_price is None or base_price == 0:
            return False, "未找到提及日及之后交易日价格"

        # 获取沪深300 对应期间数据
        index_prices = self._fetch_index_price(start, end)

        # 找到沪深300 基准价
        index_base = None
        for p in prices:
            if p['trade_date'] >= mention_date and p['trade_date'] in index_prices:
                index_base = index_prices[p['trade_date']]
                break

        # 计算各期限收益率
        returns = {}
        excess_returns = {}
        for days in periods_to_calc:
            target_idx = base_idx + days
            if target_idx < len(prices):
                target_price = prices[target_idx]['close']
                ret = (target_price - base_price) / base_price * 100
                returns[days] = round(ret, 2)

                # 超额收益
                target_date = prices[target_idx]['trade_date']
                if index_base and target_date in index_prices and index_base > 0:
                    index_ret = (index_prices[target_date] - index_base) / index_base * 100
                    excess_returns[days] = round(ret - index_ret, 2)
                else:
                    excess_returns[days] = None
            else:
                returns[days] = None
                excess_returns[days] = None

        # 计算期间最大涨幅和最大回撤（使用最长可用周期，最多250个交易日）
        max_return = 0
        max_drawdown = 0
        max_track = min(base_idx + max_period + 1, len(prices))
        for i in range(base_idx + 1, max_track):
            ret = (prices[i]['high'] - base_price) / base_price * 100
            max_return = max(max_return, ret)
            dd = (prices[i]['low'] - base_price) / base_price * 100
            max_drawdown = min(max_drawdown, dd)

        # 确定新的 freeze_level
        today = datetime.now().strftime('%Y-%m-%d')
        trading_days_elapsed = base_idx  # 粗略估计
        # 更准确：计算提及日到今天之间的交易日数
        today_idx = -1
        for i, p in enumerate(prices):
            if p['trade_date'] >= today:
                today_idx = i
                break
        if today_idx < 0:
            today_idx = len(prices)
        trading_days_elapsed = today_idx - base_idx

        new_freeze = current_freeze
        if trading_days_elapsed > 260:
            new_freeze = 3
        elif trading_days_elapsed > 130:
            new_freeze = max(current_freeze, 2)
        elif trading_days_elapsed > 70:
            new_freeze = max(current_freeze, 1)
        elif trading_days_elapsed > 25:
            new_freeze = max(current_freeze, 1)

        # 写入数据库（使用 UPSERT 模式）
        conn = self._get_conn()
        cursor = conn.cursor()

        if row:
            # 更新已存在的记录（只更新未冻结字段）
            updates = []
            params = []
            for days in periods_to_calc:
                if returns.get(days) is not None:
                    updates.append(f'return_{days}d = ?')
                    params.append(returns[days])
                    updates.append(f'excess_return_{days}d = ?')
                    params.append(excess_returns.get(days))
            updates.append('max_return = ?')
            params.append(round(max_return, 2))
            updates.append('max_drawdown = ?')
            params.append(round(max_drawdown, 2))
            updates.append('freeze_level = ?')
            params.append(new_freeze)
            params.append(mention_id)

            if updates:
                cursor.execute(f'''
                    UPDATE mention_performance SET {', '.join(updates)}
                    WHERE mention_id = ?
                ''', params)
        else:
            # 新插入
            cursor.execute('''
                INSERT OR REPLACE INTO mention_performance
                (mention_id, stock_code, mention_date, price_at_mention,
                 return_1d, return_3d, return_5d, return_10d, return_20d,
                 return_60d, return_120d, return_250d,
                 excess_return_1d, excess_return_3d, excess_return_5d,
                 excess_return_10d, excess_return_20d,
                 excess_return_60d, excess_return_120d, excess_return_250d,
                 max_return, max_drawdown, freeze_level)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                mention_id, stock_code, mention_date, round(base_price, 2),
                returns.get(1), returns.get(3), returns.get(5),
                returns.get(10), returns.get(20),
                returns.get(60), returns.get(120), returns.get(250),
                excess_returns.get(1), excess_returns.get(3), excess_returns.get(5),
                excess_returns.get(10), excess_returns.get(20),
                excess_returns.get(60), excess_returns.get(120), excess_returns.get(250),
                round(max_return, 2), round(max_drawdown, 2), new_freeze
            ))
        conn.commit()
        conn.close()
        return True, "ok"

    # ========== 全量扫描 ==========

    def scan_group(self, group_id: str = None, force: bool = False) -> Dict[str, Any]:
        """
        扫描群组全部帖子，提取股票提及并计算后续表现

        Args:
            group_id: 群组ID（默认使用初始化时的group_id）
            force: 是否强制重新扫描（清除旧数据）

        Returns:
            扫描结果统计
        """
        gid = group_id or self.group_id
        self._build_stock_dictionary()

        conn = self._get_conn()
        cursor = conn.cursor()

        if force:
            cursor.execute('DELETE FROM mention_performance')
            cursor.execute('DELETE FROM stock_mentions')
            conn.commit()
            self.log("🗑️ 已清除旧的股票分析数据")

        # 获取待处理帖子（非 force 模式下仅处理未提取过的 topic）
        cursor.execute('''
            SELECT t.topic_id, tk.text, t.create_time
            FROM topics t
            JOIN talks tk ON t.topic_id = tk.topic_id
            WHERE tk.text IS NOT NULL AND tk.text != ''
              AND (
                ? = 1
                OR NOT EXISTS (
                    SELECT 1 FROM stock_mentions sm WHERE sm.topic_id = t.topic_id
                )
              )
            ORDER BY t.create_time
        ''', (1 if force else 0,))
        topics = cursor.fetchall()

        total_topics = len(topics)
        total_mentions = 0
        stocks_found = set()

        self.log(f"🔍 开始扫描 {total_topics} 条帖子...")

        for i, (topic_id, text, create_time) in enumerate(topics):
            stocks = self.extract_stocks(text)
            if not stocks:
                continue

            # 解析日期
            mention_date = create_time[:10] if create_time else ''
            if not mention_date:
                continue

            for stock in stocks:
                cursor.execute('''
                    INSERT INTO stock_mentions
                    (topic_id, stock_code, stock_name, mention_date, mention_time, context_snippet)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    topic_id, stock['code'], stock['name'],
                    mention_date, create_time or '', stock['context']
                ))
                total_mentions += 1
                stocks_found.add(stock['code'])

            if (i + 1) % 20 == 0:
                conn.commit()
                self.log(f"📊 已扫描 {i+1}/{total_topics} 条帖子，累计提取 {total_mentions} 次股票提及")

        conn.commit()
        self.log(f"✅ 扫描完成：{total_topics} 条帖子，提取 {total_mentions} 次提及，涉及 {len(stocks_found)} 只股票")

        # 阶段二：计算每次提及的后续表现
        self.log("📈 开始计算提及后表现...")
        cursor.execute('''
            SELECT sm.id, sm.stock_code, sm.mention_date
            FROM stock_mentions sm
            LEFT JOIN mention_performance mp ON sm.id = mp.mention_id
            WHERE mp.mention_id IS NULL
        ''')
        pending = cursor.fetchall()
        conn.close()

        total_pending = len(pending)
        self.log(f"📌 待计算提及表现: {total_pending} 条")
        if total_pending == 0:
            self.log("✅ 无需增量计算，任务结束")
            return {
                'topics_scanned': total_topics,
                'mentions_extracted': total_mentions,
                'unique_stocks': len(stocks_found),
                'performance_calculated': 0
            }

        # 按股票分组：同一股票的多次提及共享行情缓存，减少 API 调用
        from collections import defaultdict
        pending_by_stock = defaultdict(list)
        for mid, code, date in pending:
            pending_by_stock[code].append((mid, date))

        done_count = 0
        total_stocks_to_calc = len(pending_by_stock)
        for stock_idx, (stock_code, items) in enumerate(pending_by_stock.items()):
            for mid, mention_date in items:
                try:
                    self._calc_mention_performance(mid, stock_code, mention_date)
                except Exception as e:
                    log_warning(f"计算 {stock_code} 表现失败: {e}")
                done_count += 1

            if (stock_idx + 1) % 5 == 0 or done_count == total_pending:
                self.log(f"📈 已计算 {done_count}/{total_pending} 条提及 ({stock_idx+1}/{total_stocks_to_calc} 只股票)")

            # 仅在切换股票时 sleep，同一股票的提及复用行情缓存无需等待
            if stock_idx < total_stocks_to_calc - 1:
                time.sleep(0.2)

        self.log(f"✅ 全部完成！共处理 {total_pending} 条提及表现计算")

        return {
            'topics_scanned': total_topics,
            'mentions_extracted': total_mentions,
            'unique_stocks': len(stocks_found),
            'performance_calculated': total_pending
        }

    # ========== 分离式方法（调度器专用）==========

    def extract_only(self, group_id: str = None) -> Dict[str, Any]:
        """
        仅提取股票名称，不计算收益表现（纯本地操作，秒级完成）
        供调度器高频循环使用
        """
        gid = group_id or self.group_id
        self._build_stock_dictionary()

        conn = self._get_conn()
        cursor = conn.cursor()

        # 获取已扫描的 topic_id 集合
        cursor.execute('SELECT DISTINCT topic_id FROM stock_mentions')
        scanned_ids = {r[0] for r in cursor.fetchall()}

        # 获取全部帖子
        cursor.execute('''
            SELECT t.topic_id, tk.text, t.create_time
            FROM topics t
            JOIN talks tk ON t.topic_id = tk.topic_id
            WHERE tk.text IS NOT NULL AND tk.text != ''
            ORDER BY t.create_time
        ''')
        topics = cursor.fetchall()

        total_topics = len(topics)
        total_mentions = 0
        stocks_found = set()
        new_topics = 0

        for i, (topic_id, text, create_time) in enumerate(topics):
            if topic_id in scanned_ids:
                continue

            new_topics += 1
            stocks = self.extract_stocks(text)
            if not stocks:
                continue

            mention_date = create_time[:10] if create_time else ''
            if not mention_date:
                continue

            for stock in stocks:
                cursor.execute('''
                    INSERT INTO stock_mentions
                    (topic_id, stock_code, stock_name, mention_date, mention_time, context_snippet)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    topic_id, stock['code'], stock['name'],
                    mention_date, create_time or '', stock['context']
                ))
                total_mentions += 1
                stocks_found.add(stock['code'])

        conn.commit()
        conn.close()

        if new_topics > 0:
            self.log(f"📝 提取完成：{new_topics} 条新帖子，{total_mentions} 次提及，{len(stocks_found)} 只股票")

        return {
            'new_topics': new_topics,
            'mentions_extracted': total_mentions,
            'unique_stocks': len(stocks_found)
        }

    def calc_pending_performance(self, calc_window_days: int = 365, progress_callback=None) -> Dict[str, Any]:
        """
        计算待处理的收益表现（需要网络，供定时任务使用）
        包括：未计算的新提及 + 未完全冻结的旧提及

        Args:
            calc_window_days: 活跃计算窗口天数（默认365天，覆盖T+250）
            progress_callback: 进度回调函数，func(current, total, msg)
        """
        self._build_stock_dictionary()
        since_date = (datetime.now() - timedelta(days=calc_window_days)).strftime('%Y-%m-%d')

        conn = self._get_conn()
        cursor = conn.cursor()

        # 查询1：未计算收益的新提及
        cursor.execute('''
            SELECT sm.id, sm.stock_code, sm.mention_date
            FROM stock_mentions sm
            LEFT JOIN mention_performance mp ON sm.id = mp.mention_id
            WHERE mp.mention_id IS NULL
            AND sm.mention_date >= ?
        ''', (since_date,))
        new_pending = cursor.fetchall()

        # 查询2：已有记录但未完全冻结的提及（需要更新长周期数据）
        cursor.execute('''
            SELECT sm.id, sm.stock_code, sm.mention_date
            FROM stock_mentions sm
            JOIN mention_performance mp ON sm.id = mp.mention_id
            WHERE (mp.freeze_level IS NULL OR mp.freeze_level < 3)
            AND sm.mention_date >= ?
        ''', (since_date,))
        update_pending = cursor.fetchall()

        conn.close()

        total_new = len(new_pending)
        total_update = len(update_pending)
        all_pending = new_pending + update_pending

        self.log(f"📈 收益计算：{total_new} 条新提及 + {total_update} 条待更新")

        processed = 0
        skipped = 0
        errors = 0
        total = len(all_pending)
        
        for i, (mention_id, stock_code, mention_date) in enumerate(all_pending, 1):
            status_msg = ""
            try:
                written, reason = self._calc_mention_performance(mention_id, stock_code, mention_date)
                if written:
                    processed += 1
                    status_msg = f"已保存 {stock_code} ({mention_date})"
                else:
                    skipped += 1
                    status_msg = f"跳过 {stock_code} ({mention_date})：{reason}"
            except Exception as e:
                log_warning(f"计算 {stock_code} 表现失败: {e}")
                errors += 1
                status_msg = f"失败 {stock_code}: {e}"

            if progress_callback:
                # The callback handles the 10s interval logic
                progress_callback(i, total, status_msg)
            
            # Internal log - keep it periodic
            if i % 20 == 0 or i == total:
                self.log(f"📈 收益计算中: {i}/{total} (成功: {processed}, 跳过: {skipped}, 错误: {errors})")

            time.sleep(0.3)

        self.log(f"✅ 收益计算完成：成功 {processed} 条，跳过 {skipped} 条，失败 {errors} 条")

        return {
            'new_calculated': total_new,
            'updated': total_update,
            'processed': processed,
            'skipped': skipped,
            'errors': errors
        }

    def _get_analysis_backlog_stats(self, calc_window_days: int = 365) -> Dict[str, Any]:
        """
        读取分析前置统计：提及总量、待计算量、是否建议先做提取。
        仅做本地 SQL 查询，不触发网络请求。
        """
        since_date = (datetime.now() - timedelta(days=calc_window_days)).strftime('%Y-%m-%d')
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('SELECT COUNT(*) FROM stock_mentions')
        mentions_total = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute('''
            SELECT COUNT(*)
            FROM stock_mentions sm
            LEFT JOIN mention_performance mp ON sm.id = mp.mention_id
            WHERE mp.mention_id IS NULL
              AND sm.mention_date >= ?
        ''', (since_date,))
        pending_new = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute('''
            SELECT COUNT(*)
            FROM stock_mentions sm
            JOIN mention_performance mp ON sm.id = mp.mention_id
            WHERE (mp.freeze_level IS NULL OR mp.freeze_level < 3)
              AND sm.mention_date >= ?
        ''', (since_date,))
        pending_update = int((cursor.fetchone() or [0])[0] or 0)
        conn.close()

        pending_total = pending_new + pending_update
        needs_extract = (mentions_total == 0) or (mentions_total > 0 and pending_total == 0)
        return {
            'mentions_total': mentions_total,
            'pending_total': pending_total,
            'pending_new': pending_new,
            'pending_update': pending_update,
            'needs_extract': needs_extract,
        }

    # ========== 查询接口 ==========

    def get_topic_mentions(self, page: int = 1, per_page: int = 20) -> Dict[str, Any]:
        """
        获取按话题分组的股票提及列表
        """
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # 1. 分页获取含有股票提及的 topic_id (按最近提及时间排序)
        offset = (page - 1) * per_page
        cursor.execute('''
            SELECT topic_id, MAX(mention_time) as latest_mention
            FROM stock_mentions
            GROUP BY topic_id
            ORDER BY latest_mention DESC
            LIMIT ? OFFSET ?
        ''', (per_page, offset))
        
        rows = cursor.fetchall()
        topic_ids = [row[0] for row in rows]

        if not topic_ids:
            conn.close()
            # Try to get total count anyway to be correct
            conn2 = self._get_conn()
            cursor2 = conn2.cursor()
            cursor2.execute('SELECT COUNT(DISTINCT topic_id) FROM stock_mentions')
            total = cursor2.fetchone()[0]
            conn2.close()
            
            return {
                'total': total,
                'page': page,
                'per_page': per_page,
                'items': []
            }

        # 2. 获取总数
        cursor.execute('SELECT COUNT(DISTINCT topic_id) FROM stock_mentions')
        total = cursor.fetchone()[0]

        # 3. 批量获取话题内容
        placeholders = ','.join('?' * len(topic_ids))
        cursor.execute(f'''
            SELECT t.topic_id, t.create_time, tk.text
            FROM topics t
            JOIN talks tk ON t.topic_id = tk.topic_id
            WHERE t.topic_id IN ({placeholders})
        ''', topic_ids)
        topics_map = {row['topic_id']: dict(row) for row in cursor.fetchall()}

        # 4. 批量获取这些话题下的股票提及和表现
        cursor.execute(f'''
            SELECT sm.topic_id, sm.stock_code, sm.stock_name,
                   mp.return_1d, mp.return_3d, mp.return_5d, mp.return_10d, mp.return_20d,
                   mp.max_return
            FROM stock_mentions sm
            LEFT JOIN mention_performance mp ON sm.id = mp.mention_id
            WHERE sm.topic_id IN ({placeholders})
        ''', topic_ids)
        
        mentions_by_topic = {}
        for row in cursor.fetchall():
            tid = row['topic_id']
            if tid not in mentions_by_topic:
                mentions_by_topic[tid] = []
            
            # Convert row to dict and handle None values for cleaner frontend JSON
            item = dict(row)
            mentions_by_topic[tid].append(item)

        # 5. 组装结果
        items = []
        for tid in topic_ids:
            # Note: It's possible a topic is in stock_mentions but missing from topics/talks if data inconsistency exists
            # We skip if topic content not found
            if tid not in topics_map:
                continue
                
            topic = topics_map[tid]
            topic['mentions'] = mentions_by_topic.get(tid, [])
            items.append(topic)

        conn.close()
        return {
            'total': total,
            'page': page,
            'per_page': per_page,
            'items': items
        }

    def get_mentions(self, stock_code: str = None, page: int = 1, per_page: int = 50,
                     sort_by: str = 'mention_date', order: str = 'desc') -> Dict[str, Any]:
        """
        获取股票提及列表
        sort_by: mention_date / return_5d / excess_return_5d / max_return
        """
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        where_clause = "WHERE 1=1"
        params = []
        # 模糊搜索逻辑
        if stock_code:
            # 预处理：如果是纯数字+后缀（如 300308.SZ），去掉后缀
            clean_code = stock_code.strip()
            if '.' in clean_code:
                parts = clean_code.split('.')
                # 如果前缀是数字，且后缀是SZ/SH/BJ等，则只取前缀
                if parts[0].isdigit() and parts[1].upper() in ['SZ', 'SH', 'BJ', 'SS']:
                    clean_code = parts[0]
            
            # 支持 代码允许前缀匹配/包含匹配，名称允许模糊匹配
            # 用户需求：300308.SZ 等同于 300308 (前缀匹配) -> 其实是清洗后的精确或前缀
            # 这里使用 OR 逻辑：代码包含 OR 名称包含
            where_clause += " AND (sm.stock_code LIKE ? OR sm.stock_name LIKE ?)"
            search_term = f"%{clean_code}%"
            params.append(search_term)
            params.append(search_term)

        # 允许的排序字段
        valid_sorts = {
            'mention_date': 'sm.mention_date',
            'return_1d': 'mp.return_1d', 'return_3d': 'mp.return_3d',
            'return_5d': 'mp.return_5d', 'return_10d': 'mp.return_10d',
            'return_20d': 'mp.return_20d',
            'excess_return_5d': 'mp.excess_return_5d',
            'excess_return_10d': 'mp.excess_return_10d',
            'max_return': 'mp.max_return',
        }
        sort_col = valid_sorts.get(sort_by, 'sm.mention_date')
        order_dir = 'DESC' if order.lower() == 'desc' else 'ASC'

        # 总数
        cursor.execute(f'''
            SELECT COUNT(*) FROM stock_mentions sm
            LEFT JOIN mention_performance mp ON sm.id = mp.mention_id
            {where_clause}
        ''', params)
        total = cursor.fetchone()[0]

        # 分页查询
        offset = (page - 1) * per_page
        cursor.execute(f'''
            SELECT sm.id, sm.topic_id, sm.stock_code, sm.stock_name,
                   sm.mention_date, sm.mention_time, sm.context_snippet, sm.sentiment,
                   mp.price_at_mention,
                   mp.return_1d, mp.return_3d, mp.return_5d, mp.return_10d, mp.return_20d,
                   mp.excess_return_1d, mp.excess_return_3d, mp.excess_return_5d,
                   mp.excess_return_10d, mp.excess_return_20d,
                   mp.max_return, mp.max_drawdown
            FROM stock_mentions sm
            LEFT JOIN mention_performance mp ON sm.id = mp.mention_id
            {where_clause}
            ORDER BY {sort_col} {order_dir}
            LIMIT ? OFFSET ?
        ''', params + [per_page, offset])

        items = [dict(row) for row in cursor.fetchall()]
        conn.close()

        return {
            'total': total,
            'page': page,
            'per_page': per_page,
            'items': items
        }

    def get_stock_events(self, stock_code: str) -> Dict[str, Any]:
        """获取某只股票的全部提及事件 + 每次表现 + 完整话题文本 + 关联股票"""
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # 查询提及 + 表现 + 话题全文
        cursor.execute('''
            SELECT sm.id, sm.topic_id, sm.stock_code, sm.stock_name,
                   sm.mention_date, sm.mention_time, sm.context_snippet as context,
                   mp.price_at_mention,
                   mp.return_1d, mp.return_3d, mp.return_5d, mp.return_10d, mp.return_20d,
                   mp.excess_return_5d, mp.excess_return_10d,
                   mp.max_return, mp.max_drawdown,
                   tk.text as full_text
            FROM stock_mentions sm
            LEFT JOIN mention_performance mp ON sm.id = mp.mention_id
            LEFT JOIN talks tk ON sm.topic_id = tk.topic_id
            WHERE sm.stock_code = ?
            ORDER BY sm.mention_time DESC
        ''', (stock_code,))

        rows = cursor.fetchall()
        topic_ids = list(set(row['topic_id'] for row in rows if row['topic_id']))

        # 批量查询每个 topic 下的其他关联股票
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
                tid = r['topic_id']
                code = r['stock_code']
                if tid not in stocks_by_topic:
                    stocks_by_topic[tid] = []
                    seen[tid] = set()
                if code not in seen[tid]:
                    seen[tid].add(code)
                    stocks_by_topic[tid].append({
                        'stock_code': code,
                        'stock_name': r['stock_name'],
                    })

        events = []
        for row in rows:
            full_text = row['full_text'] or ''
            text_snippet = full_text[:500] + ('...' if len(full_text) > 500 else '')
            topic_id = row['topic_id']
            event = {
                'mention_id': row['id'],
                'topic_id': str(topic_id) if topic_id is not None else None,
                'group_id': self.group_id,
                'group_name': '',
                'stock_code': row['stock_code'],
                'stock_name': row['stock_name'],
                'mention_date': row['mention_date'],
                'mention_time': row['mention_time'],
                'context': row['context'],
                'full_text': full_text,
                'text_snippet': text_snippet,
                'stocks': stocks_by_topic.get(topic_id, []),
                'return_1d': row['return_1d'],
                'return_3d': row['return_3d'],
                'return_5d': row['return_5d'],
                'return_10d': row['return_10d'],
                'return_20d': row['return_20d'],
                'max_return': row['max_return'],
                'max_drawdown': row['max_drawdown'],
            }
            events.append(event)

        # 统计
        valid_returns = [e['return_5d'] for e in events if e.get('return_5d') is not None]
        win_count = sum(1 for r in valid_returns if r > 0)

        stock_name = events[0]['stock_name'] if events else ''

        conn.close()

        return {
            'stock_code': stock_code,
            'stock_name': stock_name,
            'total_mentions': len(events),
            'win_rate_5d': round(win_count / len(valid_returns) * 100, 1) if valid_returns else None,
            'avg_return_5d': round(sum(valid_returns) / len(valid_returns), 2) if valid_returns else None,
            'events': events
        }

    def get_stock_price_with_mentions(self, stock_code: str, days: int = 90) -> Dict[str, Any]:
        """获取股票价格走势 + 提及标注点"""
        end_date = datetime.now(BEIJING_TZ).strftime('%Y-%m-%d')
        start_date = (datetime.now(BEIJING_TZ) - timedelta(days=days)).strftime('%Y-%m-%d')

        prices = self.fetch_price_range(stock_code, start_date, end_date)

        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
            SELECT sm.mention_date, sm.context_snippet, sm.topic_id,
                   mp.return_5d, mp.max_return
            FROM stock_mentions sm
            LEFT JOIN mention_performance mp ON sm.id = mp.mention_id
            WHERE sm.stock_code = ? AND sm.mention_date >= ?
            ORDER BY sm.mention_date
        ''', (stock_code, start_date))

        mentions = [dict(row) for row in cursor.fetchall()]
        stock_name = self._stock_dict.get(stock_code, stock_code)
        conn.close()

        return {
            'stock_code': stock_code,
            'stock_name': stock_name,
            'prices': prices,
            'mentions': mentions
        }

    def get_win_rate_ranking(self, min_mentions: int = 2, return_period: str = 'return_5d',
                             limit: int = 1000, start_date: Optional[str] = None, end_date: Optional[str] = None,
                             page: int = 1, page_size: int = 20,
                             sort_by: str = 'win_rate', order: str = 'desc') -> Dict[str, Any]:
        """
        胜率排行榜：按提及后N日正收益率排序（支持时间过滤和分页）

        Args:
            min_mentions: 最少被提及次数（过滤噪音）
            return_period: 使用哪个收益率周期
            limit: 返回数量上限
            start_date: 仅统计该日期及之后的提及 (YYYY-MM-DD)
            end_date: 仅统计该日期及之前的提及 (YYYY-MM-DD)
            page: 页码 (1-indexed)
            page_size: 每页数量
            sort_by: 排序字段 (win_rate, total_mentions, avg_return)
            order: 排序方向 (asc, desc)
        """
        valid_periods = ['return_1d', 'return_3d', 'return_5d', 'return_10d', 'return_20d']
        if return_period not in valid_periods:
            return_period = 'return_5d'
        # 防御式兜底，避免前端传入 <= 0 导致筛选异常
        min_mentions = max(1, int(min_mentions or 1))

        # 映射超额收益列名: return_5d -> excess_return_5d
        excess_col = 'excess_' + return_period

        # Build date filter
        date_filter = ''
        date_params = []
        exclude_clause, exclude_params = build_sql_exclusion_clause('sm.stock_code', 'sm.stock_name')
        if start_date and end_date:
            date_filter = 'AND sm.mention_date BETWEEN ? AND ?'
            date_params = [start_date, end_date]
        elif start_date:
            date_filter = 'AND sm.mention_date >= ?'
            date_params = [start_date]
        elif end_date:
            date_filter = 'AND sm.mention_date <= ?'
            date_params = [end_date]

        conn = self._get_conn()
        cursor = conn.cursor()

        # Get total count first
        cursor.execute(f'''
            SELECT COUNT(*) FROM (
                SELECT sm.stock_code
                FROM stock_mentions sm
                JOIN mention_performance mp ON sm.id = mp.mention_id
                WHERE mp.{return_period} IS NOT NULL {date_filter} {exclude_clause}
                GROUP BY sm.stock_code
                HAVING COUNT(*) >= ?
            )
        ''', date_params + exclude_params + [min_mentions])
        total = cursor.fetchone()[0]

        # 如果用户设置了 limit，则将总数裁剪到 limit，避免翻页请求超出范围
        total_cap = max(0, min(total, limit)) if limit and limit > 0 else total

        # Paginated query
        offset = (page - 1) * page_size
        if offset >= total_cap:
            conn.close()
            return {
                'data': [],
                'total': total_cap,
                'page': page,
                'page_size': page_size
            }

        actual_limit = min(page_size, total_cap - offset if total_cap else page_size)

        order_dir = 'ASC' if str(order).lower() == 'asc' else 'DESC'
        
        if sort_by == 'total_mentions':
            order_clause = f"COUNT(*) {order_dir}, CAST(SUM(CASE WHEN mp.{return_period} > 0 THEN 1 ELSE 0 END) AS REAL) / COUNT(*) DESC"
        elif sort_by == 'avg_return':
            order_clause = f"AVG(mp.{return_period}) {order_dir}, CAST(SUM(CASE WHEN mp.{return_period} > 0 THEN 1 ELSE 0 END) AS REAL) / COUNT(*) DESC"
        elif sort_by == 'avg_benchmark_return':
            order_clause = f"AVG(mp.{return_period} - mp.{excess_col}) {order_dir}, AVG(mp.{return_period}) DESC"
        elif sort_by == 'latest_mention':
            order_clause = f"MAX(sm.mention_date) {order_dir}, COUNT(*) DESC"
        else: # default to win_rate
            order_clause = f"CAST(SUM(CASE WHEN mp.{return_period} > 0 THEN 1 ELSE 0 END) AS REAL) / COUNT(*) {order_dir}, AVG(mp.{return_period}) {order_dir}"

        cursor.execute(f'''
            SELECT
                sm.stock_code,
                sm.stock_name,
                COUNT(*) as total_mentions,
                SUM(CASE WHEN mp.{return_period} > 0 THEN 1 ELSE 0 END) as win_count,
                ROUND(AVG(mp.{return_period}), 2) as avg_return,
                ROUND(AVG(mp.{return_period} - mp.{excess_col}), 2) as avg_benchmark_return,
                ROUND(MAX(mp.max_return), 2) as best_max_return,
                ROUND(AVG(mp.max_return), 2) as avg_max_return,
                ROUND(MIN(mp.max_drawdown), 2) as worst_drawdown,
                MAX(sm.mention_date) as latest_mention
            FROM stock_mentions sm
            JOIN mention_performance mp ON sm.id = mp.mention_id
            WHERE mp.{return_period} IS NOT NULL {date_filter} {exclude_clause}
            GROUP BY sm.stock_code
            HAVING COUNT(*) >= ?
            ORDER BY {order_clause}
            LIMIT ? OFFSET ?
        ''', date_params + exclude_params + [min_mentions, actual_limit, offset])

        # Build stock dict for name resolution
        self._build_stock_dictionary()

        results = []
        for row in cursor.fetchall():
            total_m = row[2]
            wins = row[3]
            code = row[0]
            results.append({
                'stock_code': code,
                'stock_name': self._stock_dict.get(code, row[1]),
                'total_mentions': total_m,
                'win_count': wins,
                'win_rate': round(wins / total_m * 100, 1) if total_m > 0 else 0,
                'avg_return': row[4],
                'avg_benchmark_return': row[5],
                'best_max_return': row[6],
                'avg_max_return': row[7],
                'worst_drawdown': row[8],
                'latest_mention': row[9]
            })

        conn.close()
        return {
            'data': results,
            'total': total_cap,
            'page': page,
            'page_size': page_size
        }

    def get_sector_heatmap(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict]:
        """板块热度分析（支持时间过滤）

        Args:
            start_date: 仅统计该日期及之后的帖子 (YYYY-MM-DD)
            end_date: 仅统计该日期及之前的帖子 (YYYY-MM-DD)
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        # 获取帖子文本（带时间），可选时间过滤（end_date 包含当天）
        date_clause, date_params = build_topic_time_filter(
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
        ''', date_params)
        topics = cursor.fetchall()
        conn.close()

        return aggregate_sector_heat(topics, SECTOR_KEYWORDS)

    def get_sector_topics(self, sector: str, start_date: Optional[str] = None,
                          end_date: Optional[str] = None, page: int = 1,
                          page_size: int = 20) -> Dict[str, Any]:
        """获取指定板块的命中话题明细（按时间倒序）"""
        if sector not in SECTOR_KEYWORDS:
            raise ValueError(f"未知板块: {sector}")

        page = max(1, int(page or 1))
        page_size = max(1, min(int(page_size or 20), 100))
        keywords = [kw.lower() for kw in SECTOR_KEYWORDS[sector]]

        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        date_clause, date_params = build_topic_time_filter(
            start_date=start_date,
            end_date=end_date,
            column='t.create_time',
        )

        cursor.execute(f'''
            SELECT t.topic_id, t.create_time, tk.text
            FROM topics t
            JOIN talks tk ON t.topic_id = tk.topic_id
            WHERE tk.text IS NOT NULL AND tk.text != ''
              {date_clause}
            ORDER BY t.create_time DESC
        ''', date_params)
        candidates = cursor.fetchall()

        matched_topics: List[Dict[str, Any]] = []
        for row in candidates:
            text = row['text'] or ''
            text_lower = text.lower()
            matched_keywords = [kw for kw in keywords if kw in text_lower]
            if not matched_keywords:
                continue

            matched_topics.append({
                'topic_id': str(row['topic_id']) if row['topic_id'] is not None else None,
                'create_time': row['create_time'],
                'full_text': text,
                'text_snippet': text[:280] + ('...' if len(text) > 280 else ''),
                'matched_keywords': matched_keywords,
            })

        total = len(matched_topics)
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        page_items = matched_topics[start_idx:end_idx]

        if not page_items:
            conn.close()
            return {
                'total': total,
                'page': page,
                'page_size': page_size,
                'items': []
            }

        topic_ids = [item['topic_id'] for item in page_items]
        placeholders = ','.join('?' * len(topic_ids))
        exclude_clause, exclude_params = build_sql_exclusion_clause('stock_code', 'stock_name')
        cursor.execute(f'''
            SELECT topic_id, stock_code, stock_name
            FROM stock_mentions
            WHERE topic_id IN ({placeholders})
            {exclude_clause}
            ORDER BY mention_time DESC
        ''', topic_ids + exclude_params)

        stocks_by_topic: Dict[str, List[Dict[str, str]]] = {}
        seen_codes: Dict[str, set] = {}
        for row in cursor.fetchall():
            topic_id = str(row['topic_id'])
            if topic_id not in stocks_by_topic:
                stocks_by_topic[topic_id] = []
                seen_codes[topic_id] = set()

            code = row['stock_code']
            if code in seen_codes[topic_id]:
                continue
            seen_codes[topic_id].add(code)
            stocks_by_topic[topic_id].append({
                'stock_code': code,
                'stock_name': row['stock_name'],
            })

        conn.close()

        items = []
        for item in page_items:
            tid = str(item['topic_id'])
            items.append({
                'topic_id': str(item['topic_id']) if item['topic_id'] is not None else None,
                'create_time': item['create_time'],
                'text_snippet': item['text_snippet'],
                'full_text': item['full_text'],
                'matched_keywords': item['matched_keywords'],
                'stocks': stocks_by_topic.get(tid, []),
            })

        return {
            'total': total,
            'page': page,
            'page_size': page_size,
            'items': items
        }

    def get_signals(self, lookback_days: int = 7, min_mentions: int = 2, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict]:
        """
        信号雷达：近期高频提及 + 历史胜率高的股票

        条件：
        - 近 lookback_days 天内被提及 >= min_mentions 次 (或指定 start_date/end_date)
        - 历史提及后5日胜率 >= 50%
        """
        if start_date:
            cutoff_date = start_date
        else:
            cutoff_date = (datetime.now(BEIJING_TZ) - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        # 防御式兜底，避免前端传入 <= 0 导致筛选异常
        min_mentions = max(1, int(min_mentions or 1))

        date_condition = "sm.mention_date >= ?"
        params: List[Any] = [cutoff_date]
        exclude_clause, exclude_params = build_sql_exclusion_clause('sm.stock_code', 'sm.stock_name')
        
        if end_date:
            date_condition += " AND sm.mention_date <= ?"
            params.append(end_date)

        params.extend(exclude_params)
        params.append(min_mentions)

        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute(f'''
            SELECT
                sm.stock_code,
                sm.stock_name,
                COUNT(*) as recent_mentions,
                (SELECT COUNT(*) FROM stock_mentions sm2
                 JOIN mention_performance mp2 ON sm2.id = mp2.mention_id
                 WHERE sm2.stock_code = sm.stock_code AND mp2.return_5d > 0
                ) as historical_wins,
                (SELECT COUNT(*) FROM stock_mentions sm3
                 JOIN mention_performance mp3 ON sm3.id = mp3.mention_id
                 WHERE sm3.stock_code = sm.stock_code AND mp3.return_5d IS NOT NULL
                ) as historical_total,
                (SELECT ROUND(AVG(mp4.return_5d), 2)
                 FROM stock_mentions sm4
                 JOIN mention_performance mp4 ON sm4.id = mp4.mention_id
                 WHERE sm4.stock_code = sm.stock_code
                ) as historical_avg_return,
                MAX(sm.mention_date) as latest_mention
            FROM stock_mentions sm
            WHERE {date_condition} {exclude_clause}
            GROUP BY sm.stock_code
            HAVING COUNT(*) >= ?
            ORDER BY COUNT(*) DESC
        ''', params)

        # Build stock dict for name resolution
        self._build_stock_dictionary()

        signals = []
        for row in cursor.fetchall():
            hist_total = row[4]
            hist_wins = row[3]
            win_rate = round(hist_wins / hist_total * 100, 1) if hist_total > 0 else None
            code = row[0]

            signals.append({
                'stock_code': code,
                'stock_name': self._stock_dict.get(code, row[1]),
                'recent_mentions': row[2],
                'historical_win_rate': win_rate,
                'historical_avg_return': row[5],
                'latest_mention': row[6]
            })

        conn.close()

        # 按 recent_mentions 和 win_rate 综合排序
        signals.sort(key=lambda x: (
            x['recent_mentions'] * 2 + (x['historical_win_rate'] or 0) / 10
        ), reverse=True)

        return signals

    def get_summary_stats(self) -> Dict[str, Any]:
        """获取分析概览统计"""
        conn = self._get_conn()
        cursor = conn.cursor()

        stats = {}

        cursor.execute('SELECT COUNT(*) FROM stock_mentions')
        stats['total_mentions'] = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(DISTINCT stock_code) FROM stock_mentions')
        stats['unique_stocks'] = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(DISTINCT topic_id) FROM stock_mentions')
        stats['topics_with_stocks'] = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(*) FROM mention_performance')
        stats['performance_calculated'] = cursor.fetchone()[0]

        # 整体胜率 + 平均收益
        cursor.execute('''
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN return_5d > 0 THEN 1 ELSE 0 END) as wins,
                ROUND(AVG(return_5d), 2) as avg_return_5d,
                ROUND(AVG(excess_return_5d), 2) as avg_excess_5d
            FROM mention_performance
            WHERE return_5d IS NOT NULL
        ''')
        row = cursor.fetchone()
        if row and row[0] > 0:
            stats['overall_win_rate_5d'] = round(row[1] / row[0] * 100, 1)
            stats['total_with_returns'] = row[0]
            stats['avg_return_5d'] = row[2]
            stats['avg_excess_5d'] = row[3]
        else:
            stats['overall_win_rate_5d'] = None
            stats['avg_return_5d'] = None
            stats['avg_excess_5d'] = None

        # 最近7天新增提及数
        cursor.execute('''
            SELECT COUNT(*) FROM stock_mentions
            WHERE mention_date >= date('now', '-7 days')
        ''')
        stats['recent_7d_mentions'] = cursor.fetchone()[0]

        # 最近提及时间
        cursor.execute('SELECT MAX(mention_date) FROM stock_mentions')
        stats['latest_mention_date'] = cursor.fetchone()[0]

        # 最被提及的股票 Top 10
        cursor.execute('''
            SELECT stock_code, stock_name, COUNT(*) as cnt
            FROM stock_mentions
            GROUP BY stock_code
            ORDER BY cnt DESC
            LIMIT 10
        ''')
        stats['top_mentioned'] = [
            {'stock_code': r[0], 'stock_name': r[1], 'count': r[2]}
            for r in cursor.fetchall()
        ]

        conn.close()
        return stats

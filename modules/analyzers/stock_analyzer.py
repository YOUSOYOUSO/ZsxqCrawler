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
import hashlib
import threading
import concurrent.futures
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Any, Optional, Tuple
from pathlib import Path

import ahocorasick
import akshare as ak

from modules.shared.db_path_manager import get_db_path_manager
from modules.shared.logger_config import log_info, log_warning, log_error, log_debug
from modules.shared.market_data_store import MarketDataStore
from modules.shared.market_data_config import load_market_data_config
from modules.shared.stock_exclusion import is_excluded_stock, build_sql_exclusion_clause
from modules.shared.paths import get_config_path
from modules.shared.trading_calendar import TradingCalendar
from modules.shared.t0_board import compute_session_trade_date, build_t0_dual_board
from modules.analyzers.market_data_providers import normalize_code
from modules.analyzers.market_data_sync import MarketDataSyncService
from modules.analyzers.sector_heat import build_topic_time_filter, aggregate_sector_heat


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


class MarketDataUnavailableError(RuntimeError):
    """行情数据不可用，需中止本轮收益计算任务。"""


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
    # mini_racer/V8 在并发初始化下存在崩溃风险，AkShare 调用统一串行化
    _akshare_lock = threading.Lock()
    _snapshot_lock = threading.RLock()
    _snapshot_cache: Dict[str, Dict[str, Any]] = {}
    _snapshot_inflight: Dict[str, Dict[str, Any]] = {}
    _snapshot_fail_cooldown: Dict[str, Dict[str, Any]] = {}
    _analysis_cache_lock = threading.RLock()
    _analysis_cache: Dict[str, Any] = {}
    _analysis_cache_time: Dict[str, datetime] = {}
    _analysis_cache_ttl: Dict[str, int] = {}
    _manual_refresh_guard_lock = threading.RLock()
    _manual_refresh_guard: Dict[str, float] = {}
    _passive_sync_guard_lock = threading.RLock()
    _passive_sync_guard: Dict[str, float] = {}
    _events_refresh_state_lock = threading.RLock()
    _events_refresh_state: Dict[str, Dict[str, Any]] = {}

    # 本地缓存时效（秒），默认12小时
    DICT_CACHE_TTL_SECONDS = int(os.environ.get("STOCK_DICT_CACHE_TTL_SECONDS", "43200"))
    DICT_CACHE_FILE = "stock_dict_cache.json"
    EXTRACTOR_VERSION = os.environ.get("STOCK_EXTRACTOR_VERSION", "v2")
    TOPIC_BACKFILL_DAYS = int(os.environ.get("TOPIC_ANALYSIS_BACKFILL_DAYS", "30"))
    SNAPSHOT_TTL_SECONDS = int(os.environ.get("T0_SNAPSHOT_TTL_SECONDS", "15"))
    SNAPSHOT_FAIL_COOLDOWN_SECONDS = int(os.environ.get("T0_SNAPSHOT_FAIL_COOLDOWN_SECONDS", "45"))
    FINALIZED_CACHE_TTL_SECONDS = int(os.environ.get("ANALYSIS_FINALIZED_CACHE_TTL_SECONDS", "900"))
    LIVE_CACHE_TTL_SECONDS = int(os.environ.get("ANALYSIS_LIVE_CACHE_TTL_SECONDS", "60"))
    MANUAL_REFRESH_COOLDOWN_SECONDS = int(os.environ.get("T0_MANUAL_REFRESH_COOLDOWN_SECONDS", "10"))
    PASSIVE_SYNC_COOLDOWN_SECONDS = int(os.environ.get("T0_PASSIVE_SYNC_COOLDOWN_SECONDS", "300"))
    PERF_CALC_MAX_WORKERS = int(os.environ.get("PERF_CALC_MAX_WORKERS", "6"))
    PERF_DB_BATCH_SIZE = int(os.environ.get("PERF_DB_BATCH_SIZE", "200"))
    PERF_PREWARM_ENABLED = os.environ.get("PERF_PREWARM_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
    PERF_PREWARM_CHUNK_SIZE = int(os.environ.get("PERF_PREWARM_CHUNK_SIZE", "200"))
    PERF_PROGRESS_LOG_INTERVAL_SECONDS = float(os.environ.get("PERF_PROGRESS_LOG_INTERVAL_SECONDS", "15"))
    def __init__(self, group_id: str, log_callback=None, stop_check: Optional[Callable[[], bool]] = None):
        self.group_id = group_id
        self.log_callback = log_callback
        self.stop_check = stop_check
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
        self.market_store = MarketDataStore()
        self.market_sync = MarketDataSyncService(store=self.market_store, log_callback=self.log)

    def log(self, message: str):
        """统一日志"""
        if self.log_callback:
            self.log_callback(message)
        log_info(message)

    def _is_stop_requested(self) -> bool:
        if not self.stop_check:
            return False
        try:
            return bool(self.stop_check())
        except Exception:
            return False

    def get_data_anchor_date(self) -> str:
        anchor = self.market_store.get_latest_trade_date(only_final=True)
        if anchor:
            return str(anchor)
        return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")

    def _normalize_finalized_date_window(
        self,
        start_date: Optional[str],
        end_date: Optional[str],
        default_days: Optional[int] = None,
    ) -> Tuple[Optional[str], str, str]:
        anchor_date = self.get_data_anchor_date()
        requested_end = str(end_date or anchor_date)
        effective_end = min(requested_end, anchor_date)

        effective_start = str(start_date) if start_date else None
        if not effective_start and default_days:
            try:
                end_dt = datetime.strptime(effective_end, "%Y-%m-%d")
                effective_start = (end_dt - timedelta(days=int(default_days))).strftime("%Y-%m-%d")
            except Exception:
                effective_start = None

        if effective_start and effective_start > effective_end:
            effective_start = effective_end

        return effective_start, effective_end, anchor_date

    def _cache_key(self, base: str, anchor_date: Optional[str] = None) -> str:
        return f"group:{self.group_id}|anchor:{anchor_date or ''}|{base}"

    def _get_cached_analysis(self, key: str) -> Optional[Any]:
        with StockAnalyzer._analysis_cache_lock:
            ts = StockAnalyzer._analysis_cache_time.get(key)
            if ts is None:
                return None
            ttl = int(StockAnalyzer._analysis_cache_ttl.get(key, self.FINALIZED_CACHE_TTL_SECONDS))
            if (datetime.now() - ts).total_seconds() >= ttl:
                StockAnalyzer._analysis_cache.pop(key, None)
                StockAnalyzer._analysis_cache_time.pop(key, None)
                StockAnalyzer._analysis_cache_ttl.pop(key, None)
                return None
            return StockAnalyzer._analysis_cache.get(key)

    def _set_cached_analysis(self, key: str, payload: Any, ttl_seconds: int) -> None:
        with StockAnalyzer._analysis_cache_lock:
            StockAnalyzer._analysis_cache[key] = payload
            StockAnalyzer._analysis_cache_time[key] = datetime.now()
            StockAnalyzer._analysis_cache_ttl[key] = int(ttl_seconds)

    def _with_meta(
        self,
        payload: Dict[str, Any],
        *,
        cache_hit: bool,
        data_mode: str,
        anchor_date: Optional[str] = None,
        effective_start_date: Optional[str] = None,
        effective_end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        data = dict(payload)
        data["_cache_hit"] = bool(cache_hit)
        data["_meta"] = {
            "cache_hit": bool(cache_hit),
            "data_mode": str(data_mode),
            "anchor_date": anchor_date,
            "effective_start_date": effective_start_date,
            "effective_end_date": effective_end_date,
        }
        return data

    def _get_conn(self):
        """获取带 WAL 模式和超时的数据库连接，确保表已就绪"""
        # 确保表存在，避免旧数据库缺表导致查询失败
        self._ensure_stock_tables()

        conn = sqlite3.connect(self.topics_db_path, check_same_thread=False, timeout=30)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout=30000')
        return conn

    @staticmethod
    def _topic_text_hash(text: str) -> str:
        return hashlib.sha1((text or "").encode("utf-8")).hexdigest()

    def _upsert_topic_analysis_state(
        self,
        cursor: sqlite3.Cursor,
        topic_id: int,
        text_hash: str,
        perf_status: str,
        last_error: str = "",
    ) -> None:
        now = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute(
            '''
            INSERT INTO topic_analysis_state
            (topic_id, text_hash, extractor_version, extracted_at, perf_status, last_error, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(topic_id) DO UPDATE SET
                text_hash = excluded.text_hash,
                extractor_version = excluded.extractor_version,
                extracted_at = excluded.extracted_at,
                perf_status = excluded.perf_status,
                last_error = excluded.last_error,
                updated_at = excluded.updated_at
            ''',
            (topic_id, text_hash, self.EXTRACTOR_VERSION, now, perf_status, last_error, now),
        )

    def _reset_topic_mentions(self, cursor: sqlite3.Cursor, topic_id: int) -> None:
        cursor.execute('SELECT id FROM stock_mentions WHERE topic_id = ?', (topic_id,))
        mids = [int(r[0]) for r in cursor.fetchall()]
        if mids:
            placeholders = ",".join(["?"] * len(mids))
            cursor.execute(f'DELETE FROM mention_performance WHERE mention_id IN ({placeholders})', mids)
        cursor.execute('DELETE FROM stock_mentions WHERE topic_id = ?', (topic_id,))

    # ========== 数据库初始化 ==========

    def _ensure_stock_tables(self):
        """在话题数据库中创建股票分析相关表（幂等，容错旧库缺表/缺列）"""
        db_key = os.path.abspath(self.topics_db_path)

        with StockAnalyzer._init_lock:
            # 进程内同一数据库只初始化一次，避免频繁建表事务造成锁竞争。
            if db_key in StockAnalyzer._initialized_dbs:
                return

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
                    t0_buy_price REAL,
                    t0_buy_ts TEXT,
                    t0_buy_source TEXT,
                    t0_end_price_rt REAL,
                    t0_end_price_rt_ts TEXT,
                    t0_end_price_close REAL,
                    t0_end_price_close_ts TEXT,
                    t0_return_rt REAL,
                    t0_return_close REAL,
                    t0_status TEXT,
                    t0_note TEXT,
                    FOREIGN KEY (mention_id) REFERENCES stock_mentions(id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS topic_analysis_state (
                    topic_id INTEGER PRIMARY KEY,
                    text_hash TEXT NOT NULL,
                    extractor_version TEXT NOT NULL,
                    extracted_at TEXT,
                    perf_status TEXT DEFAULT 'pending',
                    last_error TEXT DEFAULT '',
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # 兼容旧表：添加新列（如果不存在）
            for col in ['return_60d', 'return_120d', 'return_250d',
                        'excess_return_60d', 'excess_return_120d', 'excess_return_250d',
                        'freeze_level', 't0_buy_price', 't0_buy_ts', 't0_buy_source',
                        't0_end_price_rt', 't0_end_price_rt_ts', 't0_end_price_close',
                        't0_end_price_close_ts', 't0_return_rt', 't0_return_close',
                        't0_status', 't0_note']:
                try:
                    cursor.execute(f'ALTER TABLE mention_performance ADD COLUMN {col} REAL')
                except Exception:
                    pass  # 列已存在

            # 创建索引
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sm_stock_code ON stock_mentions(stock_code)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sm_mention_date ON stock_mentions(mention_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sm_topic_id ON stock_mentions(topic_id)')
            try:
                cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS uq_sm_topic_stock ON stock_mentions(topic_id, stock_code)')
            except sqlite3.IntegrityError:
                # 旧库存在重复数据时，唯一索引会创建失败；提示先运行维护脚本清洗历史数据。
                log_warning(
                    "检测到 stock_mentions 历史重复数据，暂未启用 uq_sm_topic_stock。"
                    "请运行 scripts/maintenance/dedup_stock_mentions.py 后重试。"
                )
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_mp_stock_code ON mention_performance(stock_code)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_mp_mention_date ON mention_performance(mention_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_spc_date ON stock_price_cache(trade_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_tas_perf_status ON topic_analysis_state(perf_status)')

            conn.commit()
            conn.close()
            StockAnalyzer._initialized_dbs.add(db_key)

    # ========== 股票字典构建 ==========

    def _build_stock_dictionary(self):
        """构建/复用 A 股股票字典，优先使用进程缓存和本地缓存"""
        self.log("正在准备A股股票字典")

        if self._automaton is not None:
            total = len(self._name_to_code)
            self.log("加载股票清单（实例缓存命中，跳过）")
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
            self.log("加载股票清单（进程缓存命中，跳过）")
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
                self.log("加载股票清单（进程缓存命中，跳过）")
                self.log(f"字典处理进度 {total}/{total}")
                self.log(f"索引构建进度 {total}/{total}")
                self.log("股票字典就绪")
                return

            self.log("加载股票字典清单")
            stock_dict, name_to_code = self._load_stock_dictionary_from_cache()

            if not stock_dict:
                try:
                    stock_dict, name_to_code = self._fetch_stock_dictionary_from_configured_source()
                    self._save_stock_dictionary_cache(stock_dict, name_to_code)
                except Exception as e:
                    log_warning(f"从配置行情源构建股票字典失败: {e}")
                    stale_stock_dict, stale_name_to_code = self._load_stock_dictionary_from_cache(allow_expired=True)
                    if stale_stock_dict and stale_name_to_code:
                        self.log("⚠️ 配置行情源获取失败，已回退到本地过期缓存")
                        stock_dict, name_to_code = stale_stock_dict, stale_name_to_code
                    else:
                        raise
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
        config/stock_aliases.json 格式: {"别名": "标准股票名称"}
        """
        alias_file = get_config_path("stock_aliases.json")
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

    def _load_stock_dictionary_from_cache(self, allow_expired: bool = False) -> Tuple[Dict[str, str], Dict[str, str]]:
        """尝试从本地缓存读取股票字典"""
        try:
            if not self._dict_cache_path.exists():
                return {}, {}

            with open(self._dict_cache_path, "r", encoding="utf-8") as f:
                payload = json.load(f)

            built_at = float(payload.get("built_at", 0))
            age = time.time() - built_at
            if age > self.DICT_CACHE_TTL_SECONDS:
                if not allow_expired:
                    self.log("♻️ 本地股票字典缓存已过期，准备刷新")
                    return {}, {}
                self.log("⚠️ 本地股票字典缓存已过期，配置行情源异常时将回退使用")

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

    @staticmethod
    def _build_dict_from_symbol_rows(rows: List[Dict[str, Any]]) -> Tuple[Dict[str, str], Dict[str, str]]:
        stock_dict: Dict[str, str] = {}
        name_to_code: Dict[str, str] = {}
        for row in rows:
            code = str(row.get("stock_code", "")).strip().upper()
            name = str(row.get("stock_name", "")).strip()
            if not code or not name:
                continue
            if len(name) < 2 or name in EXCLUDE_WORDS:
                continue
            stock_dict[code] = name
            name_to_code[name] = code
        return stock_dict, name_to_code

    def _fetch_stock_dictionary_from_market_sync(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        # 优先复用本地行情符号库，避免每次都触发外部请求
        local_rows = self.market_store.list_symbols()
        if local_rows:
            stock_dict, name_to_code = self._build_dict_from_symbol_rows(local_rows)
            if stock_dict and name_to_code:
                self.log(f"⚡ 从本地行情符号库加载字典（{len(name_to_code)}只）")
                return stock_dict, name_to_code

        # 本地为空时，按当前配置的数据源同步 symbols（会遵循 provider_failover_enabled）
        providers = [str(p).strip().lower() for p in self.market_sync.provider_order]
        self.log(
            f"从配置行情源同步股票清单（providers={providers}, failover={self.market_sync.provider_failover_enabled}）"
        )
        sync_res = self.market_sync.sync_symbols()
        if not sync_res.get("success"):
            raise RuntimeError(sync_res.get("message") or "sync_symbols failed")

        rows = self.market_store.list_symbols()
        stock_dict, name_to_code = self._build_dict_from_symbol_rows(rows)
        if not stock_dict or not name_to_code:
            raise RuntimeError("行情源返回空股票清单")
        self.log(
            f"字典清单来源: {sync_res.get('provider_used', 'unknown')}（{len(name_to_code)}只）"
        )
        return stock_dict, name_to_code

    def _fetch_stock_dictionary_from_configured_source(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        """
        按 market_data 配置获取股票字典。
        - 首选 MarketDataSyncService（遵循 providers/provider_failover_enabled）
        - 仅当显式允许时，才回退 AkShare 直连
        """
        try:
            return self._fetch_stock_dictionary_from_market_sync()
        except Exception as e:
            cfg = load_market_data_config()
            providers = [str(p).strip().lower() for p in cfg.get("providers", [])]
            failover_enabled = bool(cfg.get("provider_failover_enabled", True))
            allow_akshare_fallback = bool(cfg.get("provider_failover_enabled", True)) and (
                "akshare" in providers
            )
            if allow_akshare_fallback:
                log_warning(f"配置行情源同步失败，尝试 AkShare 兜底: {e}")
                return self._fetch_stock_dictionary_from_akshare()
            log_warning(
                "配置行情源同步失败，且 AkShare 兜底未启用："
                f"providers={providers}, failover={failover_enabled}, error={e}"
            )
            raise

    def _fetch_stock_dictionary_from_akshare(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        """从 AkShare 获取全量股票字典，并输出构建进度"""
        self.log("从AkShare获取清单")
        max_attempts = int(os.environ.get("AKSHARE_STOCK_DICT_MAX_RETRIES", "3"))
        for attempt in range(1, max_attempts + 1):
            try:
                with StockAnalyzer._akshare_lock:
                    df = ak.stock_zh_a_spot_em()
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
                    elif code.startswith(('4', '8', '9')):
                        full_code = f"{code}.BJ"
                    else:
                        full_code = code

                    stock_dict[full_code] = name
                    name_to_code[name] = full_code

                    if idx % 1000 == 0 or idx == total_rows:
                        self.log(f"字典处理进度 {idx}/{total_rows}")

                return stock_dict, name_to_code
            except Exception as e:
                if attempt >= max_attempts:
                    log_error(f"构建股票字典失败: {e}")
                    raise
                sleep_seconds = min(2 ** (attempt - 1), 5)
                log_warning(
                    f"AkShare获取股票清单失败（第{attempt}/{max_attempts}次）: {e}，{sleep_seconds}s后重试"
                )
                time.sleep(sleep_seconds)

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

    def fetch_price_range(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
        data_mode: str = "live",
        perf_context: Optional[Dict[str, Any]] = None,
    ) -> List[Dict]:
        """
        获取股票区间行情
        - 优先使用全局持久化行情库 market_daily_prices
        - 历史日仅使用 is_final=1
        - 当日数据在 15:05 前允许 is_final=0（盘中参考），15:05 后要求 is_final=1
        """
        normalized_code, _ = normalize_code(stock_code)
        mode = str(data_mode or "live").strip().lower()
        anchor_date = self.get_data_anchor_date()
        effective_end_date = min(str(end_date), anchor_date) if mode == "finalized_only" else str(end_date)
        today = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
        in_preopen_silent_window = mode != "finalized_only" and self._is_preopen_window() and effective_end_date >= today
        if in_preopen_silent_window:
            # 隔夜静默窗：收盘后到次日开盘前，不为“今日”触发外网回补。
            effective_end_date = TradingCalendar.shared().prev_trading_day(today)
        allow_today_unfinal = (
            (mode != "finalized_only")
            and (effective_end_date >= today)
            and (not self.market_store.is_market_closed_now())
            and (not self._is_preopen_window())
        )
        network_backfill_triggered = False
        rows = self.market_store.get_price_range(
            stock_code=normalized_code,
            start_date=start_date,
            end_date=effective_end_date,
            allow_today_unfinal=allow_today_unfinal,
        )

        if (not rows) or (not self._is_price_window_sufficient(rows, start_date, effective_end_date)):
            if in_preopen_silent_window:
                log_debug(
                    f"price_range preopen silent-window skip backfill, code={normalized_code}, "
                    f"start={start_date}, effective_end={effective_end_date}, rows={len(rows or [])}"
                )
            else:
                should_backfill = True
                if isinstance(perf_context, dict):
                    lock = perf_context.get("lock")
                    lock_cm = lock if hasattr(lock, "__enter__") else nullcontext()
                    with lock_cm:
                        attempted = perf_context.setdefault("backfill_attempted_symbols", set())
                        if normalized_code in attempted:
                            should_backfill = False
                        else:
                            attempted.add(normalized_code)
                if not should_backfill:
                    log_debug(
                        f"price_range skip duplicate backfill, code={normalized_code}, "
                        f"start={start_date}, effective_end={effective_end_date}"
                    )
                else:
                    # 缺数据或窗口覆盖不足时，按需扩大增量回补单标的
                    history_days = self._calc_history_days_for_backfill(start_date)
                    network_backfill_triggered = True
                    t0 = time.perf_counter()
                    sync_res = self.market_sync.sync_daily_incremental(
                        symbols=[normalized_code],
                        include_index=False,
                        history_days=history_days,
                    )
                    network_elapsed = time.perf_counter() - t0
                    if isinstance(perf_context, dict):
                        lock = perf_context.get("lock")
                        lock_cm = lock if hasattr(lock, "__enter__") else nullcontext()
                        with lock_cm:
                            perf_context["network_seconds"] = float(perf_context.get("network_seconds", 0.0) or 0.0) + network_elapsed
                            perf_context.setdefault("network_backfill_symbols", set()).add(normalized_code)
                    if not sync_res.get("success"):
                        raise MarketDataUnavailableError(
                            f"从持久行情库回补 {normalized_code} 失败: {sync_res.get('message') or sync_res}"
                        )
                    rows = self.market_store.get_price_range(
                        stock_code=normalized_code,
                        start_date=start_date,
                        end_date=effective_end_date,
                        allow_today_unfinal=allow_today_unfinal,
                    )
                    if not rows:
                        raise MarketDataUnavailableError(f"从持久行情库回补 {normalized_code} 后仍无可用行情")

        # 收盘后，如果区间触及当日且当日仍非 final，触发一次收盘冻结
        if (
            mode != "finalized_only"
            and
            effective_end_date >= today
            and self.market_store.is_market_closed_now()
            and not self.market_store.has_final_for_symbol_date(normalized_code, today)
        ):
            try:
                self.market_sync.finalize_today_after_close(symbols=[normalized_code])
                rows = self.market_store.get_price_range(
                    stock_code=normalized_code,
                    start_date=start_date,
                    end_date=effective_end_date,
                    allow_today_unfinal=False,
                )
            except Exception as e:
                log_warning(f"收盘冻结回补 {normalized_code} 失败: {e}")
        log_debug(
            f"price_range mode={mode}, code={normalized_code}, anchor_date={anchor_date}, "
            f"effective_end_date={effective_end_date}, network_backfill_triggered={network_backfill_triggered}"
        )

        return [
            {
                "trade_date": r["trade_date"],
                "open": r["open"],
                "close": r["close"],
                "high": r["high"],
                "low": r["low"],
                "change_pct": r["change_pct"],
                "volume": r["volume"],
            }
            for r in rows
        ]

    def _calc_history_days_for_backfill(self, start_date: str) -> int:
        """按目标起始日估算需要回补的历史天数。"""
        default_days = int(self.market_sync.config.get("incremental_history_days", 20))
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            today_dt = datetime.now(BEIJING_TZ).date()
            needed = (today_dt - start_dt).days + 7
            return max(default_days, needed, 20)
        except Exception:
            return max(default_days, 20)

    def _is_price_window_sufficient(
        self,
        rows: List[Dict[str, Any]],
        start_date: str,
        end_date: str,
        max_edge_gap_days: int = 7,
    ) -> bool:
        """
        判断本地行情窗口是否足够覆盖请求区间。
        允许边界有少量自然缺口（周末/节假日）。
        """
        if not rows:
            return False
        try:
            first_dt = datetime.strptime(str(rows[0]["trade_date"]), "%Y-%m-%d").date()
            last_dt = datetime.strptime(str(rows[-1]["trade_date"]), "%Y-%m-%d").date()
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
            today_dt = datetime.now(BEIJING_TZ).date()
            effective_end = min(end_dt, today_dt)
            gap_left = (first_dt - start_dt).days
            gap_right = (effective_end - last_dt).days
            return gap_left <= max_edge_gap_days and gap_right <= max_edge_gap_days
        except Exception:
            return True

    def _fetch_index_price(
        self,
        start_date: str,
        end_date: str,
        data_mode: str = "live",
        perf_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, float]:
        """获取沪深300指数行情（用于计算超额收益）"""
        index_code = "000300.SH"
        mode = str(data_mode or "live").strip().lower()
        anchor_date = self.get_data_anchor_date()
        effective_end_date = min(str(end_date), anchor_date) if mode == "finalized_only" else str(end_date)
        today = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
        in_preopen_silent_window = mode != "finalized_only" and self._is_preopen_window() and effective_end_date >= today
        if in_preopen_silent_window:
            effective_end_date = TradingCalendar.shared().prev_trading_day(today)
        allow_today_unfinal = (
            (mode != "finalized_only")
            and (effective_end_date >= today)
            and (not self.market_store.is_market_closed_now())
            and (not self._is_preopen_window())
        )
        network_backfill_triggered = False
        rows = self.market_store.get_price_range(
            stock_code=index_code,
            start_date=start_date,
            end_date=effective_end_date,
            allow_today_unfinal=allow_today_unfinal,
        )
        if (not rows) or (not self._is_price_window_sufficient(rows, start_date, effective_end_date)):
            if in_preopen_silent_window:
                log_debug(
                    f"index_price preopen silent-window skip backfill, start={start_date}, "
                    f"effective_end={effective_end_date}, rows={len(rows or [])}"
                )
            else:
                should_backfill = True
                if isinstance(perf_context, dict):
                    lock = perf_context.get("lock")
                    lock_cm = lock if hasattr(lock, "__enter__") else nullcontext()
                    with lock_cm:
                        attempted = perf_context.setdefault("backfill_attempted_symbols", set())
                        if index_code in attempted:
                            should_backfill = False
                        else:
                            attempted.add(index_code)
                if not should_backfill:
                    log_debug(
                        f"index_price skip duplicate backfill, start={start_date}, effective_end={effective_end_date}"
                    )
                else:
                    history_days = self._calc_history_days_for_backfill(start_date)
                    network_backfill_triggered = True
                    t0 = time.perf_counter()
                    sync_res = self.market_sync.sync_daily_incremental(
                        sync_equities=False,
                        include_index=True,
                        history_days=history_days,
                    )
                    network_elapsed = time.perf_counter() - t0
                    if isinstance(perf_context, dict):
                        lock = perf_context.get("lock")
                        lock_cm = lock if hasattr(lock, "__enter__") else nullcontext()
                        with lock_cm:
                            perf_context["network_seconds"] = float(perf_context.get("network_seconds", 0.0) or 0.0) + network_elapsed
                            perf_context.setdefault("network_backfill_symbols", set()).add(index_code)
                    if not sync_res.get("success"):
                        raise MarketDataUnavailableError(
                            f"获取沪深300指数失败: {sync_res.get('message') or sync_res}"
                        )
                    rows = self.market_store.get_price_range(
                        stock_code=index_code,
                        start_date=start_date,
                        end_date=effective_end_date,
                        allow_today_unfinal=allow_today_unfinal,
                    )
                    if not rows:
                        raise MarketDataUnavailableError("获取沪深300指数失败: 回补后仍无可用行情")
        if mode != "finalized_only" and effective_end_date >= today and self.market_store.is_market_closed_now():
            if not self.market_store.has_final_for_symbol_date(index_code, today):
                try:
                    self.market_sync.finalize_today_after_close(symbols=[], sync_equities=False)
                    rows = self.market_store.get_price_range(
                        stock_code=index_code,
                        start_date=start_date,
                        end_date=effective_end_date,
                        allow_today_unfinal=False,
                    )
                except Exception as e:
                    log_warning(f"收盘冻结回补沪深300失败: {e}")
        log_debug(
            f"index_price mode={mode}, code={index_code}, anchor_date={anchor_date}, "
            f"effective_end_date={effective_end_date}, network_backfill_triggered={network_backfill_triggered}"
        )
        return {r["trade_date"]: r["close"] for r in rows if r.get("close") is not None}

    def _is_preopen_window(self, open_time: str = "09:30") -> bool:
        now = datetime.now(BEIJING_TZ)
        try:
            h, m = [int(x) for x in str(open_time).split(":", 1)]
        except Exception:
            h, m = 9, 30
        now_minutes = now.hour * 60 + now.minute
        open_minutes = h * 60 + m
        return now_minutes < open_minutes

    def _get_market_phase(self, open_time: str = "09:30", close_finalize_time: Optional[str] = None) -> str:
        now = datetime.now(BEIJING_TZ)
        try:
            oh, om = [int(x) for x in str(open_time).split(":", 1)]
        except Exception:
            oh, om = 9, 30
        try:
            close_raw = str(close_finalize_time or self.market_store.close_finalize_time or "15:05")
            ch, cm = [int(x) for x in close_raw.split(":", 1)]
        except Exception:
            ch, cm = 15, 5
        now_minutes = now.hour * 60 + now.minute
        open_minutes = oh * 60 + om
        close_minutes = ch * 60 + cm
        if now_minutes < open_minutes:
            return "preopen"
        if now_minutes < close_minutes:
            return "intraday"
        return "postclose"

    def _maybe_passive_sync_for_t0(self, stock_code: str) -> bool:
        """
        详情被动触发：
        - 盘前：不触发外网
        - 盘中：当日快照缺失时，按单标的触发一次增量回补（带冷却）
        - 盘后：当日未固化时，按单标的触发一次 finalize（带冷却）
        """
        phase = self._get_market_phase(open_time="09:30")
        if phase == "preopen":
            return False

        today = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
        normalized_code, _ = normalize_code(stock_code)
        guard_key = f"{self.group_id}:{today}:{phase}:{normalized_code}"
        now_ts = time.time()
        with StockAnalyzer._passive_sync_guard_lock:
            last_ts = float(StockAnalyzer._passive_sync_guard.get(guard_key, 0.0) or 0.0)
            if (now_ts - last_ts) < float(self.PASSIVE_SYNC_COOLDOWN_SECONDS):
                return False

        if phase == "intraday":
            snap = self.market_store.get_symbol_day_snapshot_info(stock_code=normalized_code, trade_date=today)
            need_sync = (not bool(snap.get("exists"))) or (snap.get("open") is None)
            if need_sync:
                log_info(
                    f"[t0_passive] intraday snapshot missing, trigger incremental sync: "
                    f"stock={normalized_code}, trade_date={today}, exists={snap.get('exists')}, open={snap.get('open')}"
                )
                self.market_sync.sync_daily_incremental(
                    history_days=3,
                    symbols=[normalized_code],
                    include_index=False,
                    finalize_today=False,
                )
                with StockAnalyzer._passive_sync_guard_lock:
                    StockAnalyzer._passive_sync_guard[guard_key] = now_ts
                return True
            return False

        if phase == "postclose" and (not self.market_store.has_final_for_symbol_date(normalized_code, today)):
            log_info(
                f"[t0_passive] postclose snapshot not finalized, trigger finalize: "
                f"stock={normalized_code}, trade_date={today}"
            )
            self.market_sync.finalize_today_after_close(symbols=[normalized_code])
            with StockAnalyzer._passive_sync_guard_lock:
                StockAnalyzer._passive_sync_guard[guard_key] = now_ts
            return True
        return False

    def _parse_dt(self, value: Optional[str]) -> Optional[datetime]:
        raw = str(value or "").strip()
        if not raw:
            return None
        try:
            if raw.endswith("+0800"):
                raw = raw[:-5] + "+08:00"
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                return dt.replace(tzinfo=BEIJING_TZ)
            return dt.astimezone(BEIJING_TZ)
        except Exception:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(raw[:19], fmt)
                    return dt.replace(tzinfo=BEIJING_TZ)
                except Exception:
                    continue
        return None

    def _get_price_on_or_after(self, stock_code: str, trade_date: str, allow_today_unfinal: bool = False) -> Optional[Dict[str, Any]]:
        start = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=3)).strftime("%Y-%m-%d")
        end = (datetime.strptime(trade_date, "%Y-%m-%d") + timedelta(days=5)).strftime("%Y-%m-%d")
        normalized, _ = normalize_code(stock_code)
        rows = self.market_store.get_price_range(
            stock_code=normalized,
            start_date=start,
            end_date=end,
            allow_today_unfinal=allow_today_unfinal,
        )
        if not rows:
            return None
        for row in rows:
            if str(row.get("trade_date", "")) >= trade_date and row.get("close") is not None:
                return dict(row)
        for row in reversed(rows):
            if row.get("close") is not None:
                return dict(row)
        return None

    def _get_recent_local_price(self, stock_code: str, anchor_date: str) -> Tuple[Optional[float], Optional[str], str]:
        try:
            dt = datetime.strptime(anchor_date, "%Y-%m-%d")
        except Exception:
            dt = datetime.now(BEIJING_TZ)
        normalized, _ = normalize_code(stock_code)
        start = (dt - timedelta(days=20)).strftime("%Y-%m-%d")
        end = (dt + timedelta(days=3)).strftime("%Y-%m-%d")
        rows = self.market_store.get_price_range(
            stock_code=normalized,
            start_date=start,
            end_date=end,
            allow_today_unfinal=True,
        )
        if not rows:
            return None, None, "本地无可回退成交价"
        best = None
        for row in rows:
            if row.get("close") is None:
                continue
            if str(row.get("trade_date", "")) >= anchor_date:
                best = row
                break
            best = row
        if not best or best.get("close") is None:
            return None, None, "本地无可回退成交价"
        return float(best["close"]), str(best.get("trade_date") or ""), ""

    def _get_local_day_price(self, stock_code: str, trade_date: str) -> Optional[Dict[str, Any]]:
        normalized, _ = normalize_code(stock_code)
        rows = self.market_store.get_price_range(
            stock_code=normalized,
            start_date=trade_date,
            end_date=trade_date,
            allow_today_unfinal=True,
        )
        return dict(rows[-1]) if rows else None

    def _get_historical_buy_fallback(self, stock_code: str, mention_time: str, mention_date: str) -> Tuple[Optional[float], Optional[str], str, str]:
        dt = self._parse_dt(mention_time) or self._parse_dt(mention_date) or datetime.now(BEIJING_TZ)
        day = dt.strftime("%Y-%m-%d")
        minute = dt.hour * 60 + dt.minute
        close_cut = 15 * 60 + 5
        open_cut = 9 * 60 + 30
        cal = TradingCalendar.shared()

        # 盘前：用前一交易日收盘近似
        if minute < open_cut:
            prev_trade = cal.prev_trading_day(day)
            prev_row = self._get_local_day_price(stock_code, prev_trade)
            if prev_row and prev_row.get("close") is not None:
                return float(prev_row["close"]), prev_trade, "fallback_recent", "历史盘前提及回退前收价近似"

        # 盘中：用当日开盘价近似
        if open_cut <= minute < close_cut:
            day_row = self._get_local_day_price(stock_code, day)
            if day_row and day_row.get("open") is not None:
                return float(day_row["open"]), day, "fallback_recent", "历史盘中提及回退当日开盘价近似"

        # 收盘后：用当日收盘价近似
        day_row = self._get_local_day_price(stock_code, day)
        if day_row and day_row.get("close") is not None:
            return float(day_row["close"]), day, "fallback_recent", "历史提及回退当日收盘价近似"

        fb_price, fb_date, fb_err = self._get_recent_local_price(stock_code, mention_date)
        if fb_price is not None:
            return float(fb_price), fb_date or mention_date, "fallback_recent", "历史提及回退最近成交价近似"
        return None, None, "unavailable", fb_err or "历史提及缺少可回退价格"

    def _fetch_snapshot_price(self, stock_code: str, mention_time: str, mention_date: str, force_refresh: bool = False) -> Dict[str, Any]:
        """
        获取 T+0 买入快照价：
        - 15 秒缓存（按 stock_code + mention second）
        - single-flight 防并发重复抓取
        - 失败冷却（默认 45 秒）
        """
        now_ts = time.time()
        normalized_code, _ = normalize_code(stock_code)
        dt = self._parse_dt(mention_time) or self._parse_dt(mention_date) or datetime.now(BEIJING_TZ)
        key = f"{normalized_code}|{dt.strftime('%Y-%m-%d %H:%M:%S')}"

        with StockAnalyzer._snapshot_lock:
            cached = StockAnalyzer._snapshot_cache.get(key)
            if (not force_refresh) and cached and float(cached.get("expire_at", 0)) > now_ts:
                return dict(cached.get("payload", {}))

            flight = StockAnalyzer._snapshot_inflight.get(key)
            if flight is None:
                flight = {"event": threading.Event(), "result": None}
                StockAnalyzer._snapshot_inflight[key] = flight
                is_owner = True
            else:
                is_owner = False

        if not is_owner:
            flight["event"].wait(timeout=8)
            result = flight.get("result")
            if isinstance(result, dict):
                return dict(result)
            return {
                "buy_price": None,
                "buy_ts": "",
                "buy_source": "unavailable",
                "note": "快照并发等待超时",
            }

        result: Dict[str, Any]
        try:
            cooldown = StockAnalyzer._snapshot_fail_cooldown.get(normalized_code, {})
            cool_until = float(cooldown.get("until", 0) or 0)
            if cool_until > now_ts:
                fb_price, fb_date, fb_err = self._get_recent_local_price(normalized_code, mention_date)
                if fb_price is not None:
                    result = {
                        "buy_price": round(float(fb_price), 4),
                        "buy_ts": fb_date or mention_date,
                        "buy_source": "fallback_recent",
                        "note": f"快照冷却中，回退最近成交价: {cooldown.get('reason', '')}",
                    }
                else:
                    result = {
                        "buy_price": None,
                        "buy_ts": "",
                        "buy_source": "unavailable",
                        "note": fb_err or "快照冷却中且无可回退价格",
                    }
            else:
                today = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
                now_dt = datetime.now(BEIJING_TZ)
                mention_dt = self._parse_dt(mention_time) or self._parse_dt(mention_date) or now_dt
                open_cut = 9 * 60 + 30
                mention_minute = mention_dt.hour * 60 + mention_dt.minute
                now_minute = now_dt.hour * 60 + now_dt.minute
                if mention_dt.strftime("%Y-%m-%d") == today and mention_minute < open_cut and now_minute < open_cut:
                    # 盘前统一按开盘价口径，开盘后由批量同步回填。
                    result = {
                        "buy_price": None,
                        "buy_ts": "",
                        "buy_source": "pending_open",
                        "note": "盘前提及，待开盘价回填",
                    }
                else:
                    allow_today_unfinal = not self.market_store.is_market_closed_now()
                    rows = self.market_store.get_price_range(
                        stock_code=normalized_code,
                        start_date=today,
                        end_date=today,
                        allow_today_unfinal=allow_today_unfinal,
                    )
                    # 默认详情请求只读本地库；仅手动刷新时允许网络回补。
                    need_sync = bool(force_refresh and ((not rows) or (rows and rows[-1].get("close") is None)))
                    if need_sync:
                        sync_res = self.market_sync.sync_daily_incremental(
                            history_days=3,
                            symbols=[normalized_code],
                            include_index=False,
                            finalize_today=False,
                        )
                        if not sync_res.get("success"):
                            raise RuntimeError(sync_res.get("message") or "sync_daily_incremental failed")
                        rows = self.market_store.get_price_range(
                            stock_code=normalized_code,
                            start_date=today,
                            end_date=today,
                            allow_today_unfinal=allow_today_unfinal,
                        )

                    if rows and rows[-1].get("close") is not None:
                        row = rows[-1]
                        close_px = float(row["close"])
                        open_px = float(row["open"]) if row.get("open") is not None else None
                        buy_px = close_px
                        buy_source = "snapshot"
                        buy_ts = today
                        note = ""

                        # 同日提及采用盘前/盘中开盘价口径，盘后采用收盘口径。
                        if mention_dt.strftime("%Y-%m-%d") == today:
                            minute = mention_dt.hour * 60 + mention_dt.minute
                            if minute < 15 * 60 + 5 and open_px is not None:
                                buy_px = open_px
                                buy_source = "snapshot_open"
                                buy_ts = today
                                note = "同日提及按开盘价口径"

                        result = {
                            "buy_price": round(buy_px, 4),
                            "buy_ts": buy_ts,
                            "buy_source": buy_source,
                            "note": note,
                        }
                    else:
                        # 盘前提及必须等开盘价，不回退前收。
                        if mention_dt.strftime("%Y-%m-%d") == today and mention_minute < open_cut:
                            result = {
                                "buy_price": None,
                                "buy_ts": "",
                                "buy_source": "pending_open",
                                "note": "盘前提及，开盘价待回填",
                            }
                        else:
                            fb_price, fb_date, fb_err = self._get_recent_local_price(normalized_code, mention_date)
                            if fb_price is not None:
                                result = {
                                    "buy_price": round(float(fb_price), 4),
                                    "buy_ts": fb_date or mention_date,
                                    "buy_source": "fallback_recent",
                                    "note": "当日快照缺失，回退最近成交价",
                                }
                            elif force_refresh:
                                rt = self.market_sync.fetch_realtime_price(normalized_code)
                                rt_price = rt.get("price") if isinstance(rt, dict) else None
                                if isinstance(rt_price, (int, float)) and float(rt_price) > 0:
                                    result = {
                                        "buy_price": round(float(rt_price), 4),
                                        "buy_ts": str(rt.get("quote_time") or mention_time or mention_date),
                                        "buy_source": "fallback_realtime",
                                        "note": "当日快照缺失，回退实时价近似",
                                    }
                                else:
                                    result = {
                                        "buy_price": None,
                                        "buy_ts": "",
                                        "buy_source": "unavailable",
                                        "note": fb_err or "快照不可用且回退失败",
                                    }
                            else:
                                result = {
                                    "buy_price": None,
                                    "buy_ts": "",
                                    "buy_source": "unavailable",
                                    "note": fb_err or "快照不可用且回退失败",
                                }
        except Exception as e:
            StockAnalyzer._snapshot_fail_cooldown[normalized_code] = {
                "until": now_ts + float(self.SNAPSHOT_FAIL_COOLDOWN_SECONDS),
                "reason": str(e),
            }
            fb_price, fb_date, fb_err = self._get_recent_local_price(normalized_code, mention_date)
            if fb_price is not None:
                result = {
                    "buy_price": round(float(fb_price), 4),
                    "buy_ts": fb_date or mention_date,
                    "buy_source": "fallback_recent",
                    "note": f"快照失败回退: {e}",
                }
            else:
                result = {
                    "buy_price": None,
                    "buy_ts": "",
                    "buy_source": "unavailable",
                    "note": fb_err or f"快照失败: {e}",
                }

        with StockAnalyzer._snapshot_lock:
            StockAnalyzer._snapshot_cache[key] = {
                "expire_at": now_ts + float(self.SNAPSHOT_TTL_SECONDS),
                "payload": dict(result),
            }
            flight = StockAnalyzer._snapshot_inflight.get(key)
            if flight is not None:
                flight["result"] = dict(result)
                flight["event"].set()
                StockAnalyzer._snapshot_inflight.pop(key, None)
        return result

    def _upsert_t0_metrics(self, mention_id: int, stock_code: str, mention_date: str, payload: Dict[str, Any]) -> None:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT OR IGNORE INTO mention_performance(mention_id, stock_code, mention_date) VALUES (?, ?, ?)",
                (mention_id, stock_code, mention_date),
            )
            cursor.execute(
                '''
                UPDATE mention_performance
                SET t0_buy_price = ?,
                    t0_buy_ts = ?,
                    t0_buy_source = ?,
                    t0_end_price_rt = ?,
                    t0_end_price_rt_ts = ?,
                    t0_end_price_close = ?,
                    t0_end_price_close_ts = ?,
                    t0_return_rt = ?,
                    t0_return_close = ?,
                    t0_status = ?,
                    t0_note = ?
                WHERE mention_id = ?
                ''',
                (
                    payload.get("t0_buy_price"),
                    payload.get("t0_buy_ts"),
                    payload.get("t0_buy_source"),
                    payload.get("t0_end_price_rt"),
                    payload.get("t0_end_price_rt_ts"),
                    payload.get("t0_end_price_close"),
                    payload.get("t0_end_price_close_ts"),
                    payload.get("t0_return_rt"),
                    payload.get("t0_return_close"),
                    payload.get("t0_status"),
                    payload.get("t0_note"),
                    mention_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def _build_t0_metrics(
        self,
        mention_id: int,
        stock_code: str,
        mention_date: str,
        mention_time: str,
        existing: Dict[str, Any],
        force_refresh: bool = False,
        realtime_quote_cache: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        now = datetime.now(BEIJING_TZ)
        today = now.strftime("%Y-%m-%d")
        market_closed = self.market_store.is_market_closed_now()
        normalized_code, _ = normalize_code(stock_code)
        close_finalize_time = str(self.market_store.close_finalize_time or "15:05")
        session_trade_date, window_tag = compute_session_trade_date(
            mention_time=mention_time,
            mention_date=mention_date,
            close_finalize_time=close_finalize_time,
            open_time="09:30",
        )

        buy_price = existing.get("t0_buy_price")
        buy_ts = str(existing.get("t0_buy_ts") or "")
        buy_source = str(existing.get("t0_buy_source") or "")
        note = str(existing.get("t0_note") or "")

        if buy_price is None:
            if mention_date >= today:
                snap = self._fetch_snapshot_price(
                    stock_code=normalized_code,
                    mention_time=mention_time,
                    mention_date=mention_date,
                    force_refresh=force_refresh,
                )
                buy_price = snap.get("buy_price")
                buy_ts = str(snap.get("buy_ts") or "")
                buy_source = str(snap.get("buy_source") or "")
                note = str(snap.get("note") or "")
            else:
                fb_price, fb_ts, fb_source, fb_note = self._get_historical_buy_fallback(
                    stock_code=normalized_code,
                    mention_time=mention_time,
                    mention_date=mention_date,
                )
                buy_price = fb_price
                buy_ts = fb_ts or mention_date
                buy_source = fb_source
                note = fb_note

        payload: Dict[str, Any] = {
            "t0_buy_price": round(float(buy_price), 4) if isinstance(buy_price, (int, float)) else None,
            "t0_buy_ts": buy_ts,
            "t0_buy_source": buy_source or "unavailable",
            "t0_end_price_rt": existing.get("t0_end_price_rt"),
            "t0_end_price_rt_ts": existing.get("t0_end_price_rt_ts"),
            "t0_end_price_close": existing.get("t0_end_price_close"),
            "t0_end_price_close_ts": existing.get("t0_end_price_close_ts"),
            "t0_return_rt": existing.get("t0_return_rt"),
            "t0_return_close": existing.get("t0_return_close"),
            "t0_status": existing.get("t0_status") or "unavailable",
            "t0_note": note,
            "t0_session_trade_date": session_trade_date,
            "t0_window_tag": window_tag,
        }

        if payload["t0_buy_price"] in (None, 0):
            payload["t0_status"] = "unavailable"
            return payload

        if session_trade_date >= today:
            allow_today_unfinal = bool(session_trade_date == today and not market_closed)
            rows = self.market_store.get_price_range(
                stock_code=normalized_code,
                start_date=session_trade_date,
                end_date=session_trade_date,
                allow_today_unfinal=allow_today_unfinal,
            )
            target_row = rows[-1] if rows else None
        else:
            target_row = self._get_price_on_or_after(
                stock_code=normalized_code,
                trade_date=session_trade_date,
                allow_today_unfinal=False,
            )

        if target_row is None or target_row.get("close") is None:
            # 盘中优先尝试实时行情，避免因“当日日线未落库”导致整条 T+0 无法计算。
            if session_trade_date == today and not market_closed and (not self._is_preopen_window()) and force_refresh:
                rt: Dict[str, Any]
                cache_key = normalized_code
                if isinstance(realtime_quote_cache, dict) and cache_key in realtime_quote_cache:
                    rt = dict(realtime_quote_cache.get(cache_key) or {})
                else:
                    try:
                        rt = self.market_sync.fetch_realtime_price(normalized_code)
                    except Exception as e:
                        rt = {"success": False, "message": str(e), "price": None}
                    if isinstance(realtime_quote_cache, dict):
                        realtime_quote_cache[cache_key] = dict(rt)
                rt_price = rt.get("price")
                if isinstance(rt_price, (int, float)) and float(rt_price) > 0:
                    rt_ret = (float(rt_price) - float(payload["t0_buy_price"])) / float(payload["t0_buy_price"]) * 100
                    payload["t0_end_price_rt"] = round(float(rt_price), 4)
                    payload["t0_end_price_rt_ts"] = str(rt.get("quote_time") or now.strftime("%Y-%m-%d %H:%M:%S"))
                    payload["t0_return_rt"] = round(rt_ret, 4)
                    payload["t0_status"] = "realtime"
                    rt_src = str(rt.get("source") or rt.get("provider_used") or "")
                    if rt_src:
                        base_note = str(payload.get("t0_note") or "").strip("; ")
                        suffix = f"实时价来源: {rt_src}"
                        payload["t0_note"] = f"{base_note}; {suffix}" if base_note else suffix
                    return payload
            payload["t0_status"] = "unavailable"
            payload["t0_note"] = (payload.get("t0_note") or "") + ("; 缺少当天成交价")
            return payload

        target_date = str(target_row.get("trade_date") or mention_date)
        target_close = float(target_row["close"])
        ret = (target_close - float(payload["t0_buy_price"])) / float(payload["t0_buy_price"]) * 100

        if target_date == today and not market_closed:
            payload["t0_end_price_rt"] = round(target_close, 4)
            payload["t0_end_price_rt_ts"] = now.strftime("%Y-%m-%d %H:%M:%S")
            payload["t0_return_rt"] = round(ret, 4)
            payload["t0_status"] = "realtime"
        else:
            payload["t0_end_price_close"] = round(target_close, 4)
            payload["t0_end_price_close_ts"] = now.strftime("%Y-%m-%d %H:%M:%S")
            payload["t0_return_close"] = round(ret, 4)
            payload["t0_status"] = "finalized"
        return payload

    # ========== 事件表现计算 ==========

    def _compute_performance_payload(
        self,
        stock_code: str,
        mention_date: str,
        mention_time: str,
        current_freeze: int,
        price_cache: Optional[Dict[Tuple[str, str, str], List[Dict[str, Any]]]] = None,
        index_cache: Optional[Dict[Tuple[str, str], Dict[str, float]]] = None,
        cache_lock: Optional[threading.RLock] = None,
        perf_context: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """
        计算一次提及事件的收益 payload，不写入数据库。
        返回: (是否成功, 原因, payload)
        """
        ALL_PERIODS = [1, 3, 5, 10, 20, 60, 120, 250]

        if current_freeze >= 3:
            return False, "freeze_level 已冻结", None

        freeze_thresholds = {1: 20, 2: 60, 3: 120}
        periods_to_calc = [d for d in ALL_PERIODS if d > freeze_thresholds.get(current_freeze, 0)]
        if current_freeze == 0:
            periods_to_calc = ALL_PERIODS
        if not periods_to_calc:
            return False, "freeze_level 已冻结", None

        close_finalize_time = str(self.market_store.close_finalize_time or "15:05")
        effective_mention_date, _window_tag = compute_session_trade_date(
            mention_time=mention_time,
            mention_date=mention_date,
            close_finalize_time=close_finalize_time,
            open_time="09:30",
        )
        dt = datetime.strptime(effective_mention_date, '%Y-%m-%d')
        max_period = max(periods_to_calc)
        start = (dt - timedelta(days=10)).strftime('%Y-%m-%d')
        end = (dt + timedelta(days=int(max_period * 1.5) + 10)).strftime('%Y-%m-%d')

        price_key = (stock_code, start, end)
        cache_cm = cache_lock if hasattr(cache_lock, "__enter__") else nullcontext()
        with cache_cm:
            prices = price_cache.get(price_key) if price_cache is not None else None
        if prices is None:
            if perf_context is None:
                prices = self.fetch_price_range(stock_code, start, end, data_mode="finalized_only")
            else:
                prices = self.fetch_price_range(
                    stock_code,
                    start,
                    end,
                    data_mode="finalized_only",
                    perf_context=perf_context,
                )
            with cache_cm:
                if price_cache is not None:
                    price_cache[price_key] = prices
        if not prices:
            return False, "无可用行情数据", None

        base_price = None
        base_idx = -1
        for i, p in enumerate(prices):
            if p['trade_date'] >= effective_mention_date:
                base_price = p['close']
                base_idx = i
                break

        if base_price is None or base_price == 0:
            return False, "未找到提及日及之后交易日价格", None
        try:
            base_trade_dt = datetime.strptime(str(prices[base_idx]["trade_date"]), "%Y-%m-%d").date()
            mention_dt = datetime.strptime(effective_mention_date, "%Y-%m-%d").date()
            if (base_trade_dt - mention_dt).days > 7:
                return False, "提及日附近行情缺失（基准日偏移过大）", None
        except Exception:
            pass

        index_key = (start, end)
        with cache_cm:
            index_prices = index_cache.get(index_key) if index_cache is not None else None
        if index_prices is None:
            if perf_context is None:
                index_prices = self._fetch_index_price(start, end, data_mode="finalized_only")
            else:
                index_prices = self._fetch_index_price(
                    start,
                    end,
                    data_mode="finalized_only",
                    perf_context=perf_context,
                )
            with cache_cm:
                if index_cache is not None:
                    index_cache[index_key] = index_prices

        index_base = None
        for p in prices:
            if p['trade_date'] >= effective_mention_date and p['trade_date'] in index_prices:
                index_base = index_prices[p['trade_date']]
                break

        returns: Dict[int, Optional[float]] = {}
        excess_returns: Dict[int, Optional[float]] = {}
        for days in periods_to_calc:
            target_idx = base_idx + days
            if target_idx < len(prices):
                target_price = prices[target_idx]['close']
                ret = (target_price - base_price) / base_price * 100
                returns[days] = round(ret, 2)

                target_date = prices[target_idx]['trade_date']
                if index_base and target_date in index_prices and index_base > 0:
                    index_ret = (index_prices[target_date] - index_base) / index_base * 100
                    excess_returns[days] = round(ret - index_ret, 2)
                else:
                    excess_returns[days] = None
            else:
                returns[days] = None
                excess_returns[days] = None

        max_return = 0.0
        max_drawdown = 0.0
        max_track = min(base_idx + max_period + 1, len(prices))
        for i in range(base_idx + 1, max_track):
            ret = (prices[i]['high'] - base_price) / base_price * 100
            max_return = max(max_return, ret)
            dd = (prices[i]['low'] - base_price) / base_price * 100
            max_drawdown = min(max_drawdown, dd)

        today = datetime.now(BEIJING_TZ).strftime('%Y-%m-%d')
        market_closed = self.market_store.is_market_closed_now()
        today_idx = -1
        for i, p in enumerate(prices):
            if (market_closed and p['trade_date'] > today) or ((not market_closed) and p['trade_date'] >= today):
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

        payload = {
            "periods_to_calc": periods_to_calc,
            "price_at_mention": round(base_price, 2),
            "returns": returns,
            "excess_returns": excess_returns,
            "max_return": round(max_return, 2),
            "max_drawdown": round(max_drawdown, 2),
            "new_freeze": new_freeze,
            "effective_mention_date": effective_mention_date,
        }
        return True, "ok", payload

    def _save_performance_payload(
        self,
        mention_id: int,
        stock_code: str,
        mention_date: str,
        payload: Dict[str, Any],
        row_exists: bool,
    ) -> None:
        """将收益 payload 写入 mention_performance。"""
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            self._save_performance_payload_on_cursor(
                cursor=cursor,
                mention_id=mention_id,
                stock_code=stock_code,
                mention_date=mention_date,
                payload=payload,
                row_exists=row_exists,
            )
            conn.commit()
        finally:
            conn.close()

    def _save_performance_payload_on_cursor(
        self,
        cursor: sqlite3.Cursor,
        mention_id: int,
        stock_code: str,
        mention_date: str,
        payload: Dict[str, Any],
        row_exists: bool,
    ) -> None:
        """在已有 cursor 上写入单条收益 payload（不提交事务）。"""
        returns: Dict[int, Optional[float]] = payload["returns"]
        excess_returns: Dict[int, Optional[float]] = payload["excess_returns"]
        periods_to_calc: List[int] = payload["periods_to_calc"]
        new_freeze: int = int(payload["new_freeze"])

        if row_exists:
            updates = []
            params = []
            for days in periods_to_calc:
                if returns.get(days) is not None:
                    updates.append(f'return_{days}d = ?')
                    params.append(returns[days])
                    updates.append(f'excess_return_{days}d = ?')
                    params.append(excess_returns.get(days))
            updates.append('max_return = ?')
            params.append(payload["max_return"])
            updates.append('max_drawdown = ?')
            params.append(payload["max_drawdown"])
            updates.append('freeze_level = ?')
            params.append(new_freeze)
            params.append(mention_id)

            if updates:
                cursor.execute(f'''
                    UPDATE mention_performance SET {', '.join(updates)}
                    WHERE mention_id = ?
                ''', params)
        else:
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
                mention_id, stock_code, mention_date, payload["price_at_mention"],
                returns.get(1), returns.get(3), returns.get(5),
                returns.get(10), returns.get(20),
                returns.get(60), returns.get(120), returns.get(250),
                excess_returns.get(1), excess_returns.get(3), excess_returns.get(5),
                excess_returns.get(10), excess_returns.get(20),
                excess_returns.get(60), excess_returns.get(120), excess_returns.get(250),
                payload["max_return"], payload["max_drawdown"], new_freeze
            ))

    def _save_performance_payload_batch(self, batch_items: List[Dict[str, Any]]) -> int:
        """批量写入收益 payload，单连接单事务提交。"""
        if not batch_items:
            return 0
        conn = self._get_conn()
        cursor = conn.cursor()
        wrote = 0
        try:
            for item in batch_items:
                payload = item.get("payload")
                if not isinstance(payload, dict):
                    continue
                self._save_performance_payload_on_cursor(
                    cursor=cursor,
                    mention_id=int(item["mention_id"]),
                    stock_code=str(item["stock_code"]),
                    mention_date=str(item["mention_date"]),
                    payload=payload,
                    row_exists=bool(item.get("row_exists")),
                )
                wrote += 1
            conn.commit()
        finally:
            conn.close()
        return wrote

    def _calc_mention_performance(self, mention_id: int, stock_code: str, mention_date: str) -> Tuple[bool, str]:
        """
        计算一次提及事件的后续表现
        T+1, T+3, T+5, T+10, T+20, T+60, T+120, T+250 收益率 & 超额收益率
        支持渐进式冻结：已冻结的字段不再重新拉取行情
        """
        # 检查当前 freeze_level
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT freeze_level FROM mention_performance WHERE mention_id = ?', (mention_id,))
        row = cursor.fetchone()
        current_freeze = row[0] if row and row[0] else 0
        conn.close()
        row_exists = bool(row)

        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT mention_time FROM stock_mentions WHERE id = ?", (mention_id,))
        row_mt = cursor.fetchone()
        conn.close()
        mention_time = str(row_mt[0] if row_mt and row_mt[0] else mention_date)

        ok, reason, payload = self._compute_performance_payload(
            stock_code=stock_code,
            mention_date=mention_date,
            mention_time=mention_time,
            current_freeze=int(current_freeze),
        )
        if not ok or payload is None:
            return False, reason

        self._save_performance_payload(
            mention_id=mention_id,
            stock_code=stock_code,
            mention_date=mention_date,
            payload=payload,
            row_exists=row_exists,
        )
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
            if self._is_stop_requested():
                conn.commit()
                conn.close()
                self.log("🛑 扫描任务收到停止请求，已中断")
                return {
                    'topics_scanned': i,
                    'mentions_extracted': total_mentions,
                    'unique_stocks': len(stocks_found),
                    'performance_calculated': 0,
                    'aborted': True,
                }
            stocks = self.extract_stocks(text)
            if not stocks:
                continue

            # 解析日期
            mention_date = create_time[:10] if create_time else ''
            if not mention_date:
                continue

            for stock in stocks:
                cursor.execute('''
                    INSERT OR IGNORE INTO stock_mentions
                    (topic_id, stock_code, stock_name, mention_date, mention_time, context_snippet)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    topic_id, stock['code'], stock['name'],
                    mention_date, create_time or '', stock['context']
                ))
                inserted = int(cursor.rowcount or 0)
                if inserted > 0:
                    total_mentions += inserted
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
        unavailable_count = 0
        total_stocks_to_calc = len(pending_by_stock)
        for stock_idx, (stock_code, items) in enumerate(pending_by_stock.items()):
            if self._is_stop_requested():
                self.log("🛑 收益计算收到停止请求，已中断")
                return {
                    'topics_scanned': total_topics,
                    'mentions_extracted': total_mentions,
                    'unique_stocks': len(stocks_found),
                    'performance_calculated': done_count,
                    'skipped_unavailable': unavailable_count,
                    'aborted': True,
                }
            for mid, mention_date in items:
                if self._is_stop_requested():
                    self.log("🛑 收益计算收到停止请求，已中断")
                    return {
                        'topics_scanned': total_topics,
                        'mentions_extracted': total_mentions,
                        'unique_stocks': len(stocks_found),
                        'performance_calculated': done_count,
                        'skipped_unavailable': unavailable_count,
                        'aborted': True,
                    }
                try:
                    self._calc_mention_performance(mid, stock_code, mention_date)
                except MarketDataUnavailableError as e:
                    unavailable_count += 1
                    log_warning(f"行情不可用，跳过 {stock_code}: {e}")
                except Exception as e:
                    log_warning(f"计算 {stock_code} 表现失败: {e}")
                done_count += 1

            if (stock_idx + 1) % 5 == 0 or done_count == total_pending:
                self.log(f"📈 已计算 {done_count}/{total_pending} 条提及 ({stock_idx+1}/{total_stocks_to_calc} 只股票)")

            # 仅在切换股票时 sleep，同一股票的提及复用行情缓存无需等待
            if stock_idx < total_stocks_to_calc - 1:
                time.sleep(0.2)

        self.log(f"✅ 全部完成！共处理 {total_pending} 条提及表现计算")
        if unavailable_count > 0:
            self.log(f"⚠️ 行情不可用跳过 {unavailable_count} 条")

        return {
            'topics_scanned': total_topics,
            'mentions_extracted': total_mentions,
            'unique_stocks': len(stocks_found),
            'performance_calculated': total_pending,
            'skipped_unavailable': unavailable_count,
            'aborted': False,
        }

    # ========== 分离式方法（调度器专用）==========

    def extract_only(self, group_id: str = None) -> Dict[str, Any]:
        """
        仅提取股票名称，不计算收益表现（纯本地操作，秒级完成）
        供调度器高频循环使用

        优化：将增量过滤尽量下推到 SQL 层，减少无谓的全表扫描。
        """
        gid = group_id or self.group_id
        self._build_stock_dictionary()

        conn = self._get_conn()
        cursor = conn.cursor()

        backfill_since = (
            datetime.now(BEIJING_TZ) - timedelta(days=max(int(self.TOPIC_BACKFILL_DAYS), 1))
        ).strftime("%Y-%m-%d")

        # 增量查询：仅返回需要（重新）提取的话题
        # 条件：无 state 记录 / extractor_version 不匹配 / perf_status 未完成 / 近期话题
        # 注意：text_hash 比较需要在 Python 侧完成（需计算当前 hash）
        cursor.execute('''
            SELECT t.topic_id, tk.text, t.create_time,
                   tas.text_hash AS old_hash, tas.extractor_version AS old_version, tas.perf_status
            FROM topics t
            JOIN talks tk ON t.topic_id = tk.topic_id
            LEFT JOIN topic_analysis_state tas ON t.topic_id = tas.topic_id
            WHERE tk.text IS NOT NULL AND tk.text != ''
              AND (
                tas.topic_id IS NULL
                OR tas.extractor_version IS NULL
                OR tas.extractor_version != ?
                OR tas.perf_status IS NULL
                OR tas.perf_status != 'complete'
                OR t.create_time >= ?
              )
            ORDER BY t.create_time
        ''', (self.EXTRACTOR_VERSION, backfill_since))
        topics = cursor.fetchall()

        total_topics = len(topics)
        total_mentions = 0
        stocks_found = set()
        touched_topics = 0

        for i, (topic_id, text, create_time, old_hash, old_version, old_perf_status) in enumerate(topics):
            if self._is_stop_requested():
                conn.commit()
                conn.close()
                self.log("🛑 提取任务收到停止请求，已中断")
                return {
                    'new_topics': touched_topics,
                    'mentions_extracted': total_mentions,
                    'unique_stocks': len(stocks_found),
                    'aborted': True,
                }
            topic_id_int = int(topic_id)
            txt = text or ""
            txt_hash = self._topic_text_hash(txt)
            create_date = (create_time or "")[:10]

            # SQL 已经过滤了大部分不需要提取的话题，
            # 但 text_hash 匹配仍需 Python 侧检查（SQL 无法预算 hash）
            if old_hash is not None and old_hash == txt_hash and old_version == self.EXTRACTOR_VERSION:
                # hash 未变且版本匹配，仅因 perf_status/近期 被返回，跳过重提取
                continue

            touched_topics += 1
            self._reset_topic_mentions(cursor, topic_id_int)
            stocks = self.extract_stocks(text)
            if not stocks:
                self._upsert_topic_analysis_state(
                    cursor=cursor,
                    topic_id=topic_id_int,
                    text_hash=txt_hash,
                    perf_status="no_mentions",
                )
                continue

            mention_date = create_time[:10] if create_time else ''
            if not mention_date:
                self._upsert_topic_analysis_state(
                    cursor=cursor,
                    topic_id=topic_id_int,
                    text_hash=txt_hash,
                    perf_status="extract_error",
                    last_error="missing_mention_date",
                )
                continue

            for stock in stocks:
                cursor.execute('''
                    INSERT OR IGNORE INTO stock_mentions
                    (topic_id, stock_code, stock_name, mention_date, mention_time, context_snippet)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    topic_id, stock['code'], stock['name'],
                    mention_date, create_time or '', stock['context']
                ))
                inserted = int(cursor.rowcount or 0)
                if inserted > 0:
                    total_mentions += inserted
                    stocks_found.add(stock['code'])

            self._upsert_topic_analysis_state(
                cursor=cursor,
                topic_id=topic_id_int,
                text_hash=txt_hash,
                perf_status="pending",
            )

        conn.commit()
        conn.close()

        if touched_topics > 0:
            self.log(
                f"📝 提取完成：{touched_topics} 条帖子（含近{self.TOPIC_BACKFILL_DAYS}天回补），"
                f"{total_mentions} 次提及，{len(stocks_found)} 只股票"
            )

        return {
            'new_topics': touched_topics,
            'mentions_extracted': total_mentions,
            'unique_stocks': len(stocks_found),
            'aborted': False,
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
            SELECT sm.id, sm.topic_id, sm.stock_code, sm.mention_date, sm.mention_time, 0 AS freeze_level, 0 AS row_exists
            FROM stock_mentions sm
            LEFT JOIN mention_performance mp ON sm.id = mp.mention_id
            WHERE mp.mention_id IS NULL
            AND sm.mention_date >= ?
        ''', (since_date,))
        new_pending = cursor.fetchall()

        # 查询2：已有记录但未完全冻结的提及（需要更新长周期数据）
        cursor.execute('''
            SELECT sm.id, sm.topic_id, sm.stock_code, sm.mention_date, sm.mention_time, COALESCE(mp.freeze_level, 0) AS freeze_level, 1 AS row_exists
            FROM stock_mentions sm
            JOIN mention_performance mp ON sm.id = mp.mention_id
            WHERE (mp.freeze_level IS NULL OR mp.freeze_level < 3)
            AND sm.mention_date >= ?
        ''', (since_date,))
        update_pending = cursor.fetchall()

        conn.close()
        max_trade_date = self.market_store.get_latest_trade_date(only_final=True)

        total_new = len(new_pending)
        total_update = len(update_pending)
        all_pending = new_pending + update_pending

        self.log(f"📈 收益计算：{total_new} 条新提及 + {total_update} 条待更新")

        processed = 0
        skipped = 0
        errors = 0
        skipped_unavailable_count = 0
        skipped_unavailable_codes: List[str] = []
        aborted = False
        total = len(all_pending)

        if total == 0:
            self.log("✅ 收益计算完成：成功 0 条，跳过 0 条，失败 0 条")
            return {
                'new_calculated': total_new,
                'updated': total_update,
                'processed': 0,
                'skipped': 0,
                'errors': 0,
                'skipped_unavailable_count': 0,
                'skipped_unavailable_codes': [],
                'aborted': False,
            }

        health = self.market_sync.get_provider_health_snapshot(op_name="fetch_stock_history")
        if not health.get("routable_providers"):
            reason = "无可用行情源，已跳过本轮收益计算"
            self.log(f"⚠️ {reason}")
            return {
                'new_calculated': total_new,
                'updated': total_update,
                'processed': 0,
                'skipped': total,
                'errors': 0,
                'skipped_unavailable_count': total,
                'skipped_unavailable_codes': [],
                'aborted': True,
                'skipped_reason': reason,
            }

        # 预检查：行情缓存明显滞后时直接跳过本轮，避免大量无效循环日志
        try:
            pending_max_mention_date = max(item[3] for item in all_pending if item[3])
            if max_trade_date and pending_max_mention_date and max_trade_date < pending_max_mention_date:
                trade_dt = datetime.strptime(max_trade_date, '%Y-%m-%d').date()
                mention_dt = datetime.strptime(pending_max_mention_date, '%Y-%m-%d').date()
                stale_days = (mention_dt - trade_dt).days
                if stale_days > 3:
                    reason = (
                        f"行情缓存过旧（最新 {max_trade_date}，待算提及最晚 {pending_max_mention_date}，滞后 {stale_days} 天）"
                    )
                    self.log(f"⚠️ {reason}，本轮收益计算直接跳过 {total} 条")
                    if progress_callback:
                        progress_callback(1, total, f"跳过批次：{reason}")
                    return {
                        'new_calculated': total_new,
                        'updated': total_update,
                        'processed': 0,
                        'skipped': total,
                        'errors': 0,
                        'skipped_reason': reason
                    }
                self.log(f"⚠️ 行情缓存偏旧：最新 {max_trade_date}，待算最晚 {pending_max_mention_date}")
        except Exception as e:
            log_warning(f"收益计算前的新鲜度预检查失败: {e}")

        perf_started_at = time.perf_counter()
        calc_cpu_started_at = time.perf_counter()
        price_cache: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
        index_cache: Dict[Tuple[str, str], Dict[str, float]] = {}
        payload_cache: Dict[Tuple[str, str, int], Tuple[bool, str, Optional[Dict[str, Any]]]] = {}
        cache_lock = threading.RLock()
        perf_context: Dict[str, Any] = {
            "lock": threading.RLock(),
            "backfill_attempted_symbols": set(),
            "network_backfill_symbols": set(),
            "network_seconds": 0.0,
        }
        unique_compute_count = 0
        db_batch_commits = 0
        db_write_seconds = 0.0

        tasks: List[Dict[str, Any]] = []
        unique_symbols: set = set()
        for item in all_pending:
            mention_id, topic_id, stock_code, mention_date, mention_time, freeze_level, row_exists = item
            stock_code = str(stock_code)
            mention_date = str(mention_date)
            mention_time_s = str(mention_time or mention_date)
            unique_symbols.add(stock_code)
            tasks.append(
                {
                    "mention_id": int(mention_id),
                    "topic_id": int(topic_id),
                    "stock_code": stock_code,
                    "mention_date": mention_date,
                    "mention_time": mention_time_s,
                    "freeze_level": int(freeze_level or 0),
                    "row_exists": bool(row_exists),
                }
            )

        self.log(
            f"📦 收益批次规模: mentions={total}, unique_stocks={len(unique_symbols)}, "
            f"workers={max(1, int(self.PERF_CALC_MAX_WORKERS))}, batch_size={max(1, int(self.PERF_DB_BATCH_SIZE))}"
        )

        if self.PERF_PREWARM_ENABLED and unique_symbols and (not self._is_preopen_window()):
            prewarm_started = time.perf_counter()
            chunk_size = max(1, int(self.PERF_PREWARM_CHUNK_SIZE))
            symbols_sorted = sorted(unique_symbols)
            prewarm_chunks = [symbols_sorted[i:i + chunk_size] for i in range(0, len(symbols_sorted), chunk_size)]
            prewarm_ok = 0
            prewarm_fail = 0
            for idx, chunk in enumerate(prewarm_chunks, 1):
                if self._is_stop_requested():
                    aborted = True
                    self.log("🛑 收益计算收到停止请求，预热阶段中断")
                    break
                try:
                    res = self.market_sync.sync_daily_incremental(
                        history_days=max(20, int(calc_window_days) + 20),
                        symbols=chunk,
                        include_index=(idx == 1),
                        finalize_today=False,
                    )
                    if res.get("success"):
                        prewarm_ok += 1
                    else:
                        prewarm_fail += 1
                        log_warning(f"预热分片失败 chunk={idx}/{len(prewarm_chunks)}: {res.get('message')}")
                except Exception as e:
                    prewarm_fail += 1
                    log_warning(f"预热分片异常 chunk={idx}/{len(prewarm_chunks)}: {e}")
            prewarm_elapsed = time.perf_counter() - prewarm_started
            with perf_context["lock"]:
                perf_context["network_seconds"] = float(perf_context.get("network_seconds", 0.0) or 0.0) + prewarm_elapsed
            self.log(
                f"🧰 行情预热完成: chunks={len(prewarm_chunks)}, success={prewarm_ok}, failed={prewarm_fail}, "
                f"elapsed={prewarm_elapsed:.1f}s"
            )

        topic_status: Dict[int, str] = {}
        write_batch: List[Dict[str, Any]] = []
        batch_size = max(1, int(self.PERF_DB_BATCH_SIZE))
        progress_interval = max(1.0, float(self.PERF_PROGRESS_LOG_INTERVAL_SECONDS))
        last_progress_log_at = 0.0
        last_progress_percent = -1
        completed = 0

        def _flush_write_batch(force: bool = False) -> None:
            nonlocal db_batch_commits, db_write_seconds
            if not write_batch:
                return
            if (not force) and len(write_batch) < batch_size:
                return
            t0 = time.perf_counter()
            wrote = self._save_performance_payload_batch(write_batch[:])
            db_write_seconds += time.perf_counter() - t0
            db_batch_commits += 1
            if wrote != len(write_batch):
                log_warning(f"收益批量写入条数不一致: expected={len(write_batch)}, actual={wrote}")
            write_batch.clear()

        def _compute_one(task: Dict[str, Any]) -> Dict[str, Any]:
            nonlocal unique_compute_count
            stock_code = str(task["stock_code"])
            mention_date = str(task["mention_date"])
            mention_time_s = str(task["mention_time"])
            freeze_level = int(task["freeze_level"])
            cache_key = (stock_code, f"{mention_date}|{mention_time_s}", freeze_level)
            with cache_lock:
                cached_result = payload_cache.get(cache_key)
            if cached_result is None:
                result = self._compute_performance_payload(
                    stock_code=stock_code,
                    mention_date=mention_date,
                    mention_time=mention_time_s,
                    current_freeze=freeze_level,
                    price_cache=price_cache,
                    index_cache=index_cache,
                    cache_lock=cache_lock,
                    perf_context=perf_context,
                )
                with cache_lock:
                    if cache_key not in payload_cache:
                        payload_cache[cache_key] = result
                        unique_compute_count += 1
                    cached_result = payload_cache[cache_key]
            written, reason, payload = cached_result
            return {
                "task": task,
                "written": bool(written),
                "reason": str(reason or ""),
                "payload": payload,
            }

        workers = max(1, int(self.PERF_CALC_MAX_WORKERS))
        if not aborted:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [executor.submit(_compute_one, task) for task in tasks]
                for fut in concurrent.futures.as_completed(futures):
                    if self._is_stop_requested():
                        aborted = True
                    completed += 1
                    status_msg = ""
                    try:
                        out = fut.result()
                        task = out["task"]
                        stock_code = str(task["stock_code"])
                        mention_date = str(task["mention_date"])
                        topic_id = int(task["topic_id"])
                        if out["written"]:
                            payload = out.get("payload")
                            if not isinstance(payload, dict):
                                raise RuntimeError("收益计算结果缺少 payload")
                            write_batch.append(
                                {
                                    "mention_id": int(task["mention_id"]),
                                    "stock_code": stock_code,
                                    "mention_date": mention_date,
                                    "payload": payload,
                                    "row_exists": bool(task["row_exists"]),
                                }
                            )
                            _flush_write_batch(force=False)
                            processed += 1
                            status_msg = f"已保存 {stock_code} ({mention_date})"
                            topic_status[topic_id] = "complete"
                        else:
                            skipped += 1
                            status_msg = f"跳过 {stock_code} ({mention_date})：{out['reason']}"
                            topic_status[topic_id] = "partial"
                    except Exception as e:
                        task = None
                        try:
                            task = out.get("task")  # type: ignore[name-defined]
                        except Exception:
                            task = None
                        stock_code = str(task["stock_code"]) if isinstance(task, dict) and task.get("stock_code") else "UNKNOWN"
                        topic_id = int(task["topic_id"]) if isinstance(task, dict) and task.get("topic_id") is not None else 0
                        if isinstance(e, MarketDataUnavailableError):
                            skipped += 1
                            errors += 1
                            skipped_unavailable_count += 1
                            if stock_code not in skipped_unavailable_codes:
                                skipped_unavailable_codes.append(stock_code)
                            status_msg = f"跳过 {stock_code}：行情不可用 ({e})"
                            log_warning(f"计算 {stock_code} 表现跳过（行情不可用）: {e}")
                        else:
                            errors += 1
                            status_msg = f"失败 {stock_code}: {e}"
                            log_warning(f"计算 {stock_code} 表现失败: {e}")
                        if topic_id > 0:
                            topic_status[topic_id] = "partial"

                    now_ts = time.time()
                    percent = int(completed * 100 / total) if total > 0 else 100
                    should_progress_log = (
                        completed == 1
                        or completed == total
                        or (now_ts - last_progress_log_at) >= progress_interval
                        or percent >= (last_progress_percent + 1)
                    )
                    if progress_callback and should_progress_log:
                        progress_callback(completed, total, status_msg)
                        last_progress_log_at = now_ts
                        last_progress_percent = percent
                    if completed % 100 == 0 or completed == total:
                        self.log(f"📈 收益计算中: {completed}/{total} (成功: {processed}, 跳过: {skipped}, 错误: {errors})")
                    if aborted:
                        self.log("🛑 收益计算收到停止请求，正在收尾...")
                        break

        _flush_write_batch(force=True)
        calc_cpu_seconds = time.perf_counter() - calc_cpu_started_at

        if topic_status:
            conn2 = self._get_conn()
            cur2 = conn2.cursor()
            now = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
            for topic_id, status in topic_status.items():
                perf_status = "complete" if status == "complete" else "partial"
                cur2.execute(
                    "UPDATE topic_analysis_state SET perf_status = ?, updated_at = ? WHERE topic_id = ?",
                    (perf_status, now, int(topic_id)),
                )
            conn2.commit()
            conn2.close()

        perf_total_seconds = time.perf_counter() - perf_started_at
        with perf_context["lock"]:
            network_backfill_symbols = sorted(list(perf_context.get("network_backfill_symbols", set())))
            network_seconds = float(perf_context.get("network_seconds", 0.0) or 0.0)
        self.log(
            f"✅ 收益计算完成：成功 {processed} 条，跳过 {skipped} 条，失败 {errors} 条"
            f"（唯一计算键 {unique_compute_count}，复用 {max(total - unique_compute_count, 0)}）"
        )
        self.log(
            f"📊 计算汇总: total_mentions={total}, unique_stocks={len(unique_symbols)}, "
            f"network_backfill_symbols={len(network_backfill_symbols)}, db_batch_commits={db_batch_commits}, "
            f"calc_seconds={calc_cpu_seconds:.2f}, db_write_seconds={db_write_seconds:.2f}, "
            f"network_seconds={network_seconds:.2f}, total_seconds={perf_total_seconds:.2f}"
        )

        return {
            'new_calculated': total_new,
            'updated': total_update,
            'processed': processed,
            'skipped': skipped,
            'errors': errors,
            'skipped_unavailable_count': skipped_unavailable_count,
            'skipped_unavailable_codes': skipped_unavailable_codes[:30],
            'aborted': aborted,
            'total_mentions': total,
            'unique_stocks': len(unique_symbols),
            'network_backfill_symbols': network_backfill_symbols,
            'db_batch_commits': db_batch_commits,
            'calc_seconds': round(calc_cpu_seconds, 4),
            'db_write_seconds': round(db_write_seconds, 4),
            'network_seconds': round(network_seconds, 4),
            'total_seconds': round(perf_total_seconds, 4),
        }

    def recalculate_performance_range(
        self,
        start_date: str,
        end_date: str,
        force: bool = False,
        progress_callback=None,
    ) -> Dict[str, Any]:
        """
        按日期范围重算收益表现。

        Args:
            start_date: 起始日期（YYYY-MM-DD）
            end_date: 结束日期（YYYY-MM-DD）
            force: 强制重算。True 时先删除范围内旧收益并忽略 freeze_level。
            progress_callback: 进度回调函数，func(current, total, msg)
        """
        self._build_stock_dictionary()

        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT sm.id, sm.topic_id, sm.stock_code, sm.mention_date, sm.mention_time,
                   COALESCE(mp.freeze_level, 0) AS freeze_level,
                   CASE WHEN mp.mention_id IS NULL THEN 0 ELSE 1 END AS row_exists
            FROM stock_mentions sm
            LEFT JOIN mention_performance mp ON sm.id = mp.mention_id
            WHERE sm.mention_date >= ? AND sm.mention_date <= ?
            ORDER BY sm.mention_date ASC, sm.id ASC
            ''',
            (start_date, end_date),
        )
        records = cursor.fetchall()

        if force and records:
            mention_ids = [int(r[0]) for r in records]
            placeholders = ",".join(["?"] * len(mention_ids))
            cursor.execute(f"DELETE FROM mention_performance WHERE mention_id IN ({placeholders})", mention_ids)
            conn.commit()
        conn.close()

        total = len(records)
        if total == 0:
            self.log(f"ℹ️ 指定范围无提及记录：{start_date} ~ {end_date}")
            return {
                "range_start": start_date,
                "range_end": end_date,
                "force": force,
                "total": 0,
                "processed": 0,
                "skipped": 0,
                "errors": 0,
            }

        self.log(
            f"📈 开始按范围重算收益：{start_date} ~ {end_date}，共 {total} 条"
            + ("（强制）" if force else "")
        )

        price_cache: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
        index_cache: Dict[Tuple[str, str], Dict[str, float]] = {}
        payload_cache: Dict[Tuple[str, str, int], Tuple[bool, str, Optional[Dict[str, Any]]]] = {}

        processed = 0
        skipped = 0
        errors = 0
        topic_status: Dict[int, str] = {}
        by_stock: Dict[str, List[Tuple[int, int, str, int, int]]] = {}
        for mention_id, topic_id, stock_code, mention_date, mention_time, freeze_level, row_exists in records:
            by_stock.setdefault(stock_code, []).append(
                (
                    int(mention_id),
                    int(topic_id),
                    str(mention_date),
                    str(mention_time or mention_date),
                    int(freeze_level or 0),
                    int(row_exists or 0),
                )
            )

        i = 0
        for stock_code, items in by_stock.items():
            if self._is_stop_requested():
                self.log("🛑 范围重算收到停止请求，已中断")
                return {
                    "range_start": start_date,
                    "range_end": end_date,
                    "force": force,
                    "total": total,
                    "processed": processed,
                    "skipped": skipped,
                    "errors": errors,
                    "aborted": True,
                }
            for mention_id, topic_id, mention_date, mention_time, freeze_level, row_exists in items:
                if self._is_stop_requested():
                    self.log("🛑 范围重算收到停止请求，已中断")
                    return {
                        "range_start": start_date,
                        "range_end": end_date,
                        "force": force,
                        "total": total,
                        "processed": processed,
                        "skipped": skipped,
                        "errors": errors,
                        "aborted": True,
                    }
                i += 1
                status_msg = ""
                try:
                    effective_freeze = 0 if force else int(freeze_level or 0)
                    cache_key = (stock_code, f"{mention_date}|{mention_time}", effective_freeze)
                    cached_result = payload_cache.get(cache_key)
                    if cached_result is None:
                        cached_result = self._compute_performance_payload(
                            stock_code=stock_code,
                            mention_date=mention_date,
                            mention_time=mention_time,
                            current_freeze=effective_freeze,
                            price_cache=price_cache,
                            index_cache=index_cache,
                        )
                        payload_cache[cache_key] = cached_result

                    written, reason, payload = cached_result
                    if written:
                        if payload is None:
                            raise RuntimeError("收益计算结果缺少 payload")
                        self._save_performance_payload(
                            mention_id=mention_id,
                            stock_code=stock_code,
                            mention_date=mention_date,
                            payload=payload,
                            row_exists=(False if force else bool(row_exists)),
                        )
                        processed += 1
                        status_msg = f"已重算 {stock_code} ({mention_date})"
                        topic_status[topic_id] = "complete"
                    else:
                        skipped += 1
                        status_msg = f"跳过 {stock_code} ({mention_date})：{reason}"
                        topic_status[topic_id] = "partial"
                except Exception as e:
                    errors += 1
                    status_msg = f"失败 {stock_code} ({mention_date}): {e}"
                    topic_status[topic_id] = "partial"
                    log_warning(f"范围重算失败 {stock_code} {mention_date}: {e}")

                if progress_callback:
                    progress_callback(i, total, status_msg)

                if i % 20 == 0 or i == total:
                    self.log(f"📈 范围重算中: {i}/{total} (成功: {processed}, 跳过: {skipped}, 错误: {errors})")

        if topic_status:
            conn2 = self._get_conn()
            cur2 = conn2.cursor()
            now = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
            for topic_id, status in topic_status.items():
                perf_status = "complete" if status == "complete" else "partial"
                cur2.execute(
                    "UPDATE topic_analysis_state SET perf_status = ?, updated_at = ? WHERE topic_id = ?",
                    (perf_status, now, int(topic_id)),
                )
            conn2.commit()
            conn2.close()

        return {
            "range_start": start_date,
            "range_end": end_date,
            "force": force,
            "total": total,
            "processed": processed,
            "skipped": skipped,
            "errors": errors,
            "aborted": False,
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

    def _events_refresh_key(self, stock_code: str) -> str:
        code = str(stock_code or "").upper()
        return f"group:{self.group_id}:{code}"

    def _read_events_refresh_state(self, stock_code: str) -> Dict[str, Any]:
        key = self._events_refresh_key(stock_code)
        with StockAnalyzer._events_refresh_state_lock:
            state = dict(StockAnalyzer._events_refresh_state.get(key, {}))
        return {
            "refresh_state": str(state.get("refresh_state") or "idle"),
            "last_refresh_at": state.get("last_refresh_at"),
            "last_refresh_error": state.get("last_refresh_error"),
        }

    def _update_events_refresh_state(
        self,
        stock_code: str,
        refresh_state: str,
        *,
        last_refresh_at: Optional[str] = None,
        last_refresh_error: Optional[str] = None,
    ) -> None:
        key = self._events_refresh_key(stock_code)
        with StockAnalyzer._events_refresh_state_lock:
            current = dict(StockAnalyzer._events_refresh_state.get(key, {}))
            current["refresh_state"] = str(refresh_state)
            if last_refresh_at is not None:
                current["last_refresh_at"] = last_refresh_at
            if last_refresh_error is not None:
                current["last_refresh_error"] = last_refresh_error
            StockAnalyzer._events_refresh_state[key] = current

    def _next_refresh_allowed_at(self, stock_code: str) -> Optional[str]:
        guard_key = f"{self.group_id}:{stock_code.upper()}"
        with StockAnalyzer._manual_refresh_guard_lock:
            last_ts = float(StockAnalyzer._manual_refresh_guard.get(guard_key, 0.0) or 0.0)
        if last_ts <= 0:
            return None
        next_ts = last_ts + float(self.MANUAL_REFRESH_COOLDOWN_SECONDS)
        now_ts = time.time()
        if next_ts <= now_ts:
            return None
        return datetime.fromtimestamp(next_ts, tz=BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")

    def schedule_stock_events_refresh(self, stock_code: str) -> Dict[str, Any]:
        refresh_state = self._read_events_refresh_state(stock_code)
        if refresh_state.get("refresh_state") in {"queued", "running"}:
            return {
                "queued": False,
                "refresh_state": refresh_state.get("refresh_state"),
                "reason": "already_running",
                "next_refresh_allowed_at": self._next_refresh_allowed_at(stock_code),
            }
        if self._next_refresh_allowed_at(stock_code):
            return {
                "queued": False,
                "refresh_state": "cooldown",
                "reason": "manual_cooldown",
                "next_refresh_allowed_at": self._next_refresh_allowed_at(stock_code),
            }
        self._update_events_refresh_state(stock_code, "queued", last_refresh_error="")

        def _run_job():
            self._update_events_refresh_state(stock_code, "running", last_refresh_error="")
            try:
                self.get_stock_events(
                    stock_code=stock_code,
                    refresh_realtime=True,
                    detail_mode="full",
                    page=1,
                    per_page=50,
                    include_full_text=False,
                )
                self._update_events_refresh_state(
                    stock_code,
                    "completed",
                    last_refresh_at=datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                    last_refresh_error="",
                )
            except Exception as e:
                self._update_events_refresh_state(
                    stock_code,
                    "failed",
                    last_refresh_at=datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                    last_refresh_error=str(e),
                )

        t = threading.Thread(target=_run_job, daemon=True, name=f"stock-events-refresh-{self.group_id}-{stock_code}")
        t.start()
        return {
            "queued": True,
            "refresh_state": "queued",
            "next_refresh_allowed_at": self._next_refresh_allowed_at(stock_code),
        }

    def get_stock_events(
        self,
        stock_code: str,
        refresh_realtime: bool = False,
        detail_mode: str = "fast",
        page: int = 1,
        per_page: int = 50,
        include_full_text: bool = False,
    ) -> Dict[str, Any]:
        """获取某只股票提及事件，默认 fast 模式仅走本地快读。"""
        normalized_detail_mode = str(detail_mode or "fast").strip().lower()
        if normalized_detail_mode not in {"fast", "full"}:
            normalized_detail_mode = "fast"
        page = max(1, int(page or 1))
        per_page = max(1, min(200, int(per_page or 50)))
        offset = (page - 1) * per_page

        passive_triggered = False
        if normalized_detail_mode == "full":
            try:
                passive_triggered = bool(self._maybe_passive_sync_for_t0(stock_code))
            except Exception as e:
                log_warning(f"T+0 被动同步失败(已忽略): stock={stock_code}, error={e}")

        anchor_date = self.get_data_anchor_date()
        manual_refresh_skipped = False
        refresh_mode = "auto_local"
        if refresh_realtime and normalized_detail_mode == "full":
            guard_key = f"{self.group_id}:{stock_code.upper()}"
            now_ts = time.time()
            with StockAnalyzer._manual_refresh_guard_lock:
                last_ts = float(StockAnalyzer._manual_refresh_guard.get(guard_key, 0.0) or 0.0)
                if (now_ts - last_ts) < float(self.MANUAL_REFRESH_COOLDOWN_SECONDS):
                    manual_refresh_skipped = True
                    refresh_realtime = False
                else:
                    StockAnalyzer._manual_refresh_guard[guard_key] = now_ts
                    refresh_mode = "manual"
        if manual_refresh_skipped:
            refresh_mode = "manual_cooldown"

        cache_key = self._cache_key(
            f"stock_events:{stock_code}:mode:{normalized_detail_mode}:page:{page}:size:{per_page}:fulltext:{int(include_full_text)}",
            anchor_date=anchor_date,
        )
        if (not refresh_realtime) and (not passive_triggered):
            cached = self._get_cached_analysis(cache_key)
            if isinstance(cached, dict):
                return self._with_meta(
                    cached,
                    cache_hit=True,
                    data_mode="live",
                    anchor_date=anchor_date,
                )

        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) AS cnt FROM stock_mentions WHERE stock_code = ?", (stock_code,))
        total_mentions = int((cursor.fetchone() or {"cnt": 0})["cnt"])

        full_text_col = "tk.text as full_text" if include_full_text else "'' as full_text"
        talks_join = "LEFT JOIN talks tk ON sm.topic_id = tk.topic_id" if include_full_text else ""
        cursor.execute(
            f'''
            SELECT sm.id, sm.topic_id, sm.stock_code, sm.stock_name,
                   sm.mention_date, sm.mention_time, sm.context_snippet as context,
                   mp.price_at_mention,
                   mp.return_1d, mp.return_3d, mp.return_5d, mp.return_10d, mp.return_20d,
                   mp.excess_return_5d, mp.excess_return_10d,
                   mp.max_return, mp.max_drawdown,
                   mp.t0_buy_price, mp.t0_buy_ts, mp.t0_buy_source,
                   mp.t0_end_price_rt, mp.t0_end_price_rt_ts,
                   mp.t0_end_price_close, mp.t0_end_price_close_ts,
                   mp.t0_return_rt, mp.t0_return_close,
                   mp.t0_status, mp.t0_note,
                   {full_text_col}
            FROM stock_mentions sm
            LEFT JOIN mention_performance mp ON sm.id = mp.mention_id
            {talks_join}
            WHERE sm.stock_code = ?
            ORDER BY sm.mention_time DESC
            LIMIT ? OFFSET ?
            ''',
            (stock_code, per_page, offset),
        )

        rows = cursor.fetchall()
        topic_ids = list(set(row["topic_id"] for row in rows if row["topic_id"]))
        stocks_by_topic: Dict[int, List[Dict[str, str]]] = {}
        if topic_ids:
            placeholders = ",".join("?" * len(topic_ids))
            cursor.execute(
                f'''
                SELECT topic_id, stock_code, stock_name
                FROM stock_mentions
                WHERE topic_id IN ({placeholders})
                ORDER BY mention_time DESC
                ''',
                topic_ids,
            )
            seen: Dict[int, set] = {}
            for r in cursor.fetchall():
                tid = r["topic_id"]
                code = r["stock_code"]
                if tid not in stocks_by_topic:
                    stocks_by_topic[tid] = []
                    seen[tid] = set()
                if code not in seen[tid]:
                    seen[tid].add(code)
                    stocks_by_topic[tid].append({"stock_code": code, "stock_name": r["stock_name"]})

        events = []
        realtime_quote_cache: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            t0_existing = {
                "t0_buy_price": row["t0_buy_price"],
                "t0_buy_ts": row["t0_buy_ts"],
                "t0_buy_source": row["t0_buy_source"],
                "t0_end_price_rt": row["t0_end_price_rt"],
                "t0_end_price_rt_ts": row["t0_end_price_rt_ts"],
                "t0_end_price_close": row["t0_end_price_close"],
                "t0_end_price_close_ts": row["t0_end_price_close_ts"],
                "t0_return_rt": row["t0_return_rt"],
                "t0_return_close": row["t0_return_close"],
                "t0_status": row["t0_status"],
                "t0_note": row["t0_note"],
            }

            if normalized_detail_mode == "full":
                t0_payload = self._build_t0_metrics(
                    mention_id=int(row["id"]),
                    stock_code=str(row["stock_code"]),
                    mention_date=str(row["mention_date"]),
                    mention_time=str(row["mention_time"] or ""),
                    existing=t0_existing,
                    force_refresh=refresh_realtime,
                    realtime_quote_cache=realtime_quote_cache,
                )
                should_persist = bool(refresh_realtime or row["t0_buy_price"] is None or row["t0_status"] is None)
                if not should_persist:
                    changed_fields = [
                        ("t0_status", row["t0_status"], t0_payload.get("t0_status")),
                        ("t0_return_rt", row["t0_return_rt"], t0_payload.get("t0_return_rt")),
                        ("t0_return_close", row["t0_return_close"], t0_payload.get("t0_return_close")),
                        ("t0_end_price_rt", row["t0_end_price_rt"], t0_payload.get("t0_end_price_rt")),
                        ("t0_end_price_close", row["t0_end_price_close"], t0_payload.get("t0_end_price_close")),
                    ]
                    should_persist = any((old_val != new_val) for _, old_val, new_val in changed_fields)
                if should_persist:
                    self._upsert_t0_metrics(
                        mention_id=int(row["id"]),
                        stock_code=str(row["stock_code"]),
                        mention_date=str(row["mention_date"]),
                        payload=t0_payload,
                    )
            else:
                session_trade_date, window_tag = compute_session_trade_date(
                    mention_time=str(row["mention_time"] or ""),
                    mention_date=str(row["mention_date"] or ""),
                    close_finalize_time=str(self.market_store.close_finalize_time or "15:05"),
                    open_time="09:30",
                )
                t0_payload = dict(t0_existing)
                t0_payload["t0_session_trade_date"] = session_trade_date
                t0_payload["t0_window_tag"] = window_tag

            full_text = row["full_text"] or ""
            text_snippet = full_text[:500] + ("..." if len(full_text) > 500 else "")
            topic_id = row["topic_id"]
            events.append(
                {
                    "mention_id": row["id"],
                    "topic_id": str(topic_id) if topic_id is not None else None,
                    "group_id": self.group_id,
                    "group_name": "",
                    "stock_code": row["stock_code"],
                    "stock_name": row["stock_name"],
                    "mention_date": row["mention_date"],
                    "mention_time": row["mention_time"],
                    "context": row["context"],
                    "full_text": full_text,
                    "text_snippet": text_snippet,
                    "stocks": stocks_by_topic.get(topic_id, []),
                    "return_1d": row["return_1d"],
                    "return_3d": row["return_3d"],
                    "return_5d": row["return_5d"],
                    "return_10d": row["return_10d"],
                    "return_20d": row["return_20d"],
                    "max_return": row["max_return"],
                    "max_drawdown": row["max_drawdown"],
                    "t0_buy_price": t0_payload.get("t0_buy_price"),
                    "t0_buy_ts": t0_payload.get("t0_buy_ts"),
                    "t0_buy_source": t0_payload.get("t0_buy_source"),
                    "t0_end_price_rt": t0_payload.get("t0_end_price_rt"),
                    "t0_end_price_rt_ts": t0_payload.get("t0_end_price_rt_ts"),
                    "t0_end_price_close": t0_payload.get("t0_end_price_close"),
                    "t0_end_price_close_ts": t0_payload.get("t0_end_price_close_ts"),
                    "t0_return_rt": t0_payload.get("t0_return_rt"),
                    "t0_return_close": t0_payload.get("t0_return_close"),
                    "t0_status": t0_payload.get("t0_status"),
                    "t0_note": t0_payload.get("t0_note"),
                    "t0_session_trade_date": t0_payload.get("t0_session_trade_date"),
                    "t0_window_tag": t0_payload.get("t0_window_tag"),
                }
            )

        valid_returns = [e["return_5d"] for e in events if e.get("return_5d") is not None]
        win_count = sum(1 for r in valid_returns if r > 0)
        stock_name = events[0]["stock_name"] if events else ""
        conn.close()

        refresh_state = self._read_events_refresh_state(stock_code)
        result = {
            "stock_code": stock_code,
            "stock_name": stock_name,
            "total_mentions": total_mentions,
            "page": page,
            "per_page": per_page,
            "detail_mode": normalized_detail_mode,
            "include_full_text": bool(include_full_text),
            "win_rate_5d": round(win_count / len(valid_returns) * 100, 1) if valid_returns else None,
            "avg_return_5d": round(sum(valid_returns) / len(valid_returns), 2) if valid_returns else None,
            "t0_finalized": self.market_store.is_market_closed_now(),
            "t0_data_mode": "batch_local_snapshot",
            "refresh_source": refresh_mode,
            "refresh_skipped": bool(manual_refresh_skipped),
            "refresh_state": refresh_state.get("refresh_state") or "idle",
            "last_refresh_at": refresh_state.get("last_refresh_at"),
            "last_refresh_error": refresh_state.get("last_refresh_error"),
            "next_refresh_allowed_at": self._next_refresh_allowed_at(stock_code),
            "provider_path": list(self.market_sync.realtime_provider_order),
            "t0_refresh_cooldown_seconds": int(self.MANUAL_REFRESH_COOLDOWN_SECONDS),
            "t0_board": build_t0_dual_board(
                events=events,
                close_finalize_time=str(self.market_store.close_finalize_time or "15:05"),
                open_time="09:30",
            ),
            "events": events,
        }
        today = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
        snap = self.market_store.get_symbol_day_snapshot_info(stock_code=stock_code, trade_date=today)
        result["snapshot_ts"] = snap.get("fetched_at")
        result["snapshot_is_final"] = snap.get("is_final")
        if not refresh_realtime:
            self._set_cached_analysis(cache_key, result, self.LIVE_CACHE_TTL_SECONDS)
        return self._with_meta(
            result,
            cache_hit=False,
            data_mode="live",
            anchor_date=anchor_date,
        )

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
        effective_start, effective_end, anchor_date = self._normalize_finalized_date_window(
            start_date=start_date,
            end_date=end_date,
        )
        cache_key = self._cache_key(
            f"win_rate:{min_mentions}:{return_period}:{limit}:{effective_start or ''}:{effective_end or ''}:{page}:{page_size}:{sort_by}:{order}",
            anchor_date=anchor_date,
        )
        cached = self._get_cached_analysis(cache_key)
        if isinstance(cached, dict):
            return self._with_meta(
                cached,
                cache_hit=True,
                data_mode="finalized",
                anchor_date=anchor_date,
                effective_start_date=effective_start,
                effective_end_date=effective_end,
            )

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
        if effective_start and effective_end:
            date_filter = 'AND sm.mention_date BETWEEN ? AND ?'
            date_params = [effective_start, effective_end]
        elif effective_start:
            date_filter = 'AND sm.mention_date >= ?'
            date_params = [effective_start]
        elif effective_end:
            date_filter = 'AND sm.mention_date <= ?'
            date_params = [effective_end]

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
        result = {
            'data': results,
            'total': total_cap,
            'page': page,
            'page_size': page_size
        }
        self._set_cached_analysis(cache_key, result, self.FINALIZED_CACHE_TTL_SECONDS)
        return self._with_meta(
            result,
            cache_hit=False,
            data_mode="finalized",
            anchor_date=anchor_date,
            effective_start_date=effective_start,
            effective_end_date=effective_end,
        )

    def get_sector_heatmap(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict]:
        """板块热度分析（支持时间过滤）

        Args:
            start_date: 仅统计该日期及之后的帖子 (YYYY-MM-DD)
            end_date: 仅统计该日期及之前的帖子 (YYYY-MM-DD)
        """
        effective_start, effective_end, anchor_date = self._normalize_finalized_date_window(
            start_date=start_date,
            end_date=end_date,
            default_days=30,
        )
        cache_key = self._cache_key(
            f"sector_heat:{effective_start or ''}:{effective_end or ''}",
            anchor_date=anchor_date,
        )
        cached = self._get_cached_analysis(cache_key)
        if isinstance(cached, list):
            return cached

        conn = self._get_conn()
        cursor = conn.cursor()

        # 获取帖子文本（带时间），可选时间过滤（end_date 包含当天）
        date_clause, date_params = build_topic_time_filter(
            start_date=effective_start,
            end_date=effective_end,
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

        result = aggregate_sector_heat(topics, SECTOR_KEYWORDS)
        self._set_cached_analysis(cache_key, result, self.FINALIZED_CACHE_TTL_SECONDS)
        return result

    def get_sector_topics(self, sector: str, start_date: Optional[str] = None,
                          end_date: Optional[str] = None, page: int = 1,
                          page_size: int = 20) -> Dict[str, Any]:
        """获取指定板块的命中话题明细（按时间倒序）"""
        effective_start, effective_end, anchor_date = self._normalize_finalized_date_window(
            start_date=start_date,
            end_date=end_date,
            default_days=30,
        )
        cache_key = self._cache_key(
            f"sector_topics:{sector}:{effective_start or ''}:{effective_end or ''}:{page}:{page_size}",
            anchor_date=anchor_date,
        )
        cached = self._get_cached_analysis(cache_key)
        if isinstance(cached, dict):
            return self._with_meta(
                cached,
                cache_hit=True,
                data_mode="finalized",
                anchor_date=anchor_date,
                effective_start_date=effective_start,
                effective_end_date=effective_end,
            )

        if sector not in SECTOR_KEYWORDS:
            raise ValueError(f"未知板块: {sector}")

        page = max(1, int(page or 1))
        page_size = max(1, min(int(page_size or 20), 100))
        keywords = [kw.lower() for kw in SECTOR_KEYWORDS[sector]]

        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        date_clause, date_params = build_topic_time_filter(
            start_date=effective_start,
            end_date=effective_end,
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
            result = {
                'total': total,
                'page': page,
                'page_size': page_size,
                'items': []
            }
            self._set_cached_analysis(cache_key, result, self.FINALIZED_CACHE_TTL_SECONDS)
            return self._with_meta(
                result,
                cache_hit=False,
                data_mode="finalized",
                anchor_date=anchor_date,
                effective_start_date=effective_start,
                effective_end_date=effective_end,
            )

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

        result = {
            'total': total,
            'page': page,
            'page_size': page_size,
            'items': items
        }
        self._set_cached_analysis(cache_key, result, self.FINALIZED_CACHE_TTL_SECONDS)
        return self._with_meta(
            result,
            cache_hit=False,
            data_mode="finalized",
            anchor_date=anchor_date,
            effective_start_date=effective_start,
            effective_end_date=effective_end,
        )

    def get_signals(self, lookback_days: int = 7, min_mentions: int = 2, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict]:
        """
        信号雷达：近期高频提及 + 历史胜率高的股票

        条件：
        - 近 lookback_days 天内被提及 >= min_mentions 次 (或指定 start_date/end_date)
        - 历史提及后5日胜率 >= 50%
        """
        effective_start, effective_end, anchor_date = self._normalize_finalized_date_window(
            start_date=start_date,
            end_date=end_date,
            default_days=max(int(lookback_days or 1), 1),
        )
        cache_key = self._cache_key(
            f"signals:{lookback_days}:{min_mentions}:{effective_start or ''}:{effective_end or ''}",
            anchor_date=anchor_date,
        )
        cached = self._get_cached_analysis(cache_key)
        if isinstance(cached, list):
            return cached

        cutoff_date = effective_start or (datetime.now(BEIJING_TZ) - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        # 防御式兜底，避免前端传入 <= 0 导致筛选异常
        min_mentions = max(1, int(min_mentions or 1))

        date_condition = "sm.mention_date >= ?"
        params: List[Any] = [cutoff_date]
        exclude_clause, exclude_params = build_sql_exclusion_clause('sm.stock_code', 'sm.stock_name')
        
        if effective_end:
            date_condition += " AND sm.mention_date <= ?"
            params.append(effective_end)

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

        self._set_cached_analysis(cache_key, signals, self.FINALIZED_CACHE_TTL_SECONDS)
        return signals

    def get_summary_stats(self) -> Dict[str, Any]:
        """获取分析概览统计"""
        anchor_date = self.get_data_anchor_date()
        cache_key = self._cache_key("summary_stats", anchor_date=anchor_date)
        cached = self._get_cached_analysis(cache_key)
        if isinstance(cached, dict):
            return self._with_meta(
                cached,
                cache_hit=True,
                data_mode="finalized",
                anchor_date=anchor_date,
                effective_end_date=anchor_date,
            )

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
        self._set_cached_analysis(cache_key, stats, self.FINALIZED_CACHE_TTL_SECONDS)
        return self._with_meta(
            stats,
            cache_hit=False,
            data_mode="finalized",
            anchor_date=anchor_date,
            effective_end_date=anchor_date,
        )

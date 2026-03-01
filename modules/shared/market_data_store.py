#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Persistent local market data store."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from modules.shared.logger_config import log_warning
from modules.shared.market_data_config import is_market_closed_now, load_market_data_config, now_beijing


class MarketDataStore:
    def __init__(self):
        self.config = load_market_data_config()
        self.db_path = str(self.config["db_path"])
        self.adjust = str(self.config.get("adjust", "qfq")).lower()
        self.close_finalize_time = str(self.config.get("close_finalize_time", "15:05"))
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_schema(self) -> None:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS market_daily_prices (
                stock_code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                adjust TEXT NOT NULL DEFAULT 'qfq',
                open REAL,
                close REAL,
                high REAL,
                low REAL,
                volume REAL,
                change_pct REAL,
                source TEXT DEFAULT 'akshare',
                is_final INTEGER NOT NULL DEFAULT 0,
                fetched_at TEXT NOT NULL,
                PRIMARY KEY (stock_code, trade_date, adjust)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS market_symbols (
                stock_code TEXT PRIMARY KEY,
                stock_name TEXT NOT NULL,
                market TEXT,
                source TEXT DEFAULT 'akshare',
                synced_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS market_sync_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                last_symbols_sync_at TEXT,
                last_incremental_sync_at TEXT,
                last_backfill_sync_at TEXT,
                last_finalized_trade_date TEXT,
                bootstrap_cursor_symbol TEXT,
                bootstrap_status TEXT,
                last_error TEXT,
                updated_at TEXT
            )
            """
        )
        cur.execute("INSERT OR IGNORE INTO market_sync_state (id, updated_at) VALUES (1, ?)", (self.now_str(),))
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mdp_trade_date ON market_daily_prices(trade_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mdp_stock_date ON market_daily_prices(stock_code, trade_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mdp_final ON market_daily_prices(is_final, trade_date)")
        conn.commit()
        conn.close()

    def now_str(self) -> str:
        return now_beijing().strftime("%Y-%m-%d %H:%M:%S")

    def is_market_closed_now(self) -> bool:
        return is_market_closed_now(self.close_finalize_time)

    def upsert_symbols(self, rows: Sequence[Dict[str, str]]) -> int:
        if not rows:
            return 0
        now = self.now_str()
        payload = [
            (
                str(item.get("stock_code", "")).upper(),
                str(item.get("stock_name", "")).strip(),
                str(item.get("market", "")).strip(),
                str(item.get("source", "akshare")).strip() or "akshare",
                now,
            )
            for item in rows
            if item.get("stock_code") and item.get("stock_name")
        ]
        if not payload:
            return 0
        conn = self._get_conn()
        cur = conn.cursor()
        cur.executemany(
            """
            INSERT INTO market_symbols (stock_code, stock_name, market, source, synced_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(stock_code) DO UPDATE SET
                stock_name = excluded.stock_name,
                market = excluded.market,
                source = excluded.source,
                synced_at = excluded.synced_at
            """,
            payload,
        )
        conn.commit()
        conn.close()
        self.update_sync_state(last_symbols_sync_at=now, last_error="")
        return len(payload)

    def list_symbols(self) -> List[Dict[str, str]]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT stock_code, stock_name, market FROM market_symbols ORDER BY stock_code")
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows

    def get_symbol_codes(self) -> List[str]:
        return [row["stock_code"] for row in self.list_symbols()]

    def upsert_daily_prices(self, rows: Sequence[Dict[str, Any]], adjust: Optional[str] = None) -> int:
        if not rows:
            return 0
        adj = (adjust or self.adjust).lower()
        now = self.now_str()
        payload = []
        for item in rows:
            stock_code = str(item.get("stock_code", "")).upper()
            trade_date = str(item.get("trade_date", ""))[:10]
            if not stock_code or not trade_date:
                continue
            payload.append(
                (
                    stock_code,
                    trade_date,
                    adj,
                    item.get("open"),
                    item.get("close"),
                    item.get("high"),
                    item.get("low"),
                    item.get("volume"),
                    item.get("change_pct"),
                    str(item.get("source", "akshare")),
                    int(item.get("is_final", 0)),
                    now,
                )
            )
        if not payload:
            return 0

        conn = self._get_conn()
        cur = conn.cursor()
        cur.executemany(
            """
            INSERT INTO market_daily_prices (
                stock_code, trade_date, adjust, open, close, high, low, volume, change_pct, source, is_final, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(stock_code, trade_date, adjust) DO UPDATE SET
                open = CASE
                    WHEN market_daily_prices.is_final = 1 AND excluded.is_final = 0 THEN market_daily_prices.open
                    ELSE excluded.open
                END,
                close = CASE
                    WHEN market_daily_prices.is_final = 1 AND excluded.is_final = 0 THEN market_daily_prices.close
                    ELSE excluded.close
                END,
                high = CASE
                    WHEN market_daily_prices.is_final = 1 AND excluded.is_final = 0 THEN market_daily_prices.high
                    ELSE excluded.high
                END,
                low = CASE
                    WHEN market_daily_prices.is_final = 1 AND excluded.is_final = 0 THEN market_daily_prices.low
                    ELSE excluded.low
                END,
                volume = CASE
                    WHEN market_daily_prices.is_final = 1 AND excluded.is_final = 0 THEN market_daily_prices.volume
                    ELSE excluded.volume
                END,
                change_pct = CASE
                    WHEN market_daily_prices.is_final = 1 AND excluded.is_final = 0 THEN market_daily_prices.change_pct
                    ELSE excluded.change_pct
                END,
                source = CASE
                    WHEN market_daily_prices.is_final = 1 AND excluded.is_final = 0 THEN market_daily_prices.source
                    ELSE excluded.source
                END,
                is_final = MAX(market_daily_prices.is_final, excluded.is_final),
                fetched_at = excluded.fetched_at
            """,
            payload,
        )
        conn.commit()
        conn.close()
        return len(payload)

    def get_price_range(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
        allow_today_unfinal: bool = False,
        adjust: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        code = str(stock_code).upper()
        adj = (adjust or self.adjust).lower()
        today = now_beijing().strftime("%Y-%m-%d")

        if allow_today_unfinal:
            where = """
                stock_code = ? AND adjust = ? AND trade_date >= ? AND trade_date <= ?
                AND ((trade_date < ? AND is_final = 1) OR trade_date = ?)
            """
            params: Iterable[Any] = (code, adj, start_date, end_date, today, today)
        else:
            where = """
                stock_code = ? AND adjust = ? AND trade_date >= ? AND trade_date <= ? AND is_final = 1
            """
            params = (code, adj, start_date, end_date)

        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT trade_date, open, close, high, low, change_pct, volume, is_final
            FROM market_daily_prices
            WHERE {where}
            ORDER BY trade_date
            """,
            tuple(params),
        )
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows

    def get_latest_trade_date(self, only_final: bool = True) -> Optional[str]:
        conn = self._get_conn()
        cur = conn.cursor()
        if only_final:
            cur.execute("SELECT MAX(trade_date) AS d FROM market_daily_prices WHERE is_final = 1")
        else:
            cur.execute("SELECT MAX(trade_date) AS d FROM market_daily_prices")
        row = cur.fetchone()
        conn.close()
        return row["d"] if row and row["d"] else None

    def has_final_for_date(self, trade_date: str) -> bool:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1 FROM market_daily_prices
            WHERE trade_date = ? AND is_final = 1
            LIMIT 1
            """,
            (trade_date,),
        )
        ok = cur.fetchone() is not None
        conn.close()
        return ok

    def has_final_for_symbol_date(self, stock_code: str, trade_date: str, adjust: Optional[str] = None) -> bool:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1 FROM market_daily_prices
            WHERE stock_code = ? AND trade_date = ? AND adjust = ? AND is_final = 1
            LIMIT 1
            """,
            (str(stock_code).upper(), trade_date, (adjust or self.adjust).lower()),
        )
        ok = cur.fetchone() is not None
        conn.close()
        return ok

    def update_sync_state(self, **kwargs: Any) -> None:
        if not kwargs:
            return
        fields = []
        values: List[Any] = []
        for key, val in kwargs.items():
            fields.append(f"{key} = ?")
            values.append(val)
        fields.append("updated_at = ?")
        values.append(self.now_str())
        values.append(1)

        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(f"UPDATE market_sync_state SET {', '.join(fields)} WHERE id = ?", values)
            conn.commit()
        except Exception as e:
            log_warning(f"更新 market_sync_state 失败: {e}")
        finally:
            conn.close()

    def get_sync_state(self) -> Dict[str, Any]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM market_sync_state WHERE id = 1")
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else {}

    def get_status(self) -> Dict[str, Any]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM market_symbols")
        symbols = int(cur.fetchone()["c"])
        cur.execute("SELECT MIN(trade_date) AS d FROM market_daily_prices")
        min_date = cur.fetchone()["d"]
        cur.execute("SELECT MAX(trade_date) AS d FROM market_daily_prices")
        max_date = cur.fetchone()["d"]
        cur.execute("SELECT COUNT(*) AS c FROM market_daily_prices")
        bars = int(cur.fetchone()["c"])
        conn.close()
        state = self.get_sync_state()
        today = now_beijing().strftime("%Y-%m-%d")
        return {
            "enabled": bool(self.config.get("enabled", True)),
            "db_path": self.db_path,
            "adjust": self.adjust,
            "close_finalize_time": self.close_finalize_time,
            "is_market_closed_now": self.is_market_closed_now(),
            "symbols_count": symbols,
            "bars_count": bars,
            "min_trade_date": min_date,
            "max_trade_date": max_date,
            "today": today,
            "today_finalized": self.has_final_for_date(today),
            "sync_state": state,
        }

    def get_symbol_day_snapshot_info(self, stock_code: str, trade_date: str, adjust: Optional[str] = None) -> Dict[str, Any]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT is_final, fetched_at, open, close
            FROM market_daily_prices
            WHERE stock_code = ? AND trade_date = ? AND adjust = ?
            ORDER BY fetched_at DESC
            LIMIT 1
            """,
            (str(stock_code).upper(), str(trade_date), (adjust or self.adjust).lower()),
        )
        row = cur.fetchone()
        conn.close()
        if not row:
            return {"exists": False, "is_final": None, "fetched_at": None, "open": None, "close": None}
        return {
            "exists": True,
            "is_final": int(row["is_final"]) if row["is_final"] is not None else None,
            "fetched_at": row["fetched_at"],
            "open": row["open"],
            "close": row["close"],
        }

    def get_trade_date_coverage(self, trade_date: str, adjust: Optional[str] = None) -> Dict[str, int]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                COUNT(*) AS rows_total,
                COUNT(DISTINCT stock_code) AS symbols_total,
                SUM(CASE WHEN is_final = 1 THEN 1 ELSE 0 END) AS rows_final,
                COUNT(DISTINCT CASE WHEN is_final = 1 THEN stock_code END) AS symbols_final
            FROM market_daily_prices
            WHERE trade_date = ? AND adjust = ?
            """,
            (str(trade_date), (adjust or self.adjust).lower()),
        )
        row = cur.fetchone()
        conn.close()
        if not row:
            return {"rows_total": 0, "symbols_total": 0, "rows_final": 0, "symbols_final": 0}
        return {
            "rows_total": int(row["rows_total"] or 0),
            "symbols_total": int(row["symbols_total"] or 0),
            "rows_final": int(row["rows_final"] or 0),
            "symbols_final": int(row["symbols_final"] or 0),
        }

    def reset_bootstrap_cursor(self) -> None:
        self.update_sync_state(bootstrap_cursor_symbol=None, bootstrap_status="idle")

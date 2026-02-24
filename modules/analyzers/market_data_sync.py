#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AkShare market data synchronization into local persistent store."""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import akshare as ak

from modules.shared.logger_config import log_error, log_info, log_warning
from modules.shared.market_data_config import now_beijing
from modules.shared.market_data_store import MarketDataStore


class MarketDataSyncService:
    _akshare_lock = threading.Lock()

    def __init__(self, store: Optional[MarketDataStore] = None, log_callback=None):
        self.store = store or MarketDataStore()
        self.config = self.store.config
        self.adjust = str(self.config.get("adjust", "qfq"))
        self.retry_max = int(self.config.get("sync_retry_max", 3))
        self.retry_backoff = float(self.config.get("sync_retry_backoff_seconds", 1.0))
        self.log_callback = log_callback

    def log(self, msg: str) -> None:
        if self.log_callback:
            self.log_callback(msg)
        log_info(msg)

    @staticmethod
    def _normalize_code(raw_code: str) -> Tuple[str, str]:
        code = str(raw_code).strip()
        if code.startswith("6"):
            return f"{code}.SH", "SH"
        if code.startswith(("0", "3")):
            return f"{code}.SZ", "SZ"
        if code.startswith(("4", "8")):
            return f"{code}.BJ", "BJ"
        return code, "UNK"

    @staticmethod
    def _to_float(v: Any) -> Optional[float]:
        try:
            if v is None:
                return None
            return float(v)
        except Exception:
            return None

    def _call_with_retry(self, fn, *args, **kwargs):
        for attempt in range(1, self.retry_max + 1):
            try:
                with MarketDataSyncService._akshare_lock:
                    return fn(*args, **kwargs)
            except Exception as e:
                if attempt >= self.retry_max:
                    raise
                delay = self.retry_backoff * (2 ** (attempt - 1))
                log_warning(f"AkShare调用失败（第{attempt}/{self.retry_max}次）: {e}，{delay:.1f}s后重试")
                time.sleep(delay)

    def sync_symbols(self) -> Dict[str, Any]:
        started = now_beijing()
        try:
            df = self._call_with_retry(ak.stock_zh_a_spot_em)
            if df is None or df.empty:
                return {"success": False, "message": "AkShare返回空股票清单", "synced": 0}
            rows: List[Dict[str, str]] = []
            for _, row in df.iterrows():
                code = str(row.get("代码", "")).strip()
                name = str(row.get("名称", "")).strip()
                if not code or not name:
                    continue
                full_code, market = self._normalize_code(code)
                rows.append({"stock_code": full_code, "stock_name": name, "market": market, "source": "akshare"})
            synced = self.store.upsert_symbols(rows)
            elapsed = (now_beijing() - started).total_seconds()
            self.log(f"✅ 行情代码表同步完成: {synced} 只，耗时 {elapsed:.1f}s")
            return {"success": True, "synced": synced, "duration_seconds": elapsed}
        except Exception as e:
            self.store.update_sync_state(last_error=f"sync_symbols: {e}")
            log_error(f"sync_symbols失败: {e}")
            return {"success": False, "message": str(e), "synced": 0}

    def _fetch_stock_history_rows(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
        is_final_for_past: bool = True,
        is_final_today: Optional[bool] = None,
    ) -> List[Dict[str, Any]]:
        pure_code = stock_code.split(".")[0]
        df = self._call_with_retry(
            ak.stock_zh_a_hist,
            symbol=pure_code,
            period="daily",
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", ""),
            adjust=self.adjust,
        )
        if df is None or df.empty:
            return []
        today = now_beijing().strftime("%Y-%m-%d")
        rows: List[Dict[str, Any]] = []
        for _, r in df.iterrows():
            trade_date = str(r.get("日期", ""))[:10]
            if not trade_date:
                continue
            if trade_date < start_date or trade_date > end_date:
                continue
            if trade_date == today and is_final_today is not None:
                is_final = 1 if is_final_today else 0
            else:
                is_final = 1 if is_final_for_past else 0
            rows.append(
                {
                    "stock_code": stock_code,
                    "trade_date": trade_date,
                    "open": self._to_float(r.get("开盘")),
                    "close": self._to_float(r.get("收盘")),
                    "high": self._to_float(r.get("最高")),
                    "low": self._to_float(r.get("最低")),
                    "change_pct": self._to_float(r.get("涨跌幅")),
                    "volume": self._to_float(r.get("成交量")),
                    "source": "akshare.stock_zh_a_hist",
                    "is_final": is_final,
                }
            )
        return rows

    def _fetch_index_history_rows(
        self,
        start_date: str,
        end_date: str,
        is_final_today: Optional[bool] = None,
    ) -> List[Dict[str, Any]]:
        df = self._call_with_retry(ak.stock_zh_index_daily, symbol="sh000300")
        if df is None or df.empty:
            return []
        today = now_beijing().strftime("%Y-%m-%d")
        rows: List[Dict[str, Any]] = []
        for _, r in df.iterrows():
            trade_date = str(r.get("date", ""))[:10]
            if not trade_date or trade_date < start_date or trade_date > end_date:
                continue
            if trade_date == today and is_final_today is not None:
                is_final = 1 if is_final_today else 0
            else:
                is_final = 1
            rows.append(
                {
                    "stock_code": "000300.SH",
                    "trade_date": trade_date,
                    "open": self._to_float(r.get("open")),
                    "close": self._to_float(r.get("close")),
                    "high": self._to_float(r.get("high")),
                    "low": self._to_float(r.get("low")),
                    "change_pct": 0.0,
                    "volume": self._to_float(r.get("volume")),
                    "source": "akshare.stock_zh_index_daily",
                    "is_final": is_final,
                }
            )
        return rows

    def sync_daily_incremental(
        self,
        history_days: Optional[int] = None,
        symbols: Optional[Sequence[str]] = None,
        include_index: bool = True,
        finalize_today: bool = False,
        sync_equities: bool = True,
    ) -> Dict[str, Any]:
        if not self.config.get("enabled", True):
            return {"success": True, "message": "market_data disabled", "upserted": 0}

        if sync_equities:
            if not symbols:
                if not self.store.get_symbol_codes():
                    sync_res = self.sync_symbols()
                    if not sync_res.get("success"):
                        return {"success": False, "message": "sync_symbols failed before incremental", "upserted": 0}
                symbols = self.store.get_symbol_codes()
            symbols = [str(s).upper() for s in symbols if s]
            if not symbols:
                return {"success": False, "message": "no symbols to sync", "upserted": 0}
        else:
            symbols = []

        days = int(history_days or self.config.get("incremental_history_days", 20))
        end_date = now_beijing().strftime("%Y-%m-%d")
        start_date = (now_beijing() - timedelta(days=max(days, 2))).strftime("%Y-%m-%d")
        today_closed = self.store.is_market_closed_now()
        today_final = finalize_today and today_closed
        upserted = 0
        errors = 0

        self.log(
            f"🔄 行情增量同步开始: symbols={len(symbols)}, include_index={include_index}, "
            f"window={start_date}..{end_date}, today_final={today_final}"
        )
        for idx, stock_code in enumerate(symbols, 1):
            try:
                rows = self._fetch_stock_history_rows(
                    stock_code=stock_code,
                    start_date=start_date,
                    end_date=end_date,
                    is_final_for_past=True,
                    is_final_today=today_final,
                )
                upserted += self.store.upsert_daily_prices(rows, adjust=self.adjust)
            except Exception as e:
                errors += 1
                log_warning(f"增量同步失败 {stock_code}: {e}")
            if idx % 200 == 0 or idx == len(symbols):
                self.log(f"   进度 {idx}/{len(symbols)}")

        if include_index:
            try:
                rows = self._fetch_index_history_rows(
                    start_date=start_date,
                    end_date=end_date,
                    is_final_today=today_final,
                )
                upserted += self.store.upsert_daily_prices(rows, adjust=self.adjust)
            except Exception as e:
                errors += 1
                log_warning(f"同步HS300失败: {e}")

        now = now_beijing().strftime("%Y-%m-%d %H:%M:%S")
        self.store.update_sync_state(
            last_incremental_sync_at=now,
            last_error="" if errors == 0 else f"incremental errors={errors}",
        )
        if today_final:
            self.store.update_sync_state(last_finalized_trade_date=end_date)
        return {
            "success": errors == 0,
            "symbols": len(symbols),
            "errors": errors,
            "upserted": upserted,
            "start_date": start_date,
            "end_date": end_date,
            "today_final": today_final,
        }

    def finalize_today_after_close(
        self,
        symbols: Optional[Sequence[str]] = None,
        sync_equities: bool = True,
    ) -> Dict[str, Any]:
        today = now_beijing().strftime("%Y-%m-%d")
        if not self.store.is_market_closed_now():
            return {"success": False, "message": "market not closed yet", "today": today}
        result = self.sync_daily_incremental(
            history_days=3,
            symbols=symbols,
            include_index=True,
            finalize_today=True,
            sync_equities=sync_equities,
        )
        if result.get("success"):
            self.store.update_sync_state(last_finalized_trade_date=today, last_error="")
        return result

    def backfill_history_full(
        self,
        resume: bool = True,
        batch_size: Optional[int] = None,
        symbol_limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        if not self.config.get("enabled", True):
            return {"success": True, "message": "market_data disabled", "processed_symbols": 0}

        if not self.store.get_symbol_codes():
            sync_res = self.sync_symbols()
            if not sync_res.get("success"):
                return {"success": False, "message": "sync_symbols failed before bootstrap", "processed_symbols": 0}
        symbols = self.store.get_symbol_codes()
        if symbol_limit and symbol_limit > 0:
            symbols = symbols[: int(symbol_limit)]

        state = self.store.get_sync_state()
        cursor_symbol = state.get("bootstrap_cursor_symbol") if resume else None
        start_idx = 0
        if cursor_symbol and cursor_symbol in symbols:
            start_idx = symbols.index(cursor_symbol)

        bs = int(batch_size or self.config.get("bootstrap_batch_size", 200))
        start_date = "1990-01-01"
        end_date = now_beijing().strftime("%Y-%m-%d")
        processed_symbols = 0
        upserted = 0
        errors = 0
        self.store.update_sync_state(bootstrap_status="running", last_error="")
        self.log(f"🚀 全历史回填开始: symbols={len(symbols)}, from={start_idx + 1}")

        for idx in range(start_idx, len(symbols)):
            code = symbols[idx]
            try:
                rows = self._fetch_stock_history_rows(
                    stock_code=code,
                    start_date=start_date,
                    end_date=end_date,
                    is_final_for_past=True,
                    is_final_today=self.store.is_market_closed_now(),
                )
                upserted += self.store.upsert_daily_prices(rows, adjust=self.adjust)
                processed_symbols += 1
                self.store.update_sync_state(bootstrap_cursor_symbol=code, bootstrap_status="running")
            except Exception as e:
                errors += 1
                self.store.update_sync_state(last_error=f"bootstrap {code}: {e}")
                log_warning(f"全历史回填失败 {code}: {e}")

            if processed_symbols % max(bs, 1) == 0:
                self.log(f"   回填进度 {processed_symbols}/{len(symbols)}")

        # HS300全历史
        try:
            idx_rows = self._fetch_index_history_rows(
                start_date=start_date,
                end_date=end_date,
                is_final_today=self.store.is_market_closed_now(),
            )
            upserted += self.store.upsert_daily_prices(idx_rows, adjust=self.adjust)
        except Exception as e:
            errors += 1
            log_warning(f"全历史回填HS300失败: {e}")

        now = now_beijing().strftime("%Y-%m-%d %H:%M:%S")
        self.store.update_sync_state(
            last_backfill_sync_at=now,
            bootstrap_cursor_symbol=None,
            bootstrap_status="done" if errors == 0 else "done_with_errors",
            last_error="" if errors == 0 else f"bootstrap errors={errors}",
        )
        return {
            "success": errors == 0,
            "processed_symbols": processed_symbols,
            "errors": errors,
            "upserted": upserted,
            "from_index": start_idx,
            "total_symbols": len(symbols),
        }

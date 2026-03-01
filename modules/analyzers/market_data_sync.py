#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Market data synchronization into local persistent store with provider failover."""

from __future__ import annotations

import threading
import time
from datetime import timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

from modules.analyzers.market_data_providers import DailyPriceRow, SymbolRow, build_provider, normalize_code
from modules.shared.logger_config import log_error, log_info, log_warning
from modules.shared.market_data_config import now_beijing
from modules.shared.market_data_store import MarketDataStore


class EmptyHistoryError(RuntimeError):
    """Provider returned empty history for current symbol/window; should fail over without circuit open."""


class ProviderHealthRegistry:
    """Process-level provider health and failure summary registry."""

    def __init__(self, summary_interval_seconds: float = 300.0):
        self._lock = threading.Lock()
        self._disabled_reason: Dict[str, str] = {}
        self._disabled_until: Dict[str, float] = {}
        self._failure_counters: Dict[Tuple[str, str, str], int] = {}
        self._failure_last_at: Dict[Tuple[str, str, str], float] = {}
        self._last_summary_at: float = 0.0
        self.summary_interval_seconds = max(float(summary_interval_seconds), 30.0)

    def set_disabled(self, provider: str, reason: str, disabled_until: float = 0.0) -> None:
        with self._lock:
            self._disabled_reason[provider] = reason
            if disabled_until > 0:
                self._disabled_until[provider] = disabled_until
            else:
                self._disabled_until.pop(provider, None)

    def clear_disabled(self, provider: str) -> None:
        with self._lock:
            self._disabled_reason.pop(provider, None)
            self._disabled_until.pop(provider, None)

    def get_disabled_reason(self, provider: str) -> str:
        with self._lock:
            until = self._disabled_until.get(provider, 0.0)
            if until > 0 and until <= time.time():
                self._disabled_until.pop(provider, None)
                self._disabled_reason.pop(provider, None)
                return ""
            return self._disabled_reason.get(provider, "")

    def get_disabled_until(self, provider: str) -> float:
        with self._lock:
            until = self._disabled_until.get(provider, 0.0)
            if until > 0 and until <= time.time():
                self._disabled_until.pop(provider, None)
                self._disabled_reason.pop(provider, None)
                return 0.0
            return until

    def record_failure(self, provider: str, op_name: str, reason: str) -> None:
        key = (provider, op_name, reason)
        with self._lock:
            self._failure_counters[key] = int(self._failure_counters.get(key, 0)) + 1
            self._failure_last_at[key] = time.time()

    def drain_summary_if_due(self) -> List[Dict[str, Any]]:
        now_ts = time.time()
        with self._lock:
            if (now_ts - self._last_summary_at) < self.summary_interval_seconds:
                return []
            self._last_summary_at = now_ts
            if not self._failure_counters:
                return []
            result: List[Dict[str, Any]] = []
            for key, cnt in self._failure_counters.items():
                provider, op_name, reason = key
                result.append(
                    {
                        "provider": provider,
                        "op": op_name,
                        "reason": reason,
                        "count": int(cnt),
                        "last_at": self._failure_last_at.get(key, now_ts),
                    }
                )
            self._failure_counters.clear()
            self._failure_last_at.clear()
            result.sort(key=lambda x: x["count"], reverse=True)
            return result


class MarketDataSyncService:
    # 按 Provider 粒度加锁（不同 Provider 可并行，同一 Provider 内串行）
    _provider_locks: Dict[str, threading.Lock] = {}
    _provider_locks_meta = threading.Lock()
    _reported_init_errors: set = set()
    _health_registry = ProviderHealthRegistry(summary_interval_seconds=300.0)

    @classmethod
    def _get_provider_lock(cls, provider_name: str) -> threading.Lock:
        """获取指定 Provider 的调用锁（懒创建）。"""
        with cls._provider_locks_meta:
            if provider_name not in cls._provider_locks:
                cls._provider_locks[provider_name] = threading.Lock()
            return cls._provider_locks[provider_name]

    def __init__(self, store: Optional[MarketDataStore] = None, log_callback=None):
        self.store = store or MarketDataStore()
        self.config = self.store.config
        self.adjust = str(self.config.get("adjust", "qfq"))
        self.retry_max = int(self.config.get("sync_retry_max", 3))
        self.retry_backoff = float(self.config.get("sync_retry_backoff_seconds", 1.0))
        self.failure_cooldown = float(self.config.get("sync_failure_cooldown_seconds", 120.0))
        self.provider_failover_enabled = bool(self.config.get("provider_failover_enabled", True))
        self.realtime_provider_failover_enabled = bool(self.config.get("realtime_provider_failover_enabled", False))
        self.provider_circuit_breaker_seconds = float(self.config.get("provider_circuit_breaker_seconds", 300.0))
        self.provider_order = [
            str(name).strip().lower()
            for name in self.config.get("providers", ["tx", "sina", "akshare", "tushare"])
            if str(name).strip()
        ]
        self.realtime_provider_order = [
            str(name).strip().lower()
            for name in self.config.get("realtime_providers", ["akshare", "tx", "sina", "tushare"])
            if str(name).strip()
        ]
        if not self.provider_order:
            self.provider_order = ["tushare", "tx", "akshare", "sina"]
        if not self.realtime_provider_order:
            self.realtime_provider_order = ["akshare", "tx", "sina", "tushare"]

        self._failure_lock = threading.Lock()
        self._symbol_fail_until: Dict[str, float] = {}
        self._symbol_fail_reason: Dict[str, str] = {}

        self._provider_lock = threading.Lock()
        self._provider_disabled_until: Dict[str, float] = {}
        self._providers: Dict[str, Any] = {}
        self._provider_init_errors: Dict[str, str] = {}
        for provider_name in self.provider_order:
            if provider_name in self._providers:
                continue
            try:
                self._providers[provider_name] = build_provider(provider_name, self.config)
                MarketDataSyncService._health_registry.clear_disabled(provider_name)
            except Exception as e:
                err_text = str(e)
                self._provider_init_errors[provider_name] = err_text
                MarketDataSyncService._health_registry.set_disabled(
                    provider_name, f"init_failed: {err_text}", disabled_until=0.0
                )
                key = (provider_name, err_text)
                if key not in MarketDataSyncService._reported_init_errors:
                    log_warning(f"行情源[{provider_name}]初始化失败: {e}")
                    MarketDataSyncService._reported_init_errors.add(key)

        self.log_callback = log_callback

    def log(self, msg: str) -> None:
        if self.log_callback:
            self.log_callback(msg)
            return
        log_info(msg)

    def _emit_failure_summary_if_due(self) -> None:
        summary = MarketDataSyncService._health_registry.drain_summary_if_due()
        if not summary:
            return
        parts = []
        for item in summary[:12]:
            parts.append(
                f"{item['provider']}:{item['op']} x{item['count']} ({item['reason']})"
            )
        log_warning("行情源失败汇总(周期): " + " | ".join(parts))

    @staticmethod
    def _is_fast_fail_error(err: Exception) -> bool:
        """网络层断连类错误直接失败，避免无意义重试。"""
        text = f"{type(err).__name__}: {err}".lower()
        fast_fail_tokens = (
            "remotedisconnected",
            "connection aborted",
            "connection reset",
            "connectionreseterror",
            "remote end closed connection",
            "每分钟最多访问该接口",
            "每小时最多访问该接口",
            "每天最多访问该接口",
            "最多访问该接口",
        )
        return any(token in text for token in fast_fail_tokens)

    def _call_with_retry(self, provider_name: str, fn):
        provider_lock = self._get_provider_lock(provider_name)
        for attempt in range(1, self.retry_max + 1):
            try:
                with provider_lock:
                    return fn()
            except Exception as e:
                if self._is_fast_fail_error(e):
                    log_warning(f"行情源[{provider_name}]调用失败（快速失败，不重试）: {e}")
                    raise
                if attempt >= self.retry_max:
                    raise
                delay = self.retry_backoff * (2 ** (attempt - 1))
                log_warning(
                    f"行情源[{provider_name}]调用失败（第{attempt}/{self.retry_max}次）: {e}，{delay:.1f}s后重试"
                )
                time.sleep(delay)

    def _failure_remaining_seconds(self, stock_code: str) -> float:
        if self.failure_cooldown <= 0:
            return 0.0
        now_ts = time.time()
        with self._failure_lock:
            until = self._symbol_fail_until.get(stock_code, 0.0)
            if until <= now_ts:
                self._symbol_fail_until.pop(stock_code, None)
                self._symbol_fail_reason.pop(stock_code, None)
                return 0.0
            return until - now_ts

    def _record_failure(self, stock_code: str, err: Exception) -> None:
        if self.failure_cooldown <= 0:
            return
        with self._failure_lock:
            self._symbol_fail_until[stock_code] = time.time() + self.failure_cooldown
            self._symbol_fail_reason[stock_code] = str(err)

    def _clear_failure(self, stock_code: str) -> None:
        with self._failure_lock:
            self._symbol_fail_until.pop(stock_code, None)
            self._symbol_fail_reason.pop(stock_code, None)

    def _provider_circuit_open_remaining(self, provider_name: str) -> float:
        with self._provider_lock:
            until = self._provider_disabled_until.get(provider_name, 0.0)
            if until <= time.time():
                self._provider_disabled_until.pop(provider_name, None)
                if not self._provider_init_errors.get(provider_name):
                    MarketDataSyncService._health_registry.clear_disabled(provider_name)
                return 0.0
            return until - time.time()

    def _mark_provider_failed(self, provider_name: str) -> None:
        if self.provider_circuit_breaker_seconds <= 0:
            return
        until = time.time() + self.provider_circuit_breaker_seconds
        with self._provider_lock:
            self._provider_disabled_until[provider_name] = until
        MarketDataSyncService._health_registry.set_disabled(
            provider_name,
            f"circuit_open:{int(self.provider_circuit_breaker_seconds)}s",
            disabled_until=until,
        )

    @staticmethod
    def _provider_supports_market(provider_name: str, market: str) -> bool:
        if market in {"SH", "SZ", "UNK"}:
            return True
        if market == "BJ":
            return provider_name in {"akshare", "tushare"}
        return True

    def _get_provider_disabled_reason(self, provider_name: str) -> str:
        init_err = self._provider_init_errors.get(provider_name, "")
        if init_err:
            return f"init_failed: {init_err}"
        circuit_remaining = self._provider_circuit_open_remaining(provider_name)
        if circuit_remaining > 0:
            return f"circuit_open:{int(circuit_remaining)}s"
        return MarketDataSyncService._health_registry.get_disabled_reason(provider_name)

    def get_provider_health_snapshot(self, stock_code: Optional[str] = None, op_name: str = "fetch_stock_history") -> Dict[str, Any]:
        names = self._ordered_provider_names(stock_code=stock_code)
        market = ""
        if stock_code:
            _, market = normalize_code(stock_code)
        details: List[Dict[str, Any]] = []
        routable_names: List[str] = []
        for provider_name in names:
            disabled_reason = self._get_provider_disabled_reason(provider_name)
            if market and not self._provider_supports_market(provider_name, market):
                disabled_reason = f"market_unsupported:{market}"
            routable = provider_name in self._providers and not disabled_reason
            if routable:
                routable_names.append(provider_name)
            details.append(
                {
                    "provider": provider_name,
                    "op": op_name,
                    "routable": routable,
                    "disabled_reason": disabled_reason or "",
                    "cooldown_until": self._provider_disabled_until.get(provider_name, 0.0),
                }
            )
        return {
            "providers": details,
            "routable_providers": routable_names,
            "last_probe_at": now_beijing().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def get_provider_failure_summary(self) -> Dict[str, Any]:
        # Non-destructive read, keep pending counters for periodic flush logs.
        registry = MarketDataSyncService._health_registry
        with registry._lock:  # noqa: SLF001 - controlled internal snapshot
            items = []
            for key, cnt in registry._failure_counters.items():
                provider, op_name, reason = key
                items.append(
                    {
                        "provider": provider,
                        "op": op_name,
                        "reason": reason,
                        "count": int(cnt),
                        "last_at": registry._failure_last_at.get(key, 0.0),
                    }
                )
        items.sort(key=lambda x: x["count"], reverse=True)
        return {
            "items": items[:50],
            "last_probe_at": now_beijing().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def _ordered_provider_names(self, stock_code: Optional[str] = None) -> List[str]:
        names: List[str] = []
        for provider_name in self.provider_order:
            if provider_name not in names:
                names.append(provider_name)
        for provider_name in self._providers.keys():
            if provider_name not in names:
                names.append(provider_name)
        if stock_code:
            _, market = normalize_code(stock_code)
            # BJ 标的在 tx/sina 上通常不可用，优先可覆盖 BJ 的源，减少无效请求
            if market == "BJ":
                preferred = ["tushare", "akshare", "tx", "sina"]
                ordered = [p for p in preferred if p in names]
                for p in names:
                    if p not in ordered:
                        ordered.append(p)
                names = ordered
        return names

    def _execute_with_failover(self, op_name: str, runner, provider_names: Optional[List[str]] = None) -> Tuple[Any, str, bool, List[str]]:
        failed_providers: List[str] = []
        attempts = 0
        names = provider_names or self._ordered_provider_names()
        empty_history_only = True

        for idx, provider_name in enumerate(names):
            provider = self._providers.get(provider_name)
            if provider is None:
                empty_history_only = False
                init_err = self._provider_init_errors.get(provider_name, "provider unavailable")
                MarketDataSyncService._health_registry.record_failure(
                    provider_name, op_name, f"provider_unavailable:{init_err}"
                )
                failed_providers.append(provider_name)
                continue

            circuit_remaining = self._provider_circuit_open_remaining(provider_name)
            if circuit_remaining > 0:
                empty_history_only = False
                MarketDataSyncService._health_registry.record_failure(
                    provider_name, op_name, f"circuit_open:{int(circuit_remaining)}s"
                )
                failed_providers.append(provider_name)
                continue

            attempts += 1
            try:
                result = runner(provider_name, provider)
                switched = attempts > 1
                return result, provider_name, switched, failed_providers
            except Exception as e:
                # 空结果通常是标的不支持/该窗口无数据，不代表源整体故障，不触发全局熔断
                if not isinstance(e, EmptyHistoryError):
                    empty_history_only = False
                    self._mark_provider_failed(provider_name)
                failed_providers.append(provider_name)
                reason_text = str(e)
                MarketDataSyncService._health_registry.record_failure(provider_name, op_name, reason_text)
                log_warning(f"行情源[{provider_name}]失败 {op_name}: {e}")

                if (not self.provider_failover_enabled) or (idx >= len(names) - 1):
                    break

                # 查找下一个可尝试 provider，仅用于日志
                for next_name in names[idx + 1 :]:
                    if next_name in self._providers:
                        log_warning(f"切换行情源 -> {next_name}")
                        break

        self._emit_failure_summary_if_due()
        failed_msg = ",".join(failed_providers) if failed_providers else "none"
        if failed_providers and empty_history_only:
            raise EmptyHistoryError(f"all providers returned empty history: {op_name}, failed_providers={failed_msg}")
        raise RuntimeError(f"所有行情源失败，任务终止: {op_name}, failed_providers={failed_msg}")

    def sync_symbols(self) -> Dict[str, Any]:
        started = now_beijing()
        try:
            rows_obj, provider_used, provider_switched, failed_providers = self._execute_with_failover(
                op_name="sync_symbols",
                runner=lambda provider_name, provider: self._call_with_retry(provider_name, provider.fetch_symbols),
            )
            rows_obj = rows_obj or []
            rows = [row.__dict__ if isinstance(row, SymbolRow) else row for row in rows_obj]
            if not rows:
                return {
                    "success": False,
                    "message": "行情源返回空股票清单",
                    "synced": 0,
                    "provider_used": provider_used,
                    "provider_switched": provider_switched,
                    "failed_providers": failed_providers,
                }
            synced = self.store.upsert_symbols(rows)
            elapsed = (now_beijing() - started).total_seconds()
            self.log(f"✅ 行情代码表同步完成: {synced} 只，耗时 {elapsed:.1f}s, provider={provider_used}")
            return {
                "success": True,
                "synced": synced,
                "duration_seconds": elapsed,
                "provider_used": provider_used,
                "provider_switched": provider_switched,
                "failed_providers": failed_providers,
            }
        except Exception as e:
            self.store.update_sync_state(last_error=f"sync_symbols: {e}")
            log_error(f"sync_symbols失败: {e}")
            return {
                "success": False,
                "message": str(e),
                "synced": 0,
                "provider_used": "",
                "provider_switched": False,
                "failed_providers": self._ordered_provider_names(),
            }

    def _fetch_stock_history_rows(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
        is_final_for_past: bool = True,
        is_final_today: Optional[bool] = None,
    ) -> Tuple[List[Dict[str, Any]], str, bool, List[str]]:
        normalized_code, market = normalize_code(stock_code)
        provider_names = []
        for name in self._ordered_provider_names(stock_code=normalized_code):
            disabled_reason = self._get_provider_disabled_reason(name)
            if disabled_reason:
                continue
            if not self._provider_supports_market(name, market):
                MarketDataSyncService._health_registry.record_failure(
                    name, f"fetch_stock_history:{normalized_code}", f"market_unsupported:{market}"
                )
                continue
            provider_names.append(name)
        if not provider_names:
            health = self.get_provider_health_snapshot(stock_code=normalized_code)
            raise RuntimeError(
                f"no routable providers for {normalized_code}, market={market}, details={health.get('providers')}"
            )

        def _run(provider_name, provider):
            rows_obj = self._call_with_retry(
                provider_name,
                lambda: provider.fetch_stock_history(
                    stock_code=normalized_code,
                    start_date=start_date,
                    end_date=end_date,
                    adjust=self.adjust,
                ),
            )
            if not rows_obj:
                raise EmptyHistoryError(f"empty history from provider={provider_name}")
            return rows_obj

        rows_obj, provider_used, provider_switched, failed_providers = self._execute_with_failover(
            op_name=f"fetch_stock_history:{normalized_code}",
            runner=_run,
            provider_names=provider_names,
        )
        rows_obj = rows_obj or []

        today = now_beijing().strftime("%Y-%m-%d")
        rows: List[Dict[str, Any]] = []
        for r in rows_obj:
            row = r.__dict__ if isinstance(r, DailyPriceRow) else r
            trade_date = str(row.get("trade_date", ""))[:10]
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
                    "stock_code": normalized_code,
                    "trade_date": trade_date,
                    "open": row.get("open"),
                    "close": row.get("close"),
                    "high": row.get("high"),
                    "low": row.get("low"),
                    "change_pct": row.get("change_pct"),
                    "volume": row.get("volume"),
                    "source": row.get("source", provider_used),
                    "is_final": is_final,
                }
            )
        return rows, provider_used, provider_switched, failed_providers

    def _fetch_index_history_rows(
        self,
        start_date: str,
        end_date: str,
        is_final_today: Optional[bool] = None,
    ) -> Tuple[List[Dict[str, Any]], str, bool, List[str]]:
        rows_obj, provider_used, provider_switched, failed_providers = self._execute_with_failover(
            op_name="fetch_index_history:000300.SH",
            runner=lambda provider_name, provider: self._call_with_retry(
                provider_name,
                lambda: provider.fetch_index_history(start_date=start_date, end_date=end_date),
            ),
        )
        rows_obj = rows_obj or []

        today = now_beijing().strftime("%Y-%m-%d")
        rows: List[Dict[str, Any]] = []
        for r in rows_obj:
            row = r.__dict__ if isinstance(r, DailyPriceRow) else r
            trade_date = str(row.get("trade_date", ""))[:10]
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
                    "open": row.get("open"),
                    "close": row.get("close"),
                    "high": row.get("high"),
                    "low": row.get("low"),
                    "change_pct": row.get("change_pct") if row.get("change_pct") is not None else 0.0,
                    "volume": row.get("volume"),
                    "source": row.get("source", provider_used),
                    "is_final": is_final,
                }
            )
        return rows, provider_used, provider_switched, failed_providers

    @staticmethod
    def _pick_first_number(row: Dict[str, Any], keys: Sequence[str]) -> Optional[float]:
        for key in keys:
            if key not in row:
                continue
            val = row.get(key)
            if val in (None, ""):
                continue
            try:
                return float(val)
            except Exception:
                continue
        return None

    def _pick_latest_prev_close(self, stock_code: str) -> Optional[float]:
        """从本地日线缓存中取最近一个收盘价，作为实时接口缺少昨收时的兜底。"""
        today = now_beijing().strftime("%Y-%m-%d")
        start_date = (now_beijing() - timedelta(days=20)).strftime("%Y-%m-%d")
        try:
            rows = self.store.get_price_range(
                stock_code=stock_code,
                start_date=start_date,
                end_date=today,
                allow_today_unfinal=True,
            )
        except Exception:
            return None
        if not rows:
            return None
        for row in reversed(rows):
            close_v = row.get("close")
            if close_v in (None, ""):
                continue
            try:
                return float(close_v)
            except Exception:
                continue
        return None

    def _fetch_realtime_quote_from_provider(self, provider_name: str, provider: Any, stock_code: str) -> Optional[Dict[str, Any]]:
        normalized_code, _ = normalize_code(stock_code)
        pure_code = normalized_code.split(".")[0]

        # Tushare: 优先 rt_min(1MIN)，realtime_quote 在部分环境会报接口名错误
        if provider_name == "tushare":
            pro = getattr(provider, "pro", None)
            if pro is None:
                return None
            rt_min_err: Optional[Exception] = None
            try:
                log_info(f"[rt_quote] request tushare.rt_min ts_code={normalized_code} freq=1MIN")
                rt_df = pro.rt_min(ts_code=normalized_code, freq="1MIN")
                row_count = 0 if rt_df is None else int(len(rt_df))
                log_info(f"[rt_quote] response tushare.rt_min ts_code={normalized_code} rows={row_count}")
                if rt_df is not None and not getattr(rt_df, "empty", True):
                    row = rt_df.iloc[-1].to_dict()
                    price = self._pick_first_number(row, ["close", "last", "price", "最新价"])
                    if price not in (None, 0):
                        quote_time = str(row.get("time") or now_beijing().strftime("%Y-%m-%d %H:%M:%S"))
                        open_price = self._pick_first_number(row, ["open", "今开"])
                        pre_close = self._pick_latest_prev_close(normalized_code)
                        log_info(
                            f"[rt_quote] parsed tushare.rt_min ts_code={normalized_code} "
                            f"quote_time={quote_time} price={float(price):.4f} open={open_price} pre_close={pre_close}"
                        )
                        return {
                            "stock_code": normalized_code,
                            "price": float(price),
                            "pre_close": pre_close,
                            "open": open_price,
                            "quote_time": quote_time,
                            "source": "tushare.rt_min",
                        }
            except Exception as e:
                rt_min_err = e
                log_warning(f"[rt_quote] error tushare.rt_min ts_code={normalized_code}: {e}")

            # 兼容部分账号仅开放 stk_mins 的情况（有频次限制）
            try:
                log_info(f"[rt_quote] request tushare.stk_mins ts_code={normalized_code} freq=1min limit=1")
                df = pro.stk_mins(ts_code=normalized_code, freq="1min", limit=1)
                row_count = 0 if df is None else int(len(df))
                log_info(f"[rt_quote] response tushare.stk_mins ts_code={normalized_code} rows={row_count}")
                if df is not None and not getattr(df, "empty", True):
                    row = df.iloc[-1].to_dict()
                    price = self._pick_first_number(row, ["close", "price", "last", "最新价"])
                    if price not in (None, 0):
                        quote_time = str(row.get("trade_time") or row.get("time") or now_beijing().strftime("%Y-%m-%d %H:%M:%S"))
                        open_price = self._pick_first_number(row, ["open", "今开"])
                        pre_close = self._pick_latest_prev_close(normalized_code)
                        log_info(
                            f"[rt_quote] parsed tushare.stk_mins ts_code={normalized_code} "
                            f"quote_time={quote_time} price={float(price):.4f} open={open_price} pre_close={pre_close}"
                        )
                        return {
                            "stock_code": normalized_code,
                            "price": float(price),
                            "pre_close": pre_close,
                            "open": open_price,
                            "quote_time": quote_time,
                            "source": "tushare.stk_mins",
                        }
            except Exception:
                pass

            # 最后尝试 realtime_quote（兼容仍可用的环境）
            try:
                log_info(f"[rt_quote] request tushare.realtime_quote ts_code={normalized_code}")
                df = pro.realtime_quote(ts_code=normalized_code)
                row_count = 0 if df is None else int(len(df))
                log_info(f"[rt_quote] response tushare.realtime_quote ts_code={normalized_code} rows={row_count}")
                if df is not None and not getattr(df, "empty", True):
                    row = df.iloc[0].to_dict()
                    price = self._pick_first_number(row, ["PRICE", "price", "最新价", "last"])
                    if price not in (None, 0):
                        pre_close = self._pick_first_number(row, ["PRE_CLOSE", "pre_close", "昨收", "close"])
                        if pre_close is None:
                            pre_close = self._pick_latest_prev_close(normalized_code)
                        open_price = self._pick_first_number(row, ["OPEN", "open", "今开"])
                        log_info(
                            f"[rt_quote] parsed tushare.realtime_quote ts_code={normalized_code} "
                            f"quote_time={now_beijing().strftime('%Y-%m-%d %H:%M:%S')} "
                            f"price={float(price):.4f} open={open_price} pre_close={pre_close}"
                        )
                        return {
                            "stock_code": normalized_code,
                            "price": float(price),
                            "pre_close": pre_close,
                            "open": open_price,
                            "quote_time": now_beijing().strftime("%Y-%m-%d %H:%M:%S"),
                            "source": "tushare.realtime_quote",
                        }
            except Exception:
                if rt_min_err is not None:
                    raise rt_min_err
                return None
            if rt_min_err is not None:
                raise rt_min_err
            return None

        # 公开源兜底：akshare spot/em
        try:
            import akshare as ak  # type: ignore
        except Exception:
            return None

        df = None
        for fn_name in ("stock_zh_a_spot_em", "stock_zh_a_spot"):
            fn = getattr(ak, fn_name, None)
            if not callable(fn):
                continue
            try:
                cur = fn()
            except Exception:
                continue
            if cur is not None and not getattr(cur, "empty", True):
                df = cur
                break
        if df is None or getattr(df, "empty", True):
            return None

        try:
            code_col = "代码" if "代码" in df.columns else ("symbol" if "symbol" in df.columns else None)
            if not code_col:
                return None
            hit = df[df[code_col].astype(str).str.strip() == pure_code]
            if hit is None or hit.empty:
                return None
            row = hit.iloc[0].to_dict()
        except Exception:
            return None

        price = self._pick_first_number(row, ["最新价", "最新", "现价", "price", "last", "close"])
        if price in (None, 0):
            return None
        pre_close = self._pick_first_number(row, ["昨收", "pre_close", "昨收价", "close"])
        open_price = self._pick_first_number(row, ["今开", "open"])
        return {
            "stock_code": normalized_code,
            "price": float(price),
            "pre_close": pre_close,
            "open": open_price,
            "quote_time": now_beijing().strftime("%Y-%m-%d %H:%M:%S"),
            "source": f"{provider_name}.spot",
        }

    def fetch_realtime_price(self, stock_code: str) -> Dict[str, Any]:
        normalized_code, market = normalize_code(stock_code)
        configured_names: List[str] = []
        for name in self.realtime_provider_order:
            if self._get_provider_disabled_reason(name):
                continue
            if not self._provider_supports_market(name, market):
                continue
            configured_names.append(name)
        if not self.realtime_provider_failover_enabled and configured_names:
            configured_names = configured_names[:1]

        provider_names: List[str] = []
        for name in configured_names:
            if name not in self._providers:
                continue
            provider_names.append(name)

        if not provider_names:
            return {
                "success": False,
                "stock_code": normalized_code,
                "price": None,
                "message": "no realtime provider available",
                "configured_providers": configured_names,
            }

        failed_providers: List[str] = []
        for provider_name in provider_names:
            provider = self._providers.get(provider_name)
            if provider is None:
                failed_providers.append(provider_name)
                continue
            started_at = time.perf_counter()
            try:
                quote = self._call_with_retry(
                    provider_name,
                    lambda: self._fetch_realtime_quote_from_provider(provider_name, provider, normalized_code),
                )
                if quote and quote.get("price") not in (None, 0):
                    elapsed_ms = (time.perf_counter() - started_at) * 1000
                    log_info(
                        f"[rt_quote] success provider={provider_name} stock={normalized_code} "
                        f"price={quote.get('price')} elapsed_ms={elapsed_ms:.2f}"
                    )
                    return {
                        "success": True,
                        "stock_code": normalized_code,
                        "price": float(quote["price"]),
                        "pre_close": quote.get("pre_close"),
                        "open": quote.get("open"),
                        "quote_time": quote.get("quote_time"),
                        "provider_used": provider_name,
                        "provider_path": provider_names,
                        "source": quote.get("source") or provider_name,
                        "failed_providers": failed_providers,
                    }
                elapsed_ms = (time.perf_counter() - started_at) * 1000
                log_warning(
                    f"[rt_quote] empty provider={provider_name} stock={normalized_code} elapsed_ms={elapsed_ms:.2f}"
                )
            except Exception as e:
                failed_providers.append(provider_name)
                MarketDataSyncService._health_registry.record_failure(
                    provider_name, f"fetch_realtime_quote:{normalized_code}", str(e)
                )
                elapsed_ms = (time.perf_counter() - started_at) * 1000
                log_warning(f"行情源[{provider_name}]失败 fetch_realtime_quote:{normalized_code}: {e}")
                log_warning(
                    f"[rt_quote] failed provider={provider_name} stock={normalized_code} elapsed_ms={elapsed_ms:.2f}"
                )
                if not self.realtime_provider_failover_enabled:
                    break
                continue

            if not self.realtime_provider_failover_enabled:
                break

        return {
            "success": False,
            "stock_code": normalized_code,
            "price": None,
            "failed_providers": failed_providers,
            "message": "all providers returned empty realtime quote",
            "provider_path": provider_names,
        }

    def sync_daily_by_dates(
        self,
        start_date: str,
        end_date: str,
        symbols: Optional[Sequence[str]] = None,
        include_index: bool = True,
    ) -> Dict[str, Any]:
        """按交易**日期**批量同步行情（仅限 tushare, 历史日不含当天）。

        对 [start_date, yesterday] 范围内每个交易日调一次 pro.daily(trade_date=...)，
        每次返回全市场 ~5000 只数据，然后只保留 symbols 中的标的写入 store。
        20 天窗口 ≈ 14 个交易日 = 14 次 API 调用。

        Returns dict with success/upserted/api_calls/elapsed_seconds.
        """
        if not self.config.get("enabled", True):
            return {"success": True, "message": "market_data disabled", "upserted": 0, "api_calls": 0}

        tushare_provider = self._providers.get("tushare")
        if tushare_provider is None or not hasattr(tushare_provider, "fetch_daily_by_date"):
            return {
                "success": False,
                "message": "tushare provider not available, cannot use batch date mode",
                "upserted": 0,
                "api_calls": 0,
            }

        from modules.analyzers.market_data_providers import DailyPriceRow, normalize_code

        # 构建需要的标的集合（归一化后）
        symbol_set: set = set()
        if symbols:
            for s in symbols:
                code, _ = normalize_code(str(s).upper())
                symbol_set.add(code)

        today = now_beijing().strftime("%Y-%m-%d")
        # 不含当天（tushare daily 当天需收盘后才有数据）
        effective_end = min(end_date, (now_beijing() - timedelta(days=1)).strftime("%Y-%m-%d"))
        if effective_end < start_date:
            return {"success": True, "message": "no historical dates to sync", "upserted": 0, "api_calls": 0}

        # 枚举日期范围内每一天（跳过非交易日由 tushare 返回空来处理）
        from datetime import datetime as _dt
        current = _dt.strptime(start_date, "%Y-%m-%d")
        end_dt = _dt.strptime(effective_end, "%Y-%m-%d")
        dates_to_sync: list = []
        while current <= end_dt:
            dates_to_sync.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)

        t0 = time.time()
        upserted = 0
        api_calls = 0
        errors = 0
        provider_lock = self._get_provider_lock("tushare")

        self.log(
            f"🔄 批量日期同步开始 (tushare): dates={len(dates_to_sync)}, "
            f"symbols_filter={len(symbol_set) if symbol_set else 'ALL'}, "
            f"range={start_date}..{effective_end}"
        )

        for idx, trade_date in enumerate(dates_to_sync, 1):
            try:
                with provider_lock:
                    all_rows = tushare_provider.fetch_daily_by_date(trade_date)
                api_calls += 1

                if not all_rows:
                    # 非交易日或无数据
                    continue

                # 只保留需要的标的
                if symbol_set:
                    filtered_rows = [r for r in all_rows if r.stock_code in symbol_set]
                else:
                    filtered_rows = all_rows

                if filtered_rows:
                    rows_dict = [
                        {
                            "stock_code": r.stock_code,
                            "trade_date": r.trade_date,
                            "open": r.open,
                            "close": r.close,
                            "high": r.high,
                            "low": r.low,
                            "change_pct": r.change_pct,
                            "volume": r.volume,
                            "source": r.source,
                            "is_final": 1,  # 历史日全部 final
                        }
                        for r in filtered_rows
                    ]
                    upserted += self.store.upsert_daily_prices(rows_dict, adjust=self.adjust)

                if idx % 5 == 0 or idx == len(dates_to_sync):
                    self.log(f"   日期进度 {idx}/{len(dates_to_sync)}, upserted={upserted}")

            except Exception as e:
                errors += 1
                log_warning(f"批量日期同步失败 date={trade_date}: {e}")
                # 不中断，继续下一天
                continue

        # 同步指数
        if include_index:
            try:
                rows, used, switched, failed = self._fetch_index_history_rows(
                    start_date=start_date,
                    end_date=effective_end,
                    is_final_today=None,
                )
                upserted += self.store.upsert_daily_prices(rows, adjust=self.adjust)
                api_calls += 1
            except Exception as e:
                log_warning(f"批量日期同步: 指数同步失败: {e}")

        elapsed = time.time() - t0
        self.log(
            f"✅ 批量日期同步完成: api_calls={api_calls}, upserted={upserted}, "
            f"errors={errors}, elapsed={elapsed:.1f}s"
        )
        return {
            "success": errors == 0,
            "upserted": upserted,
            "api_calls": api_calls,
            "errors": errors,
            "elapsed_seconds": round(elapsed, 1),
            "dates_attempted": len(dates_to_sync),
            "provider_used": "tushare",
        }

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
                        return {
                            "success": False,
                            "message": f"sync_symbols failed before incremental: {sync_res.get('message')}",
                            "upserted": 0,
                            "provider_used": sync_res.get("provider_used", ""),
                            "provider_switched": bool(sync_res.get("provider_switched", False)),
                            "failed_providers": sync_res.get("failed_providers", []),
                        }
                symbols = self.store.get_symbol_codes()
            symbols = [normalize_code(str(s).upper())[0] for s in symbols if s]
            symbols = list(dict.fromkeys(symbols))
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
        skipped = 0
        used_providers: List[str] = []
        provider_switched = False
        failed_providers: List[str] = []

        self.log(
            f"🔄 行情增量同步开始: symbols={len(symbols)}, include_index={include_index}, "
            f"window={start_date}..{end_date}, today_final={today_final}"
        )
        for idx, stock_code in enumerate(symbols, 1):
            remain = self._failure_remaining_seconds(stock_code)
            if remain > 0:
                skipped += 1
                reason = self._symbol_fail_reason.get(stock_code, "recent failure")
                log_warning(f"增量同步跳过 {stock_code}: 冷却中剩余{remain:.0f}s，原因: {reason}")
                if idx % 200 == 0 or idx == len(symbols):
                    self.log(f"   进度 {idx}/{len(symbols)}")
                continue
            try:
                rows, used, switched, failed = self._fetch_stock_history_rows(
                    stock_code=stock_code,
                    start_date=start_date,
                    end_date=end_date,
                    is_final_for_past=True,
                    is_final_today=today_final,
                )
                upserted += self.store.upsert_daily_prices(rows, adjust=self.adjust)
                self._clear_failure(stock_code)
                if used and used not in used_providers:
                    used_providers.append(used)
                provider_switched = provider_switched or switched
                for item in failed:
                    if item not in failed_providers:
                        failed_providers.append(item)
            except Exception as e:
                if isinstance(e, EmptyHistoryError):
                    skipped += 1
                    self._clear_failure(stock_code)
                    log_info(f"增量同步跳过 {stock_code}: 窗口内无行情（可能停牌）")
                    if idx % 200 == 0 or idx == len(symbols):
                        self.log(f"   进度 {idx}/{len(symbols)}")
                    continue
                errors += 1
                self._record_failure(stock_code, e)
                for name in self._ordered_provider_names():
                    if name not in failed_providers:
                        failed_providers.append(name)
                message = f"增量同步失败 {stock_code}: {e}"
                log_warning(message)
                now = now_beijing().strftime("%Y-%m-%d %H:%M:%S")
                self.store.update_sync_state(last_incremental_sync_at=now, last_error=message)
                return {
                    "success": False,
                    "symbols": len(symbols),
                    "errors": errors,
                    "skipped": skipped,
                    "upserted": upserted,
                    "start_date": start_date,
                    "end_date": end_date,
                    "today_final": today_final,
                    "message": message,
                    "provider_used": used_providers[-1] if used_providers else "",
                    "provider_switched": provider_switched,
                    "failed_providers": failed_providers,
                }
            if idx % 200 == 0 or idx == len(symbols):
                self.log(f"   进度 {idx}/{len(symbols)}")

        if include_index:
            try:
                rows, used, switched, failed = self._fetch_index_history_rows(
                    start_date=start_date,
                    end_date=end_date,
                    is_final_today=today_final,
                )
                upserted += self.store.upsert_daily_prices(rows, adjust=self.adjust)
                if used and used not in used_providers:
                    used_providers.append(used)
                provider_switched = provider_switched or switched
                for item in failed:
                    if item not in failed_providers:
                        failed_providers.append(item)
            except Exception as e:
                errors += 1
                for name in self._ordered_provider_names():
                    if name not in failed_providers:
                        failed_providers.append(name)
                message = f"同步HS300失败: {e}"
                log_warning(message)
                now = now_beijing().strftime("%Y-%m-%d %H:%M:%S")
                self.store.update_sync_state(last_incremental_sync_at=now, last_error=message)
                return {
                    "success": False,
                    "symbols": len(symbols),
                    "errors": errors,
                    "skipped": skipped,
                    "upserted": upserted,
                    "start_date": start_date,
                    "end_date": end_date,
                    "today_final": today_final,
                    "message": message,
                    "provider_used": used_providers[-1] if used_providers else "",
                    "provider_switched": provider_switched,
                    "failed_providers": failed_providers,
                }

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
            "skipped": skipped,
            "upserted": upserted,
            "start_date": start_date,
            "end_date": end_date,
            "today_final": today_final,
            "provider_used": used_providers[-1] if used_providers else "",
            "provider_switched": provider_switched,
            "failed_providers": failed_providers,
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
        stop_checker=None,
        progress_every: Optional[int] = None,
    ) -> Dict[str, Any]:
        if not self.config.get("enabled", True):
            return {"success": True, "message": "market_data disabled", "processed_symbols": 0}

        if not self.store.get_symbol_codes():
            sync_res = self.sync_symbols()
            if not sync_res.get("success"):
                return {
                    "success": False,
                    "message": f"sync_symbols failed before bootstrap: {sync_res.get('message')}",
                    "processed_symbols": 0,
                    "failed_providers": sync_res.get("failed_providers", []),
                }
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
        total_symbols = len(symbols)
        processed_symbols = 0
        attempted_symbols = 0
        upserted = 0
        errors = 0
        stopped = False
        last_cursor_symbol = cursor_symbol if cursor_symbol in symbols else None
        failed_providers: List[str] = []
        self.store.update_sync_state(bootstrap_status="running", last_error="")
        progress_step = max(int(progress_every or bs), 1)
        self.log(
            f"🚀 全历史回填开始: symbols={total_symbols}, from={start_idx + 1}, "
            f"window={start_date}..{end_date}, progress_every={progress_step}"
        )

        for idx in range(start_idx, total_symbols):
            if stop_checker and stop_checker():
                stopped = True
                self.log(
                    f"🛑 检测到停止请求，回填提前结束: attempted={attempted_symbols}, "
                    f"success={processed_symbols}, errors={errors}"
                )
                break
            code = symbols[idx]
            attempted_symbols += 1
            last_cursor_symbol = code
            try:
                rows, _, _, failed = self._fetch_stock_history_rows(
                    stock_code=code,
                    start_date=start_date,
                    end_date=end_date,
                    is_final_for_past=True,
                    is_final_today=self.store.is_market_closed_now(),
                )
                upserted += self.store.upsert_daily_prices(rows, adjust=self.adjust)
                processed_symbols += 1
                for name in failed:
                    if name not in failed_providers:
                        failed_providers.append(name)
                self.store.update_sync_state(bootstrap_cursor_symbol=code, bootstrap_status="running")
            except Exception as e:
                errors += 1
                self.store.update_sync_state(last_error=f"bootstrap {code}: {e}")
                log_warning(f"全历史回填失败 {code}: {e}")

            if (attempted_symbols % progress_step == 0) or (idx == total_symbols - 1):
                self.log(
                    f"   回填进度 attempted={attempted_symbols}/{total_symbols}, "
                    f"success={processed_symbols}, errors={errors}, last={code}"
                )

        if not stopped:
            try:
                idx_rows, _, _, failed = self._fetch_index_history_rows(
                    start_date=start_date,
                    end_date=end_date,
                    is_final_today=self.store.is_market_closed_now(),
                )
                for name in failed:
                    if name not in failed_providers:
                        failed_providers.append(name)
                upserted += self.store.upsert_daily_prices(idx_rows, adjust=self.adjust)
            except Exception as e:
                errors += 1
                log_warning(f"全历史回填HS300失败: {e}")
        else:
            self.log("⚠️ 任务已停止，跳过HS300回填")

        now = now_beijing().strftime("%Y-%m-%d %H:%M:%S")
        self.store.update_sync_state(
            last_backfill_sync_at=now,
            bootstrap_cursor_symbol=None if not stopped else last_cursor_symbol,
            bootstrap_status="stopped" if stopped else ("done" if errors == 0 else "done_with_errors"),
            last_error="bootstrap stopped by user" if stopped else ("" if errors == 0 else f"bootstrap errors={errors}"),
        )
        return {
            "success": (errors == 0) and (not stopped),
            "stopped": stopped,
            "attempted_symbols": attempted_symbols,
            "processed_symbols": processed_symbols,
            "errors": errors,
            "upserted": upserted,
            "from_index": start_idx,
            "total_symbols": total_symbols,
            "failed_providers": failed_providers,
        }

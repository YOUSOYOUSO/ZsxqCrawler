#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Dict

from modules.analyzers.market_data_sync import MarketDataSyncService


class _FakeStore:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def is_market_closed_now(self):
        return False

    def get_symbol_codes(self):
        return []

    def update_sync_state(self, **kwargs):
        return None

    def upsert_symbols(self, rows):
        return len(rows)

    def upsert_daily_prices(self, rows, adjust=None):
        return len(rows)

    def get_price_range(self, *_args, **_kwargs):
        return [{"trade_date": "2026-02-25", "close": 10.0}]


class _Provider:
    def __init__(self, name: str):
        self.name = name

    def fetch_symbols(self):
        return []

    def fetch_stock_history(self, **_kwargs):
        return []

    def fetch_index_history(self, **_kwargs):
        return []


def test_realtime_provider_failover_priority(monkeypatch):
    def _fake_build_provider(name, _config):
        return _Provider(name)

    monkeypatch.setattr("modules.analyzers.market_data_sync.build_provider", _fake_build_provider)

    svc = MarketDataSyncService(
        store=_FakeStore(
            {
                "enabled": True,
                "adjust": "qfq",
                "providers": ["akshare", "tx", "sina", "tushare"],
                "realtime_providers": ["akshare", "tx", "sina", "tushare"],
                "realtime_provider_failover_enabled": True,
                "provider_failover_enabled": True,
                "provider_circuit_breaker_seconds": 60.0,
                "sync_retry_max": 1,
                "sync_retry_backoff_seconds": 0.0,
                "sync_failure_cooldown_seconds": 0.0,
            }
        )
    )

    def _fake_rt(provider_name, _provider, _stock_code):
        if provider_name == "akshare":
            raise RuntimeError("akshare unavailable")
        if provider_name == "tx":
            return {
                "stock_code": "000001.SZ",
                "price": 12.34,
                "quote_time": "2026-02-26 10:00:00",
                "source": "tx.spot",
            }
        return None

    monkeypatch.setattr(svc, "_fetch_realtime_quote_from_provider", _fake_rt)

    result = svc.fetch_realtime_price("000001.SZ")

    assert result["success"] is True
    assert result["provider_used"] == "tx"
    assert result["failed_providers"] == ["akshare"]
    assert result["provider_path"][:2] == ["akshare", "tx"]

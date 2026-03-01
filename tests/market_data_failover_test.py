#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.analyzers.market_data_sync import MarketDataSyncService


class _FakeStore:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._symbols: List[str] = []
        self.upserted_rows = 0
        self.sync_state_updates: List[Dict[str, Any]] = []

    def upsert_symbols(self, rows):
        self._symbols = [row["stock_code"] for row in rows]
        return len(rows)

    def get_symbol_codes(self):
        return list(self._symbols)

    def is_market_closed_now(self):
        return False

    def upsert_daily_prices(self, rows, adjust=None):
        self.upserted_rows += len(rows)
        return len(rows)

    def update_sync_state(self, **kwargs):
        self.sync_state_updates.append(kwargs)

    def get_sync_state(self):
        return {}


class _AkshareFailProvider:
    name = "akshare"

    def fetch_symbols(self):
        return [{"stock_code": "000001.SZ", "stock_name": "平安银行", "market": "SZ", "source": "akshare"}]

    def fetch_stock_history(self, **kwargs):
        raise RuntimeError("('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))")

    def fetch_index_history(self, **kwargs):
        return []


class _TushareOkProvider:
    name = "tushare"

    def fetch_symbols(self):
        return [{"stock_code": "000001.SZ", "stock_name": "平安银行", "market": "SZ", "source": "tushare"}]

    def fetch_stock_history(self, stock_code, start_date, end_date, adjust):
        return [
            {
                "stock_code": stock_code,
                "trade_date": end_date,
                "open": 10.0,
                "close": 10.2,
                "high": 10.3,
                "low": 9.9,
                "change_pct": 1.2,
                "volume": 1000.0,
                "source": "tushare.pro_bar",
            }
        ]

    def fetch_index_history(self, start_date, end_date):
        return []


class _AlwaysFailProvider:
    def __init__(self, name):
        self.name = name

    def fetch_symbols(self):
        raise RuntimeError("provider unavailable")

    def fetch_stock_history(self, **kwargs):
        raise RuntimeError("provider unavailable")

    def fetch_index_history(self, **kwargs):
        raise RuntimeError("provider unavailable")


class _AlwaysEmptyProvider:
    def __init__(self, name):
        self.name = name

    def fetch_symbols(self):
        return [{"stock_code": "600673.SH", "stock_name": "东阳光", "market": "SH", "source": self.name}]

    def fetch_stock_history(self, **kwargs):
        return []

    def fetch_index_history(self, **kwargs):
        return []


def test_failover_switches_to_tushare_when_akshare_disconnect(monkeypatch):
    providers = {
        "akshare": _AkshareFailProvider(),
        "tushare": _TushareOkProvider(),
    }

    def _fake_build_provider(name, config):
        return providers[name]

    monkeypatch.setattr("modules.analyzers.market_data_sync.build_provider", _fake_build_provider)
    store = _FakeStore(
        {
            "enabled": True,
            "adjust": "qfq",
            "providers": ["akshare", "tushare"],
            "provider_failover_enabled": True,
            "provider_circuit_breaker_seconds": 300.0,
            "sync_retry_max": 3,
            "sync_retry_backoff_seconds": 0.0,
            "sync_failure_cooldown_seconds": 0.0,
        }
    )
    svc = MarketDataSyncService(store=store)

    res = svc.sync_daily_incremental(symbols=["000001.SZ"], include_index=False)

    assert res["success"] is True
    assert res["provider_used"] == "tushare"
    assert res["provider_switched"] is True
    assert "akshare" in res["failed_providers"]
    assert store.upserted_rows == 1


def test_incremental_fails_fast_when_all_providers_fail(monkeypatch):
    providers = {
        "akshare": _AlwaysFailProvider("akshare"),
        "tushare": _AlwaysFailProvider("tushare"),
    }

    def _fake_build_provider(name, config):
        return providers[name]

    monkeypatch.setattr("modules.analyzers.market_data_sync.build_provider", _fake_build_provider)
    store = _FakeStore(
        {
            "enabled": True,
            "adjust": "qfq",
            "providers": ["akshare", "tushare"],
            "provider_failover_enabled": True,
            "provider_circuit_breaker_seconds": 0.0,
            "sync_retry_max": 1,
            "sync_retry_backoff_seconds": 0.0,
            "sync_failure_cooldown_seconds": 0.0,
        }
    )
    svc = MarketDataSyncService(store=store)

    res = svc.sync_daily_incremental(symbols=["000001.SZ"], include_index=False)

    assert res["success"] is False
    assert res["errors"] == 1
    assert "akshare" in res["failed_providers"]
    assert "tushare" in res["failed_providers"]
    assert "所有行情源失败" in res["message"]


def test_tushare_provider_init_failure_is_reported():
    store = _FakeStore(
        {
            "enabled": True,
            "adjust": "qfq",
            "providers": ["tushare"],
            "provider_failover_enabled": True,
            "provider_circuit_breaker_seconds": 0.0,
            "sync_retry_max": 1,
            "sync_retry_backoff_seconds": 0.0,
            "sync_failure_cooldown_seconds": 0.0,
            "tushare_token": "",
        }
    )
    svc = MarketDataSyncService(store=store)

    res = svc.sync_symbols()

    assert res["success"] is False
    assert "tushare" in res["failed_providers"]
    assert "所有行情源失败" in res["message"]


def test_incremental_skips_suspended_symbol_when_all_providers_return_empty(monkeypatch):
    providers = {
        "akshare": _AlwaysEmptyProvider("akshare"),
        "tushare": _AlwaysEmptyProvider("tushare"),
    }

    def _fake_build_provider(name, config):
        return providers[name]

    monkeypatch.setattr("modules.analyzers.market_data_sync.build_provider", _fake_build_provider)
    store = _FakeStore(
        {
            "enabled": True,
            "adjust": "qfq",
            "providers": ["akshare", "tushare"],
            "provider_failover_enabled": True,
            "provider_circuit_breaker_seconds": 0.0,
            "sync_retry_max": 1,
            "sync_retry_backoff_seconds": 0.0,
            "sync_failure_cooldown_seconds": 0.0,
        }
    )
    svc = MarketDataSyncService(store=store)

    res = svc.sync_daily_incremental(symbols=["600673.SH"], include_index=False)

    assert res["success"] is True
    assert res["errors"] == 0
    assert res["skipped"] == 1
    assert res["upserted"] == 0

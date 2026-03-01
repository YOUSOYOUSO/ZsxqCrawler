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


class _OkProvider:
    def __init__(self, name: str):
        self.name = name

    def fetch_symbols(self):
        return [{"stock_code": "000001.SZ", "stock_name": "平安银行", "market": "SZ", "source": self.name}]

    def fetch_stock_history(self, stock_code, start_date, end_date, adjust):
        return []

    def fetch_index_history(self, start_date, end_date):
        return []


def test_tushare_missing_token_not_routable(monkeypatch):
    def _fake_build_provider(name, config):
        if name == "tushare":
            raise RuntimeError("tushare token is empty")
        return _OkProvider(name)

    monkeypatch.setattr("modules.analyzers.market_data_sync.build_provider", _fake_build_provider)
    svc = MarketDataSyncService(
        store=_FakeStore(
            {
                "enabled": True,
                "adjust": "qfq",
                "providers": ["tx", "sina", "akshare", "tushare"],
                "provider_failover_enabled": True,
                "provider_circuit_breaker_seconds": 60.0,
                "sync_retry_max": 1,
                "sync_retry_backoff_seconds": 0.0,
                "sync_failure_cooldown_seconds": 0.0,
                "tushare_token": "",
            }
        )
    )

    snap = svc.get_provider_health_snapshot(stock_code="600000.SH")
    by_name = {x["provider"]: x for x in snap["providers"]}
    assert by_name["tushare"]["routable"] is False
    assert "init_failed" in by_name["tushare"]["disabled_reason"]
    assert len(snap["routable_providers"]) >= 1


def test_bj_probe_marks_tx_sina_unroutable(monkeypatch):
    def _fake_build_provider(name, config):
        if name == "tushare":
            raise RuntimeError("tushare token is empty")
        return _OkProvider(name)

    monkeypatch.setattr("modules.analyzers.market_data_sync.build_provider", _fake_build_provider)
    svc = MarketDataSyncService(
        store=_FakeStore(
            {
                "enabled": True,
                "adjust": "qfq",
                "providers": ["tx", "sina", "akshare", "tushare"],
                "provider_failover_enabled": True,
                "provider_circuit_breaker_seconds": 60.0,
                "sync_retry_max": 1,
                "sync_retry_backoff_seconds": 0.0,
                "sync_failure_cooldown_seconds": 0.0,
                "tushare_token": "",
            }
        )
    )

    snap = svc.get_provider_health_snapshot(stock_code="920368.BJ")
    by_name = {x["provider"]: x for x in snap["providers"]}
    assert by_name["tx"]["routable"] is False
    assert "market_unsupported:BJ" in by_name["tx"]["disabled_reason"]
    assert by_name["sina"]["routable"] is False

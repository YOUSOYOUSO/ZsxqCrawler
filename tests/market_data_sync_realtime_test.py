#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import pandas as pd

from modules.analyzers.market_data_sync import MarketDataSyncService


class _FakeStore:
    def get_price_range(self, *_args, **_kwargs):
        return [{"trade_date": "2026-02-25", "close": 36.2}]


class _FakeTusharePro:
    def rt_min(self, **_kwargs):
        return pd.DataFrame(
            [
                {
                    "ts_code": "000792.SZ",
                    "freq": "1MIN",
                    "time": "2026-02-26 13:43:00",
                    "open": 39.05,
                    "close": 39.06,
                    "high": 39.06,
                    "low": 39.01,
                    "vol": 580100.0,
                    "amount": 22643163.0,
                }
            ]
        )

    def stk_mins(self, **_kwargs):
        return pd.DataFrame([])

    def realtime_quote(self, **_kwargs):
        return pd.DataFrame([])


class _FakeProvider:
    def __init__(self):
        self.pro = _FakeTusharePro()


def test_fetch_realtime_quote_from_tushare_rt_min():
    svc = MarketDataSyncService.__new__(MarketDataSyncService)
    svc.store = _FakeStore()

    quote = svc._fetch_realtime_quote_from_provider(
        provider_name="tushare",
        provider=_FakeProvider(),
        stock_code="000792.SZ",
    )

    assert quote is not None
    assert quote["price"] == 39.06
    assert quote["quote_time"] == "2026-02-26 13:43:00"
    assert quote["pre_close"] == 36.2
    assert quote["source"] == "tushare.rt_min"

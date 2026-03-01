#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import sqlite3
import sys
import types
from pathlib import Path
from datetime import datetime, timedelta, timezone

if "ahocorasick" not in sys.modules:
    mod = types.ModuleType("ahocorasick")

    class _Auto:
        def add_word(self, *_args, **_kwargs):
            return None

        def make_automaton(self):
            return None

        def iter(self, _text):
            return iter([])

    mod.Automaton = _Auto  # type: ignore[attr-defined]
    sys.modules["ahocorasick"] = mod

if "akshare" not in sys.modules:
    sys.modules["akshare"] = types.ModuleType("akshare")

from modules.analyzers.stock_analyzer import StockAnalyzer

BEIJING_TZ = timezone(timedelta(hours=8))


class _FakePathManager:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir

    def get_topics_db_path(self, group_id: str) -> str:
        return str(Path(self.base_dir) / f"zsxq_topics_{group_id}.db")


def _prepare_topic_tables(db_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS topics (topic_id INTEGER PRIMARY KEY, create_time TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS talks (topic_id INTEGER PRIMARY KEY, text TEXT)")
    conn.commit()
    conn.close()


def test_build_t0_metrics_for_history_uses_local_fallback_only(monkeypatch, tmp_path):
    fake_pm = _FakePathManager(str(tmp_path))
    monkeypatch.setattr("modules.analyzers.stock_analyzer.get_db_path_manager", lambda: fake_pm)

    analyzer = StockAnalyzer(group_id="910001")
    _prepare_topic_tables(fake_pm.get_topics_db_path("910001"))

    called = {"snapshot": 0}

    def _fail_snapshot(*args, **kwargs):
        called["snapshot"] += 1
        raise AssertionError("historical mention should not fetch realtime snapshot")

    monkeypatch.setattr(analyzer, "_fetch_snapshot_price", _fail_snapshot)
    monkeypatch.setattr(analyzer, "_get_recent_local_price", lambda *_args, **_kwargs: (12.34, "2026-02-10", ""))

    payload = analyzer._build_t0_metrics(
        mention_id=1,
        stock_code="000001.SZ",
        mention_date="2026-02-10",
        mention_time="2026-02-10 10:00:00",
        existing={},
        force_refresh=False,
    )

    assert called["snapshot"] == 0
    assert payload["t0_buy_source"] == "fallback_recent"
    assert isinstance(payload["t0_buy_price"], float)


def test_snapshot_cache_ttl_reuses_result(monkeypatch, tmp_path):
    fake_pm = _FakePathManager(str(tmp_path))
    monkeypatch.setattr("modules.analyzers.stock_analyzer.get_db_path_manager", lambda: fake_pm)

    analyzer = StockAnalyzer(group_id="910002")
    _prepare_topic_tables(fake_pm.get_topics_db_path("910002"))

    state = {"has_today": False}

    def _fake_get_price_range(stock_code, start_date, end_date, allow_today_unfinal=False, adjust=None):
        _ = (stock_code, start_date, end_date, allow_today_unfinal, adjust)
        if state["has_today"]:
            return [{"trade_date": start_date, "close": 10.5}]
        state["has_today"] = True
        return []

    monkeypatch.setattr(analyzer.market_store, "get_price_range", _fake_get_price_range)
    monkeypatch.setattr(analyzer.market_store, "is_market_closed_now", lambda: False)

    r1 = analyzer._fetch_snapshot_price("000001.SZ", "2026-02-25 10:11:12", "2026-02-25", force_refresh=False)
    r2 = analyzer._fetch_snapshot_price("000001.SZ", "2026-02-25 10:11:12", "2026-02-25", force_refresh=False)

    assert r1["buy_source"] in {"snapshot", "fallback_recent"}
    assert r2["buy_price"] == r1["buy_price"]


def test_fetch_snapshot_price_preopen_returns_pending_open(monkeypatch, tmp_path):
    fake_pm = _FakePathManager(str(tmp_path))
    monkeypatch.setattr("modules.analyzers.stock_analyzer.get_db_path_manager", lambda: fake_pm)

    analyzer = StockAnalyzer(group_id="910002b")
    _prepare_topic_tables(fake_pm.get_topics_db_path("910002b"))

    today = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
    result = analyzer._fetch_snapshot_price(
        stock_code="000001.SZ",
        mention_time=f"{today} 08:43:00",
        mention_date=today,
        force_refresh=False,
    )
    assert result["buy_source"] == "pending_open"
    assert result["buy_price"] is None


def test_build_t0_metrics_uses_realtime_quote_when_today_daily_missing(monkeypatch, tmp_path):
    fake_pm = _FakePathManager(str(tmp_path))
    monkeypatch.setattr("modules.analyzers.stock_analyzer.get_db_path_manager", lambda: fake_pm)

    analyzer = StockAnalyzer(group_id="910003")
    _prepare_topic_tables(fake_pm.get_topics_db_path("910003"))

    # 固定 buy 侧，不让测试依赖外部快照
    monkeypatch.setattr(
        analyzer,
        "_fetch_snapshot_price",
        lambda *_args, **_kwargs: {
            "buy_price": 10.0,
            "buy_ts": "2026-02-26",
            "buy_source": "snapshot",
            "note": "",
        },
    )

    # 当日日线不可得
    monkeypatch.setattr(analyzer.market_store, "get_price_range", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(analyzer.market_store, "is_market_closed_now", lambda: False)
    monkeypatch.setattr(analyzer, "_is_preopen_window", lambda: False)

    # 实时报价可得
    monkeypatch.setattr(
        analyzer.market_sync,
        "fetch_realtime_price",
        lambda *_args, **_kwargs: {
            "success": True,
            "price": 10.8,
            "quote_time": "2026-02-26 10:01:02",
            "source": "tushare.realtime_quote",
        },
    )

    today = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")

    payload = analyzer._build_t0_metrics(
        mention_id=2,
        stock_code="000001.SZ",
        mention_date=today,
        mention_time=f"{today} 10:00:00",
        existing={},
        force_refresh=True,
    )

    assert payload["t0_status"] == "realtime"
    assert payload["t0_end_price_rt"] == 10.8
    assert payload["t0_return_rt"] == 8.0
    assert "实时价来源" in str(payload.get("t0_note") or "")


def test_fetch_snapshot_price_fallbacks_to_realtime_when_no_local_price(monkeypatch, tmp_path):
    fake_pm = _FakePathManager(str(tmp_path))
    monkeypatch.setattr("modules.analyzers.stock_analyzer.get_db_path_manager", lambda: fake_pm)

    analyzer = StockAnalyzer(group_id="910004")
    _prepare_topic_tables(fake_pm.get_topics_db_path("910004"))

    monkeypatch.setattr(analyzer.market_store, "is_market_closed_now", lambda: False)
    monkeypatch.setattr(analyzer.market_store, "get_price_range", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(analyzer.market_sync, "sync_daily_incremental", lambda **_kwargs: {"success": True})
    monkeypatch.setattr(analyzer, "_get_recent_local_price", lambda *_args, **_kwargs: (None, None, "no_local"))
    monkeypatch.setattr(
        analyzer.market_sync,
        "fetch_realtime_price",
        lambda *_args, **_kwargs: {
            "success": True,
            "price": 39.12,
            "quote_time": "2026-02-26 13:43:00",
            "source": "tushare.rt_min",
        },
    )

    result = analyzer._fetch_snapshot_price(
        stock_code="000792.SZ",
        mention_time="2026-02-26 09:11:00",
        mention_date="2026-02-26",
        force_refresh=True,
    )

    assert result["buy_source"] == "fallback_realtime"
    assert result["buy_price"] == 39.12


def test_passive_sync_intraday_triggers_incremental_when_open_missing(monkeypatch, tmp_path):
    fake_pm = _FakePathManager(str(tmp_path))
    monkeypatch.setattr("modules.analyzers.stock_analyzer.get_db_path_manager", lambda: fake_pm)

    analyzer = StockAnalyzer(group_id="910005")
    _prepare_topic_tables(fake_pm.get_topics_db_path("910005"))

    called = {"sync": 0}
    monkeypatch.setattr(analyzer, "_get_market_phase", lambda *args, **kwargs: "intraday")
    monkeypatch.setattr(
        analyzer.market_store,
        "get_symbol_day_snapshot_info",
        lambda **_kwargs: {"exists": False, "open": None, "close": None, "is_final": None, "fetched_at": None},
    )
    monkeypatch.setattr(
        analyzer.market_sync,
        "sync_daily_incremental",
        lambda **_kwargs: called.__setitem__("sync", called["sync"] + 1) or {"success": True},
    )

    triggered = analyzer._maybe_passive_sync_for_t0("000001.SZ")
    assert triggered is True
    assert called["sync"] == 1


def test_passive_sync_postclose_triggers_finalize_when_not_final(monkeypatch, tmp_path):
    fake_pm = _FakePathManager(str(tmp_path))
    monkeypatch.setattr("modules.analyzers.stock_analyzer.get_db_path_manager", lambda: fake_pm)

    analyzer = StockAnalyzer(group_id="910006")
    _prepare_topic_tables(fake_pm.get_topics_db_path("910006"))

    called = {"finalize": 0}
    monkeypatch.setattr(analyzer, "_get_market_phase", lambda *args, **kwargs: "postclose")
    monkeypatch.setattr(analyzer.market_store, "has_final_for_symbol_date", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(
        analyzer.market_sync,
        "finalize_today_after_close",
        lambda **_kwargs: called.__setitem__("finalize", called["finalize"] + 1) or {"success": True},
    )

    triggered = analyzer._maybe_passive_sync_for_t0("000001.SZ")
    assert triggered is True
    assert called["finalize"] == 1

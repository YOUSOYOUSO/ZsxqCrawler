#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import sqlite3
import sys
import types
from pathlib import Path

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


def _fake_payload():
    returns = {1: 1.0, 3: 1.1, 5: 1.2, 10: 1.3, 20: 1.4, 60: None, 120: None, 250: None}
    excess = {1: 0.1, 3: 0.2, 5: 0.3, 10: 0.4, 20: 0.5, 60: None, 120: None, 250: None}
    return {
        "periods_to_calc": [1, 3, 5, 10, 20, 60, 120, 250],
        "price_at_mention": 10.0,
        "returns": returns,
        "excess_returns": excess,
        "max_return": 5.0,
        "max_drawdown": -2.0,
        "new_freeze": 0,
    }


def test_calc_pending_performance_processes_all_mentions_for_same_stock(monkeypatch, tmp_path):
    fake_pm = _FakePathManager(str(tmp_path))
    monkeypatch.setattr("modules.analyzers.stock_analyzer.get_db_path_manager", lambda: fake_pm)

    gid = "900001"
    db_path = fake_pm.get_topics_db_path(gid)
    _prepare_topic_tables(db_path)
    analyzer = StockAnalyzer(group_id=gid)

    conn = analyzer._get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO stock_mentions(topic_id, stock_code, stock_name, mention_date, mention_time, context_snippet) VALUES(?,?,?,?,?,?)",
        (1, "000001.SZ", "平安银行", "2026-02-10", "2026-02-10 10:00:00", "m1"),
    )
    cur.execute(
        "INSERT INTO stock_mentions(topic_id, stock_code, stock_name, mention_date, mention_time, context_snippet) VALUES(?,?,?,?,?,?)",
        (2, "000001.SZ", "平安银行", "2026-02-11", "2026-02-11 10:00:00", "m2"),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(analyzer, "_build_stock_dictionary", lambda: None)
    monkeypatch.setattr(
        analyzer.market_sync,
        "get_provider_health_snapshot",
        lambda op_name=None: {"routable_providers": ["mock"]},
    )
    monkeypatch.setattr(analyzer.market_store, "get_latest_trade_date", lambda only_final=True: "2099-12-31")
    monkeypatch.setattr(
        analyzer,
        "_compute_performance_payload",
        lambda **kwargs: (True, "ok", _fake_payload()),
    )
    monkeypatch.setattr(analyzer, "PERF_PREWARM_ENABLED", False)

    saved_ids = []

    def _fake_save_batch(batch_items):
        for item in batch_items:
            saved_ids.append(int(item["mention_id"]))
        return len(batch_items)

    monkeypatch.setattr(analyzer, "_save_performance_payload_batch", _fake_save_batch)

    res = analyzer.calc_pending_performance(calc_window_days=365)
    assert res["processed"] == 2
    assert sorted(saved_ids) == sorted(set(saved_ids))
    assert len(saved_ids) == 2
    assert "db_batch_commits" in res
    assert "total_mentions" in res
    assert "calc_seconds" in res


def test_recalculate_performance_range_force_only_targets_date_range(monkeypatch, tmp_path):
    fake_pm = _FakePathManager(str(tmp_path))
    monkeypatch.setattr("modules.analyzers.stock_analyzer.get_db_path_manager", lambda: fake_pm)

    gid = "900002"
    db_path = fake_pm.get_topics_db_path(gid)
    _prepare_topic_tables(db_path)
    analyzer = StockAnalyzer(group_id=gid)

    conn = analyzer._get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO stock_mentions(topic_id, stock_code, stock_name, mention_date, mention_time, context_snippet) VALUES(?,?,?,?,?,?)",
        (11, "000001.SZ", "平安银行", "2026-02-05", "2026-02-05 10:00:00", "in"),
    )
    in_range_mention_id = int(cur.lastrowid)
    cur.execute(
        "INSERT INTO mention_performance(mention_id, stock_code, mention_date, freeze_level) VALUES(?,?,?,?)",
        (in_range_mention_id, "000001.SZ", "2026-02-05", 3),
    )
    cur.execute(
        "INSERT INTO stock_mentions(topic_id, stock_code, stock_name, mention_date, mention_time, context_snippet) VALUES(?,?,?,?,?,?)",
        (12, "000001.SZ", "平安银行", "2026-03-05", "2026-03-05 10:00:00", "out"),
    )
    out_range_mention_id = int(cur.lastrowid)
    cur.execute(
        "INSERT INTO mention_performance(mention_id, stock_code, mention_date, freeze_level) VALUES(?,?,?,?)",
        (out_range_mention_id, "000001.SZ", "2026-03-05", 3),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(analyzer, "_build_stock_dictionary", lambda: None)
    monkeypatch.setattr(
        analyzer,
        "_compute_performance_payload",
        lambda **kwargs: (True, "ok", _fake_payload()),
    )

    saved_ids = []

    def _fake_save(mention_id, stock_code, mention_date, payload, row_exists):
        saved_ids.append((int(mention_id), bool(row_exists)))

    monkeypatch.setattr(analyzer, "_save_performance_payload", _fake_save)

    res = analyzer.recalculate_performance_range(
        start_date="2026-02-01",
        end_date="2026-02-28",
        force=True,
    )
    assert res["total"] == 1
    assert res["processed"] == 1
    assert saved_ids == [(in_range_mention_id, False)]


def test_fetch_price_range_backfills_when_window_is_insufficient(monkeypatch, tmp_path):
    fake_pm = _FakePathManager(str(tmp_path))
    monkeypatch.setattr("modules.analyzers.stock_analyzer.get_db_path_manager", lambda: fake_pm)

    analyzer = StockAnalyzer(group_id="900003")

    first_rows = [{"trade_date": "2026-02-05", "open": 1, "close": 1, "high": 1, "low": 1, "change_pct": 0, "volume": 1}]
    second_rows = [
        {"trade_date": "2025-08-20", "open": 1, "close": 1, "high": 1, "low": 1, "change_pct": 0, "volume": 1},
        {"trade_date": "2026-02-20", "open": 1, "close": 1, "high": 1, "low": 1, "change_pct": 0, "volume": 1},
    ]
    calls = {"get": 0, "sync": []}

    def _fake_get_price_range(**kwargs):
        calls["get"] += 1
        return first_rows if calls["get"] == 1 else second_rows

    def _fake_sync_daily_incremental(**kwargs):
        calls["sync"].append(kwargs)
        return {"success": True}

    monkeypatch.setattr(analyzer.market_store, "is_market_closed_now", lambda: False)
    monkeypatch.setattr(analyzer, "_is_preopen_window", lambda: False)
    monkeypatch.setattr(analyzer.market_store, "get_price_range", _fake_get_price_range)
    monkeypatch.setattr(analyzer.market_sync, "sync_daily_incremental", _fake_sync_daily_incremental)

    rows = analyzer.fetch_price_range("688256.SH", "2025-08-18", "2026-02-25")

    assert len(rows) == 2
    assert len(calls["sync"]) == 1
    assert calls["sync"][0]["symbols"] == ["688256.SH"]
    assert calls["sync"][0]["include_index"] is False
    assert int(calls["sync"][0]["history_days"]) >= 150


def test_fetch_price_range_preopen_caps_end_to_prev_trade_and_skips_sync(monkeypatch, tmp_path):
    fake_pm = _FakePathManager(str(tmp_path))
    monkeypatch.setattr("modules.analyzers.stock_analyzer.get_db_path_manager", lambda: fake_pm)
    analyzer = StockAnalyzer(group_id="900003b")

    calls = {"sync": 0}

    def _fake_get_price_range(stock_code, start_date, end_date, allow_today_unfinal=False, adjust=None):
        _ = (stock_code, start_date, allow_today_unfinal, adjust)
        return [
            {"trade_date": start_date, "open": 1, "close": 1, "high": 1, "low": 1, "change_pct": 0, "volume": 1},
            {"trade_date": end_date, "open": 1, "close": 1, "high": 1, "low": 1, "change_pct": 0, "volume": 1},
        ]

    monkeypatch.setattr(analyzer, "get_data_anchor_date", lambda: "2026-02-26")
    monkeypatch.setattr(analyzer, "_is_preopen_window", lambda: True)
    monkeypatch.setattr(analyzer.market_store, "is_market_closed_now", lambda: False)
    monkeypatch.setattr(analyzer.market_store, "get_price_range", _fake_get_price_range)
    monkeypatch.setattr(
        analyzer.market_sync,
        "sync_daily_incremental",
        lambda **kwargs: calls.__setitem__("sync", calls["sync"] + 1) or {"success": True},
    )

    rows = analyzer.fetch_price_range("688256.SH", "2026-02-01", "2026-02-27", data_mode="live")
    assert len(rows) == 2
    assert max(r["trade_date"] for r in rows) == "2026-02-26"
    assert calls["sync"] == 0


def test_fetch_price_range_backfill_circuit_breaks_duplicate_symbol_in_same_run(monkeypatch, tmp_path):
    fake_pm = _FakePathManager(str(tmp_path))
    monkeypatch.setattr("modules.analyzers.stock_analyzer.get_db_path_manager", lambda: fake_pm)
    analyzer = StockAnalyzer(group_id="900003c")

    calls = {"sync": 0}

    def _fake_get_price_range(stock_code, start_date, end_date, allow_today_unfinal=False, adjust=None):
        _ = (stock_code, allow_today_unfinal, adjust)
        return [{"trade_date": end_date, "open": 1, "close": 1, "high": 1, "low": 1, "change_pct": 0, "volume": 1}]

    monkeypatch.setattr(analyzer, "_is_preopen_window", lambda: False)
    monkeypatch.setattr(analyzer.market_store, "is_market_closed_now", lambda: False)
    monkeypatch.setattr(analyzer.market_store, "get_price_range", _fake_get_price_range)
    monkeypatch.setattr(
        analyzer.market_sync,
        "sync_daily_incremental",
        lambda **kwargs: calls.__setitem__("sync", calls["sync"] + 1) or {"success": True, "kwargs": kwargs},
    )

    perf_context = {"lock": None, "backfill_attempted_symbols": set(), "network_backfill_symbols": set(), "network_seconds": 0.0}
    analyzer.fetch_price_range("688256.SH", "2026-01-01", "2026-02-27", data_mode="live", perf_context=perf_context)
    analyzer.fetch_price_range("688256.SH", "2026-01-01", "2026-02-27", data_mode="live", perf_context=perf_context)
    assert calls["sync"] == 1


def test_compute_performance_payload_skips_when_base_trade_date_too_late(monkeypatch, tmp_path):
    fake_pm = _FakePathManager(str(tmp_path))
    monkeypatch.setattr("modules.analyzers.stock_analyzer.get_db_path_manager", lambda: fake_pm)

    analyzer = StockAnalyzer(group_id="900004")

    late_prices = [
        {"trade_date": "2026-02-05", "open": 100, "close": 100, "high": 101, "low": 99, "change_pct": 0, "volume": 1},
        {"trade_date": "2026-02-06", "open": 101, "close": 101, "high": 102, "low": 100, "change_pct": 1, "volume": 1},
    ]

    monkeypatch.setattr(
        analyzer,
        "fetch_price_range",
        lambda stock_code, start_date, end_date, data_mode="live": late_prices,
    )
    monkeypatch.setattr(
        analyzer,
        "_fetch_index_price",
        lambda start_date, end_date, data_mode="live": {"2026-02-05": 1000.0},
    )

    ok, reason, payload = analyzer._compute_performance_payload(
        stock_code="688256.SH",
        mention_date="2025-08-28",
        mention_time="2025-08-28 10:00:00",
        current_freeze=0,
    )

    assert ok is False
    assert "提及日附近行情缺失" in reason
    assert payload is None

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


def _prepare_db(db_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS topics (topic_id INTEGER PRIMARY KEY, create_time TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS talks (topic_id INTEGER PRIMARY KEY, text TEXT)")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_mentions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic_id INTEGER NOT NULL,
            stock_code TEXT NOT NULL,
            stock_name TEXT NOT NULL,
            mention_date TEXT NOT NULL,
            mention_time TEXT NOT NULL,
            context_snippet TEXT
        )
        """
    )
    cur.execute("INSERT INTO talks(topic_id, text) VALUES(1, 'x')")
    cur.execute(
        "INSERT INTO stock_mentions(topic_id, stock_code, stock_name, mention_date, mention_time, context_snippet) VALUES(1,'000001.SZ','平安银行','2026-02-26','2026-02-26 10:00:00','ctx')"
    )
    conn.commit()
    conn.close()


def test_get_stock_events_manual_refresh_has_cooldown(monkeypatch, tmp_path):
    fake_pm = _FakePathManager(str(tmp_path))
    monkeypatch.setattr("modules.analyzers.stock_analyzer.get_db_path_manager", lambda: fake_pm)
    gid = "910100"
    _prepare_db(fake_pm.get_topics_db_path(gid))
    analyzer = StockAnalyzer(group_id=gid)

    calls = []

    def _fake_t0(*_args, **kwargs):
        calls.append(bool(kwargs.get("force_refresh")))
        return {
            "t0_buy_price": 10.0,
            "t0_buy_ts": "2026-02-26",
            "t0_buy_source": "snapshot_open",
            "t0_end_price_rt": 10.1,
            "t0_end_price_rt_ts": "2026-02-26 10:01:00",
            "t0_end_price_close": None,
            "t0_end_price_close_ts": None,
            "t0_return_rt": 1.0,
            "t0_return_close": None,
            "t0_status": "realtime",
            "t0_note": "",
            "t0_session_trade_date": "2026-02-26",
            "t0_window_tag": "intraday",
        }

    monkeypatch.setattr(analyzer, "_build_t0_metrics", _fake_t0)
    monkeypatch.setattr(analyzer.market_store, "is_market_closed_now", lambda: False)
    monkeypatch.setattr(
        analyzer.market_store,
        "get_symbol_day_snapshot_info",
        lambda stock_code, trade_date: {"fetched_at": "2026-02-26 10:02:00", "is_final": 0},
    )

    r1 = analyzer.get_stock_events("000001.SZ", refresh_realtime=True, detail_mode="full")
    r2 = analyzer.get_stock_events("000001.SZ", refresh_realtime=True, detail_mode="full")

    assert calls[0] is True
    assert calls[1] is False
    assert r1.get("refresh_source") == "manual"
    assert r2.get("refresh_source") in {"auto_local", "manual_cooldown"}

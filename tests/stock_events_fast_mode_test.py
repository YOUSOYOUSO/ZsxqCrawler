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
    cur.execute("INSERT INTO talks(topic_id, text) VALUES(1, 'topic_1_full_text')")
    cur.execute("INSERT INTO talks(topic_id, text) VALUES(2, 'topic_2_full_text')")
    cur.execute(
        "INSERT INTO stock_mentions(topic_id, stock_code, stock_name, mention_date, mention_time, context_snippet) VALUES(1,'000001.SZ','平安银行','2026-02-26','2026-02-26 10:00:00','ctx1')"
    )
    cur.execute(
        "INSERT INTO stock_mentions(topic_id, stock_code, stock_name, mention_date, mention_time, context_snippet) VALUES(2,'000001.SZ','平安银行','2026-02-26','2026-02-26 09:00:00','ctx2')"
    )
    conn.commit()
    conn.close()


def test_get_stock_events_fast_mode_skips_t0_recompute_and_supports_pagination(monkeypatch, tmp_path):
    fake_pm = _FakePathManager(str(tmp_path))
    monkeypatch.setattr("modules.analyzers.stock_analyzer.get_db_path_manager", lambda: fake_pm)
    gid = "920100"
    _prepare_db(fake_pm.get_topics_db_path(gid))
    analyzer = StockAnalyzer(group_id=gid)

    def _should_not_call(*_args, **_kwargs):
        raise AssertionError("fast mode should not call _build_t0_metrics")

    monkeypatch.setattr(analyzer, "_build_t0_metrics", _should_not_call)
    monkeypatch.setattr(analyzer.market_store, "is_market_closed_now", lambda: False)
    monkeypatch.setattr(
        analyzer.market_store,
        "get_symbol_day_snapshot_info",
        lambda stock_code, trade_date: {"fetched_at": "2026-02-26 10:02:00", "is_final": 0},
    )

    payload = analyzer.get_stock_events(
        "000001.SZ",
        detail_mode="fast",
        page=1,
        per_page=1,
        include_full_text=False,
    )

    assert payload["detail_mode"] == "fast"
    assert payload["page"] == 1
    assert payload["per_page"] == 1
    assert payload["total_mentions"] == 2
    assert len(payload["events"]) == 1
    assert payload["events"][0]["full_text"] == ""

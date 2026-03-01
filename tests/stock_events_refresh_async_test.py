#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import sqlite3
import sys
import time
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


def test_schedule_stock_events_refresh_dedups_running_jobs(monkeypatch, tmp_path):
    fake_pm = _FakePathManager(str(tmp_path))
    monkeypatch.setattr("modules.analyzers.stock_analyzer.get_db_path_manager", lambda: fake_pm)
    gid = "920101"
    _prepare_db(fake_pm.get_topics_db_path(gid))
    analyzer = StockAnalyzer(group_id=gid)

    calls = []

    def _fake_get(*_args, **kwargs):
        calls.append(kwargs)
        time.sleep(0.2)
        return {"ok": True}

    monkeypatch.setattr(analyzer, "get_stock_events", _fake_get)

    first = analyzer.schedule_stock_events_refresh("000001.SZ")
    second = analyzer.schedule_stock_events_refresh("000001.SZ")

    assert first["queued"] is True
    assert second["queued"] is False
    assert second["reason"] == "already_running"

    time.sleep(0.35)
    state = analyzer._read_events_refresh_state("000001.SZ")
    assert state["refresh_state"] == "completed"
    assert len(calls) == 1

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import sqlite3
import sys
import types
from pathlib import Path

# test environment may not have binary deps
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


def test_extract_only_reprocesses_when_extractor_version_changed(monkeypatch, tmp_path):
    fake_pm = _FakePathManager(str(tmp_path))
    monkeypatch.setattr("modules.analyzers.stock_analyzer.get_db_path_manager", lambda: fake_pm)

    gid = "999001"
    db_path = fake_pm.get_topics_db_path(gid)
    _prepare_topic_tables(db_path)

    analyzer = StockAnalyzer(group_id=gid)
    conn = analyzer._get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO topics(topic_id, create_time) VALUES(?, ?)", (1, "2026-02-23T10:00:00+0800"))
    cur.execute("INSERT OR REPLACE INTO talks(topic_id, text) VALUES(?, ?)", (1, "看好平安银行"))
    cur.execute(
        """
        INSERT OR REPLACE INTO topic_analysis_state
        (topic_id, text_hash, extractor_version, extracted_at, perf_status, last_error, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (1, "oldhash", "v1", "2026-02-23 10:00:00", "complete", "", "2026-02-23 10:00:00"),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(analyzer, "_build_stock_dictionary", lambda: None)
    monkeypatch.setattr(
        analyzer,
        "extract_stocks",
        lambda text: [{"code": "000001.SZ", "name": "平安银行", "context": "看好平安银行"}],
    )

    res = analyzer.extract_only()
    assert res["new_topics"] == 1
    assert res["mentions_extracted"] == 1

    conn = analyzer._get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM stock_mentions WHERE topic_id = 1")
    assert cur.fetchone()[0] == 1
    cur.execute("SELECT extractor_version, perf_status FROM topic_analysis_state WHERE topic_id = 1")
    row = cur.fetchone()
    conn.close()
    assert row[0] == analyzer.EXTRACTOR_VERSION
    assert row[1] == "pending"

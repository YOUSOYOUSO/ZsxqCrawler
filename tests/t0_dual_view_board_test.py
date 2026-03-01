#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from modules.shared.t0_board import build_t0_dual_board, compute_session_trade_date

BEIJING_TZ = timezone(timedelta(hours=8))


def test_compute_session_trade_date_after_close_uses_next_trade_day():
    session_date, tag = compute_session_trade_date(
        mention_time="2026-02-25 15:30:00",
        mention_date="2026-02-25",
        close_finalize_time="15:05",
        open_time="09:30",
    )
    assert session_date == "2026-02-26"
    assert tag == "after_close"


def test_compute_session_trade_date_preopen_stays_same_day():
    session_date, tag = compute_session_trade_date(
        mention_time="2026-02-25 08:45:00",
        mention_date="2026-02-25",
        close_finalize_time="15:05",
        open_time="09:30",
    )
    assert session_date == "2026-02-25"
    assert tag == "preopen"


def test_build_t0_dual_board_uses_close_to_close_windows_and_tie_breaks_to_earliest():
    events = [
        {
            "mention_id": 1,
            "topic_id": "A",
            "group_id": "g1",
            "group_name": "群A",
            "mention_date": "2026-02-24",
            "mention_time": "2026-02-24 10:10:00",
            "t0_return_rt": 4.0,
            "t0_status": "finalized",
        },
        {
            "mention_id": 2,
            "topic_id": "B",
            "group_id": "g2",
            "group_name": "群B",
            "mention_date": "2026-02-24",
            "mention_time": "2026-02-24 12:00:00",
            "t0_return_rt": 4.0,
            "t0_status": "finalized",
        },
        {
            "mention_id": 3,
            "topic_id": "C",
            "group_id": "g1",
            "group_name": "群A",
            "mention_date": "2026-02-25",
            "mention_time": "2026-02-25 11:09:00",
            "t0_return_close": 7.931,
            "t0_status": "realtime",
        },
        {
            "mention_id": 4,
            "topic_id": "D",
            "group_id": "g3",
            "group_name": "群C",
            "mention_date": "2026-02-25",
            "mention_time": "2026-02-25 15:20:00",
            "t0_return_close": 2.5,
            "t0_status": "realtime",
        },
    ]
    now = datetime(2026, 2, 25, 14, 0, 0, tzinfo=BEIJING_TZ)
    board = build_t0_dual_board(
        events=events,
        close_finalize_time="15:05",
        open_time="09:30",
        now_dt=now,
    )

    assert board["base_trade_date"] == "2026-02-25"
    assert board["next_trade_date"] == "2026-02-26"
    assert len(board["views"]) == 3

    d_minus_1 = board["views"][0]
    d_view = board["views"][1]
    next_view = board["views"][2]

    assert d_minus_1["trade_date"] == "2026-02-24"
    assert d_minus_1["status"] == "finalized"
    assert d_minus_1["has_data"] is True
    assert d_minus_1["max_return"] == 4.0
    # 并列 4.0 时选择更早的 10:10
    assert d_minus_1["max_event"]["mention_id"] == 1

    assert d_view["trade_date"] == "2026-02-25"
    assert d_view["status"] == "realtime"
    assert d_view["has_data"] is True
    assert d_view["max_return"] == 7.931
    # 11:09 位于前一交易日收盘到当日收盘区间，应落入 D
    assert any(row["mention_id"] == 3 for row in d_view["rows"])

    assert next_view["trade_date"] == "2026-02-26"
    assert next_view["status"] == "preview"
    # 15:20 已跨入 D+1 区间（当日收盘到下一交易日收盘）
    assert next_view["has_data"] is True
    assert next_view["rows"][0]["mention_id"] == 4


def test_build_t0_dual_board_d_view_turns_finalized_after_close():
    events = [
        {
            "mention_id": 100,
            "topic_id": "T",
            "group_id": "g1",
            "group_name": "群A",
            "mention_date": "2026-02-25",
            "mention_time": "2026-02-25 11:00:00",
            "t0_return_rt": 1.5,
        }
    ]
    board = build_t0_dual_board(
        events=events,
        close_finalize_time="15:05",
        open_time="09:30",
        now_dt=datetime(2026, 2, 25, 15, 6, 0, tzinfo=BEIJING_TZ),
    )
    assert board["views"][1]["view_key"] == "d_view"
    assert board["views"][1]["status"] == "finalized"

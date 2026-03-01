#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""T+0 dual-view board builder."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from modules.shared.trading_calendar import TradingCalendar

BEIJING_TZ = timezone(timedelta(hours=8))


def _hm_to_minutes(v: str, default_h: int, default_m: int) -> int:
    try:
        h, m = [int(x) for x in str(v).split(":", 1)]
        return h * 60 + m
    except Exception:
        return default_h * 60 + default_m


def parse_beijing_dt(value: Optional[str], fallback_date: Optional[str] = None) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw and fallback_date:
        raw = str(fallback_date).strip()
    if not raw:
        return None
    try:
        if raw.endswith("+0800"):
            raw = raw[:-5] + "+08:00"
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=BEIJING_TZ)
        return dt.astimezone(BEIJING_TZ)
    except Exception:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(raw[:19], fmt)
                return dt.replace(tzinfo=BEIJING_TZ)
            except Exception:
                continue
    return None


def compute_session_trade_date(
    mention_time: Optional[str],
    mention_date: Optional[str],
    close_finalize_time: str = "15:05",
    open_time: str = "09:30",
) -> Tuple[str, str]:
    cal = TradingCalendar.shared()
    dt = parse_beijing_dt(mention_time, fallback_date=mention_date) or datetime.now(BEIJING_TZ)
    day = dt.strftime("%Y-%m-%d")
    if not cal.is_trading_day(day):
        return cal.next_trading_day(day), "non_trading_shift"

    cur_minutes = dt.hour * 60 + dt.minute
    open_minutes = _hm_to_minutes(open_time, 9, 30)
    close_minutes = _hm_to_minutes(close_finalize_time, 15, 5)
    if cur_minutes >= close_minutes:
        return cal.next_trading_day(day), "after_close"
    if cur_minutes < open_minutes:
        return day, "preopen"
    return day, "intraday"


def _pick_event_return(event: Dict[str, Any]) -> Optional[float]:
    v_rt = event.get("t0_return_rt")
    if isinstance(v_rt, (int, float)):
        return float(v_rt)
    v_close = event.get("t0_return_close")
    if isinstance(v_close, (int, float)):
        return float(v_close)
    return None


def _fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M")


def _build_view_rows(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for event in events:
        mention_time = str(event.get("mention_time") or "")
        mention_date = str(event.get("mention_date") or "")
        raw_dt = parse_beijing_dt(mention_time, fallback_date=mention_date)
        dt_str = _fmt_dt(raw_dt) if raw_dt else (f"{mention_date} --:--" if mention_date else "--")
        ret = _pick_event_return(event)
        rows.append(
            {
                "mention_id": event.get("mention_id"),
                "topic_id": event.get("topic_id"),
                "group_id": event.get("group_id"),
                "group_name": event.get("group_name") or (
                    f"群组{event.get('group_id')}" if event.get("group_id") is not None else "未知群组"
                ),
                "mention_time": dt_str,
                "ret": ret,
            }
        )
    return rows


def _choose_max_event(events: List[Dict[str, Any]]) -> Tuple[Optional[float], Optional[Dict[str, Any]]]:
    best_event: Optional[Dict[str, Any]] = None
    best_return: Optional[float] = None
    best_time: Optional[str] = None
    for e in events:
        ret = _pick_event_return(e)
        if ret is None:
            continue
        cur_time = str(e.get("mention_time") or e.get("mention_date") or "")
        if best_return is None or ret > best_return:
            best_return = ret
            best_event = e
            best_time = cur_time
        elif best_return == ret and (best_time is None or (cur_time and cur_time < best_time)):
            best_event = e
            best_time = cur_time
    return best_return, best_event


def build_t0_dual_board(
    events: List[Dict[str, Any]],
    close_finalize_time: str = "15:05",
    open_time: str = "09:30",
    now_dt: Optional[datetime] = None,
) -> Dict[str, Any]:
    cal = TradingCalendar.shared()
    now = now_dt or datetime.now(BEIJING_TZ)
    today = now.strftime("%Y-%m-%d")

    base_trade_date = cal.resolve_effective_trade_day(today)
    prev_trade_date = cal.prev_trading_day(base_trade_date)
    prev_prev_trade_date = cal.prev_trading_day(prev_trade_date)
    next_trade_date = cal.next_trading_day(base_trade_date)

    close_minutes = _hm_to_minutes(close_finalize_time, 15, 5)

    def mk_dt(day: str, minutes: int) -> datetime:
        h = minutes // 60
        m = minutes % 60
        return datetime.strptime(day, "%Y-%m-%d").replace(
            hour=h, minute=m, second=0, microsecond=0, tzinfo=BEIJING_TZ
        )

    # 三段连续且不重叠：
    # D-1: 前二交易日收盘 ~ 前一交易日收盘
    # D:   前一交易日收盘 ~ 当日收盘
    # D+1: 当日收盘 ~ 下一交易日收盘
    window_0_start = mk_dt(prev_prev_trade_date, close_minutes)
    window_0_end = mk_dt(prev_trade_date, close_minutes)
    window_1_start = mk_dt(prev_trade_date, close_minutes)
    window_1_end = mk_dt(base_trade_date, close_minutes)
    window_2_start = mk_dt(base_trade_date, close_minutes)
    window_2_end = mk_dt(next_trade_date, close_minutes)

    enriched_events: List[Dict[str, Any]] = []
    for e in events:
        evt = dict(e)
        if not evt.get("t0_session_trade_date"):
            session_date, window_tag = compute_session_trade_date(
                mention_time=str(evt.get("mention_time") or ""),
                mention_date=str(evt.get("mention_date") or ""),
                close_finalize_time=close_finalize_time,
                open_time=open_time,
            )
            evt["t0_session_trade_date"] = session_date
            evt["t0_window_tag"] = window_tag
        enriched_events.append(evt)

    def in_window(evt: Dict[str, Any], ws: datetime, we: datetime) -> bool:
        dt = parse_beijing_dt(str(evt.get("mention_time") or ""), fallback_date=str(evt.get("mention_date") or ""))
        if dt is None:
            return False
        return ws <= dt < we

    view_0_events = [e for e in enriched_events if in_window(e, window_0_start, window_0_end)]
    view_1_events = [e for e in enriched_events if in_window(e, window_1_start, window_1_end)]
    view_2_events = [e for e in enriched_events if in_window(e, window_2_start, window_2_end)]

    now_minutes = now.hour * 60 + now.minute
    d_status = "finalized" if now_minutes >= close_minutes else "realtime"

    def build_view(
        view_key: str,
        label: str,
        trade_date: str,
        ws: datetime,
        we: datetime,
        pool: List[Dict[str, Any]],
        status: str,
    ) -> Dict[str, Any]:
        pool_sorted = sorted(
            pool,
            key=lambda x: str(x.get("mention_time") or x.get("mention_date") or ""),
            reverse=True,
        )
        current = pool_sorted[0] if pool_sorted else None
        current_ret = _pick_event_return(current) if current else None
        max_ret, max_evt = _choose_max_event(pool_sorted)

        return {
            "view_key": view_key,
            "label": label,
            "trade_date": trade_date,
            "window_start": _fmt_dt(ws),
            "window_end": _fmt_dt(we),
            "status": status,
            "has_data": len(pool_sorted) > 0,
            "current_return": current_ret,
            "max_return": max_ret,
            "max_event": {
                "mention_id": max_evt.get("mention_id") if max_evt else None,
                "topic_id": max_evt.get("topic_id") if max_evt else None,
                "group_id": max_evt.get("group_id") if max_evt else None,
                "group_name": max_evt.get("group_name") if max_evt else None,
            } if max_evt else None,
            "rows": _build_view_rows(pool_sorted),
        }

    return {
        "as_of": now.strftime("%Y-%m-%d %H:%M:%S"),
        "close_finalize_time": close_finalize_time,
        "open_time": open_time,
        "base_trade_date": base_trade_date,
        "prev_trade_date": prev_trade_date,
        "prev_prev_trade_date": prev_prev_trade_date,
        "next_trade_date": next_trade_date,
        "views": [
            build_view(
                view_key="d_minus_1_view",
                label=f"{prev_trade_date} 视角",
                trade_date=prev_trade_date,
                ws=window_0_start,
                we=window_0_end,
                pool=view_0_events,
                status="finalized",
            ),
            build_view(
                view_key="d_view",
                label=f"{base_trade_date} 视角",
                trade_date=base_trade_date,
                ws=window_1_start,
                we=window_1_end,
                pool=view_1_events,
                status=d_status,
            ),
            build_view(
                view_key="d_plus_1_preview",
                label=f"{next_trade_date} 视角（提前看）",
                trade_date=next_trade_date,
                ws=window_2_start,
                we=window_2_end,
                pool=view_2_events,
                status="preview",
            ),
        ],
    }

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A-share trading calendar helper with local fallback."""

from __future__ import annotations

import threading
from datetime import date, datetime, timedelta
from typing import Dict, Optional

from modules.shared.logger_config import log_warning

try:
    import exchange_calendars as xcals  # type: ignore
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    xcals = None  # type: ignore
    pd = None  # type: ignore


def _to_date(d: str | date | datetime) -> date:
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    return datetime.strptime(str(d)[:10], "%Y-%m-%d").date()


class TradingCalendar:
    """Trading day resolver for Shanghai exchange sessions."""

    _instance: Optional["TradingCalendar"] = None
    _lock = threading.RLock()

    def __init__(self) -> None:
        self._session_cache: Dict[str, bool] = {}
        self._next_cache: Dict[str, str] = {}
        self._prev_cache: Dict[str, str] = {}
        self._calendar = None
        if xcals is not None:
            try:
                self._calendar = xcals.get_calendar("XSHG")
            except Exception as e:  # pragma: no cover
                log_warning(f"交易日历库初始化失败，回退工作日近似: {e}")
                self._calendar = None

    @classmethod
    def shared(cls) -> "TradingCalendar":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def is_trading_day(self, d: str | date | datetime) -> bool:
        day = _to_date(d)
        key = day.strftime("%Y-%m-%d")
        cached = self._session_cache.get(key)
        if cached is not None:
            return cached
        result = self._is_trading_day_impl(day)
        self._session_cache[key] = result
        return result

    def next_trading_day(self, d: str | date | datetime) -> str:
        day = _to_date(d)
        key = day.strftime("%Y-%m-%d")
        cached = self._next_cache.get(key)
        if cached:
            return cached
        nxt = self._next_trading_day_impl(day).strftime("%Y-%m-%d")
        self._next_cache[key] = nxt
        return nxt

    def prev_trading_day(self, d: str | date | datetime) -> str:
        day = _to_date(d)
        key = day.strftime("%Y-%m-%d")
        cached = self._prev_cache.get(key)
        if cached:
            return cached
        prev = self._prev_trading_day_impl(day).strftime("%Y-%m-%d")
        self._prev_cache[key] = prev
        return prev

    def resolve_effective_trade_day(self, d: str | date | datetime) -> str:
        day = _to_date(d)
        if self.is_trading_day(day):
            return day.strftime("%Y-%m-%d")
        return self.next_trading_day(day)

    def _is_trading_day_impl(self, day: date) -> bool:
        if self._calendar is not None and pd is not None:
            try:
                sessions = self._calendar.sessions_in_range(pd.Timestamp(day), pd.Timestamp(day))
                return len(sessions) > 0
            except Exception:
                pass
        return day.weekday() < 5

    def _next_trading_day_impl(self, day: date) -> date:
        if self._calendar is not None and pd is not None:
            try:
                start = pd.Timestamp(day + timedelta(days=1))
                end = pd.Timestamp(day + timedelta(days=20))
                sessions = self._calendar.sessions_in_range(start, end)
                if len(sessions) > 0:
                    return sessions[0].date()
            except Exception:
                pass
        cur = day + timedelta(days=1)
        while cur.weekday() >= 5:
            cur += timedelta(days=1)
        return cur

    def _prev_trading_day_impl(self, day: date) -> date:
        if self._calendar is not None and pd is not None:
            try:
                start = pd.Timestamp(day - timedelta(days=20))
                end = pd.Timestamp(day - timedelta(days=1))
                sessions = self._calendar.sessions_in_range(start, end)
                if len(sessions) > 0:
                    return sessions[-1].date()
            except Exception:
                pass
        cur = day - timedelta(days=1)
        while cur.weekday() >= 5:
            cur -= timedelta(days=1)
        return cur


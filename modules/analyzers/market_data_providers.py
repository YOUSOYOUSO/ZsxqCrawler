#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Market data providers for failover routing."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import Any, Dict, List, Optional, Protocol, Tuple

import akshare as ak


def normalize_code(raw_code: str) -> Tuple[str, str]:
    code = str(raw_code).strip().upper()
    if "." in code:
        market = code.split(".", 1)[1]
        return code, market
    if code.startswith("6"):
        return f"{code}.SH", "SH"
    if code.startswith(("0", "3")):
        return f"{code}.SZ", "SZ"
    if code.startswith(("4", "8", "9")):
        return f"{code}.BJ", "BJ"
    return code, "UNK"


def to_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def to_tx_symbol(stock_code: str) -> str:
    full_code, market = normalize_code(stock_code)
    pure = full_code.split(".")[0]
    if market in {"SH", "SZ"}:
        return f"{market.lower()}{pure}"
    return pure


@dataclass
class SymbolRow:
    stock_code: str
    stock_name: str
    market: str
    source: str


@dataclass
class DailyPriceRow:
    stock_code: str
    trade_date: str
    open: Optional[float]
    close: Optional[float]
    high: Optional[float]
    low: Optional[float]
    change_pct: Optional[float]
    volume: Optional[float]
    source: str


class MarketDataProvider(Protocol):
    name: str

    def fetch_symbols(self) -> List[SymbolRow]:
        ...

    def fetch_stock_history(self, stock_code: str, start_date: str, end_date: str, adjust: str) -> List[DailyPriceRow]:
        ...

    def fetch_index_history(self, start_date: str, end_date: str) -> List[DailyPriceRow]:
        ...


class AkshareProvider:
    """EastMoney based provider via AkShare."""

    name = "akshare"

    def fetch_symbols(self) -> List[SymbolRow]:
        df = ak.stock_zh_a_spot_em()
        if df is None or df.empty:
            return []
        rows: List[SymbolRow] = []
        for _, row in df.iterrows():
            code = str(row.get("代码", "")).strip()
            name = str(row.get("名称", "")).strip()
            if not code or not name:
                continue
            full_code, market = normalize_code(code)
            rows.append(SymbolRow(stock_code=full_code, stock_name=name, market=market, source="akshare"))
        return rows

    def fetch_stock_history(self, stock_code: str, start_date: str, end_date: str, adjust: str) -> List[DailyPriceRow]:
        pure_code = stock_code.split(".")[0]
        df = ak.stock_zh_a_hist(
            symbol=pure_code,
            period="daily",
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", ""),
            adjust=adjust,
        )
        if df is None or df.empty:
            return []
        rows: List[DailyPriceRow] = []
        for _, r in df.iterrows():
            trade_date = str(r.get("日期", ""))[:10]
            if not trade_date or trade_date < start_date or trade_date > end_date:
                continue
            rows.append(
                DailyPriceRow(
                    stock_code=stock_code,
                    trade_date=trade_date,
                    open=to_float(r.get("开盘")),
                    close=to_float(r.get("收盘")),
                    high=to_float(r.get("最高")),
                    low=to_float(r.get("最低")),
                    change_pct=to_float(r.get("涨跌幅")),
                    volume=to_float(r.get("成交量")),
                    source="akshare.stock_zh_a_hist",
                )
            )
        return rows

    def fetch_index_history(self, start_date: str, end_date: str) -> List[DailyPriceRow]:
        df = ak.stock_zh_index_daily(symbol="sh000300")
        if df is None or df.empty:
            return []
        rows: List[DailyPriceRow] = []
        for _, r in df.iterrows():
            trade_date = str(r.get("date", ""))[:10]
            if not trade_date or trade_date < start_date or trade_date > end_date:
                continue
            rows.append(
                DailyPriceRow(
                    stock_code="000300.SH",
                    trade_date=trade_date,
                    open=to_float(r.get("open")),
                    close=to_float(r.get("close")),
                    high=to_float(r.get("high")),
                    low=to_float(r.get("low")),
                    change_pct=0.0,
                    volume=to_float(r.get("volume")),
                    source="akshare.stock_zh_index_daily",
                )
            )
        return rows


class TencentProvider:
    """Tencent quote source via AkShare stock_zh_a_hist_tx."""

    name = "tx"

    def fetch_symbols(self) -> List[SymbolRow]:
        # The tx spot endpoint is not stable in AkShare; use general spot list as symbol dictionary.
        df = ak.stock_zh_a_spot()
        if df is None or df.empty:
            return []
        rows: List[SymbolRow] = []
        for _, row in df.iterrows():
            code = str(row.get("代码", "")).strip()
            name = str(row.get("名称", "")).strip()
            if not code or not name:
                continue
            full_code, market = normalize_code(code)
            rows.append(SymbolRow(stock_code=full_code, stock_name=name, market=market, source="tx.stock_zh_a_spot"))
        return rows

    def fetch_stock_history(self, stock_code: str, start_date: str, end_date: str, adjust: str) -> List[DailyPriceRow]:
        full_code, market = normalize_code(stock_code)
        if market not in {"SH", "SZ"}:
            return []
        tx_symbol = to_tx_symbol(stock_code)
        df = ak.stock_zh_a_hist_tx(
            symbol=tx_symbol,
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", ""),
        )
        if df is None or df.empty:
            return []
        rows: List[DailyPriceRow] = []
        for _, r in df.iterrows():
            raw_date = r.get("date")
            trade_date = str(raw_date)[:10]
            if not trade_date or trade_date < start_date or trade_date > end_date:
                continue
            open_p = to_float(r.get("open"))
            close_p = to_float(r.get("close"))
            change_pct = None
            if open_p not in (None, 0) and close_p is not None:
                change_pct = round((close_p - open_p) / open_p * 100, 4)
            rows.append(
                DailyPriceRow(
                    stock_code=full_code,
                    trade_date=trade_date,
                    open=open_p,
                    close=close_p,
                    high=to_float(r.get("high")),
                    low=to_float(r.get("low")),
                    change_pct=change_pct,
                    volume=to_float(r.get("amount")),
                    source="tx.stock_zh_a_hist_tx",
                )
            )
        rows.sort(key=lambda x: x.trade_date)
        return rows

    def fetch_index_history(self, start_date: str, end_date: str) -> List[DailyPriceRow]:
        # HS300 can be fetched with the same tx interface.
        df = ak.stock_zh_a_hist_tx(
            symbol="sh000300",
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", ""),
        )
        if df is None or df.empty:
            return []
        rows: List[DailyPriceRow] = []
        for _, r in df.iterrows():
            raw_date = r.get("date")
            trade_date = str(raw_date)[:10]
            if not trade_date or trade_date < start_date or trade_date > end_date:
                continue
            open_p = to_float(r.get("open"))
            close_p = to_float(r.get("close"))
            change_pct = None
            if open_p not in (None, 0) and close_p is not None:
                change_pct = round((close_p - open_p) / open_p * 100, 4)
            rows.append(
                DailyPriceRow(
                    stock_code="000300.SH",
                    trade_date=trade_date,
                    open=open_p,
                    close=close_p,
                    high=to_float(r.get("high")),
                    low=to_float(r.get("low")),
                    change_pct=change_pct,
                    volume=to_float(r.get("amount")),
                    source="tx.stock_zh_a_hist_tx",
                )
            )
        rows.sort(key=lambda x: x.trade_date)
        return rows


class SinaProvider:
    """Sina source via AkShare daily interfaces."""

    name = "sina"

    def fetch_symbols(self) -> List[SymbolRow]:
        # Sina has no reliable all-symbol endpoint in AkShare; reuse common spot list.
        df = ak.stock_zh_a_spot()
        if df is None or df.empty:
            return []
        rows: List[SymbolRow] = []
        for _, row in df.iterrows():
            code = str(row.get("代码", "")).strip()
            name = str(row.get("名称", "")).strip()
            if not code or not name:
                continue
            full_code, market = normalize_code(code)
            rows.append(SymbolRow(stock_code=full_code, stock_name=name, market=market, source="sina.stock_zh_a_spot"))
        return rows

    def fetch_stock_history(self, stock_code: str, start_date: str, end_date: str, adjust: str) -> List[DailyPriceRow]:
        _ = adjust  # Sina interface does not expose qfq/hfq switch.
        full_code, market = normalize_code(stock_code)
        if market not in {"SH", "SZ"}:
            return []
        sina_symbol = f"{market.lower()}{full_code.split('.')[0]}"
        df = ak.stock_zh_a_daily(
            symbol=sina_symbol,
            start_date=start_date,
            end_date=end_date,
        )
        if df is None or df.empty:
            return []
        rows: List[DailyPriceRow] = []
        for _, r in df.iterrows():
            trade_date = str(r.get("date", ""))[:10]
            if not trade_date or trade_date < start_date or trade_date > end_date:
                continue
            open_p = to_float(r.get("open"))
            close_p = to_float(r.get("close"))
            preclose = to_float(r.get("preclose"))
            if preclose in (None, 0):
                change_pct = None
            elif close_p is None:
                change_pct = None
            else:
                change_pct = round((close_p - preclose) / preclose * 100, 4)
            rows.append(
                DailyPriceRow(
                    stock_code=full_code,
                    trade_date=trade_date,
                    open=open_p,
                    close=close_p,
                    high=to_float(r.get("high")),
                    low=to_float(r.get("low")),
                    change_pct=change_pct,
                    volume=to_float(r.get("volume")),
                    source="sina.stock_zh_a_daily",
                )
            )
        rows.sort(key=lambda x: x.trade_date)
        return rows

    def fetch_index_history(self, start_date: str, end_date: str) -> List[DailyPriceRow]:
        df = ak.stock_zh_index_daily(symbol="sh000300")
        if df is None or df.empty:
            return []
        rows: List[DailyPriceRow] = []
        for _, r in df.iterrows():
            trade_date = str(r.get("date", ""))[:10]
            if not trade_date or trade_date < start_date or trade_date > end_date:
                continue
            rows.append(
                DailyPriceRow(
                    stock_code="000300.SH",
                    trade_date=trade_date,
                    open=to_float(r.get("open")),
                    close=to_float(r.get("close")),
                    high=to_float(r.get("high")),
                    low=to_float(r.get("low")),
                    change_pct=0.0,
                    volume=to_float(r.get("volume")),
                    source="sina.stock_zh_index_daily",
                )
            )
        return rows


class TushareProvider:
    name = "tushare"

    def __init__(self, token: str):
        if not token:
            raise RuntimeError("tushare token is empty")
        # 常见误配：把网页登录态/cookie 串填到 token 字段。
        if "uid=" in token or "username=" in token or ";" in token:
            raise RuntimeError("tushare token invalid: looks like cookie/session, not pro token")
        try:
            ts_mod = import_module("tushare")
        except Exception as e:
            raise RuntimeError(f"tushare unavailable: {e}") from e
        self.ts = ts_mod
        self.pro = ts_mod.pro_api(token)

    @staticmethod
    def _normalize_error(err: Exception) -> RuntimeError:
        text = str(err).strip()
        lower = text.lower()
        if (
            "api init error" in lower
            or "token不对" in text
            or "请设置tushare pro的token" in text
            or lower in {"error", "error."}
        ):
            return RuntimeError("tushare token invalid or not configured correctly")
        return RuntimeError(f"tushare request failed: {text}")

    def fetch_symbols(self) -> List[SymbolRow]:
        try:
            df = self.pro.stock_basic(exchange="", list_status="L", fields="ts_code,symbol,name,market")
        except Exception as e:
            raise self._normalize_error(e) from e
        if df is None or df.empty:
            return []
        rows: List[SymbolRow] = []
        for _, row in df.iterrows():
            ts_code = str(row.get("ts_code", "")).strip().upper()
            symbol = str(row.get("symbol", "")).strip()
            name = str(row.get("name", "")).strip()
            if not ts_code or not symbol or not name:
                continue
            full_code, market = normalize_code(ts_code)
            market_val = str(row.get("market", "")).strip().upper() or market
            rows.append(
                SymbolRow(
                    stock_code=full_code,
                    stock_name=name,
                    market=market_val,
                    source="tushare.stock_basic",
                )
            )
        return rows

    def fetch_stock_history(self, stock_code: str, start_date: str, end_date: str, adjust: str) -> List[DailyPriceRow]:
        full_code, _ = normalize_code(stock_code)
        ymd_start = start_date.replace("-", "")
        ymd_end = end_date.replace("-", "")
        try:
            df = self.pro.daily(ts_code=full_code, start_date=ymd_start, end_date=ymd_end)
        except Exception as e:
            raise self._normalize_error(e) from e
        if df is None or df.empty:
            return []
        rows: List[DailyPriceRow] = []
        for _, r in df.iterrows():
            raw_date = str(r.get("trade_date", "")).strip()
            if len(raw_date) != 8:
                continue
            trade_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
            if trade_date < start_date or trade_date > end_date:
                continue
            rows.append(
                DailyPriceRow(
                    stock_code=full_code,
                    trade_date=trade_date,
                    open=to_float(r.get("open")),
                    close=to_float(r.get("close")),
                    high=to_float(r.get("high")),
                    low=to_float(r.get("low")),
                    change_pct=to_float(r.get("pct_chg")),
                    volume=to_float(r.get("vol")),
                    source="tushare.daily",
                )
            )
        rows.sort(key=lambda x: x.trade_date)
        return rows

    def fetch_index_history(self, start_date: str, end_date: str) -> List[DailyPriceRow]:
        ymd_start = start_date.replace("-", "")
        ymd_end = end_date.replace("-", "")
        try:
            df = self.pro.index_daily(
                ts_code="000300.SH",
                start_date=ymd_start,
                end_date=ymd_end,
            )
        except Exception as e:
            raise self._normalize_error(e) from e
        if df is None or df.empty:
            return []
        rows: List[DailyPriceRow] = []
        for _, r in df.iterrows():
            raw_date = str(r.get("trade_date", "")).strip()
            if len(raw_date) != 8:
                continue
            trade_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
            if trade_date < start_date or trade_date > end_date:
                continue
            rows.append(
                DailyPriceRow(
                    stock_code="000300.SH",
                    trade_date=trade_date,
                    open=to_float(r.get("open")),
                    close=to_float(r.get("close")),
                    high=to_float(r.get("high")),
                    low=to_float(r.get("low")),
                    change_pct=to_float(r.get("pct_chg")) or 0.0,
                    volume=to_float(r.get("vol")),
                    source="tushare.index_daily",
                )
            )
        rows.sort(key=lambda x: x.trade_date)
        return rows

    def fetch_daily_by_date(self, trade_date: str) -> List[DailyPriceRow]:
        """按交易日获取全市场 A 股日线（1 次请求 → ~5000 条）。

        仅适用于历史交易日；当天数据需收盘后（~16:00）才可用。
        trade_date 格式: "YYYY-MM-DD" 或 "YYYYMMDD"
        """
        ymd = trade_date.replace("-", "")
        try:
            df = self.pro.daily(trade_date=ymd)
        except Exception as e:
            raise self._normalize_error(e) from e
        if df is None or df.empty:
            return []
        td_formatted = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"
        rows: List[DailyPriceRow] = []
        for _, r in df.iterrows():
            ts_code = str(r.get("ts_code", "")).strip().upper()
            if not ts_code:
                continue
            rows.append(
                DailyPriceRow(
                    stock_code=ts_code,
                    trade_date=td_formatted,
                    open=to_float(r.get("open")),
                    close=to_float(r.get("close")),
                    high=to_float(r.get("high")),
                    low=to_float(r.get("low")),
                    change_pct=to_float(r.get("pct_chg")),
                    volume=to_float(r.get("vol")),
                    source="tushare.daily_by_date",
                )
            )
        return rows


PROVIDER_CATALOG: Dict[str, Dict[str, Any]] = {
    "tx": {
        "name": "tx",
        "label": "腾讯行情",
        "description": "基于腾讯源，适合当前 EastMoney 历史接口不稳定时优先使用",
        "requires_token": False,
    },
    "sina": {
        "name": "sina",
        "label": "新浪行情",
        "description": "基于新浪日线接口，作为公开源备用",
        "requires_token": False,
    },
    "akshare": {
        "name": "akshare",
        "label": "AkShare(东财)",
        "description": "AkShare 默认东财接口，覆盖广但可能受风控",
        "requires_token": False,
    },
    "tushare": {
        "name": "tushare",
        "label": "Tushare",
        "description": "稳定性较高，需配置 token",
        "requires_token": True,
    },
}


def list_provider_catalog() -> List[Dict[str, Any]]:
    return [PROVIDER_CATALOG[k] for k in ["tx", "sina", "akshare", "tushare"]]


def build_provider(name: str, config: Dict[str, Any]) -> MarketDataProvider:
    key = str(name).strip().lower()
    if key == "akshare":
        return AkshareProvider()
    if key == "tx":
        return TencentProvider()
    if key == "sina":
        return SinaProvider()
    if key == "tushare":
        token = str(config.get("tushare_token", "")).strip()
        return TushareProvider(token=token)
    raise RuntimeError(f"unknown provider: {name}")

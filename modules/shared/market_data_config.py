#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Market data config loader for persistent AkShare store."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict

from modules.shared.paths import PROJECT_ROOT, get_config_path

try:
    import tomllib  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    try:
        import tomli as tomllib  # type: ignore
    except Exception:  # pragma: no cover
        tomllib = None  # type: ignore


BEIJING_TZ = timezone(timedelta(hours=8))

DEFAULT_MARKET_DATA_CONFIG: Dict[str, Any] = {
    "enabled": True,
    "db_path": "output/databases/akshare_market.db",
    "adjust": "qfq",
    "close_finalize_time": "15:05",
    "bootstrap_mode": "full_history",
    "bootstrap_batch_size": 200,
    "sync_retry_max": 3,
    "sync_retry_backoff_seconds": 1.0,
    "incremental_history_days": 20,
}


def _to_abs_path(path_value: str) -> str:
    p = Path(path_value)
    if p.is_absolute():
        return str(p)
    return str((PROJECT_ROOT / p).resolve())


def load_market_data_config() -> Dict[str, Any]:
    cfg = dict(DEFAULT_MARKET_DATA_CONFIG)
    config_path = get_config_path("app.toml")
    if tomllib is not None and config_path.exists():
        try:
            with config_path.open("rb") as f:
                payload = tomllib.load(f)
            section = payload.get("market_data", {})
            if isinstance(section, dict):
                cfg.update(section)
        except Exception:
            # fail-open: keep defaults
            pass

    cfg["enabled"] = bool(cfg.get("enabled", True))
    cfg["db_path"] = _to_abs_path(str(cfg.get("db_path", DEFAULT_MARKET_DATA_CONFIG["db_path"])))
    cfg["adjust"] = str(cfg.get("adjust", "qfq")).lower() or "qfq"
    cfg["close_finalize_time"] = str(cfg.get("close_finalize_time", "15:05"))
    cfg["bootstrap_mode"] = str(cfg.get("bootstrap_mode", "full_history"))
    cfg["bootstrap_batch_size"] = int(cfg.get("bootstrap_batch_size", 200))
    cfg["sync_retry_max"] = int(cfg.get("sync_retry_max", 3))
    cfg["sync_retry_backoff_seconds"] = float(cfg.get("sync_retry_backoff_seconds", 1.0))
    cfg["incremental_history_days"] = int(cfg.get("incremental_history_days", 20))

    # Optional env overrides
    if os.environ.get("MARKET_DATA_DB_PATH"):
        cfg["db_path"] = _to_abs_path(os.environ["MARKET_DATA_DB_PATH"])
    if os.environ.get("MARKET_DATA_CLOSE_FINALIZE_TIME"):
        cfg["close_finalize_time"] = os.environ["MARKET_DATA_CLOSE_FINALIZE_TIME"]
    if os.environ.get("MARKET_DATA_ENABLED"):
        cfg["enabled"] = os.environ["MARKET_DATA_ENABLED"].strip().lower() in {"1", "true", "yes", "on"}

    return cfg


def now_beijing() -> datetime:
    return datetime.now(BEIJING_TZ)


def is_market_closed_now(close_finalize_time: str = "15:05") -> bool:
    now = now_beijing()
    try:
        hour, minute = [int(x) for x in close_finalize_time.split(":", 1)]
    except Exception:
        hour, minute = 15, 5
    boundary = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return now >= boundary

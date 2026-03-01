from __future__ import annotations

import re
import time
from datetime import timedelta
from typing import Any, Dict, List

from modules.analyzers.market_data_providers import build_provider, list_provider_catalog
from modules.analyzers.market_data_sync import MarketDataSyncService
from modules.shared.market_data_config import DEFAULT_MARKET_DATA_CONFIG, load_market_data_config, now_beijing
from modules.shared.paths import get_config_path


class MarketDataSourceService:
    _MARKET_KEYS = [
        "enabled",
        "db_path",
        "adjust",
        "providers",
        "realtime_providers",
        "realtime_provider_failover_enabled",
        "provider_failover_enabled",
        "provider_circuit_breaker_seconds",
        "tushare_token",
        "close_finalize_time",
        "bootstrap_mode",
        "bootstrap_batch_size",
        "sync_retry_max",
        "sync_retry_backoff_seconds",
        "sync_failure_cooldown_seconds",
        "incremental_history_days",
    ]

    def _render_value(self, value: Any) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, list):
            escaped = [str(v).replace('"', '\\"') for v in value if str(v).strip()]
            return "[" + ", ".join(f'\"{v}\"' for v in escaped) + "]"
        if value is None:
            return '""'
        escaped = str(value).replace('"', '\\"')
        return f'"{escaped}"'

    def _render_market_section(self, cfg: Dict[str, Any]) -> str:
        lines = ["[market_data]"]
        for key in self._MARKET_KEYS:
            if key not in cfg:
                continue
            lines.append(f"{key} = {self._render_value(cfg.get(key))}")
        return "\n".join(lines) + "\n"

    def _update_market_section_text(self, original: str, section_text: str) -> str:
        pattern = r"(?ms)^\[market_data\]\n.*?(?=^\[|\Z)"
        if re.search(pattern, original):
            replaced = re.sub(pattern, section_text, original)
            return replaced if replaced.endswith("\n") else replaced + "\n"

        base = original.rstrip() + "\n\n" if original.strip() else ""
        return base + section_text

    def get_settings(self) -> Dict[str, Any]:
        cfg = load_market_data_config()
        providers = [str(p).strip().lower() for p in cfg.get("providers", []) if str(p).strip()]
        realtime_providers = [str(p).strip().lower() for p in cfg.get("realtime_providers", []) if str(p).strip()]
        return {
            "providers": providers,
            "realtime_providers": realtime_providers,
            "realtime_provider_failover_enabled": bool(cfg.get("realtime_provider_failover_enabled", True)),
            "provider_failover_enabled": bool(cfg.get("provider_failover_enabled", True)),
            "provider_circuit_breaker_seconds": float(cfg.get("provider_circuit_breaker_seconds", 300.0)),
            "sync_retry_max": int(cfg.get("sync_retry_max", 3)),
            "sync_retry_backoff_seconds": float(cfg.get("sync_retry_backoff_seconds", 1.0)),
            "sync_failure_cooldown_seconds": float(cfg.get("sync_failure_cooldown_seconds", 120.0)),
            "tushare_token": str(cfg.get("tushare_token", "")).strip(),
            "tushare_token_configured": bool(str(cfg.get("tushare_token", "")).strip()),
            "catalog": list_provider_catalog(),
            "updated_at": now_beijing().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def update_settings(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        providers = [str(p).strip().lower() for p in payload.get("providers", []) if str(p).strip()]
        if not providers:
            providers = list(DEFAULT_MARKET_DATA_CONFIG.get("providers", ["tx", "sina", "akshare", "tushare"]))
        realtime_providers = [str(p).strip().lower() for p in payload.get("realtime_providers", []) if str(p).strip()]
        if not realtime_providers:
            realtime_providers = list(DEFAULT_MARKET_DATA_CONFIG.get("realtime_providers", ["akshare", "tx", "sina", "tushare"]))

        normalized: Dict[str, Any] = {
            "providers": providers,
            "realtime_providers": realtime_providers,
            "realtime_provider_failover_enabled": bool(payload.get("realtime_provider_failover_enabled", True)),
            "provider_failover_enabled": bool(payload.get("provider_failover_enabled", True)),
            "provider_circuit_breaker_seconds": float(payload.get("provider_circuit_breaker_seconds", 300.0)),
            "sync_retry_max": int(payload.get("sync_retry_max", 3)),
            "sync_retry_backoff_seconds": float(payload.get("sync_retry_backoff_seconds", 1.0)),
            "sync_failure_cooldown_seconds": float(payload.get("sync_failure_cooldown_seconds", 120.0)),
        }

        cfg = load_market_data_config()
        # Retain current value unless explicitly passed.
        if "tushare_token" in payload:
            normalized["tushare_token"] = str(payload.get("tushare_token") or "").strip()
        else:
            normalized["tushare_token"] = str(cfg.get("tushare_token", "")).strip()

        merged = dict(DEFAULT_MARKET_DATA_CONFIG)
        merged.update(cfg)
        merged.update(normalized)

        config_path = get_config_path("app.toml")
        original = ""
        if config_path.exists():
            original = config_path.read_text(encoding="utf-8")

        section_text = self._render_market_section(merged)
        new_text = self._update_market_section_text(original, section_text)
        config_path.write_text(new_text, encoding="utf-8")

        return {"success": True, "message": "行情源配置已保存", "settings": self.get_settings()}

    def probe(self, providers: List[str] | None = None, symbol: str = "000001.SZ") -> Dict[str, Any]:
        cfg = load_market_data_config()
        provider_names = [str(p).strip().lower() for p in (providers or cfg.get("providers", [])) if str(p).strip()]
        if not provider_names:
            provider_names = list(DEFAULT_MARKET_DATA_CONFIG.get("providers", ["tx", "sina", "akshare", "tushare"]))

        end_date = now_beijing().strftime("%Y-%m-%d")
        start_date = (now_beijing() - timedelta(days=5)).strftime("%Y-%m-%d")
        health = MarketDataSyncService().get_provider_health_snapshot(stock_code=symbol, op_name="probe")
        health_map = {x["provider"]: x for x in health.get("providers", [])}

        details: List[Dict[str, Any]] = []
        for name in provider_names:
            t0 = time.perf_counter()
            h = health_map.get(name, {})
            item = {
                "provider": name,
                "ok": False,
                "latency_ms": 0,
                "symbol_rows": 0,
                "stock_rows": 0,
                "index_rows": 0,
                "error": "",
                "routable": bool(h.get("routable", False)),
                "disabled_reason": str(h.get("disabled_reason", "")),
                "cooldown_until": float(h.get("cooldown_until", 0.0) or 0.0),
            }
            try:
                if item["routable"]:
                    provider = build_provider(name, cfg)
                    stock_rows = provider.fetch_stock_history(symbol, start_date, end_date, str(cfg.get("adjust", "qfq"))) or []
                    index_rows = provider.fetch_index_history(start_date, end_date) or []
                    item["ok"] = True
                    item["symbol_rows"] = -1
                    item["stock_rows"] = len(stock_rows)
                    item["index_rows"] = len(index_rows)
                elif not item["disabled_reason"]:
                    item["disabled_reason"] = "not_routable"
            except Exception as e:
                item["error"] = str(e) or type(e).__name__
            finally:
                item["latency_ms"] = round((time.perf_counter() - t0) * 1000, 2)
            details.append(item)

        return {
            "success": any(d["ok"] for d in details),
            "symbol": symbol,
            "window": f"{start_date}..{end_date}",
            "details": details,
            "tested_at": now_beijing().strftime("%Y-%m-%d %H:%M:%S"),
        }

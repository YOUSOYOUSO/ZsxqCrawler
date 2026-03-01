from __future__ import annotations

from typing import Any, Dict, Optional

from modules.accounts.accounts_sql_manager import get_accounts_sql_manager
from modules.shared.paths import get_config_path
from modules.zsxq.zsxq_interactive_crawler import load_config


class ConfigService:
    def _get_primary_cookie(self) -> Optional[str]:
        try:
            sql_mgr = get_accounts_sql_manager()
            first_acc = sql_mgr.get_first_account(mask_cookie=False)
            if first_acc:
                cookie = (first_acc.get("cookie") or "").strip()
                if cookie:
                    return cookie
        except Exception:
            pass

        try:
            config = load_config()
            if not config:
                return None
            auth_config = config.get("auth", {}) or {}
            cookie = (auth_config.get("cookie") or "").strip()
            if cookie and cookie != "your_cookie_here":
                return cookie
        except Exception:
            return None

        return None

    def is_configured(self) -> bool:
        return self._get_primary_cookie() is not None

    def get_config(self) -> Dict[str, Any]:
        config = load_config()
        auth_config = (config or {}).get("auth", {}) if config else {}
        cookie = auth_config.get("cookie", "") if auth_config else ""

        return {
            "configured": self.is_configured(),
            "auth": {
                "cookie": "***" if cookie and cookie != "your_cookie_here" else "未配置",
            },
            "database": config.get("database", {}) if config else {},
            "download": config.get("download", {}) if config else {},
        }

    def update_config(self, cookie: str) -> Dict[str, Any]:
        config_content = f"""# 知识星球数据采集器配置文件
# 通过Web界面自动生成

[auth]
# 知识星球登录Cookie
cookie = \"{cookie}\"

[download]
# 下载目录
dir = \"downloads\"

[market_data]
enabled = true
db_path = \"output/databases/akshare_market.db\"
adjust = \"qfq\"
providers = [\"tx\", \"sina\", \"akshare\", \"tushare\"]
realtime_providers = [\"tushare\", \"tx\", \"sina\", \"akshare\"]
realtime_provider_failover_enabled = false
provider_failover_enabled = true
provider_circuit_breaker_seconds = 300.0
tushare_token = \"\"
close_finalize_time = \"15:05\"
bootstrap_mode = \"full_history\"
bootstrap_batch_size = 200
sync_retry_max = 3
sync_retry_backoff_seconds = 1.0
sync_failure_cooldown_seconds = 120.0
"""

        config_path = str(get_config_path("app.toml"))
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(config_content)

        return {"message": "配置更新成功", "success": True}

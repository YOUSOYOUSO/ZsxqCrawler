from __future__ import annotations

from typing import Any, Dict

from fastapi import HTTPException

from modules.shared.db_path_manager import get_db_path_manager
from modules.zsxq.zsxq_interactive_crawler import ZSXQInteractiveCrawler, load_config


class RuntimeSettingsService:
    CRAWLER_DEFAULTS = {
        "min_delay": 2.0,
        "max_delay": 5.0,
        "long_delay_interval": 15,
        "timestamp_offset_ms": 1,
        "debug_mode": False,
    }

    DOWNLOADER_DEFAULTS = {
        "download_interval_min": 30,
        "download_interval_max": 60,
        "long_delay_interval": 10,
        "long_delay_min": 300,
        "long_delay_max": 600,
    }

    def _build_default_crawler(self) -> ZSXQInteractiveCrawler | None:
        cfg = load_config() or {}
        auth = cfg.get("auth", {}) or {}
        cookie = str(auth.get("cookie", "")).strip()
        group_id = str(auth.get("group_id", "")).strip()

        if not cookie or not group_id or cookie == "your_cookie_here" or group_id == "your_group_id_here":
            return None

        db_path = get_db_path_manager().get_topics_db_path(group_id)
        return ZSXQInteractiveCrawler(cookie, group_id, db_path)

    def get_crawler_settings(self) -> Dict[str, Any]:
        crawler = self._build_default_crawler()
        if not crawler:
            return dict(self.CRAWLER_DEFAULTS)

        return {
            "min_delay": crawler.min_delay,
            "max_delay": crawler.max_delay,
            "long_delay_interval": crawler.long_delay_interval,
            "timestamp_offset_ms": crawler.timestamp_offset_ms,
            "debug_mode": crawler.debug_mode,
        }

    def update_crawler_settings(self, request) -> Dict[str, Any]:
        crawler = self._build_default_crawler()
        if not crawler:
            raise HTTPException(status_code=404, detail="爬虫未初始化")

        if request.min_delay >= request.max_delay:
            raise HTTPException(status_code=400, detail="最小延迟必须小于最大延迟")

        crawler.min_delay = request.min_delay
        crawler.max_delay = request.max_delay
        crawler.long_delay_interval = request.long_delay_interval
        crawler.timestamp_offset_ms = request.timestamp_offset_ms
        crawler.debug_mode = request.debug_mode

        return {
            "message": "爬虫设置已更新",
            "settings": {
                "min_delay": crawler.min_delay,
                "max_delay": crawler.max_delay,
                "long_delay_interval": crawler.long_delay_interval,
                "timestamp_offset_ms": crawler.timestamp_offset_ms,
                "debug_mode": crawler.debug_mode,
            },
        }

    def get_downloader_settings(self) -> Dict[str, Any]:
        crawler = self._build_default_crawler()
        if not crawler:
            return dict(self.DOWNLOADER_DEFAULTS)

        downloader = crawler.get_file_downloader()
        return {
            "download_interval_min": downloader.download_interval_min,
            "download_interval_max": downloader.download_interval_max,
            "long_delay_interval": downloader.long_delay_interval,
            "long_delay_min": downloader.long_delay_min,
            "long_delay_max": downloader.long_delay_max,
        }

    def update_downloader_settings(self, request) -> Dict[str, Any]:
        crawler = self._build_default_crawler()
        if not crawler:
            raise HTTPException(status_code=404, detail="爬虫未初始化")

        if request.download_interval_min >= request.download_interval_max:
            raise HTTPException(status_code=400, detail="最小下载间隔必须小于最大下载间隔")
        if request.long_delay_min >= request.long_delay_max:
            raise HTTPException(status_code=400, detail="最小长休眠时间必须小于最大长休眠时间")

        downloader = crawler.get_file_downloader()
        downloader.download_interval_min = request.download_interval_min
        downloader.download_interval_max = request.download_interval_max
        downloader.long_delay_interval = request.long_delay_interval
        downloader.long_delay_min = request.long_delay_min
        downloader.long_delay_max = request.long_delay_max

        return {
            "message": "下载器设置已更新",
            "settings": {
                "download_interval_min": downloader.download_interval_min,
                "download_interval_max": downloader.download_interval_max,
                "long_delay_interval": downloader.long_delay_interval,
                "long_delay_min": downloader.long_delay_min,
                "long_delay_max": downloader.long_delay_max,
            },
        }

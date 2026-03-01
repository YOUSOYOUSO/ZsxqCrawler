from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from fastapi import BackgroundTasks, HTTPException

from api.services.global_task_service import GlobalTaskService
from api.services.group_service import GroupService
from modules.analyzers.global_analyzer import get_global_analyzer
from modules.shared.logger_config import log_error


class GlobalDataCorrectionService:
    """全局数据订正模块：统一处理重算、词云刷新与群组元数据刷新。"""

    def __init__(
        self,
        task_service: GlobalTaskService | None = None,
        group_service: GroupService | None = None,
    ):
        self.task_service = task_service or GlobalTaskService()
        self.group_service = group_service or GroupService()

    def start_performance_correction(
        self,
        background_tasks: BackgroundTasks,
        force: bool = False,
        calc_window_days: Optional[int] = None,
    ):
        return self.task_service.start_global_analyze_performance(
            background_tasks=background_tasks,
            force=force,
            calc_window_days=calc_window_days,
        )

    def start_range_recalculation(
        self,
        background_tasks: BackgroundTasks,
        start_date: str,
        end_date: str,
        force: bool = False,
    ):
        self._validate_date_range(start_date=start_date, end_date=end_date)
        return self.task_service.start_scan_global(
            background_tasks=background_tasks,
            force=force,
            exclude_non_stock=False,
            start_date=start_date,
            end_date=end_date,
        )

    def refresh_group_metadata(self):
        return self.group_service.refresh_local_groups()

    def get_hot_words(
        self,
        days: int = 1,
        limit: int = 50,
        force: bool = False,
        window_hours: Optional[int] = None,
        normalize: bool = True,
        fallback: bool = True,
        fallback_windows: str = "24,36,48,168",
    ):
        requested_window = int(window_hours or (int(days or 1) * 24))
        allowed_windows = {24, 36, 48, 168}
        if requested_window not in allowed_windows:
            raise HTTPException(status_code=400, detail=f"window_hours 仅支持 {sorted(allowed_windows)}")

        parsed_fallback_windows = self._parse_fallback_windows(fallback_windows, allowed_windows)

        try:
            analyzer = get_global_analyzer()
            return analyzer.get_global_hot_words(
                days=days,
                limit=limit,
                force_refresh=force,
                window_hours=requested_window,
                normalize=normalize,
                fallback=fallback,
                fallback_windows=parsed_fallback_windows,
            )
        except Exception as e:
            log_error(f"Failed to get global hot words: {e}")
            return {
                "words": [],
                "window_hours_requested": requested_window,
                "window_hours_effective": requested_window,
                "fallback_applied": False,
                "fallback_reason": f"服务异常: {str(e)}",
                "data_points_total": 0,
                "time_range": {},
            }

    @staticmethod
    def _parse_fallback_windows(raw_value: str, allowed_windows: set[int]) -> List[int]:
        parsed: List[int] = []
        for token in str(raw_value or "").split(","):
            value = token.strip()
            if not value:
                continue
            try:
                window = int(value)
            except Exception:
                continue
            if window in allowed_windows and window not in parsed:
                parsed.append(window)

        if not parsed:
            parsed = [24, 36, 48, 168]
        return parsed

    @staticmethod
    def _validate_date_range(start_date: str, end_date: str) -> None:
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="日期格式错误，请使用 YYYY-MM-DD")

        if start_dt > end_dt:
            raise HTTPException(status_code=400, detail="start_date 不能晚于 end_date")

from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException

from api.services.global_data_correction_service import GlobalDataCorrectionService

router = APIRouter(tags=["global-correction"])
service = GlobalDataCorrectionService()


@router.post("/api/global/correction/performance")
def start_global_correction_performance(
    background_tasks: BackgroundTasks,
    force: bool = False,
    calc_window_days: int | None = None,
):
    return service.start_performance_correction(
        background_tasks=background_tasks,
        force=force,
        calc_window_days=calc_window_days,
    )


@router.post("/api/global/correction/range-recalc")
def start_global_correction_range_recalc(
    background_tasks: BackgroundTasks,
    start_date: str,
    end_date: str,
    force: bool = False,
):
    return service.start_range_recalculation(
        background_tasks=background_tasks,
        start_date=start_date,
        end_date=end_date,
        force=force,
    )


@router.post("/api/global/correction/group-metadata/refresh")
async def refresh_global_group_metadata():
    try:
        return service.refresh_group_metadata()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"刷新本地群失败: {str(e)}")


@router.get("/api/global/correction/hot-words")
async def get_global_correction_hot_words(
    days: int = 1,
    limit: int = 50,
    force: bool = False,
    window_hours: Optional[int] = None,
    normalize: bool = True,
    fallback: bool = True,
    fallback_windows: str = "24,36,48,168",
):
    try:
        return service.get_hot_words(
            days=days,
            limit=limit,
            force=force,
            window_hours=window_hours,
            normalize=normalize,
            fallback=fallback,
            fallback_windows=fallback_windows,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取全局热词失败: {str(e)}")

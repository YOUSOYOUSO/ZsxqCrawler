from fastapi import APIRouter, BackgroundTasks, HTTPException

from api.schemas.models import (
    GlobalCrawlRequest,
    GlobalFileCollectRequest,
    GlobalFileDownloadRequest,
    ScanFilterConfigRequest,
)
from api.services.global_scan_filter_service import GlobalScanFilterService
from api.services.global_task_service import GlobalTaskService

router = APIRouter(tags=["global-tasks"])
service = GlobalTaskService()
scan_filter_service = GlobalScanFilterService()


@router.post("/api/global/crawl")
def api_global_crawl(request: GlobalCrawlRequest, background_tasks: BackgroundTasks):
    return service.start_global_crawl(request=request, background_tasks=background_tasks)


@router.post("/api/global/files/collect")
def api_global_files_collect(request: GlobalFileCollectRequest, background_tasks: BackgroundTasks):
    return service.start_global_files_collect(request=request, background_tasks=background_tasks)


@router.post("/api/global/files/download")
def api_global_files_download(request: GlobalFileDownloadRequest, background_tasks: BackgroundTasks):
    return service.start_global_files_download(request=request, background_tasks=background_tasks)


@router.post("/api/global/analyze/performance")
def api_global_analyze_performance(background_tasks: BackgroundTasks, force: bool = False):
    return service.start_global_analyze_performance(background_tasks=background_tasks, force=force)


@router.post("/api/stocks/exclude/cleanup")
async def cleanup_excluded_stocks(scope: str = "all", group_id: str | None = None):
    return service.cleanup_excluded_stocks(scope=scope, group_id=group_id)


@router.post("/api/global/scan-filter/cleanup-blacklist")
async def cleanup_blacklist_data(background_tasks: BackgroundTasks):
    return service.start_cleanup_blacklist(background_tasks=background_tasks)


@router.post("/api/global/scan")
def scan_global(background_tasks: BackgroundTasks, force: bool = False, exclude_non_stock: bool = False):
    return service.start_scan_global(
        background_tasks=background_tasks,
        force=force,
        exclude_non_stock=exclude_non_stock,
    )


@router.get("/api/global/scan-filter/config")
async def get_global_scan_filter_config():
    try:
        return scan_filter_service.get_config()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取扫描过滤配置失败: {str(e)}")


@router.put("/api/global/scan-filter/config")
async def update_global_scan_filter_config(request: ScanFilterConfigRequest):
    try:
        return scan_filter_service.update_config(
            default_action=request.default_action,
            whitelist_group_ids=request.whitelist_group_ids,
            blacklist_group_ids=request.blacklist_group_ids,
        )
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新扫描过滤配置失败: {str(e)}")


@router.get("/api/global/scan-filter/preview")
async def preview_global_scan_filter(exclude_non_stock: bool = True):
    try:
        return scan_filter_service.preview(exclude_non_stock=exclude_non_stock)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"预览扫描过滤结果失败: {str(e)}")


@router.get("/api/global/scan-filter/cleanup-blacklist/preview")
async def preview_blacklist_cleanup():
    try:
        return scan_filter_service.preview_blacklist_cleanup()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"预览黑名单清理失败: {str(e)}")

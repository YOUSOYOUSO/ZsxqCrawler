from fastapi import APIRouter, BackgroundTasks

from api.schemas.models import GlobalCrawlRequest, GlobalFileCollectRequest, GlobalFileDownloadRequest
from api.services.global_task_service import GlobalTaskService

router = APIRouter(tags=["global-tasks"])
service = GlobalTaskService()


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

from fastapi import APIRouter, BackgroundTasks, HTTPException

from api.schemas.models import CrawlHistoricalRequest, CrawlSettingsRequest, CrawlTimeRangeRequest
from api.services.crawl_service import CrawlService

router = APIRouter(tags=["crawl"])
service = CrawlService()


@router.post("/api/crawl/historical/{group_id}")
async def crawl_historical(group_id: str, request: CrawlHistoricalRequest, background_tasks: BackgroundTasks):
    try:
        return service.start_crawl_historical(group_id=group_id, request=request, background_tasks=background_tasks)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建爬取任务失败: {str(e)}")


@router.post("/api/crawl/all/{group_id}")
async def crawl_all(group_id: str, request: CrawlSettingsRequest, background_tasks: BackgroundTasks):
    try:
        return service.start_crawl_all(group_id=group_id, request=request, background_tasks=background_tasks)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建全量爬取任务失败: {str(e)}")


@router.post("/api/crawl/incremental/{group_id}")
async def crawl_incremental(group_id: str, request: CrawlHistoricalRequest, background_tasks: BackgroundTasks):
    try:
        return service.start_crawl_incremental(group_id=group_id, request=request, background_tasks=background_tasks)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建增量爬取任务失败: {str(e)}")


@router.post("/api/crawl/latest-until-complete/{group_id}")
async def crawl_latest_until_complete(group_id: str, request: CrawlSettingsRequest, background_tasks: BackgroundTasks):
    try:
        return service.start_crawl_latest_until_complete(group_id=group_id, request=request, background_tasks=background_tasks)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建获取最新记录任务失败: {str(e)}")


@router.post("/api/crawl/range/{group_id}")
async def crawl_by_time_range(group_id: str, request: CrawlTimeRangeRequest, background_tasks: BackgroundTasks):
    try:
        return service.start_crawl_time_range(group_id=group_id, request=request, background_tasks=background_tasks)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建时间区间爬取任务失败: {str(e)}")

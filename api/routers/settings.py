from fastapi import APIRouter, HTTPException

from api.schemas.models import CrawlBehaviorSettingsRequest
from api.services.crawl_service import CrawlService

router = APIRouter(tags=["settings"])
crawl_service = CrawlService()


@router.get("/api/settings/crawl")
async def get_crawl_settings():
    try:
        return crawl_service.get_crawl_settings()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取爬取设置失败: {str(e)}")


@router.post("/api/settings/crawl")
async def update_crawl_settings(settings: CrawlBehaviorSettingsRequest):
    try:
        persisted = crawl_service.update_crawl_settings(settings)
        return {"success": True, "message": "爬取设置已更新", "settings": persisted}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新爬取设置失败: {str(e)}")

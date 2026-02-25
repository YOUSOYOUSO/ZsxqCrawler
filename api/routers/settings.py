from fastapi import APIRouter, HTTPException

from api.schemas.models import CrawlBehaviorSettingsRequest, CrawlerSettingsRequest, DownloaderSettingsRequest
from api.services.crawl_service import CrawlService
from api.services.runtime_settings_service import RuntimeSettingsService

router = APIRouter(tags=["settings"])
crawl_service = CrawlService()
runtime_settings_service = RuntimeSettingsService()


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


@router.get("/api/settings/crawler")
async def get_crawler_settings():
    try:
        return runtime_settings_service.get_crawler_settings()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取爬虫设置失败: {str(e)}")


@router.post("/api/settings/crawler")
async def update_crawler_settings(request: CrawlerSettingsRequest):
    try:
        return runtime_settings_service.update_crawler_settings(request)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新爬虫设置失败: {str(e)}")


@router.get("/api/settings/downloader")
async def get_downloader_settings():
    try:
        return runtime_settings_service.get_downloader_settings()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取下载器设置失败: {str(e)}")


@router.post("/api/settings/downloader")
async def update_downloader_settings(request: DownloaderSettingsRequest):
    try:
        return runtime_settings_service.update_downloader_settings(request)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新下载器设置失败: {str(e)}")

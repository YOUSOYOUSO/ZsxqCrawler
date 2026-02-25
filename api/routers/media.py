from fastapi import APIRouter, HTTPException

from api.services.media_service import MediaService

router = APIRouter(tags=["media"])
service = MediaService()


@router.get("/api/proxy-image")
async def proxy_image_with_cache(url: str, group_id: str | None = None):
    try:
        return service.proxy_image_with_cache(url, group_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"代理图片失败: {str(e)}")


@router.get("/api/cache/images/info/{group_id}")
async def get_image_cache_info(group_id: str):
    try:
        return service.get_image_cache_info(group_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取缓存信息失败: {str(e)}")


@router.delete("/api/cache/images/{group_id}")
async def clear_image_cache(group_id: str):
    try:
        return service.clear_image_cache(group_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"清空缓存失败: {str(e)}")


@router.get("/api/groups/{group_id}/images/{image_path:path}")
async def get_local_image(group_id: str, image_path: str):
    try:
        return service.get_local_image(group_id, image_path)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取图片失败: {str(e)}")


@router.get("/api/groups/{group_id}/videos/{video_path:path}")
async def get_local_video(group_id: str, video_path: str):
    try:
        return service.get_local_video(group_id, video_path)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取视频失败: {str(e)}")


@router.get("/api/proxy/image")
async def proxy_image_plain(url: str):
    try:
        return service.proxy_image_plain(url)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"图片加载失败: {str(e)}")

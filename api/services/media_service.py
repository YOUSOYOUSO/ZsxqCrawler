from __future__ import annotations

import mimetypes
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from fastapi import HTTPException
from fastapi.responses import FileResponse, Response

from app.runtime.image_cache_manager import get_image_cache_manager
from modules.shared.db_path_manager import get_db_path_manager


class MediaService:
    def proxy_image_with_cache(self, url: str, group_id: Optional[str] = None) -> Response:
        cache_manager = get_image_cache_manager(group_id)

        if cache_manager.is_cached(url):
            cached_path = cache_manager.get_cached_path(url)
            if cached_path and cached_path.exists():
                content_type = mimetypes.guess_type(str(cached_path))[0] or "image/jpeg"
                with open(cached_path, "rb") as f:
                    content = f.read()
                return Response(
                    content=content,
                    media_type=content_type,
                    headers={
                        "Cache-Control": "public, max-age=86400",
                        "Access-Control-Allow-Origin": "*",
                        "X-Cache-Status": "HIT",
                    },
                )

        success, cached_path, error = cache_manager.download_and_cache(url)
        if success and cached_path and cached_path.exists():
            content_type = mimetypes.guess_type(str(cached_path))[0] or "image/jpeg"
            with open(cached_path, "rb") as f:
                content = f.read()
            return Response(
                content=content,
                media_type=content_type,
                headers={
                    "Cache-Control": "public, max-age=86400",
                    "Access-Control-Allow-Origin": "*",
                    "X-Cache-Status": "MISS",
                },
            )

        raise HTTPException(status_code=404, detail=f"图片加载失败: {error}")

    def get_image_cache_info(self, group_id: str) -> Dict[str, Any]:
        cache_manager = get_image_cache_manager(group_id)
        return cache_manager.get_cache_info()

    def clear_image_cache(self, group_id: str) -> Dict[str, Any]:
        cache_manager = get_image_cache_manager(group_id)
        success, message = cache_manager.clear_cache()
        if not success:
            raise HTTPException(status_code=500, detail=message)
        return {"success": True, "message": message}

    def get_local_image(self, group_id: str, image_path: str) -> Response:
        path_manager = get_db_path_manager()
        group_dir = path_manager.get_group_data_dir(group_id)
        images_dir = Path(group_dir) / "images"

        image_file = (images_dir / image_path).resolve()
        if not str(image_file).startswith(str(images_dir.resolve())):
            raise HTTPException(status_code=403, detail="禁止访问该路径")

        if not image_file.exists():
            raise HTTPException(status_code=404, detail="图片不存在")

        content_type = mimetypes.guess_type(str(image_file))[0] or "application/octet-stream"
        with open(image_file, "rb") as f:
            content = f.read()
        return Response(content=content, media_type=content_type)

    def get_local_video(self, group_id: str, video_path: str) -> FileResponse:
        path_manager = get_db_path_manager()
        group_dir = path_manager.get_group_dir(group_id)
        videos_dir = Path(group_dir) / "column_videos"

        video_file = (videos_dir / video_path).resolve()
        if not str(video_file).startswith(str(videos_dir.resolve())):
            raise HTTPException(status_code=403, detail="禁止访问该路径")

        if not video_file.exists():
            raise HTTPException(status_code=404, detail="视频不存在")

        content_type = mimetypes.guess_type(str(video_file))[0] or "video/mp4"
        return FileResponse(path=str(video_file), media_type=content_type, filename=video_file.name)

    def proxy_image_plain(self, url: str) -> Response:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/91.0.4472.124 Safari/537.36"
            ),
            "Referer": "https://wx.zsxq.com/",
            "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
        }

        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return Response(
            content=response.content,
            media_type=response.headers.get("content-type", "image/jpeg"),
            headers={
                "Cache-Control": "public, max-age=3600",
                "Access-Control-Allow-Origin": "*",
            },
        )

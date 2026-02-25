from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException

from api.schemas.models import FileDownloadRequest
from api.services.file_service import FileService

router = APIRouter(tags=["files"])
service = FileService()


@router.post("/api/files/collect/{group_id}")
async def collect_files(group_id: str, background_tasks: BackgroundTasks):
    try:
        return service.start_collect_files(group_id=group_id, background_tasks=background_tasks)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建文件收集任务失败: {str(e)}")


@router.post("/api/files/download/{group_id}")
async def download_files(group_id: str, request: FileDownloadRequest, background_tasks: BackgroundTasks):
    try:
        return service.start_download_files(group_id=group_id, request=request, background_tasks=background_tasks)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建文件下载任务失败: {str(e)}")


@router.post("/api/files/download-single/{group_id}/{file_id}")
async def download_single_file(
    group_id: str,
    file_id: int,
    background_tasks: BackgroundTasks,
    file_name: Optional[str] = None,
    file_size: Optional[int] = None,
):
    try:
        return service.start_download_single_file(
            group_id=group_id,
            file_id=file_id,
            background_tasks=background_tasks,
            file_name=file_name,
            file_size=file_size,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建单个文件下载任务失败: {str(e)}")


@router.get("/api/files/status/{group_id}/{file_id}")
async def get_file_status(group_id: str, file_id: int):
    try:
        return service.get_file_status(group_id=group_id, file_id=file_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取文件状态失败: {str(e)}")


@router.get("/api/files/check-local/{group_id}")
async def check_local_file_status(group_id: str, file_name: str, file_size: int):
    try:
        return service.check_local_file_status(group_id=group_id, file_name=file_name, file_size=file_size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"检查本地文件失败: {str(e)}")


@router.get("/api/files/stats/{group_id}")
async def get_file_stats(group_id: str):
    try:
        return service.get_file_stats(group_id=group_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取文件统计失败: {str(e)}")


@router.post("/api/files/clear/{group_id}")
async def clear_file_database(group_id: str):
    try:
        return service.clear_file_database(group_id=group_id)
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除文件数据库失败: {str(e)}")


@router.get("/api/files/{group_id}")
async def get_files(group_id: str, page: int = 1, per_page: int = 20, status: Optional[str] = None):
    try:
        return service.list_files(group_id=group_id, page=page, per_page=per_page, status=status)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取文件列表失败: {str(e)}")

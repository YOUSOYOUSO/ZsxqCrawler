from fastapi import APIRouter, BackgroundTasks, HTTPException

from api.schemas.models import ColumnsSettingsRequest
from api.services.columns_service import ColumnsService

router = APIRouter(tags=["columns"])
service = ColumnsService()


@router.get("/api/groups/{group_id}/columns")
async def get_group_columns(group_id: str):
    try:
        return service.get_group_columns(group_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取专栏目录失败: {str(e)}")


@router.get("/api/groups/{group_id}/columns/{column_id}/topics")
async def get_column_topics(group_id: str, column_id: int):
    try:
        return service.get_column_topics(group_id, column_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取专栏文章列表失败: {str(e)}")


@router.get("/api/groups/{group_id}/columns/topics/{topic_id}")
async def get_column_topic_detail(group_id: str, topic_id: int):
    try:
        return service.get_column_topic_detail(group_id, topic_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取文章详情失败: {str(e)}")


@router.get("/api/groups/{group_id}/columns/summary")
async def get_group_columns_summary(group_id: str):
    try:
        return service.get_group_columns_summary(group_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取专栏信息失败: {str(e)}")


@router.get("/api/groups/{group_id}/columns/stats")
async def get_columns_stats(group_id: str):
    try:
        return service.get_columns_stats(group_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取专栏统计失败: {str(e)}")


@router.delete("/api/groups/{group_id}/columns/all")
async def delete_all_columns(group_id: str):
    try:
        return service.delete_all_columns(group_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除专栏数据失败: {str(e)}")


@router.get("/api/groups/{group_id}/columns/topics/{topic_id}/comments")
async def get_column_topic_full_comments(group_id: str, topic_id: int):
    try:
        return service.get_column_topic_full_comments(group_id, topic_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取完整评论失败: {str(e)}")


@router.post("/api/groups/{group_id}/columns/fetch")
async def fetch_group_columns(group_id: str, request: ColumnsSettingsRequest, background_tasks: BackgroundTasks):
    try:
        return service.start_fetch(group_id=group_id, request=request, background_tasks=background_tasks)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"启动专栏采集失败: {str(e)}")

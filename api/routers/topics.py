from fastapi import APIRouter, HTTPException

from api.services.topic_service import TopicService

router = APIRouter(tags=["topics"])
service = TopicService()


@router.post("/api/topics/clear/{group_id}")
async def clear_topic_database(group_id: str):
    try:
        return service.clear_topic_database(group_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除话题数据库失败: {str(e)}")


@router.get("/api/topics")
async def get_topics(page: int = 1, per_page: int = 20, search: str | None = None):
    try:
        return service.get_topics(page=page, per_page=per_page, search=search)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取话题列表失败: {str(e)}")


@router.get("/api/topics/{topic_id}/{group_id}")
async def get_topic_detail(topic_id: int, group_id: str):
    try:
        return service.get_topic_detail(topic_id, group_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取话题详情失败: {str(e)}")


@router.post("/api/topics/{topic_id}/{group_id}/refresh")
async def refresh_topic(topic_id: int, group_id: str):
    try:
        return service.refresh_topic(topic_id, group_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新话题失败: {str(e)}")


@router.post("/api/topics/{topic_id}/{group_id}/fetch-comments")
async def fetch_more_comments(topic_id: int, group_id: str):
    try:
        return service.fetch_more_comments(topic_id, group_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取更多评论失败: {str(e)}")


@router.delete("/api/topics/{topic_id}/{group_id}")
async def delete_single_topic(topic_id: int, group_id: int):
    try:
        return service.delete_single_topic(topic_id, group_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除话题失败: {str(e)}")


@router.post("/api/topics/fetch-single/{group_id}/{topic_id}")
async def fetch_single_topic(group_id: str, topic_id: int, fetch_comments: bool = True):
    try:
        return service.fetch_single_topic(group_id, topic_id, fetch_comments)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"单个话题采集失败: {str(e)}")


@router.get("/api/groups/{group_id}/tags")
async def get_group_tags(group_id: str):
    try:
        return service.get_group_tags(group_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取标签列表失败: {str(e)}")


@router.get("/api/groups/{group_id}/tags/{tag_id}/topics")
async def get_topics_by_tag(group_id: int, tag_id: int, page: int = 1, per_page: int = 20):
    try:
        return service.get_topics_by_tag(group_id, tag_id, page, per_page)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"根据标签获取话题失败: {str(e)}")


@router.get("/api/groups/{group_id}/topics")
async def get_group_topics(group_id: int, page: int = 1, per_page: int = 20, search: str | None = None):
    try:
        return service.get_group_topics(group_id, page, per_page, search)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取群组话题失败: {str(e)}")


@router.get("/api/groups/{group_id}/stats")
async def get_group_stats(group_id: int):
    try:
        return service.get_group_stats(group_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取群组统计失败: {str(e)}")


@router.get("/api/groups/{group_id}/database-info")
async def get_group_database_info(group_id: int):
    try:
        return service.get_group_database_info(group_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取数据库信息失败: {str(e)}")


@router.delete("/api/groups/{group_id}/topics")
async def delete_group_topics(group_id: int):
    try:
        return service.delete_group_topics(group_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除话题数据失败: {str(e)}")

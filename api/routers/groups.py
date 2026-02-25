from fastapi import APIRouter, HTTPException

from api.services.database_stats_service import DatabaseStatsService

router = APIRouter(tags=["groups"])
service = DatabaseStatsService()


@router.get("/api/database/stats")
async def get_database_stats():
    try:
        return service.get_database_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取数据库统计失败: {str(e)}")

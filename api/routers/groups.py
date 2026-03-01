from fastapi import APIRouter, HTTPException

from api.services.global_data_correction_service import GlobalDataCorrectionService
from api.services.database_stats_service import DatabaseStatsService
from api.services.group_service import GroupService

router = APIRouter(tags=["groups"])
stats_service = DatabaseStatsService()
group_service = GroupService()
correction_service = GlobalDataCorrectionService(group_service=group_service)


@router.get("/api/database/stats")
async def get_database_stats():
    try:
        return stats_service.get_database_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取数据库统计失败: {str(e)}")


@router.post("/api/local-groups/refresh")
async def refresh_local_groups():
    try:
        return correction_service.refresh_group_metadata()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"刷新本地群失败: {str(e)}")


@router.get("/api/groups")
async def get_groups():
    try:
        return group_service.get_groups()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取群组列表失败: {str(e)}")


@router.get("/api/groups/{group_id}/info")
async def get_group_info(group_id: str):
    try:
        return group_service.get_group_info(group_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取群组信息失败: {str(e)}")


@router.delete("/api/groups/{group_id}")
async def delete_group_local(group_id: str):
    try:
        return group_service.delete_group_local(group_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除群组本地数据失败: {str(e)}")


@router.delete("/api/groups")
async def delete_all_groups_local():
    try:
        return group_service.delete_all_groups_local()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除全部群组本地数据失败: {str(e)}")

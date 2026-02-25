from datetime import datetime

from fastapi import APIRouter, HTTPException

from api.schemas.models import ConfigModel
from api.services.config_service import ConfigService

router = APIRouter(tags=["core"])
config_service = ConfigService()


@router.get("/")
async def root():
    return {"message": "知识星球数据采集器 API 服务", "version": "1.0.0"}


@router.get("/api/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now()}


@router.get("/api/meta/features")
async def get_meta_features():
    return {
        "global_sector_topics": True,
        "scheduler_v2_status": True,
        "scheduler_next_runs": True,
        "global_scan_filter": True,
        "market_data_persistence": True,
    }


@router.get("/api/config")
async def get_config():
    try:
        return config_service.get_config()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取配置失败: {str(e)}")


@router.post("/api/config")
async def update_config(config: ConfigModel):
    try:
        return config_service.update_config(config.cookie)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新配置失败: {str(e)}")

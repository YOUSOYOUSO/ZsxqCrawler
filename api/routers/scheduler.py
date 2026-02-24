import asyncio

from fastapi import APIRouter, HTTPException

from modules.shared.logger_config import log_error

router = APIRouter(prefix="/api/scheduler", tags=["scheduler"])


@router.get("/status")
async def scheduler_status():
    """调度器状态"""
    try:
        from auto_scheduler import get_scheduler

        scheduler = get_scheduler()
        return scheduler.get_status()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取调度器状态失败: {str(e)}")


@router.get("/next-runs")
async def scheduler_next_runs(count: int = 5):
    """下一批调度触发时间点。"""
    try:
        from auto_scheduler import get_scheduler

        scheduler = get_scheduler()
        return scheduler.get_next_runs(count=count)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取下次调度时间失败: {str(e)}")


@router.post("/start")
async def scheduler_start():
    """启动调度器"""
    from auto_scheduler import get_scheduler

    scheduler = get_scheduler()
    try:
        await scheduler.start()
        return {
            "success": True,
            "status": "started",
            "message": "调度器已启动",
            "scheduler": scheduler.get_status(),
        }
    except Exception as e:
        log_error(f"启动调度器失败: {e}")
        return {
            "success": False,
            "status": "error",
            "message": f"启动调度器失败: {str(e)}",
            "scheduler": scheduler.get_status(),
        }


@router.post("/stop")
async def stop_scheduler_api():
    """停止调度器"""
    from auto_scheduler import get_scheduler

    scheduler = get_scheduler()
    try:
        await scheduler.stop()
        return {
            "success": True,
            "status": "stopped",
            "message": "调度器已停止",
            "scheduler": scheduler.get_status(),
        }
    except Exception as e:
        log_error(f"停止调度器失败: {e}")
        return {
            "success": False,
            "status": "error",
            "message": f"停止调度器失败: {str(e)}",
            "scheduler": scheduler.get_status(),
        }


@router.post("/analyze")
async def analyze_scheduler_api():
    """Trigger manual analysis immediately."""
    from auto_scheduler import get_scheduler

    scheduler = get_scheduler()
    asyncio.create_task(scheduler.trigger_manual_analysis_task())
    return {"status": "analysis_triggered", "message": "数据分析已触发", "task_id": "scheduler"}


@router.post("/stop_analysis")
async def stop_analysis_api():
    """手动停止数据分析任务"""
    try:
        from auto_scheduler import get_scheduler

        scheduler = get_scheduler()
        result = await scheduler.stop_manual_analysis()
        if result:
            return {"status": "stopped", "message": "数据分析任务已停止"}
        return {"status": "idle", "message": "没有正在运行的数据分析任务或无法单独停止定时任务"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"停止数据分析失败: {str(e)}")


@router.post("/config")
async def scheduler_update_config(config: dict):
    """更新调度器配置"""
    try:
        from auto_scheduler import get_scheduler

        scheduler = get_scheduler()
        scheduler.update_config(config)
        return {"status": "updated", "config": scheduler.config}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新配置失败: {str(e)}")

from datetime import datetime

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from api.services.task_facade import TaskFacade

router = APIRouter(tags=["tasks"])
service = TaskFacade()


@router.get("/api/tasks")
async def get_tasks():
    return service.get_tasks()


@router.get("/api/tasks/summary")
async def get_tasks_summary():
    return service.build_summary()


@router.get("/api/tasks/{task_id}")
async def get_task(task_id: str):
    try:
        return service.get_task(task_id)
    except HTTPException as e:
        if e.status_code != 404:
            raise
        # 兼容前端轮询：服务重启后内存任务会丢失，返回终态快照以停止轮询。
        now = datetime.now().isoformat()
        return {
            "task_id": task_id,
            "type": "unknown",
            "status": "cancelled",
            "message": "任务不存在（可能已结束或服务已重启）",
            "result": None,
            "created_at": now,
            "updated_at": now,
        }


@router.post("/api/tasks/{task_id}/stop")
async def stop_task_api(task_id: str):
    return await service.stop_task(task_id)


@router.get("/api/tasks/{task_id}/logs")
async def get_task_logs(task_id: str):
    try:
        return service.get_task_logs(task_id)
    except HTTPException as e:
        if e.status_code != 404:
            raise
        return {"task_id": task_id, "logs": []}


@router.get("/api/tasks/{task_id}/stream")
async def stream_task_logs(task_id: str):
    return StreamingResponse(
        service.stream_task_logs(task_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
        },
    )

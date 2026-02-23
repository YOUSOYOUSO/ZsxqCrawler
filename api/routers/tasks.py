from fastapi import APIRouter

router = APIRouter(tags=["tasks"])


@router.get("/api/tasks")
async def get_tasks():
    import main as legacy

    return await legacy.get_tasks()


@router.get("/api/tasks/summary")
async def get_tasks_summary():
    import main as legacy

    return await legacy.get_tasks_summary()


@router.get("/api/tasks/{task_id}")
async def get_task(task_id: str):
    import main as legacy

    return await legacy.get_task(task_id)


@router.post("/api/tasks/{task_id}/stop")
async def stop_task_api(task_id: str):
    import main as legacy

    return await legacy.stop_task_api(task_id)


@router.get("/api/tasks/{task_id}/logs")
async def get_task_logs(task_id: str):
    import main as legacy

    return await legacy.get_task_logs(task_id)


@router.get("/api/tasks/{task_id}/stream")
async def stream_task_logs(task_id: str):
    import main as legacy

    return await legacy.stream_task_logs(task_id)


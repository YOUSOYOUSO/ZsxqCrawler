"""知识星球数据采集器 FastAPI 入口。"""

import asyncio
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import List

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# 添加项目根目录到 Python 路径（app/main.py 在 app 目录下）
project_root = str(Path(__file__).resolve().parents[1])
if project_root not in sys.path:
    sys.path.append(project_root)

from api.app_factory import register_core_routers
from api.deps.container import get_task_runtime
from api.services.group_service import GroupService
from modules.shared.logger_config import ensure_configured

ensure_configured()


def _parse_cors_origins() -> List[str]:
    raw = os.environ.get("CORS_ALLOW_ORIGINS", "http://localhost:3060,http://127.0.0.1:3060")
    origins = [origin.strip().rstrip("/") for origin in raw.split(",") if origin.strip()]
    return origins or ["http://localhost:3060"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时预热本地群扫描缓存
    try:
        await asyncio.to_thread(GroupService().scan_local_groups)
    except Exception as e:
        print(f"⚠️ 启动扫描本地群失败: {e}")
    yield


app = FastAPI(
    title="知识星球数据采集器 API",
    description="为知识星球数据采集器提供 RESTful API 接口",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_parse_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

register_core_routers(app)


# 调度器与任务运行时桥接
_runtime = get_task_runtime()


def _scheduler_log_callback(message: str):
    _runtime.set_scheduler_log(message, cap=500)


def _scheduler_status_callback(status: str, message: str):
    _runtime.update_task(task_id="scheduler", status=status, message=message)
    _runtime.append_log("scheduler", f"状态更新: {message}")


try:
    from app.scheduler.auto_scheduler import get_scheduler

    scheduler = get_scheduler()
    scheduler.set_log_callback(_scheduler_log_callback)
    scheduler.set_status_callback(_scheduler_status_callback)
    snapshot = scheduler.get_status()
    _runtime.update_task(
        task_id="scheduler",
        status=snapshot.get("state", "stopped"),
        message="自动调度系统",
    )
except ImportError:
    pass


if __name__ == "__main__":
    port = 8208
    if len(sys.argv) > 2 and sys.argv[1] == "--port":
        try:
            port = int(sys.argv[2])
        except ValueError:
            port = 8208
    print(f"[startup] API version=1.0.0, port={port}, at={datetime.now().isoformat()}")
    uvicorn.run(app, host="0.0.0.0", port=port)

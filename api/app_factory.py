from fastapi import FastAPI

from api.routers.global_read import router as global_read_router
from api.routers.global_tasks import router as global_tasks_router
from api.routers.crawl import router as crawl_router
from api.routers.files import router as files_router
from api.routers.groups import router as groups_router
from api.routers.columns import router as columns_router
from api.routers.settings import router as settings_router
from api.routers.topics import router as topics_router
from api.routers.scheduler import router as scheduler_router
from api.routers.stocks import router as stocks_router
from api.routers.tasks import router as tasks_router


def register_core_routers(app: FastAPI) -> None:
    """Register all core routers while preserving existing API paths."""
    app.include_router(scheduler_router)
    app.include_router(stocks_router)
    app.include_router(global_read_router)
    app.include_router(tasks_router)
    app.include_router(global_tasks_router)
    app.include_router(crawl_router)
    app.include_router(files_router)
    app.include_router(groups_router)
    app.include_router(columns_router)
    app.include_router(settings_router)
    app.include_router(topics_router)

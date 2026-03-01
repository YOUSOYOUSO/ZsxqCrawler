import asyncio

from fastapi import APIRouter, BackgroundTasks, HTTPException
from api.schemas.models import MarketDataProbeRequest, MarketDataSourceSettingsRequest
from api.services.market_data_source_service import MarketDataSourceService
from api.services.task_facade import TaskFacade

router = APIRouter(prefix="/api/market-data", tags=["market-data"])
source_service = MarketDataSourceService()
task_facade = TaskFacade()


@router.get("/status")
async def market_data_status():
    try:
        from modules.shared.market_data_store import MarketDataStore
        from modules.analyzers.market_data_sync import MarketDataSyncService

        store = MarketDataStore()
        svc = MarketDataSyncService(store=store)
        status = store.get_status()
        status["provider_health"] = svc.get_provider_health_snapshot()
        status["provider_failure_summary"] = svc.get_provider_failure_summary()
        status["last_probe_at"] = status["provider_health"].get("last_probe_at")
        return status
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取行情库状态失败: {e}")


@router.post("/sync")
async def market_data_sync_incremental():
    try:
        from modules.analyzers.market_data_sync import MarketDataSyncService

        service = MarketDataSyncService()
        result = await asyncio.to_thread(service.sync_daily_incremental)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"执行行情增量同步失败: {e}")


def _run_market_data_bootstrap_task(task_id: str, resume: bool, symbol_limit: int):
    task_facade.update_task(
        task_id,
        "running",
        "正在执行行情全历史回补（耗时较长，请查看任务日志进度）",
    )
    task_facade.append_log(
        task_id,
        "⚠️ 全历史回补会遍历大量股票与历史交易日，通常需要较长时间，期间可随时停止任务。",
    )
    try:
        from modules.analyzers.market_data_sync import MarketDataSyncService

        service = MarketDataSyncService(log_callback=lambda msg: task_facade.append_log(task_id, msg))
        result = service.backfill_history_full(
            resume=resume,
            batch_size=None,
            symbol_limit=symbol_limit if symbol_limit > 0 else None,
            stop_checker=lambda: task_facade.is_task_stopped(task_id),
            progress_every=50,
        )
        if result.get("stopped"):
            task_facade.update_task(
                task_id,
                "cancelled",
                f"行情全历史回补已停止：{result.get('processed_symbols', 0)}/{result.get('total_symbols', 0)}",
                result,
            )
            return
        if result.get("success"):
            task_facade.update_task(
                task_id,
                "completed",
                f"行情全历史回补完成：{result.get('processed_symbols', 0)}/{result.get('total_symbols', 0)}",
                result,
            )
        else:
            task_facade.update_task(
                task_id,
                "failed",
                f"行情全历史回补完成但有错误：errors={result.get('errors', 0)}",
                result,
            )
    except Exception as e:
        task_facade.append_log(task_id, f"❌ 行情全历史回补异常: {e}")
        task_facade.update_task(task_id, "failed", f"行情全历史回补失败: {e}")


@router.post("/bootstrap")
async def market_data_bootstrap(background_tasks: BackgroundTasks, resume: bool = True, symbol_limit: int = 0, confirm: bool = False):
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail="全历史回补耗时较长且负载较高，请确认风险后重试（query: confirm=true）",
        )
    task_id = task_facade.create_task(
        task_type="market_data_bootstrap",
        description="行情全历史回补（待启动）",
    )
    background_tasks.add_task(_run_market_data_bootstrap_task, task_id, resume, symbol_limit)
    return {
        "task_id": task_id,
        "message": "已启动行情全历史回补，请在任务日志查看进度",
        "notice": "全历史回补通常耗时较长；建议优先设置 symbol_limit 小范围验证。",
    }


@router.get("/providers")
async def market_data_providers():
    try:
        return source_service.get_settings()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取行情源配置失败: {e}")


@router.post("/providers")
async def market_data_update_providers(request: MarketDataSourceSettingsRequest):
    try:
        return source_service.update_settings(request.model_dump())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"保存行情源配置失败: {e}")


@router.post("/providers/probe")
async def market_data_probe_providers(request: MarketDataProbeRequest):
    try:
        return await asyncio.to_thread(
            source_service.probe,
            request.providers,
            request.symbol,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"执行行情源探活失败: {e}")

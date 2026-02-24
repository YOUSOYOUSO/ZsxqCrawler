import asyncio

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/market-data", tags=["market-data"])


@router.get("/status")
async def market_data_status():
    try:
        from modules.shared.market_data_store import MarketDataStore

        store = MarketDataStore()
        return store.get_status()
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


@router.post("/bootstrap")
async def market_data_bootstrap(resume: bool = True, symbol_limit: int = 0):
    try:
        from modules.analyzers.market_data_sync import MarketDataSyncService

        service = MarketDataSyncService()
        result = await asyncio.to_thread(
            service.backfill_history_full,
            resume,
            None,
            symbol_limit if symbol_limit > 0 else None,
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"执行行情全历史回填失败: {e}")

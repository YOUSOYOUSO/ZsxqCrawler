from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Response

from modules.analyzers.ai_analyzer import AIAnalyzer
from modules.shared.db_path_manager import get_db_path_manager
from modules.analyzers.stock_analyzer import StockAnalyzer

from api.schemas.models import AIConfigModel
from api.services.stock_scan_service import StockScanService

router = APIRouter(tags=["stocks", "ai"])
scan_service = StockScanService()


def _extract_cache_hit(payload: object) -> Optional[bool]:
    if isinstance(payload, dict):
        meta = payload.get("_meta")
        if isinstance(meta, dict) and "cache_hit" in meta:
            return bool(meta.get("cache_hit"))
        if "_cache_hit" in payload:
            return bool(payload.get("_cache_hit"))
    return None


def _set_data_headers(response: Response, payload: object) -> None:
    cache_hit = _extract_cache_hit(payload)
    if cache_hit is not None:
        response.headers["X-Cache-Hit"] = "1" if cache_hit else "0"
    if not isinstance(payload, dict):
        return
    meta = payload.get("_meta")
    if not isinstance(meta, dict):
        return
    anchor_date = str(meta.get("anchor_date") or "").strip()
    data_mode = str(meta.get("data_mode") or "").strip()
    if anchor_date:
        response.headers["X-Data-Anchor-Date"] = anchor_date
    if data_mode:
        response.headers["X-Data-Mode"] = data_mode


def _get_ai_analyzer(group_id: str) -> AIAnalyzer:
    db_path = get_db_path_manager().get_topics_db_path(group_id)
    return AIAnalyzer(db_path=db_path, group_id=group_id)


@router.get("/api/groups/{group_id}/stock/topics")
def get_stock_topics(group_id: str, page: int = 1, per_page: int = 20):
    try:
        analyzer = StockAnalyzer(group_id)
        return analyzer.get_topic_mentions(page=page, per_page=per_page)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取话题列表失败: {str(e)}")


@router.get("/api/groups/{group_id}/stock/mentions")
def get_stock_mentions(
    group_id: str,
    stock_code: str = None,
    page: int = 1,
    per_page: int = 50,
    sort_by: str = "mention_date",
    order: str = "desc",
):
    try:
        analyzer = StockAnalyzer(group_id)
        return analyzer.get_mentions(stock_code=stock_code, page=page, per_page=per_page, sort_by=sort_by, order=order)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取提及数据失败: {str(e)}")


@router.get("/api/groups/{group_id}/stock/{stock_code}/events")
def get_stock_events(
    group_id: str,
    stock_code: str,
    response: Response,
    refresh_realtime: bool = False,
    detail_mode: str = "fast",
    page: int = 1,
    per_page: int = 50,
    include_full_text: bool = False,
):
    try:
        analyzer = StockAnalyzer(group_id)
        data = analyzer.get_stock_events(
            stock_code,
            refresh_realtime=refresh_realtime,
            detail_mode=detail_mode,
            page=page,
            per_page=per_page,
            include_full_text=include_full_text,
        )
        if bool(data.get("refresh_skipped")):
            response.headers["X-Refresh-Skipped"] = "1"
        _set_data_headers(response, data)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取股票事件失败: {str(e)}")


@router.post("/api/groups/{group_id}/stock/{stock_code}/events/refresh")
def trigger_stock_events_refresh(group_id: str, stock_code: str):
    try:
        analyzer = StockAnalyzer(group_id)
        return analyzer.schedule_stock_events_refresh(stock_code)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"触发股票事件刷新失败: {str(e)}")


@router.get("/api/groups/{group_id}/stock/{stock_code}/price")
def get_stock_price_with_mentions(group_id: str, stock_code: str, days: int = 90):
    try:
        analyzer = StockAnalyzer(group_id)
        return analyzer.get_stock_price_with_mentions(stock_code, days=days)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取价格数据失败: {str(e)}")


@router.get("/api/groups/{group_id}/stock/win-rate")
def get_stock_win_rate(
    group_id: str,
    response: Response,
    min_mentions: int = 2,
    return_period: str = "return_5d",
    limit: int = 50,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
    sort_by: str = "win_rate",
    order: str = "desc",
):
    try:
        min_mentions = max(1, min_mentions)
        analyzer = StockAnalyzer(group_id)
        data = analyzer.get_win_rate_ranking(
            min_mentions=min_mentions,
            return_period=return_period,
            limit=limit,
            start_date=start_date,
            end_date=end_date,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            order=order,
        )
        _set_data_headers(response, data)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取胜率排行失败: {str(e)}")


@router.get("/api/groups/{group_id}/stock/sector-heat")
def get_sector_heatmap(group_id: str, response: Response, start_date: Optional[str] = None, end_date: Optional[str] = None):
    try:
        analyzer = StockAnalyzer(group_id)
        data = analyzer.get_sector_heatmap(start_date=start_date, end_date=end_date)
        _set_data_headers(response, data)
        if "X-Data-Mode" not in response.headers:
            response.headers["X-Data-Mode"] = "finalized"
        if "X-Data-Anchor-Date" not in response.headers:
            response.headers["X-Data-Anchor-Date"] = analyzer.get_data_anchor_date()
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取板块热度失败: {str(e)}")


@router.get("/api/groups/{group_id}/stock/signals")
def get_stock_signals(
    group_id: str,
    response: Response,
    lookback_days: int = 7,
    min_mentions: int = 2,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    try:
        min_mentions = max(1, min_mentions)
        analyzer = StockAnalyzer(group_id)
        data = analyzer.get_signals(
            lookback_days=lookback_days,
            min_mentions=min_mentions,
            start_date=start_date,
            end_date=end_date,
        )
        _set_data_headers(response, data)
        if "X-Data-Mode" not in response.headers:
            response.headers["X-Data-Mode"] = "finalized"
        if "X-Data-Anchor-Date" not in response.headers:
            response.headers["X-Data-Anchor-Date"] = analyzer.get_data_anchor_date()
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取信号雷达失败: {str(e)}")


@router.get("/api/groups/{group_id}/stock/sector-topics")
def get_sector_topics(
    group_id: str,
    response: Response,
    sector: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
):
    try:
        analyzer = StockAnalyzer(group_id)
        data = analyzer.get_sector_topics(
            sector=sector,
            start_date=start_date,
            end_date=end_date,
            page=page,
            page_size=page_size,
        )
        _set_data_headers(response, data)
        return data
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取板块话题失败: {str(e)}")


@router.get("/api/groups/{group_id}/stock/stats")
def get_stock_stats(group_id: str, response: Response):
    try:
        analyzer = StockAnalyzer(group_id)
        data = analyzer.get_summary_stats()
        _set_data_headers(response, data)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取统计失败: {str(e)}")




@router.post("/api/groups/{group_id}/stock/scan")
def scan_group_stocks(
    group_id: str,
    background_tasks: BackgroundTasks,
    force: bool = False,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    if (start_date and not end_date) or (end_date and not start_date):
        raise HTTPException(status_code=400, detail="start_date 与 end_date 必须同时提供")

    if start_date and end_date:
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="日期格式错误，请使用 YYYY-MM-DD")
        if start_dt > end_dt:
            raise HTTPException(status_code=400, detail="start_date 不能晚于 end_date")

    return scan_service.start_scan(
        group_id=group_id,
        background_tasks=background_tasks,
        force=force,
        start_date=start_date,
        end_date=end_date,
    )


@router.get("/api/ai/config")
async def get_ai_config():
    try:
        analyzer = AIAnalyzer()
        return analyzer.get_config_status()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取AI配置失败: {str(e)}")


@router.post("/api/ai/config")
async def update_ai_config(config: AIConfigModel):
    try:
        analyzer = AIAnalyzer()
        analyzer.update_config(api_key=config.api_key, base_url=config.base_url, model=config.model)
        return {"success": True, "message": "AI 配置已更新"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新AI配置失败: {str(e)}")


@router.post("/api/groups/{group_id}/ai/analyze/{stock_code}")
async def ai_analyze_stock(group_id: str, stock_code: str, force: bool = False):
    try:
        ai = _get_ai_analyzer(group_id)
        result = ai.analyze_stock(stock_code, force=force)
        if result.get("error"):
            raise HTTPException(status_code=400, detail=result["error"])
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"AI分析失败: {str(e)}")


@router.post("/api/groups/{group_id}/ai/daily-brief")
async def ai_daily_brief(group_id: str, lookback_days: int = 7, force: bool = False):
    try:
        ai = _get_ai_analyzer(group_id)
        result = ai.generate_daily_brief(lookback_days=lookback_days, force=force)
        if result.get("error"):
            raise HTTPException(status_code=400, detail=result["error"])
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成简报失败: {str(e)}")


@router.post("/api/groups/{group_id}/ai/consensus")
async def ai_consensus(group_id: str, top_n: int = 10, force: bool = False):
    try:
        ai = _get_ai_analyzer(group_id)
        result = ai.analyze_consensus(top_n=top_n, force=force)
        if result.get("error"):
            raise HTTPException(status_code=400, detail=result["error"])
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"共识分析失败: {str(e)}")


@router.get("/api/groups/{group_id}/ai/history")
async def ai_history(group_id: str, summary_type: Optional[str] = None, limit: int = 20):
    try:
        ai = _get_ai_analyzer(group_id)
        return ai.get_history(summary_type=summary_type, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取历史失败: {str(e)}")


@router.get("/api/groups/{group_id}/ai/history/{summary_id}")
async def ai_history_detail(group_id: str, summary_id: int):
    try:
        ai = _get_ai_analyzer(group_id)
        result = ai.get_history_detail(summary_id)
        if not result:
            raise HTTPException(status_code=404, detail="未找到该分析记录")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取详情失败: {str(e)}")

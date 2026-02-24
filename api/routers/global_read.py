import time
from typing import List, Optional

from fastapi import APIRouter, HTTPException

from modules.analyzers.ai_analyzer import AIAnalyzer
from modules.shared.logger_config import log_error, log_info

router = APIRouter(tags=["global"])


def _get_global_ai_analyzer() -> AIAnalyzer:
    return AIAnalyzer(db_path=None, group_id=None)


@router.get("/api/global/stats")
async def global_stats():
    """全局统计概览"""
    try:
        from modules.analyzers.global_analyzer import get_global_analyzer

        analyzer = get_global_analyzer()
        return analyzer.get_global_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"全局统计失败: {str(e)}")


@router.get("/api/global/hot-words")
async def get_global_hot_words(
    days: int = 1,
    limit: int = 50,
    force: bool = False,
    window_hours: Optional[int] = None,
    normalize: bool = True,
    fallback: bool = True,
    fallback_windows: str = "24,36,48,168",
):
    """获取全局热词（滑动小时窗口，支持回退与归一化）。"""
    try:
        allowed_windows = {24, 36, 48, 168}
        requested_window = int(window_hours or (int(days or 1) * 24))
        if requested_window not in allowed_windows:
            raise HTTPException(status_code=400, detail=f"window_hours 仅支持 {sorted(allowed_windows)}")

        parsed_fallback_windows: List[int] = []
        for token in str(fallback_windows or "").split(","):
            t = token.strip()
            if not t:
                continue
            try:
                w = int(t)
            except Exception:
                continue
            if w in allowed_windows and w not in parsed_fallback_windows:
                parsed_fallback_windows.append(w)
        if not parsed_fallback_windows:
            parsed_fallback_windows = [24, 36, 48, 168]

        from modules.analyzers.global_analyzer import get_global_analyzer

        analyzer = get_global_analyzer()
        return analyzer.get_global_hot_words(
            days=days,
            limit=limit,
            force_refresh=force,
            window_hours=requested_window,
            normalize=normalize,
            fallback=fallback,
            fallback_windows=parsed_fallback_windows,
        )
    except HTTPException:
        raise
    except Exception as e:
        log_error(f"Failed to get global hot words: {e}")
        return {
            "words": [],
            "window_hours_requested": int(window_hours or (int(days or 1) * 24)),
            "window_hours_effective": int(window_hours or (int(days or 1) * 24)),
            "fallback_applied": False,
            "fallback_reason": f"服务异常: {str(e)}",
            "data_points_total": 0,
            "time_range": {},
        }


@router.get("/api/global/win-rate")
async def get_global_win_rate(
    min_mentions: int = 2,
    return_period: str = "return_5d",
    limit: int = 1000,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    sort_by: str = "win_rate",
    order: str = "desc",
    page: int = 1,
    page_size: int = 20,
):
    start_time = time.time()
    try:
        from modules.analyzers.global_analyzer import get_global_analyzer

        analyzer = get_global_analyzer()
        data = analyzer.get_global_win_rate(
            min_mentions=min_mentions,
            return_period=return_period,
            limit=limit,
            start_date=start_date,
            end_date=end_date,
            sort_by=sort_by,
            order=order,
            page=page,
            page_size=page_size,
        )
        duration = time.time() - start_time
        log_info(f"API /global/win-rate took {duration:.2f}s (page={page}, items={len(data.get('data',[]))}, start={start_date}, end={end_date})")
        return data
    except Exception as e:
        log_error(f"Failed to get global win rate: {e}")
        return {"error": str(e), "data": [], "total": 0}


@router.get("/api/global/stock/{stock_code}/events")
async def global_stock_events(stock_code: str):
    """全局股票事件详情"""
    try:
        from modules.analyzers.global_analyzer import get_global_analyzer

        analyzer = get_global_analyzer()
        return analyzer.get_global_stock_events(stock_code)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取全局股票事件失败: {str(e)}")


@router.get("/api/global/sector-heat")
async def get_global_sector_heat(start_date: Optional[str] = None, end_date: Optional[str] = None):
    try:
        from modules.analyzers.global_analyzer import get_global_analyzer

        analyzer = get_global_analyzer()
        return analyzer.get_global_sector_heat(start_date=start_date, end_date=end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"板块热度查询失败: {str(e)}")


@router.get("/api/global/sector-topics")
async def get_global_sector_topics(
    sector: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
):
    try:
        from modules.analyzers.global_analyzer import get_global_analyzer

        analyzer = get_global_analyzer()
        return analyzer.get_global_sector_topics(
            sector=sector,
            start_date=start_date,
            end_date=end_date,
            page=page,
            page_size=page_size,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"全局板块话题查询失败: {str(e)}")


@router.get("/api/global/signals")
async def global_signals(lookback_days: int = 7, min_mentions: int = 2, start_date: Optional[str] = None, end_date: Optional[str] = None):
    try:
        from modules.analyzers.global_analyzer import get_global_analyzer

        analyzer = get_global_analyzer()
        return analyzer.get_global_signals(lookback_days, min_mentions, start_date=start_date, end_date=end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"信号查询失败: {str(e)}")


@router.get("/api/global/groups")
async def global_groups_overview():
    try:
        from modules.analyzers.global_analyzer import get_global_analyzer

        analyzer = get_global_analyzer()
        return analyzer.get_groups_overview()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"群组概览失败: {str(e)}")


@router.get("/api/global/topics")
async def global_whitelist_topics(page: int = 1, per_page: int = 20, search: Optional[str] = None):
    try:
        from modules.analyzers.global_analyzer import get_global_analyzer

        analyzer = get_global_analyzer()
        return analyzer.get_whitelist_topic_mentions(page=page, per_page=per_page, search=search)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取白名单话题失败: {str(e)}")


@router.post("/api/global/ai/daily-brief")
async def global_ai_daily_brief(lookback_days: int = 7, force: bool = False):
    try:
        ai = _get_global_ai_analyzer()
        result = ai.generate_global_daily_brief(lookback_days=lookback_days, force=force)
        if result.get("error"):
            raise HTTPException(status_code=400, detail=result["error"])
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成全局简报失败: {str(e)}")


@router.post("/api/global/ai/consensus")
async def global_ai_consensus(top_n: int = 15, force: bool = False):
    try:
        ai = _get_global_ai_analyzer()
        result = ai.analyze_global_consensus(top_n=top_n, force=force)
        if result.get("error"):
            raise HTTPException(status_code=400, detail=result["error"])
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"全局共识分析失败: {str(e)}")


@router.get("/api/global/ai/history")
async def global_ai_history(summary_type: Optional[str] = None, limit: int = 20):
    try:
        ai = _get_global_ai_analyzer()
        return ai.get_history(summary_type=summary_type, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取全局历史失败: {str(e)}")


@router.get("/api/global/ai/history/{summary_id}")
async def global_ai_history_detail(summary_id: int):
    try:
        ai = _get_global_ai_analyzer()
        result = ai.get_history_detail(summary_id)
        if not result:
            raise HTTPException(status_code=404, detail="未找到该分析记录")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取全局详情失败: {str(e)}")

'use client';

import React, { useMemo, useState, useEffect, useCallback, useRef } from 'react';
import {
    Sheet,
    SheetContent,
    SheetHeader,
    SheetTitle,
    SheetDescription,
} from "@/components/ui/sheet";
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { apiClient, StockEventsResponse } from '@/lib/api';
import { Loader2, Calendar, AlertTriangle } from 'lucide-react';

interface StockDetailDrawerProps {
    stockCode: string | null;
    groupId?: number | string;
    onClose: () => void;
}

interface StockEventDetail {
    mention_id?: number;
    topic_id?: number | string;
    group_id?: number | string;
    group_name?: string;
    mention_date?: string;
    mention_time?: string;
    price_at_mention?: number;
    context_snippet?: string;
    full_text?: string;
    text_snippet?: string;
    stocks?: Array<{ stock_code: string; stock_name: string }>;
    return_1d?: number | null;
    return_3d?: number | null;
    return_5d?: number | null;
    return_10d?: number | null;
    return_20d?: number | null;
    t0_buy_price?: number | null;
    t0_buy_ts?: string;
    t0_buy_source?: string;
    t0_end_price_rt?: number | null;
    t0_end_price_rt_ts?: string;
    t0_end_price_close?: number | null;
    t0_end_price_close_ts?: string;
    t0_return_rt?: number | null;
    t0_return_close?: number | null;
    t0_status?: string;
    t0_note?: string;
    t0_session_trade_date?: string;
    t0_window_tag?: string;
}

interface T0BoardRow {
    mention_id?: number | null;
    topic_id?: number | string | null;
    group_id?: number | string | null;
    group_name?: string;
    mention_time?: string;
    ret?: number | null;
}

interface T0BoardView {
    view_key: string;
    label: string;
    trade_date: string;
    window_start: string;
    window_end: string;
    status: string;
    has_data: boolean;
    current_return?: number | null;
    max_return?: number | null;
    max_event?: {
        mention_id?: number | null;
        topic_id?: number | string | null;
        group_id?: number | string | null;
        group_name?: string | null;
    } | null;
    rows: T0BoardRow[];
}

interface T0Board {
    as_of?: string;
    close_finalize_time?: string;
    open_time?: string;
    base_trade_date?: string;
    next_trade_date?: string;
    views?: T0BoardView[];
}

interface StockDetailsData {
    stock_name?: string;
    total_mentions?: number;
    win_rate_5d?: number;
    avg_return_5d?: number;
    t0_finalized?: boolean;
    t0_data_mode?: string;
    snapshot_ts?: string | null;
    snapshot_is_final?: number | null;
    refresh_source?: string;
    refresh_state?: string;
    next_refresh_allowed_at?: string | null;
    provider_path?: string[];
    t0_board?: T0Board;
    events?: StockEventDetail[];
}

const todayStr = () => new Date().toLocaleDateString('sv-SE');

export default function StockDetailDrawer({ stockCode, groupId, onClose }: StockDetailDrawerProps) {
    const [loading, setLoading] = useState(false);
    const [details, setDetails] = useState<StockDetailsData | null>(null);
    const [error, setError] = useState<string | null>(null);
    const [expandedEvents, setExpandedEvents] = useState<Set<number>>(new Set());
    const [refreshingRealtime, setRefreshingRealtime] = useState(false);
    const [lastRealtimeRefreshAt, setLastRealtimeRefreshAt] = useState(0);
    const [highlightEventKey, setHighlightEventKey] = useState('');
    const [activeViewKey, setActiveViewKey] = useState('d_view');
    const [hoveredTrendIdx, setHoveredTrendIdx] = useState<number | null>(null);
    const eventRefs = useRef<Record<string, HTMLDivElement | null>>({});
    const requestSeqRef = useRef(0);

    const fetchDetails = useCallback(async (opts?: {
        silent?: boolean;
        refreshRealtime?: boolean;
        detailMode?: 'fast' | 'full';
        page?: number;
        perPage?: number;
        includeFullText?: boolean;
    }) => {
        if (!stockCode) return null;
        const refreshRealtime = !!opts?.refreshRealtime;
        const silent = !!opts?.silent;
        const reqSeq = ++requestSeqRef.current;

        if (!silent) {
            setLoading(true);
            setError(null);
        }
        try {
            const rawData: StockEventsResponse = groupId
                ? await apiClient.getStockEvents(groupId, stockCode, {
                    refreshRealtime,
                    detailMode: opts?.detailMode || 'fast',
                    page: opts?.page ?? 1,
                    perPage: opts?.perPage ?? 50,
                    includeFullText: opts?.includeFullText ?? false,
                })
                : await apiClient.getGlobalStockEvents(stockCode, {
                    refreshRealtime,
                    detailMode: opts?.detailMode || 'fast',
                    page: opts?.page ?? 1,
                    perPage: opts?.perPage ?? 50,
                    includeFullText: opts?.includeFullText ?? false,
                });

            const rawEvents = Array.isArray(rawData?.events) ? rawData.events : [];
            const events: StockEventDetail[] = rawEvents.map((evt) => ({
                mention_id: typeof evt.mention_id === 'number' ? evt.mention_id : undefined,
                topic_id: evt.topic_id,
                group_id: evt.group_id ?? groupId,
                group_name: evt.group_name,
                mention_date: typeof evt.mention_date === 'string' ? evt.mention_date : undefined,
                mention_time: typeof evt.mention_time === 'string'
                    ? evt.mention_time
                    : (typeof evt.mention_date === 'string' ? evt.mention_date : undefined),
                price_at_mention: typeof evt.price_at_mention === 'number' ? evt.price_at_mention : undefined,
                context_snippet:
                    (typeof evt.context_snippet === 'string' && evt.context_snippet) ||
                    (typeof evt.context === 'string' && evt.context) ||
                    '',
                full_text: typeof evt.full_text === 'string' ? evt.full_text : undefined,
                text_snippet: typeof evt.text_snippet === 'string' ? evt.text_snippet : undefined,
                stocks: Array.isArray(evt.stocks) ? evt.stocks : [],
                return_1d: typeof evt.return_1d === 'number' ? evt.return_1d : null,
                return_3d: typeof evt.return_3d === 'number' ? evt.return_3d : null,
                return_5d: typeof evt.return_5d === 'number' ? evt.return_5d : null,
                return_10d: typeof evt.return_10d === 'number' ? evt.return_10d : null,
                return_20d: typeof evt.return_20d === 'number' ? evt.return_20d : null,
                t0_buy_price: typeof evt.t0_buy_price === 'number' ? evt.t0_buy_price : null,
                t0_buy_ts: typeof evt.t0_buy_ts === 'string' ? evt.t0_buy_ts : '',
                t0_buy_source: typeof evt.t0_buy_source === 'string' ? evt.t0_buy_source : '',
                t0_end_price_rt: typeof evt.t0_end_price_rt === 'number' ? evt.t0_end_price_rt : null,
                t0_end_price_rt_ts: typeof evt.t0_end_price_rt_ts === 'string' ? evt.t0_end_price_rt_ts : '',
                t0_end_price_close: typeof evt.t0_end_price_close === 'number' ? evt.t0_end_price_close : null,
                t0_end_price_close_ts: typeof evt.t0_end_price_close_ts === 'string' ? evt.t0_end_price_close_ts : '',
                t0_return_rt: typeof evt.t0_return_rt === 'number' ? evt.t0_return_rt : null,
                t0_return_close: typeof evt.t0_return_close === 'number' ? evt.t0_return_close : null,
                t0_status: typeof evt.t0_status === 'string' ? evt.t0_status : '',
                t0_note: typeof evt.t0_note === 'string' ? evt.t0_note : '',
                t0_session_trade_date: typeof evt.t0_session_trade_date === 'string' ? evt.t0_session_trade_date : '',
                t0_window_tag: typeof evt.t0_window_tag === 'string' ? evt.t0_window_tag : '',
            }));

            const valid5d = events
                .map((e) => e.return_5d)
                .filter((v): v is number => typeof v === 'number');
            const computedWinRate = valid5d.length > 0
                ? Math.round((valid5d.filter((v) => v > 0).length / valid5d.length) * 1000) / 10
                : undefined;
            const computedAvgReturn = valid5d.length > 0
                ? Math.round((valid5d.reduce((s, v) => s + v, 0) / valid5d.length) * 100) / 100
                : undefined;

            if (reqSeq !== requestSeqRef.current) return null;
            setDetails({
                stock_name: rawData?.stock_name,
                total_mentions: rawData?.total_mentions ?? events.length,
                win_rate_5d: typeof rawData?.win_rate_5d === 'number' ? rawData.win_rate_5d : computedWinRate,
                avg_return_5d: typeof rawData?.avg_return_5d === 'number' ? rawData.avg_return_5d : computedAvgReturn,
                t0_finalized: !!rawData?.t0_finalized,
                t0_data_mode: typeof rawData?.t0_data_mode === 'string' ? rawData.t0_data_mode : undefined,
                snapshot_ts: typeof rawData?.snapshot_ts === 'string' ? rawData.snapshot_ts : null,
                snapshot_is_final: typeof rawData?.snapshot_is_final === 'number' ? rawData.snapshot_is_final : null,
                refresh_source: typeof rawData?.refresh_source === 'string' ? rawData.refresh_source : undefined,
                refresh_state: typeof rawData?.refresh_state === 'string' ? rawData.refresh_state : undefined,
                next_refresh_allowed_at: typeof rawData?.next_refresh_allowed_at === 'string' ? rawData.next_refresh_allowed_at : null,
                provider_path: Array.isArray(rawData?.provider_path) ? rawData.provider_path : [],
                t0_board: rawData?.t0_board,
                events,
            });
            if (refreshRealtime) setLastRealtimeRefreshAt(Date.now());
            return rawData;
        } catch (err) {
            if (reqSeq !== requestSeqRef.current) return null;
            console.error('Failed to fetch stock details', err);
            setError('获取详情失败，请稍后重试');
            return null;
        } finally {
            if (reqSeq !== requestSeqRef.current) return;
            if (!silent) setLoading(false);
        }
    }, [groupId, stockCode]);

    const handleRealtimeRefresh = useCallback(async () => {
        if (!stockCode) return;
        const now = Date.now();
        if (now - lastRealtimeRefreshAt < 10000) return;
        setRefreshingRealtime(true);
        setLastRealtimeRefreshAt(now);
        try {
            if (groupId) {
                await apiClient.triggerStockEventsRefresh(groupId, stockCode);
            } else {
                await apiClient.triggerGlobalStockEventsRefresh(stockCode);
            }
            for (let i = 0; i < 20; i++) {
                const payload = await fetchDetails({ silent: true, detailMode: 'fast', page: 1, perPage: 50, includeFullText: false });
                const state = String(payload?.refresh_state || '').toLowerCase();
                if (!state || state === 'completed' || state === 'failed' || state === 'idle' || state === 'cooldown') {
                    break;
                }
                await new Promise((resolve) => setTimeout(resolve, 800));
            }
        } catch (err) {
            console.error('Failed to trigger stock realtime refresh', err);
            setError('触发实时刷新失败，请稍后重试');
        } finally {
            setRefreshingRealtime(false);
        }
    }, [stockCode, lastRealtimeRefreshAt, groupId, fetchDetails]);

    useEffect(() => {
        if (!stockCode) return;
        setDetails(null);
        setError(null);
        setExpandedEvents(new Set());
        setHoveredTrendIdx(null);
        setHighlightEventKey('');
        void fetchDetails({ detailMode: 'fast', page: 1, perPage: 50, includeFullText: false });
    }, [stockCode, groupId, fetchDetails]);

    const handleOpenChange = (open: boolean) => {
        if (!open) onClose();
    };

    const fmtP = (val: number | null | undefined) => {
        if (val == null) return null;
        return `${val > 0 ? '+' : ''}${val.toFixed(2)}%`;
    };

    const toggleEventExpand = (idx: number) => {
        setExpandedEvents(prev => {
            const next = new Set(prev);
            if (next.has(idx)) next.delete(idx);
            else next.add(idx);
            return next;
        });
    };

    const eventKey = useCallback((event: StockEventDetail, idx: number) => {
        return `${event.mention_id ?? event.topic_id ?? idx}`;
    }, []);

    const jumpToEvent = useCallback((event: StockEventDetail | null) => {
        if (!event || !details?.events) return;
        const idx = details.events.indexOf(event);
        if (idx < 0) return;
        const key = eventKey(event, idx);
        setHighlightEventKey(key);
        const el = eventRefs.current[key];
        if (el) {
            el.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
        window.setTimeout(() => {
            setHighlightEventKey((prev) => (prev === key ? '' : prev));
        }, 2200);
    }, [details?.events, eventKey]);

    const latestMention = useMemo(() => details?.events?.[0]?.mention_date || '—', [details?.events]);

    const t0Views = useMemo(() => {
        const views = details?.t0_board?.views;
        if (Array.isArray(views) && views.length > 0) {
            const order = ['d_minus_1_view', 'd_view', 'd_plus_1_preview'];
            return [...views].sort((a, b) => order.indexOf(a.view_key) - order.indexOf(b.view_key));
        }

        const events = (details?.events || []).filter((e) => typeof e.t0_return_rt === 'number' || typeof e.t0_return_close === 'number');
        const parseDt = (e: StockEventDetail): Date | null => {
            const raw = String(e.mention_time || '').replace('T', ' ');
            const text = raw || `${String(e.mention_date || '')} 00:00:00`;
            const dt = new Date(text);
            return Number.isNaN(dt.getTime()) ? null : dt;
        };
        const pickRet = (e: StockEventDetail): number | null => {
            if (typeof e.t0_return_rt === 'number') return e.t0_return_rt;
            if (typeof e.t0_return_close === 'number') return e.t0_return_close;
            return null;
        };
        const today = new Date(`${todayStr()}T00:00:00`);
        const weekday = today.getDay();
        const prevDayOffset = weekday === 1 ? 3 : (weekday === 0 ? 2 : 1);
        const nextDayOffset = weekday === 5 ? 3 : (weekday === 6 ? 2 : 1);
        const prevTrade = new Date(today.getTime() - prevDayOffset * 24 * 3600 * 1000);
        const prevPrevTrade = new Date(prevTrade.getTime() - (prevTrade.getDay() === 1 ? 3 : (prevTrade.getDay() === 0 ? 2 : 1)) * 24 * 3600 * 1000);
        const nextTrade = new Date(today.getTime() + nextDayOffset * 24 * 3600 * 1000);
        const setHm = (d: Date, h: number, m: number) => {
            const x = new Date(d);
            x.setHours(h, m, 0, 0);
            return x;
        };
        const fmtDate = (d: Date) => d.toLocaleDateString('sv-SE');
        const fmtDt = (d: Date) => `${d.toLocaleDateString('sv-SE')} ${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
        const dMinusWindowStart = setHm(prevPrevTrade, 15, 5);
        const dMinusWindowEnd = setHm(prevTrade, 9, 30);
        const dWindowStart = setHm(prevTrade, 15, 5);
        const dWindowEnd = setHm(today, 9, 30);
        const nWindowStart = setHm(today, 15, 5);
        const nWindowEnd = setHm(nextTrade, 9, 30);

        const inWindow = (e: StockEventDetail, start: Date, end: Date) => {
            const dt = parseDt(e);
            return !!dt && dt >= start && dt < end;
        };
        const dMinusPool = events.filter((e) => inWindow(e, dMinusWindowStart, dMinusWindowEnd)).sort((a, b) => String(b.mention_time || '').localeCompare(String(a.mention_time || '')));
        const dPool = events.filter((e) => inWindow(e, dWindowStart, dWindowEnd)).sort((a, b) => String(b.mention_time || '').localeCompare(String(a.mention_time || '')));
        const nPool = events.filter((e) => inWindow(e, nWindowStart, nWindowEnd)).sort((a, b) => String(b.mention_time || '').localeCompare(String(a.mention_time || '')));

        const buildView = (viewKey: string, label: string, tradeDate: string, status: string, pool: StockEventDetail[], windowStart: Date, windowEnd: Date): T0BoardView => {
            let maxRet: number | null = null;
            let maxEvent: StockEventDetail | null = null;
            let maxTime = '';
            for (const e of pool) {
                const ret = pickRet(e);
                if (ret == null) continue;
                const curTime = String(e.mention_time || e.mention_date || '');
                if (maxRet == null || ret > maxRet || (ret === maxRet && (!maxTime || curTime < maxTime))) {
                    maxRet = ret;
                    maxEvent = e;
                    maxTime = curTime;
                }
            }
            const current = pool[0];
            return {
                view_key: viewKey,
                label,
                trade_date: tradeDate,
                window_start: fmtDt(windowStart),
                window_end: fmtDt(windowEnd),
                status,
                has_data: pool.length > 0,
                current_return: current ? pickRet(current) : null,
                max_return: maxRet,
                max_event: maxEvent ? {
                    mention_id: maxEvent.mention_id ?? null,
                    topic_id: maxEvent.topic_id ?? null,
                    group_id: maxEvent.group_id ?? null,
                    group_name: maxEvent.group_name ?? null,
                } : null,
                rows: pool.map((e) => ({
                    mention_id: e.mention_id ?? null,
                    topic_id: e.topic_id ?? null,
                    group_id: e.group_id ?? null,
                    group_name: e.group_name || (e.group_id != null ? `群组${String(e.group_id)}` : '未知群组'),
                    mention_time: String(e.mention_time || '').replace('T', ' ').slice(0, 16) || `${String(e.mention_date || '')} --:--`,
                    ret: pickRet(e),
                })),
            };
        };

        return [
            buildView('d_minus_1_view', `${fmtDate(prevTrade)} 视角`, fmtDate(prevTrade), 'finalized', dMinusPool, dMinusWindowStart, dMinusWindowEnd),
            buildView('d_view', `${fmtDate(today)} 视角`, fmtDate(today), details?.t0_finalized ? 'finalized' : 'realtime', dPool, dWindowStart, dWindowEnd),
            buildView('d_plus_1_preview', `${fmtDate(nextTrade)} 视角（提前看）`, fmtDate(nextTrade), 'preview', nPool, nWindowStart, nWindowEnd),
        ];
    }, [details?.t0_board?.views, details?.events, details?.t0_finalized]);

    useEffect(() => {
        if (t0Views.length === 0) {
            setActiveViewKey('d_view');
            return;
        }
        const hasActive = t0Views.some((v) => v.view_key === activeViewKey);
        if (!hasActive) setActiveViewKey(t0Views[0].view_key);
    }, [t0Views, activeViewKey]);

    const activeView = useMemo(
        () => t0Views.find((v) => v.view_key === activeViewKey) || t0Views[0] || null,
        [t0Views, activeViewKey],
    );

    const t0TrendChart = useMemo(() => {
        const rows = activeView?.rows || [];
        const orderedAll = [...rows].sort((a, b) => String(a.mention_time || '').localeCompare(String(b.mention_time || '')));
        const events = details?.events || [];
        const asOf = String(details?.t0_board?.as_of || '').replace('T', ' ').slice(0, 16);
        const normTime = (v: string | undefined | null) => String(v || '--').replace('T', ' ').slice(0, 16);
        const findEvent = (row: T0BoardRow): StockEventDetail | null => {
            return events.find((e) => {
                const sameMention = row.mention_id != null && e.mention_id != null && Number(e.mention_id) === Number(row.mention_id);
                const sameTopic = row.topic_id != null && e.topic_id != null && String(e.topic_id) === String(row.topic_id);
                const sameGroup = row.group_id == null || e.group_id == null || String(e.group_id) === String(row.group_id);
                const sameTime = !!row.mention_time && !!e.mention_time && normTime(e.mention_time) === normTime(row.mention_time);
                return (sameMention || sameTopic || sameTime) && sameGroup;
            }) || null;
        };
        if (orderedAll.length === 0) {
            return {
                empty: true,
                rowCount: 0,
                firstTime: '--',
                lastTime: '--',
            } as const;
        }

        const enriched = orderedAll.map((row) => {
            const evt = findEvent(row);
            const ret = typeof row.ret === 'number'
                ? row.ret
                : (typeof evt?.t0_return_rt === 'number' ? evt.t0_return_rt : (typeof evt?.t0_return_close === 'number' ? evt.t0_return_close : null));
            const currentPriceRaw = typeof evt?.t0_end_price_rt === 'number'
                ? evt.t0_end_price_rt
                : (typeof evt?.t0_end_price_close === 'number' ? evt.t0_end_price_close : null);
            const mentionPriceRaw = typeof evt?.t0_buy_price === 'number'
                ? evt.t0_buy_price
                : (typeof evt?.price_at_mention === 'number' ? evt.price_at_mention : null);
            let mentionPrice: number | null = mentionPriceRaw;
            let currentPrice: number | null = currentPriceRaw;
            if (mentionPrice == null && currentPrice != null && typeof ret === 'number') {
                const denom = 1 + ret / 100;
                if (denom !== 0) mentionPrice = currentPrice / denom;
            }
            if (currentPrice == null && mentionPrice != null && typeof ret === 'number') {
                currentPrice = mentionPrice * (1 + ret / 100);
            }
            const currentTime = normTime(
                evt?.t0_end_price_rt_ts
                || evt?.t0_end_price_close_ts
                || asOf
                || evt?.mention_time
                || evt?.mention_date
                || '--'
            );
            return {
                mentionTime: normTime(row.mention_time),
                ret,
                mentionPrice,
                currentPrice,
                currentTime,
            };
        });

        const numericVals = enriched
            .map((p) => p.ret)
            .filter((v): v is number => typeof v === 'number');

        if (numericVals.length === 0) {
            if (orderedAll.length === 0) {
                return {
                    empty: true,
                    rowCount: 0,
                    firstTime: '--',
                    lastTime: '--',
                } as const;
            }
            const width = 560;
            const height = 190;
            const pl = 28;
            const pr = 12;
            const pt = 14;
            const pb = 26;
            const innerW = Math.max(1, width - pl - pr);
            const baseY = pt + (height - pt - pb) * 0.5;
            const xAt = (idx: number) => (orderedAll.length === 1 ? pl + innerW / 2 : pl + (idx / (orderedAll.length - 1)) * innerW);
            const points = enriched.map((row, idx) => ({
                x: xAt(idx),
                y: baseY,
                ret: row.ret,
                mentionTime: row.mentionTime,
                mentionPrice: row.mentionPrice,
                currentPrice: row.currentPrice,
                currentTime: row.currentTime,
            }));
            const pathD = points.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(2)},${p.y.toFixed(2)}`).join(' ');
            return {
                empty: false,
                timelineOnly: true,
                rowCount: orderedAll.length,
                firstTime: points[0]?.mentionTime || '--',
                lastTime: points[points.length - 1]?.mentionTime || '--',
                width,
                height,
                pl,
                pr,
                pt,
                pb,
                min: 0,
                max: 0,
                points,
                pathD,
            } as const;
        }

        const values = numericVals;
        const rawMin = Math.min(...values);
        const rawMax = Math.max(...values);
        const pad = rawMax === rawMin ? 1 : Math.max(0.5, (rawMax - rawMin) * 0.15);
        const min = rawMin - pad;
        const max = rawMax + pad;

        const width = 560;
        const height = 190;
        const pl = 28;
        const pr = 12;
        const pt = 14;
        const pb = 26;
        const innerW = Math.max(1, width - pl - pr);
        const innerH = Math.max(1, height - pt - pb);
        const xAt = (idx: number) => (enriched.length === 1 ? pl + innerW / 2 : pl + (idx / (enriched.length - 1)) * innerW);
        const yAt = (val: number) => pt + ((max - val) / (max - min)) * innerH;
        const zeroY = yAt(0);
        const points = enriched.map((row, idx) => ({
            x: xAt(idx),
            y: typeof row.ret === 'number' ? yAt(row.ret) : zeroY,
            ret: row.ret,
            mentionTime: row.mentionTime,
            mentionPrice: row.mentionPrice,
            currentPrice: row.currentPrice,
            currentTime: row.currentTime,
        }));
        const pathD = points.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(2)},${p.y.toFixed(2)}`).join(' ');
        const firstTime = points[0]?.mentionTime || '--';
        const lastTime = points[points.length - 1]?.mentionTime || '--';

        return { empty: false, timelineOnly: false, rowCount: orderedAll.length, width, height, pl, pr, pt, pb, min, max, points, pathD, firstTime, lastTime } as const;
    }, [activeView, details?.events, details?.t0_board?.as_of]);

    const selectedTrendPoint = useMemo(() => {
        if (!t0TrendChart || t0TrendChart.empty || t0TrendChart.points.length === 0) return null;
        const idx = hoveredTrendIdx != null ? hoveredTrendIdx : (t0TrendChart.points.length - 1);
        return t0TrendChart.points[Math.max(0, Math.min(idx, t0TrendChart.points.length - 1))] || null;
    }, [t0TrendChart, hoveredTrendIdx]);

    const findEventByRef = useCallback((row: T0BoardRow) => {
        const events = details?.events || [];
        return events.find((e) => {
            const sameMention = row.mention_id != null && e.mention_id != null && Number(e.mention_id) === Number(row.mention_id);
            const sameTopic = row.topic_id != null && e.topic_id != null && String(e.topic_id) === String(row.topic_id);
            const sameGroup = row.group_id == null || e.group_id == null || String(e.group_id) === String(row.group_id);
            return (sameMention || sameTopic) && sameGroup;
        }) || null;
    }, [details?.events]);

    if (!stockCode) return null;

    return (
        <Sheet open={!!stockCode} onOpenChange={handleOpenChange}>
            <SheetContent
                side="right"
                className="!max-w-none w-[100vw] sm:w-[85vw] md:w-[70vw] lg:w-[60vw] xl:w-[50vw] flex flex-col p-6 h-full overflow-hidden shadow-2xl"
                onOpenAutoFocus={(e) => e.preventDefault()}
            >
                <SheetHeader className="pb-4 border-b shrink-0">
                    <div className="flex items-center justify-between mr-8 gap-2">
                        <SheetTitle className="text-xl flex items-center gap-2">
                            {details?.stock_name || stockCode}
                            <span className="text-sm font-normal text-muted-foreground bg-muted px-2 py-0.5 rounded">
                                {stockCode}
                            </span>
                        </SheetTitle>
                        <Button
                            size="sm"
                            variant="outline"
                            className="h-7 text-xs"
                            onClick={() => void handleRealtimeRefresh()}
                            disabled={loading || refreshingRealtime}
                            title="手动刷新T+0（10秒防抖）"
                        >
                            {refreshingRealtime ? '刷新中...' : '手动刷新T+0'}
                        </Button>
                    </div>
                    <SheetDescription>
                        {loading ? '正在加载数据...' : `共 ${details?.total_mentions || 0} 次提及 · 5日胜率 ${details?.win_rate_5d ? details.win_rate_5d + '%' : '—'}`}
                    </SheetDescription>
                </SheetHeader>

                <div className="flex-1 overflow-hidden relative mt-4">
                    {loading ? (
                        <div className="flex flex-col items-center justify-center h-full py-12">
                            <Loader2 className="h-8 w-8 animate-spin text-primary/50 mb-2" />
                            <span className="text-sm text-muted-foreground">加载详细数据...</span>
                        </div>
                    ) : error ? (
                        <div className="flex flex-col items-center justify-center h-full text-destructive">
                            <AlertTriangle className="h-8 w-8 mb-2" />
                            <span>{error}</span>
                        </div>
                    ) : (
                        <div className="h-full overflow-y-auto pr-2 pb-10">
                            <div className="space-y-6">
                                <div className="grid grid-cols-2 gap-3 mb-4">
                                    <div className="p-3 bg-muted/30 rounded-lg border flex flex-col justify-between overflow-hidden">
                                        <div className="text-xs text-muted-foreground mb-1 whitespace-nowrap">提及次数</div>
                                        <div className="text-lg lg:text-xl font-bold break-all">{details?.total_mentions}</div>
                                    </div>
                                    <div className="p-3 bg-muted/30 rounded-lg border flex flex-col justify-between overflow-hidden">
                                        <div className="text-xs text-muted-foreground mb-1 whitespace-nowrap">平均5日收益</div>
                                        <div className={`text-lg lg:text-xl font-bold font-mono tracking-tighter leading-tight mt-1 break-all ${(details?.avg_return_5d || 0) > 0 ? 'text-emerald-500' : 'text-red-500'}`}>
                                            {details?.avg_return_5d ? `${details.avg_return_5d > 0 ? '+' : ''}${details.avg_return_5d}%` : '—'}
                                        </div>
                                    </div>
                                    <div className="p-3 bg-muted/30 rounded-lg border flex flex-col justify-between overflow-hidden">
                                        <div className="text-xs text-muted-foreground mb-1 whitespace-nowrap">5日胜率</div>
                                        <div className={`text-lg lg:text-xl font-bold font-mono tracking-tighter leading-tight mt-1 break-all ${(details?.win_rate_5d || 0) > 50 ? 'text-emerald-500' : ''}`}>
                                            {details?.win_rate_5d ? `${details.win_rate_5d}%` : '—'}
                                        </div>
                                        <div className="text-[10px] text-muted-foreground mt-1">仅统计已固化交易日</div>
                                    </div>
                                    <div className="p-3 bg-muted/30 rounded-lg border flex flex-col justify-between overflow-hidden">
                                        <div className="text-xs text-muted-foreground mb-1 whitespace-nowrap">最新提及</div>
                                        <div className="text-sm font-medium mt-1 break-words leading-tight">{latestMention}</div>
                                    </div>
                                </div>

                                <div className="rounded-xl border border-blue-200 bg-gradient-to-br from-blue-50 via-sky-50 to-white p-4 shadow-sm">
                                    <div className="flex items-center justify-between gap-2">
                                        <div className="text-sm font-semibold text-blue-900">T+0 今日效果看板</div>
                                        <Badge variant="outline" className="text-[11px] border-blue-300 text-blue-700">
                                            {details?.t0_finalized ? '已收盘固化' : '盘中实时'}
                                        </Badge>
                                    </div>

                                    {t0Views.length > 0 && (
                                        <Tabs value={activeView?.view_key || ''} onValueChange={setActiveViewKey} className="mt-3">
                                            <TabsList className={`grid w-full h-8 bg-blue-100/60 ${t0Views.length >= 3 ? 'grid-cols-3' : 'grid-cols-2'}`}>
                                                {t0Views.slice(0, 3).map((view) => (
                                                    <TabsTrigger key={view.view_key} value={view.view_key} className="text-xs">
                                                        {view.label}
                                                    </TabsTrigger>
                                                ))}
                                            </TabsList>
                                        </Tabs>
                                    )}
                                    {activeView && (
                                        <div className="text-[11px] text-blue-800/80 mt-2">
                                            交易段窗口：{activeView.window_start || '--'} ~ {activeView.window_end || '--'}
                                        </div>
                                    )}
                                    <div className="text-[10px] text-blue-900/80 mt-1 space-y-0.5">
                                        <div>盘前：待开盘价回填；盘中：本地快照口径；盘后：收盘固化口径</div>
                                        <div>快照时间：{details?.snapshot_ts || '—'} · 快照状态：{details?.snapshot_is_final === 1 ? '已固化' : '未固化'}</div>
                                    </div>

                                    {activeView && (
                                        <div className="rounded border bg-white/75 p-2 mt-3">
                                            <div className="text-[11px] text-muted-foreground mb-1">提及时间-当前涨幅线图</div>
                                            <div className="text-[10px] text-slate-500 mb-2 flex items-center justify-between">
                                                <span>纵轴：当前涨幅（%）</span>
                                                <span>横轴：提及时间（HH:mm）</span>
                                            </div>
                                            {selectedTrendPoint && !t0TrendChart?.empty && (
                                                <div className="grid grid-cols-1 md:grid-cols-2 gap-2 mb-2 text-[11px]">
                                                    <div className="rounded border bg-slate-50 p-2">
                                                        <div className="text-slate-600 mb-1">提及时刻</div>
                                                        <div className="font-mono">{selectedTrendPoint.mentionTime || '--'}</div>
                                                        <div className="font-mono">价格：{selectedTrendPoint.mentionPrice != null ? selectedTrendPoint.mentionPrice.toFixed(3) : '待回补'}</div>
                                                        <div className="font-mono">当时涨幅：+0.00%</div>
                                                    </div>
                                                    <div className="rounded border bg-sky-50 p-2">
                                                        <div className="text-sky-700 mb-1">当前时刻</div>
                                                        <div className="font-mono">{selectedTrendPoint.currentTime || '待回补'}</div>
                                                        <div className="font-mono">价格：{selectedTrendPoint.currentPrice != null ? selectedTrendPoint.currentPrice.toFixed(3) : '待回补'}</div>
                                                        <div className="font-mono">当前涨幅：{selectedTrendPoint.ret == null ? '待回补' : (fmtP(selectedTrendPoint.ret) || '待回补')}</div>
                                                    </div>
                                                </div>
                                            )}
                                            {t0TrendChart && !t0TrendChart.empty ? (
                                                <>
                                                    <svg
                                                        viewBox={`0 0 ${t0TrendChart.width} ${t0TrendChart.height}`}
                                                        className="w-full h-40"
                                                        onMouseLeave={() => setHoveredTrendIdx(null)}
                                                    >
                                                        <line
                                                            x1={t0TrendChart.pl}
                                                            y1={t0TrendChart.pt}
                                                            x2={t0TrendChart.pl}
                                                            y2={t0TrendChart.height - t0TrendChart.pb}
                                                            stroke="#cbd5e1"
                                                            strokeWidth="1"
                                                        />
                                                        <line
                                                            x1={t0TrendChart.pl}
                                                            y1={t0TrendChart.height - t0TrendChart.pb}
                                                            x2={t0TrendChart.width - t0TrendChart.pr}
                                                            y2={t0TrendChart.height - t0TrendChart.pb}
                                                            stroke="#cbd5e1"
                                                            strokeWidth="1"
                                                        />
                                                        <path
                                                            d={t0TrendChart.pathD}
                                                            fill="none"
                                                            stroke={t0TrendChart.timelineOnly ? '#94a3b8' : '#0284c7'}
                                                            strokeWidth="2.2"
                                                            strokeDasharray={t0TrendChart.timelineOnly ? '4 3' : undefined}
                                                        />
                                                        {t0TrendChart.points.map((p, idx) => (
                                                            <circle
                                                                key={`${p.mentionTime}-${idx}`}
                                                                cx={p.x}
                                                                cy={p.y}
                                                                r={hoveredTrendIdx === idx ? "4.5" : "3"}
                                                                fill={p.ret == null ? '#94a3b8' : (p.ret >= 0 ? '#059669' : '#dc2626')}
                                                                onMouseEnter={() => setHoveredTrendIdx(idx)}
                                                            >
                                                                <title>{`${p.mentionTime}  ${p.ret == null ? '待回补' : (fmtP(p.ret) || '-')}`}</title>
                                                            </circle>
                                                        ))}
                                                        {!t0TrendChart.timelineOnly && (
                                                            <>
                                                                <text x={t0TrendChart.width - t0TrendChart.pr - 2} y={t0TrendChart.pt - 2} textAnchor="end" fontSize="10" fill="#64748b">
                                                                    单位: %
                                                                </text>
                                                                <text x={t0TrendChart.pl} y={t0TrendChart.pt - 2} fontSize="10" fill="#64748b">
                                                                    {fmtP(t0TrendChart.max)}
                                                                </text>
                                                                <text x={t0TrendChart.pl} y={t0TrendChart.height - 6} fontSize="10" fill="#64748b">
                                                                    {fmtP(t0TrendChart.min)}
                                                                </text>
                                                            </>
                                                        )}
                                                    </svg>
                                                    {t0TrendChart.timelineOnly && (
                                                        <div className="text-[10px] text-slate-500 mt-1">当前涨幅待回补，先展示提及时间分布</div>
                                                    )}
                                                    <div className="flex items-center justify-between text-[10px] text-muted-foreground">
                                                        <span>{t0TrendChart.firstTime}</span>
                                                        <span>{t0TrendChart.lastTime}</span>
                                                    </div>
                                                </>
                                            ) : (
                                                <div className="h-40 rounded border border-dashed bg-slate-50 flex items-center justify-center text-xs text-muted-foreground">
                                                    {t0TrendChart?.rowCount
                                                        ? '该视角有提及时间，但暂无可计算的实时涨幅点'
                                                        : '该视角暂无提及数据'}
                                                </div>
                                            )}
                                        </div>
                                    )}

                                    {!activeView || !activeView.has_data ? (
                                        <div className="text-xs text-muted-foreground mt-2">暂无可用 T+0 数据</div>
                                    ) : (
                                        <div className="space-y-3 mt-3">
                                            <div>
                                                <div className="text-muted-foreground text-xs mb-1">T+0 逐条明细（时间-群组-涨幅）</div>
                                                <div className="max-h-44 overflow-y-auto rounded border bg-white/70 divide-y">
                                                    {activeView.rows.length === 0 ? (
                                                        <div className="p-2 text-xs text-muted-foreground">—</div>
                                                    ) : activeView.rows.map((row, idx) => (
                                                        <div key={`${row.mention_time}-${row.group_name}-${idx}`} className="p-2 flex items-center justify-between gap-2 text-xs">
                                                            <div className="flex items-center gap-2 min-w-0">
                                                                <span className="font-mono text-muted-foreground">{row.mention_time || '--'}</span>
                                                                <span className="truncate">{row.group_name || '未知群组'}</span>
                                                                <span className={`font-mono ${((row.ret || 0) >= 0) ? 'text-emerald-600' : 'text-red-600'}`}>
                                                                    {fmtP(row.ret) || '—'}
                                                                </span>
                                                            </div>
                                                            <Button
                                                                size="sm"
                                                                variant="outline"
                                                                className="h-6 px-2 text-[11px] whitespace-nowrap"
                                                                onClick={() => jumpToEvent(findEventByRef(row))}
                                                            >
                                                                定位
                                                            </Button>
                                                        </div>
                                                    ))}
                                                </div>
                                            </div>

                                            <div className="text-xs bg-white/80 border rounded p-2 flex items-center justify-between gap-2">
                                                <div>
                                                <span className="text-muted-foreground">T+0 最高涨幅：</span>
                                                <span className={`font-mono font-semibold ${(activeView.max_return || 0) >= 0 ? 'text-emerald-600' : 'text-red-600'}`}>
                                                    {fmtP(activeView.max_return) || '—'}
                                                </span>
                                                {activeView.max_event && (
                                                    <span className="text-muted-foreground">
                                                        {' '}· {activeView.max_event.group_name || (activeView.max_event.group_id != null ? `群组${String(activeView.max_event.group_id)}` : '未知群组')}
                                                        {' '}· 帖子 {String(activeView.max_event.topic_id || '-')}
                                                    </span>
                                                )}
                                                </div>
                                                {activeView.max_event && (
                                                    <Button
                                                        size="sm"
                                                        variant="outline"
                                                        className="h-6 px-2 text-[11px] whitespace-nowrap"
                                                        onClick={() => jumpToEvent(findEventByRef({
                                                            mention_id: activeView.max_event?.mention_id ?? null,
                                                            topic_id: activeView.max_event?.topic_id ?? null,
                                                            group_id: activeView.max_event?.group_id ?? null,
                                                        }))}
                                                    >
                                                        定位到该帖子
                                                    </Button>
                                                )}
                                            </div>
                                        </div>
                                    )}
                                </div>

                                <div>
                                    <h3 className="text-sm font-semibold mb-3 flex items-center gap-2 sticky top-0 bg-background z-10 py-2">
                                        <Calendar className="h-4 w-4" /> 提及事件时间轴
                                    </h3>
                                    <div className="space-y-4 pl-2">
                                        {details?.events?.map((event: StockEventDetail, idx: number) => {
                                            const gid = String(event.group_id ?? groupId ?? '');
                                            const displayText = event.full_text || event.text_snippet || event.context_snippet || '';
                                            const isTextLong = displayText.length > 300;
                                            const isExpanded = expandedEvents.has(idx);
                                            const shownText = isExpanded ? displayText : (isTextLong ? displayText.slice(0, 300) + '...' : displayText);
                                            const timeStr = event.mention_time
                                                ? event.mention_time.replace('T', ' ').slice(0, 16)
                                                : event.mention_date || '';
                                            const siblingStocks = (event.stocks || []).filter(s => s.stock_code !== stockCode);

                                            const key = eventKey(event, idx);
                                            return (
                                                <div
                                                    key={key}
                                                    ref={(el) => { eventRefs.current[key] = el; }}
                                                    className={`relative pl-6 border-l-2 pb-4 last:pb-0 last:border-l-0 transition-colors ${highlightEventKey === key ? 'border-blue-400 bg-blue-50/40 rounded-md' : 'border-muted'}`}
                                                >
                                                    <div className="absolute -left-[5px] top-1 h-2.5 w-2.5 rounded-full bg-primary ring-4 ring-background" />

                                                    <div className="flex flex-col gap-2">
                                                        <div className="flex items-center flex-wrap gap-2 text-xs">
                                                            <span className="font-medium text-foreground">📅 {timeStr}</span>
                                                            {event.group_name && (
                                                                <Badge variant="secondary" className="text-[10px]">📍 {event.group_name}</Badge>
                                                            )}
                                                            {!event.group_name && gid && groupId == null && (
                                                                <Badge variant="outline" className="text-[10px]">群组 {gid}</Badge>
                                                            )}
                                                        </div>

                                                        <div className="bg-muted/30 p-3 rounded-md text-sm leading-relaxed">
                                                            {displayText ? (
                                                                <div>
                                                                    <div className="whitespace-pre-wrap break-words">{shownText}</div>
                                                                    {isTextLong && (
                                                                        <div className="mt-2 flex justify-end">
                                                                            <Button
                                                                                size="sm"
                                                                                variant="ghost"
                                                                                className="h-6 px-2 text-xs"
                                                                                onClick={() => toggleEventExpand(idx)}
                                                                            >
                                                                                {isExpanded ? '收起' : '展开全部'}
                                                                            </Button>
                                                                        </div>
                                                                    )}
                                                                </div>
                                                            ) : (
                                                                <span className="italic text-muted-foreground">无话题内容</span>
                                                            )}
                                                        </div>

                                                        {siblingStocks.length > 0 && (
                                                            <div className="flex flex-wrap gap-1.5 items-center">
                                                                <span className="text-[10px] text-muted-foreground">同话题股票:</span>
                                                                {siblingStocks.map((s) => (
                                                                    <Badge key={s.stock_code} variant="outline" className="text-[10px] font-normal">
                                                                        {s.stock_name} ({s.stock_code})
                                                                    </Badge>
                                                                ))}
                                                            </div>
                                                        )}

                                                        <div className="flex flex-wrap gap-2 text-xs">
                                                            <span className={`px-2 py-0.5 rounded bg-muted font-mono ${(event.return_1d || 0) > 0 ? 'text-emerald-600' : (event.return_1d || 0) < 0 ? 'text-red-600' : ''}`}>
                                                                T+1: {fmtP(event.return_1d) || '—'}
                                                            </span>
                                                            <span className={`px-2 py-0.5 rounded bg-muted font-mono ${(event.return_3d || 0) > 0 ? 'text-emerald-600' : (event.return_3d || 0) < 0 ? 'text-red-600' : ''}`}>
                                                                T+3: {fmtP(event.return_3d) || '—'}
                                                            </span>
                                                            <span className={`px-2 py-0.5 rounded bg-muted font-mono ${(event.return_5d || 0) > 0 ? 'text-emerald-600' : (event.return_5d || 0) < 0 ? 'text-red-600' : ''}`}>
                                                                T+5: {fmtP(event.return_5d) || '—'}
                                                            </span>
                                                            <span className={`px-2 py-0.5 rounded bg-muted font-mono ${(event.return_10d || 0) > 0 ? 'text-emerald-600' : (event.return_10d || 0) < 0 ? 'text-red-600' : ''}`}>
                                                                T+10: {fmtP(event.return_10d) || '—'}
                                                            </span>
                                                            <span className={`px-2 py-0.5 rounded bg-muted font-mono ${(event.return_20d || 0) > 0 ? 'text-emerald-600' : (event.return_20d || 0) < 0 ? 'text-red-600' : ''}`}>
                                                                T+20: {fmtP(event.return_20d) || '—'}
                                                            </span>
                                                        </div>
                                                    </div>
                                                </div>
                                            );
                                        })}
                                    </div>
                                </div>
                            </div>
                        </div>
                    )}
                </div>
            </SheetContent>
        </Sheet>
    );
}

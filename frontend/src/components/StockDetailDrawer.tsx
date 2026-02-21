'use client';

import React, { useMemo, useState, useEffect } from 'react';
import {
    Sheet,
    SheetContent,
    SheetHeader,
    SheetTitle,
    SheetDescription,
} from "@/components/ui/sheet";
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
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
}

interface StockDetailsData {
    stock_name?: string;
    total_mentions?: number;
    win_rate_5d?: number;
    avg_return_5d?: number;
    events?: StockEventDetail[];
}

export default function StockDetailDrawer({ stockCode, groupId, onClose }: StockDetailDrawerProps) {
    const [loading, setLoading] = useState(false);
    const [details, setDetails] = useState<StockDetailsData | null>(null);
    const [error, setError] = useState<string | null>(null);
    const [expandedEvents, setExpandedEvents] = useState<Set<number>>(new Set());

    useEffect(() => {
        if (!stockCode) return;

        const fetchDetails = async () => {
            setLoading(true);
            setError(null);
            try {
                const rawData: StockEventsResponse = groupId
                    ? await apiClient.getStockEvents(groupId, stockCode)
                    : await apiClient.getGlobalStockEvents(stockCode);

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

                setDetails({
                    stock_name: rawData?.stock_name,
                    total_mentions: rawData?.total_mentions ?? events.length,
                    win_rate_5d: typeof rawData?.win_rate_5d === 'number' ? rawData.win_rate_5d : computedWinRate,
                    avg_return_5d: typeof rawData?.avg_return_5d === 'number' ? rawData.avg_return_5d : computedAvgReturn,
                    events,
                });
                setExpandedEvents(new Set());
            } catch (err) {
                console.error('Failed to fetch stock details', err);
                setError('获取详情失败，请稍后重试');
            } finally {
                setLoading(false);
            }
        };

        void fetchDetails();
    }, [stockCode, groupId]);

    const handleOpenChange = (open: boolean) => {
        if (!open) onClose();
    };

    const fmtP = (val: number | null | undefined) => {
        if (val == null) return null;
        return `${val > 0 ? '+' : ''}${val}%`;
    };

    const toggleEventExpand = (idx: number) => {
        setExpandedEvents(prev => {
            const next = new Set(prev);
            if (next.has(idx)) next.delete(idx);
            else next.add(idx);
            return next;
        });
    };

    const latestMention = useMemo(() => details?.events?.[0]?.mention_date || '—', [details?.events]);

    if (!stockCode) return null;

    return (
        <Sheet open={!!stockCode} onOpenChange={handleOpenChange}>
            <SheetContent
                side="right"
                className="!max-w-none w-[100vw] sm:w-[85vw] md:w-[70vw] lg:w-[60vw] xl:w-[50vw] flex flex-col p-6 h-full overflow-hidden shadow-2xl"
                onOpenAutoFocus={(e) => e.preventDefault()}
            >
                <SheetHeader className="pb-4 border-b shrink-0">
                    <div className="flex items-center justify-between mr-8">
                        <SheetTitle className="text-xl flex items-center gap-2">
                            {details?.stock_name || stockCode}
                            <span className="text-sm font-normal text-muted-foreground bg-muted px-2 py-0.5 rounded">
                                {stockCode}
                            </span>
                        </SheetTitle>
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
                                    </div>
                                    <div className="p-3 bg-muted/30 rounded-lg border flex flex-col justify-between overflow-hidden">
                                        <div className="text-xs text-muted-foreground mb-1 whitespace-nowrap">最新提及</div>
                                        <div className="text-sm font-medium mt-1 break-words leading-tight">{latestMention}</div>
                                    </div>
                                </div>

                                <div>
                                    <h3 className="text-sm font-semibold mb-3 flex items-center gap-2 sticky top-0 bg-background z-10 py-2">
                                        <Calendar className="h-4 w-4" /> 提及事件时间轴
                                    </h3>
                                    <div className="space-y-4 pl-2">
                                        {details?.events?.map((event: StockEventDetail, idx: number) => {
                                            const gid = String(event.group_id ?? groupId ?? '');

                                            // 使用 full_text（含完整话题内容）或 text_snippet 或回退到 context_snippet
                                            const displayText = event.full_text || event.text_snippet || event.context_snippet || '';
                                            const isTextLong = displayText.length > 300;
                                            const isExpanded = expandedEvents.has(idx);
                                            const shownText = isExpanded ? displayText : (isTextLong ? displayText.slice(0, 300) + '...' : displayText);
                                            // 格式化时间
                                            const timeStr = event.mention_time
                                                ? event.mention_time.replace('T', ' ').slice(0, 16)
                                                : event.mention_date || '';
                                            // 关联股票（排除当前股票）
                                            const siblingStocks = (event.stocks || []).filter(s => s.stock_code !== stockCode);

                                            return (
                                                <div key={`${event.mention_id || event.topic_id || idx}`} className="relative pl-6 border-l-2 border-muted pb-4 last:pb-0 last:border-l-0">
                                                    <div className="absolute -left-[5px] top-1 h-2.5 w-2.5 rounded-full bg-primary ring-4 ring-background" />

                                                    <div className="flex flex-col gap-2">
                                                        {/* 时间 + 群组信息 */}
                                                        <div className="flex items-center flex-wrap gap-2 text-xs">
                                                            <span className="font-medium text-foreground">
                                                                📅 {timeStr}
                                                            </span>
                                                            {event.group_name && (
                                                                <Badge variant="secondary" className="text-[10px]">
                                                                    📍 {event.group_name}
                                                                </Badge>
                                                            )}
                                                            {!event.group_name && gid && groupId == null && (
                                                                <Badge variant="outline" className="text-[10px]">
                                                                    群组 {gid}
                                                                </Badge>
                                                            )}
                                                        </div>

                                                        {/* 话题内容（完整文本） */}
                                                        <div className="bg-muted/30 p-3 rounded-md text-sm leading-relaxed">
                                                            {displayText ? (
                                                                <div>
                                                                    <div className="whitespace-pre-wrap break-words">
                                                                        {shownText}
                                                                    </div>
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

                                                        {/* 关联股票标签 */}
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

                                                        {/* 收益数据 */}
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

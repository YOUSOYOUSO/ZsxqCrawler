'use client';

import React, { useState, useEffect } from 'react';
import {
    Sheet,
    SheetContent,
    SheetHeader,
    SheetTitle,
    SheetDescription,
    SheetClose
} from "@/components/ui/sheet";
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { apiClient } from '@/lib/api';
import { Loader2, TrendingUp, Calendar, MessageSquare, AlertTriangle, ExternalLink } from 'lucide-react';

interface StockDetailDrawerProps {
    stockCode: string | null;
    groupId?: number | string;
    onClose: () => void;
}

export default function StockDetailDrawer({ stockCode, groupId, onClose }: StockDetailDrawerProps) {
    const [loading, setLoading] = useState(false);
    const [details, setDetails] = useState<any>(null);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        if (!stockCode || !groupId) return;

        const fetchDetails = async () => {
            setLoading(true);
            setError(null);
            try {
                // Fetch basic events and stats
                const data = await apiClient.getStockEvents(groupId, stockCode);
                setDetails(data);

                // Optionally fetch price history if needed, but events usually suffice for now
            } catch (err) {
                console.error("Failed to fetch stock details", err);
                setError("获取详情失败，请稍后重试");
            } finally {
                setLoading(false);
            }
        };

        fetchDetails();
    }, [stockCode, groupId]);

    const handleOpenChange = (open: boolean) => {
        if (!open) onClose();
    };

    if (!stockCode) return null;

    // Helper to format percentage safely
    const fmtP = (val: number | null | undefined) => {
        if (val == null) return null;
        return `${val > 0 ? '+' : ''}${val}%`;
    };

    return (
        <Sheet open={!!stockCode} onOpenChange={handleOpenChange}>
            <SheetContent
                side="right"
                className="w-[95vw] sm:w-[720px] lg:w-[800px] flex flex-col p-6 h-full overflow-hidden"
                // Prevent auto-focus to avoid unwanted scrolling behavior when opening
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
                        // Use native div scanning with overflow-y-auto to ensure scrollability
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
                                        <div className="text-sm font-medium mt-1 break-words leading-tight">
                                            {details?.events?.[0]?.mention_date || '—'}
                                        </div>
                                    </div>
                                </div>

                                {/* Event Timeline */}
                                <div>
                                    <h3 className="text-sm font-semibold mb-3 flex items-center gap-2 sticky top-0 bg-background z-10 py-2">
                                        <Calendar className="h-4 w-4" /> 提及事件时间轴
                                    </h3>
                                    <div className="space-y-4 pl-2">
                                        {details?.events?.map((event: any, idx: number) => (
                                            <div key={idx} className="relative pl-6 border-l-2 border-muted pb-4 last:pb-0 last:border-l-0">
                                                {/* Timeline dot */}
                                                <div className="absolute -left-[5px] top-1 h-2.5 w-2.5 rounded-full bg-primary ring-4 ring-background" />

                                                <div className="flex flex-col gap-2">
                                                    <div className="flex items-center justify-between text-xs text-muted-foreground">
                                                        <span>{event.mention_date} {event.mention_time?.slice(11, 16)}</span>
                                                        <Badge variant="outline" className="font-mono text-[10px]">
                                                            基准价: {event.price_at_mention?.toFixed(2) || '—'}
                                                        </Badge>
                                                    </div>

                                                    {/* Context */}
                                                    <div className="bg-muted/30 p-3 rounded-md text-sm leading-relaxed">
                                                        {event.context_snippet ? (
                                                            <div dangerouslySetInnerHTML={{
                                                                __html: event.context_snippet.replace(new RegExp(stockCode, 'gi'), `<span class="bg-yellow-200 dark:bg-yellow-900/50 px-0.5 rounded">${stockCode}</span>`)
                                                            }} />
                                                        ) : (
                                                            <span className="italic text-muted-foreground">无上下文内容</span>
                                                        )}
                                                    </div>

                                                    {/* Returns breakdown - Extended to show T+3 and T+10 */}
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
                                        ))}
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

'use client';

/* eslint-disable @typescript-eslint/no-explicit-any */
import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import Link from 'next/link';
import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Input } from '@/components/ui/input';
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from '@/components/ui/sheet';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from '@/components/ui/alert-dialog';
import { toast } from 'sonner';
import { apiClient } from '@/lib/api';
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Calendar } from "@/components/ui/calendar";
import { format } from "date-fns";
import { cn } from "@/lib/utils";
import SafeImage from '@/components/SafeImage';
import {
    TrendingUp, BarChart3, Search,
    Activity, Target, Flame,
    Zap, Clock, ChevronRight, ChevronLeft, Loader2,
    Sparkles, Settings, Send, History, Bot, FileText as FileTextIcon, Users,
    Play, CalendarIcon, Info, Trash2
} from 'lucide-react';
import TaskLogViewer from './TaskLogViewer';
import StockDetailDrawer from './StockDetailDrawer';

interface StockDashboardProps {
    groupId?: number | string; // Optional for global mode
    mode?: 'group' | 'global';
    onTaskCreated?: (taskId: string) => void;
    onDataChanged?: () => void | Promise<void>;
    hideScanActions?: boolean;
    externalSearchTerm?: string;
    initialView?: 'overview' | 'winrate' | 'sector' | 'signals' | 'ai';
    allowedViews?: Array<'overview' | 'winrate' | 'sector' | 'signals' | 'ai'>;
    surfaceVariant?: 'default' | 'group-consistent';
    hideSummaryCards?: boolean;
}

interface SectorTopicItem {
    topic_id: string | number;
    create_time: string;
    text_snippet: string;
    full_text?: string;
    matched_keywords: string[];
    stocks: Array<{ stock_code: string; stock_name: string }>;
}


const getToday = () => format(new Date(), 'yyyy-MM-dd');
const getPastDate = (days: number) => {
    const d = new Date();
    d.setDate(d.getDate() - days);
    return format(d, 'yyyy-MM-dd');
};

/* ────────── Time Range Picker Component ────────── */
function TimeRangePicker({
    range, start, end,
    onRangeChange, onStartChange, onEndChange
}: {
    range: string; start: string; end: string;
    onRangeChange: (r: any) => void;
    onStartChange: (s: string) => void;
    onEndChange: (e: string) => void;
}) {
    const presets = ['10d', '20d', '30d', '60d', '180d', '365d'];
    const formatPresetLabel = (daysStr: string) => {
        const days = parseInt(daysStr.replace('d', ''), 10);
        if (days === 365) return '近1年';
        return `近${days}天`;
    };

    const applyPreset = (daysStr: string) => {
        const days = parseInt(daysStr.replace('d', ''));
        onRangeChange(daysStr);
        onStartChange(getPastDate(days));
        onEndChange(getToday());
    };

    const handleDateSelect = (r: { from?: Date; to?: Date } | undefined) => {
        onRangeChange('custom');
        if (!r) {
            onStartChange('');
            onEndChange('');
            return;
        }
        onStartChange(r.from ? format(r.from, 'yyyy-MM-dd') : '');
        onEndChange(r.to ? format(r.to, 'yyyy-MM-dd') : '');

    };

    const selectedFrom = start ? new Date(start) : undefined;
    const selectedTo = end ? new Date(end) : undefined;

    return (
        <div className="flex items-center gap-2 flex-wrap bg-muted/20 p-1.5 rounded-md">
            <span className="text-xs text-muted-foreground ml-1">时间:</span>
            <div className="flex gap-1">
                {presets.map(p => (
                    <Button
                        key={p}
                        size="sm"
                        variant={range === p ? 'secondary' : 'ghost'}
                        className={`h-6 px-2 text-xs ${range === p ? 'bg-background shadow-sm text-primary font-medium' : 'text-muted-foreground'}`}
                        onClick={() => applyPreset(p)}
                    >
                        {formatPresetLabel(p)}
                    </Button>
                ))}
            </div>
            <div className="w-px h-4 bg-border/50 mx-1" />

            <Popover>
                <PopoverTrigger asChild>
                    <Button
                        id="date"
                        variant={"outline"}
                        size="sm"
                        className={cn(
                            "h-6 justify-start text-left font-normal px-2 text-[10px]",
                            !start && "text-muted-foreground"
                        )}
                    >
                        <CalendarIcon className="mr-2 h-3 w-3" />
                        {start ? (
                            end ? (
                                <>
                                    {start} - {end}
                                </>
                            ) : (
                                start
                            )
                        ) : (
                            <span>选择日期</span>
                        )}
                    </Button>
                </PopoverTrigger>
                <PopoverContent className="w-auto p-0" align="end">
                    <Calendar
                        initialFocus
                        mode="range"
                        defaultMonth={selectedFrom}
                        selected={{ from: selectedFrom, to: selectedTo }}
                        onSelect={handleDateSelect}
                        numberOfMonths={2}
                        className="p-3"
                    />
                </PopoverContent>
            </Popover>
        </div>
    );
}

function HeaderInfo({ text }: { text: string }) {
    return (
        <Popover>
            <PopoverTrigger asChild>
                <button
                    type="button"
                    className="inline-flex items-center justify-center text-muted-foreground hover:text-foreground"
                    onClick={(e) => e.stopPropagation()}
                    aria-label="说明"
                >
                    <Info className="h-3.5 w-3.5" />
                </button>
            </PopoverTrigger>
            <PopoverContent align="end" className="w-64 p-2 text-xs leading-relaxed">
                {text}
            </PopoverContent>
        </Popover>
    );
}

/* ────────── heat bar for sector ────────── */
function HeatBar({ value, max, label }: { value: number; max: number; label: string }) {

    const pct = max > 0 ? (value / max) * 100 : 0;
    const intensity = Math.min(pct / 100, 1);
    return (
        <div className="flex items-center gap-2 text-xs">
            <span className="w-14 text-right font-medium text-muted-foreground shrink-0">{label}</span>
            <div className="flex-1 h-5 rounded-sm bg-muted/30 relative overflow-hidden">
                <div
                    className="h-full rounded-sm transition-all duration-500"
                    style={{
                        width: `${pct}%`,
                        background: `linear-gradient(90deg, rgba(249,115,22,${0.25 + intensity * 0.6}), rgba(239,68,68,${0.3 + intensity * 0.7}))`,
                    }}
                />
                <span className="absolute inset-y-0 right-1 flex items-center text-[10px] font-mono text-foreground/70">
                    {value}
                </span>
            </div>
        </div>
    );
}

/* ────────── lightweight markdown → HTML ────────── */
function simpleMarkdown(md: string): string {
    return md
        // code blocks
        .replace(/```[\s\S]*?```/g, (m) => {
            const inner = m.slice(3, -3).replace(/^[^\n]*\n/, '');
            return `<pre class="bg-muted/50 rounded p-2 text-xs overflow-x-auto"><code>${inner.replace(/</g, '&lt;')}</code></pre>`;
        })
        // tables: header row → th, separator → skip, data → td
        .replace(/^(\|.+\|)\n(\|[\s:|-]+\|)\n((?:\|.+\|\n?)*)/gm, (_match, hdr: string, _sep: string, body: string) => {
            const ths = hdr.split('|').filter(Boolean).map((c: string) => `<th class="border px-2 py-1 text-left">${c.trim()}</th>`).join('');
            const rows = body.trim().split('\n').map((r: string) => {
                const tds = r.split('|').filter(Boolean).map((c: string) => `<td class="border px-2 py-1">${c.trim()}</td>`).join('');
                return `<tr>${tds}</tr>`;
            }).join('');
            return `<table class="w-full border-collapse text-xs my-2"><thead><tr>${ths}</tr></thead><tbody>${rows}</tbody></table>`;
        })
        // headers
        .replace(/^#### (.+)$/gm, '<h4 class="text-sm font-semibold mt-3 mb-1">$1</h4>')
        .replace(/^### (.+)$/gm, '<h3 class="text-sm font-bold mt-4 mb-1">$1</h3>')
        .replace(/^## (.+)$/gm, '<h2 class="text-base font-bold mt-4 mb-2">$1</h2>')
        .replace(/^# (.+)$/gm, '<h1 class="text-lg font-bold mt-4 mb-2">$1</h1>')
        // bold & italic
        .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
        .replace(/\*(.+?)\*/g, '<em>$1</em>')
        // inline code
        .replace(/`([^`]+)`/g, '<code class="bg-muted px-1 rounded text-xs">$1</code>')
        // unordered lists
        .replace(/^[-*] (.+)$/gm, '<li class="ml-4 list-disc">$1</li>')
        // ordered lists
        .replace(/^\d+\. (.+)$/gm, '<li class="ml-4 list-decimal">$1</li>')
        // paragraphs (double newline)
        .replace(/\n\n/g, '</p><p class="my-1.5">')
        // single newline (within paragraph)
        .replace(/\n/g, '<br/>');
}

/* ═══════════════  MAIN COMPONENT  ═══════════════ */
export default function StockDashboard({
    groupId,
    mode = 'group',
    onTaskCreated,
    onDataChanged,
    hideScanActions = false,
    externalSearchTerm,
    initialView = 'overview',
    allowedViews,
    surfaceVariant = 'default',
    hideSummaryCards = false,
}: StockDashboardProps) {
    const isGlobal = mode === 'global';
    const scanTaskStorageKey = useMemo(
        () => (isGlobal ? 'stock-dashboard:scan-task:global' : `stock-dashboard:scan-task:group:${String(groupId ?? 'unknown')}`),
        [groupId, isGlobal]
    );
    const [activeView, setActiveView] = useState<'overview' | 'winrate' | 'sector' | 'signals' | 'ai'>(initialView);
    const effectiveAllowedViews = useMemo(() => (
        allowedViews && allowedViews.length > 0
            ? allowedViews
            : ['overview', 'winrate', 'sector', 'signals', 'ai']
    ), [allowedViews]);
    const [stats, setStats] = useState<any>(null);
    const [mentions, setMentions] = useState<any[]>([]); // Keeping for legacy or unused? Or maybe remove? Let's keep for search/pagination compatibility if needed or replace.
    const [topics, setTopics] = useState<any[]>([]); // New state for topics
    const [winRate, setWinRate] = useState<any[]>([]);
    const [sectors, setSectors] = useState<any[]>([]);
    const [signals, setSignals] = useState<any[]>([]);
    const [globalGroups, setGlobalGroups] = useState<any[]>([]);
    const [groupMetaMap, setGroupMetaMap] = useState<Record<string, any>>({});
    const [featureFlags, setFeatureFlags] = useState<Record<string, any>>({});
    const [lastError, setLastError] = useState<string | null>(null);

    const [loading, setLoading] = useState(true);
    const [scanning, setScanning] = useState(false);
    const [scanTaskId, setScanTaskId] = useState<string | null>(null);
    const [showTaskLog, setShowTaskLog] = useState(false);
    const [deletingGroupId, setDeletingGroupId] = useState<number | null>(null);

    const [mentionPage, setMentionPage] = useState(1);
    const [mentionTotal, setMentionTotal] = useState(0);
    const [returnPeriod, setReturnPeriod] = useState('return_5d');
    const [searchStock, setSearchStock] = useState(externalSearchTerm || '');

    // Time range filter state - default 30d (approx 20 working days)
    const [winRateRange, setWinRateRange] = useState<string>('30d');
    const [winRateStart, setWinRateStart] = useState<string>(getPastDate(30));
    const [winRateEnd, setWinRateEnd] = useState<string>(getToday());

    const [sectorRange, setSectorRange] = useState<string>('30d');
    const [sectorStart, setSectorStart] = useState<string>(getPastDate(30));
    const [sectorEnd, setSectorEnd] = useState<string>(getToday());

    const [signalRange, setSignalRange] = useState<string>('30d');
    const [signalStart, setSignalStart] = useState<string>(getPastDate(30));
    const [signalEnd, setSignalEnd] = useState<string>(getToday());

    // Win rate pagination and sort state
    const [winRatePage, setWinRatePage] = useState(1);
    const [winRateTotal, setWinRateTotal] = useState(0);
    const winRatePageSize = 20;
    const [winRateSortColumn, setWinRateSortColumn] = useState<string>('win_rate');
    const [winRateSortOrder, setWinRateSortOrder] = useState<'desc' | 'asc'>('desc');
    const [winRateMinMentions, setWinRateMinMentions] = useState<number>(2);
    const [signalMinMentions, setSignalMinMentions] = useState<number>(2);

    const [selectedStock, setSelectedStock] = useState<string | null>(null);
    const [expandedOverviewTopics, setExpandedOverviewTopics] = useState<Set<string>>(new Set());
    const [selectedSector, setSelectedSector] = useState<any | null>(null);
    const [sectorTopics, setSectorTopics] = useState<SectorTopicItem[]>([]);
    const [sectorTopicsTotal, setSectorTopicsTotal] = useState(0);
    const [sectorTopicsPage, setSectorTopicsPage] = useState(1);
    const [sectorTopicsLoading, setSectorTopicsLoading] = useState(false);
    const [sectorTopicsError, setSectorTopicsError] = useState<string | null>(null);
    const [expandedSectorTopics, setExpandedSectorTopics] = useState<Set<string>>(new Set());
    const sectorTopicsPageSize = 20;
    const sectorDrawerScrollRef = useRef<HTMLDivElement>(null);

    // AI analysis state
    const [aiConfig, setAiConfig] = useState<any>(null);
    const [aiResult, setAiResult] = useState<any>(null);
    const [aiLoading, setAiLoading] = useState(false);
    const [aiHistory, setAiHistory] = useState<any[]>([]);
    const [aiStockInput, setAiStockInput] = useState('');
    const [aiConfigKey, setAiConfigKey] = useState('');
    const [aiConfigBaseUrl, setAiConfigBaseUrl] = useState('https://api.deepseek.com');
    const [aiConfigModel, setAiConfigModel] = useState('deepseek-chat');
    const [showAiConfig, setShowAiConfig] = useState(false);
    const sectorMaxMentions = useMemo(
        () => sectors.reduce((max: number, item: any) => Math.max(max, Number(item?.total_mentions || 0)), 0),
        [sectors]
    );

    const getGlobalGroupDisplayName = useCallback((group: any) => {
        const gid = String(group?.group_id ?? '').trim();
        const rawName = String(group?.group_name ?? '').trim();
        const metaName = String(groupMetaMap[gid]?.name ?? '').trim();
        const isFallbackName = !rawName || rawName === gid || /^group\s+\d+$/i.test(rawName);
        if (!isFallbackName) return rawName;
        if (metaName) return metaName;
        return rawName || `Group ${gid}`;
    }, [groupMetaMap]);

    const isAnalyzeTaskForCurrentDashboard = useCallback((task: any) => {
        const taskType = String(task?.type ?? '');
        if (isGlobal) {
            return taskType === 'global_scan' || taskType.startsWith('global_analyze_performance');
        }
        return taskType === `stock_scan_${String(groupId ?? '')}`;
    }, [groupId, isGlobal]);

    const sortTasksByTimeDesc = useCallback((a: any, b: any) => {
        const aTs = new Date(a?.updated_at ?? a?.created_at ?? 0).getTime();
        const bTs = new Date(b?.updated_at ?? b?.created_at ?? 0).getTime();
        return bTs - aTs;
    }, []);

    useEffect(() => {
        if (typeof window === 'undefined') return;
        if (scanTaskId) {
            window.localStorage.setItem(scanTaskStorageKey, scanTaskId);
        } else {
            window.localStorage.removeItem(scanTaskStorageKey);
        }
    }, [scanTaskId, scanTaskStorageKey]);

    useEffect(() => {
        if (hideScanActions) return;
        let cancelled = false;

        const recoverTask = async () => {
            let restoredTaskId: string | null = null;

            try {
                const tasks = await apiClient.getTasks();
                const candidates = tasks.filter(isAnalyzeTaskForCurrentDashboard).sort(sortTasksByTimeDesc);
                const running = candidates.find((task: any) => ['pending', 'running', 'stopping'].includes(String(task?.status ?? '')));

                if (running) {
                    restoredTaskId = running.task_id;
                    if (!cancelled) {
                        setScanning(true);
                        setShowTaskLog(true);
                    }
                } else if (candidates.length > 0) {
                    restoredTaskId = candidates[0].task_id;
                }
            } catch (err) {
                console.warn('[StockDashboard] Failed to recover task from server:', err);
            }

            if (!restoredTaskId && typeof window !== 'undefined') {
                restoredTaskId = window.localStorage.getItem(scanTaskStorageKey);
            }

            if (cancelled) return;
            if (restoredTaskId) {
                setScanTaskId(restoredTaskId);
            } else {
                setScanTaskId(null);
                setScanning(false);
            }
        };

        recoverTask();

        return () => { cancelled = true; };
    }, [hideScanActions, isAnalyzeTaskForCurrentDashboard, scanTaskStorageKey, sortTasksByTimeDesc]);

    useEffect(() => {
        if (!scanTaskId || !scanning) return;
        const timer = setInterval(async () => {
            try {
                const task = await apiClient.getTask(scanTaskId);
                const status = String(task?.status ?? '');
                if (['completed', 'failed', 'cancelled', 'stopped', 'idle'].includes(status)) {
                    setScanning(false);
                }
            } catch (err) {
                console.warn('[StockDashboard] Failed to poll task status:', err);
            }
        }, 5000);
        return () => clearInterval(timer);
    }, [scanTaskId, scanning]);

    /* ── loaders ── */
    const loadStats = useCallback(async () => {
        try {
            setLastError(null);
            console.log('[StockDashboard] Loading stats...', { isGlobal, groupId });
            const s = isGlobal ? await apiClient.getGlobalStats() : await apiClient.getStockStats(groupId!);
            console.log('[StockDashboard] Stats loaded:', s);
            setStats(s);
        } catch (err: any) {
            console.error('[StockDashboard] Failed to load stats:', err);
            setLastError(err.message || 'Failed to load stats');
        }
    }, [groupId, isGlobal]);

    const loadMentions = useCallback(async () => {
        if (isGlobal) {
            // Global mentions are not supported in the same way, we might load groups instead
            const res = await apiClient.getGlobalGroups();
            const rows = Array.isArray(res) ? res : (res?.data || res?.groups || []);
            setGlobalGroups(rows);
            return;
        }
        try {
            console.log('[StockDashboard] Loading topics...', { groupId, page: mentionPage });
            // Use getStockTopics instead of getStockMentions
            const res = (await apiClient.getStockTopics(groupId!, mentionPage, 20)) as any;

            console.log('[StockDashboard] Topics loaded:', res);
            setTopics(res.items || []);
            setMentionTotal(res.total || 0);

            // Still fetch plain mentions for search if user searches? 
            // Actually, search is not supported in topic view yet. 
            // If searchStock is present, we might want to fall back to mention list or filter topics?
            // For now, let's assume search is disabled or we clear topics.
            if (searchStock) {
                const res2 = await apiClient.getStockMentions(groupId!, {
                    page: mentionPage,
                    per_page: 20,
                    stock_code: searchStock,
                    sort_by: 'mention_time',
                    order: 'desc',
                });
                setMentions(res2.items || res2.mentions || []);
            } else {
                setMentions([]);
            }

        } catch (err: any) {
            console.error('[StockDashboard] Failed to load topics:', err);
            // Don't set global error for mentions failure to avoid blocking other views
        }
    }, [groupId, mentionPage, searchStock, isGlobal]);

    const loadGroupMeta = useCallback(async () => {
        if (!isGlobal) return;
        try {
            const res = await apiClient.getGroups();
            const rows = res?.groups || [];
            const map = rows.reduce((acc: Record<string, any>, g: any) => {
                acc[String(g.group_id)] = g;
                return acc;
            }, {});
            setGroupMetaMap(map);
        } catch (err) {
            console.warn('[StockDashboard] Failed to load group meta:', err);
        }
    }, [isGlobal]);

    const loadFeatures = useCallback(async () => {
        if (!isGlobal) return;
        try {
            const features = await apiClient.getFeatures();
            setFeatureFlags(features || {});
        } catch {
            setFeatureFlags({});
        }
    }, [isGlobal]);

    // Helper to compute start_date from time range - REMOVED, using explicit start/end state


    const loadWinRate = useCallback(async () => {
        try {
            console.log('[StockDashboard] Loading win rate...', { start: winRateStart, end: winRateEnd, sort: winRateSortColumn, order: winRateSortOrder });
            if (isGlobal) {
                const res = await apiClient.getGlobalWinRate(
                    winRateMinMentions,
                    returnPeriod,
                    1000,
                    winRateStart,
                    winRateEnd,
                    winRateSortColumn,
                    winRateSortOrder,
                    winRatePage,
                    winRatePageSize
                );
                setWinRate(res?.data || []);
                setWinRateTotal(res?.total || 0);
            } else {
                const res = await apiClient.getStockWinRate(groupId!, {
                    min_mentions: winRateMinMentions,
                    return_period: returnPeriod,
                    limit: 500,
                    start_date: winRateStart,
                    end_date: winRateEnd,
                    page: winRatePage,
                    page_size: winRatePageSize,
                    sort_by: winRateSortColumn,
                    order: winRateSortOrder,
                });
                // Handle both paginated (dict) and legacy (array) responses
                if (res && res.data && typeof res.total === 'number') {
                    setWinRate(res.data);
                    setWinRateTotal(res.total);
                } else if (Array.isArray(res)) {
                    setWinRate(res);
                    setWinRateTotal(res.length);
                } else {
                    setWinRate([]);
                    setWinRateTotal(0);
                }
            }
        } catch (err) {
            console.error('[StockDashboard] Failed to load win rate:', err);
        }
    }, [groupId, returnPeriod, isGlobal, winRateStart, winRateEnd, winRatePage, winRatePageSize, winRateSortColumn, winRateSortOrder, winRateMinMentions]);

    const loadSectors = useCallback(async () => {
        try {
            console.log('[StockDashboard] Loading sectors...', { start: sectorStart, end: sectorEnd });
            const res = isGlobal
                ? await apiClient.getGlobalSectorHeat(sectorStart, sectorEnd)
                : await apiClient.getSectorHeat(groupId!, sectorStart, sectorEnd);
            console.log('[StockDashboard] Sectors loaded:', res?.length);
            setSectors(res || []);
        } catch (err) {
            console.error('[StockDashboard] Failed to load sectors:', err);
        }
    }, [groupId, isGlobal, sectorStart, sectorEnd]);

    const loadSignals = useCallback(async () => {
        try {
            console.log('[StockDashboard] Loading signals...', { start: signalStart, end: signalEnd });
            const lookbackDays = 30; // Default fallback if needed, but we use explicit dates now
            const res = isGlobal
                ? await apiClient.getGlobalSignals(lookbackDays, signalMinMentions, signalStart, signalEnd)
                : await apiClient.getStockSignals(groupId!, lookbackDays, signalMinMentions, signalStart, signalEnd);
            console.log('[StockDashboard] Signals loaded:', res?.length);
            setSignals(res || []);
        } catch (err) {
            console.error('[StockDashboard] Failed to load signals:', err);
        }
    }, [groupId, isGlobal, signalStart, signalEnd, signalMinMentions]);

    const loadSectorTopics = useCallback(async () => {
        if (!selectedSector?.sector) return;
        if (isGlobal && featureFlags.global_sector_topics === false) {
            setSectorTopics([]);
            setSectorTopicsTotal(0);
            setSectorTopicsError('当前后端版本不支持全局板块详情接口');
            return;
        }

        setSectorTopicsLoading(true);
        setSectorTopicsError(null);
        try {
            const res = isGlobal
                ? await apiClient.getGlobalSectorTopics({
                    sector: selectedSector.sector,
                    start_date: sectorStart,
                    end_date: sectorEnd,
                    page: sectorTopicsPage,
                    page_size: sectorTopicsPageSize,
                })
                : await apiClient.getSectorTopics(groupId!, {
                    sector: selectedSector.sector,
                    start_date: sectorStart,
                    end_date: sectorEnd,
                    page: sectorTopicsPage,
                    page_size: sectorTopicsPageSize,
                });
            setSectorTopics(res?.items || []);
            setSectorTopicsTotal(res?.total || 0);
        } catch (err) {
            console.error('[StockDashboard] Failed to load sector topics:', err);
            setSectorTopics([]);
            setSectorTopicsTotal(0);
            const msg = err instanceof Error ? err.message : '';
            if (msg.includes('404') || msg.toLowerCase().includes('not found')) {
                setSectorTopicsError('后端未提供 /api/global/sector-topics，请确认服务已更新并重启');
            } else {
                setSectorTopicsError('加载失败，请重试');
            }
        } finally {
            setSectorTopicsLoading(false);
        }
    }, [groupId, isGlobal, featureFlags.global_sector_topics, selectedSector?.sector, sectorStart, sectorEnd, sectorTopicsPage]);

    const loadAll = useCallback(async () => {
        setLoading(true);
        const jobs: Array<Promise<any>> = [loadStats(), loadMentions(), loadSectors()];
        if (isGlobal) {
            jobs.push(loadGroupMeta());
        }
        await Promise.all(jobs);
        setLoading(false);
    }, [loadStats, loadMentions, loadSectors, loadGroupMeta, isGlobal]);

    useEffect(() => { loadAll(); }, [loadAll]);
    useEffect(() => { loadFeatures(); }, [loadFeatures]);

    // Sync external search term from parent component
    useEffect(() => {
        if (externalSearchTerm !== undefined) {
            setSearchStock(externalSearchTerm);
            setMentionPage(1);
        }
    }, [externalSearchTerm]);

    // For local mentions pagination
    useEffect(() => {
        if (!isGlobal) loadMentions();
    }, [mentionPage, searchStock, loadMentions, isGlobal]);

    useEffect(() => {
        if (activeView === 'winrate') loadWinRate();
        else if (activeView === 'sector') loadSectors();
        else if (activeView === 'signals') loadSignals();
    }, [activeView, loadWinRate, loadSectors, loadSignals]);

    useEffect(() => {
        if (!selectedSector) return;
        loadSectorTopics();
    }, [selectedSector, loadSectorTopics]);

    useEffect(() => {
        if (!selectedSector) return;
        setSectorTopicsPage(1);
        setExpandedSectorTopics(new Set());
    }, [selectedSector?.sector, sectorStart, sectorEnd, selectedSector]);

    useEffect(() => {
        if (!selectedSector) return;
        sectorDrawerScrollRef.current?.scrollTo({ top: 0, behavior: 'smooth' });
    }, [sectorTopicsPage, selectedSector]);

    useEffect(() => {
        if (!effectiveAllowedViews.includes(activeView)) {
            setActiveView(effectiveAllowedViews[0]);
        }
    }, [activeView, effectiveAllowedViews]);

    /* ── scan ── */
    const handleScan = async (force = false) => {
        setScanning(true);
        setShowTaskLog(true);
        try {
            const res = isGlobal
                ? await apiClient.scanGlobal(force)
                : await apiClient.scanStocks(groupId!, force);

            setScanTaskId(res.task_id);
            if (onTaskCreated) {
                onTaskCreated(res.task_id);
            }
            toast.success(`分析任务已启动: ${res.task_id}`);

            // Poll stats every 5 s for 2 min (can be kept as a backup update mechanism)
            const poll = setInterval(async () => { await loadStats(); }, 5000);
            setTimeout(() => { clearInterval(poll); loadAll(); }, 120_000);
        } catch (err) {
            toast.error(`分析启动失败: ${err instanceof Error ? err.message : '未知错误'}`);
            setScanning(false);
            setShowTaskLog(false);
        }
    };

    const handleClearGroupTopics = async (group: any) => {
        const gid = Number(group?.group_id);
        if (!Number.isFinite(gid)) {
            toast.error('群组 ID 无效，无法删除');
            return;
        }

        setDeletingGroupId(gid);
        try {
            await apiClient.clearTopicDatabase(gid);
            toast.success(`已删除群组 ${gid} 的所有话题数据`);
            await Promise.all([loadStats(), loadMentions(), loadSectors(), loadSignals(), loadWinRate()]);
            if (onDataChanged) {
                await onDataChanged();
            }
        } catch (err: any) {
            const detail = err?.message || '未知错误';
            toast.error(`删除失败: ${detail}`);
        } finally {
            setDeletingGroupId(null);
        }
    };

    /* ── Stock Detail Logic (mostly specific to Group mode or if Global supports drill down) ── */
    // Note: Global mode might not support detailed stock events easily without group context.
    // However, if we click a stock code, we might want to show some details.
    // For now, only group mode supports full event drill down nicely. 
    // BUT we can use the first available group or fail gracefully.
    // The existing 'getStockEvents' requires groupId.
    // Let's assume Global Detail View is a future enhancement or disable it for global currently.
    // Adjusted: We will allow clicking but wrap in try/catch or disable if global.
    const openStockDetail = (stockCode: string) => {
        setSelectedStock(stockCode);
    };
    const toggleOverviewTopicExpand = (topicId: string | number) => {
        const id = String(topicId);
        setExpandedOverviewTopics(prev => {
            const next = new Set(prev);
            if (next.has(id)) next.delete(id);
            else next.add(id);
            return next;
        });
    };

    /* ── helpers ── */
    const fmtPct = (v: number | null | undefined) => {
        if (v == null) return '—';
        const sign = v > 0 ? '+' : '';
        return `${sign}${v.toFixed(2)}%`;
    };

    const pctColor = (v: number | null | undefined) => {
        if (v == null) return 'text-muted-foreground';
        return v > 0 ? 'text-emerald-500' : v < 0 ? 'text-red-500' : 'text-muted-foreground';
    };

    const sectorUniqueKeywords = new Set(sectorTopics.flatMap((t) => t.matched_keywords));
    const sectorMonthlyStats = selectedSector
        ? Object.entries(selectedSector.daily_mentions || {}).reduce((acc: Record<string, { total: number; days: number; peak: number }>, [date, count]) => {
            const monthKey = String(date).slice(0, 7);
            if (!acc[monthKey]) {
                acc[monthKey] = { total: 0, days: 0, peak: 0 };
            }
            const numericCount = Number(count || 0);
            acc[monthKey].total += numericCount;
            acc[monthKey].days += 1;
            acc[monthKey].peak = Math.max(acc[monthKey].peak, numericCount);
            return acc;
        }, {})
        : {};

    const totalMentionPages = Math.ceil(mentionTotal / 20) || 1;

    /* ══════════════════ RENDER ══════════════════ */
    if (loading && !stats) {
        return (
            <div className="flex items-center justify-center h-64">
                <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
                <span className="ml-2 text-muted-foreground">加载数据中...</span>
            </div>
        );
    }

    if (lastError) {
        return (
            <div className="p-4 border border-red-200 bg-red-50 rounded-lg text-red-700">
                <h3 className="font-bold">数据加载失败</h3>
                <p className="text-sm">{lastError}</p>
                <Button variant="outline" size="sm" className="mt-2 text-red-700 border-red-300 hover:bg-red-100" onClick={loadAll}>
                    重试
                </Button>
            </div>
        );
    }

    return (
        <div className={cn('flex flex-col h-full relative', surfaceVariant === 'group-consistent' ? 'gap-3' : 'gap-4')}>
            {/* ─── Task Log Overlay/Panel ─── */}
            {!hideScanActions && showTaskLog && scanTaskId && (
                <div className={`
                    fixed inset-y-0 right-0 z-50 bg-background border-l shadow-2xl transition-transform duration-300
                    ${showTaskLog ? 'translate-x-0' : 'translate-x-full'}
                    w-[500px] flex flex-col
                `}>
                    <div className="p-3 border-b flex items-center justify-between bg-muted/30">
                        <span className="font-semibold text-sm flex items-center gap-2">
                            <Activity className="h-4 w-4" /> 任务日志
                        </span>
                        <div className="flex items-center gap-2">
                            <Button size="sm" variant="ghost" onClick={() => setShowTaskLog(false)} className="h-6 px-2 text-xs">
                                收起
                            </Button>
                        </div>
                    </div>
                    <div className="flex-1 overflow-hidden">
                        <TaskLogViewer
                            taskId={scanTaskId}
                            onClose={() => setShowTaskLog(false)}
                            inline={true}
                            onTaskStop={() => setScanning(false)}
                        />
                    </div>
                </div>
            )}

            {/* ─── Header cards ─── */}
            {!hideSummaryCards && (
                <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                    <Card className="bg-gradient-to-br from-violet-500/10 to-purple-500/5 border-violet-500/20">
                        <CardContent className="p-3">
                            <div className="flex items-center gap-2 mb-1">
                                {isGlobal ? <Users className="h-4 w-4 text-violet-400" /> : <Activity className="h-4 w-4 text-violet-400" />}
                                <span className="text-xs text-muted-foreground">{isGlobal ? '总群组数' : '总提及'}</span>
                            </div>
                            <p className="text-2xl font-bold">{isGlobal ? stats?.group_count ?? 0 : stats?.total_mentions ?? 0}</p>
                        </CardContent>
                    </Card>
                    <Card className="bg-gradient-to-br from-blue-500/10 to-cyan-500/5 border-blue-500/20">
                        <CardContent className="p-3">
                            <div className="flex items-center gap-2 mb-1">
                                <Target className="h-4 w-4 text-blue-400" />
                                <span className="text-xs text-muted-foreground">涉及股票</span>
                            </div>
                            <p className="text-2xl font-bold">{stats?.unique_stocks ?? 0}</p>
                        </CardContent>
                    </Card>
                    <Card className="bg-gradient-to-br from-emerald-500/10 to-green-500/5 border-emerald-500/20">
                        <CardContent className="p-3">
                            <div className="flex items-center gap-2 mb-1">
                                <TrendingUp className="h-4 w-4 text-emerald-400" />
                                <span className="text-xs text-muted-foreground">5日胜率</span>
                            </div>
                            <p className="text-2xl font-bold">
                                {stats?.overall_win_rate_5d != null ? `${stats.overall_win_rate_5d.toFixed(1)}%` : '—'}
                            </p>
                        </CardContent>
                    </Card>
                    <Card className="bg-gradient-to-br from-orange-500/10 to-amber-500/5 border-orange-500/20">
                        <CardContent className="p-3">
                            <div className="flex items-center gap-2 mb-1">
                                <BarChart3 className="h-4 w-4 text-orange-400" />
                                <span className="text-xs text-muted-foreground">{isGlobal ? '总表现计算' : '已计算'}</span>
                            </div>
                            <p className="text-2xl font-bold">{isGlobal ? stats?.total_performance ?? 0 : stats?.performance_calculated ?? 0}</p>
                        </CardContent>
                    </Card>
                </div>
            )}

            {/* 🔥 高频 section removed per user request */}

            {/* ─── Action bar ─── */}
            <div className="flex flex-col gap-3">
                {/* Row 1: Scan Actions (if any) */}
                {!hideScanActions && (
                    <div className="flex items-center gap-2">
                        <Button
                            size="sm"
                            variant="default"
                            onClick={() => handleScan(false)}
                            disabled={scanning}
                            className="gap-1.5"
                        >
                            {scanning ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
                            {scanning ? '分析中...' : '开始数据分析'}
                        </Button>
                        {(scanning || scanTaskId) && (
                            <Button size="sm" variant="outline" onClick={() => setShowTaskLog(!showTaskLog)}>
                                {showTaskLog ? '隐藏日志' : '查看日志'}
                            </Button>
                        )}
                    </div>
                )}

                {/* Row 2: Navigation Buttons (Evenly Distributed) */}
                {effectiveAllowedViews.length > 1 && (
                    <div
                        className="grid gap-2 mx-auto w-full max-w-[720px]"
                        style={{ gridTemplateColumns: `repeat(${effectiveAllowedViews.length}, minmax(0, 1fr))` }}
                    >
                        {effectiveAllowedViews.includes('overview') && (
                            <Button
                                size="sm"
                                variant={activeView === 'overview' ? 'default' : 'ghost'}
                                onClick={() => setActiveView('overview')}
                                className="gap-1 w-full"
                            >
                                <Activity className="h-3.5 w-3.5" /> 概览
                            </Button>
                        )}
                        {effectiveAllowedViews.includes('winrate') && (
                            <Button
                                size="sm"
                                variant={activeView === 'winrate' ? 'default' : 'ghost'}
                                onClick={() => setActiveView('winrate')}
                                className="gap-1 w-full"
                            >
                                <TrendingUp className="h-3.5 w-3.5" /> 胜率
                            </Button>
                        )}
                        {effectiveAllowedViews.includes('sector') && (
                            <Button
                                size="sm"
                                variant={activeView === 'sector' ? 'default' : 'ghost'}
                                onClick={() => setActiveView('sector')}
                                className="gap-1 w-full"
                            >
                                <Flame className="h-3.5 w-3.5" /> 板块
                            </Button>
                        )}
                        {effectiveAllowedViews.includes('signals') && (
                            <Button
                                size="sm"
                                variant={activeView === 'signals' ? 'default' : 'ghost'}
                                onClick={() => setActiveView('signals')}
                                className="gap-1 w-full"
                            >
                                <Zap className="h-3.5 w-3.5" /> 信号
                            </Button>
                        )}
                        {effectiveAllowedViews.includes('ai') && (
                            <Button
                                size="sm"
                                variant={activeView === 'ai' ? 'default' : 'ghost'}
                                onClick={() => {
                                    setActiveView('ai');
                                    if (!aiConfig) {
                                        apiClient.getAIConfig().then(setAiConfig).catch(() => { });
                                        if (isGlobal) {
                                            apiClient.getGlobalAIHistory().then(setAiHistory).catch(() => { });
                                        } else {
                                            apiClient.getAIHistory(groupId!).then(setAiHistory).catch(() => { });
                                        }
                                    }
                                }}
                                className="gap-1 w-full"
                            >
                                <Sparkles className="h-3.5 w-3.5" /> AI分析
                            </Button>
                        )}
                    </div>
                )}
            </div>

            {/* ─── Search Bar (Fixed, below nav, for Overview) ─── */}
            {activeView === 'overview' && externalSearchTerm === undefined && (
                <div className="flex items-center gap-2 bg-background z-10">
                    <div className="relative flex-1">
                        <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                        <Input
                            placeholder={isGlobal ? "搜索群组名称 / ID..." : "搜索股票代码或名称 (支持模糊搜索)..."}
                            className="pl-9 h-9"
                            value={searchStock}
                            onChange={e => { setSearchStock(e.target.value); setMentionPage(1); }}
                        />
                    </div>
                    {!isGlobal && <span className="text-xs text-muted-foreground whitespace-nowrap">共 {mentionTotal} 条{searchStock ? '记录' : '话题'}</span>}
                    {isGlobal && searchStock && <span className="text-xs text-muted-foreground whitespace-nowrap">匹配 {globalGroups.filter((g: any) => {
                        const q = searchStock.toLowerCase();
                        const name = getGlobalGroupDisplayName(g).toLowerCase();
                        const gid = String(g.group_id || '');
                        return name.includes(q) || gid.includes(q);
                    }).length} / {globalGroups.length} 个群组</span>}
                </div>
            )}

            {/* ─── Views ─── */}
            <div className="flex-1 min-h-0 overflow-auto">

                {/* ── OVERVIEW ── */}
                {activeView === 'overview' && (
                    <div className="space-y-3">
                        {isGlobal ? (
                            // Global Groups List
                            <div className="space-y-2">
                                <h3 className="text-sm font-medium text-muted-foreground">已纳入监控的群组 ({globalGroups.length})</h3>
                                <div className="space-y-3">
                                    {(searchStock ? globalGroups.filter((g: any) => {
                                        const q = searchStock.toLowerCase();
                                        const name = getGlobalGroupDisplayName(g).toLowerCase();
                                        const gid = String(g.group_id || '');
                                        return name.includes(q) || gid.includes(q);
                                    }) : globalGroups).map((group: any) => (
                                        <Card key={group.group_id} className="hover:border-primary/30 transition-colors">
                                            <CardContent className="p-3">
                                                <div className="flex items-start justify-between gap-3">
                                                    <div className="flex items-start gap-3 min-w-0 flex-1">
                                                        {(() => {
                                                            const meta = groupMetaMap[String(group.group_id)] || {};
                                                            const avatar = meta?.owner?.avatar_url || meta?.background_url || '';
                                                            const name = getGlobalGroupDisplayName(group);
                                                            return (
                                                                <SafeImage
                                                                    src={avatar}
                                                                    alt={name}
                                                                    className="w-12 h-12 rounded-lg object-cover flex-shrink-0"
                                                                    fallbackClassName="w-12 h-12 rounded-lg flex-shrink-0"
                                                                    fallbackText={String(name).slice(0, 2)}
                                                                    fallbackGradient="from-blue-500 to-indigo-600"
                                                                />
                                                            );
                                                        })()}
                                                        <div className="min-w-0">
                                                            <Link href={`/groups/${group.group_id}`} className="font-medium text-sm hover:text-primary transition-colors">
                                                                {getGlobalGroupDisplayName(group)}
                                                            </Link>
                                                            <div className="text-xs text-muted-foreground mt-1">ID: {group.group_id}</div>
                                                            <div className="mt-2 text-xs text-muted-foreground flex flex-wrap gap-x-3 gap-y-1">
                                                                <span>股票: {group.unique_stocks || 0}</span>
                                                                <span>话题: {group.topics_count || group.total_topics || 0}</span>
                                                                <span>提及: {group.mentions_count || group.total_mentions || 0}</span>
                                                                <span>最后更新: {group.last_updated || group.latest_topic || '—'}</span>
                                                            </div>
                                                        </div>
                                                    </div>

                                                    <div className="flex flex-col items-end gap-2 shrink-0">
                                                        <Badge variant="outline" className="text-xs">
                                                            {group.mentions_count || group.total_mentions || 0} 提及
                                                        </Badge>
                                                        <AlertDialog>
                                                            <AlertDialogTrigger asChild>
                                                                <Button
                                                                    size="sm"
                                                                    variant="destructive"
                                                                    className="h-8 px-2 text-xs"
                                                                    disabled={deletingGroupId === Number(group.group_id)}
                                                                >
                                                                    <Trash2 className="h-3.5 w-3.5 mr-1" />
                                                                    {deletingGroupId === Number(group.group_id) ? '删除中...' : '删除所有话题'}
                                                                </Button>
                                                            </AlertDialogTrigger>
                                                            <AlertDialogContent>
                                                                <AlertDialogHeader>
                                                                    <AlertDialogTitle className="text-red-600">确认删除话题数据</AlertDialogTitle>
                                                                    <AlertDialogDescription>
                                                                        ⚠️ 该操作将删除群组 {group.group_id} 的所有本地话题数据（含评论、用户等关联数据），且不可撤销。
                                                                    </AlertDialogDescription>
                                                                </AlertDialogHeader>
                                                                <AlertDialogFooter>
                                                                    <AlertDialogCancel>取消</AlertDialogCancel>
                                                                    <AlertDialogAction
                                                                        onClick={() => handleClearGroupTopics(group)}
                                                                        className="bg-red-600 hover:bg-red-700"
                                                                    >
                                                                        确认删除
                                                                    </AlertDialogAction>
                                                                </AlertDialogFooter>
                                                            </AlertDialogContent>
                                                        </AlertDialog>
                                                    </div>
                                                </div>
                                            </CardContent>
                                        </Card>
                                    ))}
                                </div>
                            </div>
                        ) : (
                            // Group Mentions List
                            <div className="space-y-3">
                                {/* Search bar moved out */}

                                {searchStock ? (
                                    /* ─── Search Result: Table Mode ─── */
                                    <div className="rounded-md border overflow-hidden">
                                        <table className="w-full text-xs">
                                            <thead>
                                                <tr className="bg-muted/40 text-muted-foreground">
                                                    <th className="text-left p-2 font-medium">股票</th>
                                                    <th className="text-left p-2 font-medium">提及时间</th>
                                                    <th className="text-right p-2 font-medium">T+1</th>
                                                    <th className="text-right p-2 font-medium">T+5</th>
                                                    <th className="text-right p-2 font-medium">T+10</th>
                                                    <th className="text-right p-2 font-medium">超额5d</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {mentions.length === 0 ? (
                                                    <tr><td colSpan={6} className="text-center py-8 text-muted-foreground">
                                                        无匹配股票
                                                    </td></tr>
                                                ) : mentions.map((m: any, i: number) => (
                                                    <tr
                                                        key={`${m.stock_code}-${m.mention_time}-${i}`}
                                                        className="border-t border-border/50 hover:bg-muted/20 transition-colors cursor-pointer"
                                                        onClick={() => openStockDetail(m.stock_code)}
                                                    >
                                                        <td className="p-2">
                                                            <span className="font-medium">{m.stock_name}</span>
                                                            <span className="ml-1 text-muted-foreground">{m.stock_code}</span>
                                                        </td>
                                                        <td className="p-2 text-muted-foreground">
                                                            {m.mention_time ? new Date(m.mention_time).toLocaleDateString('zh-CN') : '—'}
                                                        </td>
                                                        <td className={`p-2 text-right font-mono ${pctColor(m.return_1d)}`}>
                                                            {fmtPct(m.return_1d)}
                                                        </td>
                                                        <td className={`p-2 text-right font-mono ${pctColor(m.return_5d)}`}>
                                                            {fmtPct(m.return_5d)}
                                                        </td>
                                                        <td className={`p-2 text-right font-mono ${pctColor(m.return_10d)}`}>
                                                            {fmtPct(m.return_10d)}
                                                        </td>
                                                        <td className={`p-2 text-right font-mono ${pctColor(m.excess_return_5d ?? m.excess_5d)}`}>
                                                            {fmtPct(m.excess_return_5d ?? m.excess_5d)}
                                                        </td>
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    </div>
                                ) : (
                                    /* ─── Topic Mode ─── */
                                    <div className="space-y-4">
                                        {topics.length === 0 ? (
                                            <div className="text-center py-12 text-muted-foreground">
                                                {stats?.total_mentions === 0
                                                    ? (hideScanActions ? '暂无数据，请先在右侧执行“开始数据分析”' : '暂无数据，请先执行"开始数据分析"')
                                                    : '暂无包含股票的话题'}
                                            </div>
                                        ) : topics.map((topic: any) => (
                                            <Card key={topic.topic_id} className="overflow-hidden hover:border-primary/20 transition-colors">
                                                <CardContent className="p-4 space-y-3">
                                                    {/* Topic Header: Time & Text */}
                                                    <div className="space-y-1">
                                                        <div className="flex items-center gap-2 text-xs text-muted-foreground">
                                                            <Clock className="h-3 w-3" />
                                                            {topic.create_time}
                                                        </div>
                                                        <div className="text-sm text-foreground/90 whitespace-pre-wrap break-words">
                                                            {(() => {
                                                                const isExpanded = expandedOverviewTopics.has(String(topic.topic_id));
                                                                const fullText = topic.text || '';
                                                                const previewText = fullText.length > 220 ? `${fullText.slice(0, 220)}...` : fullText;
                                                                return isExpanded ? fullText : previewText;
                                                            })()}
                                                        </div>
                                                        {(topic.text?.length || 0) > 220 && (
                                                            <div className="flex justify-end">
                                                                <Button
                                                                    size="sm"
                                                                    variant="ghost"
                                                                    className="h-6 px-2 text-xs"
                                                                    onClick={() => toggleOverviewTopicExpand(topic.topic_id)}
                                                                >
                                                                    {expandedOverviewTopics.has(String(topic.topic_id)) ? '收起' : '展开全部'}
                                                                </Button>
                                                            </div>
                                                        )}
                                                    </div>

                                                    {/* Stock List with Performance */}
                                                    <div className="pt-2 border-t border-border/50">
                                                        <div className="flex flex-col gap-2">
                                                            {(topic.mentions || []).map((m: any, idx: number) => (
                                                                <div key={idx} className="flex items-center gap-3 bg-muted/20 p-2 rounded-md hover:bg-muted/40 transition-colors cursor-pointer group/stock"
                                                                    onClick={() => openStockDetail(m.stock_code)}>

                                                                    {/* Stock Info */}
                                                                    <div className="flex items-center gap-2 min-w-[140px]">
                                                                        <Badge variant="outline" className="font-normal bg-background group-hover/stock:border-primary/50 transition-colors">
                                                                            {m.stock_name}
                                                                            <span className="ml-1 opacity-50 text-[10px]">{m.stock_code}</span>
                                                                        </Badge>
                                                                    </div>

                                                                    {/* Performance Matrix */}
                                                                    <div className="flex-1 grid grid-cols-5 gap-2 text-xs">
                                                                        <div className="flex flex-col items-center">
                                                                            <span className="text-[10px] text-muted-foreground uppercase opacity-70">T+1</span>
                                                                            <span className={`font-mono font-medium ${pctColor(m.return_1d)}`}>{fmtPct(m.return_1d)}</span>
                                                                        </div>
                                                                        <div className="flex flex-col items-center">
                                                                            <span className="text-[10px] text-muted-foreground uppercase opacity-70">T+3</span>
                                                                            <span className={`font-mono font-medium ${pctColor(m.return_3d)}`}>{fmtPct(m.return_3d)}</span>
                                                                        </div>
                                                                        <div className="flex flex-col items-center">
                                                                            <span className="text-[10px] text-muted-foreground uppercase opacity-70">T+5</span>
                                                                            <span className={`font-mono font-medium ${pctColor(m.return_5d)}`}>{fmtPct(m.return_5d)}</span>
                                                                        </div>
                                                                        <div className="flex flex-col items-center">
                                                                            <span className="text-[10px] text-muted-foreground uppercase opacity-70">T+10</span>
                                                                            <span className={`font-mono font-medium ${pctColor(m.return_10d)}`}>{fmtPct(m.return_10d)}</span>
                                                                        </div>
                                                                        <div className="flex flex-col items-center">
                                                                            <span className="text-[10px] text-muted-foreground uppercase opacity-70">T+20</span>
                                                                            <span className={`font-mono font-medium ${pctColor(m.return_20d)}`}>{fmtPct(m.return_20d)}</span>
                                                                        </div>
                                                                    </div>
                                                                </div>
                                                            ))}
                                                        </div>
                                                    </div>
                                                </CardContent>
                                            </Card>
                                        ))}
                                    </div>
                                )}

                                {/* pagination */}
                                {totalMentionPages > 1 && (
                                    <div className="flex items-center justify-center gap-2">
                                        <Button
                                            size="sm"
                                            variant="ghost"
                                            disabled={mentionPage <= 1}
                                            onClick={() => setMentionPage(p => p - 1)}
                                        >
                                            <ChevronLeft className="h-4 w-4" />
                                        </Button>
                                        <span className="text-xs text-muted-foreground">
                                            {mentionPage} / {totalMentionPages}
                                        </span>
                                        <Button
                                            size="sm"
                                            variant="ghost"
                                            disabled={mentionPage >= totalMentionPages}
                                            onClick={() => setMentionPage(p => p + 1)}
                                        >
                                            <ChevronRight className="h-4 w-4" />
                                        </Button>
                                    </div>
                                )}
                            </div>
                        )}
                    </div>
                )}

                {/* ── WIN-RATE RANKING ── */}
                {activeView === 'winrate' && (
                    <div className="space-y-3">
                        {/* Search bar for win rate (only when no external search) */}
                        {externalSearchTerm === undefined && (
                            <div className="flex items-center gap-2 bg-background z-10">
                                <div className="relative flex-1">
                                    <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                                    <Input
                                        placeholder="搜索股票代码或名称..."
                                        className="pl-9 h-9"
                                        value={searchStock}
                                        onChange={e => { setSearchStock(e.target.value); }}
                                    />
                                </div>
                                {searchStock && <span className="text-xs text-muted-foreground whitespace-nowrap">匹配 {winRate.filter((w: any) => {
                                    const q = searchStock.toLowerCase();
                                    return (w.stock_name || '').toLowerCase().includes(q) || (w.stock_code || '').toLowerCase().includes(q);
                                }).length} / {winRate.length}</span>}
                            </div>
                        )}
                        <div className="flex items-center gap-2 flex-wrap justify-between">
                            <div className="flex items-center gap-2 flex-wrap">
                                <span className="text-xs text-muted-foreground">收益周期:</span>
                                <Select value={returnPeriod} onValueChange={v => { setReturnPeriod(v); setWinRatePage(1); }}>
                                    <SelectTrigger className="w-24 h-7 text-xs">
                                        <SelectValue />
                                    </SelectTrigger>
                                    <SelectContent>
                                        <SelectItem value="return_1d">T+1</SelectItem>
                                        <SelectItem value="return_3d">T+3</SelectItem>
                                        <SelectItem value="return_5d">T+5</SelectItem>
                                        <SelectItem value="return_10d">T+10</SelectItem>
                                        <SelectItem value="return_20d">T+20</SelectItem>
                                    </SelectContent>
                                </Select>
                                <span className="text-xs text-muted-foreground">最少提及:</span>
                                <Select value={String(winRateMinMentions)} onValueChange={(v) => { setWinRateMinMentions(Number(v)); setWinRatePage(1); }}>
                                    <SelectTrigger className="w-20 h-7 text-xs">
                                        <SelectValue />
                                    </SelectTrigger>
                                    <SelectContent>
                                        <SelectItem value="1">1次</SelectItem>
                                        <SelectItem value="2">2次</SelectItem>
                                        <SelectItem value="3">3次</SelectItem>
                                        <SelectItem value="5">5次</SelectItem>
                                        <SelectItem value="10">10次</SelectItem>
                                    </SelectContent>
                                </Select>
                                <span className="text-[11px] text-muted-foreground">当前门槛会影响可见股票数量</span>
                            </div>

                            <TimeRangePicker
                                range={winRateRange}
                                start={winRateStart}
                                end={winRateEnd}
                                onRangeChange={setWinRateRange}
                                onStartChange={setWinRateStart}
                                onEndChange={setWinRateEnd}
                            />
                        </div>

                        <div className="rounded-md border overflow-hidden">
                            <table className="w-full text-xs">
                                <thead>
                                    <tr className="bg-muted/40 text-muted-foreground">
                                        <th className="text-left p-2 font-medium">#</th>
                                        <th className="text-left p-2 font-medium">股票</th>
                                        <th
                                            className="text-right p-2 font-medium cursor-pointer hover:bg-muted/60 transition-colors select-none group"
                                            onClick={() => {
                                                if (winRateSortColumn === 'latest_mention') {
                                                    setWinRateSortOrder(winRateSortOrder === 'desc' ? 'asc' : 'desc');
                                                } else {
                                                    setWinRateSortColumn('latest_mention');
                                                    setWinRateSortOrder('desc');
                                                }
                                                setWinRatePage(1);
                                            }}
                                        >
                                            <div className="flex items-center justify-end gap-1">
                                                <span className="border-b border-transparent group-hover:border-muted-foreground/30 border-dotted">最后提及时间</span>
                                                <span className="text-[10px] opacity-50 w-2 flex justify-center">{winRateSortColumn === 'latest_mention' ? (winRateSortOrder === 'desc' ? '↓' : '↑') : ''}</span>
                                            </div>
                                        </th>
                                        <th
                                            className="text-right p-2 font-medium cursor-pointer hover:bg-muted/60 transition-colors select-none group"
                                            onClick={() => {
                                                if (winRateSortColumn === 'total_mentions') {
                                                    setWinRateSortOrder(winRateSortOrder === 'desc' ? 'asc' : 'desc');
                                                } else {
                                                    setWinRateSortColumn('total_mentions');
                                                    setWinRateSortOrder('desc');
                                                }
                                                setWinRatePage(1);
                                            }}
                                        >
                                            <div className="flex items-center justify-end gap-1">
                                                <span className="border-b border-transparent group-hover:border-muted-foreground/30 border-dotted">提及次数</span>
                                                <span className="text-[10px] opacity-50 w-2 flex justify-center">{winRateSortColumn === 'total_mentions' ? (winRateSortOrder === 'desc' ? '↓' : '↑') : ''}</span>
                                            </div>
                                        </th>
                                        <th
                                            className="text-right p-2 font-medium cursor-pointer hover:bg-muted/60 transition-colors select-none group"
                                            onClick={() => {
                                                if (winRateSortColumn === 'win_rate') {
                                                    setWinRateSortOrder(winRateSortOrder === 'desc' ? 'asc' : 'desc');
                                                } else {
                                                    setWinRateSortColumn('win_rate');
                                                    setWinRateSortOrder('desc');
                                                }
                                                setWinRatePage(1);
                                            }}
                                        >
                                            <div className="flex items-center justify-end gap-1">
                                                <span className="border-b border-transparent group-hover:border-muted-foreground/30 border-dotted">胜率</span>
                                                <span className="text-[10px] opacity-50 w-2 flex justify-center">{winRateSortColumn === 'win_rate' ? (winRateSortOrder === 'desc' ? '↓' : '↑') : ''}</span>
                                            </div>
                                        </th>
                                        <th
                                            className="text-right p-2 font-medium cursor-pointer hover:bg-muted/60 transition-colors select-none group"
                                            onClick={() => {
                                                if (winRateSortColumn === 'avg_return') {
                                                    setWinRateSortOrder(winRateSortOrder === 'desc' ? 'asc' : 'desc');
                                                } else {
                                                    setWinRateSortColumn('avg_return');
                                                    setWinRateSortOrder('desc');
                                                }
                                                setWinRatePage(1);
                                            }}
                                        >
                                            <div className="flex items-center justify-end gap-1">
                                                <span>平均收益</span>
                                                <HeaderInfo text="个股提及后在所选周期内的平均收益率" />
                                                <span className="text-[10px] opacity-50 w-2 flex justify-center">{winRateSortColumn === 'avg_return' ? (winRateSortOrder === 'desc' ? '↓' : '↑') : ''}</span>
                                            </div>
                                        </th>
                                        <th
                                            className="text-right p-2 font-medium cursor-pointer hover:bg-muted/60 transition-colors select-none group"
                                            onClick={() => {
                                                if (winRateSortColumn === 'avg_benchmark_return') {
                                                    setWinRateSortOrder(winRateSortOrder === 'desc' ? 'asc' : 'desc');
                                                } else {
                                                    setWinRateSortColumn('avg_benchmark_return');
                                                    setWinRateSortOrder('desc');
                                                }
                                                setWinRatePage(1);
                                            }}
                                        >
                                            <div className="flex items-center justify-end gap-1">
                                                <span>同期沪深300涨幅</span>
                                                <HeaderInfo text="由个股收益与超额收益推导出的同期基准收益" />
                                                <span className="text-[10px] opacity-50 w-2 flex justify-center">{winRateSortColumn === 'avg_benchmark_return' ? (winRateSortOrder === 'desc' ? '↓' : '↑') : ''}</span>
                                            </div>
                                        </th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {winRate.length === 0 ? (
                                        <tr><td colSpan={7} className="text-center py-8 text-muted-foreground">暂无胜率数据</td></tr>
                                    ) : (searchStock ? winRate.filter((w: any) => {
                                        const q = searchStock.toLowerCase();
                                        return (w.stock_name || '').toLowerCase().includes(q) || (w.stock_code || '').toLowerCase().includes(q);
                                    }) : winRate).map((w: any, i: number) => (
                                        <tr
                                            key={w.stock_code}
                                            className="border-t border-border/50 hover:bg-muted/20 transition-colors cursor-pointer"
                                            onClick={() => openStockDetail(w.stock_code)}
                                        >
                                            <td className="p-2 text-muted-foreground">{(winRatePage - 1) * winRatePageSize + i + 1}</td>
                                            <td className="p-2">
                                                <span className="font-medium">{w.stock_name}</span>
                                                <span className="ml-1 text-muted-foreground">{w.stock_code}</span>
                                            </td>
                                            <td className="p-2 text-right text-muted-foreground">
                                                {w.latest_mention ? new Date(w.latest_mention).toLocaleDateString() : '—'}
                                            </td>
                                            <td className="p-2 text-right">{w.total_mentions}</td>
                                            <td className="p-2 text-right">
                                                <span className={`font-mono font-medium ${w.win_rate > 60 ? 'text-emerald-500' :
                                                    w.win_rate < 40 ? 'text-red-500' :
                                                        'text-foreground'
                                                    }`}>
                                                    {w.win_rate.toFixed(1)}%
                                                </span>
                                            </td>
                                            <td className={`p-2 text-right font-mono ${pctColor(w.avg_return)}`}>
                                                {fmtPct(w.avg_return)}
                                            </td>
                                            <td className={`p-2 text-right font-mono ${pctColor(w.avg_benchmark_return)}`}>
                                                {fmtPct(w.avg_benchmark_return)}
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>

                        {/* Win rate pagination */}
                        {winRateTotal > winRatePageSize && (
                            <div className="flex items-center justify-center gap-2">
                                <Button
                                    size="sm"
                                    variant="ghost"
                                    disabled={winRatePage <= 1}
                                    onClick={() => setWinRatePage(p => p - 1)}
                                >
                                    <ChevronLeft className="h-4 w-4" />
                                </Button>
                                <span className="text-xs text-muted-foreground">
                                    {winRatePage} / {Math.ceil(winRateTotal / winRatePageSize)} (共 {winRateTotal} 条)
                                </span>
                                <Button
                                    size="sm"
                                    variant="ghost"
                                    disabled={winRatePage >= Math.ceil(winRateTotal / winRatePageSize)}
                                    onClick={() => setWinRatePage(p => p + 1)}
                                >
                                    <ChevronRight className="h-4 w-4" />
                                </Button>
                            </div>
                        )}
                    </div>
                )}

                {/* ── SECTOR HEATMAP ── */}
                {activeView === 'sector' && (
                    <div className="space-y-3">
                        <div className="flex justify-between items-center">
                            <h3 className="text-sm font-medium">板块热度</h3>
                            <TimeRangePicker
                                range={sectorRange}
                                start={sectorStart}
                                end={sectorEnd}
                                onRangeChange={setSectorRange}
                                onStartChange={setSectorStart}
                                onEndChange={setSectorEnd}
                            />
                        </div>
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                            {sectors.length === 0 ? (
                                <div className="col-span-full text-center py-8 text-muted-foreground font-mono text-xs">
                                    暂无板块数据
                                </div>
                            ) : sectors.map(s => (
                                <Card
                                    key={s.sector}
                                    className="hover:border-primary/50 transition-colors cursor-pointer"
                                    onClick={() => {
                                        setSelectedSector(s);
                                        setSectorTopicsPage(1);
                                        setExpandedSectorTopics(new Set());
                                    }}
                                >
                                    <CardContent className="p-3">
                                        <div className="flex justify-between items-center mb-2">
                                            <span className="font-medium text-sm">{s.sector}</span>
                                            <span className="text-xs text-muted-foreground">{s.total_mentions} 提及</span>
                                        </div>
                                        <HeatBar value={s.total_mentions} max={sectorMaxMentions} label="热度" />
                                        {/* peak info */}
                                        <div className="mt-2 text-[10px] text-muted-foreground flex justify-between">
                                            <span>峰值: {s.peak_count} ({s.peak_date})</span>
                                        </div>
                                    </CardContent>
                                </Card>
                            ))}
                        </div>
                    </div>
                )}

                {/* ── SIGNALS ── */}
                {activeView === 'signals' && (
                    <div className="space-y-3">
                        <div className="flex justify-between items-center">
                            <div className="flex items-center gap-2 flex-wrap">
                                <h3 className="text-sm font-medium">信号雷达</h3>
                                <span className="text-xs text-muted-foreground">最少提及:</span>
                                <Select value={String(signalMinMentions)} onValueChange={(v) => setSignalMinMentions(Number(v))}>
                                    <SelectTrigger className="w-20 h-7 text-xs">
                                        <SelectValue />
                                    </SelectTrigger>
                                    <SelectContent>
                                        <SelectItem value="1">1次</SelectItem>
                                        <SelectItem value="2">2次</SelectItem>
                                        <SelectItem value="3">3次</SelectItem>
                                        <SelectItem value="5">5次</SelectItem>
                                        <SelectItem value="10">10次</SelectItem>
                                    </SelectContent>
                                </Select>
                            </div>
                            <TimeRangePicker
                                range={signalRange}
                                start={signalStart}
                                end={signalEnd}
                                onRangeChange={setSignalRange}
                                onStartChange={setSignalStart}
                                onEndChange={setSignalEnd}
                            />
                        </div>
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                            {signals.length === 0 ? (
                                <div className="col-span-full text-center py-8 text-muted-foreground font-mono text-xs">
                                    暂无信号数据
                                </div>
                            ) : signals.map(s => (
                                <Card key={s.stock_code} className="hover:border-primary/50 transition-colors cursor-pointer" onClick={() => openStockDetail(s.stock_code)}>
                                    <CardContent className="p-3 space-y-2">
                                        <div className="flex justify-between items-start gap-3">
                                            <div className="min-w-0 flex-1">
                                                <div className="font-semibold text-sm leading-5 break-words">
                                                    {s.stock_name || '—'}
                                                </div>
                                                <div className="mt-1">
                                                    <span className="inline-flex text-[11px] px-2 py-0.5 rounded-full bg-muted text-muted-foreground font-mono">
                                                        {s.stock_code}
                                                    </span>
                                                </div>
                                                <div className="text-xs text-muted-foreground mt-0.5">
                                                    最近提及: {s.latest_mention ? new Date(s.latest_mention).toLocaleDateString() : '—'}
                                                </div>
                                            </div>
                                            <Badge
                                                variant={s.historical_win_rate >= 60 ? 'default' : 'secondary'}
                                                className="shrink-0 whitespace-nowrap"
                                            >
                                                胜率 {s.historical_win_rate ?? '—'}%
                                            </Badge>
                                        </div>
                                        <div className="grid grid-cols-2 gap-2 text-xs bg-muted/20 p-2 rounded">
                                            <div>
                                                <span className="text-muted-foreground block">近期提及</span>
                                                <span className="font-mono font-medium">{s.recent_mentions}</span>
                                            </div>
                                            <div className="text-right">
                                                <span className="text-muted-foreground block">历史均收</span>
                                                <span className={`font-mono font-medium ${pctColor(s.historical_avg_return)}`}>
                                                    {fmtPct(s.historical_avg_return)}
                                                </span>
                                            </div>
                                        </div>
                                    </CardContent>
                                </Card>
                            ))}
                        </div>
                    </div>
                )}

                {/* ── AI ANALYSIS VIEW ── */}
                {activeView === 'ai' && (
                    <div className="space-y-4">
                        {/* AI Config Banner */}
                        {aiConfig && !aiConfig.configured && !showAiConfig && (
                            <Card className="border-amber-500/30 bg-amber-500/5">
                                <CardContent className="p-4 flex items-center justify-between">
                                    <div className="flex items-center gap-3">
                                        <Settings className="h-5 w-5 text-amber-500" />
                                        <div>
                                            <p className="text-sm font-medium">需要配置 DeepSeek API Key</p>
                                            <p className="text-xs text-muted-foreground">配置后即可使用 AI 智能分析功能</p>
                                        </div>
                                    </div>
                                    <Button size="sm" variant="outline" onClick={() => setShowAiConfig(true)} className="gap-1">
                                        <Settings className="h-3.5 w-3.5" /> 配置
                                    </Button>
                                </CardContent>
                            </Card>
                        )}

                        {/* AI Config Form */}
                        {showAiConfig && (
                            <Card className="border-blue-500/30">
                                <CardContent className="p-4 space-y-3">
                                    <div className="flex items-center justify-between">
                                        <h4 className="text-sm font-semibold flex items-center gap-2">
                                            <Bot className="h-4 w-4" /> DeepSeek API 配置
                                        </h4>
                                        <Button size="sm" variant="ghost" onClick={() => setShowAiConfig(false)}>✕</Button>
                                    </div>
                                    <div className="space-y-2">
                                        <div>
                                            <label className="text-xs text-muted-foreground">API Key *</label>
                                            <Input
                                                placeholder="sk-..."
                                                className="h-8 text-xs font-mono"
                                                type="password"
                                                value={aiConfigKey}
                                                onChange={e => setAiConfigKey(e.target.value)}
                                            />
                                        </div>
                                        <div className="grid grid-cols-2 gap-2">
                                            <div>
                                                <label className="text-xs text-muted-foreground">Base URL</label>
                                                <Input
                                                    className="h-8 text-xs"
                                                    value={aiConfigBaseUrl}
                                                    onChange={e => setAiConfigBaseUrl(e.target.value)}
                                                />
                                            </div>
                                            <div>
                                                <label className="text-xs text-muted-foreground">Model</label>
                                                <Input
                                                    className="h-8 text-xs"
                                                    value={aiConfigModel}
                                                    onChange={e => setAiConfigModel(e.target.value)}
                                                />
                                            </div>
                                        </div>
                                        <Button
                                            size="sm"
                                            className="w-full gap-1"
                                            disabled={!aiConfigKey.trim()}
                                            onClick={async () => {
                                                try {
                                                    await apiClient.updateAIConfig({
                                                        api_key: aiConfigKey,
                                                        base_url: aiConfigBaseUrl || undefined,
                                                        model: aiConfigModel || undefined
                                                    });
                                                    toast.success('AI 配置已保存');
                                                    const cfg = await apiClient.getAIConfig();
                                                    setAiConfig(cfg);
                                                    setShowAiConfig(false);
                                                } catch {
                                                    toast.error('保存配置失败');
                                                }
                                            }}
                                        >
                                            保存配置
                                        </Button>
                                    </div>
                                </CardContent>
                            </Card>
                        )}

                        {/* AI Config Status */}
                        {aiConfig?.configured && (
                            <div className="flex items-center justify-between">
                                <div className="flex items-center gap-2 text-xs text-muted-foreground">
                                    <div className="h-2 w-2 rounded-full bg-emerald-500 animate-pulse" />
                                    <span>已连接 {aiConfig.model}</span>
                                    <span className="opacity-50">|</span>
                                    <span>{aiConfig.api_key_preview}</span>
                                </div>
                                <Button size="sm" variant="ghost" className="h-6 text-xs gap-1" onClick={() => setShowAiConfig(true)}>
                                    <Settings className="h-3 w-3" /> 修改
                                </Button>
                            </div>
                        )}

                        {/* Analysis Cards */}
                        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                            {/* Stock Analysis (Only for group mode for now, or update if endpoint supports global) */}
                            <Card className={`bg-gradient-to-br from-violet-500/10 to-purple-500/5 border-violet-500/20 hover:border-violet-500/40 transition-colors ${isGlobal ? 'opacity-50' : ''}`}>
                                <CardContent className="p-4 space-y-3">
                                    <div className="flex items-center gap-2">
                                        <Target className="h-4 w-4 text-violet-400" />
                                        <span className="text-sm font-semibold">单股分析</span>
                                    </div>
                                    <p className="text-xs text-muted-foreground">AI 分析某只股票的全部提及数据和表现</p>
                                    <div className="flex gap-2">
                                        <Input
                                            placeholder="输入股票代码 如 300579.SZ"
                                            className="h-8 text-xs flex-1"
                                            value={aiStockInput}
                                            onChange={e => setAiStockInput(e.target.value)}
                                            disabled={isGlobal} // Disable for global for now as we don't have global stock events aggregation yet
                                        />
                                        <Button
                                            size="sm"
                                            disabled={isGlobal || !aiStockInput.trim() || aiLoading || !aiConfig?.configured}
                                            onClick={async () => {
                                                setAiLoading(true);
                                                setAiResult(null);
                                                try {
                                                    const res = await apiClient.aiAnalyzeStock(groupId!, aiStockInput.trim());
                                                    setAiResult({ type: 'stock', ...res });
                                                    apiClient.getAIHistory(groupId!).then(setAiHistory).catch(() => { });
                                                } catch (e: any) {
                                                    toast.error(e.message || 'AI分析失败');
                                                } finally {
                                                    setAiLoading(false);
                                                }
                                            }}
                                            className="gap-1"
                                        >
                                            <Send className="h-3.5 w-3.5" />
                                        </Button>
                                    </div>
                                    {isGlobal && <p className="text-[10px] text-red-400">全局模式暂不支持单股透视</p>}
                                </CardContent>
                            </Card>

                            {/* Daily Brief */}
                            <Card className="bg-gradient-to-br from-cyan-500/10 to-blue-500/5 border-cyan-500/20 hover:border-cyan-500/40 transition-colors">
                                <CardContent className="p-4 space-y-3">
                                    <div className="flex items-center gap-2">
                                        <FileTextIcon className="h-4 w-4 text-cyan-400" />
                                        <span className="text-sm font-semibold">每日简报</span>
                                    </div>
                                    <p className="text-xs text-muted-foreground">汇总近期信号，生成投资观察报告</p>
                                    <Button
                                        size="sm"
                                        variant="outline"
                                        className="w-full gap-1"
                                        disabled={aiLoading || !aiConfig?.configured}
                                        onClick={async () => {
                                            setAiLoading(true);
                                            setAiResult(null);
                                            try {
                                                const res = isGlobal
                                                    ? await apiClient.aiGlobalDailyBrief()
                                                    : await apiClient.aiDailyBrief(groupId!);

                                                setAiResult({ type: 'daily', ...res });
                                                if (isGlobal) {
                                                    apiClient.getGlobalAIHistory().then(setAiHistory).catch(() => { });
                                                } else {
                                                    apiClient.getAIHistory(groupId!).then(setAiHistory).catch(() => { });
                                                }
                                            } catch (e: any) {
                                                toast.error(e.message || '生成简报失败');
                                            } finally {
                                                setAiLoading(false);
                                            }
                                        }}
                                    >
                                        <Sparkles className="h-3.5 w-3.5" /> 生成简报
                                    </Button>
                                </CardContent>
                            </Card>

                            {/* Consensus */}
                            <Card className="bg-gradient-to-br from-amber-500/10 to-orange-500/5 border-amber-500/20 hover:border-amber-500/40 transition-colors">
                                <CardContent className="p-4 space-y-3">
                                    <div className="flex items-center gap-2">
                                        <Users className="h-4 w-4 text-amber-400" />
                                        <span className="text-sm font-semibold">共识分析</span>
                                    </div>
                                    <p className="text-xs text-muted-foreground">对比热门股票，寻找市场共识和分歧</p>
                                    <Button
                                        size="sm"
                                        variant="outline"
                                        className="w-full gap-1"
                                        disabled={aiLoading || !aiConfig?.configured}
                                        onClick={async () => {
                                            setAiLoading(true);
                                            setAiResult(null);
                                            try {
                                                const res = isGlobal
                                                    ? await apiClient.aiGlobalConsensus()
                                                    : await apiClient.aiConsensus(groupId!);

                                                setAiResult({ type: 'consensus', ...res });
                                                if (isGlobal) {
                                                    apiClient.getGlobalAIHistory().then(setAiHistory).catch(() => { });
                                                } else {
                                                    apiClient.getAIHistory(groupId!).then(setAiHistory).catch(() => { });
                                                }
                                            } catch (e: any) {
                                                toast.error(e.message || '共识分析失败');
                                            } finally {
                                                setAiLoading(false);
                                            }
                                        }}
                                    >
                                        <Sparkles className="h-3.5 w-3.5" /> 生成分析
                                    </Button>
                                </CardContent>
                            </Card>
                        </div>

                        {/* Result Area */}
                        {aiLoading && (
                            <div className="flex flex-col items-center justify-center p-12 text-muted-foreground">
                                <Loader2 className="h-8 w-8 animate-spin mb-2" />
                                <p className="text-sm">正在深入分析数据，请稍候...</p>
                                <p className="text-xs opacity-70">DeepSeek 正在思考中</p>
                            </div>
                        )}

                        {aiResult && !aiLoading && (
                            <div className="animate-in fade-in slide-in-from-bottom-4 duration-500">
                                <Card className="border-primary/20 bg-primary/5">
                                    <CardContent className="p-5">
                                        <div className="flex items-center justify-between mb-4 pb-2 border-b border-border/50">
                                            <div className="flex items-center gap-2">
                                                <Bot className="h-5 w-5 text-primary" />
                                                <h3 className="font-semibold text-lg">分析报告</h3>
                                            </div>
                                            <div className="text-xs text-muted-foreground flex items-center gap-3">
                                                <span>Model: {aiResult.model}</span>
                                                <span className="bg-background/50 px-2 py-0.5 rounded">
                                                    Tokens: {aiResult.tokens_used}
                                                </span>
                                            </div>
                                        </div>
                                        <div
                                            className="prose prose-sm dark:prose-invert max-w-none text-sm"
                                            dangerouslySetInnerHTML={{ __html: simpleMarkdown(aiResult.content) }}
                                        />
                                    </CardContent>
                                </Card>
                            </div>
                        )}

                        {/* History List */}
                        {aiHistory.length > 0 && (
                            <div className="space-y-2 mt-4">
                                <h4 className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                                    <History className="h-4 w-4" /> 历史分析
                                </h4>
                                <div className="space-y-2 max-h-60 overflow-y-auto pr-1">
                                    {aiHistory.map((h: any) => (
                                        <div
                                            key={h.id}
                                            className="text-xs p-3 rounded border hover:bg-muted/50 cursor-pointer transition-colors flex justify-between items-center group"
                                            onClick={async () => {
                                                setAiResult(null);
                                                setAiLoading(true);
                                                try {
                                                    // Handle fetching detail based on mode
                                                    const detail = isGlobal
                                                        ? await apiClient.getGlobalAIHistoryDetail(h.id)
                                                        : await apiClient.getAIHistoryDetail(groupId!, h.id);
                                                    setAiResult({ type: 'history', ...detail });
                                                    window.scrollTo({ top: 0, behavior: 'smooth' });
                                                } catch {
                                                    toast.error('无法加载历史记录');
                                                } finally {
                                                    setAiLoading(false);
                                                }
                                            }}
                                        >
                                            <div className="space-y-1">
                                                <div className="font-medium flex items-center gap-2">
                                                    <Badge variant="outline" className="text-[10px] h-5">
                                                        {h.summary_type === 'stock' ? '单股' :
                                                            h.summary_type === 'global_daily' ? '全局日报' :
                                                                h.summary_type === 'global_consensus' ? '全局共识' :
                                                                    h.summary_type === 'daily' ? '日报' : '共识'}
                                                    </Badge>
                                                    <span>{h.target_key || '综合分析'}</span>
                                                </div>
                                                <div className="text-muted-foreground line-clamp-1">{h.preview}</div>
                                            </div>
                                            <div className="text-right text-muted-foreground opacity-70 flex flex-col items-end gap-1">
                                                <span>{new Date(h.created_at).toLocaleDateString()}</span>
                                                <span className="text-[10px]">{new Date(h.created_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}</span>
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}
                    </div>
                )}
            </div>

            {/* ─── Detail Drawer ─── */}
            <StockDetailDrawer
                stockCode={selectedStock}
                groupId={groupId}
                onClose={() => setSelectedStock(null)}
            />

            <Sheet open={!!selectedSector} onOpenChange={(open) => {
                if (!open) {
                    setSelectedSector(null);
                    setSectorTopics([]);
                    setSectorTopicsTotal(0);
                    setSectorTopicsPage(1);
                    setExpandedSectorTopics(new Set());
                }
            }}>
                <SheetContent side="right" className="!max-w-none w-[100vw] sm:w-[85vw] md:w-[70vw] lg:w-[60vw] xl:w-[50vw] p-0 shadow-2xl">
                    <SheetHeader className="px-6 pt-5 pb-3 border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/80">
                        <SheetTitle className="flex items-center justify-between gap-2">
                            <span className="truncate text-base">{selectedSector?.sector || '板块详情'}</span>
                            <div className="flex items-center gap-1.5">
                                <Badge variant="outline" className="text-[11px]">{selectedSector?.total_mentions ?? 0} 提及</Badge>
                                <Badge variant="secondary" className="text-[11px]">页 {sectorTopicsPage}</Badge>
                            </div>
                        </SheetTitle>
                        <SheetDescription className="text-[11px] leading-relaxed">
                            峰值 {selectedSector?.peak_count ?? 0}（{selectedSector?.peak_date || '—'}） · 时间范围 {sectorStart} ~ {sectorEnd}
                        </SheetDescription>
                    </SheetHeader>

                    <div ref={sectorDrawerScrollRef} className="px-6 py-4 space-y-3 overflow-y-auto h-[calc(100vh-96px)]">
                        <div className="grid grid-cols-2 lg:grid-cols-4 gap-2">
                            <Card>
                                <CardContent className="p-3">
                                    <div className="text-[11px] text-muted-foreground">命中话题</div>
                                    <div className="text-lg font-semibold mt-1 leading-none">{sectorTopicsTotal}</div>
                                </CardContent>
                            </Card>
                            <Card>
                                <CardContent className="p-3">
                                    <div className="text-[11px] text-muted-foreground flex items-center gap-1">
                                        关键词
                                        <HeaderInfo text="当前时间范围和当前页话题中命中的不重复关键词数量。" />
                                    </div>
                                    <div className="text-lg font-semibold mt-1 leading-none">{sectorUniqueKeywords.size}</div>
                                </CardContent>
                            </Card>
                            <Card>
                                <CardContent className="p-3">
                                    <div className="text-[11px] text-muted-foreground">峰值提及</div>
                                    <div className="text-lg font-semibold mt-1 leading-none">{selectedSector?.peak_count ?? 0}</div>
                                </CardContent>
                            </Card>
                            <Card>
                                <CardContent className="p-3">
                                    <div className="text-[11px] text-muted-foreground">峰值日期</div>
                                    <div className="text-sm font-semibold mt-1 truncate">{selectedSector?.peak_date || '—'}</div>
                                </CardContent>
                            </Card>
                        </div>

                        <Card>
                            <CardContent className="p-3 space-y-2">
                                <div className="flex items-center justify-between">
                                    <div className="text-sm font-medium">时间热度（月视图）</div>
                                    <div className="text-[11px] text-muted-foreground">按月聚合</div>
                                </div>
                                <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-4 gap-2">
                                    {Object.entries(sectorMonthlyStats)
                                        .sort((a, b) => b[0].localeCompare(a[0]))
                                        .map(([month, stat]) => (
                                            <div key={month} className="rounded-md border px-2.5 py-2">
                                                <div className="flex items-center justify-between text-[11px]">
                                                    <span className="font-medium">{month}</span>
                                                    <span className="text-muted-foreground">峰值 {stat.peak}</span>
                                                </div>
                                                <div className="mt-1.5 flex items-end justify-between">
                                                    <div className="text-base font-semibold font-mono leading-none">{stat.total}</div>
                                                    <div className="text-[10px] text-muted-foreground">{stat.days} 天</div>
                                                </div>
                                                <div className="mt-1 text-[10px] text-muted-foreground">
                                                    日均 {(stat.total / Math.max(stat.days, 1)).toFixed(1)}
                                                </div>
                                            </div>
                                        ))}
                                </div>
                            </CardContent>
                        </Card>

                        <Card>
                            <CardContent className="p-3">
                                <div className="flex items-center justify-between">
                                    <div className="text-sm font-medium">提及话题时间线</div>
                                    <span className="text-[11px] text-muted-foreground">共 {sectorTopicsTotal} 条（每页 {sectorTopicsPageSize} 条）</span>
                                </div>
                                <div className="text-[11px] text-muted-foreground mt-1">
                                    按时间倒序，展示摘要、命中关键词、关联股票和话题标识
                                </div>
                            </CardContent>
                        </Card>

                        {sectorTopicsLoading ? (
                            <div className="py-6 text-center text-xs text-muted-foreground">加载话题中...</div>
                        ) : sectorTopicsError ? (
                            <div className="py-6 text-center text-xs text-muted-foreground space-y-2">
                                <div>{sectorTopicsError}</div>
                                <Button size="sm" variant="outline" onClick={loadSectorTopics}>重试</Button>
                            </div>
                        ) : sectorTopics.length === 0 ? (
                            <div className="py-6 text-center text-xs text-muted-foreground">当前筛选下暂无命中话题</div>
                        ) : (
                            <div className="space-y-2.5">
                                {sectorTopics.map((topic) => {
                                    const topicId = String(topic.topic_id);
                                    const isExpanded = expandedSectorTopics.has(topicId);
                                    const displayText = isExpanded ? (topic.full_text || topic.text_snippet) : topic.text_snippet;
                                    const hasLongText = (topic.full_text?.length || topic.text_snippet.length) > topic.text_snippet.length;

                                    return (
                                        <div key={topicId} className="relative pl-5 border-l border-muted/60">
                                            <div className="absolute -left-[4px] top-2.5 h-2 w-2 rounded-full bg-primary ring-2 ring-background" />
                                            <Card>
                                                <CardContent className="p-3 space-y-2">
                                                    <div className="flex items-start justify-between gap-2">
                                                        <div className="space-y-1 min-w-0">
                                                            <div className="text-[11px] text-muted-foreground">
                                                                {topic.create_time ? new Date(topic.create_time).toLocaleString('zh-CN') : '—'}
                                                            </div>
                                                            {topic.group_id && (
                                                                <div className="text-[10px] text-muted-foreground flex items-center gap-1">
                                                                    <span>群组:</span>
                                                                    <Link
                                                                        href={`/groups/${topic.group_id}`}
                                                                        className="underline underline-offset-2 hover:text-primary"
                                                                    >
                                                                        {topic.group_name || topic.group_id}
                                                                    </Link>
                                                                </div>
                                                            )}
                                                            <div className="flex items-center gap-1.5 text-[10px] text-muted-foreground">
                                                                <span>关键词 {topic.matched_keywords.length}</span>
                                                                <span>·</span>
                                                                <span>关联股票 {topic.stocks.length}</span>
                                                            </div>
                                                        </div>
                                                        <Badge variant="outline" className="text-[10px] font-mono shrink-0">
                                                            topic {topicId}
                                                        </Badge>
                                                    </div>

                                                    <div className="rounded-md bg-muted/35 px-2.5 py-2">
                                                        <div className="text-xs whitespace-pre-wrap break-words leading-relaxed">
                                                            {displayText}
                                                        </div>
                                                    </div>

                                                    {hasLongText && (
                                                        <div className="flex justify-end">
                                                            <Button
                                                                size="sm"
                                                                variant="ghost"
                                                                className="h-6 px-2 text-[11px]"
                                                                onClick={() => {
                                                                    setExpandedSectorTopics(prev => {
                                                                        const next = new Set(prev);
                                                                        if (next.has(topicId)) next.delete(topicId);
                                                                        else next.add(topicId);
                                                                        return next;
                                                                    });
                                                                }}
                                                            >
                                                                {isExpanded ? '收起' : '展开全文'}
                                                            </Button>
                                                        </div>
                                                    )}

                                                    <div className="grid grid-cols-1 xl:grid-cols-2 gap-2">
                                                        <div className="space-y-1">
                                                            <div className="text-[10px] text-muted-foreground">命中关键词</div>
                                                            <div className="flex flex-wrap gap-1">
                                                                {topic.matched_keywords.length === 0 ? (
                                                                    <span className="text-[10px] text-muted-foreground">无</span>
                                                                ) : topic.matched_keywords.map((kw, idx) => (
                                                                    <Badge key={`${topicId}-kw-${idx}`} variant="secondary" className="text-[10px] px-1.5 py-0">
                                                                        {kw}
                                                                    </Badge>
                                                                ))}
                                                            </div>
                                                        </div>

                                                        <div className="space-y-1">
                                                            <div className="text-[10px] text-muted-foreground">关联股票</div>
                                                            <div className="flex flex-wrap gap-1">
                                                                {topic.stocks.length === 0 ? (
                                                                    <span className="text-[10px] text-muted-foreground">无关联股票</span>
                                                                ) : topic.stocks.map((stock) => (
                                                                    <Badge
                                                                        key={`${topicId}-${stock.stock_code}`}
                                                                        variant="outline"
                                                                        className="text-[10px] px-1.5 py-0 cursor-pointer hover:border-primary"
                                                                        onClick={() => openStockDetail(stock.stock_code)}
                                                                    >
                                                                        {stock.stock_name}
                                                                        <span className="ml-1 opacity-60 font-mono">{stock.stock_code}</span>
                                                                    </Badge>
                                                                ))}
                                                            </div>
                                                        </div>
                                                    </div>

                                                    <div className="flex justify-end">
                                                        <Button
                                                            size="sm"
                                                            variant="ghost"
                                                            className="h-6 px-2 text-[11px]"
                                                            onClick={() => toast.info(`话题ID: ${topicId}，可在概览中按时间定位原文`)}
                                                        >
                                                            查看话题原文
                                                        </Button>
                                                    </div>
                                                </CardContent>
                                            </Card>
                                        </div>
                                    );
                                })}
                            </div>
                        )}

                        {sectorTopicsTotal > sectorTopicsPageSize && (
                            <div className="sticky bottom-0 bg-background/95 backdrop-blur border rounded-md px-2 py-1.5 flex items-center justify-center gap-2">
                                <Button
                                    size="sm"
                                    variant="ghost"
                                    className="h-7 w-7 p-0"
                                    disabled={sectorTopicsPage <= 1}
                                    onClick={() => setSectorTopicsPage(p => p - 1)}
                                >
                                    <ChevronLeft className="h-4 w-4" />
                                </Button>
                                <span className="text-xs text-muted-foreground">
                                    {sectorTopicsPage} / {Math.ceil(sectorTopicsTotal / sectorTopicsPageSize)}
                                </span>
                                <Button
                                    size="sm"
                                    variant="ghost"
                                    className="h-7 w-7 p-0"
                                    disabled={sectorTopicsPage >= Math.ceil(sectorTopicsTotal / sectorTopicsPageSize)}
                                    onClick={() => setSectorTopicsPage(p => p + 1)}
                                >
                                    <ChevronRight className="h-4 w-4" />
                                </Button>
                            </div>
                        )}
                    </div>
                </SheetContent>
            </Sheet>
        </div>
    );
}

'use client';

/* eslint-disable @typescript-eslint/no-explicit-any */
import React, { useState, useEffect, useCallback } from 'react';
import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Input } from '@/components/ui/input';
import { toast } from 'sonner';
import { apiClient } from '@/lib/api';
import {
    TrendingUp, BarChart3, Search, RefreshCw,
    Activity, Target, Flame,
    Zap, Clock, ChevronRight, ChevronLeft, Loader2,
    Sparkles, Settings, Send, History, Bot, FileText as FileTextIcon, Users,
    Play
} from 'lucide-react';
import TaskLogViewer from './TaskLogViewer';
import StockDetailDrawer from './StockDetailDrawer';

interface StockDashboardProps {
    groupId?: number | string; // Optional for global mode
    mode?: 'group' | 'global';
    onTaskCreated?: (taskId: string) => void;
    hideScanActions?: boolean;
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
export default function StockDashboard({ groupId, mode = 'group', onTaskCreated, hideScanActions = false }: StockDashboardProps) {
    const isGlobal = mode === 'global';
    const [activeView, setActiveView] = useState<'overview' | 'winrate' | 'sector' | 'signals' | 'ai'>('overview');
    const [stats, setStats] = useState<any>(null);
    const [mentions, setMentions] = useState<any[]>([]); // Keeping for legacy or unused? Or maybe remove? Let's keep for search/pagination compatibility if needed or replace.
    const [topics, setTopics] = useState<any[]>([]); // New state for topics
    const [winRate, setWinRate] = useState<any[]>([]);
    const [sectors, setSectors] = useState<any[]>([]);
    const [signals, setSignals] = useState<any[]>([]);
    const [globalGroups, setGlobalGroups] = useState<any[]>([]);
    const [lastError, setLastError] = useState<string | null>(null);

    const [loading, setLoading] = useState(true);
    const [scanning, setScanning] = useState(false);
    const [scanTaskId, setScanTaskId] = useState<string | null>(null);
    const [showTaskLog, setShowTaskLog] = useState(false);

    const [mentionPage, setMentionPage] = useState(1);
    const [mentionTotal, setMentionTotal] = useState(0);
    const [returnPeriod, setReturnPeriod] = useState('return_5d');
    const [searchStock, setSearchStock] = useState('');

    // detail drawer state
    const [selectedStock, setSelectedStock] = useState<string | null>(null);
    const [stockEvents, setStockEvents] = useState<any[]>([]);
    const [eventsLoading, setEventsLoading] = useState(false);

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
            setGlobalGroups(res || []);
            return;
        }
        try {
            console.log('[StockDashboard] Loading topics...', { groupId, page: mentionPage });
            // Use getStockTopics instead of getStockMentions
            const res = await apiClient.getStockTopics(groupId!, mentionPage, 20);

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

    const loadWinRate = useCallback(async () => {
        try {
            console.log('[StockDashboard] Loading win rate...');
            const res = isGlobal
                ? await apiClient.getGlobalWinRate(2, returnPeriod, 50)
                : await apiClient.getStockWinRate(groupId!, {
                    min_mentions: 2,
                    return_period: returnPeriod,
                    limit: 50,
                });
            console.log('[StockDashboard] Win rate loaded:', res?.length);
            setWinRate(res || []);
        } catch (err) {
            console.error('[StockDashboard] Failed to load win rate:', err);
        }
    }, [groupId, returnPeriod, isGlobal]);

    const loadSectors = useCallback(async () => {
        try {
            console.log('[StockDashboard] Loading sectors...');
            const res = isGlobal
                ? await apiClient.getGlobalSectorHeat()
                : await apiClient.getSectorHeat(groupId!);
            console.log('[StockDashboard] Sectors loaded:', res?.length);
            setSectors(res || []);
        } catch (err) {
            console.error('[StockDashboard] Failed to load sectors:', err);
        }
    }, [groupId, isGlobal]);

    const loadSignals = useCallback(async () => {
        try {
            console.log('[StockDashboard] Loading signals...');
            const res = isGlobal
                ? await apiClient.getGlobalSignals(7, 2)
                : await apiClient.getStockSignals(groupId!, 7, 2);
            console.log('[StockDashboard] Signals loaded:', res?.length);
            setSignals(res || []);
        } catch (err) {
            console.error('[StockDashboard] Failed to load signals:', err);
        }
    }, [groupId, isGlobal]);

    const loadAll = useCallback(async () => {
        setLoading(true);
        await Promise.all([loadStats(), loadMentions(), loadSectors()]);
        setLoading(false);
    }, [loadStats, loadMentions, loadSectors]);

    useEffect(() => { loadAll(); }, [loadAll]);

    // For local mentions pagination
    useEffect(() => {
        if (!isGlobal) loadMentions();
    }, [mentionPage, searchStock, loadMentions, isGlobal]);

    useEffect(() => {
        if (activeView === 'winrate') loadWinRate();
        else if (activeView === 'signals') loadSignals();
    }, [activeView, loadWinRate, loadSignals]);

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

    const handleTaskComplete = () => {
        setScanning(false);
        loadAll();
        toast.success("数据分析完成");
        // Don't close log automatically, let user see result
    };

    /* ── Stock Detail Logic (mostly specific to Group mode or if Global supports drill down) ── */
    // Note: Global mode might not support detailed stock events easily without group context.
    // However, if we click a stock code, we might want to show some details.
    // For now, only group mode supports full event drill down nicely. 
    // BUT we can use the first available group or fail gracefully.
    // The existing 'getStockEvents' requires groupId.
    // Let's assume Global Detail View is a future enhancement or disable it for global currently.
    // Adjusted: We will allow clicking but wrap in try/catch or disable if global.
    const openStockDetail = async (stockCode: string) => {
        if (isGlobal) {
            toast.info("全局模式下暂不支持查看个股详细事件");
            return;
        }

        setSelectedStock(stockCode);
        setEventsLoading(true);
        try {
            const res = await apiClient.getStockEvents(groupId!, stockCode);
            setStockEvents(res?.events || []);
        } catch {
            setStockEvents([]);
        } finally {
            setEventsLoading(false);
        }
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
        <div className="flex flex-col gap-4 h-full relative">
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
                            onTaskStop={() => { }}
                        />
                    </div>
                </div>
            )}

            {/* ─── Header cards ─── */}
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

            {/* ─── Top mentioned badges (Group Mode Only) ─── */}
            {!isGlobal && stats?.top_mentioned && stats.top_mentioned.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                    <span className="text-xs text-muted-foreground self-center mr-1">🔥 高频:</span>
                    {stats.top_mentioned.slice(0, 12).map((s: any) => (
                        <Badge
                            key={s.code}
                            variant="secondary"
                            className="cursor-pointer hover:bg-primary/20 transition-colors text-xs"
                            onClick={() => openStockDetail(s.code)}
                        >
                            {s.name} <span className="ml-1 opacity-60">{s.count}</span>
                        </Badge>
                    ))}
                </div>
            )}

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
                <div className="grid grid-cols-5 gap-2">
                    <Button
                        size="sm"
                        variant={activeView === 'overview' ? 'default' : 'ghost'}
                        onClick={() => setActiveView('overview')}
                        className="gap-1 w-full"
                    >
                        <Activity className="h-3.5 w-3.5" /> 概览
                    </Button>
                    <Button
                        size="sm"
                        variant={activeView === 'winrate' ? 'default' : 'ghost'}
                        onClick={() => setActiveView('winrate')}
                        className="gap-1 w-full"
                    >
                        <TrendingUp className="h-3.5 w-3.5" /> 胜率
                    </Button>
                    <Button
                        size="sm"
                        variant={activeView === 'sector' ? 'default' : 'ghost'}
                        onClick={() => setActiveView('sector')}
                        className="gap-1 w-full"
                    >
                        <Flame className="h-3.5 w-3.5" /> 板块
                    </Button>
                    <Button
                        size="sm"
                        variant={activeView === 'signals' ? 'default' : 'ghost'}
                        onClick={() => setActiveView('signals')}
                        className="gap-1 w-full"
                    >
                        <Zap className="h-3.5 w-3.5" /> 信号
                    </Button>
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
                </div>
            </div>

            {/* ─── Search Bar (Fixed, below nav, only for Overview) ─── */}
            {activeView === 'overview' && !isGlobal && (
                <div className="flex items-center gap-2 bg-background z-10">
                    <div className="relative flex-1">
                        <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                        <Input
                            placeholder="搜索股票代码或名称 (支持模糊搜索)..."
                            className="pl-9 h-9"
                            value={searchStock}
                            onChange={e => { setSearchStock(e.target.value); setMentionPage(1); }}
                        />
                    </div>
                    <span className="text-xs text-muted-foreground whitespace-nowrap">共 {mentionTotal} 条{searchStock ? '记录' : '话题'}</span>
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
                                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                                    {globalGroups.map((group: any) => (
                                        <Card key={group.group_id} className="hover:border-primary/30 transition-colors">
                                            <CardContent className="p-3">
                                                <div className="flex justify-between items-start">
                                                    <div>
                                                        <div className="font-medium text-sm">{group.group_name || `Group ${group.group_id}`}</div>
                                                        <div className="text-xs text-muted-foreground mt-1">ID: {group.group_id}</div>
                                                    </div>
                                                    <Badge variant="outline" className="text-xs">
                                                        {group.mentions_count || 0} 提及
                                                    </Badge>
                                                </div>
                                                <div className="mt-2 text-xs text-muted-foreground flex gap-3">
                                                    <span>股票: {group.unique_stocks || 0}</span>
                                                    <span>话题: {group.topics_count || 0}</span>
                                                    <span>最后更新: {group.last_updated || '—'}</span>
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
                                                        <div className="text-sm line-clamp-3 text-foreground/90 whitespace-pre-wrap">
                                                            {topic.text?.length > 200 ? topic.text.slice(0, 200) + '...' : topic.text}
                                                        </div>
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
                        <div className="flex items-center gap-2">
                            <span className="text-xs text-muted-foreground">收益周期:</span>
                            <Select value={returnPeriod} onValueChange={v => setReturnPeriod(v)}>
                                <SelectTrigger className="w-28 h-8 text-xs">
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
                            <Button size="sm" variant="ghost" onClick={loadWinRate}>
                                <RefreshCw className="h-3.5 w-3.5" />
                            </Button>
                        </div>

                        <div className="rounded-md border overflow-hidden">
                            <table className="w-full text-xs">
                                <thead>
                                    <tr className="bg-muted/40 text-muted-foreground">
                                        <th className="text-left p-2 font-medium">#</th>
                                        <th className="text-left p-2 font-medium">股票</th>
                                        <th className="text-right p-2 font-medium">提及次数</th>
                                        <th className="text-right p-2 font-medium">胜率</th>
                                        <th className="text-right p-2 font-medium">平均收益</th>
                                        {/* Global API v.s. Stock API diff: avg_excess might be missing in global if simply not aggregated, but let's assume it is there or check */}
                                        <th className="text-right p-2 font-medium">平均超额</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {winRate.length === 0 ? (
                                        <tr><td colSpan={6} className="text-center py-8 text-muted-foreground">暂无胜率数据</td></tr>
                                    ) : winRate.map((w: any, i: number) => (
                                        <tr
                                            key={w.stock_code}
                                            className="border-t border-border/50 hover:bg-muted/20 transition-colors cursor-pointer"
                                            onClick={() => openStockDetail(w.stock_code)}
                                        >
                                            <td className="p-2 text-muted-foreground">{i + 1}</td>
                                            <td className="p-2">
                                                <span className="font-medium">{w.stock_name}</span>
                                                <span className="ml-1 text-muted-foreground">{w.stock_code}</span>
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
                                            <td className={`p-2 text-right font-mono ${pctColor(w.avg_excess)}`}>
                                                {fmtPct(w.avg_excess)}
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </div>
                )}

                {/* ── SECTOR HEAT ── */}
                {activeView === 'sector' && (
                    <div className="space-y-3">
                        <div className="flex items-center justify-between">
                            <span className="text-xs text-muted-foreground">板块提及热度排行</span>
                            <Button size="sm" variant="ghost" onClick={loadSectors}>
                                <RefreshCw className="h-3.5 w-3.5" />
                            </Button>
                        </div>
                        {sectors.length === 0 ? (
                            <div className="text-center py-8 text-muted-foreground text-xs">暂无板块数据</div>
                        ) : (
                            <div className="space-y-2">
                                {sectors.map((s: any) => (
                                    <HeatBar
                                        key={s.sector}
                                        label={s.sector}
                                        value={s.total_mentions}
                                        max={sectors[0]?.total_mentions || 1}
                                    />
                                ))}
                            </div>
                        )}
                    </div>
                )}

                {/* ── SIGNALS ── */}
                {activeView === 'signals' && (
                    <div className="space-y-3">
                        <div className="flex items-center justify-between">
                            <span className="text-xs text-muted-foreground">近7天信号雷达（≥2次提及 + 历史正收益）</span>
                            <Button size="sm" variant="ghost" onClick={loadSignals}>
                                <RefreshCw className="h-3.5 w-3.5" />
                            </Button>
                        </div>
                        {signals.length === 0 ? (
                            <div className="text-center py-12 text-muted-foreground text-sm">
                                <Zap className="h-8 w-8 mx-auto mb-2 opacity-30" />
                                🤷 近期无符合条件的信号
                            </div>
                        ) : (
                            <div className="grid gap-3 sm:grid-cols-2">
                                {signals.map((sig: any) => (
                                    <Card
                                        key={sig.stock_code}
                                        className="cursor-pointer hover:border-primary/40 transition-all"
                                        onClick={() => openStockDetail(sig.stock_code)}
                                    >
                                        <CardContent className="p-3">
                                            <div className="flex items-center gap-2 mb-1">
                                                <Zap className="h-4 w-4 text-amber-400" />
                                                <span className="font-medium text-sm">{sig.stock_name}</span>
                                                <span className="text-xs text-muted-foreground">{sig.stock_code}</span>
                                            </div>
                                            <div className="flex items-center gap-4 text-xs text-muted-foreground">
                                                <span>近期提及 <b className="text-foreground">{isGlobal ? sig.mention_count : sig.recent_mentions}</b> 次</span>
                                                <span>历史胜率 <b className={pctColor((isGlobal ? sig.win_rate : sig.historical_win_rate) > 50 ? 1 : -1)}>
                                                    {(isGlobal ? sig.win_rate : sig.historical_win_rate) != null ? `${(isGlobal ? sig.win_rate : sig.historical_win_rate).toFixed(0)}%` : '—'}
                                                </b></span>
                                                <span>均收益 <b className={pctColor(isGlobal ? sig.avg_return : sig.avg_return_5d)}>
                                                    {fmtPct(isGlobal ? sig.avg_return : sig.avg_return_5d)}
                                                </b></span>
                                            </div>
                                            {(isGlobal ? sig.reason : sig.reason) && (
                                                <p className="text-xs text-muted-foreground mt-1 line-clamp-1">{isGlobal ? sig.reason : sig.reason}</p>
                                            )}
                                        </CardContent>
                                    </Card>
                                ))}
                            </div>
                        )}
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
        </div>
    );
}

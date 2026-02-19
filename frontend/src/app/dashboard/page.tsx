'use client';


import { useEffect, useState, useCallback } from 'react';
import { apiClient } from '@/lib/api';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Progress } from '@/components/ui/progress';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import Link from 'next/link';
import { BarChart3, Clock, Loader2, Zap, Target, Flame, Activity, AlertTriangle } from 'lucide-react';
import TaskLogViewer from '@/components/TaskLogViewer';

/* =========================================================
   Types
   ========================================================= */

interface GlobalStats {
    total_topics: number;
    total_mentions: number;
    unique_stocks: number;
    performance_records: number;
    group_count: number;
}

interface WinRateItem {
    stock_code: string;
    stock_name: string;
    mention_count: number;
    win_count: number;
    win_rate: number;
    avg_return: number;
    group_count?: number;
}

interface SectorItem {
    sector: string;
    count: number;
    stocks: string[];
}

interface SignalItem {
    stock_code: string;
    stock_name: string;
    mention_count: number;
    group_count: number;
    groups: string[];
    latest_mention: string;
}

interface GroupOverview {
    group_id: string;
    group_name: string;
    total_topics: number;
    total_mentions: number;
    unique_stocks: number;
    latest_topic: string;
    win_rate: number;
}

interface SchedulerStatus {
    state: string;
    is_crawling: boolean;
    is_calculating: boolean;
    crawl_rounds: number;
    calc_rounds: number;
    last_crawl: string | null;
    last_calc: string | null;
    current_group: string | null;
    errors_total: number;
    config: Record<string, unknown>;
    groups: Record<string, unknown>;
}

/* =========================================================
   Mini Components
   ========================================================= */

function StatCard({
    label,
    value,
    icon,
    color = 'blue',
}: {
    label: string;
    value: string | number;
    icon: string;
    color?: string;
}) {
    const gradients: Record<string, string> = {
        blue: 'from-blue-500/10 to-blue-600/5 border-blue-500/20',
        green: 'from-emerald-500/10 to-emerald-600/5 border-emerald-500/20',
        purple: 'from-violet-500/10 to-violet-600/5 border-violet-500/20',
        amber: 'from-amber-500/10 to-amber-600/5 border-amber-500/20',
        rose: 'from-rose-500/10 to-rose-600/5 border-rose-500/20',
    };

    return (
        <Card className={`bg-gradient-to-br ${gradients[color] || gradients.blue} border backdrop-blur-sm`}>
            <CardContent className="p-5">
                <div className="flex items-center justify-between">
                    <div>
                        <p className="text-sm text-muted-foreground mb-1">{label}</p>
                        <p className="text-2xl font-bold tracking-tight">{value}</p>
                    </div>
                    <span className="text-3xl opacity-60">{icon}</span>
                </div>
            </CardContent>
        </Card>
    );
}

function ReturnBadge({ value }: { value: number | null | undefined }) {
    if (value === null || value === undefined) return <span className="text-muted-foreground text-xs">—</span>;
    const v = Number(value);
    return (
        <span
            className={`text-xs font-mono font-semibold ${v > 0 ? 'text-emerald-500' : v < 0 ? 'text-red-500' : 'text-muted-foreground'
                }`}
        >
            {v > 0 ? '+' : ''}
            {v.toFixed(2)}%
        </span>
    );
}

/* =========================================================
   Main Dashboard Page
   ========================================================= */

export default function DashboardPage() {
    // --- state ---
    const [stats, setStats] = useState<GlobalStats | null>(null);
    const [winRate, setWinRate] = useState<WinRateItem[]>([]);
    const [sectors, setSectors] = useState<SectorItem[]>([]);
    const [signals, setSignals] = useState<SignalItem[]>([]);
    const [groups, setGroups] = useState<GroupOverview[]>([]);
    const [scheduler, setScheduler] = useState<SchedulerStatus | null>(null);
    const [loading, setLoading] = useState(true);
    const [returnPeriod, setReturnPeriod] = useState('return_5d');
    const [startDate, setStartDate] = useState('30'); // Default 30 days
    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(20);
    const [total, setTotal] = useState(0);
    const [sortBy, setSortBy] = useState('win_rate');
    const [sortOrder, setSortOrder] = useState('desc');
    const [winRateLoading, setWinRateLoading] = useState(false);

    const [schedulerLoading, setSchedulerLoading] = useState(false);
    const [analyzing, setAnalyzing] = useState(false);
    const [selectedStock, setSelectedStock] = useState<string | null>(null);
    const [stockEvents, setStockEvents] = useState<any[]>([]);
    const [eventsLoading, setEventsLoading] = useState(false);
    const [showLogs, setShowLogs] = useState(false);

    // --- fetch helpers ---

    const getDateFilter = useCallback(() => {
        if (startDate === 'all') return undefined;
        const d = new Date();
        d.setDate(d.getDate() - parseInt(startDate));
        return d.toISOString().split('T')[0];
    }, [startDate]);

    const fetchWinRate = useCallback(async () => {
        setWinRateLoading(true);
        try {
            const dateStr = getDateFilter();
            const res = await apiClient.getGlobalWinRate(
                2, returnPeriod, 1000, dateStr, sortBy, sortOrder, page, pageSize
            );
            if (res) {
                setWinRate(res.data || []);
                setTotal(res.total || 0);
            }
        } catch (e) {
            console.error(e);
        } finally {
            setWinRateLoading(false);
        }
    }, [returnPeriod, startDate, sortBy, sortOrder, page, pageSize, getDateFilter]);

    const fetchAll = useCallback(async () => {
        setLoading(true);
        try {
            const dateStr = getDateFilter();
            const [s, sc, sg, g, sch] = await Promise.allSettled([
                apiClient.getGlobalStats(),
                apiClient.getGlobalSectorHeat(dateStr),
                apiClient.getGlobalSignals(7, 2),
                apiClient.getGlobalGroups(),
                apiClient.getSchedulerStatus(),
            ]);

            // Win rate is fetched separately to support pagination/sorting/filtering without reloading everything
            await fetchWinRate();

            if (s.status === 'fulfilled') setStats(s.value);
            if (sc.status === 'fulfilled') setSectors(Array.isArray(sc.value) ? sc.value : sc.value?.data ?? []);
            if (sg.status === 'fulfilled') setSignals(Array.isArray(sg.value) ? sg.value : sg.value?.data ?? []);
            if (g.status === 'fulfilled') setGroups(Array.isArray(g.value) ? g.value : g.value?.data ?? []);
            if (sch.status === 'fulfilled') setScheduler(sch.value);
        } catch {
            /* swallow – individual calls handled above */
        } finally {
            setLoading(false);
        }
    }, [fetchWinRate, getDateFilter]);

    useEffect(() => {
        fetchAll();
    }, [fetchAll]);

    // Refresh win-rate when params change (outside initial load)
    useEffect(() => {
        fetchWinRate();
    }, [fetchWinRate]);

    const toggleScheduler = async () => {
        if (!scheduler) return;
        setSchedulerLoading(true);
        try {
            if (scheduler.state === 'running') {
                await apiClient.stopScheduler();
            } else {
                await apiClient.startScheduler();
                setShowLogs(true); // 启动时自动显示日志
            }
            const s = await apiClient.getSchedulerStatus();
            setScheduler(s);
        } catch {
            /* ignore */
        } finally {
            setSchedulerLoading(false);
        }
    };

    const handleDataAnalysis = async () => {
        if (scheduler?.is_calculating) {
            setSchedulerLoading(true);
            try {
                await apiClient.stopManualAnalysis();
                // Wait briefly for state change
                setTimeout(async () => {
                    const s = await apiClient.getSchedulerStatus();
                    setScheduler(s);
                    setSchedulerLoading(false);
                }, 1000);
            } catch (e) {
                console.error(e);
                setSchedulerLoading(false);
            }
            return;
        }

        setAnalyzing(true);
        setShowLogs(true);
        try {
            await apiClient.triggerManualAnalysis();
            // Wait and refresh
            setTimeout(async () => {
                const s = await apiClient.getSchedulerStatus();
                setScheduler(s);
                setAnalyzing(false);
            }, 1500);
        } catch {
            setAnalyzing(false);
        }
    };

    const openStockDetail = async (code: string) => {
        setSelectedStock(code);
        setEventsLoading(true);
        try {
            // 使用全局事件接口
            const data = await apiClient.getGlobalStockEvents(code);
            setStockEvents(data.events || []);
        } catch (error) {
            console.error('获取股票事件失败:', error);
            setStockEvents([]);
        } finally {
            setEventsLoading(false);
        }
    };

    const handleSort = (column: string) => {
        if (sortBy === column) {
            setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
        } else {
            setSortBy(column);
            setSortOrder('desc');
        }
    };

    // --- render ---

    if (loading) {
        return (
            <div className="min-h-screen flex items-center justify-center bg-background">
                <div className="flex flex-col items-center gap-4">
                    <div className="h-10 w-10 animate-spin rounded-full border-4 border-primary border-t-transparent" />
                    <p className="text-muted-foreground animate-pulse">加载全局看板中…</p>
                </div>
            </div>
        );
    }

    return (
        <div className="min-h-screen bg-background">
            {/* Header */}
            <header className="sticky top-0 z-30 border-b bg-background/80 backdrop-blur-lg">
                <div className="mx-auto flex max-w-7xl items-center justify-between px-6 py-3">
                    <div className="flex items-center gap-3">
                        <Link href="/" className="text-muted-foreground hover:text-foreground transition-colors">
                            ← 返回首页
                        </Link>
                        <span className="text-muted-foreground/40">|</span>
                        <h1 className="text-lg font-bold tracking-tight">📊 全局股票看板</h1>
                    </div>
                    <div className="flex items-center gap-3">
                        <Button variant="outline" size="sm" onClick={fetchAll} className="gap-1.5">
                            🔄 刷新
                        </Button>
                        {scheduler && (
                            <div className="flex items-center gap-2">
                                <Button
                                    variant="outline"
                                    size="sm"
                                    onClick={() => setShowLogs(!showLogs)}
                                    className={`gap-1.5 ${showLogs ? 'bg-primary/10 text-primary border-primary/30' : ''}`}
                                >
                                    📜 日志
                                </Button>

                                <Button
                                    variant={scheduler.is_calculating ? 'destructive' : 'outline'}
                                    size="sm"
                                    onClick={handleDataAnalysis}
                                    disabled={analyzing || schedulerLoading}
                                    className="gap-1.5 min-w-[100px]"
                                >
                                    {analyzing ? (
                                        <Loader2 className="h-3 w-3 animate-spin" />
                                    ) : scheduler.is_calculating ? (
                                        '⏹ 停止分析'
                                    ) : (
                                        '📊 数据分析'
                                    )}
                                </Button>
                                <Button
                                    variant={scheduler.state === 'running' ? 'destructive' : 'default'}
                                    size="sm"
                                    onClick={toggleScheduler}
                                    disabled={schedulerLoading}
                                    className="gap-1.5 min-w-[120px]"
                                >
                                    {schedulerLoading ? (
                                        <span className="animate-spin">⏳</span>
                                    ) : scheduler.state === 'running' ? (
                                        '⏹ 停止调度'
                                    ) : (
                                        '▶ 启动调度'
                                    )}
                                </Button>
                            </div>
                        )}
                    </div>
                </div>
            </header >

            <main className="mx-auto max-w-7xl px-6 py-6 space-y-6">
                {/* ---- Stats Row ---- */}
                <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-5">
                    <StatCard label="群组数" value={stats?.group_count ?? 0} icon="👥" color="blue" />
                    <StatCard label="话题总数" value={stats?.total_topics?.toLocaleString() ?? '0'} icon="📝" color="green" />
                    <StatCard label="股票提及" value={stats?.total_mentions?.toLocaleString() ?? '0'} icon="📈" color="purple" />
                    <StatCard label="不同股票" value={stats?.unique_stocks ?? 0} icon="🎯" color="amber" />
                    <StatCard label="收益记录" value={stats?.performance_records?.toLocaleString() ?? '0'} icon="📊" color="rose" />
                </div>

                {/* ---- Scheduler Logs ---- */}
                {showLogs && (
                    <div className="space-y-4 animate-in fade-in slide-in-from-top-4 duration-500">
                        {/* Compact Status Header above logs */}
                        <div className="flex items-center justify-between px-2">
                            <div className="flex items-center gap-3">
                                <Badge
                                    variant={scheduler?.state === 'running' ? 'default' : 'secondary'}
                                    className={`text-xs px-2.5 py-1 ${scheduler?.state === 'running' ? 'bg-blue-500 animate-pulse' : ''}`}
                                >
                                    调度循环: {scheduler?.state === 'running' ? '运行中' : '已停止'}
                                </Badge>
                                <Badge
                                    variant={scheduler?.is_calculating || analyzing ? 'default' : 'secondary'}
                                    className={`text-xs px-2.5 py-1 ${(scheduler?.is_calculating || analyzing) ? 'bg-indigo-500 animate-pulse' : ''}`}
                                >
                                    数据分析: {(scheduler?.is_calculating || analyzing) ? '运行中' : '空闲'}
                                </Badge>
                                {scheduler?.current_group && (
                                    <div className="flex items-center gap-2 text-xs text-blue-600 bg-blue-50 px-3 py-1 rounded-full border border-blue-100 shadow-sm animate-in zoom-in-95 duration-300">
                                        <Activity className="w-3.5 h-3.5" />
                                        <span className="opacity-70 font-medium">当前处理中</span>
                                        <span className="font-bold">{scheduler.current_group}</span>
                                    </div>
                                )}
                            </div>
                            <Button variant="ghost" size="sm" onClick={() => setShowLogs(false)} className="h-8 text-xs text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-all">
                                收起日志
                            </Button>
                        </div>
                        <div className="h-[480px]">
                            <TaskLogViewer
                                taskId="scheduler"
                                onClose={() => setShowLogs(false)}
                                inline={true}
                            />
                        </div>
                    </div>
                )}

                {/* ---- Scheduler Status Bar (mini) ---- */}
                {scheduler && !showLogs && (
                    <Card className="border border-border/60 bg-card/50 backdrop-blur-sm">
                        <CardContent className="p-4">
                            <div className="flex flex-wrap items-center gap-x-6 gap-y-2 text-sm">
                                <div className="flex items-center gap-2">
                                    <span className={`h-2 w-2 rounded-full ${scheduler.state === 'running' ? 'bg-emerald-500 animate-pulse' : 'bg-muted-foreground'}`} />
                                    <span className="font-medium text-xs">自动采集调度器</span>
                                    <Badge variant={scheduler.state === 'running' ? 'default' : 'secondary'} className="text-[10px] px-1.5 py-0">
                                        {scheduler.state === 'running' ? '运行中' : '已停止'}
                                    </Badge>
                                </div>
                                {scheduler.current_group && (
                                    <span className="text-muted-foreground text-xs flex items-center gap-1.5">
                                        <Activity className="w-3 h-3 opacity-50" />
                                        当前处理 <span className="text-foreground font-medium">{scheduler.current_group}</span>
                                    </span>
                                )}
                                {scheduler.errors_total > 0 && (
                                    <span className="text-red-500 text-xs flex items-center gap-1">
                                        <AlertTriangle className="w-3 h-3" />
                                        错误 <strong>{scheduler.errors_total}</strong>
                                    </span>
                                )}
                                <Button variant="ghost" size="sm" onClick={() => setShowLogs(true)} className="ml-auto text-xs h-7 px-3 bg-muted/30 hover:bg-muted/60 border border-border/20">
                                    查看运行日志
                                </Button>
                            </div>
                        </CardContent>
                    </Card>
                )}

                {/* ---- Tabs ---- */}
                <Tabs defaultValue="winrate" className="space-y-4">
                    <TabsList className="grid w-full grid-cols-4 lg:w-[480px]">
                        <TabsTrigger value="winrate">🏆 胜率排行</TabsTrigger>
                        <TabsTrigger value="signals">🚀 信号雷达</TabsTrigger>
                        <TabsTrigger value="sectors">🔥 板块热度</TabsTrigger>
                        <TabsTrigger value="groups">👥 群组概览</TabsTrigger>
                    </TabsList>

                    {/* --- Win Rate Tab --- */}
                    <TabsContent value="winrate" className="space-y-4">
                        <div className="flex flex-wrap items-center justify-between gap-4">
                            <div className="flex items-center gap-3">
                                <Select value={returnPeriod} onValueChange={setReturnPeriod}>
                                    <SelectTrigger className="w-[140px]">
                                        <SelectValue placeholder="收益周期" />
                                    </SelectTrigger>
                                    <SelectContent>
                                        <SelectItem value="return_1d">T+1 日</SelectItem>
                                        <SelectItem value="return_3d">T+3 日</SelectItem>
                                        <SelectItem value="return_5d">T+5 日</SelectItem>
                                        <SelectItem value="return_10d">T+10 日</SelectItem>
                                        <SelectItem value="return_20d">T+20 日</SelectItem>
                                    </SelectContent>
                                </Select>

                                <Select value={startDate} onValueChange={setStartDate}>
                                    <SelectTrigger className="w-[140px]">
                                        <SelectValue placeholder="时间范围" />
                                    </SelectTrigger>
                                    <SelectContent>
                                        <SelectItem value="30">近 30 天</SelectItem>
                                        <SelectItem value="60">近 60 天</SelectItem>
                                        <SelectItem value="90">近 90 天</SelectItem>
                                        <SelectItem value="180">近 180 天</SelectItem>
                                        <SelectItem value="365">近 1 年</SelectItem>
                                        <SelectItem value="all">全部时间</SelectItem>
                                    </SelectContent>
                                </Select>
                            </div>

                            <div className="flex items-center gap-2">
                                <Button
                                    variant="outline"
                                    size="sm"
                                    onClick={() => setPage(Math.max(1, page - 1))}
                                    disabled={page === 1}
                                >
                                    上一页
                                </Button>
                                <span className="text-sm text-muted-foreground min-w-[60px] text-center">
                                    {page} / {Math.ceil(total / pageSize) || 1}
                                </span>
                                <Button
                                    variant="outline"
                                    size="sm"
                                    onClick={() => setPage(page + 1)}
                                    disabled={page * pageSize >= total}
                                >
                                    下一页
                                </Button>
                            </div>
                        </div>

                        <Card className="relative">
                            {winRateLoading && (
                                <div className="absolute inset-0 bg-background/50 flex items-center justify-center z-10 backdrop-blur-[1px]">
                                    <Loader2 className="h-8 w-8 animate-spin text-primary" />
                                </div>
                            )}
                            <CardContent className="p-0">
                                <div className="overflow-x-auto">
                                    <table className="w-full text-sm">
                                        <thead>
                                            <tr className="border-b bg-muted/40 transition-colors">
                                                <th className="px-4 py-3 text-left font-medium text-muted-foreground w-[60px]">#</th>
                                                <th className="px-4 py-3 text-left font-medium text-muted-foreground cursor-pointer hover:text-foreground" onClick={() => handleSort('stock_code')}>
                                                    股票 {sortBy === 'stock_code' && (sortOrder === 'asc' ? '↑' : '↓')}
                                                </th>
                                                <th className="px-4 py-3 text-right font-medium text-muted-foreground cursor-pointer hover:text-foreground" onClick={() => handleSort('mention_count')}>
                                                    提及数 {sortBy === 'mention_count' && (sortOrder === 'asc' ? '↑' : '↓')}
                                                </th>
                                                <th className="px-4 py-3 text-right font-medium text-muted-foreground cursor-pointer hover:text-foreground" onClick={() => handleSort('win_rate')}>
                                                    胜率 {sortBy === 'win_rate' && (sortOrder === 'asc' ? '↑' : '↓')}
                                                </th>
                                                <th className="px-4 py-3 text-right font-medium text-muted-foreground cursor-pointer hover:text-foreground" onClick={() => handleSort('avg_return')}>
                                                    平均收益 {sortBy === 'avg_return' && (sortOrder === 'asc' ? '↑' : '↓')}
                                                </th>
                                                <th className="px-4 py-3 text-right font-medium text-muted-foreground">群组数</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {winRate.length === 0 ? (
                                                <tr>
                                                    <td colSpan={6} className="py-12 text-center text-muted-foreground">
                                                        暂无数据
                                                    </td>
                                                </tr>
                                            ) : (
                                                winRate.map((item, i) => (
                                                    <tr
                                                        key={item.stock_code}
                                                        className="border-b last:border-0 hover:bg-muted/30 transition-colors cursor-pointer"
                                                        onClick={() => openStockDetail(item.stock_code)}
                                                    >
                                                        <td className="px-4 py-3 font-mono text-muted-foreground">{(page - 1) * pageSize + i + 1}</td>
                                                        <td className="px-4 py-3">
                                                            <div className="flex items-center gap-2">
                                                                <span className="font-medium">{item.stock_name || item.stock_code}</span>
                                                                <span className="text-xs text-muted-foreground font-mono">{item.stock_code}</span>
                                                            </div>
                                                        </td>
                                                        <td className="px-4 py-3 text-right font-mono">{item.mention_count}</td>
                                                        <td className="px-4 py-3 text-right">
                                                            <div className="flex items-center justify-end gap-2">
                                                                <Progress value={item.win_rate} className="h-2 w-16" />
                                                                <span className="font-mono text-xs w-12 text-right">
                                                                    {item.win_rate.toFixed(0)}%
                                                                </span>
                                                            </div>
                                                        </td>
                                                        <td className="px-4 py-3 text-right">
                                                            <ReturnBadge value={item.avg_return} />
                                                        </td>
                                                        <td className="px-4 py-3 text-right font-mono text-muted-foreground">
                                                            {item.group_count ?? '—'}
                                                        </td>
                                                    </tr>
                                                ))
                                            )}
                                        </tbody>
                                    </table>
                                </div>
                            </CardContent>
                        </Card>
                    </TabsContent>

                    {/* --- Signals Tab --- */}
                    <TabsContent value="signals" className="space-y-4">
                        <p className="text-sm text-muted-foreground">近7天内被多个群组同时提及的高共识股票</p>
                        {signals.length === 0 ? (
                            <Card>
                                <CardContent className="py-12 text-center text-muted-foreground">暂无共识信号</CardContent>
                            </Card>
                        ) : (
                            <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
                                {signals.map((s) => (
                                    <Card key={s.stock_code} className="border border-border/60 hover:shadow-md transition-shadow">
                                        <CardHeader className="pb-2">
                                            <CardTitle className="text-base flex items-center justify-between">
                                                <span>
                                                    {s.stock_name}{' '}
                                                    <span className="text-xs text-muted-foreground font-mono ml-1">{s.stock_code}</span>
                                                </span>
                                                <Badge variant="secondary" className="text-xs">
                                                    {s.group_count} 个群组提及
                                                </Badge>
                                            </CardTitle>
                                        </CardHeader>
                                        <CardContent className="text-sm space-y-2">
                                            <div className="flex items-center justify-between text-muted-foreground">
                                                <span>提及 {s.mention_count} 次</span>
                                                <span className="text-xs">最新 {s.latest_mention?.slice(0, 10) ?? '—'}</span>
                                            </div>
                                            <div className="flex flex-wrap gap-1">
                                                {s.groups?.map((g) => (
                                                    <Badge key={g} variant="outline" className="text-[10px]">
                                                        {g}
                                                    </Badge>
                                                ))}
                                            </div>
                                        </CardContent>
                                    </Card>
                                ))}
                            </div>
                        )}
                    </TabsContent>

                    {/* --- Sectors Tab --- */}
                    <TabsContent value="sectors" className="space-y-4">
                        <p className="text-sm text-muted-foreground">按板块聚合的股票提及热度</p>
                        {sectors.length === 0 ? (
                            <Card>
                                <CardContent className="py-12 text-center text-muted-foreground">暂无板块数据</CardContent>
                            </Card>
                        ) : (
                            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                                {sectors.slice(0, 18).map((s) => {
                                    const maxCount = sectors[0]?.count || 1;
                                    return (
                                        <Card key={s.sector} className="border border-border/40">
                                            <CardContent className="p-4 space-y-2">
                                                <div className="flex items-center justify-between">
                                                    <span className="font-medium text-sm">{s.sector}</span>
                                                    <Badge variant="outline" className="text-xs font-mono">{s.count}</Badge>
                                                </div>
                                                <Progress value={(s.count / maxCount) * 100} className="h-1.5" />
                                                <div className="flex flex-wrap gap-1">
                                                    {s.stocks?.slice(0, 5).map((st) => (
                                                        <span key={st} className="text-[10px] text-muted-foreground bg-muted rounded px-1.5 py-0.5">
                                                            {st}
                                                        </span>
                                                    ))}
                                                    {(s.stocks?.length ?? 0) > 5 && (
                                                        <span className="text-[10px] text-muted-foreground">+{(s.stocks?.length ?? 0) - 5}</span>
                                                    )}
                                                </div>
                                            </CardContent>
                                        </Card>
                                    );
                                })}
                            </div>
                        )}
                    </TabsContent>

                    {/* --- Groups Tab --- */}
                    <TabsContent value="groups" className="space-y-4">
                        <p className="text-sm text-muted-foreground">各群组数据概览</p>
                        {groups.length === 0 ? (
                            <Card>
                                <CardContent className="py-12 text-center text-muted-foreground">暂无群组数据</CardContent>
                            </Card>
                        ) : (
                            <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
                                {groups.map((g) => (
                                    <Link
                                        key={g.group_id}
                                        href={`/groups/${g.group_id}`}
                                        className="block group"
                                    >
                                        <Card className="border border-border/60 group-hover:shadow-md group-hover:border-primary/30 transition-all cursor-pointer">
                                            <CardHeader className="pb-2">
                                                <CardTitle className="text-base flex items-center gap-2 justify-between">
                                                    <span className="truncate" title={g.group_name}>{g.group_name || `群组 ${g.group_id}`}</span>
                                                    <Badge variant="outline" className="font-mono text-xs shrink-0">{g.group_id}</Badge>
                                                </CardTitle>
                                            </CardHeader>
                                            <CardContent className="text-sm space-y-1.5">
                                                <div className="flex justify-between text-muted-foreground">
                                                    <span>话题</span>
                                                    <span className="font-mono">{g.total_topics?.toLocaleString() ?? 0}</span>
                                                </div>
                                                <div className="flex justify-between text-muted-foreground">
                                                    <span>股票提及</span>
                                                    <span className="font-mono">{g.total_mentions?.toLocaleString() ?? 0}</span>
                                                </div>
                                                <div className="flex justify-between text-muted-foreground">
                                                    <span>不同股票</span>
                                                    <span className="font-mono">{g.unique_stocks ?? 0}</span>
                                                </div>
                                                {g.latest_topic && (
                                                    <div className="text-xs text-muted-foreground pt-1 border-t border-border/40">
                                                        最新更新 {g.latest_topic.slice(0, 10)}
                                                    </div>
                                                )}
                                            </CardContent>
                                        </Card>
                                    </Link>
                                ))}
                            </div>
                        )}
                    </TabsContent>
                </Tabs>
            </main>

            {/* ─── Stock Detail Drawer ─── */}
            {
                selectedStock && (
                    <div className="fixed inset-0 bg-black/40 z-50 flex justify-end" onClick={() => setSelectedStock(null)}>
                        <div
                            className="w-full max-w-md bg-background border-l shadow-2xl overflow-auto animate-in slide-in-from-right"
                            onClick={e => e.stopPropagation()}
                        >
                            <div className="sticky top-0 bg-background/95 backdrop-blur p-4 border-b flex items-center justify-between">
                                <h3 className="font-semibold text-sm flex items-center gap-2">
                                    <BarChart3 className="h-4 w-4" /> 股票事件详情
                                </h3>
                                <Button size="sm" variant="ghost" onClick={() => setSelectedStock(null)}>✕</Button>
                            </div>

                            <div className="p-4 space-y-3">
                                {eventsLoading ? (
                                    <div className="flex items-center justify-center py-12">
                                        <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
                                    </div>
                                ) : stockEvents.length === 0 ? (
                                    <p className="text-center text-muted-foreground text-sm py-8">暂无事件数据</p>
                                ) : (
                                    <>
                                        <div className="flex items-center gap-2 mb-3">
                                            <span className="font-semibold">{stockEvents[0]?.stock_name}</span>
                                            <Badge variant="outline">{selectedStock}</Badge>
                                            <Badge variant="secondary">{stockEvents.length} 次提及</Badge>
                                        </div>
                                        {stockEvents.map((ev: any, idx: number) => (
                                            <Card key={idx} className="border-border/50">
                                                <CardContent className="p-3 space-y-2">
                                                    <div className="flex items-center justify-between text-xs">
                                                        <span className="text-muted-foreground flex items-center gap-1">
                                                            <Clock className="h-3 w-3" />
                                                            {ev.mention_date} {ev.mention_time}
                                                        </span>
                                                        {ev.group_name && (
                                                            <Badge variant="secondary" className="text-[10px] h-5 px-1.5">
                                                                {ev.group_name}
                                                            </Badge>
                                                        )}
                                                    </div>
                                                    {ev.context && (
                                                        <p className="text-xs text-muted-foreground line-clamp-3 bg-muted/30 rounded p-2">
                                                            {ev.context}
                                                        </p>
                                                    )}
                                                    <div className="grid grid-cols-5 gap-1 text-center text-[10px]">
                                                        {[
                                                            { label: 'T+1', val: ev.return_1d, icon: <Zap className="h-2 w-2" /> },
                                                            { label: 'T+3', val: ev.return_3d, icon: <Target className="h-2 w-2" /> },
                                                            { label: 'T+5', val: ev.return_5d, icon: <Flame className="h-2 w-2" /> },
                                                            { label: 'T+10', val: ev.return_10d, icon: <Activity className="h-2 w-2" /> },
                                                            { label: 'T+20', val: ev.return_20d, icon: <TrendingUp className="h-2 w-2" /> },
                                                        ].map((p, i) => (
                                                            <div key={i} className="flex flex-col items-center bg-muted/50 rounded py-1">
                                                                <span className="text-muted-foreground mb-1">{p.label}</span>
                                                                <ReturnBadge value={p.val} />
                                                            </div>
                                                        ))}
                                                    </div>
                                                </CardContent>
                                            </Card>
                                        ))}
                                    </>
                                )}
                            </div>
                        </div>
                    </div>
                )
            }
        </div >
    );
}

// 辅助组件：趋势图图标（用于 Drawer）
function TrendingUp({ className }: { className?: string }) {
    return (
        <svg
            xmlns="http://www.w3.org/2000/svg"
            width="24"
            height="24"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className={className}
        >
            <polyline points="23 6 13.5 15.5 8.5 10.5 1 18" />
            <polyline points="17 6 23 6 23 12" />
        </svg>

    );
}

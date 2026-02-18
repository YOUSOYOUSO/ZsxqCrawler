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
    total_topics: number;
    total_mentions: number;
    unique_stocks: number;
    latest_topic: string;
}

interface SchedulerStatus {
    state: string;
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
    const [schedulerLoading, setSchedulerLoading] = useState(false);

    // --- fetch helpers ---
    const fetchAll = useCallback(async () => {
        setLoading(true);
        try {
            const [s, w, sc, sg, g, sch] = await Promise.allSettled([
                apiClient.getGlobalStats(),
                apiClient.getGlobalWinRate(2, returnPeriod, 30),
                apiClient.getGlobalSectorHeat(),
                apiClient.getGlobalSignals(7, 2),
                apiClient.getGlobalGroups(),
                apiClient.getSchedulerStatus(),
            ]);
            if (s.status === 'fulfilled') setStats(s.value);
            if (w.status === 'fulfilled') setWinRate(Array.isArray(w.value) ? w.value : w.value?.data ?? []);
            if (sc.status === 'fulfilled') setSectors(Array.isArray(sc.value) ? sc.value : sc.value?.data ?? []);
            if (sg.status === 'fulfilled') setSignals(Array.isArray(sg.value) ? sg.value : sg.value?.data ?? []);
            if (g.status === 'fulfilled') setGroups(Array.isArray(g.value) ? g.value : g.value?.data ?? []);
            if (sch.status === 'fulfilled') setScheduler(sch.value);
        } catch {
            /* swallow – individual calls handled above */
        } finally {
            setLoading(false);
        }
    }, [returnPeriod]);

    useEffect(() => {
        fetchAll();
    }, [fetchAll]);

    // Refresh win-rate when period selector changes
    useEffect(() => {
        apiClient.getGlobalWinRate(2, returnPeriod, 30).then((d) => {
            setWinRate(Array.isArray(d) ? d : d?.data ?? []);
        }).catch(() => { });
    }, [returnPeriod]);

    const toggleScheduler = async () => {
        if (!scheduler) return;
        setSchedulerLoading(true);
        try {
            if (scheduler.state === 'running') {
                await apiClient.stopScheduler();
            } else {
                await apiClient.startScheduler();
            }
            const s = await apiClient.getSchedulerStatus();
            setScheduler(s);
        } catch {
            /* ignore */
        } finally {
            setSchedulerLoading(false);
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
                        )}
                    </div>
                </div>
            </header>

            <main className="mx-auto max-w-7xl px-6 py-6 space-y-6">
                {/* ---- Stats Row ---- */}
                <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-5">
                    <StatCard label="群组数" value={stats?.group_count ?? 0} icon="👥" color="blue" />
                    <StatCard label="话题总数" value={stats?.total_topics?.toLocaleString() ?? '0'} icon="📝" color="green" />
                    <StatCard label="股票提及" value={stats?.total_mentions?.toLocaleString() ?? '0'} icon="📈" color="purple" />
                    <StatCard label="不同股票" value={stats?.unique_stocks ?? 0} icon="🎯" color="amber" />
                    <StatCard label="收益记录" value={stats?.performance_records?.toLocaleString() ?? '0'} icon="📊" color="rose" />
                </div>

                {/* ---- Scheduler Status Bar ---- */}
                {scheduler && (
                    <Card className="border border-border/60 bg-card/50 backdrop-blur-sm">
                        <CardContent className="p-4">
                            <div className="flex flex-wrap items-center gap-x-6 gap-y-2 text-sm">
                                <div className="flex items-center gap-2">
                                    <span className={`h-2.5 w-2.5 rounded-full ${scheduler.state === 'running' ? 'bg-emerald-500 animate-pulse' : 'bg-muted-foreground'}`} />
                                    <span className="font-medium">调度器</span>
                                    <Badge variant={scheduler.state === 'running' ? 'default' : 'secondary'} className="text-xs">
                                        {scheduler.state === 'running' ? '运行中' : scheduler.state === 'stopped' ? '已停止' : scheduler.state}
                                    </Badge>
                                </div>
                                <span className="text-muted-foreground">爬取轮次 <strong>{scheduler.crawl_rounds}</strong></span>
                                <span className="text-muted-foreground">计算轮次 <strong>{scheduler.calc_rounds}</strong></span>
                                {scheduler.current_group && (
                                    <span className="text-muted-foreground">
                                        当前群组 <Badge variant="outline" className="ml-1 text-xs">{scheduler.current_group}</Badge>
                                    </span>
                                )}
                                {scheduler.errors_total > 0 && (
                                    <span className="text-red-500">错误 <strong>{scheduler.errors_total}</strong></span>
                                )}
                                {scheduler.last_crawl && (
                                    <span className="text-muted-foreground text-xs">
                                        上次爬取 {new Date(scheduler.last_crawl).toLocaleTimeString()}
                                    </span>
                                )}
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
                        <div className="flex items-center gap-3">
                            <Select value={returnPeriod} onValueChange={setReturnPeriod}>
                                <SelectTrigger className="w-[160px]">
                                    <SelectValue placeholder="收益周期" />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="return_1d">T+1 日</SelectItem>
                                    <SelectItem value="return_3d">T+3 日</SelectItem>
                                    <SelectItem value="return_5d">T+5 日</SelectItem>
                                    <SelectItem value="return_10d">T+10 日</SelectItem>
                                    <SelectItem value="return_20d">T+20 日</SelectItem>
                                    <SelectItem value="return_60d">T+60 日</SelectItem>
                                    <SelectItem value="return_120d">T+120 日</SelectItem>
                                    <SelectItem value="return_250d">T+250 日</SelectItem>
                                </SelectContent>
                            </Select>
                            <span className="text-sm text-muted-foreground">Top 30</span>
                        </div>

                        <Card>
                            <CardContent className="p-0">
                                <div className="overflow-x-auto">
                                    <table className="w-full text-sm">
                                        <thead>
                                            <tr className="border-b bg-muted/40">
                                                <th className="px-4 py-3 text-left font-medium text-muted-foreground">#</th>
                                                <th className="px-4 py-3 text-left font-medium text-muted-foreground">股票</th>
                                                <th className="px-4 py-3 text-right font-medium text-muted-foreground">提及数</th>
                                                <th className="px-4 py-3 text-right font-medium text-muted-foreground">胜率</th>
                                                <th className="px-4 py-3 text-right font-medium text-muted-foreground">平均收益</th>
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
                                                    <tr key={item.stock_code} className="border-b last:border-0 hover:bg-muted/30 transition-colors">
                                                        <td className="px-4 py-3 font-mono text-muted-foreground">{i + 1}</td>
                                                        <td className="px-4 py-3">
                                                            <div className="flex items-center gap-2">
                                                                <span className="font-medium">{item.stock_name || item.stock_code}</span>
                                                                <span className="text-xs text-muted-foreground font-mono">{item.stock_code}</span>
                                                            </div>
                                                        </td>
                                                        <td className="px-4 py-3 text-right font-mono">{item.mention_count}</td>
                                                        <td className="px-4 py-3 text-right">
                                                            <div className="flex items-center justify-end gap-2">
                                                                <Progress value={item.win_rate * 100} className="h-2 w-16" />
                                                                <span className="font-mono text-xs w-12 text-right">
                                                                    {(item.win_rate * 100).toFixed(0)}%
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
                                                    {s.group_count} 群
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
                                                <CardTitle className="text-base flex items-center gap-2">
                                                    <Badge variant="outline" className="font-mono text-xs">{g.group_id}</Badge>
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
        </div>
    );
}

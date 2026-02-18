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
    Sparkles, Settings, Send, History, Bot, FileText as FileTextIcon, Users
} from 'lucide-react';

interface StockDashboardProps {
    groupId: number;
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
export default function StockDashboard({ groupId }: StockDashboardProps) {
    const [activeView, setActiveView] = useState<'overview' | 'winrate' | 'sector' | 'signals' | 'ai'>('overview');
    const [stats, setStats] = useState<any>(null);
    const [mentions, setMentions] = useState<any[]>([]);
    const [winRate, setWinRate] = useState<any[]>([]);
    const [sectors, setSectors] = useState<any[]>([]);
    const [signals, setSignals] = useState<any[]>([]);
    const [loading, setLoading] = useState(true);
    const [scanning, setScanning] = useState(false);
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
            const s = await apiClient.getStockStats(groupId);
            setStats(s);
        } catch { /* ignore */ }
    }, [groupId]);

    const loadMentions = useCallback(async () => {
        try {
            const res = await apiClient.getStockMentions(groupId, {
                page: mentionPage,
                per_page: 20,
                stock_code: searchStock || undefined,
                sort_by: 'mention_time',
                order: 'desc',
            });
            setMentions(res.mentions || []);
            setMentionTotal(res.total || 0);
        } catch { /* ignore */ }
    }, [groupId, mentionPage, searchStock]);

    const loadWinRate = useCallback(async () => {
        try {
            const res = await apiClient.getStockWinRate(groupId, {
                min_mentions: 2,
                return_period: returnPeriod,
                limit: 50,
            });
            setWinRate(res || []);
        } catch { /* ignore */ }
    }, [groupId, returnPeriod]);

    const loadSectors = useCallback(async () => {
        try {
            const res = await apiClient.getSectorHeat(groupId);
            setSectors(res || []);
        } catch { /* ignore */ }
    }, [groupId]);

    const loadSignals = useCallback(async () => {
        try {
            const res = await apiClient.getStockSignals(groupId, 7, 2);
            setSignals(res || []);
        } catch { /* ignore */ }
    }, [groupId]);

    const loadAll = useCallback(async () => {
        setLoading(true);
        await Promise.all([loadStats(), loadMentions(), loadSectors()]);
        setLoading(false);
    }, [loadStats, loadMentions, loadSectors]);

    useEffect(() => { loadAll(); }, [loadAll]);
    useEffect(() => { loadMentions(); }, [mentionPage, searchStock, loadMentions]);

    useEffect(() => {
        if (activeView === 'winrate') loadWinRate();
        else if (activeView === 'signals') loadSignals();
    }, [activeView, loadWinRate, loadSignals]);

    /* ── scan ── */
    const handleScan = async (force = false) => {
        setScanning(true);
        try {
            const res = await apiClient.scanStocks(groupId, force);
            toast.success(`扫描任务已启动: ${res.task_id}`);
            // Poll stats every 5 s for 2 min
            const poll = setInterval(async () => { await loadStats(); }, 5000);
            setTimeout(() => { clearInterval(poll); loadAll(); }, 120_000);
        } catch (err) {
            toast.error(`扫描失败: ${err instanceof Error ? err.message : '未知错误'}`);
        } finally {
            setScanning(false);
        }
    };

    /* ── stock events drawer ── */
    const openStockDetail = async (stockCode: string) => {
        setSelectedStock(stockCode);
        setEventsLoading(true);
        try {
            const res = await apiClient.getStockEvents(groupId, stockCode);
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
                <span className="ml-2 text-muted-foreground">加载股票分析数据...</span>
            </div>
        );
    }

    return (
        <div className="flex flex-col gap-4 h-full">
            {/* ─── Header cards ─── */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                <Card className="bg-gradient-to-br from-violet-500/10 to-purple-500/5 border-violet-500/20">
                    <CardContent className="p-3">
                        <div className="flex items-center gap-2 mb-1">
                            <Activity className="h-4 w-4 text-violet-400" />
                            <span className="text-xs text-muted-foreground">总提及</span>
                        </div>
                        <p className="text-2xl font-bold">{stats?.total_mentions ?? 0}</p>
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
                            <span className="text-xs text-muted-foreground">已计算</span>
                        </div>
                        <p className="text-2xl font-bold">{stats?.performance_calculated ?? 0}</p>
                    </CardContent>
                </Card>
            </div>

            {/* ─── Top mentioned badges ─── */}
            {stats?.top_mentioned && stats.top_mentioned.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                    <span className="text-xs text-muted-foreground self-center mr-1">🔥 高频:</span>
                    {stats.top_mentioned.slice(0, 12).map((s: any) => (
                        <Badge
                            key={s.stock_code}
                            variant="secondary"
                            className="cursor-pointer hover:bg-primary/20 transition-colors text-xs"
                            onClick={() => openStockDetail(s.stock_code)}
                        >
                            {s.stock_name} <span className="ml-1 opacity-60">{s.count}</span>
                        </Badge>
                    ))}
                </div>
            )}

            {/* ─── Action bar ─── */}
            <div className="flex items-center gap-2 flex-wrap">
                <Button
                    size="sm"
                    variant="default"
                    onClick={() => handleScan(false)}
                    disabled={scanning}
                    className="gap-1.5"
                >
                    {scanning ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
                    {scanning ? '扫描中...' : '一键扫描'}
                </Button>
                <Button size="sm" variant="outline" onClick={() => handleScan(true)} disabled={scanning}>
                    强制重扫
                </Button>
                <div className="flex-1" />
                <Button
                    size="sm"
                    variant={activeView === 'overview' ? 'default' : 'ghost'}
                    onClick={() => setActiveView('overview')}
                    className="gap-1"
                >
                    <Activity className="h-3.5 w-3.5" /> 概览
                </Button>
                <Button
                    size="sm"
                    variant={activeView === 'winrate' ? 'default' : 'ghost'}
                    onClick={() => setActiveView('winrate')}
                    className="gap-1"
                >
                    <TrendingUp className="h-3.5 w-3.5" /> 胜率
                </Button>
                <Button
                    size="sm"
                    variant={activeView === 'sector' ? 'default' : 'ghost'}
                    onClick={() => setActiveView('sector')}
                    className="gap-1"
                >
                    <Flame className="h-3.5 w-3.5" /> 板块
                </Button>
                <Button
                    size="sm"
                    variant={activeView === 'signals' ? 'default' : 'ghost'}
                    onClick={() => setActiveView('signals')}
                    className="gap-1"
                >
                    <Zap className="h-3.5 w-3.5" /> 信号
                </Button>
                <Button
                    size="sm"
                    variant={activeView === 'ai' ? 'default' : 'ghost'}
                    onClick={() => {
                        setActiveView('ai');
                        // load AI config and history on first open
                        if (!aiConfig) {
                            apiClient.getAIConfig().then(setAiConfig).catch(() => { });
                            apiClient.getAIHistory(groupId).then(setAiHistory).catch(() => { });
                        }
                    }}
                    className="gap-1"
                >
                    <Sparkles className="h-3.5 w-3.5" /> AI分析
                </Button>
            </div>

            {/* ─── Views ─── */}
            <div className="flex-1 min-h-0 overflow-auto">

                {/* ── OVERVIEW: recent mentions ── */}
                {activeView === 'overview' && (
                    <div className="space-y-3">
                        {/* search bar */}
                        <div className="flex items-center gap-2">
                            <div className="relative flex-1 max-w-xs">
                                <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                                <Input
                                    placeholder="搜索股票代码..."
                                    className="pl-8 h-8 text-xs"
                                    value={searchStock}
                                    onChange={e => { setSearchStock(e.target.value); setMentionPage(1); }}
                                />
                            </div>
                            <span className="text-xs text-muted-foreground">共 {mentionTotal} 条</span>
                        </div>

                        {/* mention table */}
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
                                            {stats?.total_mentions === 0 ? '暂无数据，请先执行"一键扫描"' : '无匹配结果'}
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
                                            <td className={`p-2 text-right font-mono ${pctColor(m.excess_5d)}`}>
                                                {fmtPct(m.excess_5d)}
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>

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
                                            <td className="p-2 text-right">{w.mention_count}</td>
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
                                                <span>近期提及 <b className="text-foreground">{sig.recent_mentions}</b> 次</span>
                                                <span>历史胜率 <b className={pctColor(sig.historical_win_rate > 50 ? 1 : -1)}>
                                                    {sig.historical_win_rate != null ? `${sig.historical_win_rate.toFixed(0)}%` : '—'}
                                                </b></span>
                                                <span>均收益 <b className={pctColor(sig.avg_return_5d)}>
                                                    {fmtPct(sig.avg_return_5d)}
                                                </b></span>
                                            </div>
                                            {sig.reason && (
                                                <p className="text-xs text-muted-foreground mt-1 line-clamp-1">{sig.reason}</p>
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
                            {/* Stock Analysis */}
                            <Card className="bg-gradient-to-br from-violet-500/10 to-purple-500/5 border-violet-500/20 hover:border-violet-500/40 transition-colors">
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
                                        />
                                        <Button
                                            size="sm"
                                            disabled={!aiStockInput.trim() || aiLoading || !aiConfig?.configured}
                                            onClick={async () => {
                                                setAiLoading(true);
                                                setAiResult(null);
                                                try {
                                                    const res = await apiClient.aiAnalyzeStock(groupId, aiStockInput.trim());
                                                    setAiResult({ type: 'stock', ...res });
                                                    apiClient.getAIHistory(groupId).then(setAiHistory).catch(() => { });
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
                                                const res = await apiClient.aiDailyBrief(groupId);
                                                setAiResult({ type: 'daily', ...res });
                                                apiClient.getAIHistory(groupId).then(setAiHistory).catch(() => { });
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
                                                const res = await apiClient.aiConsensus(groupId);
                                                setAiResult({ type: 'consensus', ...res });
                                                apiClient.getAIHistory(groupId).then(setAiHistory).catch(() => { });
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

                        {/* AI Loading */}
                        {aiLoading && (
                            <Card className="border-dashed">
                                <CardContent className="p-8 flex flex-col items-center gap-3">
                                    <Loader2 className="h-8 w-8 animate-spin text-violet-500" />
                                    <p className="text-sm text-muted-foreground">AI 正在分析中，请稍候...</p>
                                    <p className="text-xs text-muted-foreground/60">首次分析可能需要 10-30 秒</p>
                                </CardContent>
                            </Card>
                        )}

                        {/* AI Result */}
                        {aiResult?.content && !aiLoading && (
                            <Card className="border-violet-500/20">
                                <CardContent className="p-4 space-y-3">
                                    <div className="flex items-center justify-between">
                                        <div className="flex items-center gap-2">
                                            <Sparkles className="h-4 w-4 text-violet-400" />
                                            <span className="text-sm font-semibold">
                                                {aiResult.type === 'stock' ? `${aiResult.stock_name || aiResult.stock_code} 分析报告` :
                                                    aiResult.type === 'daily' ? '每日投资简报' : '市场共识分析'}
                                            </span>
                                            {aiResult.from_cache && (
                                                <Badge variant="secondary" className="text-[10px]">缓存</Badge>
                                            )}
                                        </div>
                                        <div className="flex items-center gap-2 text-xs text-muted-foreground">
                                            <span>{aiResult.model}</span>
                                            {aiResult.tokens_used > 0 && <span>{aiResult.tokens_used} tokens</span>}
                                        </div>
                                    </div>
                                    {/* Render Markdown as styled content */}
                                    <div
                                        className="prose prose-sm dark:prose-invert max-w-none
                                            prose-headings:text-foreground prose-headings:font-semibold
                                            prose-p:text-muted-foreground prose-p:leading-relaxed
                                            prose-strong:text-foreground prose-li:text-muted-foreground
                                            prose-table:text-xs"
                                        dangerouslySetInnerHTML={{ __html: simpleMarkdown(aiResult.content) }}
                                    />
                                    <div className="text-[10px] text-muted-foreground/50 text-right">
                                        生成于 {aiResult.created_at}
                                    </div>
                                </CardContent>
                            </Card>
                        )}

                        {/* AI History */}
                        {aiHistory.length > 0 && (
                            <div className="space-y-2">
                                <div className="flex items-center gap-2 text-xs text-muted-foreground">
                                    <History className="h-3.5 w-3.5" />
                                    <span>分析历史</span>
                                </div>
                                <div className="space-y-1">
                                    {aiHistory.map((h: any) => (
                                        <div
                                            key={h.id}
                                            className="flex items-center justify-between p-2 rounded-md hover:bg-muted/40 cursor-pointer transition-colors text-xs"
                                            onClick={async () => {
                                                try {
                                                    const detail = await apiClient.getAIHistoryDetail(groupId, h.id);
                                                    setAiResult({
                                                        type: detail.summary_type,
                                                        content: detail.content,
                                                        model: detail.model,
                                                        tokens_used: detail.tokens_used,
                                                        created_at: detail.created_at,
                                                        from_cache: true
                                                    });
                                                } catch {
                                                    toast.error('加载详情失败');
                                                }
                                            }}
                                        >
                                            <div className="flex items-center gap-2">
                                                <Badge variant="outline" className="text-[10px]">
                                                    {h.summary_type === 'stock' ? '个股' :
                                                        h.summary_type === 'daily' ? '简报' : '共识'}
                                                </Badge>
                                                <span className="text-muted-foreground truncate max-w-[200px]">
                                                    {h.target_key || h.preview?.slice(0, 40)}
                                                </span>
                                            </div>
                                            <div className="flex items-center gap-2 text-muted-foreground/60">
                                                <span>{h.created_at?.slice(5, 16)}</span>
                                                <ChevronRight className="h-3 w-3" />
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}
                    </div>
                )}
            </div>

            {/* ─── Stock Detail Drawer ─── */}
            {selectedStock && (
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
                                                        {ev.mention_time ? new Date(ev.mention_time).toLocaleString('zh-CN') : '—'}
                                                    </span>
                                                </div>
                                                {ev.context && (
                                                    <p className="text-xs text-muted-foreground line-clamp-3 bg-muted/30 rounded p-2">
                                                        {ev.context}
                                                    </p>
                                                )}
                                                <div className="grid grid-cols-5 gap-1 text-center text-[10px]">
                                                    {[
                                                        { label: 'T+1', val: ev.return_1d },
                                                        { label: 'T+3', val: ev.return_3d },
                                                        { label: 'T+5', val: ev.return_5d },
                                                        { label: 'T+10', val: ev.return_10d },
                                                        { label: 'T+20', val: ev.return_20d },
                                                    ].map(({ label, val }) => (
                                                        <div key={label} className="bg-muted/20 rounded p-1">
                                                            <div className="text-muted-foreground mb-0.5">{label}</div>
                                                            <div className={`font-mono font-medium ${pctColor(val)}`}>
                                                                {fmtPct(val)}
                                                            </div>
                                                        </div>
                                                    ))}
                                                </div>
                                                {(ev.excess_5d != null || ev.max_return != null) && (
                                                    <div className="flex gap-3 text-[10px] text-muted-foreground">
                                                        {ev.excess_5d != null && (
                                                            <span>超额5d: <b className={pctColor(ev.excess_5d)}>{fmtPct(ev.excess_5d)}</b></span>
                                                        )}
                                                        {ev.max_return != null && (
                                                            <span>最高: <b className="text-emerald-500">{fmtPct(ev.max_return)}</b></span>
                                                        )}
                                                        {ev.max_drawdown != null && (
                                                            <span>最大回撤: <b className="text-red-500">{fmtPct(ev.max_drawdown)}</b></span>
                                                        )}
                                                    </div>
                                                )}
                                            </CardContent>
                                        </Card>
                                    ))}
                                </>
                            )}
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}

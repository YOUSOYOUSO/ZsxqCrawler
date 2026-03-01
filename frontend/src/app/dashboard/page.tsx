'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useRouter } from 'next/navigation';
import Link from 'next/link';
import { toast } from 'sonner';
import { Menu, RefreshCw, Activity, Globe, ArrowLeft, Search } from 'lucide-react';
import { apiClient, Task, TaskSummaryResponse } from '@/lib/api';
import type { GlobalHotWordItem, GlobalHotWordResponse } from '@/lib/api';
import StockDashboard from '@/components/StockDashboard';
import TaskLogViewer from '@/components/TaskLogViewer';
import GlobalOpsPanel, { SchedulerStatus } from '@/components/GlobalOpsPanel';
import GlobalTopicList from '@/components/GlobalTopicList';
import MiddlePanelShell, { middlePanelTokens } from '@/components/MiddlePanelShell';
import dynamic from 'next/dynamic';
const HotWordCloud = dynamic(() => import('@/components/HotWordCloud'), { ssr: false });
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetTrigger } from '@/components/ui/sheet';
import { ScrollArea } from '@/components/ui/scroll-area';
import StockDetailDrawer from '@/components/StockDetailDrawer';

interface GlobalStats {
  group_count: number;
  total_topics: number;
  unique_stocks: number;
  total_mentions: number;
}

interface FeatureFlags {
  scheduler_next_runs?: boolean;
}

export default function DashboardPage() {
  const router = useRouter();
  const [activeTab, setActiveTab] = useState<'topics' | 'stocks' | 'logs'>('topics');
  const [searchTerm, setSearchTerm] = useState('');
  const [stats, setStats] = useState<GlobalStats | null>(null);
  const [scheduler, setScheduler] = useState<SchedulerStatus | null>(null);
  const [groups, setGroups] = useState<Array<Record<string, unknown>>>([]);
  const [loadingFlags, setLoadingFlags] = useState({
    scheduler: false,
    scan: false,
    refreshing: false,
  });
  const [topicStats, setTopicStats] = useState({ whitelistGroupCount: 0, total: 0 });
  const [selectedStock, setSelectedStock] = useState<string | null>(null);
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);
  const [features, setFeatures] = useState<FeatureFlags>({});
  const [nextRuns, setNextRuns] = useState<string[]>([]);
  // 日志联动状态
  const [logTaskId, setLogTaskId] = useState<string | null>(null);
  const [logSourceLabel, setLogSourceLabel] = useState<string>('全局日志总览');
  const [currentTask, setCurrentTask] = useState<Task | null>(null);

  // 全局热词云
  const [hotWords, setHotWords] = useState<GlobalHotWordItem[]>([]);
  const [hotWordsMeta, setHotWordsMeta] = useState<GlobalHotWordResponse | null>(null);
  const [hotWordWindow, setHotWordWindow] = useState<24 | 36 | 48 | 168>(24);
  const [hotWordsLoading, setHotWordsLoading] = useState(false);
  const getErrorMessage = (error: unknown) => error instanceof Error ? error.message : '未知错误';

  const refreshHotWords = useCallback(async (force: boolean = false) => {
    setHotWordsLoading(true);
    try {
      const [res] = await Promise.all([
        apiClient.getGlobalHotWords({
          windowHours: hotWordWindow,
          limit: 45,
          force,
          normalize: true,
          fallback: true,
        }),
        force ? new Promise(r => setTimeout(r, 600)) : Promise.resolve()
      ]);
      if (res) {
        setHotWords(res.words || []);
        setHotWordsMeta(res);
      }
      if (force) toast.success('词云缓存已强制刷新 (本操作无后台任务日志)');
    } catch (error) {
      console.error('Failed to refresh hot words:', error);
      if (force) toast.error('词云数据刷新失败');
    } finally {
      setHotWordsLoading(false);
    }
  }, [hotWordWindow]);

  const refreshSchedulerStatus = useCallback(async () => {
    try {
      const next = await apiClient.getSchedulerStatus();
      setScheduler((prev) => {
        const prevSig = prev ? JSON.stringify(prev) : '';
        const nextSig = next ? JSON.stringify(next) : '';
        return prevSig === nextSig ? prev : next;
      });
    } catch (error) {
      console.error('Failed to refresh scheduler status:', error);
    }
  }, []);

  const prevCalcRounds = useRef<number | undefined>(undefined);
  useEffect(() => {
    if (scheduler?.calc_rounds !== undefined) {
      if (prevCalcRounds.current !== undefined && scheduler.calc_rounds > prevCalcRounds.current) {
        void refreshHotWords(true);
      }
      prevCalcRounds.current = scheduler.calc_rounds;
    }
  }, [scheduler?.calc_rounds, refreshHotWords]);

  const loadDisplayData = useCallback(async (silent = false) => {
    if (!silent) {
      setLoadingFlags(prev => ({ ...prev, scan: true }));
    }
    try {
      const promises: Array<Promise<unknown>> = [
        apiClient.getGlobalStats(),
        apiClient.getSchedulerStatus(),
        apiClient.getGlobalSectorHeat(),
        apiClient.getGlobalSignals(7, 2),
        apiClient.getGlobalGroups(),
        apiClient.getGlobalWinRate(2, 'return_5d', 20),
        apiClient.getFeatures(),
        apiClient.getSchedulerNextRuns(3),
      ];

      if (!silent) {
        promises.push(apiClient.getGlobalHotWords({
          windowHours: hotWordWindow,
          limit: 45,
          force: false,
          normalize: true,
          fallback: true,
        }));
      }

      const results = await Promise.allSettled(promises);

      if (results[0].status === 'fulfilled') setStats(results[0].value);
      if (results[1].status === 'fulfilled') {
        const nextScheduler = results[1].value as SchedulerStatus;
        setScheduler((prev) => {
          const prevSig = prev ? JSON.stringify(prev) : '';
          const nextSig = nextScheduler ? JSON.stringify(nextScheduler) : '';
          return prevSig === nextSig ? prev : nextScheduler;
        });
      }
      if (results[4].status === 'fulfilled') {
        const rows = Array.isArray(results[4].value) ? results[4].value : (results[4].value?.groups || []);
        setGroups(rows);
      }
      if (results[6].status === 'fulfilled') setFeatures(results[6].value || {});
      if (results[7].status === 'fulfilled') setNextRuns(results[7].value || []);

      if (!silent && results[8] && results[8].status === 'fulfilled') {
        const hotRes = results[8].value as GlobalHotWordResponse;
        setHotWords(hotRes?.words || []);
        setHotWordsMeta(hotRes || null);
      }
    } catch (error) {
      console.error('Failed to load global data:', error);
      toast.error('加载全局数据失败');
    } finally {
      if (!silent) {
        setLoadingFlags(prev => ({ ...prev, scan: false }));
      }
    }
  }, [hotWordWindow]);

  useEffect(() => {
    void loadDisplayData();
  }, [loadDisplayData]);

  useEffect(() => {
    void refreshHotWords(false);
  }, [hotWordWindow, refreshHotWords]);

  useEffect(() => {
    let tick = 0;
    const timer = setInterval(() => {
      tick += 1;
      const hidden = document.visibilityState !== 'visible';
      if (hidden && tick % 4 !== 0) {
        return;
      }
      void refreshSchedulerStatus();
    }, 12000);

    return () => clearInterval(timer);
  }, [refreshSchedulerStatus]);

  useEffect(() => {
    if (!logTaskId) {
      setCurrentTask(null);
      return;
    }

    let disposed = false;
    const terminal = new Set(['completed', 'failed', 'cancelled']);

    const pollTask = async () => {
      try {
        const task = await apiClient.getTask(logTaskId);
        if (disposed) return;
        setCurrentTask(prev => {
          if (!prev) return task;
          if (
            prev.task_id === task.task_id &&
            prev.status === task.status &&
            prev.updated_at === task.updated_at
          ) {
            return prev;
          }
          return task;
        });

        if (terminal.has(task.status)) {
          void refreshSchedulerStatus();
        }
      } catch (error) {
        if (!disposed) {
          console.error('Failed to poll task:', error);
        }
      }
    };

    void pollTask();
    let tick = 0;
    const timer = setInterval(() => {
      tick += 1;
      const hidden = document.visibilityState !== 'visible';
      if (hidden && tick % 4 !== 0) {
        return;
      }
      void pollTask();
    }, 6000);

    return () => {
      disposed = true;
      clearInterval(timer);
    };
  }, [logTaskId, refreshSchedulerStatus]);

  const openLogs = (taskId?: string | null, sourceLabel: string = '系统任务') => {
    if (taskId) {
      setLogTaskId(taskId);
      setLogSourceLabel(sourceLabel);
    } else {
      setLogTaskId(null);
      setLogSourceLabel('全局日志总览');
    }
    setActiveTab('logs');
  };

  const taskSourceLabel = useCallback((taskType?: string) => {
    const t = String(taskType || '').trim();
    if (!t) return '系统任务';
    if (t.startsWith('global_analyze_performance') || t.startsWith('global_analyze')) return '全区收益计算';
    if (t.startsWith('global_crawl')) return '全区话题采集';
    if (t.startsWith('global_files_collect')) return '全区文件收集';
    if (t.startsWith('global_files_download')) return '全区文件下载';
    if (t.startsWith('global_cleanup_blacklist')) return '黑名单数据清理';
    if (t.startsWith('global_scan')) return '全局扫描';
    if (t.startsWith('stock_scan_')) return '群组收益扫描';
    return '系统任务';
  }, []);

  const pickRunningGlobalTask = useCallback((summary: TaskSummaryResponse | null | undefined): Task | null => {
    if (!summary) return null;
    const byTaskType = summary.running_by_task_type || {};
    const byCategory = summary.running_by_type || {};
    const candidates: Array<Task | undefined> = [
      byTaskType['global_analyze_performance'],
      byTaskType['global_analyze'],
      byTaskType['global_crawl'],
      byTaskType['global_files_collect'],
      byTaskType['global_files_download'],
      byTaskType['global_cleanup_blacklist'],
      byTaskType['global_scan'],
      byCategory['analyze'],
      byCategory['crawl'],
      byCategory['files'],
    ];
    return (candidates.find(Boolean) as Task | undefined) || null;
  }, []);

  const recoverLogTaskIfNeeded = useCallback(async () => {
    // 已经绑定到运行中任务时不抢占
    if (logTaskId && currentTask && ['pending', 'running', 'stopping'].includes(currentTask.status)) {
      return;
    }
    try {
      const summary = await apiClient.getTaskSummary();
      const runningTask = pickRunningGlobalTask(summary);
      if (!runningTask?.task_id) return;
      setLogTaskId(runningTask.task_id);
      setLogSourceLabel(taskSourceLabel(runningTask.type));
      setCurrentTask(runningTask);
    } catch (error) {
      console.warn('Failed to recover running task for logs:', error);
    }
  }, [currentTask, logTaskId, pickRunningGlobalTask, taskSourceLabel]);

  useEffect(() => {
    void recoverLogTaskIfNeeded();
  }, [recoverLogTaskIfNeeded]);

  useEffect(() => {
    let tick = 0;
    const timer = setInterval(() => {
      tick += 1;
      const hidden = document.visibilityState !== 'visible';
      if (hidden && tick % 2 !== 0) {
        return;
      }
      void recoverLogTaskIfNeeded();
    }, 15000);
    return () => clearInterval(timer);
  }, [recoverLogTaskIfNeeded]);

  const handleToggleScheduler = async () => {
    setLoadingFlags(prev => ({ ...prev, scheduler: true }));
    try {
      const nextTargetState = scheduler?.state === 'running' ? 'stopped' : 'running';
      const res = nextTargetState === 'running'
        ? await apiClient.startScheduler()
        : await apiClient.stopScheduler();
      if (res?.scheduler) {
        setScheduler(res.scheduler);
      } else {
        await refreshSchedulerStatus();
      }

      if (res?.success === false) {
        toast.error(res?.message || `调度器${nextTargetState === 'running' ? '启动' : '停止'}失败`);
      } else {
        toast.success(res?.message || `调度器已${nextTargetState === 'running' ? '启动' : '停止'}`);
      }
    } catch (error: unknown) {
      toast.error(`操作调度器失败: ${getErrorMessage(error)}`);
    } finally {
      setLoadingFlags(prev => ({ ...prev, scheduler: false }));
    }
  };

  const handleScanGlobal = async () => {
    setLoadingFlags(prev => ({ ...prev, scan: true }));
    try {
      const res = await apiClient.refreshLocalGroups();
      await loadDisplayData(true);
      if (res?.success) {
        toast.success(`群组元数据已刷新，共扫描 ${res.count ?? 0} 个本地群组`);
      } else {
        toast.warning(`群组元数据刷新完成（降级结果），当前群组数 ${res?.count ?? 0}`);
      }
    } catch (error: unknown) {
      toast.error(`群组元数据刷新失败: ${getErrorMessage(error)}`);
    } finally {
      setLoadingFlags(prev => ({ ...prev, scan: false }));
    }
  };

  const handleRefresh = async () => {
    setLoadingFlags(prev => ({ ...prev, refreshing: true }));
    try {
      await loadDisplayData(false);
      toast.success('仪表盘数据已刷新');
    } finally {
      setLoadingFlags(prev => ({ ...prev, refreshing: false }));
    }
  };

  const formatTs = (ts?: string | null) => {
    if (!ts) return '—';
    try {
      return new Date(ts).toLocaleString('zh-CN', {
        month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit'
      });
    } catch {
      return ts;
    }
  };

  // if (loading && !stats) {
  //   return (
  //     <div className="flex h-screen items-center justify-center bg-slate-50/50">
  //       <Loader2 className="h-8 w-8 animate-spin text-primary" />
  //       <span className="ml-2 mt-1 text-sm text-muted-foreground">加载全局数据...</span>
  //     </div>
  //   );
  // }

  const handleTabChange = useCallback((v: string) => {
    setActiveTab(v as 'topics' | 'stocks' | 'logs');
  }, []);

  const handleMentionClick = useCallback((stockCode: string) => {
    setSelectedStock(stockCode);
  }, []);

  const handleStockDataChanged = useCallback(() => loadDisplayData(true), [loadDisplayData]);

  const middleTabs = useMemo(() => ([
    {
      value: 'topics',
      label: (
        <>
          <span>💬</span>
          <span>话题列表</span>
        </>
      ),
      content: (
        <div className="h-full flex flex-col min-h-0">
          <GlobalTopicList
            searchTerm={searchTerm}
            onStatsUpdate={setTopicStats}
            onMentionClick={handleMentionClick}
          />
        </div>
      ),
    },
    {
      value: 'stocks',
      label: (
        <>
          <span>📊</span>
          <span>股票分析</span>
        </>
      ),
      content: (
        <div className="h-full">
          <StockDashboard
            mode="global"
            hideScanActions={true}
            onDataChanged={handleStockDataChanged}
            initialView="winrate"
            allowedViews={['winrate', 'sector', 'signals', 'ai']}
            surfaceVariant="group-consistent"
            hideSummaryCards={true}
          />
        </div>
      ),
    },
    {
      value: 'logs',
      label: (
        <>
          <span>📜</span>
          <span>任务日志</span>
        </>
      ),
      content: (
        <div className="h-full flex flex-col">
          <div className="mb-3 flex items-center gap-2 text-xs text-muted-foreground shrink-0">
            <Badge variant="outline">日志来源</Badge>
            <span>{logSourceLabel}</span>
            {currentTask?.status && <Badge variant="secondary">{currentTask.status}</Badge>}
          </div>
          <div className={middlePanelTokens.logSurface}>
            <TaskLogViewer taskId={logTaskId} inline={true} onClose={() => undefined} />
          </div>
        </div>
      ),
      contentClassName: 'h-full flex flex-col min-h-0',
    },
  ]), [searchTerm, handleMentionClick, handleStockDataChanged, logSourceLabel, currentTask?.status, logTaskId]);

  return (
    <div className="h-screen bg-slate-50/50 overflow-hidden flex flex-col">
      {/* 顶部导航 - 与群组详情页风格一致 */}
      <div className="flex-shrink-0 p-4 bg-white">
        <div className="flex items-center gap-4">
          <Button
            variant="ghost"
            onClick={() => router.push('/')}
            className="flex items-center gap-2"
          >
            <ArrowLeft className="h-4 w-4" />
            返回群组列表
          </Button>

          <div className="flex items-center gap-4 flex-1 justify-center max-w-2xl mx-auto">
            <h1 className="text-lg font-bold whitespace-nowrap flex items-center gap-2">
              <span className="text-xl">📊</span> 小作文数据大盘
            </h1>
            <div className="relative flex-1">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 h-4 w-4" />
              <Input
                placeholder="搜索话题内容 / 话题ID..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-10"
              />
            </div>
            <Button onClick={handleRefresh} disabled={loadingFlags.refreshing}>
              {loadingFlags.refreshing ? <RefreshCw className="h-4 w-4 animate-spin mr-2" /> : <RefreshCw className="h-4 w-4 mr-2" />}
              刷新
            </Button>
          </div>

          {/* 移动端菜单 */}
          <Sheet open={isMobileMenuOpen} onOpenChange={setIsMobileMenuOpen}>
            <SheetTrigger asChild>
              <Button variant="ghost" size="icon" className="lg:hidden">
                <Menu className="h-5 w-5" />
              </Button>
            </SheetTrigger>
            <SheetContent side="right" className="w-[85vw] sm:w-[400px] overflow-y-auto">
              <SheetHeader className="mb-4">
                <SheetTitle>全局操作</SheetTitle>
              </SheetHeader>
              <div className="space-y-4">
                <Button variant="outline" className="w-full justify-start" onClick={() => { handleRefresh(); setIsMobileMenuOpen(false); }}>
                  {loadingFlags.refreshing ? <RefreshCw className="h-4 w-4 animate-spin mr-2" /> : <RefreshCw className="h-4 w-4 mr-2" />}
                  刷新数据
                </Button>
                <GlobalOpsPanel
                  scheduler={scheduler}
                  loading={loadingFlags}
                  groups={groups}
                  onRefresh={handleRefresh}
                  onOpenLogs={(taskId) => { openLogs(taskId); setIsMobileMenuOpen(false); }}
                  onToggleScheduler={handleToggleScheduler}
                  onScanGlobal={handleScanGlobal}
                  onRefreshHotWords={() => refreshHotWords(true)}
                  hotWordsLoading={hotWordsLoading}
                />
              </div>
            </SheetContent>
          </Sheet>
        </div>
      </div>

      <main className="flex-1 flex gap-4 px-4 pb-4 min-h-0 w-full max-w-[1800px] mx-auto">
        <div className="flex-1 flex gap-4 min-h-0 w-full">
          <div className="hidden lg:flex w-[320px] flex-shrink-0 h-full overflow-hidden flex-col">
            <Card className="border border-gray-200 shadow-none h-full">
              <ScrollArea className="h-full">
                <CardContent className="p-4 flex flex-col">
                  <div className="flex items-center gap-3 mb-4">
                    <div className="w-12 h-12 rounded-lg bg-gradient-to-br from-indigo-500 to-purple-600 flex items-center justify-center text-white font-bold shadow-sm">
                      <Globe className="h-6 w-6" />
                    </div>
                    <div className="flex-1">
                      <h2 className="text-lg font-bold text-gray-900 mb-1">全局监控总览</h2>
                      <div className="flex items-center gap-2">
                        <Badge variant="secondary" className="bg-blue-50 text-blue-700 hover:bg-blue-50">系统级</Badge>
                      </div>
                    </div>
                  </div>

                  <div className="grid grid-cols-2 gap-3 text-sm">
                    <div>
                      <span className="text-gray-500 block">监控群组</span>
                      <span className="text-gray-900 font-medium">{stats?.group_count || groups.length || 0}</span>
                    </div>
                    <div>
                      <span className="text-gray-500 block">累计话题</span>
                      <span className="text-gray-900 font-medium">{(stats?.total_topics || 0).toLocaleString()}</span>
                    </div>
                    <div>
                      <span className="text-gray-500 block">白名单群组</span>
                      <span className="text-gray-900 font-medium">{topicStats.whitelistGroupCount}</span>
                    </div>
                    <div>
                      <span className="text-gray-500 block">检索命中话题</span>
                      <span className="text-gray-900 font-medium">{topicStats.total}</span>
                    </div>
                    <div>
                      <span className="text-gray-500 block">发现股票</span>
                      <span className="text-blue-600 font-semibold">{stats?.unique_stocks || 0}</span>
                    </div>
                    <div>
                      <span className="text-gray-500 block">股票提及记录</span>
                      <span className="text-gray-900 font-medium">{(stats?.total_mentions || 0).toLocaleString()}</span>
                    </div>
                  </div>

                  <div className="mt-6 border-t border-gray-200 pt-4 flex flex-col items-center">
                    {features.scheduler_next_runs && nextRuns.length > 0 && (
                      <div className="mb-3 w-full rounded border border-gray-200 bg-gray-50 p-2 text-xs text-gray-600">
                        <div className="font-medium text-gray-800 mb-1">下次调度</div>
                        <div>{formatTs(nextRuns[0])}</div>
                      </div>
                    )}
                    <div className="w-full flex items-center justify-between mb-2 gap-2">
                      <h3 className="text-sm font-medium text-gray-900 flex items-center gap-1.5 opacity-90 whitespace-nowrap">
                        <Activity className="h-4 w-4 text-rose-500" />
                        热度词云
                      </h3>
                      <div className="flex items-center gap-1">
                        {([
                          { label: '24h', value: 24 as const },
                          { label: '36h', value: 36 as const },
                          { label: '48h', value: 48 as const },
                          { label: '7d', value: 168 as const },
                        ]).map((w) => (
                          <Button
                            key={w.value}
                            size="sm"
                            variant={hotWordWindow === w.value ? 'default' : 'outline'}
                            className="h-6 px-2 text-[10px]"
                            onClick={() => setHotWordWindow(w.value)}
                          >
                            {w.label}
                          </Button>
                        ))}
                      </div>
                    </div>
                    {hotWordsMeta && (
                      <div className="w-full mb-2 space-y-1">
                        <div className="text-[11px] text-gray-600">
                          当前窗口: {hotWordsMeta.window_hours_requested}h
                          {hotWordsMeta.window_hours_effective !== hotWordsMeta.window_hours_requested && (
                            <span> · 实际窗口: {hotWordsMeta.window_hours_effective}h</span>
                          )}
                        </div>
                        {hotWordsMeta.fallback_applied && hotWordsMeta.fallback_reason && (
                          <div className="text-[11px] text-amber-700 bg-amber-50 border border-amber-200 rounded px-2 py-1">
                            {hotWordsMeta.fallback_reason}
                          </div>
                        )}
                        {!hotWordsLoading && hotWords.length === 0 && (
                          <div className="text-[11px] text-gray-500">
                            当前窗口期内暂无数据
                          </div>
                        )}
                      </div>
                    )}
                    <div className="w-full flex justify-center">
                      <HotWordCloud
                        words={hotWords}
                        loading={hotWordsLoading}
                        onWordClick={(word) => {
                          setSearchTerm(word);
                          setActiveTab('topics');
                        }}
                      />
                    </div>
                  </div>

                  <div className="mt-4 border-t border-gray-200 pt-4">
                    <div className="text-sm font-medium text-gray-900 mb-2">群组概览</div>
                    <div className="space-y-2 max-h-[260px] overflow-auto pr-1">
                      {(groups || []).slice(0, 10).map((group: Record<string, unknown>) => {
                        const groupId = String(group['group_id'] || '');
                        const name = String(group['group_name'] || groupId || '未命名群组');
                        const topics = Number(group['total_topics'] || group['topics_count'] || 0);
                        const mentions = Number(group['total_mentions'] || group['mentions_count'] || 0);
                        return (
                          <div key={groupId} className="rounded-md border border-gray-200 p-2">
                            <div className="text-xs font-medium truncate">
                              <Link href={`/groups/${groupId}`} className="hover:text-blue-600">{name}</Link>
                            </div>
                            <div className="mt-1 text-[11px] text-gray-500 flex gap-3">
                              <span>话题 {topics}</span>
                              <span>提及 {mentions}</span>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                </CardContent>
              </ScrollArea>
            </Card>
          </div>

          <div className="flex-1 min-w-0 flex flex-col h-full overflow-hidden">
            <MiddlePanelShell
              value={activeTab}
              onValueChange={handleTabChange}
              tabs={middleTabs}
            />
          </div>

          <div className="hidden xl:flex w-[320px] flex-shrink-0 h-full overflow-hidden flex-col">
            <ScrollArea className="h-full">
              <GlobalOpsPanel
                scheduler={scheduler}
                loading={loadingFlags}
                groups={groups}
                onRefresh={handleRefresh}
                onOpenLogs={openLogs}
                onToggleScheduler={handleToggleScheduler}
                onScanGlobal={handleScanGlobal}
                onRefreshHotWords={() => refreshHotWords(true)}
                hotWordsLoading={hotWordsLoading}
              />
            </ScrollArea>
          </div>
        </div>
      </main>

      <StockDetailDrawer
        stockCode={selectedStock}
        onClose={() => setSelectedStock(null)}
      />
    </div>
  );
}

'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from '@/components/ui/alert-dialog';
import { Loader2, Activity, Play, Square, Zap, RefreshCw, BarChart3, Database, Download, Cloud, Globe, Settings, ChevronDown } from 'lucide-react';
import { apiClient, type TaskSummaryResponse } from '@/lib/api';
import { toast } from 'sonner';
import CrawlSettingsDialog from './CrawlSettingsDialog';

export interface SchedulerStatus {
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
  groups?: Record<string, unknown>;
}

export interface GlobalOpsLoadingFlags {
  scheduler: boolean;
  scan: boolean;
  refreshing: boolean;
}

interface GlobalOpsPanelProps {
  scheduler: SchedulerStatus | null;
  loading: GlobalOpsLoadingFlags;
  groups?: Array<Record<string, unknown>>;
  onRefresh: () => void;
  onOpenLogs?: (taskId?: string | null) => void;
  onToggleScheduler: () => void;
  onScanGlobal: () => void;
  onRefreshHotWords?: () => void;
  hotWordsLoading?: boolean;
}

interface ScanFilterPreviewData {
  total_groups: number;
  included_groups: Array<Record<string, unknown>>;
  excluded_groups: Array<Record<string, unknown>>;
  reason_counts?: Record<string, number>;
}

interface BlacklistCleanupPreviewData {
  blacklist_group_count: number;
  matched_group_count: number;
  total_stock_mentions: number;
  total_mention_performance: number;
  groups: Array<{
    group_id: string;
    group_name?: string;
    stock_mentions_count: number;
    mention_performance_count: number;
  }>;
}

interface MarketProbeDetail {
  provider: string;
  ok: boolean;
  latency_ms: number;
  symbol_rows: number;
  stock_rows: number;
  index_rows: number;
  error: string;
  routable?: boolean;
  disabled_reason?: string;
  cooldown_until?: number;
}

interface MarketProbeResult {
  success: boolean;
  symbol: string;
  window: string;
  details: MarketProbeDetail[];
  tested_at: string;
}

function formatTime(ts?: string | null) {
  if (!ts) return '—';
  try {
    return new Date(ts).toLocaleString('zh-CN', {
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
    });
  } catch {
    return ts;
  }
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  return '未知错误';
}

function formatTaskStatus(status?: string | null): string {
  const normalized = String(status || 'idle');
  switch (normalized) {
    case 'running': return '运行中';
    case 'pending': return '排队中';
    case 'stopping': return '停止中';
    case 'completed': return '已完成';
    case 'failed': return '失败';
    case 'cancelled': return '已取消';
    case 'stopped': return '已停止';
    case 'idle': return '空闲';
    default: return normalized;
  }
}

export default function GlobalOpsPanel({
  scheduler,
  loading,
  groups = [],
  onRefresh,
  onOpenLogs,
  onToggleScheduler,
  onScanGlobal,
  onRefreshHotWords,
  hotWordsLoading,
}: GlobalOpsPanelProps) {
  const [activeTab, setActiveTab] = useState('crawl');
  const [advancedAnalyzeOpen, setAdvancedAnalyzeOpen] = useState(false);

  // Global Crawl State
  const [crawlMode, setCrawlMode] = useState<'latest' | 'all' | 'incremental' | 'range'>('latest');
  const [globalCrawlLoading, setGlobalCrawlLoading] = useState(false);
  const [globalCrawlTaskId, setGlobalCrawlTaskId] = useState<string | null>(null);
  const [globalCrawlTaskStatus, setGlobalCrawlTaskStatus] = useState<string>('idle');

  // Global File State
  const [globalFileCollectLoading, setGlobalFileCollectLoading] = useState(false);
  const [globalFileDownloadLoading, setGlobalFileDownloadLoading] = useState(false);
  const [globalFileCollectTaskId, setGlobalFileCollectTaskId] = useState<string | null>(null);
  const [globalFileCollectTaskStatus, setGlobalFileCollectTaskStatus] = useState<string>('idle');
  const [globalFileDownloadTaskId, setGlobalFileDownloadTaskId] = useState<string | null>(null);
  const [globalFileDownloadTaskStatus, setGlobalFileDownloadTaskStatus] = useState<string>('idle');

  // Global Analyze State
  const [globalPerformanceLoading, setGlobalPerformanceLoading] = useState(false);
  const [globalPerformanceTaskId, setGlobalPerformanceTaskId] = useState<string | null>(null);
  const [globalPerformanceTaskStatus, setGlobalPerformanceTaskStatus] = useState<string>('idle');
  const [globalAnalyzeStartDate, setGlobalAnalyzeStartDate] = useState('');
  const [globalAnalyzeEndDate, setGlobalAnalyzeEndDate] = useState('');
  const [resetAllGroupsLoading, setResetAllGroupsLoading] = useState(false);
  const [taskSummary, setTaskSummary] = useState<TaskSummaryResponse | null>(null);

  // 爬取设置状态
  const [crawlSettingsOpen, setCrawlSettingsOpen] = useState(false);
  const [crawlInterval, setCrawlInterval] = useState(3.5);
  const [longSleepInterval, setLongSleepInterval] = useState(240);
  const [pagesPerBatch, setPagesPerBatch] = useState(15);
  const [crawlIntervalMin, setCrawlIntervalMin] = useState<number>(2);
  const [crawlIntervalMax, setCrawlIntervalMax] = useState<number>(5);
  const [longSleepIntervalMin, setLongSleepIntervalMin] = useState<number>(180);
  const [longSleepIntervalMax, setLongSleepIntervalMax] = useState<number>(300);

  // 时间区间采集参数
  const [lastDays, setLastDays] = useState<number | ''>(7);
  const [startTime, setStartTime] = useState('');
  const [endTime, setEndTime] = useState('');
  const [rangeInputMode, setRangeInputMode] = useState<'last_days' | 'time_range'>('last_days');
  const [scanFilterLoading, setScanFilterLoading] = useState(false);
  const [scanFilterSaving, setScanFilterSaving] = useState(false);
  const [scanFilterPreviewLoading, setScanFilterPreviewLoading] = useState(false);
  const [whitelistGroupIds, setWhitelistGroupIds] = useState<string[]>([]);
  const [blacklistGroupIds, setBlacklistGroupIds] = useState<string[]>([]);
  const [defaultAction, setDefaultAction] = useState<'include' | 'exclude'>('include');
  const [selectedGroupId, setSelectedGroupId] = useState<string>('');
  const [previewData, setPreviewData] = useState<ScanFilterPreviewData | null>(null);
  const [cleanupPreviewLoading, setCleanupPreviewLoading] = useState(false);
  const [cleanupRunning, setCleanupRunning] = useState(false);
  const [cleanupPreviewData, setCleanupPreviewData] = useState<BlacklistCleanupPreviewData | null>(null);
  const [cleanupTaskStatus, setCleanupTaskStatus] = useState<string>('idle');
  const [groupNameMap, setGroupNameMap] = useState<Record<string, string>>({});
  const [allGroups, setAllGroups] = useState<Array<Record<string, unknown>>>([]);
  const [marketSettingsLoading, setMarketSettingsLoading] = useState(false);
  const [marketSettingsSaving, setMarketSettingsSaving] = useState(false);
  const [marketProbing, setMarketProbing] = useState(false);
  const [marketBootstrapping, setMarketBootstrapping] = useState(false);
  const [marketBootstrapTaskId, setMarketBootstrapTaskId] = useState<string | null>(null);
  const [marketBootstrapTaskStatus, setMarketBootstrapTaskStatus] = useState<string>('idle');
  const [marketBootstrapLimit, setMarketBootstrapLimit] = useState('');
  const [providerCatalog, setProviderCatalog] = useState<Array<Record<string, unknown>>>([]);
  const [providersText, setProvidersText] = useState('tx,sina,akshare,tushare');
  const [defaultProvider, setDefaultProvider] = useState('tx');
  const [realtimeProvidersText, setRealtimeProvidersText] = useState('tushare');
  const [realtimeDefaultProvider, setRealtimeDefaultProvider] = useState('tushare');
  const [realtimeProviderFailoverEnabled, setRealtimeProviderFailoverEnabled] = useState(false);
  const [providerFailoverEnabled, setProviderFailoverEnabled] = useState(true);
  const [providerCircuitBreakerSeconds, setProviderCircuitBreakerSeconds] = useState('300');
  const [syncRetryMax, setSyncRetryMax] = useState('3');
  const [syncRetryBackoffSeconds, setSyncRetryBackoffSeconds] = useState('1');
  const [syncFailureCooldownSeconds, setSyncFailureCooldownSeconds] = useState('120');
  const [tushareTokenConfigured, setTushareTokenConfigured] = useState(false);
  const [tushareToken, setTushareToken] = useState('');
  const [probeSymbol, setProbeSymbol] = useState('000001.SZ');
  const [probeResult, setProbeResult] = useState<MarketProbeResult | null>(null);

  const isPlaceholderGroupName = (name: string, groupId: string) => {
    const n = (name || '').trim();
    if (!n) return true;
    if (n === groupId) return true;
    if (/^group\s+\d+$/i.test(n)) return true;
    return false;
  };

  const sourceGroups = allGroups.length > 0 ? allGroups : groups;

  const normalizedGroups = useMemo(() => {
    const dedup: Record<string, { group_id: string; group_name: string }> = {};
    (sourceGroups || []).forEach((g) => {
      const rawId = g.group_id ?? g.id;
      const groupId = rawId != null ? String(rawId) : '';
      const rawName = String(g.group_name ?? g.name ?? '').trim();
      const fallbackName = groupNameMap[groupId] || '';
      const groupName = isPlaceholderGroupName(rawName, groupId)
        ? (fallbackName || rawName || groupId)
        : rawName;
      if (!groupId) return;
      dedup[groupId] = {
        group_id: groupId,
        group_name: String(groupName || groupId),
      };
    });
    return Object.values(dedup);
  }, [sourceGroups, groupNameMap]);

  const selectableGroups = useMemo(
    () => normalizedGroups.filter((g) => !blacklistGroupIds.includes(g.group_id)),
    [normalizedGroups, blacklistGroupIds]
  );

  const formatGroupLabel = (groupId: string, preferredName?: string) => {
    const name = (preferredName || groupNameMap[groupId] || '').trim();
    if (!name || isPlaceholderGroupName(name, groupId)) return groupId;
    return `${name} (${groupId})`;
  };

  const loadScanFilterConfig = async () => {
    setScanFilterLoading(true);
    try {
      const res = await apiClient.getGlobalScanFilterConfig();
      setWhitelistGroupIds(Array.isArray(res?.whitelist_group_ids) ? res.whitelist_group_ids.map(String) : []);
      setBlacklistGroupIds(Array.isArray(res?.blacklist_group_ids) ? res.blacklist_group_ids.map(String) : []);
      setDefaultAction(res?.default_action === 'exclude' ? 'exclude' : 'include');
    } catch (error: unknown) {
      toast.error(`加载过滤规则失败: ${errorMessage(error)}`);
    } finally {
      setScanFilterLoading(false);
    }
  };

  const applyTaskSummary = useCallback((summary: TaskSummaryResponse) => {
    setTaskSummary(summary);
    const pickTask = (category: string) =>
      summary?.running_by_type?.[category] || summary?.latest_by_type?.[category] || null;
    const pickTaskType = (taskType: string) =>
      summary?.running_by_task_type?.[taskType] || summary?.latest_by_task_type?.[taskType] || null;

    const crawlTask = pickTask('crawl');
    setGlobalCrawlTaskId(crawlTask?.task_id || null);
    setGlobalCrawlTaskStatus(String(crawlTask?.status || 'idle'));

    const filesCollectTask = pickTaskType('global_files_collect');
    setGlobalFileCollectTaskId(filesCollectTask?.task_id || null);
    setGlobalFileCollectTaskStatus(String(filesCollectTask?.status || 'idle'));

    const filesDownloadTask = pickTaskType('global_files_download');
    setGlobalFileDownloadTaskId(filesDownloadTask?.task_id || null);
    setGlobalFileDownloadTaskStatus(String(filesDownloadTask?.status || 'idle'));

    const analyzeTask = pickTaskType('global_analyze_performance') || pickTaskType('global_analyze');
    setGlobalPerformanceTaskId(analyzeTask?.task_id || null);
    setGlobalPerformanceTaskStatus(String(analyzeTask?.status || 'idle'));

    const cleanupTask = pickTaskType('global_cleanup_blacklist');
    setCleanupTaskStatus(String(cleanupTask?.status || 'idle'));

    const marketBootstrapTask = pickTaskType('market_data_bootstrap');
    setMarketBootstrapTaskId(marketBootstrapTask?.task_id || null);
    setMarketBootstrapTaskStatus(String(marketBootstrapTask?.status || 'idle'));
  }, []);

  const refreshTaskSummary = useCallback(async () => {
    try {
      const summary = await apiClient.getTaskSummary();
      applyTaskSummary(summary);
    } catch {
      // 保持当前状态，避免短暂网络波动造成抖动
    }
  }, [applyTaskSummary]);

  useEffect(() => {
    void refreshTaskSummary();
  }, [refreshTaskSummary]);

  useEffect(() => {
    const timer = setInterval(() => {
      void refreshTaskSummary();
    }, 3000);
    return () => clearInterval(timer);
  }, [refreshTaskSummary]);

  useEffect(() => {
    void loadScanFilterConfig();
  }, []);

  useEffect(() => {
    const loadGroupNames = async () => {
      try {
        const res = await apiClient.getGroups();
        const rows = Array.isArray(res?.groups) ? res.groups : [];
        const map: Record<string, string> = {};
        rows.forEach((g: Record<string, unknown>) => {
          const gid = g.group_id != null ? String(g.group_id) : '';
          const name = g.name != null ? String(g.name).trim() : '';
          if (gid && name && !isPlaceholderGroupName(name, gid)) {
            map[gid] = name;
          }
        });
        setGroupNameMap(map);
        setAllGroups(rows);
      } catch {
        // Ignore metadata fetch failure and keep using global groups payload.
      }
    };
    void loadGroupNames();
  }, []);

  const parseProviders = (text: string): string[] => {
    const normalized = text
      .split(',')
      .map((v) => v.trim().toLowerCase())
      .filter(Boolean);
    return Array.from(new Set(normalized));
  };

  const loadMarketProviders = useCallback(async () => {
    setMarketSettingsLoading(true);
    try {
      const res = await apiClient.getMarketDataProviders();
      const providers = Array.isArray(res?.providers) ? res.providers.map((p: unknown) => String(p).toLowerCase()) : [];
      const realtimeProviders = Array.isArray(res?.realtime_providers)
        ? res.realtime_providers.map((p: unknown) => String(p).toLowerCase())
        : [];
      const joined = providers.join(',');
      const realtimeJoined = realtimeProviders.join(',');
      setProviderCatalog(Array.isArray(res?.catalog) ? res.catalog : []);
      setProvidersText(joined || 'tx,sina,akshare,tushare');
      setDefaultProvider(providers[0] || 'tx');
      setRealtimeProvidersText(realtimeJoined || 'tushare');
      setRealtimeDefaultProvider(realtimeProviders[0] || 'tushare');
      setRealtimeProviderFailoverEnabled(Boolean(res?.realtime_provider_failover_enabled ?? false));
      setProviderFailoverEnabled(Boolean(res?.provider_failover_enabled ?? true));
      setProviderCircuitBreakerSeconds(String(res?.provider_circuit_breaker_seconds ?? 300));
      setSyncRetryMax(String(res?.sync_retry_max ?? 3));
      setSyncRetryBackoffSeconds(String(res?.sync_retry_backoff_seconds ?? 1));
      setSyncFailureCooldownSeconds(String(res?.sync_failure_cooldown_seconds ?? 120));
      setTushareToken(String(res?.tushare_token ?? ''));
      setTushareTokenConfigured(Boolean(res?.tushare_token_configured));
    } catch (error: unknown) {
      toast.error(`读取行情源配置失败: ${errorMessage(error)}`);
    } finally {
      setMarketSettingsLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadMarketProviders();
  }, [loadMarketProviders]);

  const handleChangeDefaultProvider = (nextDefault: string) => {
    const providers = parseProviders(providersText);
    const reordered = [nextDefault, ...providers.filter((p) => p !== nextDefault)];
    setProvidersText(reordered.join(','));
    setDefaultProvider(nextDefault);
  };

  const handleChangeRealtimeDefaultProvider = (nextDefault: string) => {
    const providers = parseProviders(realtimeProvidersText);
    const reordered = [nextDefault, ...providers.filter((p) => p !== nextDefault)];
    setRealtimeProvidersText(reordered.join(','));
    setRealtimeDefaultProvider(nextDefault);
  };

  const handleSaveMarketProviders = async () => {
    const providers = parseProviders(providersText);
    if (providers.length === 0) {
      toast.error('请至少保留一个行情源');
      return;
    }
    if (!providers.includes(defaultProvider)) {
      providers.unshift(defaultProvider);
    }
    const reordered = [defaultProvider, ...providers.filter((p) => p !== defaultProvider)];
    const realtimeProviders = parseProviders(realtimeProvidersText);
    if (realtimeProviders.length === 0) {
      realtimeProviders.push('tushare');
    }
    if (!realtimeProviders.includes(realtimeDefaultProvider)) {
      realtimeProviders.unshift(realtimeDefaultProvider);
    }
    const realtimeReordered = [realtimeDefaultProvider, ...realtimeProviders.filter((p) => p !== realtimeDefaultProvider)];
    setMarketSettingsSaving(true);
    try {
      const nextToken = tushareToken.trim();
      const payload: {
        providers: string[];
        realtime_providers: string[];
        realtime_provider_failover_enabled: boolean;
        provider_failover_enabled: boolean;
        provider_circuit_breaker_seconds: number;
        sync_retry_max: number;
        sync_retry_backoff_seconds: number;
        sync_failure_cooldown_seconds: number;
        tushare_token?: string;
      } = {
        providers: reordered,
        realtime_providers: realtimeReordered,
        realtime_provider_failover_enabled: realtimeProviderFailoverEnabled,
        provider_failover_enabled: providerFailoverEnabled,
        provider_circuit_breaker_seconds: Number(providerCircuitBreakerSeconds) || 300,
        sync_retry_max: Number(syncRetryMax) || 3,
        sync_retry_backoff_seconds: Number(syncRetryBackoffSeconds) || 1,
        sync_failure_cooldown_seconds: Number(syncFailureCooldownSeconds) || 120,
      };
      if (nextToken) {
        payload.tushare_token = nextToken;
      }
      const res = await apiClient.updateMarketDataProviders(payload);
      const providersSaved = Array.isArray(res?.settings?.providers) ? res.settings.providers : reordered;
      const realtimeProvidersSaved = Array.isArray(res?.settings?.realtime_providers) ? res.settings.realtime_providers : realtimeReordered;
      setProvidersText(providersSaved.join(','));
      setDefaultProvider(providersSaved[0] || defaultProvider);
      setRealtimeProvidersText(realtimeProvidersSaved.join(','));
      setRealtimeDefaultProvider(realtimeProvidersSaved[0] || realtimeDefaultProvider);
      setRealtimeProviderFailoverEnabled(Boolean(res?.settings?.realtime_provider_failover_enabled ?? realtimeProviderFailoverEnabled));
      setTushareToken(String(res?.settings?.tushare_token ?? nextToken));
      const tokenConfigured = Boolean(res?.settings?.tushare_token_configured);
      setTushareTokenConfigured(tokenConfigured);
      if (!nextToken && tokenConfigured) {
        toast.success('行情源配置已保存（Tushare Token 未修改，出于安全不回显）');
      } else {
        toast.success('行情源配置已保存');
      }
    } catch (error: unknown) {
      toast.error(`保存行情源配置失败: ${errorMessage(error)}`);
    } finally {
      setMarketSettingsSaving(false);
    }
  };

  const handleProbeMarketProviders = async () => {
    const providers = parseProviders(providersText);
    setMarketProbing(true);
    try {
      const res = await apiClient.probeMarketDataProviders({
        providers,
        symbol: probeSymbol.trim() || '000001.SZ',
      });
      setProbeResult(res);
      if (res?.success) {
        toast.success('行情源探活完成');
      } else {
        toast.error('行情源探活未通过，请查看明细');
      }
    } catch (error: unknown) {
      toast.error(`行情源探活失败: ${errorMessage(error)}`);
    } finally {
      setMarketProbing(false);
    }
  };

  const handleMarketBootstrap = async () => {
    const limit = Number(marketBootstrapLimit);
    const symbolLimit = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : undefined;
    const limitText = symbolLimit ? `仅前 ${symbolLimit} 只股票` : '全市场';
    const confirmed = window.confirm(
      [
        '全历史回补是高耗时任务，可能运行很久并占用较多行情接口配额。',
        `本次范围：${limitText}；模式：断点续跑（resume=true）。`,
        '确认后将启动后台任务，并可在任务日志中查看实时进度。',
        '',
        '是否继续？',
      ].join('\n')
    );
    if (!confirmed) return;

    setMarketBootstrapping(true);
    try {
      const res = await apiClient.bootstrapMarketData({
        resume: true,
        symbol_limit: symbolLimit,
        confirm: true,
      });
      const taskId = typeof res === 'object' && res && 'task_id' in res ? String((res as { task_id?: unknown }).task_id ?? '') : '';
      if (taskId) {
        setMarketBootstrapTaskId(taskId);
        setMarketBootstrapTaskStatus('running');
      }
      onOpenLogs?.(taskId || null);
      toast.success('已启动全历史回补，请在任务日志中查看进度');
    } catch (error: unknown) {
      toast.error(`启动全历史回补失败: ${errorMessage(error)}`);
    } finally {
      setMarketBootstrapping(false);
    }
  };

  const handleStopMarketBootstrap = async () => {
    if (!marketBootstrapTaskId) return;
    try {
      await apiClient.stopTask(marketBootstrapTaskId);
      setMarketBootstrapTaskStatus('stopping');
      toast.success('已发送全历史回补停止请求');
    } catch (error: unknown) {
      toast.error(`停止全历史回补失败: ${errorMessage(error)}`);
    }
  };

  useEffect(() => {
    if (!selectedGroupId) return;
    if (blacklistGroupIds.includes(selectedGroupId)) {
      setSelectedGroupId('');
    }
  }, [selectedGroupId, blacklistGroupIds]);

  const handleGlobalCrawl = async () => {
    setGlobalCrawlLoading(true);
    try {
      const params: Record<string, unknown> = {
        mode: crawlMode,
        crawl_interval_min: crawlIntervalMin,
        crawl_interval_max: crawlIntervalMax,
        long_sleep_interval_min: longSleepIntervalMin,
        long_sleep_interval_max: longSleepIntervalMax,
        pages_per_batch: Math.max(pagesPerBatch, 5),
      };

      // 时间区间模式额外参数
      if (crawlMode === 'range') {
        if (rangeInputMode === 'time_range') {
          if (startTime) params.start_time = new Date(startTime).toISOString();
          if (endTime) params.end_time = new Date(endTime).toISOString();
        } else if (lastDays !== '' && !Number.isNaN(Number(lastDays))) {
          params.last_days = Number(lastDays);
        }

        // 防御性校验：保证最近天数与时间区间互斥
        const hasLastDays = Object.prototype.hasOwnProperty.call(params, 'last_days');
        const hasTimeRange = Object.prototype.hasOwnProperty.call(params, 'start_time') || Object.prototype.hasOwnProperty.call(params, 'end_time');
        if (hasLastDays && hasTimeRange) {
          toast.error('“最近天数”和“开始/结束时间”是二选一，请仅保留一种条件');
          return;
        }
      }

      const res = await apiClient.crawlGlobal(params as Parameters<typeof apiClient.crawlGlobal>[0]);
      const taskId = typeof res === 'object' && res && 'task_id' in res ? String((res as { task_id?: unknown }).task_id ?? '') : '';
      if (taskId) {
        setGlobalCrawlTaskId(taskId);
        setGlobalCrawlTaskStatus('running');
      }
      onOpenLogs?.(taskId || null);
      toast.success('已启动全区采集任务，请在任务日志中查看进度');
    } catch (error: unknown) {
      toast.error(`启动全区采集失败: ${errorMessage(error)}`);
    } finally {
      setGlobalCrawlLoading(false);
    }
  };

  const handleStopGlobalCrawl = async () => {
    if (!globalCrawlTaskId) return;
    try {
      await apiClient.stopTask(globalCrawlTaskId);
      setGlobalCrawlTaskStatus('stopping');
      toast.success('已发送全区采集停止请求');
    } catch (error: unknown) {
      toast.error(`停止全区采集失败: ${errorMessage(error)}`);
    }
  };

  const handleGlobalFileCollect = async () => {
    setGlobalFileCollectLoading(true);
    try {
      const res = await apiClient.collectGlobalFiles();
      const taskId = typeof res === 'object' && res && 'task_id' in res ? String((res as { task_id?: unknown }).task_id ?? '') : '';
      if (taskId) {
        setGlobalFileCollectTaskId(taskId);
        setGlobalFileCollectTaskStatus('running');
      }
      onOpenLogs?.(taskId || null);
      toast.success('已启动全区文件收集任务，请在任务日志中查看进度');
    } catch (error: unknown) {
      toast.error(`启动全区文件收集失败: ${errorMessage(error)}`);
    } finally {
      setGlobalFileCollectLoading(false);
    }
  };

  const handleStopGlobalFileCollect = async () => {
    if (!globalFileCollectTaskId) return;
    try {
      await apiClient.stopTask(globalFileCollectTaskId);
      setGlobalFileCollectTaskStatus('stopping');
      toast.success('已发送文件收集停止请求');
    } catch (error: unknown) {
      toast.error(`停止文件收集失败: ${errorMessage(error)}`);
    }
  };

  const handleGlobalFileDownload = async () => {
    setGlobalFileDownloadLoading(true);
    try {
      const res = await apiClient.downloadGlobalFiles();
      const taskId = typeof res === 'object' && res && 'task_id' in res ? String((res as { task_id?: unknown }).task_id ?? '') : '';
      if (taskId) {
        setGlobalFileDownloadTaskId(taskId);
        setGlobalFileDownloadTaskStatus('running');
      }
      onOpenLogs?.(taskId || null);
      toast.success('已启动全区文件下载任务，请在任务日志中查看进度');
    } catch (error: unknown) {
      toast.error(`启动全区文件下载失败: ${errorMessage(error)}`);
    } finally {
      setGlobalFileDownloadLoading(false);
    }
  };

  const handleStopGlobalFileDownload = async () => {
    if (!globalFileDownloadTaskId) return;
    try {
      await apiClient.stopTask(globalFileDownloadTaskId);
      setGlobalFileDownloadTaskStatus('stopping');
      toast.success('已发送文件下载停止请求');
    } catch (error: unknown) {
      toast.error(`停止文件下载失败: ${errorMessage(error)}`);
    }
  };

  const handleGlobalPerformance = async (force = false, calcWindowDays?: number) => {
    const hasStart = !!globalAnalyzeStartDate;
    const hasEnd = !!globalAnalyzeEndDate;
    if ((hasStart && !hasEnd) || (!hasStart && hasEnd)) {
      toast.error('请同时选择开始和结束日期');
      return;
    }
    if (hasStart && hasEnd && globalAnalyzeStartDate > globalAnalyzeEndDate) {
      toast.error('开始日期不能晚于结束日期');
      return;
    }

    setGlobalPerformanceLoading(true);
    try {
      const res = (hasStart && hasEnd)
        ? await apiClient.recalculateGlobalPerformanceRange(globalAnalyzeStartDate, globalAnalyzeEndDate, force)
        : await apiClient.analyzeGlobalPerformance(force, calcWindowDays);
      const taskId = typeof res === 'object' && res && 'task_id' in res ? String((res as { task_id?: unknown }).task_id ?? '') : '';
      if (taskId) {
        setGlobalPerformanceTaskId(taskId);
        setGlobalPerformanceTaskStatus('running');
      }
      onOpenLogs?.(taskId || null);
      toast.success(
        hasStart && hasEnd
          ? '已启动全区范围重算任务，请在任务日志中查看进度'
          : `已启动全区收益计算任务${calcWindowDays ? `（近${calcWindowDays}天）` : ''}，请在任务日志中查看进度`
      );
    } catch (error: unknown) {
      toast.error(`启动全区分析失败: ${errorMessage(error)}`);
    } finally {
      setGlobalPerformanceLoading(false);
    }
  };

  const handleIndependentAnalyze = async (calcWindowDays: number) => {
    setGlobalPerformanceLoading(true);
    try {
      const res = await apiClient.analyzeGlobalPerformance(false, calcWindowDays);
      const taskId = typeof res === 'object' && res && 'task_id' in res ? String((res as { task_id?: unknown }).task_id ?? '') : '';
      if (taskId) {
        setGlobalPerformanceTaskId(taskId);
        setGlobalPerformanceTaskStatus('running');
      }
      onOpenLogs?.(taskId || null);
      toast.success(`已启动全区收益计算任务（近${calcWindowDays}天），请在任务日志中查看进度`);
    } catch (error: unknown) {
      toast.error(`启动全区分析失败: ${errorMessage(error)}`);
    } finally {
      setGlobalPerformanceLoading(false);
    }
  };

  const handleStopGlobalPerformance = async () => {
    if (!globalPerformanceTaskId) return;
    try {
      await apiClient.stopTask(globalPerformanceTaskId);
      setGlobalPerformanceTaskStatus('stopping');
      toast.success('已发送收益计算停止请求');
    } catch (error: unknown) {
      toast.error(`停止收益计算失败: ${errorMessage(error)}`);
    }
  };

  const handleDeleteAllGroups = async () => {
    setResetAllGroupsLoading(true);
    try {
      const res = await apiClient.deleteAllGroups();
      const totalGroups = Number(res?.total_groups || 0);
      const deletedGroups = Number(res?.deleted_groups || 0);
      const failedGroups = Number(res?.failed_groups || 0);
      await onRefresh();
      if (failedGroups > 0) {
        toast.warning(`全量删除完成：总计 ${totalGroups}，成功 ${deletedGroups}，失败 ${failedGroups}`);
      } else {
        toast.success(`已删除全部群组本地数据：${deletedGroups}/${totalGroups}`);
      }
    } catch (error: unknown) {
      toast.error(`删除全部群组数据失败: ${errorMessage(error)}`);
    } finally {
      setResetAllGroupsLoading(false);
    }
  };

  // 处理爬取设置变更
  const handleCrawlSettingsChange = (settings: {
    crawlInterval: number;
    longSleepInterval: number;
    pagesPerBatch: number;
    crawlIntervalMin?: number;
    crawlIntervalMax?: number;
    longSleepIntervalMin?: number;
    longSleepIntervalMax?: number;
  }) => {
    setCrawlInterval(settings.crawlInterval);
    setLongSleepInterval(settings.longSleepInterval);
    setPagesPerBatch(settings.pagesPerBatch);
    setCrawlIntervalMin(settings.crawlIntervalMin || 2);
    setCrawlIntervalMax(settings.crawlIntervalMax || 5);
    setLongSleepIntervalMin(settings.longSleepIntervalMin || 180);
    setLongSleepIntervalMax(settings.longSleepIntervalMax || 300);
  };

  const addWhitelist = () => {
    if (!selectedGroupId) return;
    setWhitelistGroupIds((prev) => prev.includes(selectedGroupId) ? prev : [...prev, selectedGroupId]);
    setBlacklistGroupIds((prev) => prev.filter((id) => id !== selectedGroupId));
  };

  const addBlacklist = () => {
    if (!selectedGroupId) return;
    setBlacklistGroupIds((prev) => prev.includes(selectedGroupId) ? prev : [...prev, selectedGroupId]);
    setWhitelistGroupIds((prev) => prev.filter((id) => id !== selectedGroupId));
  };

  const saveScanFilterConfig = async () => {
    setScanFilterSaving(true);
    try {
      await apiClient.updateGlobalScanFilterConfig({
        default_action: defaultAction,
        whitelist_group_ids: whitelistGroupIds,
        blacklist_group_ids: blacklistGroupIds,
      });
      toast.success('非股票群规则已保存');
    } catch (error: unknown) {
      toast.error(`保存过滤规则失败: ${errorMessage(error)}`);
    } finally {
      setScanFilterSaving(false);
    }
  };

  const loadScanFilterPreview = async () => {
    setScanFilterPreviewLoading(true);
    try {
      const data = await apiClient.previewGlobalScanFilter();
      setPreviewData(data);
    } catch (error: unknown) {
      toast.error(`加载预览失败: ${errorMessage(error)}`);
    } finally {
      setScanFilterPreviewLoading(false);
    }
  };

  const loadBlacklistCleanupPreview = async () => {
    setCleanupPreviewLoading(true);
    try {
      const data = await apiClient.previewBlacklistCleanup();
      setCleanupPreviewData(data);
    } catch (error: unknown) {
      toast.error(`加载清理预览失败: ${errorMessage(error)}`);
    } finally {
      setCleanupPreviewLoading(false);
    }
  };

  const handleCleanupBlacklist = async () => {
    setCleanupRunning(true);
    try {
      const res = await apiClient.cleanupBlacklistData();
      const taskId = typeof res === 'object' && res && 'task_id' in res ? String((res as { task_id?: unknown }).task_id ?? '') : '';
      if (taskId) {
        setCleanupTaskStatus('running');
      }
      onOpenLogs?.(taskId || null);
      toast.success('已启动黑名单历史数据清理任务');
    } catch (error: unknown) {
      toast.error(`启动清理失败: ${errorMessage(error)}`);
    } finally {
      setCleanupRunning(false);
    }
  };

  const fallbackLastCrawl = taskSummary?.latest_by_type?.crawl?.updated_at || null;
  const fallbackLastAnalyze = taskSummary?.latest_by_type?.analyze?.updated_at || null;
  const mergedLastCrawl = scheduler?.last_crawl || fallbackLastCrawl;
  const mergedLastAnalyze = scheduler?.last_calc || fallbackLastAnalyze;

  const mergedCrawling =
    Boolean(scheduler?.is_crawling) ||
    globalCrawlTaskStatus === 'running' ||
    globalFileCollectTaskStatus === 'running' ||
    globalFileDownloadTaskStatus === 'running';
  const mergedAnalyzing =
    Boolean(scheduler?.is_calculating) ||
    globalPerformanceTaskStatus === 'running';

  const mergedCurrentGroup =
    scheduler?.current_group ||
    (mergedCrawling || mergedAnalyzing ? '全区任务执行中' : '—');

  const taskFailures = Object.values(taskSummary?.latest_by_type || {}).filter(
    (task) => task?.status === 'failed'
  ).length;
  const mergedErrorsTotal = Math.max(0, Number(scheduler?.errors_total || 0) + taskFailures);
  const hasRunningWork = mergedCrawling || mergedAnalyzing;

  return (
    <div className="space-y-4 flex flex-col">
      {/* 扫描范围规则（全局生效） */}
      <Card className="border border-gray-200 shadow-none order-last">
        <CardHeader className="pb-3 border-b border-gray-100">
          <CardTitle className="text-sm flex items-center justify-between">
            <span className="flex items-center gap-2">
              <Settings className="h-4 w-4" /> 扫描范围规则
            </span>
            <Badge variant="default">已全局生效</Badge>
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-3 pt-4">
          <div className="rounded border border-blue-200 bg-blue-50/40 p-2.5 text-xs text-blue-800">
            该规则已强制作用于「dashboard 展示」「全区轮询」「自动调度」「资源收集」「数据分析」。
          </div>

          <div className="rounded border border-dashed border-gray-200 px-2 py-1.5 text-xs text-muted-foreground">
            黑名单优先于白名单；未配置群组按“默认策略”处理。
          </div>

          <div className="rounded border border-gray-200 p-2.5 space-y-2">
            <div className="flex items-center justify-between">
              <div className="text-xs font-medium">非股票群规则（仅手动白黑名单）</div>
              <Button size="sm" variant="outline" className="h-6 text-[11px]" onClick={loadScanFilterConfig} disabled={scanFilterLoading}>
                {scanFilterLoading ? '加载中' : '刷新'}
              </Button>
            </div>

            <div className="space-y-1">
              <Label className="text-[11px] text-muted-foreground">未配置群组默认策略</Label>
              <Select value={defaultAction} onValueChange={(v) => setDefaultAction(v as 'include' | 'exclude')}>
                <SelectTrigger className="h-7 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="include" className="text-xs">默认纳入扫描（推荐）</SelectItem>
                  <SelectItem value="exclude" className="text-xs">默认排除，仅白名单纳入</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-1">
              <Label className="text-[11px] text-muted-foreground">从群组概览选择</Label>
              <Select value={selectedGroupId} onValueChange={setSelectedGroupId}>
                <SelectTrigger className="h-7 text-xs">
                  <SelectValue placeholder="选择群组" />
                </SelectTrigger>
                <SelectContent>
                  {selectableGroups.map((g) => (
                    <SelectItem key={g.group_id} value={g.group_id} className="text-xs">
                      {formatGroupLabel(g.group_id, g.group_name)}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <div className="grid grid-cols-2 gap-2">
                <Button size="sm" className="h-7 text-xs" onClick={addWhitelist} disabled={!selectedGroupId}>加入白名单</Button>
                <Button size="sm" variant="destructive" className="h-7 text-xs" onClick={addBlacklist} disabled={!selectedGroupId}>加入黑名单</Button>
              </div>
            </div>

            <div>
              <div className="text-[11px] text-muted-foreground mb-1">白名单（保留扫描）</div>
              <div className="flex flex-wrap gap-1">
                {whitelistGroupIds.length === 0 && <span className="text-[11px] text-muted-foreground">空</span>}
                {whitelistGroupIds.map((id) => (
                  <Badge key={`w-${id}`} variant="secondary" className="text-[10px]">
                    {formatGroupLabel(id)}
                    <button className="ml-1" onClick={() => setWhitelistGroupIds((prev) => prev.filter((x) => x !== id))}>×</button>
                  </Badge>
                ))}
              </div>
            </div>

            <div>
              <div className="text-[11px] text-muted-foreground mb-1">黑名单（强制排除）</div>
              <div className="flex flex-wrap gap-1">
                {blacklistGroupIds.length === 0 && <span className="text-[11px] text-muted-foreground">空</span>}
                {blacklistGroupIds.map((id) => (
                  <Badge key={`b-${id}`} variant="destructive" className="text-[10px]">
                    {formatGroupLabel(id)}
                    <button className="ml-1" onClick={() => setBlacklistGroupIds((prev) => prev.filter((x) => x !== id))}>×</button>
                  </Badge>
                ))}
              </div>
            </div>

            <div className="grid grid-cols-2 gap-2">
              <Button size="sm" variant="outline" className="h-7 text-xs" onClick={loadScanFilterPreview} disabled={scanFilterPreviewLoading}>
                {scanFilterPreviewLoading ? '预览中' : '预览结果'}
              </Button>
              <Button size="sm" className="h-7 text-xs" onClick={saveScanFilterConfig} disabled={scanFilterSaving}>
                {scanFilterSaving ? '保存中' : '保存规则'}
              </Button>
            </div>

            {previewData && (
              <div className="rounded border border-dashed border-gray-200 p-2 text-[11px] text-muted-foreground space-y-1">
                <div>总群组: {previewData.total_groups ?? 0}</div>
                <div>将扫描: {(previewData.included_groups || []).length}</div>
                <div>将排除: {(previewData.excluded_groups || []).length}</div>
                {!!previewData.reason_counts && (
                  <div>命中原因: {Object.entries(previewData.reason_counts).map(([k, v]) => `${k}:${v}`).join(' / ')}</div>
                )}
              </div>
            )}

            <div className="rounded border border-orange-200 bg-orange-50/30 p-2 text-[11px] space-y-2">
              <div className="font-medium text-orange-800">黑名单历史数据清理（仅分析表）</div>
              <div className="text-muted-foreground">
                仅清理黑名单群组的 `stock_mentions` 与 `mention_performance`，不会删除原始话题/文件。
              </div>
              <div className="grid grid-cols-2 gap-2">
                <Button
                  size="sm"
                  variant="outline"
                  className="h-7 text-xs"
                  onClick={loadBlacklistCleanupPreview}
                  disabled={cleanupPreviewLoading}
                >
                  {cleanupPreviewLoading ? '加载中' : '清理预览'}
                </Button>
                <AlertDialog>
                  <AlertDialogTrigger asChild>
                    <Button
                      size="sm"
                      variant="destructive"
                      className="h-7 text-xs"
                      disabled={cleanupRunning || cleanupTaskStatus === 'running'}
                    >
                      {cleanupRunning ? '启动中' : '执行清理'}
                    </Button>
                  </AlertDialogTrigger>
                  <AlertDialogContent>
                    <AlertDialogHeader>
                      <AlertDialogTitle>确认执行黑名单历史数据清理</AlertDialogTitle>
                      <AlertDialogDescription>
                        将删除黑名单群组中的股票提及与收益分析记录。该操作不可撤销，确认继续？
                      </AlertDialogDescription>
                    </AlertDialogHeader>
                    <AlertDialogFooter>
                      <AlertDialogCancel>取消</AlertDialogCancel>
                      <AlertDialogAction onClick={handleCleanupBlacklist} className="bg-red-600 hover:bg-red-700">
                        确认清理
                      </AlertDialogAction>
                    </AlertDialogFooter>
                  </AlertDialogContent>
                </AlertDialog>
              </div>
              {cleanupPreviewData && (
                <div className="rounded border border-dashed border-gray-200 p-2 text-[11px] text-muted-foreground space-y-1">
                  <div>黑名单群组: {cleanupPreviewData.blacklist_group_count}</div>
                  <div>本地匹配群组: {cleanupPreviewData.matched_group_count}</div>
                  <div>可删提及记录: {cleanupPreviewData.total_stock_mentions}</div>
                  <div>可删收益记录: {cleanupPreviewData.total_mention_performance}</div>
                </div>
              )}
            </div>
          </div>
        </CardContent>
      </Card>

      {/* 全区操作面板 - 顶部优先 */}
      <Card className="border border-gray-200 shadow-none overflow-hidden">
        <CardHeader className="pb-0 pt-3 px-3 border-b border-gray-100 bg-gray-50/50">
          <CardTitle className="text-sm flex items-center gap-2 pb-3">
            <Globe className="h-4 w-4 text-primary" /> 全区轮询操作
          </CardTitle>
          <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
            <TabsList className="grid w-full grid-cols-3 h-8 rounded-lg bg-muted/40 p-1 gap-1">
              <TabsTrigger
                value="crawl"
                className="text-xs rounded-md data-[state=active]:bg-background data-[state=active]:shadow-sm focus:outline-none focus-visible:outline-none focus-visible:ring-0"
              >
                话题采集
              </TabsTrigger>
              <TabsTrigger
                value="file"
                className="text-xs rounded-md data-[state=active]:bg-background data-[state=active]:shadow-sm focus:outline-none focus-visible:outline-none focus-visible:ring-0"
              >
                资源收集
              </TabsTrigger>
              <TabsTrigger
                value="analyze"
                className="text-xs rounded-md data-[state=active]:bg-background data-[state=active]:shadow-sm focus:outline-none focus-visible:outline-none focus-visible:ring-0"
              >
                数据分析
              </TabsTrigger>
            </TabsList>
          </Tabs>
        </CardHeader>
        <CardContent className="p-4 pt-4">
          <Tabs value={activeTab} className="w-full">
            {/* 话题采集 */}
            <TabsContent value="crawl" className="mt-0 space-y-4">
              <div className="flex items-center justify-between rounded-lg border border-blue-200 bg-blue-50/40 px-2.5 py-2 text-xs">
                <span className="text-muted-foreground">运行状态</span>
                <Badge variant={globalCrawlTaskStatus === 'running' ? 'default' : 'secondary'}>
                  {formatTaskStatus(globalCrawlTaskStatus)}
                </Badge>
              </div>
              <div className="flex items-center justify-between rounded-lg border border-gray-200 p-2.5">
                <label className="text-xs font-medium text-muted-foreground">采集模式</label>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setCrawlSettingsOpen(true)}
                  className="h-6 text-[11px] gap-1"
                >
                  <Settings className="h-3 w-3" />
                  爬取设置
                </Button>
              </div>
              <div className="space-y-2">
                <Select value={crawlMode} onValueChange={(v: 'latest' | 'all' | 'incremental' | 'range') => setCrawlMode(v)}>
                  <SelectTrigger className="h-8 text-xs">
                    <SelectValue placeholder="选择模式" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="latest" className="text-xs">最新话题 (推荐)</SelectItem>
                    <SelectItem value="incremental" className="text-xs">增量更新</SelectItem>
                    <SelectItem value="range" className="text-xs">时间区间</SelectItem>
                    <SelectItem value="all" className="text-xs text-orange-600">全量历史 (耗时长)</SelectItem>
                  </SelectContent>
                </Select>
                <div className="text-[10px] text-muted-foreground">
                  ⏱️ 间隔: {crawlIntervalMin}-{crawlIntervalMax}秒 · 长休眠: {Math.round(longSleepIntervalMin / 60)}-{Math.round(longSleepIntervalMax / 60)}分 · {pagesPerBatch}页/批
                </div>
              </div>

              {crawlMode === 'range' && (
                <div className="space-y-3 rounded-lg border border-dashed border-blue-200 bg-blue-50/20 p-3">
                  <div className="space-y-1.5">
                    <Label className="text-xs">时间条件（必选其一）</Label>
                    <Select
                      value={rangeInputMode}
                      onValueChange={(v: 'last_days' | 'time_range') => {
                        setRangeInputMode(v);
                        if (v === 'last_days') {
                          setStartTime('');
                          setEndTime('');
                        } else {
                          setLastDays('');
                        }
                      }}
                    >
                      <SelectTrigger className="h-7 text-xs">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="last_days" className="text-xs">最近天数</SelectItem>
                        <SelectItem value="time_range" className="text-xs">开始/结束时间</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-1.5">
                    <Label className="text-xs">最近天数（可选）</Label>
                    <Input
                      type="number"
                      min="1"
                      max="3650"
                      placeholder="7"
                      value={lastDays}
                      disabled={rangeInputMode !== 'last_days'}
                      onChange={(e) => {
                        const v = e.target.value;
                        if (v === '') setLastDays('');
                        else {
                          const n = parseInt(v);
                          if (!Number.isNaN(n)) {
                            setLastDays(n);
                            setRangeInputMode('last_days');
                            setStartTime('');
                            setEndTime('');
                          }
                        }
                      }}
                      className="h-7 text-xs"
                    />
                  </div>
                  <div className="grid grid-cols-1 gap-2">
                    <div className="space-y-1">
                      <Label className="text-xs">开始时间（可选）</Label>
                      <Input
                        type="datetime-local"
                        value={startTime}
                        disabled={rangeInputMode !== 'time_range'}
                        onChange={(e) => {
                          setStartTime(e.target.value);
                          setRangeInputMode('time_range');
                          setLastDays('');
                        }}
                        className="h-7 text-xs"
                      />
                    </div>
                    <div className="space-y-1">
                      <Label className="text-xs">结束时间（可选）</Label>
                      <Input
                        type="datetime-local"
                        value={endTime}
                        disabled={rangeInputMode !== 'time_range'}
                        onChange={(e) => {
                          setEndTime(e.target.value);
                          setRangeInputMode('time_range');
                          setLastDays('');
                        }}
                        className="h-7 text-xs"
                      />
                    </div>
                  </div>
                  <p className="text-[10px] text-muted-foreground">
                    规则：最近天数 与 开始/结束时间 为二选一。时间区间可只填开始或结束，缺失的一端由系统自动补齐。
                  </p>
                </div>
              )}

              {crawlMode === 'all' ? (
                <AlertDialog>
                  <AlertDialogTrigger asChild>
                    <Button size="sm" className="w-full h-8 text-xs bg-orange-600 hover:bg-orange-700" disabled={globalCrawlLoading || globalCrawlTaskStatus === 'running'}>
                      <Cloud className="h-3.5 w-3.5 mr-1" /> 开始全区采集
                    </Button>
                  </AlertDialogTrigger>
                  <AlertDialogContent>
                    <AlertDialogHeader>
                      <AlertDialogTitle>确认全区全量采集</AlertDialogTitle>
                      <AlertDialogDescription>
                        ⚠️ 全量采集将持续请求各群组的全部历史数据。<br /><br />
                        这可能会消耗较多时间并可能触发平台风控，建议仅在初次部署时使用。<br />
                        确定要继续吗？
                      </AlertDialogDescription>
                    </AlertDialogHeader>
                    <AlertDialogFooter>
                      <AlertDialogCancel>取消</AlertDialogCancel>
                      <AlertDialogAction onClick={handleGlobalCrawl} className="bg-orange-600 hover:bg-orange-700">确认执行</AlertDialogAction>
                    </AlertDialogFooter>
                  </AlertDialogContent>
                </AlertDialog>
              ) : (
                <Button size="sm" className="w-full h-8 text-xs bg-blue-600 hover:bg-blue-700" onClick={handleGlobalCrawl} disabled={globalCrawlLoading || globalCrawlTaskStatus === 'running'}>
                  {globalCrawlLoading ? <Loader2 className="h-3.5 w-3.5 animate-spin mr-1" /> : <Cloud className="h-3.5 w-3.5 mr-1" />}
                  开始全区采集
                </Button>
              )}
              <Button
                size="sm"
                variant="destructive"
                className="w-full h-8 text-xs"
                onClick={handleStopGlobalCrawl}
                disabled={!globalCrawlTaskId || globalCrawlTaskStatus !== 'running'}
              >
                <Square className="h-3.5 w-3.5 mr-1" /> 停止全区采集
              </Button>
            </TabsContent>

            {/* 资源收集 */}
            <TabsContent value="file" className="mt-0 space-y-4">
              <div className="space-y-2 rounded-lg border border-purple-200 bg-purple-50/30 p-2.5">
                <div className="flex items-center justify-between text-xs">
                  <span className="text-muted-foreground">文件列表收集状态</span>
                  <Badge variant={globalFileCollectTaskStatus === 'running' ? 'default' : 'secondary'}>
                    {formatTaskStatus(globalFileCollectTaskStatus)}
                  </Badge>
                </div>
                <div className="grid grid-cols-2 gap-2">
                  <Button size="sm" variant="outline" className="h-8 text-xs" onClick={handleGlobalFileCollect} disabled={globalFileCollectLoading || globalFileCollectTaskStatus === 'running'}>
                    {globalFileCollectLoading ? <Loader2 className="h-3.5 w-3.5 animate-spin mr-1" /> : <Database className="h-3.5 w-3.5 mr-1 text-blue-600" />}
                    开始
                  </Button>
                  <Button size="sm" variant="destructive" className="h-8 text-xs" onClick={handleStopGlobalFileCollect} disabled={!globalFileCollectTaskId || globalFileCollectTaskStatus !== 'running'}>
                    <Square className="h-3.5 w-3.5 mr-1" />
                    停止
                  </Button>
                </div>
                <div className="text-[10px] text-muted-foreground">
                  获取所有群组最新文件列表，不下载实体文件。
                </div>
              </div>

              <div className="space-y-2 rounded-lg border border-emerald-200 bg-emerald-50/30 p-2.5">
                <div className="flex items-center justify-between text-xs">
                  <span className="text-muted-foreground">文件下载状态</span>
                  <Badge variant={globalFileDownloadTaskStatus === 'running' ? 'default' : 'secondary'}>
                    {formatTaskStatus(globalFileDownloadTaskStatus)}
                  </Badge>
                </div>
                <div className="grid grid-cols-2 gap-2">
                  <Button size="sm" variant="outline" className="h-8 text-xs" onClick={handleGlobalFileDownload} disabled={globalFileDownloadLoading || globalFileDownloadTaskStatus === 'running'}>
                    {globalFileDownloadLoading ? <Loader2 className="h-3.5 w-3.5 animate-spin mr-1" /> : <Download className="h-3.5 w-3.5 mr-1 text-green-600" />}
                    开始
                  </Button>
                  <Button size="sm" variant="destructive" className="h-8 text-xs" onClick={handleStopGlobalFileDownload} disabled={!globalFileDownloadTaskId || globalFileDownloadTaskStatus !== 'running'}>
                    <Square className="h-3.5 w-3.5 mr-1" />
                    停止
                  </Button>
                </div>
                <div className="text-[10px] text-muted-foreground">
                  下载所有群组中被标记为待下载的文件。
                </div>
              </div>
            </TabsContent>

            {/* 数据分析 */}
            <TabsContent value="analyze" className="mt-0 space-y-4">
              <div className="space-y-2 rounded-lg border border-cyan-200 bg-cyan-50/30 p-2.5">
                <div className="flex items-center justify-between text-xs">
                  <span className="text-muted-foreground">行情源配置与探活</span>
                  <Badge variant="secondary">
                    默认源: {defaultProvider || 'tx'}
                  </Badge>
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">行情源优先级（逗号分隔，左到右）</Label>
                  <Input
                    className="h-8 text-xs"
                    value={providersText}
                    onChange={(e) => setProvidersText(e.target.value)}
                    placeholder="tx,sina,akshare,tushare"
                    disabled={marketSettingsLoading}
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">实时价优先级（建议 tushare 放首位）</Label>
                  <Input
                    className="h-8 text-xs"
                    value={realtimeProvidersText}
                    onChange={(e) => setRealtimeProvidersText(e.target.value)}
                    placeholder="tushare,tx,sina,akshare"
                    disabled={marketSettingsLoading}
                  />
                </div>
                <div className="grid grid-cols-2 gap-2">
                  <div className="space-y-1">
                    <Label className="text-xs">默认源</Label>
                    <Select value={defaultProvider} onValueChange={handleChangeDefaultProvider}>
                      <SelectTrigger className="h-8 text-xs">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {parseProviders(providersText).map((p) => (
                          <SelectItem key={p} value={p} className="text-xs">
                            {String((providerCatalog.find((x) => x.name === p)?.label) || p)}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-1">
                    <Label className="text-xs">故障切换</Label>
                    <Select
                      value={providerFailoverEnabled ? 'on' : 'off'}
                      onValueChange={(v) => setProviderFailoverEnabled(v === 'on')}
                    >
                      <SelectTrigger className="h-8 text-xs">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="on" className="text-xs">开启</SelectItem>
                        <SelectItem value="off" className="text-xs">关闭</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-2">
                  <div className="space-y-1">
                    <Label className="text-xs">实时默认源</Label>
                    <Select value={realtimeDefaultProvider} onValueChange={handleChangeRealtimeDefaultProvider}>
                      <SelectTrigger className="h-8 text-xs">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {parseProviders(realtimeProvidersText).map((p) => (
                          <SelectItem key={`rt-${p}`} value={p} className="text-xs">
                            {String((providerCatalog.find((x) => x.name === p)?.label) || p)}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-1">
                    <Label className="text-xs">实时价回退</Label>
                    <Select
                      value={realtimeProviderFailoverEnabled ? 'on' : 'off'}
                      onValueChange={(v) => setRealtimeProviderFailoverEnabled(v === 'on')}
                    >
                      <SelectTrigger className="h-8 text-xs">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="off" className="text-xs">关闭（仅首源）</SelectItem>
                        <SelectItem value="on" className="text-xs">开启（按顺序回退）</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                </div>
                <div className="grid grid-cols-3 gap-2">
                  <div className="space-y-1">
                    <Label className="text-xs">熔断(秒)</Label>
                    <Input className="h-8 text-xs" value={providerCircuitBreakerSeconds} onChange={(e) => setProviderCircuitBreakerSeconds(e.target.value)} />
                  </div>
                  <div className="space-y-1">
                    <Label className="text-xs">重试次数</Label>
                    <Input className="h-8 text-xs" value={syncRetryMax} onChange={(e) => setSyncRetryMax(e.target.value)} />
                  </div>
                  <div className="space-y-1">
                    <Label className="text-xs">失败冷却(秒)</Label>
                    <Input className="h-8 text-xs" value={syncFailureCooldownSeconds} onChange={(e) => setSyncFailureCooldownSeconds(e.target.value)} />
                  </div>
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">重试退避(秒)</Label>
                  <Input className="h-8 text-xs" value={syncRetryBackoffSeconds} onChange={(e) => setSyncRetryBackoffSeconds(e.target.value)} />
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">Tushare Token（可选）</Label>
                  <Input
                    className="h-8 text-xs"
                    type="text"
                    value={tushareToken}
                    onChange={(e) => setTushareToken(e.target.value)}
                    placeholder={tushareTokenConfigured ? '已配置，可直接查看和编辑' : '未配置'}
                  />
                  <div className="text-[10px] text-muted-foreground">
                    当前状态: {tushareTokenConfigured ? '已配置（输入框显示当前值；留空保存将保持不变）' : '未配置'}
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-2">
                  <Button size="sm" variant="outline" className="h-8 text-xs" onClick={handleSaveMarketProviders} disabled={marketSettingsSaving || marketSettingsLoading}>
                    {marketSettingsSaving ? <Loader2 className="h-3.5 w-3.5 animate-spin mr-1" /> : <Settings className="h-3.5 w-3.5 mr-1 text-cyan-600" />}
                    保存配置
                  </Button>
                  <Button size="sm" variant="outline" className="h-8 text-xs" onClick={loadMarketProviders} disabled={marketSettingsLoading}>
                    {marketSettingsLoading ? <Loader2 className="h-3.5 w-3.5 animate-spin mr-1" /> : <RefreshCw className="h-3.5 w-3.5 mr-1" />}
                    刷新配置
                  </Button>
                </div>
                <div className="grid grid-cols-[1fr_auto] gap-2">
                  <Input
                    className="h-8 text-xs"
                    value={probeSymbol}
                    onChange={(e) => setProbeSymbol(e.target.value)}
                    placeholder="探活股票代码，例如 000001.SZ"
                  />
                  <Button size="sm" className="h-8 text-xs px-3" onClick={handleProbeMarketProviders} disabled={marketProbing}>
                    {marketProbing ? <Loader2 className="h-3.5 w-3.5 animate-spin mr-1" /> : <Activity className="h-3.5 w-3.5 mr-1" />}
                    探活
                  </Button>
                </div>
                {probeResult?.details && (
                  <div className="rounded border border-dashed border-cyan-200 p-2 text-[11px] space-y-1">
                    {probeResult.details.map((d: MarketProbeDetail) => (
                      <div key={String(d.provider)} className="flex items-center justify-between gap-2">
                        <span>{d.provider}</span>
                        <span className={d.ok ? 'text-green-600' : 'text-red-600'}>
                          {d.ok
                            ? `OK ${d.latency_ms}ms`
                            : `FAIL ${d.error || d.disabled_reason || (d.routable === false ? 'not_routable' : 'unknown')}`}
                        </span>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {/* ─── 收益分析（统一模块） ─── */}
              <div className="space-y-2.5 rounded-lg border border-blue-200 bg-blue-50/30 p-2.5">
                <div className="flex items-center justify-between text-xs">
                  <span className="font-medium text-foreground flex items-center gap-1.5">
                    <BarChart3 className="h-3.5 w-3.5 text-blue-600" />
                    收益分析
                  </span>
                  <Badge
                    variant={globalPerformanceTaskStatus === 'running' ? 'default' : 'secondary'}
                    className={globalPerformanceTaskStatus === 'running' ? 'animate-pulse' : ''}
                  >
                    {formatTaskStatus(globalPerformanceTaskStatus)}
                  </Badge>
                </div>
                <div className="grid grid-cols-2 gap-2">
                  <Button
                    size="sm"
                    variant="outline"
                    className="h-9 text-xs"
                    onClick={() => handleIndependentAnalyze(3)}
                    disabled={globalPerformanceLoading || globalPerformanceTaskStatus === 'running'}
                  >
                    {globalPerformanceLoading ? <Loader2 className="h-3.5 w-3.5 animate-spin mr-1 shrink-0" /> : <Zap className="h-3.5 w-3.5 mr-1 text-amber-600 shrink-0" />}
                    快速分析（近3天）
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    className="h-9 text-xs"
                    onClick={() => handleIndependentAnalyze(365)}
                    disabled={globalPerformanceLoading || globalPerformanceTaskStatus === 'running'}
                  >
                    {globalPerformanceLoading ? <Loader2 className="h-3.5 w-3.5 animate-spin mr-1 shrink-0" /> : <BarChart3 className="h-3.5 w-3.5 mr-1 text-blue-600 shrink-0" />}
                    全量补全（近365天）
                  </Button>
                </div>
                {globalPerformanceTaskStatus === 'running' && (
                  <Button size="sm" variant="destructive" className="h-8 text-xs w-full" onClick={handleStopGlobalPerformance} disabled={!globalPerformanceTaskId}>
                    <Square className="h-3.5 w-3.5 mr-1 shrink-0" />
                    停止当前分析任务
                  </Button>
                )}
                <div className="text-[10px] text-muted-foreground">
                  增量收益计算：仅计算新增和未冻结的提及记录。
                </div>

                {/* 高级选项折叠 */}
                <button
                  type="button"
                  className="flex items-center gap-1 text-[11px] text-muted-foreground hover:text-foreground transition-colors w-full"
                  onClick={() => setAdvancedAnalyzeOpen((prev) => !prev)}
                >
                  <ChevronDown className={`h-3.5 w-3.5 transition-transform ${advancedAnalyzeOpen ? 'rotate-180' : ''}`} />
                  高级选项（日期范围重算 / 强制重算）
                </button>
                {advancedAnalyzeOpen && (
                  <div className="space-y-2 rounded border border-blue-200 bg-white/60 p-2">
                    <div className="text-[10px] text-muted-foreground">日期范围（可选，留空则全量重算）</div>
                    <div className="grid grid-cols-[1fr_auto_1fr] items-center gap-2">
                      <Input
                        type="date"
                        value={globalAnalyzeStartDate}
                        onChange={(e) => setGlobalAnalyzeStartDate(e.target.value)}
                        className="h-8 text-xs"
                        disabled={globalPerformanceLoading || globalPerformanceTaskStatus === 'running'}
                      />
                      <span className="text-[10px] text-muted-foreground">~</span>
                      <Input
                        type="date"
                        value={globalAnalyzeEndDate}
                        onChange={(e) => setGlobalAnalyzeEndDate(e.target.value)}
                        className="h-8 text-xs"
                        disabled={globalPerformanceLoading || globalPerformanceTaskStatus === 'running'}
                      />
                    </div>
                    <div className="grid grid-cols-2 gap-2">
                      <Button size="sm" variant="outline" className="h-8 text-xs" onClick={() => handleGlobalPerformance(true)} disabled={globalPerformanceLoading || globalPerformanceTaskStatus === 'running'}>
                        <Zap className="h-3.5 w-3.5 mr-1 text-amber-600 shrink-0" />
                        强制重算
                      </Button>
                      <Button size="sm" variant="destructive" className="h-8 text-xs" onClick={handleStopGlobalPerformance} disabled={!globalPerformanceTaskId || globalPerformanceTaskStatus !== 'running'}>
                        <Square className="h-3.5 w-3.5 mr-1 shrink-0" />
                        停止
                      </Button>
                    </div>
                    <div className="text-[10px] text-muted-foreground">
                      用于异常恢复：留空日期为全量重算；填写日期为按范围重算，可显著缩短执行时间。
                    </div>
                  </div>
                )}
              </div>

              {/* ─── 系统维护工具 ─── */}
              <div className="space-y-2 rounded-lg border border-gray-200 bg-gray-50/40 p-2.5">
                <div className="text-xs font-medium text-muted-foreground">系统维护工具</div>
                <Button size="sm" variant="outline" className="w-full h-8 text-xs justify-start px-3" onClick={onRefreshHotWords} disabled={hotWordsLoading}>
                  {hotWordsLoading ? <Loader2 className="h-3.5 w-3.5 animate-spin mr-2" /> : <RefreshCw className="h-3.5 w-3.5 mr-2 text-indigo-500" />}
                  强制刷新热度词云
                </Button>
                <Button size="sm" variant="outline" className="w-full h-8 text-xs justify-start px-3" onClick={onScanGlobal} disabled={loading.scan}>
                  {loading.scan ? <Loader2 className="h-3.5 w-3.5 animate-spin mr-2" /> : <Zap className="h-3.5 w-3.5 mr-2 text-amber-500" />}
                  手动修正群组元数据
                </Button>
                <AlertDialog>
                  <AlertDialogTrigger asChild>
                    <Button
                      size="sm"
                      variant="destructive"
                      className="h-9 text-xs w-full"
                      disabled={resetAllGroupsLoading}
                    >
                      {resetAllGroupsLoading ? <Loader2 className="h-3.5 w-3.5 animate-spin mr-1 shrink-0" /> : null}
                      删除全部群组数据（重置）
                    </Button>
                  </AlertDialogTrigger>
                  <AlertDialogContent>
                    <AlertDialogHeader>
                      <AlertDialogTitle>确认删除全部群组本地数据</AlertDialogTitle>
                      <AlertDialogDescription>
                        将删除所有本地群组的 topics 数据库、files 数据库、下载目录和图片缓存。该操作不可撤销，确认继续？
                      </AlertDialogDescription>
                    </AlertDialogHeader>
                    <AlertDialogFooter>
                      <AlertDialogCancel>取消</AlertDialogCancel>
                      <AlertDialogAction onClick={handleDeleteAllGroups} className="bg-red-600 hover:bg-red-700">
                        确认删除
                      </AlertDialogAction>
                    </AlertDialogFooter>
                  </AlertDialogContent>
                </AlertDialog>
                <div className="mt-1 grid grid-cols-[1fr_auto_auto] gap-2">
                  <Input
                    className="h-8 text-xs"
                    value={marketBootstrapLimit}
                    onChange={(e) => setMarketBootstrapLimit(e.target.value)}
                    placeholder="全历史回补数量上限（可选）"
                    disabled={marketBootstrapping || marketBootstrapTaskStatus === 'running' || marketBootstrapTaskStatus === 'pending' || marketBootstrapTaskStatus === 'stopping'}
                  />
                  <Button
                    size="sm"
                    variant="outline"
                    className="h-8 text-xs px-3 whitespace-nowrap"
                    onClick={handleMarketBootstrap}
                    disabled={marketBootstrapping || marketBootstrapTaskStatus === 'running' || marketBootstrapTaskStatus === 'pending' || marketBootstrapTaskStatus === 'stopping'}
                  >
                    {marketBootstrapping ? <Loader2 className="h-3.5 w-3.5 animate-spin mr-1" /> : <Database className="h-3.5 w-3.5 mr-1 text-indigo-600" />}
                    全历史回补
                  </Button>
                  <Button
                    size="sm"
                    variant="destructive"
                    className="h-8 text-xs px-3 whitespace-nowrap"
                    onClick={handleStopMarketBootstrap}
                    disabled={!marketBootstrapTaskId || marketBootstrapTaskStatus !== 'running'}
                  >
                    <Square className="h-3.5 w-3.5 mr-1 shrink-0" />
                    停止
                  </Button>
                </div>
                <div className="text-[10px] text-muted-foreground">
                  全历史回补默认断点续跑（resume=true）；留空表示全市场，填写数字表示仅前N只股票。启动后请在任务日志查看分段进度。
                </div>
                <div className="text-[10px] text-muted-foreground">
                  任务状态：{formatTaskStatus(marketBootstrapTaskStatus)}{marketBootstrapTaskId ? `（${marketBootstrapTaskId}）` : ''}
                </div>
              </div>

            </TabsContent>
          </Tabs>
        </CardContent>
      </Card>

      {/* 行程/调度 面板 */}
      <Card className="border border-gray-200 shadow-none">
        <CardHeader className="pb-3 border-b border-gray-100">
          <CardTitle className="text-sm flex items-center justify-between">
            <span className="flex items-center gap-2">
              <Activity className="h-4 w-4" /> 自动调度系统
            </span>
            <Badge
              variant={scheduler?.state === 'running' || hasRunningWork ? 'default' : 'secondary'}
              className={scheduler?.state === 'running' || hasRunningWork ? 'animate-pulse' : ''}
            >
              {(scheduler?.state === 'running' || hasRunningWork)
                ? (mergedCrawling
                  ? '运行中-采集'
                  : (mergedAnalyzing ? '运行中-分析' : '运行中-空闲'))
                : '已停止'}
            </Badge>
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-3 pt-4">
          <div className="rounded border border-gray-200 p-2.5 space-y-2">
            <div className="text-xs font-medium">自动触发</div>
            <div className="text-xs text-muted-foreground space-y-1">
              <div>当前群组: <span className="text-foreground">{mergedCurrentGroup}</span></div>
              <div>采集状态: <span className="text-foreground">{mergedCrawling ? '采集中' : '空闲'}</span></div>
              <div>分析状态: <span className="text-foreground">{mergedAnalyzing ? '分析中' : '空闲'}</span></div>
              <div>错误计数: <span className={mergedErrorsTotal ? 'text-destructive font-medium' : 'text-foreground'}>{mergedErrorsTotal}</span></div>
              <div>最近采集: <span className="text-foreground">{formatTime(mergedLastCrawl)}</span></div>
              <div>最近分析: <span className="text-foreground">{formatTime(mergedLastAnalyze)}</span></div>
            </div>
            <div className="grid grid-cols-2 gap-2 mt-2">
              <Button size="sm" variant="outline" onClick={onRefresh} disabled={loading.refreshing}>
                {loading.refreshing ? <Loader2 className="h-3.5 w-3.5 animate-spin mr-1" /> : <RefreshCw className="h-3.5 w-3.5 mr-1" />}
                刷新状态
              </Button>
              <Button
                size="sm"
                variant={scheduler?.state === 'running' ? 'destructive' : 'default'}
                onClick={onToggleScheduler}
                disabled={loading.scheduler}
              >
                {loading.scheduler ? <Loader2 className="h-3.5 w-3.5 animate-spin mr-1" /> : scheduler?.state === 'running' ? <Square className="h-3.5 w-3.5 mr-1" /> : <Play className="h-3.5 w-3.5 mr-1" />}
                {scheduler?.state === 'running' ? '停止调度' : '启动调度'}
              </Button>
            </div>
          </div>

          <div className="rounded border border-gray-200 bg-muted/20 p-2.5 space-y-2">
            <div className="text-xs font-medium">执行说明</div>
            <div className="text-[11px] text-muted-foreground">定时触发：非节假日开盘时段每半小时，其余时段每 3 小时（00:00 起）</div>
            <div className="text-[11px] text-muted-foreground">每次触发均执行：增量采集 + 分析</div>
            <Button
              size="sm"
              variant="outline"
              className="h-7 text-[11px]"
              onClick={() => onOpenLogs?.('scheduler')}
            >
              查看日志
            </Button>
          </div>
        </CardContent>
      </Card>

      <CrawlSettingsDialog
        open={crawlSettingsOpen}
        onOpenChange={setCrawlSettingsOpen}
        crawlInterval={crawlInterval}
        longSleepInterval={longSleepInterval}
        pagesPerBatch={pagesPerBatch}
        onSettingsChange={handleCrawlSettingsChange}
      />
    </div>
  );
}

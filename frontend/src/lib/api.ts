/* eslint-disable @typescript-eslint/no-explicit-any */
/**
 * API客户端 - 与后端FastAPI服务通信
 */

export const API_BASE_URL = (process.env.NEXT_PUBLIC_API_BASE_URL?.trim() || 'http://localhost:8208').replace(/\/$/, '');

import type {
  ApiResponse,
  WinRateResponse,
  Task,
  TaskSummaryResponse,
  DatabaseStats,
  Topic,
  FileItem,
  FileStatus,
  PaginatedResponse,
  Group,
  GroupStats,
  Account,
  AccountSelf,
  GlobalHotWordItem,
  GlobalHotWordResponse,
  StockEvent,
  StockEventsResponse,
  ColumnInfo,
  ColumnTopic,
  ColumnTopicDetail,
  ColumnImage,
  ColumnVideo,
  ColumnFile,
  ColumnComment,
  ColumnsStats,
  ColumnsFetchSettings
} from './api-types';

export type {
  ApiResponse,
  WinRateResponse,
  Task,
  TaskSummaryResponse,
  DatabaseStats,
  Topic,
  FileItem,
  FileStatus,
  PaginatedResponse,
  Group,
  GroupStats,
  Account,
  AccountSelf,
  GlobalHotWordItem,
  GlobalHotWordResponse,
  StockEvent,
  StockEventsResponse,
  ColumnInfo,
  ColumnTopic,
  ColumnTopicDetail,
  ColumnImage,
  ColumnVideo,
  ColumnFile,
  ColumnComment,
  ColumnsStats,
  ColumnsFetchSettings
} from './api-types';


// 类型定义
// API客户端类
class ApiClient {
  private baseUrl: string;

  constructor(baseUrl: string = API_BASE_URL) {
    this.baseUrl = baseUrl;
  }

  private async request<T>(
    endpoint: string,
    options: RequestInit = {}
  ): Promise<T> {
    const url = `${this.baseUrl}${endpoint}`;

    const response = await fetch(url, {
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
      ...options,
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(errorData.detail || `HTTP ${response.status}: ${response.statusText}`);
    }

    return response.json();
  }

  // 健康检查
  async healthCheck() {
    return this.request('/api/health');
  }

  async getFeatures(): Promise<any> {
    return this.request('/api/meta/features');
  }

  // 配置相关
  async getConfig() {
    return this.request('/api/config');
  }

  async updateConfig(config: { cookie: string }) {
    return this.request('/api/config', {
      method: 'POST',
      body: JSON.stringify(config),
    });
  }

  // 数据库统计
  async getDatabaseStats(): Promise<DatabaseStats> {
    return this.request('/api/database/stats');
  }

  // 任务相关
  async getTasks(): Promise<Task[]> {
    return this.request('/api/tasks');
  }

  async getTaskSummary(): Promise<TaskSummaryResponse> {
    return this.request('/api/tasks/summary');
  }

  async getTask(taskId: string): Promise<Task> {
    return this.request(`/api/tasks/${taskId}`);
  }

  async stopTask(taskId: string) {
    return this.request(`/api/tasks/${taskId}/stop`, {
      method: 'POST',
    });
  }

  // 爬取相关
  async crawlHistorical(groupId: number, pages: number = 10, perPage: number = 20, crawlSettings?: {
    crawlIntervalMin?: number;
    crawlIntervalMax?: number;
    longSleepIntervalMin?: number;
    longSleepIntervalMax?: number;
    pagesPerBatch?: number;
  }) {
    return this.request(`/api/crawl/historical/${groupId}`, {
      method: 'POST',
      body: JSON.stringify({
        pages,
        per_page: perPage,
        ...crawlSettings
      }),
    });
  }

  async crawlAll(groupId: number, crawlSettings?: {
    crawlIntervalMin?: number;
    crawlIntervalMax?: number;
    longSleepIntervalMin?: number;
    longSleepIntervalMax?: number;
    pagesPerBatch?: number;
  }) {
    return this.request(`/api/crawl/all/${groupId}`, {
      method: 'POST',
      body: JSON.stringify(crawlSettings || {}),
    });
  }

  async crawlIncremental(groupId: number, pages: number = 10, perPage: number = 20, crawlSettings?: {
    crawlIntervalMin?: number;
    crawlIntervalMax?: number;
    longSleepIntervalMin?: number;
    longSleepIntervalMax?: number;
    pagesPerBatch?: number;
  }) {
    return this.request(`/api/crawl/incremental/${groupId}`, {
      method: 'POST',
      body: JSON.stringify({
        pages,
        per_page: perPage,
        ...crawlSettings
      }),
    });
  }

  async crawlLatestUntilComplete(groupId: number, crawlSettings?: {
    crawlIntervalMin?: number;
    crawlIntervalMax?: number;
    longSleepIntervalMin?: number;
    longSleepIntervalMax?: number;
    pagesPerBatch?: number;
  }) {
    return this.request(`/api/crawl/latest-until-complete/${groupId}`, {
      method: 'POST',
      body: JSON.stringify(crawlSettings || {}),
    });
  }

  async getTopicDetail(topicId: number | string, groupId: number | string) {
    // 统一转为字符串，避免大整数在前端被 Number 处理后精度丢失
    const id = String(topicId);
    return this.request(`/api/topics/${id}/${groupId}`);
  }

  async refreshTopic(topicId: number | string, groupId: number) {
    const id = String(topicId);
    return this.request(`/api/topics/${id}/${groupId}/refresh`, {
      method: 'POST',
    });
  }

  // 删除单个话题
  async deleteSingleTopic(groupId: number | string, topicId: number | string) {
    return this.request(`/api/topics/${topicId}/${groupId}`, {
      method: 'DELETE',
    });
  }

  // 单个话题采集（测试特殊话题）
  async fetchSingleTopic(groupId: number | string, topicId: number | string, fetchComments: boolean = false) {
    const id = String(topicId);
    const params = new URLSearchParams();
    if (fetchComments) params.append('fetch_comments', 'true');
    const url = `/api/topics/fetch-single/${groupId}/${id}${params.toString() ? '?' + params.toString() : ''}`;
    return this.request(url, { method: 'POST' });
  }

  // 获取代理图片URL，解决防盗链问题
  getProxyImageUrl(originalUrl: string, groupId?: string): string {
    if (!originalUrl) return '';
    const params = new URLSearchParams({ url: originalUrl });
    if (groupId) {
      params.append('group_id', groupId);
    }
    return `${API_BASE_URL}/api/proxy-image?${params.toString()}`;
  }

  // 获取本地缓存图片URL
  getLocalImageUrl(groupId: string, localPath: string): string {
    if (!localPath) return '';
    return `${API_BASE_URL}/api/groups/${groupId}/images/${encodeURIComponent(localPath)}`;
  }

  // 获取本地缓存视频URL
  getLocalVideoUrl(groupId: string, videoFilename: string): string {
    if (!videoFilename) return '';
    return `${API_BASE_URL}/api/groups/${groupId}/videos/${encodeURIComponent(videoFilename)}`;
  }

  // 图片缓存管理
  async getImageCacheInfo(groupId: string) {
    return this.request(`/api/cache/images/info/${groupId}`);
  }

  async clearImageCache(groupId: string) {
    return this.request(`/api/cache/images/${groupId}`, {
      method: 'DELETE',
    });
  }

  // 群组相关
  async getGroupInfo(groupId: number) {
    return this.request(`/api/groups/${groupId}/info`);
  }

  // 文件相关
  async collectFiles(groupId: number | string) {
    return this.request(`/api/files/collect/${groupId}`, {
      method: 'POST',
      body: JSON.stringify({}),
    });
  }

  async downloadFiles(groupId: number, maxFiles?: number, sortBy: string = 'download_count',
    downloadInterval: number = 1.0, longSleepInterval: number = 60.0,
    filesPerBatch: number = 10, downloadIntervalMin?: number,
    downloadIntervalMax?: number, longSleepIntervalMin?: number,
    longSleepIntervalMax?: number) {
    const requestBody: any = {
      max_files: maxFiles,
      sort_by: sortBy,
      download_interval: downloadInterval,
      long_sleep_interval: longSleepInterval,
      files_per_batch: filesPerBatch
    };

    // 如果提供了随机间隔范围参数，则添加到请求中
    if (downloadIntervalMin !== undefined) {
      requestBody.download_interval_min = downloadIntervalMin;
      requestBody.download_interval_max = downloadIntervalMax;
      requestBody.long_sleep_interval_min = longSleepIntervalMin;
      requestBody.long_sleep_interval_max = longSleepIntervalMax;
    }

    return this.request(`/api/files/download/${groupId}`, {
      method: 'POST',
      body: JSON.stringify(requestBody),
    });
  }

  async clearFileDatabase(groupId: number) {
    return this.request(`/api/files/clear/${groupId}`, {
      method: 'POST',
    });
  }

  async clearTopicDatabase(groupId: number) {
    return this.request(`/api/topics/clear/${groupId}`, {
      method: 'POST',
    });
  }

  async getFileStats(groupId: number) {
    return this.request(`/api/files/stats/${groupId}`);
  }

  async downloadSingleFile(groupId: string, fileId: number, fileName?: string, fileSize?: number) {
    const params = new URLSearchParams();
    if (fileName) params.append('file_name', fileName);
    if (fileSize !== undefined) params.append('file_size', fileSize.toString());

    const url = `/api/files/download-single/${groupId}/${fileId}${params.toString() ? '?' + params.toString() : ''}`;
    return this.request(url, {
      method: 'POST',
    });
  }

  async getFileStatus(groupId: string, fileId: number) {
    return this.request(`/api/files/status/${groupId}/${fileId}`);
  }

  async checkLocalFileStatus(groupId: string, fileName: string, fileSize: number) {
    const params = new URLSearchParams({
      file_name: fileName,
      file_size: fileSize.toString(),
    });
    return this.request(`/api/files/check-local/${groupId}?${params}`);
  }

  // 数据查询
  async getTopics(page: number = 1, perPage: number = 20, search?: string): Promise<PaginatedResponse<Topic>> {
    const params = new URLSearchParams({
      page: page.toString(),
      per_page: perPage.toString(),
    });

    if (search) {
      params.append('search', search);
    }

    const response = await this.request<{ topics: Topic[], pagination: any }>(`/api/topics?${params}`);
    return {
      data: response.topics,
      pagination: response.pagination,
    };
  }

  async getFiles(groupId: number, page: number = 1, perPage: number = 20, status?: string): Promise<PaginatedResponse<FileItem>> {
    const params = new URLSearchParams({
      page: page.toString(),
      per_page: perPage.toString(),
    });

    if (status) {
      params.append('status', status);
    }

    const response = await this.request<{ files: FileItem[], pagination: any }>(`/api/files/${groupId}?${params}`);
    return {
      data: response.files,
      pagination: response.pagination,
    };
  }

  // 群组相关
  async refreshLocalGroups(): Promise<{ success: boolean; count: number; groups: number[]; error?: string }> {
    return this.request('/api/local-groups/refresh', {
      method: 'POST',
    });
  }

  async getGroups(): Promise<{ groups: Group[], total: number }> {
    return this.request('/api/groups');
  }

  async getGroupTopics(groupId: number, page: number = 1, perPage: number = 20, search?: string): Promise<PaginatedResponse<Topic>> {
    // 🧪 调试输出：请求参数
    console.log('[apiClient.getGroupTopics] request params:', { groupId, page, perPage, search });

    const params = new URLSearchParams({
      page: page.toString(),
      per_page: perPage.toString(),
    });

    if (search) {
      params.append('search', search);
    }

    const url = `/api/groups/${groupId}/topics?${params}`;
    const response = await this.request<{ topics: Topic[], pagination: any }>(url);

    // 🧪 调试输出：原始返回中的 topic_id（前 10 条）
    try {
      const debugTopics = (response.topics || []).slice(0, 10).map((t: any) => ({
        topic_id: t.topic_id,
        title: t.title,
      }));
      console.log('[apiClient.getGroupTopics] raw response topics (first 10):', debugTopics);
    } catch (e) {
      console.warn('[apiClient.getGroupTopics] debug log failed:', e);
    }

    const result: PaginatedResponse<Topic> = {
      data: response.topics,
      pagination: response.pagination,
    };

    // 🧪 调试输出：返回给调用方的数据结构
    try {
      const offerTopic = (result.data || []).find((t: any) =>
        typeof t.title === 'string' && t.title.startsWith('Offer选择')
      );
      if (offerTopic) {
        console.log('[apiClient.getGroupTopics] Offer topic in result:', {
          topic_id: (offerTopic as any).topic_id,
          title: offerTopic.title,
        });
      } else {
        console.log('[apiClient.getGroupTopics] Offer topic not found in result');
      }
    } catch (e) {
      console.warn('[apiClient.getGroupTopics] debug Offer topic failed:', e);
    }

    return result;
  }

  async getGroupStats(groupId: number): Promise<GroupStats> {
    return this.request(`/api/groups/${groupId}/stats`);
  }

  // 获取群组专栏摘要信息
  async getGroupColumnsSummary(groupId: number): Promise<{
    has_columns: boolean;
    title: string | null;
    error?: string;
  }> {
    return this.request(`/api/groups/${groupId}/columns/summary`);
  }

  async getGroupTags(groupId: number) {
    return this.request(`/api/groups/${groupId}/tags`);
  }

  async getTagTopics(groupId: number, tagId: number, page: number = 1, perPage: number = 20): Promise<PaginatedResponse<Topic>> {
    const params = new URLSearchParams({
      page: page.toString(),
      per_page: perPage.toString(),
    });

    const response = await this.request<{ topics: Topic[], pagination: any }>(`/api/groups/${groupId}/tags/${tagId}/topics?${params}`);
    return {
      data: response.topics,
      pagination: response.pagination,
    };
  }

  // 设置相关
  async getCrawlerSettings() {
    return this.request('/api/settings/crawler');
  }

  async updateCrawlerSettings(settings: {
    min_delay: number;
    max_delay: number;
    long_delay_interval: number;
    timestamp_offset_ms: number;
    debug_mode: boolean;
  }) {
    return this.request('/api/settings/crawler', {
      method: 'POST',
      body: JSON.stringify(settings),
    });
  }

  async getDownloaderSettings() {
    return this.request('/api/settings/downloader');
  }

  async updateDownloaderSettings(settings: {
    download_interval_min: number;
    download_interval_max: number;
    long_delay_interval: number;
    long_delay_min: number;
    long_delay_max: number;
  }) {
    return this.request('/api/settings/downloader', {
      method: 'POST',
      body: JSON.stringify(settings),
    });
  }

  async getCrawlSettings() {
    return this.request('/api/settings/crawl');
  }

  async updateCrawlSettings(settings: {
    crawl_interval_min: number;
    crawl_interval_max: number;
    long_sleep_interval_min: number;
    long_sleep_interval_max: number;
    pages_per_batch: number;
  }) {
    return this.request('/api/settings/crawl', {
      method: 'POST',
      body: JSON.stringify(settings),
    });
  }

  // 账号管理
  async listAccounts(): Promise<{ accounts: Account[] }> {
    return this.request('/api/accounts');
  }

  async createAccount(params: { cookie: string; name?: string }) {
    return this.request('/api/accounts', {
      method: 'POST',
      body: JSON.stringify({
        cookie: params.cookie,
        name: params.name,
      }),
    });
  }

  async deleteAccount(accountId: string) {
    return this.request(`/api/accounts/${accountId}`, {
      method: 'DELETE',
    });
  }

  async assignGroupAccount(groupId: number | string, accountId: string) {
    return this.request(`/api/groups/${groupId}/assign-account`, {
      method: 'POST',
      body: JSON.stringify({ account_id: accountId }),
    });
  }

  async getGroupAccount(groupId: number | string): Promise<{ account: Account | null }> {
    return this.request(`/api/groups/${groupId}/account`);
  }

  // 账号自我信息（/v3/users/self）
  async getAccountSelf(accountId: string): Promise<{ self: AccountSelf | null }> {
    return this.request(`/api/accounts/${accountId}/self`);
  }

  async refreshAccountSelf(accountId: string): Promise<{ self: AccountSelf | null }> {
    return this.request(`/api/accounts/${accountId}/self/refresh`, {
      method: 'POST',
    });
  }

  async getGroupAccountSelf(groupId: number | string): Promise<{ self: AccountSelf | null }> {
    return this.request(`/api/groups/${groupId}/self`);
  }

  async refreshGroupAccountSelf(groupId: number | string): Promise<{ self: AccountSelf | null }> {
    return this.request(`/api/groups/${groupId}/self/refresh`, {
      method: 'POST',
    });
  }
  async crawlByTimeRange(
    groupId: number,
    params: {
      startTime?: string;
      endTime?: string;
      lastDays?: number;
      perPage?: number;
      crawlIntervalMin?: number;
      crawlIntervalMax?: number;
      longSleepIntervalMin?: number;
      longSleepIntervalMax?: number;
      pagesPerBatch?: number;
    }
  ) {
    return this.request(`/api/crawl/range/${groupId}`, {
      method: 'POST',
      body: JSON.stringify(params || {}),
    });
  }
  // 删除社群本地数据
  async deleteGroup(groupId: number | string) {
    return this.request(`/api/groups/${groupId}`, {
      method: 'DELETE',
    });
  }

  // =========================
  // 专栏相关 API
  // =========================

  // 获取群组专栏目录列表
  async getGroupColumns(groupId: number | string): Promise<{
    columns: ColumnInfo[];
    stats: ColumnsStats;
  }> {
    return this.request(`/api/groups/${groupId}/columns`);
  }

  // 获取专栏下的文章列表
  async getColumnTopics(groupId: number | string, columnId: number): Promise<{
    column: ColumnInfo;
    topics: ColumnTopic[];
  }> {
    return this.request(`/api/groups/${groupId}/columns/${columnId}/topics`);
  }

  // 获取专栏文章详情
  async getColumnTopicDetail(groupId: number | string, topicId: number): Promise<ColumnTopicDetail> {
    return this.request(`/api/groups/${groupId}/columns/topics/${topicId}`);
  }

  // 获取专栏文章完整评论
  async getColumnTopicFullComments(groupId: number | string, topicId: number): Promise<{
    success: boolean;
    comments: ColumnComment[];
    total: number;
  }> {
    return this.request(`/api/groups/${groupId}/columns/topics/${topicId}/comments`);
  }

  // 采集群组所有专栏内容
  async fetchGroupColumns(groupId: number | string, settings?: ColumnsFetchSettings): Promise<{
    success: boolean;
    task_id: string;
    message: string;
  }> {
    return this.request(`/api/groups/${groupId}/columns/fetch`, {
      method: 'POST',
      body: JSON.stringify(settings || {}),
    });
  }

  // 获取专栏统计信息
  async getColumnsStats(groupId: number | string): Promise<ColumnsStats> {
    return this.request(`/api/groups/${groupId}/columns/stats`);
  }

  // 删除群组所有专栏数据
  async deleteAllColumns(groupId: number | string): Promise<{
    success: boolean;
    message: string;
    deleted: {
      columns_deleted: number;
      topics_deleted: number;
      details_deleted: number;
      images_deleted: number;
      files_deleted: number;
      videos_deleted: number;
      comments_deleted: number;
    };
  }> {
    return this.request(`/api/groups/${groupId}/columns/all`, {
      method: 'DELETE',
    });
  }

  // =========================
  // 股票舆情分析 API
  // =========================

  async scanStocks(groupId: number | string, force: boolean = false): Promise<{ task_id: string; message: string }> {
    return this.request(`/api/groups/${groupId}/stock/scan?force=${force}`, { method: 'POST' });
  }

  async getStockStats(groupId: number | string): Promise<any> {
    return this.request(`/api/groups/${groupId}/stock/stats`);
  }

  async getStockTopics(groupId: number | string, page: number = 1, perPage: number = 20) {
    const q = new URLSearchParams({ page: String(page), per_page: String(perPage) });
    return this.request(`/api/groups/${groupId}/stock/topics?${q.toString()}`);
  }

  async getStockMentions(groupId: number | string, params?: {
    stock_code?: string; page?: number; per_page?: number;
    sort_by?: string; order?: string;
  }): Promise<any> {
    const q = new URLSearchParams();
    if (params?.stock_code) q.append('stock_code', params.stock_code);
    if (params?.page) q.append('page', params.page.toString());
    if (params?.per_page) q.append('per_page', params.per_page.toString());
    if (params?.sort_by) q.append('sort_by', params.sort_by);
    if (params?.order) q.append('order', params.order);
    return this.request(`/api/groups/${groupId}/stock/mentions?${q}`);
  }

  async getStockEvents(groupId: number | string, stockCode: string): Promise<StockEventsResponse> {
    return this.request(`/api/groups/${groupId}/stock/${stockCode}/events`);
  }

  async getStockPrice(groupId: number | string, stockCode: string, days: number = 90): Promise<any> {
    return this.request(`/api/groups/${groupId}/stock/${stockCode}/price?days=${days}`);
  }

  async getStockWinRate(groupId: number | string, params?: {
    min_mentions?: number; return_period?: string; limit?: number;
    start_date?: string; end_date?: string; page?: number; page_size?: number;
    sort_by?: string; order?: string;
  }): Promise<WinRateResponse> {
    const q = new URLSearchParams();
    if (params?.min_mentions) q.append('min_mentions', params.min_mentions.toString());
    if (params?.return_period) q.append('return_period', params.return_period);
    if (params?.limit) q.append('limit', params.limit.toString());
    if (params?.start_date) q.append('start_date', params.start_date);
    if (params?.end_date) q.append('end_date', params.end_date);
    if (params?.page) q.append('page', params.page.toString());
    if (params?.page_size) q.append('page_size', params.page_size.toString());
    if (params?.sort_by) q.append('sort_by', params.sort_by);
    if (params?.order) q.append('order', params.order);
    return this.request(`/api/groups/${groupId}/stock/win-rate?${q}`);
  }

  async getSectorHeat(groupId: number | string, startDate?: string, endDate?: string): Promise<any> {
    const q = new URLSearchParams();
    if (startDate) q.append('start_date', startDate);
    if (endDate) q.append('end_date', endDate);
    return this.request(`/api/groups/${groupId}/stock/sector-heat?${q}`);
  }

  async getSectorTopics(groupId: number | string, params: {
    sector: string; start_date?: string; end_date?: string; page?: number; page_size?: number;
  }): Promise<any> {
    const q = new URLSearchParams();
    q.append('sector', params.sector);
    if (params.start_date) q.append('start_date', params.start_date);
    if (params.end_date) q.append('end_date', params.end_date);
    if (params.page) q.append('page', params.page.toString());
    if (params.page_size) q.append('page_size', params.page_size.toString());
    return this.request(`/api/groups/${groupId}/stock/sector-topics?${q}`);
  }

  async getStockSignals(groupId: number | string, lookbackDays: number = 7, minMentions: number = 2, startDate?: string, endDate?: string): Promise<any> {
    const q = new URLSearchParams();
    q.append('lookback_days', lookbackDays.toString());
    q.append('min_mentions', minMentions.toString());
    if (startDate) q.append('start_date', startDate);
    if (endDate) q.append('end_date', endDate);
    return this.request(`/api/groups/${groupId}/stock/signals?${q}`);
  }

  // ========== AI 智能分析 API ==========

  async getAIConfig(): Promise<any> {
    return this.request('/api/ai/config');
  }

  async updateAIConfig(config: { api_key: string; base_url?: string; model?: string }): Promise<any> {
    return this.request('/api/ai/config', { method: 'POST', body: JSON.stringify(config) });
  }

  async aiAnalyzeStock(groupId: number | string, stockCode: string, force: boolean = false): Promise<any> {
    return this.request(`/api/groups/${groupId}/ai/analyze/${stockCode}?force=${force}`, { method: 'POST' });
  }

  async aiDailyBrief(groupId: number | string, lookbackDays: number = 7, force: boolean = false): Promise<any> {
    return this.request(`/api/groups/${groupId}/ai/daily-brief?lookback_days=${lookbackDays}&force=${force}`, { method: 'POST' });
  }

  async aiConsensus(groupId: number | string, topN: number = 10, force: boolean = false): Promise<any> {
    return this.request(`/api/groups/${groupId}/ai/consensus?top_n=${topN}&force=${force}`, { method: 'POST' });
  }

  async getAIHistory(groupId: number | string, summaryType?: string, limit: number = 20): Promise<any> {
    const params = new URLSearchParams({ limit: String(limit) });
    if (summaryType) params.set('summary_type', summaryType);
    return this.request(`/api/groups/${groupId}/ai/history?${params.toString()}`);
  }

  async getAIHistoryDetail(groupId: number | string, summaryId: number): Promise<any> {
    return this.request(`/api/groups/${groupId}/ai/history/${summaryId}`);
  }

  // ========== 全局看板 API ==========

  async getGlobalStats(): Promise<any> {
    return this.request('/api/global/stats');
  }

  async getGlobalHotWords(params?: {
    windowHours?: 24 | 36 | 48 | 168;
    limit?: number;
    force?: boolean;
    normalize?: boolean;
    fallback?: boolean;
  }): Promise<GlobalHotWordResponse> {
    const p = params || {};
    const windowHours = p.windowHours ?? 24;
    const limit = p.limit ?? 50;
    const force = p.force ?? false;
    const normalize = p.normalize ?? true;
    const fallback = p.fallback ?? true;

    const q = new URLSearchParams({
      window_hours: String(windowHours),
      limit: String(limit),
      force: String(force),
      normalize: String(normalize),
      fallback: String(fallback),
      fallback_windows: '24,36,48,168',
      // 兼容旧后端实现，按天传递后备口径
      days: String(Math.max(1, Math.ceil(windowHours / 24))),
    });

    const res = await this.request<any>(`/api/global/hot-words?${q.toString()}`);

    // 兼容旧接口：若返回数组则包装成新结构
    if (Array.isArray(res)) {
      const words = (res || []) as GlobalHotWordItem[];
      return {
        words,
        window_hours_requested: windowHours,
        window_hours_effective: windowHours,
        fallback_applied: false,
        fallback_reason: null,
        data_points_total: words.reduce((acc, w) => acc + (Number(w.raw_count ?? w.value) || 0), 0),
        time_range: {},
      };
    }

    return {
      words: Array.isArray(res?.words) ? res.words : [],
      window_hours_requested: Number(res?.window_hours_requested ?? windowHours),
      window_hours_effective: Number(res?.window_hours_effective ?? windowHours),
      fallback_applied: Boolean(res?.fallback_applied),
      fallback_reason: res?.fallback_reason ?? null,
      data_points_total: Number(res?.data_points_total ?? 0),
      time_range: res?.time_range || {},
    };
  }

  async getGlobalWinRate(
    minMentions: number = 2,
    returnPeriod: string = 'return_5d',
    limit: number = 1000,
    startDate?: string,
    endDate?: string,
    sortBy: string = 'win_rate',
    order: string = 'desc',
    page: number = 1,
    pageSize: number = 20
  ): Promise<WinRateResponse> {
    const params = new URLSearchParams({
      min_mentions: String(minMentions),
      return_period: returnPeriod,
      limit: String(limit),
      sort_by: sortBy,
      order: order,
      page: String(page),
      page_size: String(pageSize),
    });
    if (startDate) {
      params.append('start_date', startDate);
    }
    if (endDate) {
      params.append('end_date', endDate);
    }
    const res = await this.request(`/api/global/win-rate?${params.toString()}`);

    // New format: { data: [...], total: N, ... }
    if (res.data && typeof res.total === 'number') {
      return res as WinRateResponse;
    }

    // Fallback for old format (if any): [item1, item2, ...]
    if (Array.isArray(res)) {
      return { data: res, total: res.length, page: 1, page_size: res.length };
    }

    // Just in case
    return { data: [], total: 0, page: 1, page_size: pageSize };
  }

  async getGlobalStockEvents(stockCode: string): Promise<StockEventsResponse> {
    return this.request(`/api/global/stock/${stockCode}/events`);
  }

  async getGlobalSectorHeat(startDate?: string, endDate?: string): Promise<any> {
    const params = new URLSearchParams();
    if (startDate) {
      params.append('start_date', startDate);
    }
    if (endDate) {
      params.append('end_date', endDate);
    }
    return this.request(`/api/global/sector-heat?${params.toString()}`);
  }

  async getGlobalSectorTopics(params: {
    sector: string; start_date?: string; end_date?: string; page?: number; page_size?: number;
  }): Promise<any> {
    const q = new URLSearchParams();
    q.append('sector', params.sector);
    if (params.start_date) q.append('start_date', params.start_date);
    if (params.end_date) q.append('end_date', params.end_date);
    if (params.page) q.append('page', params.page.toString());
    if (params.page_size) q.append('page_size', params.page_size.toString());
    return this.request(`/api/global/sector-topics?${q.toString()}`);
  }

  async triggerManualAnalysis(): Promise<any> {
    return this.request('/api/scheduler/analyze', { method: 'POST' });
  }

  async getGlobalSignals(
    lookbackDays: number = 7,
    minMentions: number = 2,
    startDate?: string,
    endDate?: string
  ): Promise<any> {
    const q = new URLSearchParams({
      lookback_days: String(lookbackDays),
      min_mentions: String(minMentions),
    });
    if (startDate) q.append('start_date', startDate);
    if (endDate) q.append('end_date', endDate);
    return this.request(`/api/global/signals?${q.toString()}`);
  }

  async getGlobalGroups(): Promise<any> {
    return this.request('/api/global/groups');
  }

  async getGlobalTopics(page: number = 1, perPage: number = 20, search?: string): Promise<any> {
    const q = new URLSearchParams({
      page: String(page),
      per_page: String(perPage),
    });
    if (search?.trim()) q.append('search', search.trim());
    return this.request(`/api/global/topics?${q.toString()}`);
  }

  async scanGlobal(force: boolean = false, excludeNonStock: boolean = false): Promise<any> {
    return this.request(`/api/global/scan?force=${force}&exclude_non_stock=${excludeNonStock}`, { method: 'POST' });
  }

  async getGlobalScanFilterConfig(): Promise<any> {
    return this.request('/api/global/scan-filter/config');
  }

  async updateGlobalScanFilterConfig(payload: {
    default_action?: 'include' | 'exclude';
    whitelist_group_ids: string[];
    blacklist_group_ids: string[];
  }): Promise<any> {
    return this.request('/api/global/scan-filter/config', {
      method: 'PUT',
      body: JSON.stringify(payload),
    });
  }

  async previewGlobalScanFilter(excludeNonStock: boolean = true): Promise<any> {
    return this.request(`/api/global/scan-filter/preview?exclude_non_stock=${excludeNonStock}`);
  }

  async previewBlacklistCleanup(): Promise<any> {
    return this.request('/api/global/scan-filter/cleanup-blacklist/preview');
  }

  async cleanupBlacklistData(): Promise<any> {
    return this.request('/api/global/scan-filter/cleanup-blacklist', { method: 'POST' });
  }

  async cleanupExcludedStocks(scope: 'all' | 'group' = 'all', groupId?: string | number): Promise<any> {
    const q = new URLSearchParams();
    q.append('scope', scope);
    if (scope === 'group' && groupId != null) q.append('group_id', String(groupId));
    return this.request(`/api/stocks/exclude/cleanup?${q.toString()}`, { method: 'POST' });
  }

  async aiGlobalDailyBrief(lookbackDays: number = 7, force: boolean = false): Promise<any> {
    return this.request(`/api/global/ai/daily-brief?lookback_days=${lookbackDays}&force=${force}`, { method: 'POST' });
  }

  async aiGlobalConsensus(topN: number = 15, force: boolean = false): Promise<any> {
    return this.request(`/api/global/ai/consensus?top_n=${topN}&force=${force}`, { method: 'POST' });
  }

  async getGlobalAIHistory(summaryType?: string, limit: number = 20): Promise<any> {
    const params = new URLSearchParams({ limit: String(limit) });
    if (summaryType) params.set('summary_type', summaryType);
    return this.request(`/api/global/ai/history?${params.toString()}`);
  }

  async getGlobalAIHistoryDetail(summaryId: number): Promise<any> {
    return this.request(`/api/global/ai/history/${summaryId}`);
  }

  // ========== 调度器 API ==========

  async getSchedulerStatus(): Promise<any> {
    return this.request('/api/scheduler/status');
  }

  async getSchedulerNextRuns(count: number = 5): Promise<any> {
    return this.request(`/api/scheduler/next-runs?count=${count}`);
  }

  async startScheduler(): Promise<any> {
    return this.request('/api/scheduler/start', { method: 'POST' });
  }

  async stopScheduler(): Promise<any> {
    return this.request('/api/scheduler/stop', { method: 'POST' });
  }

  async stopManualAnalysis(): Promise<any> {
    return this.request('/api/scheduler/stop_analysis', { method: 'POST' });
  }

  async updateSchedulerConfig(config: Record<string, any>): Promise<any> {
    return this.request('/api/scheduler/config', {
      method: 'POST',
      body: JSON.stringify(config),
    });
  }

  // ========== 全区轮询操作 API ==========
  async crawlGlobal(crawlSettings: {
    mode?: 'latest' | 'all' | 'incremental' | 'range';
    pages?: number;
    per_page?: number;
    start_time?: string;
    end_time?: string;
    max_items?: number;
    last_days?: number;
    // 间隔参数
    crawl_interval_min?: number;
    crawl_interval_max?: number;
    long_sleep_interval_min?: number;
    long_sleep_interval_max?: number;
    pages_per_batch?: number;
  } = {}) {
    return this.request('/api/global/crawl', {
      method: 'POST',
      body: JSON.stringify(crawlSettings),
    });
  }

  async collectGlobalFiles() {
    return this.request('/api/global/files/collect', {
      method: 'POST',
      body: JSON.stringify({}),
    });
  }

  async downloadGlobalFiles(downloadSettings?: {
    max_files?: number;
    sort_by?: string;
    download_interval?: number;
    long_sleep_interval?: number;
    files_per_batch?: number;
    download_interval_min?: number;
    download_interval_max?: number;
    long_sleep_interval_min?: number;
    long_sleep_interval_max?: number;
  }) {
    return this.request('/api/global/files/download', {
      method: 'POST',
      body: JSON.stringify(downloadSettings || {}),
    });
  }

  async analyzeGlobalPerformance(force: boolean = false) {
    const params = new URLSearchParams();
    if (force) params.append('force', 'true');
    const url = `/api/global/analyze/performance${params.toString() ? '?' + params.toString() : ''}`;
    return this.request(url, {
      method: 'POST',
    });
  }
}

// 专栏相关类型定义
// 导出单例实例
export const apiClient = new ApiClient();
export default apiClient;

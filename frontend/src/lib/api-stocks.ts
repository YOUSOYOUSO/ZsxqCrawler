import { apiClient } from './api';

export const stocksApi = {
  scanGroup: (groupId: number | string, force: boolean = false) => apiClient.scanStocks(groupId, { force }),
  getMentions: (groupId: number | string, stockCode?: string, page?: number, perPage?: number) =>
    apiClient.getStockMentions(groupId, { stock_code: stockCode, page, per_page: perPage }),
  getWinRate: (groupId: number | string, minMentions?: number, returnPeriod?: string, limit?: number) =>
    apiClient.getStockWinRate(groupId, { min_mentions: minMentions, return_period: returnPeriod, limit }),
};

import { apiClient } from './api';

export const globalApi = {
  getStats: () => apiClient.getGlobalStats(),
  getHotWords: (params?: Parameters<typeof apiClient.getGlobalHotWords>[0]) => apiClient.getGlobalHotWords(params),
  getWinRate: (...args: Parameters<typeof apiClient.getGlobalWinRate>) => apiClient.getGlobalWinRate(...args),
  getGroups: () => apiClient.getGlobalGroups(),
  getTopics: (page?: number, perPage?: number, search?: string) => apiClient.getGlobalTopics(page, perPage, search),
};


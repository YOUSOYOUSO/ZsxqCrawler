import { apiClient } from './api';

export const groupsApi = {
  list: () => apiClient.getGroups(),
  getStats: (groupId: number | string) => apiClient.getGroupStats(Number(groupId)),
  getTopics: (groupId: number | string, page?: number, perPage?: number, search?: string) =>
    apiClient.getGroupTopics(Number(groupId), page, perPage, search),
};

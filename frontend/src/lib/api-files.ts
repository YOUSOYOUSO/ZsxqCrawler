import { apiClient } from './api';

export const filesApi = {
  collect: (groupId: number | string) => apiClient.collectFiles(groupId),
  download: (groupId: number | string, maxFiles?: number, sortBy: string = 'download_count') =>
    apiClient.downloadFiles(Number(groupId), maxFiles, sortBy),
  list: (groupId: number | string, page?: number, perPage?: number) => apiClient.getFiles(Number(groupId), page, perPage),
};

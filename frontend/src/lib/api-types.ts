/* eslint-disable @typescript-eslint/no-explicit-any */

export interface ApiResponse<T = any> {
  data?: T;
  message?: string;
  error?: string;
}

export interface WinRateResponse {
  data: any[];
  total: number;
  page: number;
  page_size: number;
}

export interface Task {
  task_id: string;
  type: string;
  status: 'pending' | 'running' | 'stopping' | 'completed' | 'failed' | 'cancelled' | 'stopped' | 'idle';
  message: string;
  result?: any;
  created_at: string;
  updated_at: string;
}

export interface TaskSummaryResponse {
  running_by_type: Record<string, Task>;
  latest_by_type: Record<string, Task>;
  running_by_task_type?: Record<string, Task>;
  latest_by_task_type?: Record<string, Task>;
  scheduler?: Record<string, any>;
}

export interface DatabaseStats {
  configured?: boolean;
  topic_database: {
    stats: Record<string, number>;
    timestamp_info: {
      total_topics: number;
      oldest_timestamp: string;
      newest_timestamp: string;
      has_data: boolean;
    };
  };
  file_database: {
    stats: Record<string, number>;
  };
}

export interface Topic {
  topic_id: string;
  title: string;
  create_time: string;
  likes_count: number;
  comments_count: number;
  reading_count: number;
  type: string;
  imported_at?: string;
}

export interface FileItem {
  file_id: number;
  name: string;
  size: number;
  download_count: number;
  create_time: string;
  download_status: string;
}

export interface FileStatus {
  file_id: number;
  name: string;
  size: number;
  download_status: string;
  local_exists: boolean;
  local_size: number;
  local_path?: string;
  is_complete: boolean;
}

export interface PaginatedResponse<T> {
  data: T[];
  pagination: {
    page: number;
    per_page: number;
    total: number;
    pages: number;
  };
}

export interface Group {
  account?: Account;
  group_id: number;
  name: string;
  type: string;
  background_url?: string;
  description?: string;
  create_time?: string;
  subscription_time?: string;
  expiry_time?: string;
  join_time?: string;
  last_active_time?: string;
  status?: string;
  source?: string; // "account" | "local" | "account|local"
  is_trial?: boolean;
  trial_end_time?: string;
  membership_end_time?: string;
  owner?: {
    user_id: number;
    name: string;
    alias?: string;
    avatar_url?: string;
    description?: string;
  };
  statistics?: {
    members?: {
      count: number;
    };
    topics?: {
      topics_count: number;
      answers_count: number;
      digests_count: number;
    };
    files?: {
      count: number;
    };
  };
}

export interface GroupStats {
  group_id: number;
  group_name?: string; // Added
  topics_count: number;
  users_count: number;
  latest_topic_time?: string;
  earliest_topic_time?: string;
  total_likes: number;
  total_comments: number;
  total_readings: number;
  total_topics?: number; // Added
  total_mentions?: number; // Added
  unique_stocks?: number; // Added
  latest_topic?: string; // Added
  win_rate?: number; // Added
}
export interface Account {
  id: string;
  name?: string;
  cookie?: string; // 已掩码
  created_at?: string;
}

export interface AccountSelf {
  account_id: string;
  uid?: string;
  name?: string;
  avatar_url?: string;
  location?: string;
  user_sid?: string;
  grade?: string;
  fetched_at?: string;
  raw_json?: any;
}

export interface GlobalHotWordItem {
  name: string;
  value: number;
  raw_count?: number;
  normalized_count?: number;
}

export interface GlobalHotWordResponse {
  words: GlobalHotWordItem[];
  window_hours_requested: number;
  window_hours_effective: number;
  fallback_applied: boolean;
  fallback_reason?: string | null;
  data_points_total: number;
  time_range?: {
    start_at?: string;
    end_at?: string;
  };
}

export interface StockEvent {
  mention_id?: number;
  topic_id?: number | string;
  group_id?: number | string;
  group_name?: string;
  stock_code?: string;
  stock_name?: string;
  mention_date?: string;
  mention_time?: string;
  context?: string;
  context_snippet?: string;
  full_text?: string;
  text_snippet?: string;
  stocks?: Array<{ stock_code: string; stock_name: string }>;
  price_at_mention?: number | null;
  return_1d?: number | null;
  return_3d?: number | null;
  return_5d?: number | null;
  return_10d?: number | null;
  return_20d?: number | null;
  excess_return_5d?: number | null;
  excess_return_10d?: number | null;
  max_return?: number | null;
  max_drawdown?: number | null;
}

export interface StockEventsResponse {
  stock_code: string;
  stock_name?: string;
  total_mentions?: number;
  win_rate_5d?: number | null;
  avg_return_5d?: number | null;
  events?: StockEvent[];
}

export interface ColumnInfo {
  column_id: number;
  group_id: number;
  name: string;
  cover_url?: string;
  topics_count: number;
  create_time?: string;
  last_topic_attach_time?: string;
  imported_at?: string;
}

export interface ColumnTopic {
  topic_id: number;
  column_id: number;
  group_id: number;
  title?: string;
  text?: string;
  create_time?: string;
  attached_to_column_time?: string;
  imported_at?: string;
  has_detail?: boolean;
}

export interface ColumnTopicDetail {
  topic_id: number;
  group_id: number;
  type?: string;
  title?: string;
  full_text?: string;
  likes_count: number;
  comments_count: number;
  readers_count: number;
  digested: boolean;
  sticky: boolean;
  create_time?: string;
  modify_time?: string;
  raw_json?: string;
  imported_at?: string;
  updated_at?: string;
  owner?: {
    user_id: number;
    name: string;
    alias?: string;
    avatar_url?: string;
    description?: string;
    location?: string;
  };
  // Q&A type content
  question?: {
    text?: string;
    owner?: {
      user_id: number;
      name: string;
      alias?: string;
      avatar_url?: string;
    };
    images?: ColumnImage[];
  };
  answer?: {
    text?: string;
    owner?: {
      user_id: number;
      name: string;
      alias?: string;
      avatar_url?: string;
    };
    images?: ColumnImage[];
  };
  images: ColumnImage[];
  files: ColumnFile[];
  videos: ColumnVideo[];
  comments: ColumnComment[];
}

export interface ColumnImage {
  image_id: number;
  type?: string;
  thumbnail?: { url?: string; width?: number; height?: number };
  large?: { url?: string; width?: number; height?: number };
  original?: { url?: string; width?: number; height?: number; size?: number };
  local_path?: string;
}

export interface ColumnVideo {
  video_id: number;
  size?: number;
  duration?: number;
  cover?: {
    url?: string;
    width?: number;
    height?: number;
    local_path?: string;
  };
  video_url?: string;
  download_status?: string;
  local_path?: string;
  download_time?: string;
}

export interface ColumnFile {
  file_id: number;
  name: string;
  hash?: string;
  size?: number;
  duration?: number;
  download_count?: number;
  create_time?: string;
  download_status?: string;
  local_path?: string;
  download_time?: string;
}

export interface ColumnComment {
  comment_id: number;
  parent_comment_id?: number;
  text?: string;
  create_time?: string;
  likes_count: number;
  rewards_count: number;
  replies_count: number;
  sticky: boolean;
  owner?: {
    user_id: number;
    name: string;
    alias?: string;
    avatar_url?: string;
    location?: string;
  };
  repliee?: {
    user_id: number;
    name: string;
    alias?: string;
    avatar_url?: string;
  };
  images?: Array<{
    image_id?: number;
    type?: string;
    thumbnail?: { url?: string; width?: number; height?: number };
    large?: { url?: string; width?: number; height?: number };
    original?: { url?: string; width?: number; height?: number };
  }>;
  // Nested replies
  replied_comments?: ColumnComment[];
}

export interface ColumnsStats {
  columns_count: number;
  topics_count: number;
  details_count: number;
  images_count: number;
  files_count: number;
  files_downloaded: number;
  videos_count: number;
  videos_downloaded: number;
  comments_count: number;
}

export interface ColumnsFetchSettings {
  crawlIntervalMin?: number;
  crawlIntervalMax?: number;
  longSleepIntervalMin?: number;
  longSleepIntervalMax?: number;
  itemsPerBatch?: number;
  downloadFiles?: boolean;
  downloadVideos?: boolean;
  cacheImages?: boolean;
  incrementalMode?: boolean;
}


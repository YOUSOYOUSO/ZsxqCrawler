from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ConfigModel(BaseModel):
    cookie: str = Field(..., description="知识星球Cookie")


class CrawlHistoricalRequest(BaseModel):
    pages: int = Field(default=10, ge=1, le=1000, description="爬取页数")
    per_page: int = Field(default=20, ge=1, le=100, description="每页数量")
    crawlIntervalMin: Optional[float] = Field(default=None, ge=1.0, le=60.0, description="爬取间隔最小值(秒)")
    crawlIntervalMax: Optional[float] = Field(default=None, ge=1.0, le=60.0, description="爬取间隔最大值(秒)")
    longSleepIntervalMin: Optional[float] = Field(default=None, ge=60.0, le=3600.0, description="长休眠间隔最小值(秒)")
    longSleepIntervalMax: Optional[float] = Field(default=None, ge=60.0, le=3600.0, description="长休眠间隔最大值(秒)")
    pagesPerBatch: Optional[int] = Field(default=None, ge=5, le=50, description="每批次页面数")


class CrawlSettingsRequest(BaseModel):
    crawlIntervalMin: Optional[float] = Field(default=None, ge=1.0, le=60.0, description="爬取间隔最小值(秒)")
    crawlIntervalMax: Optional[float] = Field(default=None, ge=1.0, le=60.0, description="爬取间隔最大值(秒)")
    longSleepIntervalMin: Optional[float] = Field(default=None, ge=60.0, le=3600.0, description="长休眠间隔最小值(秒)")
    longSleepIntervalMax: Optional[float] = Field(default=None, ge=60.0, le=3600.0, description="长休眠间隔最大值(秒)")
    pagesPerBatch: Optional[int] = Field(default=None, ge=5, le=50, description="每批次页面数")


class CrawlBehaviorSettingsRequest(BaseModel):
    crawl_interval_min: float = Field(default=2.0, ge=1.0, le=60.0)
    crawl_interval_max: float = Field(default=5.0, ge=1.0, le=60.0)
    long_sleep_interval_min: float = Field(default=180.0, ge=60.0, le=3600.0)
    long_sleep_interval_max: float = Field(default=300.0, ge=60.0, le=3600.0)
    pages_per_batch: int = Field(default=15, ge=5, le=50)


class FileDownloadRequest(BaseModel):
    max_files: Optional[int] = Field(default=None, description="最大下载文件数")
    sort_by: str = Field(default="download_count", description="排序方式: download_count 或 time")
    download_interval: float = Field(default=1.0, ge=0.1, le=300.0, description="单次下载间隔（秒）")
    long_sleep_interval: float = Field(default=60.0, ge=10.0, le=3600.0, description="长休眠间隔（秒）")
    files_per_batch: int = Field(default=10, ge=1, le=100, description="下载多少文件后触发长休眠")
    download_interval_min: Optional[float] = Field(default=None, ge=1.0, le=300.0, description="随机下载间隔最小值（秒）")
    download_interval_max: Optional[float] = Field(default=None, ge=1.0, le=300.0, description="随机下载间隔最大值（秒）")
    long_sleep_interval_min: Optional[float] = Field(default=None, ge=10.0, le=3600.0, description="随机长休眠间隔最小值（秒）")
    long_sleep_interval_max: Optional[float] = Field(default=None, ge=10.0, le=3600.0, description="随机长休眠间隔最大值（秒）")


class ColumnsSettingsRequest(BaseModel):
    crawlIntervalMin: Optional[float] = Field(default=2.0, ge=1.0, le=60.0, description="采集间隔最小值(秒)")
    crawlIntervalMax: Optional[float] = Field(default=5.0, ge=1.0, le=60.0, description="采集间隔最大值(秒)")
    longSleepIntervalMin: Optional[float] = Field(default=30.0, ge=10.0, le=600.0, description="长休眠间隔最小值(秒)")
    longSleepIntervalMax: Optional[float] = Field(default=60.0, ge=10.0, le=600.0, description="长休眠间隔最大值(秒)")
    itemsPerBatch: Optional[int] = Field(default=10, ge=3, le=50, description="每批次处理数量")
    downloadFiles: Optional[bool] = Field(default=True, description="是否下载文件")
    downloadVideos: Optional[bool] = Field(default=True, description="是否下载视频(需要ffmpeg)")
    cacheImages: Optional[bool] = Field(default=True, description="是否缓存图片")
    incrementalMode: Optional[bool] = Field(default=False, description="增量模式：跳过已存在的文章详情")


class AccountCreateRequest(BaseModel):
    cookie: str = Field(..., description="账号Cookie")
    name: Optional[str] = Field(default=None, description="账号名称")


class AssignGroupAccountRequest(BaseModel):
    account_id: str = Field(..., description="账号ID")


class GroupInfo(BaseModel):
    group_id: int
    name: str
    type: str
    background_url: Optional[str] = None
    owner: Optional[dict] = None
    statistics: Optional[dict] = None


class TaskResponse(BaseModel):
    task_id: str
    status: str
    message: str
    result: Optional[Dict[str, Any]] = None
    created_at: datetime
    updated_at: datetime


class CrawlerSettingsRequest(BaseModel):
    min_delay: float = Field(default=2.0, ge=0.5, le=10.0)
    max_delay: float = Field(default=5.0, ge=1.0, le=20.0)
    long_delay_interval: int = Field(default=15, ge=5, le=100)
    timestamp_offset_ms: int = Field(default=1, ge=0, le=1000)
    debug_mode: bool = Field(default=False)


class DownloaderSettingsRequest(BaseModel):
    download_interval_min: int = Field(default=30, ge=1, le=300)
    download_interval_max: int = Field(default=60, ge=5, le=600)
    long_delay_interval: int = Field(default=10, ge=1, le=100)
    long_delay_min: int = Field(default=300, ge=60, le=1800)
    long_delay_max: int = Field(default=600, ge=120, le=3600)


class CrawlTimeRangeRequest(BaseModel):
    startTime: Optional[str] = Field(default=None, description="开始时间，支持 YYYY-MM-DD 或 ISO8601，缺省则按 lastDays 推导")
    endTime: Optional[str] = Field(default=None, description="结束时间，默认当前时间（本地东八区）")
    lastDays: Optional[int] = Field(default=None, ge=1, le=3650, description="最近N天（与 startTime/endTime 互斥优先；当 startTime 缺省时可用）")
    perPage: Optional[int] = Field(default=20, ge=1, le=100, description="每页数量")
    crawlIntervalMin: Optional[float] = Field(default=None, ge=1.0, le=60.0, description="爬取间隔最小值(秒)")
    crawlIntervalMax: Optional[float] = Field(default=None, ge=1.0, le=60.0, description="爬取间隔最大值(秒)")
    longSleepIntervalMin: Optional[float] = Field(default=None, ge=60.0, le=3600.0, description="长休眠间隔最小值(秒)")
    longSleepIntervalMax: Optional[float] = Field(default=None, ge=60.0, le=3600.0, description="长休眠间隔最大值(秒)")
    pagesPerBatch: Optional[int] = Field(default=None, ge=5, le=50, description="每批次页面数")


class AIConfigModel(BaseModel):
    api_key: str
    base_url: Optional[str] = None
    model: Optional[str] = None


class GlobalCrawlRequest(BaseModel):
    mode: str = "latest"
    pages: Optional[int] = 100
    per_page: Optional[int] = 20
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    max_items: Optional[int] = 500
    last_days: Optional[int] = None
    crawl_interval_min: Optional[float] = None
    crawl_interval_max: Optional[float] = None
    long_sleep_interval_min: Optional[float] = None
    long_sleep_interval_max: Optional[float] = None
    pages_per_batch: Optional[int] = None


class GlobalFileCollectRequest(BaseModel):
    pass


class GlobalFileDownloadRequest(BaseModel):
    max_files: int = 50
    sort_by: str = "create_time"
    download_interval: float = 30.0
    long_sleep_interval: float = 60.0
    files_per_batch: int = 10
    download_interval_min: Optional[float] = None
    download_interval_max: Optional[float] = None
    long_sleep_interval_min: Optional[float] = None
    long_sleep_interval_max: Optional[float] = None


class MarketDataSourceSettingsRequest(BaseModel):
    providers: List[str] = Field(default_factory=lambda: ["tx", "sina", "akshare", "tushare"])
    realtime_providers: List[str] = Field(default_factory=lambda: ["akshare", "tx", "sina", "tushare"])
    realtime_provider_failover_enabled: bool = True
    provider_failover_enabled: bool = True
    provider_circuit_breaker_seconds: float = Field(default=300.0, ge=0.0, le=3600.0)
    sync_retry_max: int = Field(default=3, ge=1, le=10)
    sync_retry_backoff_seconds: float = Field(default=1.0, ge=0.0, le=30.0)
    sync_failure_cooldown_seconds: float = Field(default=120.0, ge=0.0, le=3600.0)
    tushare_token: Optional[str] = Field(default=None, description="可选；传空字符串将清空")


class MarketDataProbeRequest(BaseModel):
    providers: Optional[List[str]] = None
    symbol: str = Field(default="000001.SZ")


class ScanFilterConfigRequest(BaseModel):
    default_action: str = Field(default="include")
    whitelist_group_ids: List[str] = Field(default_factory=list)
    blacklist_group_ids: List[str] = Field(default_factory=list)

"""
知识星球数据采集器 - FastAPI 后端服务
提供RESTful API接口来操作现有的爬虫功能
"""

import os
import sys
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime
from contextlib import asynccontextmanager
import json
import requests

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import uvicorn
import random
import time
from pathlib import Path

# 添加项目根目录到Python路径（app/main.py 在 app 目录下）
project_root = str(Path(__file__).resolve().parents[1])
if project_root not in sys.path:
    sys.path.append(project_root)
from modules.shared.paths import get_config_path

# 导入现有的业务逻辑模块
from modules.zsxq.zsxq_interactive_crawler import ZSXQInteractiveCrawler, load_config
from modules.zsxq.zsxq_database import ZSXQDatabase
from modules.zsxq.zsxq_file_database import ZSXQFileDatabase
from modules.shared.db_path_manager import get_db_path_manager
# 使用SQL账号管理器
from modules.accounts.accounts_sql_manager import get_accounts_sql_manager
from modules.accounts.account_info_db import get_account_info_db
from modules.zsxq.zsxq_columns_database import ZSXQColumnsDatabase
from modules.shared.logger_config import log_info, log_warning, log_error, log_exception, log_debug, ensure_configured
from api.app_factory import register_core_routers
from api.services.account_resolution_service import (
    build_account_group_detection,
    clear_account_detect_cache,
    fetch_groups_from_api,
    get_account_summary_for_group_auto,
    get_cookie_for_group,
)
from api.deps.container import get_task_runtime
from api.schemas.models import (
    AccountCreateRequest,
    AssignGroupAccountRequest,
    ColumnsSettingsRequest,
    ConfigModel,
    CrawlBehaviorSettingsRequest,
    CrawlHistoricalRequest,
    CrawlSettingsRequest,
    CrawlTimeRangeRequest,
    CrawlerSettingsRequest,
    DownloaderSettingsRequest,
    FileDownloadRequest,
    GlobalCrawlRequest,
    GlobalFileCollectRequest,
    GlobalFileDownloadRequest,
    ScanFilterConfigRequest,
)

# 初始化日志系统
ensure_configured()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理：启动时扫描本地群"""
    # 启动时执行
    try:
        await asyncio.to_thread(scan_local_groups)
    except Exception as e:
        print(f"⚠️ 启动扫描本地群失败: {e}")
    yield
    # 关闭时执行（如需要可添加清理逻辑）


app = FastAPI(
    title="知识星球数据采集器 API",
    description="为知识星球数据采集器提供RESTful API接口",
    version="1.0.0",
    lifespan=lifespan
)
register_core_routers(app)

def _parse_cors_origins() -> List[str]:
    """
    解析 CORS 白名单，默认仅允许本地开发端口。
    通过环境变量 CORS_ALLOW_ORIGINS 以逗号分隔覆盖。
    """
    raw = os.environ.get(
        "CORS_ALLOW_ORIGINS",
        "http://localhost:3060,http://127.0.0.1:3060"
    )
    origins = [origin.strip().rstrip("/") for origin in raw.split(",") if origin.strip()]
    return origins or ["http://localhost:3060"]

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=_parse_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 全局变量存储爬虫实例和任务状态
crawler_instance: Optional[ZSXQInteractiveCrawler] = None
task_runtime = get_task_runtime()
current_tasks: Dict[str, Dict[str, Any]] = task_runtime.tasks
task_logs: Dict[str, List[str]] = task_runtime.logs  # 存储任务日志
task_counter = 0  # legacy task id counter (to be removed after full router migration)
sse_connections: Dict[str, List] = {}  # 存储SSE连接
task_stop_flags: Dict[str, bool] = task_runtime.stop_flags  # 任务停止标志
file_downloader_instances: Dict[str, Any] = {}  # 存储文件下载器实例

# 调度器日志钩子
def scheduler_log_callback(msg: str):
    task_runtime.set_scheduler_log(msg, cap=500)

# 调度器状态更新钩子
def scheduler_status_callback(status: str, message: str):
    update_task("scheduler", status, message)

# 延迟导入并初始化调度器回调
try:
    from app.scheduler.auto_scheduler import get_scheduler
    sc = get_scheduler()
    sc.set_log_callback(scheduler_log_callback)
    sc.set_status_callback(scheduler_status_callback)
    initial_status = sc.get_status()
    current_tasks["scheduler"]["status"] = initial_status.get("state", "stopped")
    current_tasks["scheduler"]["message"] = "自动调度系统"
    current_tasks["scheduler"]["updated_at"] = datetime.now().isoformat()
except ImportError:
    pass

# =========================
# 本地群扫描（output 目录）
# =========================

# 可配置：默认 ./output；可通过环境变量 OUTPUT_DIR 覆盖
LOCAL_OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "output")
# 处理上限保护，默认 10000；可通过 LOCAL_GROUPS_SCAN_LIMIT 覆盖
try:
    LOCAL_SCAN_LIMIT = int(os.environ.get("LOCAL_GROUPS_SCAN_LIMIT", "10000"))
except Exception:
    LOCAL_SCAN_LIMIT = 10000

# 本地群缓存
_local_groups_cache = {
    "ids": set(),     # set[int]
    "scanned_at": 0.0 # epoch 秒
}


def _safe_listdir(path: str):
    """安全列目录，异常不抛出，返回空列表并告警"""
    try:
        return os.listdir(path)
    except Exception as e:
        print(f"⚠️ 无法读取目录 {path}: {e}")
        return []


def _collect_numeric_dirs(base: str, limit: int) -> set:
    r"""
    扫描 base 的一级子目录，收集纯数字目录名（^\d+$）作为群ID。
    忽略：非目录、软链接、隐藏目录（以 . 开头）。
    """
    ids = set()
    if not base:
        return ids

    base_abs = os.path.abspath(base)
    if not (os.path.exists(base_abs) and os.path.isdir(base_abs)):
        # 视为空集合，不报错
        print(f"⚠️ 目录不存在或不可读: {base_abs}，视为空集合")
        return ids

    processed = 0
    for name in _safe_listdir(base_abs):
        # 隐藏目录
        if not name or name.startswith('.'):
            continue

        path = os.path.join(base_abs, name)
        try:
            # 软链接/非目录忽略
            if os.path.islink(path) or not os.path.isdir(path):
                continue

            # 仅纯数字目录名
            if name.isdigit():
                ids.add(int(name))
                processed += 1
                if processed >= limit:
                    print(f"⚠️ 子目录数量超过上限 {limit}，已截断")
                    break
        except Exception:
            # 单项失败安全降级
            continue

    return ids


def scan_local_groups(output_dir: str = None, limit: int = None) -> set:
    """
    扫描本地 output 的一级子目录，获取群ID集合。
    同时兼容 output/databases 结构（如存在）。
    同步执行（用于手动刷新或强制刷新），异常安全降级。
    """
    try:
        odir = output_dir or LOCAL_OUTPUT_DIR
        lim = int(limit or LOCAL_SCAN_LIMIT)

        # 主路径：仅扫描 output 的一级子目录
        ids_primary = _collect_numeric_dirs(odir, lim)

        # 兼容路径：output/databases 的一级子目录（若存在）
        ids_secondary = _collect_numeric_dirs(os.path.join(odir, "databases"), lim)

        ids = set(ids_primary) | set(ids_secondary)

        # 更新缓存
        _local_groups_cache["ids"] = ids
        _local_groups_cache["scanned_at"] = time.time()

        return ids
    except Exception as e:
        print(f"⚠️ 本地群扫描异常: {e}")
        # 安全降级为旧缓存
        return _local_groups_cache.get("ids", set())


def get_cached_local_group_ids(force_refresh: bool = False) -> set:
    """
    获取缓存中的本地群ID集合；可选强制刷新。
    未扫描过或要求强更时触发同步扫描。
    """
    if force_refresh or not _local_groups_cache.get("ids"):
        return scan_local_groups()
    return _local_groups_cache.get("ids", set())


# Pydantic模型定义已迁移到 api/schemas/models.py

# 辅助函数
def get_crawler(log_callback=None) -> ZSXQInteractiveCrawler:
    """获取爬虫实例"""
    global crawler_instance
    if crawler_instance is None:
        config = load_config()
        if not config:
            raise HTTPException(status_code=500, detail="配置文件加载失败")

        auth_config = config.get('auth', {})

        cookie = auth_config.get('cookie', '')
        group_id = auth_config.get('group_id', '')

        if cookie == "your_cookie_here" or group_id == "your_group_id_here" or not cookie or not group_id:
            raise HTTPException(status_code=400, detail="请先在 config/app.toml 中配置Cookie和群组ID")

        # 使用路径管理器获取数据库路径
        path_manager = get_db_path_manager()
        db_path = path_manager.get_topics_db_path(group_id)

        crawler_instance = ZSXQInteractiveCrawler(cookie, group_id, db_path, log_callback)

    return crawler_instance

def get_crawler_for_group(group_id: str, log_callback=None) -> ZSXQInteractiveCrawler:
    """为指定群组获取爬虫实例"""
    config = load_config()
    if not config:
        raise HTTPException(status_code=500, detail="配置文件加载失败")

    # 自动匹配该群组所属账号，获取对应Cookie
    cookie = get_cookie_for_group(group_id)

    if not cookie or cookie == "your_cookie_here":
        raise HTTPException(status_code=400, detail="未找到可用Cookie，请先在账号管理或 config/app.toml 中配置")

    # 使用路径管理器获取指定群组的数据库路径
    path_manager = get_db_path_manager()
    db_path = path_manager.get_topics_db_path(group_id)

    return ZSXQInteractiveCrawler(cookie, group_id, db_path, log_callback)

def get_crawler_safe() -> Optional[ZSXQInteractiveCrawler]:
    """安全获取爬虫实例，配置未设置时返回None"""
    try:
        return get_crawler()
    except HTTPException:
        return None

def get_primary_cookie() -> Optional[str]:
    """
    获取当前优先使用的Cookie：
    1. 若账号管理中存在账号，则优先使用第一个账号的Cookie
    2. 否则回退到 config/app.toml 中的 Cookie（若已配置）
    """
    # 1. 第一个账号
    try:
        sql_mgr = get_accounts_sql_manager()
        first_acc = sql_mgr.get_first_account(mask_cookie=False)
        if first_acc:
            cookie = (first_acc.get("cookie") or "").strip()
            if cookie:
                return cookie
    except Exception:
        pass

    # 2. config/app.toml 中的 Cookie
    try:
        config = load_config()
        if not config:
            return None
        auth_config = config.get("auth", {}) or {}
        cookie = (auth_config.get("cookie") or "").strip()
        if cookie and cookie != "your_cookie_here":
            return cookie
    except Exception:
        return None

    return None


def is_configured() -> bool:
    """检查是否已配置至少一个可用的认证Cookie（账号管理或 config/app.toml 均可）"""
    return get_primary_cookie() is not None

def create_task(task_type: str, description: str) -> str:
    """创建新任务"""
    task_id = task_runtime.create_task(task_type=task_type, message=description, status="pending")
    add_task_log(task_id, f"任务创建: {description}")
    return task_id

def add_task_log(task_id: str, log_message: str):
    """添加任务日志"""
    task_runtime.append_log(task_id, log_message)
    logs = task_logs.get(task_id, [])
    formatted_log = logs[-1] if logs else log_message

    # 广播日志到所有SSE连接
    broadcast_log(task_id, formatted_log)

def broadcast_log(task_id: str, log_message: str):
    """广播日志到SSE连接"""
    # 这个函数现在主要用于存储日志，实际的SSE广播在stream端点中实现
    pass

def build_stealth_headers(cookie: str) -> Dict[str, str]:
    """构造更接近官网的请求头，提升成功率"""
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    ]
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7",
        "Cache-Control": "no-cache",
        "Cookie": cookie,
        "Origin": "https://wx.zsxq.com",
        "Pragma": "no-cache",
        "Priority": "u=1, i",
        "Referer": "https://wx.zsxq.com/",
        "Sec-Ch-Ua": "\"Google Chrome\";v=\"137\", \"Chromium\";v=\"137\", \"Not/A)Brand\";v=\"24\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"Windows\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": random.choice(user_agents),
        "X-Aduid": "a3be07cd6-dd67-3912-0093-862d844e7fe",
        "X-Request-Id": f"dcc5cb6ab-1bc3-8273-cc26-{random.randint(100000000000, 999999999999)}",
        "X-Signature": "733fd672ddf6d4e367730d9622cdd1e28a4b6203",
        "X-Timestamp": str(int(time.time())),
        "X-Version": "2.77.0",
    }
    return headers

def update_task(task_id: str, status: str, message: str, result: Optional[Dict[str, Any]] = None):
    """更新任务状态"""
    task_runtime.update_task(task_id=task_id, status=status, message=message, result=result)
    if task_id in current_tasks:
        add_task_log(task_id, f"状态更新: {message}")


def _to_iso_datetime(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        v = value.strip()
        if not v:
            return None
        try:
            return datetime.fromisoformat(v).isoformat()
        except Exception:
            return v
    return str(value)


def _task_sort_key(task: Dict[str, Any]) -> str:
    return (
        _to_iso_datetime(task.get("updated_at"))
        or _to_iso_datetime(task.get("created_at"))
        or ""
    )


def _normalize_task_snapshot(task: Dict[str, Any]) -> Dict[str, Any]:
    return {
        **task,
        "created_at": _to_iso_datetime(task.get("created_at")),
        "updated_at": _to_iso_datetime(task.get("updated_at")),
    }


def _task_category(task_type: str) -> str:
    t = str(task_type or "").strip()
    if t == "scheduler":
        return "scheduler"
    if t.startswith("global_crawl") or t.startswith("crawl_"):
        return "crawl"
    if t.startswith("global_files_collect") or t.startswith("global_files_download"):
        return "files"
    if t.startswith("global_analyze_performance") or t.startswith("global_analyze") or t.startswith("stock_scan_"):
        return "analyze"
    return "other"


def _build_task_summary() -> Dict[str, Any]:
    running_status = {"pending", "running", "stopping"}
    terminal_status = {"completed", "failed", "cancelled", "stopped", "idle"}

    running_by_type: Dict[str, Dict[str, Any]] = {}
    latest_by_type: Dict[str, Dict[str, Any]] = {}
    running_by_task_type: Dict[str, Dict[str, Any]] = {}
    latest_by_task_type: Dict[str, Dict[str, Any]] = {}

    grouped: Dict[str, List[Dict[str, Any]]] = {"crawl": [], "files": [], "analyze": [], "scheduler": [], "other": []}
    for raw_task in current_tasks.values():
        task = _normalize_task_snapshot(raw_task)
        task_type = str(task.get("type", ""))
        grouped[_task_category(str(task.get("type", "")))].append(task)
        if task_type:
            prev_running = running_by_task_type.get(task_type)
            if str(task.get("status", "")) in running_status and (prev_running is None or _task_sort_key(task) > _task_sort_key(prev_running)):
                running_by_task_type[task_type] = task
            prev_latest = latest_by_task_type.get(task_type)
            if prev_latest is None or _task_sort_key(task) > _task_sort_key(prev_latest):
                latest_by_task_type[task_type] = task

    for category, items in grouped.items():
        if not items:
            continue
        items_sorted = sorted(items, key=_task_sort_key, reverse=True)
        running_items = [t for t in items_sorted if str(t.get("status", "")) in running_status]
        if running_items:
            running_by_type[category] = running_items[0]

        terminal_items = [t for t in items_sorted if str(t.get("status", "")) in terminal_status]
        latest_by_type[category] = terminal_items[0] if terminal_items else items_sorted[0]

    try:
        from app.scheduler.auto_scheduler import get_scheduler
        scheduler_snapshot = get_scheduler().get_status()
    except Exception:
        scheduler_snapshot = {}

    return {
        "running_by_type": running_by_type,
        "latest_by_type": latest_by_type,
        "running_by_task_type": running_by_task_type,
        "latest_by_task_type": latest_by_task_type,
        "scheduler": scheduler_snapshot,
    }

def stop_task(task_id: str) -> bool:
    """停止任务"""
    if task_id not in current_tasks:
        return False

    task = current_tasks[task_id]

    if task["status"] not in ["pending", "running"]:
        return False

    # 设置停止标志
    task_runtime.request_stop(task_id)
    add_task_log(task_id, "🛑 收到停止请求，正在停止任务...")

    # 如果有爬虫实例，也设置爬虫的停止标志
    global crawler_instance, file_downloader_instances
    if crawler_instance:
        crawler_instance.set_stop_flag()

    # 如果有文件下载器实例，也设置停止标志
    if task_id in file_downloader_instances:
        downloader = file_downloader_instances[task_id]
        downloader.set_stop_flag()

    # 特殊处理调度器：调用其内部 stop 方法
    if task_id == "scheduler":
        try:
            from app.scheduler.auto_scheduler import get_scheduler
            update_task(task_id, "stopping", "调度器停止请求已发送，正在收尾...")
            # 使用 create_task 异步停止，避免阻塞 API
            asyncio.create_task(get_scheduler().stop())
            return True
        except Exception as e:
            log_error(f"停止调度器失败: {e}")
            return False

    update_task(task_id, "cancelled", "任务已被用户停止")

    return True

def is_task_stopped(task_id: str) -> bool:
    """检查任务是否被停止"""
    return task_runtime.is_stopped(task_id)

# 应用设置（持久化）
CRAWL_SETTINGS_DEFAULTS = {
    "crawl_interval_min": 2.0,
    "crawl_interval_max": 5.0,
    "long_sleep_interval_min": 180.0,
    "long_sleep_interval_max": 300.0,
    "pages_per_batch": 15,
}

APP_SETTINGS_PATH = os.path.join(get_db_path_manager().base_dir, "app_settings.json")


def _load_app_settings() -> Dict[str, Any]:
    """读取应用设置（失败时降级为空配置）"""
    try:
        if not os.path.exists(APP_SETTINGS_PATH):
            return {}
        with open(APP_SETTINGS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception as e:
        log_warning(f"读取应用设置失败，使用默认值: {e}")
        return {}


def _save_app_settings(settings: Dict[str, Any]):
    """保存应用设置"""
    os.makedirs(os.path.dirname(APP_SETTINGS_PATH), exist_ok=True)
    with open(APP_SETTINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(settings, f, ensure_ascii=False, indent=2)


def _get_crawl_settings() -> Dict[str, Any]:
    """读取并合并爬取设置"""
    settings = _load_app_settings()
    crawl_settings = settings.get("crawl", {}) if isinstance(settings, dict) else {}
    merged = dict(CRAWL_SETTINGS_DEFAULTS)
    if isinstance(crawl_settings, dict):
        merged.update({k: v for k, v in crawl_settings.items() if k in CRAWL_SETTINGS_DEFAULTS})
    return merged


def _update_crawl_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
    """更新并持久化爬取设置"""
    all_settings = _load_app_settings()
    if not isinstance(all_settings, dict):
        all_settings = {}
    all_settings["crawl"] = settings
    _save_app_settings(all_settings)
    return settings


def _resolve_crawl_interval_values(request_obj: Optional[Any]) -> Dict[str, Any]:
    """
    计算实际生效的爬取间隔参数：
    - 优先使用请求里的显式值
    - 未提供时回退到持久化设置
    """
    persisted = _get_crawl_settings()
    return {
        "crawl_interval_min": getattr(request_obj, "crawlIntervalMin", None) or persisted["crawl_interval_min"],
        "crawl_interval_max": getattr(request_obj, "crawlIntervalMax", None) or persisted["crawl_interval_max"],
        "long_sleep_interval_min": getattr(request_obj, "longSleepIntervalMin", None) or persisted["long_sleep_interval_min"],
        "long_sleep_interval_max": getattr(request_obj, "longSleepIntervalMax", None) or persisted["long_sleep_interval_max"],
        "pages_per_batch": getattr(request_obj, "pagesPerBatch", None) or persisted["pages_per_batch"],
    }

# API路由定义
@app.get("/")
async def root():
    """根路径"""
    return {"message": "知识星球数据采集器 API 服务", "version": "1.0.0"}

@app.get("/api/health")
async def health_check():
    """健康检查"""
    return {"status": "healthy", "timestamp": datetime.now()}

@app.get("/api/meta/features")
async def get_meta_features():
    """前端能力探测，避免版本不一致导致的404/字段缺失。"""
    return {
        "global_sector_topics": True,
        "scheduler_v2_status": True,
        "scheduler_next_runs": True,
        "global_scan_filter": True,
        "market_data_persistence": True,
    }

@app.get("/api/config")
async def get_config():
    """获取当前配置"""
    try:
        config = load_config()
        auth_config = (config or {}).get('auth', {}) if config else {}
        cookie = auth_config.get('cookie', '') if auth_config else ''

        configured = is_configured()

        # 隐藏敏感信息，仅返回配置状态和下载相关配置
        return {
            "configured": configured,
            "auth": {
                "cookie": "***" if cookie and cookie != "your_cookie_here" else "未配置",
            },
            "database": config.get('database', {}) if config else {},
            "download": config.get('download', {}) if config else {}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取配置失败: {str(e)}")

@app.post("/api/config")
async def update_config(config: ConfigModel):
    """更新配置"""
    try:
        # 创建配置内容
        config_content = f"""# 知识星球数据采集器配置文件
# 通过Web界面自动生成

[auth]
# 知识星球登录Cookie
cookie = "{config.cookie}"

[download]
# 下载目录
dir = "downloads"

[market_data]
enabled = true
db_path = "output/databases/akshare_market.db"
adjust = "qfq"
close_finalize_time = "15:05"
bootstrap_mode = "full_history"
bootstrap_batch_size = 200
sync_retry_max = 3
sync_retry_backoff_seconds = 1.0
"""

        # 保存配置文件
        config_path = str(get_config_path("app.toml"))
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(config_content)

        # 重置爬虫实例，强制重新加载配置
        global crawler_instance
        crawler_instance = None

        return {"message": "配置更新成功", "success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新配置失败: {str(e)}")

# 账号管理 API
@app.get("/api/accounts")
async def list_accounts():
    """获取所有账号列表"""
    try:
        sql_mgr = get_accounts_sql_manager()
        accounts = sql_mgr.get_accounts(mask_cookie=True)
        return {"accounts": accounts}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve account list: {str(e)}")

@app.post("/api/accounts")
async def create_account(request: AccountCreateRequest):
    """创建新账号"""
    try:
        sql_mgr = get_accounts_sql_manager()
        acc = sql_mgr.add_account(request.cookie, request.name)
        safe_acc = sql_mgr.get_account_by_id(acc.get("id"), mask_cookie=True)
        # 清除账号群组检测缓存，使新账号的群组立即可见
        clear_account_detect_cache()
        return {"account": safe_acc}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create account: {str(e)}")

@app.delete("/api/accounts/{account_id}")
async def remove_account(account_id: str):
    """删除账号"""
    try:
        sql_mgr = get_accounts_sql_manager()
        ok = sql_mgr.delete_account(account_id)
        if not ok:
            raise HTTPException(status_code=404, detail="Account does not exist")
        # 清除账号群组检测缓存
        clear_account_detect_cache()
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete account: {str(e)}")

@app.post("/api/groups/{group_id}/assign-account")
async def assign_account_to_group(group_id: str, request: AssignGroupAccountRequest):
    """分配群组到指定账号"""
    try:
        sql_mgr = get_accounts_sql_manager()
        ok, msg = sql_mgr.assign_group_account(group_id, request.account_id)
        if not ok:
            raise HTTPException(status_code=400, detail=msg)
        return {"success": True, "message": msg}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to assign account: {str(e)}")

@app.get("/api/groups/{group_id}/account")
async def get_group_account(group_id: str):
    try:
        summary = get_account_summary_for_group_auto(group_id)
        return {"account": summary}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取群组账号失败: {str(e)}")

# 账号“自我信息”持久化 (/v3/users/self)
@app.get("/api/accounts/{account_id}/self")
async def get_account_self(account_id: str):
    """获取并返回指定账号的已持久化自我信息；若无则尝试抓取并保存"""
    try:
        db = get_account_info_db()
        info = db.get_self_info(account_id)
        if info:
            return {"self": info}

        # 若数据库无记录则抓取
        sql_mgr = get_accounts_sql_manager()
        acc = sql_mgr.get_account_by_id(account_id, mask_cookie=False)
        if not acc:
            raise HTTPException(status_code=404, detail="Account does not exist")

        cookie = acc.get("cookie", "")
        if not cookie:
            raise HTTPException(status_code=400, detail="Account has no configured Cookie")

        headers = build_stealth_headers(cookie)
        resp = requests.get('https://api.zsxq.com/v3/users/self', headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data.get('succeeded'):
            raise HTTPException(status_code=400, detail="API returned failure")

        rd = data.get('resp_data', {}) or {}
        user = rd.get('user', {}) or {}
        wechat = (rd.get('accounts', {}) or {}).get('wechat', {}) or {}

        self_info = {
            "uid": user.get("uid"),
            "name": user.get("name") or wechat.get("name"),
            "avatar_url": user.get("avatar_url") or wechat.get("avatar_url"),
            "location": user.get("location"),
            "user_sid": user.get("user_sid"),
            "grade": user.get("grade"),
        }
        db.upsert_self_info(account_id, self_info, raw_json=data)
        return {"self": db.get_self_info(account_id)}
    except HTTPException:
        raise
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Network request failed: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve account info: {str(e)}")

@app.post("/api/accounts/{account_id}/self/refresh")
async def refresh_account_self(account_id: str):
    """强制抓取 /v3/users/self 并更新持久化"""
    try:
        sql_mgr = get_accounts_sql_manager()
        acc = sql_mgr.get_account_by_id(account_id, mask_cookie=False)
        if not acc:
            raise HTTPException(status_code=404, detail="Account does not exist")

        cookie = acc.get("cookie", "")
        if not cookie:
            raise HTTPException(status_code=400, detail="Account has no configured Cookie")

        headers = build_stealth_headers(cookie)
        resp = requests.get('https://api.zsxq.com/v3/users/self', headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data.get('succeeded'):
            raise HTTPException(status_code=400, detail="API returned failure")

        rd = data.get('resp_data', {}) or {}
        user = rd.get('user', {}) or {}
        wechat = (rd.get('accounts', {}) or {}).get('wechat', {}) or {}

        self_info = {
            "uid": user.get("uid"),
            "name": user.get("name") or wechat.get("name"),
            "avatar_url": user.get("avatar_url") or wechat.get("avatar_url"),
            "location": user.get("location"),
            "user_sid": user.get("user_sid"),
            "grade": user.get("grade"),
        }
        db = get_account_info_db()
        db.upsert_self_info(account_id, self_info, raw_json=data)
        return {"self": db.get_self_info(account_id)}
    except HTTPException:
        raise
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Network request failed: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to refresh account info: {str(e)}")

@app.get("/api/groups/{group_id}/self")
async def get_group_account_self(group_id: str):
    """获取群组当前使用账号的自我信息（若无则尝试抓取并保存）"""
    try:
        summary = get_account_summary_for_group_auto(group_id)
        cookie = get_cookie_for_group(group_id)
        account_id = (summary or {}).get('id', 'default')

        if not cookie:
            raise HTTPException(status_code=400, detail="未找到可用Cookie，请先配置账号或默认Cookie")

        db = get_account_info_db()
        info = db.get_self_info(account_id)
        if info:
            return {"self": info}

        # 抓取并写入
        headers = build_stealth_headers(cookie)
        resp = requests.get('https://api.zsxq.com/v3/users/self', headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data.get('succeeded'):
            raise HTTPException(status_code=400, detail="API返回失败")

        rd = data.get('resp_data', {}) or {}
        user = rd.get('user', {}) or {}
        wechat = (rd.get('accounts', {}) or {}).get('wechat', {}) or {}

        self_info = {
            "uid": user.get("uid"),
            "name": user.get("name") or wechat.get("name"),
            "avatar_url": user.get("avatar_url") or wechat.get("avatar_url"),
            "location": user.get("location"),
            "user_sid": user.get("user_sid"),
            "grade": user.get("grade"),
        }
        db.upsert_self_info(account_id, self_info, raw_json=data)
        return {"self": db.get_self_info(account_id)}
    except HTTPException:
        raise
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"网络请求失败: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取群组账号信息失败: {str(e)}")

@app.post("/api/groups/{group_id}/self/refresh")
async def refresh_group_account_self(group_id: str):
    """强制抓取群组当前使用账号的自我信息并持久化"""
    try:
        summary = get_account_summary_for_group_auto(group_id)
        cookie = get_cookie_for_group(group_id)
        account_id = (summary or {}).get('id', 'default')

        if not cookie:
            raise HTTPException(status_code=400, detail="未找到可用Cookie，请先配置账号或默认Cookie")

        headers = build_stealth_headers(cookie)
        resp = requests.get('https://api.zsxq.com/v3/users/self', headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data.get('succeeded'):
            raise HTTPException(status_code=400, detail="API返回失败")

        rd = data.get('resp_data', {}) or {}
        user = rd.get('user', {}) or {}
        wechat = (rd.get('accounts', {}) or {}).get('wechat', {}) or {}

        self_info = {
            "uid": user.get("uid"),
            "name": user.get("name") or wechat.get("name"),
            "avatar_url": user.get("avatar_url") or wechat.get("avatar_url"),
            "location": user.get("location"),
            "user_sid": user.get("user_sid"),
            "grade": user.get("grade"),
        }
        db = get_account_info_db()
        db.upsert_self_info(account_id, self_info, raw_json=data)
        return {"self": db.get_self_info(account_id)}
    except HTTPException:
        raise
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"网络请求失败: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"刷新群组账号信息失败: {str(e)}")

# migrated to api/routers/groups.py: @app.get("/api/database/stats")
async def get_database_stats():
    """获取数据库统计信息"""
    try:
        configured = is_configured()
        if not configured:
            return {
                "configured": False,
                "topic_database": {
                    "stats": {},
                    "timestamp_info": {
                        "total_topics": 0,
                        "oldest_timestamp": "",
                        "newest_timestamp": "",
                        "has_data": False,
                    },
                },
                "file_database": {
                    "stats": {},
                },
            }

        # 聚合所有本地群组的数据库统计信息
        path_manager = get_db_path_manager()
        groups_info = path_manager.list_all_groups()

        if not groups_info:
            # 已配置但尚未产生本地数据
            return {
                "configured": True,
                "topic_database": {
                    "stats": {},
                    "timestamp_info": {
                        "total_topics": 0,
                        "oldest_timestamp": "",
                        "newest_timestamp": "",
                        "has_data": False,
                    },
                },
                "file_database": {
                    "stats": {},
                },
            }

        aggregated_topic_stats: Dict[str, int] = {}
        aggregated_file_stats: Dict[str, int] = {}

        oldest_ts: Optional[str] = None
        newest_ts: Optional[str] = None
        total_topics = 0
        has_data = False

        for gi in groups_info:
            group_id = gi.get("group_id")
            topics_db_path = gi.get("topics_db")
            if not topics_db_path:
                continue

            # 话题数据库统计
            db = ZSXQDatabase(topics_db_path)
            try:
                topic_stats = db.get_database_stats()
                ts_info = db.get_timestamp_range_info()
            finally:
                db.close()

            for table, count in (topic_stats or {}).items():
                aggregated_topic_stats[table] = aggregated_topic_stats.get(table, 0) + int(count or 0)

            if ts_info.get("has_data"):
                has_data = True
                ot = ts_info.get("oldest_timestamp")
                nt = ts_info.get("newest_timestamp")
                if ot:
                    if oldest_ts is None or ot < oldest_ts:
                        oldest_ts = ot
                if nt:
                    if newest_ts is None or nt > newest_ts:
                        newest_ts = nt
                total_topics += int(ts_info.get("total_topics") or 0)

            # 文件数据库统计（如存在）
            db_paths = path_manager.list_group_databases(str(group_id))
            files_db_path = db_paths.get("files")
            if files_db_path:
                fdb = ZSXQFileDatabase(files_db_path)
                try:
                    file_stats = fdb.get_database_stats()
                finally:
                    fdb.close()

                for table, count in (file_stats or {}).items():
                    aggregated_file_stats[table] = aggregated_file_stats.get(table, 0) + int(count or 0)

        timestamp_info = {
            "total_topics": total_topics,
            "oldest_timestamp": oldest_ts or "",
            "newest_timestamp": newest_ts or "",
            "has_data": has_data,
        }

        return {
            "configured": True,
            "topic_database": {
                "stats": aggregated_topic_stats,
                "timestamp_info": timestamp_info,
            },
            "file_database": {
                "stats": aggregated_file_stats,
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取数据库统计失败: {str(e)}")

async def get_tasks():
    """获取所有任务状态"""
    return list(current_tasks.values())

async def get_tasks_summary():
    """按业务类别返回运行中 + 最近一次任务快照，用于 Dashboard 恢复状态。"""
    return _build_task_summary()

async def get_task(task_id: str):
    """获取特定任务状态"""
    if task_id not in current_tasks:
        raise HTTPException(status_code=404, detail="任务不存在")

    return current_tasks[task_id]

async def stop_task_api(task_id: str):
    """停止任务"""
    if stop_task(task_id):
        return {"message": "任务停止请求已发送", "task_id": task_id}
    else:
        raise HTTPException(status_code=404, detail="任务不存在或无法停止")

# 抓取后自动提取股票提及并刷新收益（避免“已抓到新帖但未做股票分析”）
# migrated to api/routers/crawl.py: legacy crawl implementation removed

# migrated to api/routers/files.py: files domain legacy implementation removed

# migrated to api/routers/topics.py: topics read/write endpoints removed

# migrated to api/routers/files.py: @app.get("/api/files/{group_id}")
async def get_files(group_id: str, page: int = 1, per_page: int = 20, status: Optional[str] = None):
    """获取指定群组的文件列表"""
    try:
        crawler = get_crawler_for_group(group_id)
        downloader = crawler.get_file_downloader()

        offset = (page - 1) * per_page

        # 构建查询SQL
        if status:
            query = """
                SELECT file_id, name, size, download_count, create_time, download_status
                FROM files
                WHERE download_status = ?
                ORDER BY create_time DESC
                LIMIT ? OFFSET ?
            """
            params = (status, per_page, offset)
        else:
            query = """
                SELECT file_id, name, size, download_count, create_time, download_status
                FROM files
                ORDER BY create_time DESC
                LIMIT ? OFFSET ?
            """
            params = (per_page, offset)

        downloader.file_db.cursor.execute(query, params)
        files = downloader.file_db.cursor.fetchall()

        # 获取总数
        if status:
            downloader.file_db.cursor.execute("SELECT COUNT(*) FROM files WHERE download_status = ?", (status,))
        else:
            downloader.file_db.cursor.execute("SELECT COUNT(*) FROM files")
        total = downloader.file_db.cursor.fetchone()[0]

        return {
            "files": [
                {
                    "file_id": file[0],
                    "name": file[1],
                    "size": file[2],
                    "download_count": file[3],
                    "create_time": file[4],
                    "download_status": file[5] if len(file) > 5 else "unknown"
                }
                for file in files
            ],
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "pages": (total + per_page - 1) // per_page
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取文件列表失败: {str(e)}")

# 群组相关API端点
@app.post("/api/local-groups/refresh")
async def refresh_local_groups():
    """
    手动刷新本地群（output）扫描缓存；不抛错，异常时返回旧缓存。
    """
    try:
        ids = await asyncio.to_thread(scan_local_groups)
        try:
            from modules.analyzers.global_analyzer import get_global_analyzer
            get_global_analyzer().invalidate_cache()
        except Exception:
            pass
        return {"success": True, "count": len(ids), "groups": sorted(list(ids))}
    except Exception as e:
        cached = get_cached_local_group_ids(force_refresh=False) or set()
        # 不报错，返回降级结果
        return {"success": False, "count": len(cached), "groups": sorted(list(cached)), "error": str(e)}

def _persist_group_meta_local(group_id: int, info: Dict[str, Any]):
    """
    将群组的封面、名称、群主与时间等元信息持久化到本地目录。
    这样即使后续账号 Cookie 失效，仅保留本地数据时，也能展示完整信息。
    """
    try:
        from pathlib import Path

        path_manager = get_db_path_manager()
        group_dir = path_manager.get_group_data_dir(str(group_id))
        meta_path = Path(group_dir) / "group_meta.json"

        meta = {
            "group_id": group_id,
            "name": info.get("name") or f"本地群（{group_id}）",
            "type": info.get("type", ""),
            "background_url": info.get("background_url", ""),
            "owner": info.get("owner", {}) or {},
            "statistics": info.get("statistics", {}) or {},
            "create_time": info.get("create_time"),
            "subscription_time": info.get("subscription_time"),
            "expiry_time": info.get("expiry_time"),
            "join_time": info.get("join_time"),
            "last_active_time": info.get("last_active_time"),
            "description": info.get("description", ""),
            "is_trial": info.get("is_trial", False),
            "trial_end_time": info.get("trial_end_time"),
            "membership_end_time": info.get("membership_end_time"),
        }

        with meta_path.open("w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️ 写入本地群组元数据失败: {e}")


@app.get("/api/groups")
async def get_groups():
    """获取群组列表：账号群 ∪ 本地目录群（去重合并）"""
    try:
        # 自动构建群组→账号映射（多账号支持）
        group_account_map = build_account_group_detection()
        local_ids = get_cached_local_group_ids(force_refresh=False)

        # 获取“当前账号”的群列表（优先账号默认账号，其次 config/app.toml；若未配置则视为空集合）
        groups_data: List[dict] = []
        try:
            primary_cookie = get_primary_cookie()
            if primary_cookie:
                groups_data = fetch_groups_from_api(primary_cookie)
        except Exception as e:
            # 不阻断，记录告警
            print(f"⚠️ 获取账号群失败，降级为本地集合: {e}")
            groups_data = []

        # 组装账号侧群为字典（id -> info）
        by_id: Dict[int, dict] = {}

        for group in groups_data or []:
            # 提取用户特定信息
            user_specific = group.get('user_specific', {}) or {}
            validity = user_specific.get('validity', {}) or {}
            trial = user_specific.get('trial', {}) or {}

            # 过期信息与状态
            actual_expiry_time = trial.get('end_time') or validity.get('end_time')
            is_trial = bool(trial.get('end_time'))

            status = None
            if actual_expiry_time:
                from datetime import datetime, timezone
                try:
                    end_time = datetime.fromisoformat(actual_expiry_time.replace('Z', '+00:00'))
                    now = datetime.now(timezone.utc)
                    days_until_expiry = (end_time - now).days
                    if days_until_expiry < 0:
                        status = 'expired'
                    elif days_until_expiry <= 7:
                        status = 'expiring_soon'
                    else:
                        status = 'active'
                except Exception:
                    pass

            gid = group.get('group_id')
            try:
                gid = int(gid)
            except Exception:
                continue

            info = {
                "group_id": gid,
                "name": group.get('name', ''),
                "type": group.get('type', ''),
                "background_url": group.get('background_url', ''),
                "owner": group.get('owner', {}) or {},
                "statistics": group.get('statistics', {}) or {},
                "status": status,
                "create_time": group.get('create_time'),
                "subscription_time": validity.get('begin_time'),
                "expiry_time": actual_expiry_time,
                "join_time": user_specific.get('join_time'),
                "last_active_time": user_specific.get('last_active_time'),
                "description": group.get('description', ''),
                "is_trial": is_trial,
                "trial_end_time": trial.get('end_time'),
                "membership_end_time": validity.get('end_time'),
                "account": group_account_map.get(str(gid)),
                "source": "account"
            }
            by_id[gid] = info

        # 合并本地目录群
        for gid in local_ids or []:
            try:
                gid_int = int(gid)
            except Exception:
                continue
            if gid_int in by_id:
                # 标注来源为 account|local，并持久化一份元信息到本地
                src = by_id[gid_int].get("source", "account")
                if "local" not in src:
                    by_id[gid_int]["source"] = "account|local"
                _persist_group_meta_local(gid_int, by_id[gid_int])
            else:
                # 仅存在于本地：优先从 group_meta.json 读取元信息，其次从本地数据库补全
                local_name = f"本地群（{gid_int}）"
                local_type = "local"
                local_bg = ""
                owner: Dict[str, Any] = {}
                join_time = None
                expiry_time = None
                last_active_time = None
                description = ""
                statistics: Dict[str, Any] = {}

                # 1. 优先读取本地元数据文件（如果之前有账号+本地时已经落盘）
                try:
                    from pathlib import Path

                    path_manager = get_db_path_manager()
                    group_dir = path_manager.get_group_data_dir(str(gid_int))
                    meta_path = Path(group_dir) / "group_meta.json"
                    if meta_path.exists():
                        with meta_path.open("r", encoding="utf-8") as f:
                            meta = json.load(f)
                        local_name = meta.get("name", local_name)
                        local_type = meta.get("type", local_type)
                        local_bg = meta.get("background_url", local_bg)
                        owner = meta.get("owner", {}) or owner
                        statistics = meta.get("statistics", {}) or statistics
                        join_time = meta.get("join_time", join_time)
                        expiry_time = meta.get("expiry_time", expiry_time)
                        last_active_time = meta.get("last_active_time", last_active_time)
                        description = meta.get("description", description)
                except Exception as e:
                    print(f"⚠️ 读取本地群组 {gid_int} 元数据文件失败: {e}")

                # 2. 若元数据文件中仍缺少信息，再从本地数据库补充
                try:
                    path_manager = get_db_path_manager()
                    db_paths = path_manager.list_group_databases(str(gid_int))
                    topics_db = db_paths.get("topics")
                    if topics_db and os.path.exists(topics_db):
                        db = ZSXQDatabase(topics_db)
                        try:
                            cur = db.cursor
                            # 群组基础信息
                            if not local_bg or local_name.startswith("本地群（"):
                                cur.execute(
                                    "SELECT name, type, background_url FROM groups WHERE group_id = ? LIMIT 1",
                                    (gid_int,),
                                )
                                row = cur.fetchone()
                                if row:
                                    if row[0]:
                                        local_name = row[0]
                                    if row[1]:
                                        local_type = row[1]
                                    if row[2]:
                                        local_bg = row[2]

                            # 本地数据时间范围（以话题时间替代“加入/过期时间”的近似）
                            if not join_time or not expiry_time:
                                cur.execute(
                                    """
                                    SELECT MIN(create_time), MAX(create_time)
                                    FROM topics
                                    WHERE group_id = ? AND create_time IS NOT NULL AND create_time != ''
                                    """,
                                    (gid_int,),
                                )
                                trow = cur.fetchone()
                                if trow:
                                    if not join_time:
                                        join_time = trow[0]
                                    if not expiry_time:
                                        expiry_time = trow[1]
                                    if not last_active_time:
                                        last_active_time = trow[1]

                            # 简单统计：话题数量
                            if not statistics:
                                cur.execute(
                                    "SELECT COUNT(*) FROM topics WHERE group_id = ?",
                                    (gid_int,),
                                )
                                topics_count = cur.fetchone()[0] or 0
                                statistics = {
                                    "topics": {
                                        "topics_count": topics_count,
                                        "answers_count": 0,
                                        "digests_count": 0,
                                    }
                                }
                        finally:
                            db.close()
                except Exception as e:
                    # 出错时降级为占位信息，不中断整个接口
                    print(f"⚠️ 读取本地群组 {gid_int} 元数据失败: {e}")

                by_id[gid_int] = {
                    "group_id": gid_int,
                    "name": local_name,
                    "type": local_type,
                    "background_url": local_bg,
                    "owner": owner,
                    "statistics": statistics,
                    "status": None,
                    "create_time": join_time,
                    "subscription_time": None,
                    "expiry_time": expiry_time,
                    "join_time": join_time,
                    "last_active_time": last_active_time,
                    "description": description,
                    "is_trial": False,
                    "trial_end_time": None,
                    "membership_end_time": None,
                    "account": None,
                    "source": "local",
                }

        # 排序：按群ID升序；如需二级排序再按来源（账号优先）
        merged = [by_id[k] for k in sorted(by_id.keys())]

        return {
            "groups": merged,
            "total": len(merged)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取群组列表失败: {str(e)}")

# migrated to api/routers/topics.py: topic detail/refresh/comments/delete/fetch-single/tags endpoints removed

# migrated to api/routers/media.py: proxy/cache/local media endpoints removed


# migrated to api/routers/settings.py: @app.get("/api/settings/crawl")
# migrated to api/routers/settings.py: @app.post("/api/settings/crawl")
# legacy settings(crawl) implementation removed

# migrated to api/routers/columns.py: @app.get("/api/groups/{group_id}/columns/summary")
# legacy implementation removed

@app.get("/api/groups/{group_id}/info")
async def get_group_info(group_id: str):
    """获取群组信息（带本地回退，避免401/500导致前端报错）"""
    try:
        # 自动匹配该群组所属账号，获取对应Cookie
        cookie = get_cookie_for_group(group_id)

        # 本地回退数据构造（不访问官方API）
        def build_fallback(source: str = "fallback", note: str = None) -> dict:
            files_count = 0
            try:
                crawler = get_crawler_for_group(group_id)
                downloader = crawler.get_file_downloader()
                try:
                    downloader.file_db.cursor.execute("SELECT COUNT(*) FROM files")
                    row = downloader.file_db.cursor.fetchone()
                    files_count = (row[0] or 0) if row else 0
                except Exception:
                    files_count = 0
            except Exception:
                files_count = 0

            try:
                gid = int(group_id)
            except Exception:
                gid = group_id

            result = {
                "group_id": gid,
                "name": f"群组 {group_id}",
                "description": "",
                "statistics": {"files": {"count": files_count}},
                "background_url": None,
                "account": get_account_summary_for_group_auto(group_id),
                "source": source,
            }
            if note:
                result["note"] = note
            return result

        # 若没有可用 Cookie，直接返回本地回退，避免抛 400/500
        if not cookie:
            return build_fallback(note="no_cookie")

        # 调用官方接口
        url = f"https://api.zsxq.com/v2/groups/{group_id}"
        headers = {
            'Cookie': cookie,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code == 200:
            data = response.json()
            if data.get('succeeded'):
                group_data = data.get('resp_data', {}).get('group', {})
                return {
                    "group_id": group_data.get('group_id'),
                    "name": group_data.get('name'),
                    "description": group_data.get('description'),
                    "statistics": group_data.get('statistics', {}),
                    "background_url": group_data.get('background_url'),
                    "account": get_account_summary_for_group_auto(group_id),
                    "source": "remote"
                }
            # 官方返回非 succeeded，也走回退
            return build_fallback(note="remote_response_failed")
        else:
            # 授权失败/权限不足 → 使用本地回退（200返回，减少前端告警）
            if response.status_code in (401, 403):
                return build_fallback(note=f"remote_api_{response.status_code}")
            # 其他状态码也回退
            return build_fallback(note=f"remote_api_{response.status_code}")

    except Exception:
        # 任何异常都回退为本地信息，避免 500
        return build_fallback(note="exception_fallback")

# migrated to api/routers/topics.py: group topics/tags/stats/database-info endpoints removed

async def get_task_logs(task_id: str):
    """获取任务日志"""
    if task_id not in task_logs:
        raise HTTPException(status_code=404, detail="任务不存在")

    return {
        "task_id": task_id,
        "logs": task_logs[task_id]
    }

async def stream_task_logs(task_id: str):
    """SSE流式传输任务日志"""
    async def event_stream():
        # 初始化连接
        if task_id not in sse_connections:
            sse_connections[task_id] = []

        # 发送历史日志
        if task_id in task_logs:
            for log in task_logs[task_id]:
                yield f"data: {json.dumps({'type': 'log', 'message': log})}\n\n"

        # 发送任务状态
        last_status = None
        last_message = None
        if task_id in current_tasks:
            task = current_tasks[task_id]
            last_status = task.get('status')
            last_message = task.get('message')
            yield f"data: {json.dumps({'type': 'status', 'status': task['status'], 'message': task['message']})}\n\n"

        # 记录当前日志数量，用于检测新日志
        last_log_count = len(task_logs.get(task_id, []))

        # 保持连接活跃
        try:
            while True:
                # 检查是否有新日志
                current_log_count = len(task_logs.get(task_id, []))
                if current_log_count > last_log_count:
                    # 发送新日志
                    new_logs = task_logs[task_id][last_log_count:]
                    for log in new_logs:
                        yield f"data: {json.dumps({'type': 'log', 'message': log})}\n\n"
                    last_log_count = current_log_count

                # 检查任务状态变化
                if task_id in current_tasks:
                    task = current_tasks[task_id]
                    status = task.get('status')
                    message = task.get('message')

                    # 仅在状态或消息发生变化时推送，避免前端持续抖动
                    if status != last_status or message != last_message:
                        yield f"data: {json.dumps({'type': 'status', 'status': status, 'message': message})}\n\n"
                        last_status = status
                        last_message = message

                    if status in ['completed', 'failed', 'cancelled', 'stopped', 'idle']:
                        break

                # 发送心跳
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"
                await asyncio.sleep(0.5)  # 更频繁的检查

        except asyncio.CancelledError:
            # 客户端断开连接
            pass

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
        }
    )

# migrated to api/routers/media.py: /api/proxy/image endpoint removed

# 设置相关API路由
@app.get("/api/settings/crawler")
async def get_crawler_settings():
    """获取爬虫设置"""
    try:
        crawler = get_crawler_safe()
        if not crawler:
            return {
                "min_delay": 2.0,
                "max_delay": 5.0,
                "long_delay_interval": 15,
                "timestamp_offset_ms": 1,
                "debug_mode": False
            }

        return {
            "min_delay": crawler.min_delay,
            "max_delay": crawler.max_delay,
            "long_delay_interval": crawler.long_delay_interval,
            "timestamp_offset_ms": crawler.timestamp_offset_ms,
            "debug_mode": crawler.debug_mode
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取爬虫设置失败: {str(e)}")

@app.post("/api/settings/crawler")
async def update_crawler_settings(request: CrawlerSettingsRequest):
    """更新爬虫设置"""
    try:
        crawler = get_crawler_safe()
        if not crawler:
            raise HTTPException(status_code=404, detail="爬虫未初始化")

        # 验证设置
        if request.min_delay >= request.max_delay:
            raise HTTPException(status_code=400, detail="最小延迟必须小于最大延迟")

        # 更新设置
        crawler.min_delay = request.min_delay
        crawler.max_delay = request.max_delay
        crawler.long_delay_interval = request.long_delay_interval
        crawler.timestamp_offset_ms = request.timestamp_offset_ms
        crawler.debug_mode = request.debug_mode

        return {
            "message": "爬虫设置已更新",
            "settings": {
                "min_delay": crawler.min_delay,
                "max_delay": crawler.max_delay,
                "long_delay_interval": crawler.long_delay_interval,
                "timestamp_offset_ms": crawler.timestamp_offset_ms,
                "debug_mode": crawler.debug_mode
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新爬虫设置失败: {str(e)}")

@app.get("/api/settings/downloader")
async def get_downloader_settings():
    """获取文件下载器设置"""
    try:
        crawler = get_crawler_safe()
        if not crawler:
            return {
                "download_interval_min": 30,
                "download_interval_max": 60,
                "long_delay_interval": 10,
                "long_delay_min": 300,
                "long_delay_max": 600
            }

        downloader = crawler.get_file_downloader()
        return {
            "download_interval_min": downloader.download_interval_min,
            "download_interval_max": downloader.download_interval_max,
            "long_delay_interval": downloader.long_delay_interval,
            "long_delay_min": downloader.long_delay_min,
            "long_delay_max": downloader.long_delay_max
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取下载器设置失败: {str(e)}")

@app.post("/api/settings/downloader")
async def update_downloader_settings(request: DownloaderSettingsRequest):
    """更新文件下载器设置"""
    try:
        crawler = get_crawler_safe()
        if not crawler:
            raise HTTPException(status_code=404, detail="爬虫未初始化")

        # 验证设置
        if request.download_interval_min >= request.download_interval_max:
            raise HTTPException(status_code=400, detail="最小下载间隔必须小于最大下载间隔")

        if request.long_delay_min >= request.long_delay_max:
            raise HTTPException(status_code=400, detail="最小长休眠时间必须小于最大长休眠时间")

        downloader = crawler.get_file_downloader()

        # 更新设置
        downloader.download_interval_min = request.download_interval_min
        downloader.download_interval_max = request.download_interval_max
        downloader.long_delay_interval = request.long_delay_interval
        downloader.long_delay_min = request.long_delay_min
        downloader.long_delay_max = request.long_delay_max

        return {
            "message": "下载器设置已更新",
            "settings": {
                "download_interval_min": downloader.download_interval_min,
                "download_interval_max": downloader.download_interval_max,
                "long_delay_interval": downloader.long_delay_interval,
                "long_delay_min": downloader.long_delay_min,
                "long_delay_max": downloader.long_delay_max
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新下载器设置失败: {str(e)}")

# account auto-resolution helpers migrated to api/services/account_resolution_service.py

# =========================
# 新增：按时间区间爬取
# =========================


# migrated to api/routers/crawl.py: @app.post("/api/crawl/range/{group_id}")
# legacy crawl(range) implementation removed

@app.delete("/api/groups/{group_id}")
async def delete_group_local(group_id: str):
    """
    删除指定社群的本地数据（数据库、下载文件、图片缓存），不影响账号对该社群的访问权限
    """
    try:
        details = {
            "topics_db_removed": False,
            "files_db_removed": False,
            "downloads_dir_removed": False,
            "images_cache_removed": False,
            "group_dir_removed": False,
        }

        # 尝试关闭数据库连接，避免文件占用
        try:
            crawler = get_crawler_for_group(group_id)
            try:
                if hasattr(crawler, "file_downloader") and crawler.file_downloader:
                    if hasattr(crawler.file_downloader, "file_db") and crawler.file_downloader.file_db:
                        crawler.file_downloader.file_db.close()
                        print(f"✅ 已关闭文件数据库连接（群 {group_id}）")
            except Exception as e:
                print(f"⚠️ 关闭文件数据库连接时出错: {e}")
            try:
                if hasattr(crawler, "db") and crawler.db:
                    crawler.db.close()
                    print(f"✅ 已关闭话题数据库连接（群 {group_id}）")
            except Exception as e:
                print(f"⚠️ 关闭话题数据库连接时出错: {e}")
        except Exception as e:
            print(f"⚠️ 获取爬虫实例以关闭连接失败: {e}")

        # 垃圾回收 + 等待片刻，确保句柄释放
        import gc, time, shutil
        gc.collect()
        time.sleep(0.3)

        path_manager = get_db_path_manager()
        group_dir = path_manager.get_group_dir(group_id)
        topics_db = path_manager.get_topics_db_path(group_id)
        files_db = path_manager.get_files_db_path(group_id)

        # 删除话题数据库
        try:
            if os.path.exists(topics_db):
                os.remove(topics_db)
                details["topics_db_removed"] = True
                print(f"🗑️ 已删除话题数据库: {topics_db}")
        except PermissionError as pe:
            raise HTTPException(status_code=500, detail=f"话题数据库被占用，无法删除: {pe}")
        except Exception as e:
            print(f"⚠️ 删除话题数据库失败: {e}")

        # 删除文件数据库
        try:
            if os.path.exists(files_db):
                os.remove(files_db)
                details["files_db_removed"] = True
                print(f"🗑️ 已删除文件数据库: {files_db}")
        except PermissionError as pe:
            raise HTTPException(status_code=500, detail=f"文件数据库被占用，无法删除: {pe}")
        except Exception as e:
            print(f"⚠️ 删除文件数据库失败: {e}")

        # 删除下载目录
        downloads_dir = os.path.join(group_dir, "downloads")
        if os.path.exists(downloads_dir):
            try:
                shutil.rmtree(downloads_dir, ignore_errors=False)
                details["downloads_dir_removed"] = True
                print(f"🗑️ 已删除下载目录: {downloads_dir}")
            except Exception as e:
                print(f"⚠️ 删除下载目录失败: {e}")

        # 清空并删除图片缓存目录，同时释放缓存管理器
        try:
            from app.runtime.image_cache_manager import get_image_cache_manager, clear_group_cache_manager
            cache_manager = get_image_cache_manager(group_id)
            ok, msg = cache_manager.clear_cache()
            if ok:
                details["images_cache_removed"] = True
                print(f"🗑️ 图片缓存清空: {msg}")
            images_dir = os.path.join(group_dir, "images")
            if os.path.exists(images_dir):
                try:
                    shutil.rmtree(images_dir, ignore_errors=True)
                    print(f"🗑️ 已删除图片缓存目录: {images_dir}")
                except Exception as e:
                    print(f"⚠️ 删除图片缓存目录失败: {e}")
            clear_group_cache_manager(group_id)
        except Exception as e:
            print(f"⚠️ 清理图片缓存失败: {e}")

        # 若群组目录已空，则删除该目录
        try:
            if os.path.exists(group_dir) and len(os.listdir(group_dir)) == 0:
                os.rmdir(group_dir)
                details["group_dir_removed"] = True
                print(f"🗑️ 已删除空群组目录: {group_dir}")
        except Exception as e:
            print(f"⚠️ 删除群组目录失败: {e}")

        # 更新本地群缓存（从缓存集合移除）
        try:
            gid_int = int(group_id)
            if gid_int in _local_groups_cache.get("ids", set()):
                _local_groups_cache["ids"].discard(gid_int)
                _local_groups_cache["scanned_at"] = time.time()
        except Exception as e:
            print(f"⚠️ 更新本地群缓存失败: {e}")

        any_removed = any(details.values())
        return {
            "success": True,
            "message": f"群组 {group_id} 本地数据" + ("已删除" if any_removed else "不存在"),
            "details": details,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除群组本地数据失败: {str(e)}")


# =========================
# 专栏相关 API
# =========================

# migrated to api/routers/columns.py: columns domain legacy implementation removed

# ========== 股票舆情分析 API ==========

from modules.analyzers.stock_analyzer import StockAnalyzer


# migrated to api/routers/stocks.py: @app.post("/api/groups/{group_id}/stock/scan")
def scan_group_stocks(group_id: str, background_tasks: BackgroundTasks, force: bool = False):
    """扫描群组帖子，提取股票提及并计算后续表现（后台任务）"""
    task_id = create_task(f"stock_scan_{group_id}", f"股票提及扫描: {group_id}")

    def _scan_task():
        try:
            update_task(task_id, "running", "正在扫描...")
            add_task_log(task_id, "🚀 开始股票提及扫描...")
            add_task_log(task_id, "🧭 分析引擎版本: dict-log-v2")
            update_task(task_id, "running", "正在准备股票字典...")

            def _log_progress(msg: str):
                add_task_log(task_id, msg)
                # 将关键进度同步到任务摘要，便于前端侧边栏展示
                if any(k in msg for k in ["开始扫描", "已扫描", "开始计算", "已计算", "扫描完成", "全部完成"]):
                    update_task(task_id, "running", msg)

            analyzer = StockAnalyzer(group_id, log_callback=_log_progress)
            result = analyzer.scan_group(force=force)

            add_task_log(task_id, f"✅ 扫描完成: {result['mentions_extracted']} 次提及, {result['unique_stocks']} 只股票")
            update_task(task_id, "completed",
                        f"完成: {result['topics_scanned']} 帖子, {result['mentions_extracted']} 次提及, "
                        f"{result['unique_stocks']} 只股票, {result['performance_calculated']} 条表现计算")
        except Exception as e:
            add_task_log(task_id, f"❌ 扫描失败: {e}")
            update_task(task_id, "failed", f"扫描失败: {e}")

    background_tasks.add_task(_scan_task)
    return {"task_id": task_id, "message": "股票扫描任务已启动"}


# ========== 全局看板 API ==========


def _parse_global_crawl_time(raw: Optional[str], field_name: str) -> Optional[datetime]:
    """解析并校验全区 range 模式时间参数。"""
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        # 兼容 datetime-local（无秒）
        if "T" in text and len(text) == 16:
            text = text + ":00"
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        if len(text) >= 24 and text[-5] in ["+", "-"] and text[-3] != ":":
            text = text[:-2] + ":" + text[-2:]
        dt = datetime.fromisoformat(text)
        return dt
    except Exception:
        raise HTTPException(
            status_code=422,
            detail=f"{field_name} 格式无效，请使用 ISO8601（例如 2026-02-21T10:00:00+08:00）",
        )


def api_global_crawl(request: GlobalCrawlRequest, background_tasks: BackgroundTasks):
    """全区话题采集（轮询所有群组）"""
    if request.mode == "range":
        has_last_days = request.last_days is not None
        has_time_range = bool((request.start_time or "").strip() or (request.end_time or "").strip())
        if has_last_days and int(request.last_days) < 1:
            raise HTTPException(status_code=422, detail="last_days 必须大于 0")
        if has_last_days and has_time_range:
            raise HTTPException(
                status_code=422,
                detail="range 模式下，“最近天数(last_days)”与“开始/结束时间(start_time/end_time)”必须二选一",
            )
        if request.start_time:
            _parse_global_crawl_time(request.start_time, "start_time")
        if request.end_time:
            _parse_global_crawl_time(request.end_time, "end_time")

    global task_counter
    task_counter += 1
    task_id = f"global_crawl_{task_counter}"
    
    current_tasks[task_id] = {
        "task_id": task_id,
        "type": "global_crawl",
        "status": "running",
        "message": "正在初始化全区话题采集...",
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "result": None
    }
    task_logs[task_id] = []
    task_stop_flags[task_id] = False

    def _global_crawl_task(task_id: str):
        from api.services.global_crawl_service import GlobalCrawlService

        GlobalCrawlService().run(
            task_id=task_id,
            request=request,
            add_task_log=add_task_log,
            update_task=update_task,
            is_task_stopped=is_task_stopped,
            get_cookie_for_group=get_cookie_for_group,
        )

    background_tasks.add_task(_global_crawl_task, task_id)
    return {"task_id": task_id, "message": "全区采集任务已启动"}

def api_global_files_collect(request: GlobalFileCollectRequest, background_tasks: BackgroundTasks):
    """全区文件列表收集"""
    global task_counter
    task_counter += 1
    task_id = f"global_files_collect_{task_counter}"
    
    current_tasks[task_id] = {
        "task_id": task_id,
        "type": "global_files_collect",
        "status": "running",
        "message": "正在初始化全区文件列表收集...",
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "result": None
    }
    task_logs[task_id] = []
    task_stop_flags[task_id] = False

    def _global_collect_task(task_id: str):
        from api.services.global_file_task_service import GlobalFileTaskService

        GlobalFileTaskService().run_collect(
            task_id=task_id,
            add_task_log=add_task_log,
            update_task=update_task,
            is_task_stopped=is_task_stopped,
            get_cookie_for_group=get_cookie_for_group,
            file_downloader_instances=file_downloader_instances,
        )

    background_tasks.add_task(_global_collect_task, task_id)
    return {"task_id": task_id, "message": "全区收集任务已启动"}

def api_global_files_download(request: GlobalFileDownloadRequest, background_tasks: BackgroundTasks):
    """全区文件下载"""
    # 我们可以复用 run_file_download_task_logic
    global task_counter
    task_counter += 1
    task_id = f"global_files_download_{task_counter}"
    
    current_tasks[task_id] = {
        "task_id": task_id,
        "type": "global_files_download",
        "status": "running",
        "message": "正在初始化全区文件下载...",
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "result": None
    }
    task_logs[task_id] = []
    task_stop_flags[task_id] = False

    def _global_download_task(task_id: str):
        from api.services.global_file_task_service import GlobalFileTaskService

        GlobalFileTaskService().run_download(
            task_id=task_id,
            request=request,
            add_task_log=add_task_log,
            update_task=update_task,
            is_task_stopped=is_task_stopped,
            get_cookie_for_group=get_cookie_for_group,
            file_downloader_instances=file_downloader_instances,
        )

    background_tasks.add_task(_global_download_task, task_id)
    return {"task_id": task_id, "message": "全区下载任务已启动"}

def api_global_analyze_performance(background_tasks: BackgroundTasks, force: bool = False):
    """全区收益刷新"""
    global task_counter
    task_counter += 1
    task_id = f"global_analyze_performance_{task_counter}"
    
    current_tasks[task_id] = {
        "task_id": task_id,
        "type": "global_analyze_performance",
        "status": "running",
        "message": "正在初始化全区收益计算...",
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "result": None
    }
    task_logs[task_id] = []
    task_stop_flags[task_id] = False

    def _global_analyze_task(task_id: str):
        from api.services.global_analyze_service import GlobalAnalyzePerformanceService

        GlobalAnalyzePerformanceService().run(
            task_id=task_id,
            add_task_log=add_task_log,
            update_task=update_task,
            is_task_stopped=is_task_stopped,
            calc_window_days=365,
        )

    background_tasks.add_task(_global_analyze_task, task_id)
    return {"task_id": task_id, "message": "全区计算任务已启动"}

async def cleanup_excluded_stocks(scope: str = "all", group_id: Optional[str] = None):
    """清理被 stock_exclude.json 命中的历史股票数据"""
    try:
        from modules.shared.stock_exclusion import build_sql_exclusion_clause
        from modules.shared.db_path_manager import get_db_path_manager

        if scope not in ("all", "group"):
            raise HTTPException(status_code=400, detail="scope 仅支持 all 或 group")
        if scope == "group" and not group_id:
            raise HTTPException(status_code=400, detail="scope=group 时必须提供 group_id")

        manager = get_db_path_manager()
        groups = manager.list_all_groups()
        if scope == "group":
            groups = [g for g in groups if str(g.get("group_id")) == str(group_id)]

        exclude_clause, exclude_params = build_sql_exclusion_clause("stock_code", "stock_name")
        if not exclude_clause:
            return {
                "groups_processed": 0,
                "mentions_deleted": 0,
                "performances_deleted": 0,
                "details": [],
                "message": "未配置排除规则，无需清理"
            }

        total_mentions_deleted = 0
        total_perf_deleted = 0
        details: List[Dict[str, Any]] = []

        for group in groups:
            gid = str(group.get("group_id"))
            db_path = group.get("topics_db")
            if not db_path or not os.path.exists(db_path):
                continue

            mentions_deleted = 0
            perf_deleted = 0
            conn = None
            try:
                import sqlite3
                conn = sqlite3.connect(db_path, timeout=30)
                cursor = conn.cursor()

                cursor.execute('''
                    SELECT 1 FROM sqlite_master
                    WHERE type = 'table' AND name = 'stock_mentions'
                ''')
                if cursor.fetchone() is None:
                    continue

                cursor.execute(
                    f"SELECT id FROM stock_mentions WHERE NOT (1=1 {exclude_clause})",
                    exclude_params
                )
                mention_ids = [row[0] for row in cursor.fetchall()]

                if mention_ids:
                    placeholders = ",".join(["?"] * len(mention_ids))
                    cursor.execute(
                        f"DELETE FROM mention_performance WHERE mention_id IN ({placeholders})",
                        mention_ids
                    )
                    perf_deleted = cursor.rowcount or 0

                    cursor.execute(
                        f"DELETE FROM stock_mentions WHERE id IN ({placeholders})",
                        mention_ids
                    )
                    mentions_deleted = cursor.rowcount or 0

                conn.commit()
            except Exception as e:
                if conn:
                    conn.rollback()
                details.append({
                    "group_id": gid,
                    "mentions_deleted": 0,
                    "performances_deleted": 0,
                    "error": str(e)
                })
                continue
            finally:
                if conn:
                    conn.close()

            total_mentions_deleted += mentions_deleted
            total_perf_deleted += perf_deleted
            details.append({
                "group_id": gid,
                "mentions_deleted": mentions_deleted,
                "performances_deleted": perf_deleted
            })

        try:
            from modules.analyzers.global_analyzer import get_global_analyzer
            get_global_analyzer().invalidate_cache()
        except Exception:
            pass

        return {
            "groups_processed": len(details),
            "mentions_deleted": total_mentions_deleted,
            "performances_deleted": total_perf_deleted,
            "details": details
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"清理排除股票失败: {str(e)}")


# migrated to api/routers/global_tasks.py: @app.get("/api/global/scan-filter/config")
async def get_global_scan_filter_config():
    """获取非股票群排除规则（手动白黑名单）"""
    try:
        from modules.shared.group_scan_filter import get_filter_config, CONFIG_FILE
        data = get_filter_config()
        data["source_file"] = CONFIG_FILE
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取扫描过滤配置失败: {str(e)}")


# migrated to api/routers/global_tasks.py: @app.put("/api/global/scan-filter/config")
async def update_global_scan_filter_config(request: ScanFilterConfigRequest):
    """更新非股票群排除规则（手动白黑名单）"""
    try:
        from modules.shared.group_scan_filter import save_filter_config
        data = save_filter_config(
            default_action=request.default_action,
            whitelist_group_ids=request.whitelist_group_ids,
            blacklist_group_ids=request.blacklist_group_ids
        )
        return {
            **data,
            "effective_counts": {
                "whitelist": len(data.get("whitelist_group_ids", [])),
                "blacklist": len(data.get("blacklist_group_ids", [])),
            }
        }
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新扫描过滤配置失败: {str(e)}")


# migrated to api/routers/global_tasks.py: @app.get("/api/global/scan-filter/preview")
async def preview_global_scan_filter(exclude_non_stock: bool = True):
    """预览当前扫描过滤命中结果"""
    try:
        from modules.shared.db_path_manager import get_db_path_manager
        from modules.shared.group_scan_filter import decide_group

        manager = get_db_path_manager()
        groups = manager.list_all_groups()

        included_groups = []
        excluded_groups = []
        reason_counts: Dict[str, int] = {}

        for g in groups:
            gid = str(g.get("group_id"))
            gname = _get_group_name_for_scan_filter(gid, g.get("topics_db"))
            decision, reason = decide_group(gid)

            item = {
                "group_id": gid,
                "group_name": gname or gid,
                "decision": decision,
                "reason": reason,
            }
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
            if decision == "included":
                included_groups.append(item)
            else:
                excluded_groups.append(item)

        return {
            "total_groups": len(groups),
            "included_groups": included_groups,
            "excluded_groups": excluded_groups,
            "reason_counts": reason_counts,
            "compat_note": (
                "exclude_non_stock 参数已兼容保留，当前版本始终应用白黑名单规则"
                if exclude_non_stock is False else None
            ),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"预览扫描过滤结果失败: {str(e)}")


# migrated to api/routers/global_tasks.py: @app.get("/api/global/scan-filter/cleanup-blacklist/preview")
async def preview_blacklist_cleanup():
    """预览黑名单群组可清理的分析数据规模。"""
    try:
        from modules.shared.db_path_manager import get_db_path_manager
        from modules.shared.group_scan_filter import get_filter_config
        import sqlite3

        cfg = get_filter_config()
        blacklist_ids = set(str(v).strip() for v in cfg.get("blacklist_group_ids", []) if str(v).strip())
        manager = get_db_path_manager()
        groups = manager.list_all_groups()

        details = []
        total_mentions = 0
        total_performance = 0

        for g in groups:
            gid = str(g.get("group_id", "")).strip()
            if not gid or gid not in blacklist_ids:
                continue

            db_path = g.get("topics_db")
            mentions_count = 0
            perf_count = 0
            if db_path and os.path.exists(db_path):
                conn = None
                try:
                    conn = sqlite3.connect(db_path, timeout=30)
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='stock_mentions'")
                    if (cursor.fetchone() or [0])[0]:
                        cursor.execute("SELECT COUNT(*) FROM stock_mentions")
                        mentions_count = int((cursor.fetchone() or [0])[0] or 0)
                        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='mention_performance'")
                        if (cursor.fetchone() or [0])[0]:
                            cursor.execute("SELECT COUNT(*) FROM mention_performance")
                            perf_count = int((cursor.fetchone() or [0])[0] or 0)
                except Exception:
                    pass
                finally:
                    if conn:
                        conn.close()

            total_mentions += mentions_count
            total_performance += perf_count
            details.append({
                "group_id": gid,
                "group_name": _get_group_name_for_scan_filter(gid, db_path),
                "stock_mentions_count": mentions_count,
                "mention_performance_count": perf_count,
            })

        return {
            "blacklist_group_count": len(blacklist_ids),
            "matched_group_count": len(details),
            "total_stock_mentions": total_mentions,
            "total_mention_performance": total_performance,
            "groups": details,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"预览黑名单清理失败: {str(e)}")


async def cleanup_blacklist_data(background_tasks: BackgroundTasks):
    """清理黑名单群组中的分析数据（stock_mentions / mention_performance）。"""
    global task_counter
    task_counter += 1
    task_id = f"global_cleanup_blacklist_{task_counter}"

    current_tasks[task_id] = {
        "task_id": task_id,
        "type": "global_cleanup_blacklist",
        "status": "running",
        "message": "正在初始化黑名单数据清理...",
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "result": None,
    }
    task_logs[task_id] = []
    task_stop_flags[task_id] = False

    def _cleanup_task(task_id: str):
        try:
            from modules.shared.db_path_manager import get_db_path_manager
            from modules.shared.group_scan_filter import get_filter_config
            import sqlite3

            update_task(task_id, "running", "开始清理黑名单历史分析数据...")
            cfg = get_filter_config()
            blacklist_ids = set(str(v).strip() for v in cfg.get("blacklist_group_ids", []) if str(v).strip())
            manager = get_db_path_manager()
            groups = manager.list_all_groups()
            target_groups = [g for g in groups if str(g.get("group_id", "")).strip() in blacklist_ids]

            add_task_log(task_id, f"📋 黑名单群组总数: {len(blacklist_ids)}，本地匹配: {len(target_groups)}")
            if not target_groups:
                update_task(task_id, "completed", "黑名单清理完成: 无匹配本地群组")
                return

            total_mentions_deleted = 0
            total_perf_deleted = 0
            processed = 0

            for i, g in enumerate(target_groups, 1):
                if is_task_stopped(task_id):
                    add_task_log(task_id, "🛑 清理任务已停止")
                    break

                gid = str(g.get("group_id", "")).strip()
                db_path = g.get("topics_db")
                add_task_log(task_id, f"👉 [{i}/{len(target_groups)}] 清理群组 {gid}")

                if not db_path or not os.path.exists(db_path):
                    add_task_log(task_id, f"   ⚠️ 群组 {gid} 无可用 topics_db，跳过")
                    continue

                conn = None
                try:
                    conn = sqlite3.connect(db_path, timeout=30)
                    cursor = conn.cursor()

                    cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='stock_mentions'")
                    has_mentions = bool((cursor.fetchone() or [0])[0])
                    cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='mention_performance'")
                    has_perf = bool((cursor.fetchone() or [0])[0])
                    if not has_mentions:
                        add_task_log(task_id, f"   ℹ️ 群组 {gid} 无 stock_mentions，跳过")
                        continue

                    perf_deleted = 0
                    if has_perf:
                        cursor.execute(
                            "DELETE FROM mention_performance WHERE mention_id IN (SELECT id FROM stock_mentions)"
                        )
                        perf_deleted = cursor.rowcount or 0

                    cursor.execute("DELETE FROM stock_mentions")
                    mentions_deleted = cursor.rowcount or 0
                    conn.commit()

                    total_perf_deleted += perf_deleted
                    total_mentions_deleted += mentions_deleted
                    processed += 1
                    add_task_log(task_id, f"   ✅ 完成: 删除提及 {mentions_deleted}，收益 {perf_deleted}")
                except Exception as e:
                    if conn:
                        conn.rollback()
                    add_task_log(task_id, f"   ❌ 清理失败: {e}")
                finally:
                    if conn:
                        conn.close()

            try:
                from modules.analyzers.global_analyzer import get_global_analyzer
                get_global_analyzer().invalidate_cache()
                add_task_log(task_id, "🔄 全局统计缓存已刷新")
            except Exception:
                pass

            if is_task_stopped(task_id):
                update_task(task_id, "cancelled", "黑名单清理已停止")
            else:
                update_task(
                    task_id,
                    "completed",
                    f"黑名单清理完成: {processed}/{len(target_groups)} 个群组，删除提及 {total_mentions_deleted}，收益 {total_perf_deleted}",
                    {
                        "groups_processed": processed,
                        "groups_total": len(target_groups),
                        "mentions_deleted": total_mentions_deleted,
                        "performances_deleted": total_perf_deleted,
                    },
                )
        except Exception as e:
            add_task_log(task_id, f"❌ 黑名单清理异常: {e}")
            update_task(task_id, "failed", f"黑名单清理失败: {e}")

    background_tasks.add_task(_cleanup_task, task_id)
    return {"task_id": task_id, "message": "黑名单清理任务已启动"}


STOCK_GROUP_HINT_KEYWORDS = (
    "股票", "a股", "港股", "美股", "基金", "投资", "交易", "复盘", "量化", "财经", "证券", "研报", "择时"
)


def _get_group_name_for_scan_filter(group_id: str, topics_db_path: Optional[str]) -> str:
    """尽量获取群组名称（本地DB -> group_meta.json）"""
    import sqlite3
    from pathlib import Path

    if topics_db_path and os.path.exists(topics_db_path):
        try:
            conn = sqlite3.connect(topics_db_path, timeout=10)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM groups WHERE group_id = ? LIMIT 1", (int(group_id),))
            row = cursor.fetchone()
            conn.close()
            if row and row[0]:
                return str(row[0])
        except Exception:
            pass

    try:
        path_manager = get_db_path_manager()
        group_dir = path_manager.get_group_data_dir(str(group_id))
        meta_path = Path(group_dir) / "group_meta.json"
        if meta_path.exists():
            with meta_path.open("r", encoding="utf-8") as f:
                meta = json.load(f)
            if meta.get("name"):
                return str(meta["name"])
    except Exception:
        pass

    return ""


def _group_has_stock_mentions_for_scan_filter(topics_db_path: Optional[str]) -> bool:
    """判断群组本地库是否已有股票提及记录。"""
    import sqlite3

    if not topics_db_path or not os.path.exists(topics_db_path):
        return False

    try:
        conn = sqlite3.connect(topics_db_path, timeout=10)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'stock_mentions'")
        has_table = cursor.fetchone() is not None
        if not has_table:
            conn.close()
            return False

        cursor.execute("SELECT 1 FROM stock_mentions LIMIT 1")
        has_mentions = cursor.fetchone() is not None
        conn.close()
        return has_mentions
    except Exception:
        return False


def _is_stock_candidate_group_for_scan(group: Dict[str, Any]):
    """扫描过滤规则：历史有股票提及，或群名包含股票关键词。"""
    group_id = str(group.get("group_id", ""))
    topics_db_path = group.get("topics_db")
    group_name = _get_group_name_for_scan_filter(group_id, topics_db_path)
    normalized_name = group_name.lower()

    name_hit = any(keyword in normalized_name for keyword in STOCK_GROUP_HINT_KEYWORDS)
    mentions_hit = _group_has_stock_mentions_for_scan_filter(topics_db_path)

    if mentions_hit:
        return True, "已有股票提及"
    if name_hit:
        return True, "群名命中关键词"
    return False, "无提及且群名未命中"


def scan_global(background_tasks: BackgroundTasks, force: bool = False, exclude_non_stock: bool = False):
    """全局扫描所有群组的股票数据（后台任务）"""
    global task_counter
    task_counter += 1
    task_id = f"global_scan_{task_counter}"
    
    current_tasks[task_id] = {
        "task_id": task_id,
        "type": "global_scan",
        "status": "running",
        "message": "正在初始化全局扫描...",
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "result": None
    }
    task_logs[task_id] = []
    task_stop_flags[task_id] = False

    def _global_scan_task(task_id: str):
        try:
            update_task(task_id, "running", "准备开始全局扫描...")
            add_task_log(task_id, "🚀 开始全局股票提及扫描...")
            
            from modules.shared.db_path_manager import get_db_path_manager
            from modules.analyzers.global_pipeline import run_serial_incremental_pipeline
            
            manager = get_db_path_manager()
            groups = manager.list_all_groups()
            original_count = len(groups)
            add_task_log(task_id, f"📋 共发现 {original_count} 个群组")
            if force:
                add_task_log(task_id, "ℹ️ 当前全局扫描的编排模式不区分 force，按增量采集执行")
            if exclude_non_stock is False:
                add_task_log(task_id, "ℹ️ 参数 exclude_non_stock 已兼容保留，当前版本始终强制应用白黑名单规则")

            from api.services.group_filter_service import apply_group_scan_filter

            filtered = apply_group_scan_filter(groups)
            groups = filtered["included_groups"]
            excluded_groups = filtered["excluded_groups"]
            reason_counts = filtered["reason_counts"]
            default_action = filtered["default_action"]
            add_task_log(task_id, f"⚙️ 过滤策略: 未配置群组默认{'纳入' if default_action == 'include' else '排除'}")
            add_task_log(task_id, f"🧹 白黑名单过滤后：保留 {len(groups)}/{original_count} 个群组")
            if reason_counts:
                add_task_log(task_id, f"📌 命中统计: {reason_counts}")
            if excluded_groups:
                preview = "，".join(
                    f"{g.get('group_id')}({g.get('scan_filter_reason', 'unknown')})"
                    for g in excluded_groups[:20]
                )
                suffix = " ..." if len(excluded_groups) > 20 else ""
                add_task_log(task_id, f"🚫 已排除: {preview}{suffix}")

            if not groups:
                add_task_log(task_id, "ℹ️ 过滤后无可扫描群组，任务结束")
                update_task(task_id, "completed", "全局扫描完成: 过滤后无可扫描群组")
                return

            successes, failures = run_serial_incremental_pipeline(
                groups=groups,
                pages=2,
                per_page=20,
                calc_window_days=365,
                do_analysis=False,
                stop_check=lambda: is_task_stopped(task_id),
                log_callback=lambda msg: add_task_log(task_id, msg),
            )
            total_mentions = sum((item.get("extract") or {}).get("mentions_extracted", 0) for item in successes)

            if is_task_stopped(task_id):
                update_task(task_id, "cancelled", "全局扫描已停止")
            else:
                add_task_log(task_id, "")
                add_task_log(task_id, "=" * 50)
                add_task_log(task_id, f"🎉 全局扫描完成！共处理 {len(successes)}/{len(groups)} 个群组")
                add_task_log(task_id, f"📊 本次累计提取提及: {total_mentions} 次")
                if failures:
                    add_task_log(task_id, f"⚠️ 失败群组: {len(failures)} 个")
                
                # 触发全局分析器缓存失效
                try:
                    from modules.analyzers.global_analyzer import get_global_analyzer
                    get_global_analyzer().invalidate_cache()
                    add_task_log(task_id, "🔄 全局统计缓存已刷新")
                except:
                    pass
                
                update_task(task_id, "completed", f"全局扫描完成: {len(successes)} 个群组, {total_mentions} 次提及")

        except Exception as e:
            add_task_log(task_id, f"❌ 全局扫描异常: {e}")
            update_task(task_id, "failed", f"全局扫描失败: {e}")

    background_tasks.add_task(_global_scan_task, task_id)
    return {"task_id": task_id, "message": "全局扫描任务已启动"}


if __name__ == "__main__":
    import sys
    port = 8208  # 默认端口
    if len(sys.argv) > 2 and sys.argv[1] == "--port":
        try:
            port = int(sys.argv[2])
        except ValueError:
            port = 8208
    print(f"[startup] API version=1.0.0, port={port}")
    print("[startup] feature routes: /api/global/sector-topics, /api/scheduler/next-runs, /api/meta/features")
    uvicorn.run(app, host="0.0.0.0", port=port)

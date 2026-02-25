# Routing Map

路径归属映射（持续更新）。

## Core Routers

| Path Prefix / Route | Router File | Notes |
|---|---|---|
| `/api/scheduler/*` | `api/routers/scheduler.py` | 调度器接口 |
| `/api/groups/{group_id}/stock/*` | `api/routers/stocks.py` | 股票分析接口 |
| `/api/ai/config` | `api/routers/stocks.py` | AI 配置接口 |
| `/api/global/stats` 等只读 global 接口 | `api/routers/global_read.py` | 全局看板读取 |
| `/api/tasks/*` | `api/routers/tasks.py` | 任务查询/停止/日志/SSE |
| `/api/global/crawl` 等任务型 global 接口 | `api/routers/global_tasks.py` | 全局任务编排 |
| `/api/database/stats` | `api/routers/groups.py` | 数据库聚合统计 |
| `/api/global/scan-filter/*` | `api/routers/global_tasks.py` | 全局扫描过滤配置与预览 |
| `/api/crawl/*` | `api/routers/crawl.py` | 话题抓取任务编排 |
| `/api/settings/crawl` | `api/routers/settings.py` | 抓取行为设置 |
| `/api/settings/crawler` | `api/routers/settings.py` | 运行时爬虫参数设置 |
| `/api/settings/downloader` | `api/routers/settings.py` | 运行时下载器参数设置 |
| `/api/files/*` | `api/routers/files.py` | 文件收集/下载/状态/清理 |
| `/api/groups/{group_id}/columns*` | `api/routers/columns.py` | 专栏读取/统计/清理/评论 |
| `/api/topics*` | `api/routers/topics.py` | 话题读取/刷新/删除/单话题补采 |
| `/api/groups/{group_id}/topics` | `api/routers/topics.py` | 群组话题列表与批量清理 |
| `/api/groups/{group_id}/tags*` | `api/routers/topics.py` | 标签及标签话题读取 |
| `/api/groups/{group_id}/stats` | `api/routers/topics.py` | 群组话题统计 |
| `/api/groups/{group_id}/database-info` | `api/routers/topics.py` | 群组数据库信息 |
| `/api/proxy-image` 等媒体接口 | `api/routers/media.py` | 图片代理/缓存/本地媒体读取 |
| `/api/accounts*` 与 `groups/*account*` | `api/routers/accounts.py` | 账号管理、账号自信息、群组账号绑定 |

已完成从 `app/main.py` 下沉到 `api/services/global_task_service.py` 的接口：

- `/api/global/crawl`
- `/api/global/files/collect`
- `/api/global/files/download`
- `/api/global/analyze/performance`
- `/api/stocks/exclude/cleanup`
- `/api/global/scan-filter/cleanup-blacklist`
- `/api/global/scan`
- `/api/global/scan-filter/config`
- `/api/global/scan-filter/preview`
- `/api/global/scan-filter/cleanup-blacklist/preview`
- `/api/groups/{group_id}/stock/scan`
- `/api/database/stats`
- `/api/settings/crawl`
- `/api/crawl/range/{group_id}`
- `/api/crawl/latest-until-complete/{group_id}`
- `/api/crawl/incremental/{group_id}`
- `/api/crawl/all/{group_id}`
- `/api/crawl/historical/{group_id}`
- `/api/groups/{group_id}/columns`
- `/api/groups/{group_id}/columns/{column_id}/topics`
- `/api/groups/{group_id}/columns/topics/{topic_id}`
- `/api/groups/{group_id}/columns/summary`
- `/api/groups/{group_id}/columns/stats`
- `/api/groups/{group_id}/columns/all`
- `/api/groups/{group_id}/columns/topics/{topic_id}/comments`
- `/api/groups/{group_id}/columns/fetch`
- `/api/files/collect/{group_id}`
- `/api/files/download/{group_id}`
- `/api/files/download-single/{group_id}/{file_id}`
- `/api/files/status/{group_id}/{file_id}`
- `/api/files/check-local/{group_id}`
- `/api/files/stats/{group_id}`
- `/api/files/clear/{group_id}`
- `/api/files/{group_id}`
- `/api/topics/clear/{group_id}`
- `/api/topics`
- `/api/topics/{topic_id}/{group_id}`
- `/api/topics/{topic_id}/{group_id}/refresh`
- `/api/topics/{topic_id}/{group_id}/fetch-comments`
- `/api/topics/{topic_id}/{group_id}`
- `/api/topics/fetch-single/{group_id}/{topic_id}`
- `/api/groups/{group_id}/tags`
- `/api/groups/{group_id}/tags/{tag_id}/topics`
- `/api/groups/{group_id}/topics`
- `/api/groups/{group_id}/stats`
- `/api/groups/{group_id}/database-info`
- `/api/groups/{group_id}/topics`
- `/api/proxy-image`
- `/api/cache/images/info/{group_id}`
- `/api/cache/images/{group_id}`
- `/api/groups/{group_id}/images/{image_path:path}`
- `/api/groups/{group_id}/videos/{video_path:path}`
- `/api/proxy/image`
- `/api/settings/crawler`
- `/api/settings/downloader`
- `/api/accounts`
- `/api/accounts/{account_id}`
- `/api/groups/{group_id}/assign-account`
- `/api/groups/{group_id}/account`
- `/api/accounts/{account_id}/self`
- `/api/accounts/{account_id}/self/refresh`
- `/api/groups/{group_id}/self`
- `/api/groups/{group_id}/self/refresh`

## Legacy In Main (to be migrated)

以下路径仍在 `app/main.py` 直接定义，按域分批迁移：

- groups domain: `/api/groups*`
- settings domain: `/api/settings/*`

## Migration Rule

1. 新 router 先保持路径与参数不变。
2. 迁移后移除 `app/main.py` 对应 `@app.*` 装饰器，防止重复注册。
3. 逻辑搬迁到 service 后更新本表。

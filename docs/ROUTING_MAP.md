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

已完成从 `app/main.py` 下沉到 `api/services/global_task_service.py` 的接口：

- `/api/global/crawl`
- `/api/global/files/collect`
- `/api/global/files/download`
- `/api/global/analyze/performance`
- `/api/stocks/exclude/cleanup`
- `/api/global/scan-filter/cleanup-blacklist`
- `/api/global/scan`

## Legacy In Main (to be migrated)

以下路径仍在 `app/main.py` 直接定义，按域分批迁移：

- crawl domain: `/api/crawl/*`
- file domain: `/api/files/*`
- topics domain: `/api/topics*`
- groups/account domain: `/api/groups*`, `/api/accounts*`
- columns domain: `/api/groups/{group_id}/columns*`
- settings domain: `/api/settings/*`

## Migration Rule

1. 新 router 先保持路径与参数不变。
2. 迁移后移除 `app/main.py` 对应 `@app.*` 装饰器，防止重复注册。
3. 逻辑搬迁到 service 后更新本表。

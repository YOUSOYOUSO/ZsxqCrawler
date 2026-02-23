# ZsxqCrawler 架构执行手册

## 1. 目标与边界

本次重构目标：

- 保持既有 API 路径与参数 **100% 兼容**
- 将 `main.py` 从“路由+编排+状态存储”转为“入口+装配”
- 以 `api/routers` + `api/services` + `api/schemas` 建立稳定分层
- 将任务状态统一收敛到 `TaskRuntime`

非目标：

- 不在本阶段改动业务算法（抓取、分析、收益计算逻辑）
- 不在本阶段引入新 URL 版本（如 `/api/v2`）

---

## 2. 分层定义（必须遵守）

### 2.1 Router 层（`api/routers`）
- 负责：HTTP 入参、响应、错误码、调用服务
- 禁止：直接持有跨路由共享状态；禁止堆积长任务流程细节

### 2.2 Service 层（`api/services`）
- 负责：任务编排、状态流转、跨模块协作
- 当前关键服务：
  - `task_runtime.py`: 统一任务状态/日志/停止标记
  - 其它服务文件作为后续拆分落点（crawl/file/group/columns/global task）

### 2.3 Schema 层（`api/schemas`）
- 负责：Pydantic 模型与域模型定义
- 规则：请求/响应模型不在 router 中重复定义

### 2.4 入口层（`main.py` + `api/app_factory.py`）
- `main.py`：应用启动、生命周期、历史兼容逻辑
- `api/app_factory.py`：统一路由注册

---

## 3. 当前目录基线

```text
api/
  app_factory.py
  deps/
    container.py
  routers/
    scheduler.py
    stocks.py
    global_read.py
    global_tasks.py
    tasks.py
    crawl.py
    files.py
    topics.py
    groups.py
    settings.py
    columns.py
  schemas/
    models.py
    task_models.py
    global_models.py
  services/
    task_runtime.py
    global_task_service.py
    group_filter_service.py
    global_crawl_service.py
    global_analyze_service.py
    global_file_task_service.py
    crawl_service.py
    file_service.py
    group_service.py
    columns_service.py
docs/
  ARCHITECTURE.md
  ROUTING_MAP.md
  ADR/
    ADR-0001-layering.md
scripts/
  maintenance/
main.py
```

---

## 4. 任务生命周期（统一模型）

状态集合：

- 运行态：`pending`, `running`, `stopping`
- 终态：`completed`, `failed`, `cancelled`, `stopped`, `idle`

生命周期：

1. `create_task` 创建任务并初始化日志/stop flag
2. 进入 `running`，持续 `append_log`
3. 收到停止请求后进入 `stopping/cancelled` 分支
4. 最终进入终态并固定 `result`

约束：

- 所有路由/后台任务通过 `TaskRuntime` 操作状态
- 不再新增散落的 `dict` 全局状态

---

## 5. 路由归属策略

- 全局只读看板：`api/routers/global_read.py`
- 全局任务型接口：`api/routers/global_tasks.py`
- 调度器：`api/routers/scheduler.py`
- 股票与单群 AI：`api/routers/stocks.py`
- 任务查询与日志：`api/routers/tasks.py`
- 其余域逐步从 `main.py` 迁移到对应 router（crawl/files/topics/groups/settings/columns）

详见 `docs/ROUTING_MAP.md`。

---

## 6. 迁移步骤（可执行）

### Step A：迁移前检查

1. `python3 -m py_compile main.py api/routers/*.py api/services/*.py api/schemas/*.py`
2. 记录 `main.py` 路由数（`rg "@app\\." main.py`）
3. 保存 API 冒烟结果（关键路径）

### Step B：迁移规则

1. 新 router 先做“代理迁移”（调用 legacy handler），确保不改行为
2. 再将编排逻辑下沉至 service，并替换代理
3. 完成后删除 `main.py` 对应 `@app.*` 装饰器

### Step C：验收

1. API 路径、query、body 兼容
2. 任务日志/SSE/stop 语义一致
3. 文档与目录一致

### Step D：回滚

1. 回滚单一 router 提交
2. 恢复 `main.py` 装饰器绑定
3. 保持 `TaskRuntime` 不回滚（除非出现状态一致性问题）

---

## 7. 风险清单与控制

1. 路由重复注册：迁移后必须移除 `main.py` 同路径装饰器
2. 任务状态不一致：只允许通过 `TaskRuntime` 改写
3. 循环导入：router 不做顶层 `import main` 业务依赖，必要时函数内延迟导入

---

## 8. 本阶段完成定义（DoD）

1. `api/app_factory.py` 成为统一路由注册入口
2. `TaskRuntime` 接管任务状态存储
3. `global_read / global_tasks / tasks / scheduler / stocks` 可独立维护
4. `README` 与本手册、`ROUTING_MAP`、`ADR` 相互可导航

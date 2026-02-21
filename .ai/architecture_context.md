# ZsxqCrawler 项目架构与 AI 上下文设计文档

本文档旨在梳理 ZsxqCrawler 项目的整体架构与核心业务逻辑，为不同的 AI Coding Models（例如 Gemini, Claude, GPT 等）提供稳定、清晰的全局上下文（Context）。同时，探讨了如何引入更先进的记忆与追踪管理方式（如 Beads 框架）来优化多轮次、长周期 AI 结对编程体验。

---

## 1. 项目概述
ZsxqCrawler 是一个全栈的“知识星球（ZSXQ）”数据采集与智能分析平台。系统能够执行定时抓取、文件下载、多维度多群组信息聚合，并通过可视化 Dashboard 直观呈现板块热度、股票胜率与词云等分析指标。

## 2. 技术栈架构
### 2.1 后端服务 (Python 3)
- **核心框架**: `FastAPI` 提供 RESTful API 和 SSE 推送；`uvicorn` 作为 ASGI 服务器。
- **爬虫引擎**: 基于 `requests` 重放请求的 `zsxq_interactive_crawler.py`，内置复杂的反爬虫防御伪装（Stealth Headers、随机延迟、长休眠机制）。
- **数据存储**: 基于 `SQLite`。创新性地采用了**一目标（群组）一数据库**的文件级分片存储方式，并通过配置 WAL（Write-Ahead Logging）模式支持极高频率的读写并发。
- **分析器组**:
  - `global_analyzer.py`: 跨数据库/多群组合并计算，支持全局胜率、板块热度和提及排名。
  - `stock_analyzer.py` & `ai_analyzer.py`: 完成针对自然语言的股票提取和 AI 语义层面的预处理。
- **系统调度与多账户**: `auto_scheduler.py` 支持按 cron 或规则触发批量调度；`accounts_sql_manager.py` 支持动态切换爬虫账号池（Cookie轮询）。

### 2.2 前端工程 (`/frontend`)
- **核心框架**: 基于 React 18 的 `Next.js 15` (App Router)。
- **样式与组件**: 原生使用 `TailwindCSS 4` 和轻量级无头 UI 库 `Radix UI`。
- **数据可视化**: `ECharts` 用于复杂的折线、热力图及热门词云 (`echarts-wordcloud`) 渲染。

---

## 3. 为 AI Agent 提供的核心协作准则 (Rules for AI)
任何接入本项目的 AI Coding Helper 应当遵守以下领域特定开发原则：

1. **数据库并发处理**: 任何新增对 `output/` 数据库或 `app_settings.json` 的 I/O 操作，必须先经过锁管理（`asyncio.Lock` 或依赖 `ZSXQDatabase` 的上下文管理器，防止 `database is locked` 错误）。
2. **前后端解耦边界**: API JSON 的字段名必须与 `BaseModel` (如 `TaskResponse`, `SectorStat`) 强一致。前端不要尝试绕过后端直连 SQLite。
3. **安全与静默设计**: 爬虫网络请求如果遇到 403 或验证码，务必处理异常，触发保护机制并让相应的 `Task` 进入 `failed` 状态，严禁无限重试导致宿主封号。

---

## 4. 更好的 AI 交互与持久化实现方式探讨 (例如 Beads)

随着项目复杂度的提升，传统的“每次打开对话附带一个包含海量内容的 `.md` 文件”这种做法将严重浪费 Context Window 的 Token，并容易让 AI “遗忘”细节或产生代码覆盖冲撞。为了让各种 Coding Model 实现更好的跨分支、跨周期协同，可以接入和优化以下方式：

### 4.1 Beads 框架 (本项目推荐)
你可能已经注意到项目结构中包含 `.beads` 目录。**Beads**（由 Steve Yegge 发明）非常适合解决此痛点：
- **本质机理**: 它是一个专门给 AI Coding Agent 用的轻量级 Git-backed 图状任务追踪及记忆网络。
- **相比于传统 Markdown 的优势**:
  1. **按需记忆检索**: Agent 不需要一上来就阅读这篇长文档。在解决特定 Issue 或模块时，Agent 可以对 `.beads/` 里的 JSONL 依赖链发起查询，精准加载当前需要的计划和报错记忆。
  2. **与 Git 状态同构**: 项目在切换分支处理特定 BUG 时，`.beads/` 会跟着当前代码 Git 树一起切换。这就避免了“任务记录和代码不匹配”的平行线噩梦。
  3. **降低冲突**: 推荐将 AI 生成的中间思路、代办事项都通过 Beads 工具流落盘，不同模型（如今天你用了 Claude 写 UI，明天让 Gemini 写 Python）可通过读取 Bead 节点实现“记忆交接”。

### 4.2 其他优化辅助建议
1. **MCP (Model Context Protocol)** 动态查库
   - **痛点**: 由于你的 SQLite 数据库是按群组动态生成的（存放在未追踪的 `output/`），每次改表结构都要给模型重复解释 Schema。
   - **解法**: 引入官方 SQLite MCP Server 并针对 `output/` 建立连接。未来 AI Model 可以自主去发 SQL 探索当前线上的数据字段，大幅减少“猜结构”的时间和重试次数。
2. **结构化 `.cursorrules` / `.windsurfrules`**
   - 将本项目常用的命令（如：`uv run main.py`, `npm run dev` 所在的目录）和禁忌行为（如：禁止使用 `cat` 命令替换长代码等）写入规范。
   - 这可以从“被动查阅”转变为大部分现代 IDE 的“内置强制规则”。

---
**版本**: `1.0.0`
**目的**: 为人类开发者与 AI (LLMs) 提供统一的代码空间和意图同频保证。

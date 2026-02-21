# ZsxqCrawler 项目设计文档

## 1. 项目概述

**ZsxqCrawler** 是一个全栈的“知识星球（ZSXQ）”数据采集、内容固化与智能分析平台。系统的核心价值在于：
1. **自动化采集**：支持按需、定时对目标星球话题、专栏、文件进行增量和全量拉取。
2. **股票与舆情分析**：内置 A 股字典同步与自然语言提取模块，能够自动从帖子中提取股票代码，并结合量化行情数据（AkShare）进行回测计算（T+1 至 T+250 收益与超额收益）。
3. **全局协同查询**：支持多群组之间的数据联合分析，提供全局胜率、板块热度和共识信号等高级数据资产。
4. **可视化大屏**：提供现代化的 Web 端 Dashboard 动态展示以上分析数据及爬虫运行状态。

---

## 2. 系统核心架构

系统采用清晰的前后端分离架构，后端主要职责是“网络爬虫+数据计算引擎”，前端主要职责是“数据可视化与任务下发”。

### 2.1 架构图概念模型
```mermaid
graph TD
    subgraph 外部依赖
        A[知识星球 API]
        B[AkShare A股行情]
    end

    subgraph 后端服务 (Python FastAPI)
        C(爬虫调度器 \n auto_scheduler)
        D(多账号轮询 \n accounts_manager)
        E[交互式爬虫核心 \n zsxq_crawler]
        F[股票清洗与计算引擎 \n stock_analyzer]
        G[全局/跨群组聚合查询 \n global_analyzer]
        H(FastAPI 接口层 \n main.py)
    end

    subgraph 数据存储层 (SQLite WAL)
        I[(群组 A DB)]
        J[(群组 B DB)]
        K[(全局配置或通用 DB)]
    end

    subgraph 前端控制台 (Next.js)
        L[Dashboard 展示面板]
        M[配置与任务管理]
    end

    C -->|触发任务| E
    D -->|分配 Cookie| E
    E <-->|发起请求| A
    E -->|保存原始数据| I
    E -->|保存原始数据| J
    
    F <-->|获取最新行情| B
    F <-->|读取帖子,写回报率| I
    F <-->|读取帖子,写回报率| J
    
    G -->|联邦查询| I
    G -->|联邦查询| J
    
    H <-->|读写| C
    H <-->|返回聚合结果| G
    H <-->|直接查独立群组| I
    
    L <-->|RESTful/SSE| H
    M <-->|配置写入| H
```

---

## 3. 核心模块与文件体系结构

### 3.1 爬虫模块 (Crawler Engine)
- **核心文件**: `zsxq_interactive_crawler.py`
- **设计思路**:
  - **精细化伪装与反爬**: 通过 `get_stealth_headers()` 和动态 `smart_delay()` 避免触发服务器安全策略。遇到速率限制（403等）时，能自动触发长休眠和中断退出，禁止死磕重试。
  - **数据抓取模式**: 支持纯历史溯源（基于翻页或时间戳）、增量追赶（从本地最新记录追溯至现在）、最新话题扫描等。

### 3.2 舆情抽取与性能计算模块 (Analyzer)
- **核心文件**: `stock_analyzer.py`, `global_analyzer.py`
- **设计思路**:
  - **单群组提取** (`stock_analyzer`): 内置 `_build_stock_dictionary()` 定期从 AkShare 拉取最新票池，辅以 `stock_aliases.json` 自定义映射，通过快速串扫描/正则提取自然语言中的股票。随后生成每个提及事件的“回报率（Returns）”。
  - **全局联邦分析** (`global_analyzer`): 由于每个群组数据相互隔离，该模块提供 `get_global_win_rate()`、`get_global_sector_heat()` 等方法跨多个 SQLite 库聚合查询数据，并结合内存级再排序支持基于多维指标（例如胜率、提及次数）的全局筛选。

### 3.3 数据库与持久化设计 (Storage)
- **核心文件**: `zsxq_database.py`, `zsxq_columns_database.py`, `zsxq_file_database.py`
- **设计思路**:
  - **分片存储模式**: 采用“一群组一文件夹”的结构。即群组号为 `123`，会在 `output/databases/123/` 下生成 `zsxq_topics_123.db`, `zsxq_files_123.db` 等多个职责单一的 SQLite 文件。
  - **高并发支持 (WAL)**: 所有数据库连接均开启 Write-Ahead Logging 模式并设定较高的 timeout，从而允许调度器写数据的同时前端能查询并刷新数据面板。

### 3.4 API 层与调度系统
- **核心文件**: `main.py`, `auto_scheduler.py`, `accounts_sql_manager.py`
- **设计思路**:
  - `main.py` 承载 FastAPI 框架，暴露给前端标准 REST 接口，同时利用 SSE 推送爬虫过程中的实时日志给前端。
  - `auto_scheduler.py` 支持设定休眠间隔的死循环型自动化任务抓取，同时可以触发分析模块同步计算回报率。

### 3.5 Web 前端 (Frontend)
- **存放路径**: `/frontend/`
- **技术栈**: Next.js 15 (App Router), React 18, TailwindCSS 4, ECharts
- **设计思路**:
  - 提供模块化的图表查看（热词云、K线表现、全局板块等）与交互式表格（专栏分页、历史提及详情）。
  - 直接对接 `http://localhost:8208` 暴露的后端端点。

---

## 4. 关键业务数据流向

1. **爬虫采集阶段**:
   - `auto_scheduler.py` 周期性或手动通过 API 唤起 `ZSXQInteractiveCrawler`。
   - `ZSXQInteractiveCrawler` 根据时间游标向知识星球发起请求获取 json。
   - 提取出的话题 / 评论 / 文件元数据存入对应群组的 `topics.db` 和 `files.db`。
2. **数据清洗与分析阶段**:
   - 爬虫存库完成后或在调度钩子中，`StockAnalyzer.calc_pending_performance()` 被唤醒。
   - 分析器读取最近拉取的纯文本内容，清洗出股票标识 `[股票名(代码)]`，记录 `topic_stock_mentions`。
   - 接着向 AkShare 请求该时段的 A 股收盘数据。
   - 计算该记录触发点之后的 1、3、5、10、20 ... 天绝对与相对（基准沪深300）回报。
3. **数据展示阶段**:
   - 用户打开 Web 后台。
   - 前端携带筛选条件请求 `main.py` 的 `/api/global_win_rate` 等接口。
   - `global_analyzer.py` 打开多个群组 DB，执行合并过滤。
   - 返回整理并排序后的最终 JSON 结构。

---

## 5. 运行时约束与扩展指引

- **并发/锁（重要）**: 虽然 SQLite 开启了 WAL 支持多读一写，但同一群组下请勿让多个 Python 进程同时启动爬虫向同一库写数据。建议通过基于锁的调度控制并发写。
- **自定义黑白名单**: 系统在分析时会加载本地 `stock_exclude.json`（避免部分非股票的高频词被误杀）。
- **账户限制保护**: 为了对抗硬性封禁风险，API 请求严格依赖延迟策略；长期部署应依赖多账号轮换（存储于 `accounts_db`）。

_此文档为项目的架构蓝图设计整理，供后续迭代或引入新 AI Agent 接手开发时作为**全局上下文理解依据**。_

# AI Prompting & Workflow Guide (for Humans)

欢迎来到 ZsxqCrawler 的开发流！为了让你（人类开发者）与各种 AI Agent（如 Cursor, Windsurf, Copilot, 甚至 ChatGPT/Claude Web 等）实现最高效的结对编程，请参考以下针对本项目的“最佳对话法则”。

## 1. 为什么我们需要特定的工作流？
本项目包含：
- **复杂并行的数据库写锁** (SQLite WAL)
- **多账户切换与严苛的反爬限制** (403 封号、随机延迟)
- **跨 SQLite 的联邦聚合分析** (Global Analyzer)
- **全栈项目** (FastAPI + Next.js App Router)

如果你只对 AI 说：“帮我修一下胜率显示的 Bug”，它可能会因为**缺乏全局视图**而强行重写前后端逻辑甚至破坏数据库锁。

---

## 2. 核心提示词范式 (The Prompt Pattern)

当你准备给 AI 下达一个新任务时，请尽量**包含以下三个要素**（你可以直接复制作为模板）：

### 📝 模板
> **目标**: [一句话描述你要做什么，例如：在前端大屏增加一个热门股票词云组件]
> **上下文**: 
> - [如果涉及后端逻辑，告诉它数据流来源：数据是从 global_analyzer.py 的 `get_global_hot_words` 方法出的]
> - [告诉 AI 当前你的项目架构档在哪：阅读 `.ai/project_design.md` 和 `.ai/architecture_context.md` 中关于数据聚合的部分]
> **约束**: [例如：不要修改 SQLite 的表结构；使用 Echarts 且符合 TailwindCSS 4 的风格]

### ✅ 示例
> "目标：帮我写一个接口，能跨群组查询特定股票在这几天的提及明细。
> 上下文：我们的爬虫数据分属于不同的 SQLite 文件 (`output/databases/{group_id}/`)。你可以先看看 `.ai/project_design.md` 里的数据流架构。
> 约束：必须把方法写在 `global_analyzer.py` 里面，参考已有的 `_query_all_groups` 方法。千万不要让爬虫代码因此产生并发死锁。"

---

## 3. 高级技巧：处理多轮复杂迭代

### 3.1 善用 `.beads` (如果你用特定的支持 Beads 的 Agent)
如果你的 AI 支持任务规划（如 Google 的内部框架），请提醒它：
> “请先在 `.beads/` 中立项，把你的大纲写下来。然后再分步骤执行。执行途中遇到了报错，也将报错上下文记录进去。”

### 3.2 遇到 "Database Is Locked" 怎么跟 AI 说？
这是本项目高频坑点，如果 AI 导致了这个报错，直接对它说：
> "你刚写的代码导致了 `database is locked`。请遵守 `.cursorrules` 里的规定，不要绕过 `ZSXQDatabase` 对象的上下文管理器。你需要用 `async with lock:` 或者在写库阶段避免多线程并发同时请求文件系统。"

### 3.3 遇到 "403 Forbidden" (反爬虫) 怎么跟 AI 说？
> "知识星球接口报了 403。你不要帮我写任何无限重试 (While True -> retry) 的逻辑！去 `zsxq_interactive_crawler.py` 里面看看能否调用 `smart_delay()`，并且如果连续失败两次，必须触发长休眠 (Long Sleep) 或直接中断当前用户的此次抓取任务。"

---

## 4. 推荐的 Debug 流程
当你不知道系统为什么挂了，向 AI 求助的标准话术：
1. **给日志**：粘贴终端里的 `ERROR` 堆栈，或者提供最新的 `server.log` 甚至特定群组里的 SQLite 的几条报错数据结构。
2. **给最后修改点**：告诉 AI “我刚刚在 `stock_analyzer` 改了正则”。
3. **指令**：“深呼吸，一步一步地推演。结合我提供的错误日志和 `.ai/project_design.md` 针对 Python 模块的边界描述，告诉我哪个变量在跨群运算时变成了 None？”

**祝你与 AI 结对编程愉快！**

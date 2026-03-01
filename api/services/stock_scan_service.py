from __future__ import annotations

from fastapi import BackgroundTasks

from api.services.task_facade import TaskFacade
from modules.analyzers.stock_analyzer import StockAnalyzer


class StockScanService:
    def __init__(self):
        self.tasks = TaskFacade()

    def start_scan(
        self,
        group_id: str,
        background_tasks: BackgroundTasks,
        force: bool = False,
        start_date: str | None = None,
        end_date: str | None = None,
    ):
        has_range = bool(start_date and end_date)
        desc = (
            f"股票收益重算: {group_id} [{start_date}~{end_date}]"
            if has_range
            else f"股票提及扫描: {group_id}"
        )
        task_id = self.tasks.create_task(f"stock_scan_{group_id}", desc)

        def _scan_task() -> None:
            try:
                if self.tasks.is_task_stopped(task_id):
                    self.tasks.update_task(task_id, "cancelled", "任务在启动前已被停止")
                    return

                self.tasks.update_task(task_id, "running", "正在扫描...")
                self.tasks.append_log(task_id, "🚀 开始股票提及扫描...")
                self.tasks.append_log(task_id, "🧭 分析引擎版本: dict-log-v2")
                self.tasks.update_task(task_id, "running", "正在准备股票字典...")

                def _log_progress(msg: str):
                    self.tasks.append_log(task_id, msg)
                    if any(k in msg for k in ["开始扫描", "已扫描", "开始计算", "已计算", "扫描完成", "全部完成"]):
                        self.tasks.update_task(task_id, "running", msg)

                analyzer = StockAnalyzer(
                    group_id,
                    log_callback=_log_progress,
                    stop_check=lambda: self.tasks.is_task_stopped(task_id),
                )
                if has_range:
                    self.tasks.append_log(
                        task_id,
                        f"🗓️ 按日期范围重算: {start_date} ~ {end_date}" + ("（强制）" if force else ""),
                    )
                    result = analyzer.recalculate_performance_range(
                        start_date=str(start_date),
                        end_date=str(end_date),
                        force=force,
                    )
                    if self.tasks.is_task_stopped(task_id) or bool(result.get("aborted")):
                        self.tasks.update_task(task_id, "cancelled", "范围重算已停止", result)
                    else:
                        self.tasks.update_task(
                            task_id,
                            "completed",
                            f"完成: 范围 {start_date}~{end_date}, 总计 {result.get('total', 0)}, "
                            f"成功 {result.get('processed', 0)}, 跳过 {result.get('skipped', 0)}, "
                            f"失败 {result.get('errors', 0)}",
                            result,
                        )
                    return

                result = analyzer.scan_group(force=force)

                if self.tasks.is_task_stopped(task_id) or bool(result.get("aborted")):
                    self.tasks.update_task(task_id, "cancelled", "股票扫描已停止", result)
                    return

                self.tasks.append_log(
                    task_id,
                    f"✅ 扫描完成: {result['mentions_extracted']} 次提及, {result['unique_stocks']} 只股票",
                )
                self.tasks.update_task(
                    task_id,
                    "completed",
                    f"完成: {result['topics_scanned']} 帖子, {result['mentions_extracted']} 次提及, "
                    f"{result['unique_stocks']} 只股票, {result['performance_calculated']} 条表现计算",
                )
            except Exception as e:
                self.tasks.append_log(task_id, f"❌ 扫描失败: {e}")
                self.tasks.update_task(task_id, "failed", f"扫描失败: {e}")

        background_tasks.add_task(_scan_task)
        return {"task_id": task_id, "message": "股票分析任务已启动"}

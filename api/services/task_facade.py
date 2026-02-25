from __future__ import annotations

import asyncio
import json
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List, Optional

from fastapi import HTTPException

from api.deps.container import get_task_runtime


class TaskFacade:
    """Unified task read/update/stop facade for routers and services."""

    def __init__(self):
        self.runtime = get_task_runtime()

    @property
    def tasks(self) -> Dict[str, Dict[str, Any]]:
        return self.runtime.tasks

    @property
    def logs(self) -> Dict[str, List[str]]:
        return self.runtime.logs

    def append_log(self, task_id: str, message: str) -> None:
        self.runtime.append_log(task_id, message)

    def update_task(
        self,
        task_id: str,
        status: str,
        message: str,
        result: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.runtime.update_task(task_id=task_id, status=status, message=message, result=result)
        if task_id in self.tasks:
            self.append_log(task_id, f"状态更新: {message}")

    def create_task(self, task_type: str, description: str) -> str:
        task_id = self.runtime.create_task(task_type=task_type, message=description, status="pending")
        self.append_log(task_id, f"任务创建: {description}")
        return task_id

    def get_tasks(self) -> List[Dict[str, Any]]:
        return list(self.tasks.values())

    def get_task(self, task_id: str) -> Dict[str, Any]:
        task = self.tasks.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="任务不存在")
        return task

    def get_task_logs(self, task_id: str) -> Dict[str, Any]:
        if task_id not in self.logs:
            raise HTTPException(status_code=404, detail="任务不存在")
        return {"task_id": task_id, "logs": self.logs[task_id]}

    def is_task_stopped(self, task_id: str) -> bool:
        return self.runtime.is_stopped(task_id)

    async def stop_task(self, task_id: str) -> Dict[str, Any]:
        task = self.tasks.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="任务不存在或无法停止")

        if task.get("status") not in ["pending", "running"]:
            raise HTTPException(status_code=404, detail="任务不存在或无法停止")

        self.runtime.request_stop(task_id)
        self.append_log(task_id, "🛑 收到停止请求，正在停止任务...")

        if task_id == "scheduler":
            try:
                from app.scheduler.auto_scheduler import get_scheduler

                self.update_task(task_id, "stopping", "调度器停止请求已发送，正在收尾...")
                asyncio.create_task(get_scheduler().stop())
                return {"message": "任务停止请求已发送", "task_id": task_id}
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"停止调度器失败: {e}")

        self.update_task(task_id, "cancelled", "任务已被用户停止")
        return {"message": "任务停止请求已发送", "task_id": task_id}

    def _to_iso_datetime(self, value: Any) -> Optional[str]:
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

    def _task_sort_key(self, task: Dict[str, Any]) -> str:
        return self._to_iso_datetime(task.get("updated_at")) or self._to_iso_datetime(task.get("created_at")) or ""

    def _normalize_task_snapshot(self, task: Dict[str, Any]) -> Dict[str, Any]:
        return {
            **task,
            "created_at": self._to_iso_datetime(task.get("created_at")),
            "updated_at": self._to_iso_datetime(task.get("updated_at")),
        }

    def _task_category(self, task_type: str) -> str:
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

    def build_summary(self) -> Dict[str, Any]:
        running_status = {"pending", "running", "stopping"}
        terminal_status = {"completed", "failed", "cancelled", "stopped", "idle"}

        running_by_type: Dict[str, Dict[str, Any]] = {}
        latest_by_type: Dict[str, Dict[str, Any]] = {}
        running_by_task_type: Dict[str, Dict[str, Any]] = {}
        latest_by_task_type: Dict[str, Dict[str, Any]] = {}

        grouped: Dict[str, List[Dict[str, Any]]] = {
            "crawl": [],
            "files": [],
            "analyze": [],
            "scheduler": [],
            "other": [],
        }

        for raw_task in self.tasks.values():
            task = self._normalize_task_snapshot(raw_task)
            task_type = str(task.get("type", ""))
            grouped[self._task_category(task_type)].append(task)
            if task_type:
                prev_running = running_by_task_type.get(task_type)
                if str(task.get("status", "")) in running_status and (
                    prev_running is None or self._task_sort_key(task) > self._task_sort_key(prev_running)
                ):
                    running_by_task_type[task_type] = task
                prev_latest = latest_by_task_type.get(task_type)
                if prev_latest is None or self._task_sort_key(task) > self._task_sort_key(prev_latest):
                    latest_by_task_type[task_type] = task

        for category, items in grouped.items():
            if not items:
                continue
            items_sorted = sorted(items, key=self._task_sort_key, reverse=True)
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

    async def stream_task_logs(self, task_id: str) -> AsyncGenerator[str, None]:
        if task_id in self.logs:
            for log in self.logs[task_id]:
                yield f"data: {json.dumps({'type': 'log', 'message': log})}\\n\\n"

        last_status = None
        last_message = None
        if task_id in self.tasks:
            task = self.tasks[task_id]
            last_status = task.get("status")
            last_message = task.get("message")
            yield f"data: {json.dumps({'type': 'status', 'status': task['status'], 'message': task['message']})}\\n\\n"

        last_log_count = len(self.logs.get(task_id, []))

        while True:
            current_log_count = len(self.logs.get(task_id, []))
            if current_log_count > last_log_count:
                new_logs = self.logs[task_id][last_log_count:]
                for log in new_logs:
                    yield f"data: {json.dumps({'type': 'log', 'message': log})}\\n\\n"
                last_log_count = current_log_count

            if task_id in self.tasks:
                task = self.tasks[task_id]
                status = task.get("status")
                message = task.get("message")
                if status != last_status or message != last_message:
                    yield f"data: {json.dumps({'type': 'status', 'status': status, 'message': message})}\\n\\n"
                    last_status = status
                    last_message = message

                if status in ["completed", "failed", "cancelled", "stopped", "idle"]:
                    break

            yield f"data: {json.dumps({'type': 'heartbeat'})}\\n\\n"
            await asyncio.sleep(0.5)

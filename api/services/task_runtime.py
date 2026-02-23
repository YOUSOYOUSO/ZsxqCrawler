from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional


class TaskRuntime:
    """In-memory task runtime with thread-safe state/log/stop-flag management."""

    def __init__(self):
        self._lock = threading.RLock()
        self._task_counter = 0
        self._tasks: Dict[str, Dict[str, Any]] = {
            "scheduler": {
                "task_id": "scheduler",
                "type": "scheduler",
                "status": "stopped",
                "message": "自动调度系统（未启动）",
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
                "result": None,
            }
        }
        self._logs: Dict[str, List[str]] = {"scheduler": []}
        self._stop_flags: Dict[str, bool] = {}

    @property
    def tasks(self) -> Dict[str, Dict[str, Any]]:
        return self._tasks

    @property
    def logs(self) -> Dict[str, List[str]]:
        return self._logs

    @property
    def stop_flags(self) -> Dict[str, bool]:
        return self._stop_flags

    def next_id(self, prefix: str = "task") -> str:
        with self._lock:
            self._task_counter += 1
            return f"{prefix}_{self._task_counter}_{int(time.time())}"

    def create_task(self, task_type: str, message: str, task_id: Optional[str] = None, status: str = "pending") -> str:
        with self._lock:
            if not task_id:
                self._task_counter += 1
                task_id = f"task_{self._task_counter}_{int(time.time())}"

            now = datetime.now().isoformat()
            self._tasks[task_id] = {
                "task_id": task_id,
                "type": task_type,
                "status": status,
                "message": message,
                "result": None,
                "created_at": now,
                "updated_at": now,
            }
            self._logs.setdefault(task_id, [])
            self._stop_flags[task_id] = False
            return task_id

    def update_task(self, task_id: str, status: str, message: str, result: Optional[Dict[str, Any]] = None):
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return
            task.update(
                {
                    "status": status,
                    "message": message,
                    "result": result,
                    "updated_at": datetime.now().isoformat(),
                }
            )

    def append_log(self, task_id: str, message: str):
        with self._lock:
            logs = self._logs.setdefault(task_id, [])
            timestamp = datetime.now().strftime("%H:%M:%S")
            logs.append(f"[{timestamp}] {message}")

    def set_scheduler_log(self, message: str, cap: int = 500):
        with self._lock:
            logs = self._logs.setdefault("scheduler", [])
            logs.append(message)
            if len(logs) > cap:
                self._logs["scheduler"] = logs[-cap:]

    def request_stop(self, task_id: str) -> bool:
        with self._lock:
            if task_id not in self._tasks:
                return False
            self._stop_flags[task_id] = True
            return True

    def is_stopped(self, task_id: str) -> bool:
        with self._lock:
            return bool(self._stop_flags.get(task_id, False))


from fastapi import BackgroundTasks


class GlobalTaskService:
    """Facade for global long-running tasks.

    Current implementation delegates to legacy handlers to preserve behavior.
    """

    def start(self, fn, background_tasks: BackgroundTasks, *args, **kwargs):
        return fn(*args, background_tasks=background_tasks, **kwargs)


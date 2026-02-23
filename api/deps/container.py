from api.services.task_runtime import TaskRuntime

_task_runtime = TaskRuntime()


def get_task_runtime() -> TaskRuntime:
    return _task_runtime


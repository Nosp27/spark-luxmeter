def task_status(task):
    status = "running"
    if task.cancelled():
        status = "cancelled"
    if task.done():
        status = "done"
    return status

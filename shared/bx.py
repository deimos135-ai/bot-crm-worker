import requests
from .settings import settings

def bx(method: str, payload: dict):
    url = f"{settings.BITRIX_WEBHOOK_BASE}/{method}.json"
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(data)
    return data.get("result")

def list_tasks(filter_, select):
    return bx("tasks.task.list", {"filter": filter_, "select": select})

def complete_task(task_id: int):
    try:
        return bx("tasks.task.complete", {"taskId": task_id})
    except Exception:
        return bx("tasks.task.update", {"taskId": task_id, "fields": {"STATUS": 5}})

def add_comment(task_id: int, text: str):
    try:
        bx("task.commentitem.add", {"TASKID": task_id, "POST_MESSAGE": text})
    except Exception:
        pass

def add_elapsed(task_id: int, user_id: int, seconds: int, comment: str = ""):
    return bx("tasks.task.elapseditem.add", {
        "taskId": task_id, "USER_ID": user_id, "SECONDS": seconds, "COMMENT_TEXT": comment
    })

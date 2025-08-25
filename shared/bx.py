import requests
from .settings import settings

def _call(method: str, payload: dict):
    url = f"{settings.BITRIX_WEBHOOK_BASE}/{method}.json"
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(data)
    return data.get("result")

def list_tasks(filter_, select):
    # новий метод → старі фолбеки
    try:
        return _call("tasks.task.list", {"filter": filter_, "select": select})
    except Exception:
        try:
            return _call("task.tasks.getlist", {"FILTER": filter_, "SELECT": select})
        except Exception:
            return _call("task.item.list", {"FILTER": filter_, "SELECT": select})

def complete_task(task_id: int):
    try:
        return _call("tasks.task.complete", {"taskId": task_id})
    except Exception:
        try:
            return _call("tasks.task.update", {"taskId": task_id, "fields": {"STATUS": 5}})
        except Exception:
            return _call("task.item.update", {"taskId": task_id, "fields": {"STATUS": 5}})

def add_comment(task_id: int, text: str):
    # стабільний старий метод
    return _call("task.commentitem.add", {"TASKID": task_id, "POST_MESSAGE": text})

def add_elapsed(task_id: int, user_id: int, seconds: int, comment: str = ""):
    try:
        return _call("tasks.task.elapseditem.add", {
            "taskId": task_id, "USER_ID": user_id, "SECONDS": seconds, "COMMENT_TEXT": comment
        })
    except Exception:
        return _call("task.elapseditem.add", {
            "TASKID": task_id, "USER_ID": user_id, "SECONDS": seconds, "COMMENT_TEXT": comment
        })

def search_user_by_email(email: str):
    return _call("user.search", {"FILTER": {"EMAIL": email}})

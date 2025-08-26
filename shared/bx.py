# shared/bx.py
import requests
from typing import Any, Dict, List, Optional
from shared.settings import settings

BASE = settings.BITRIX_WEBHOOK_BASE.rstrip("/")  # типу https://.../rest/123/abc123

def call_bx(method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{BASE}/{method}.json"
    resp = requests.post(url, json=params or {}, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"BX error: {data.get('error_description') or data.get('error')}")
    return data

# ---------- ЗАДАЧІ (залишаємо як було)
def list_tasks(filt: Dict[str, Any], select: List[str]) -> Dict[str, Any]:
    return call_bx("tasks.task.list", {"filter": filt, "select": select})

def complete_task(task_id: int) -> Dict[str, Any]:
    return call_bx("tasks.task.complete", {"taskId": int(task_id)})

def add_comment(task_id: int, text: str) -> Dict[str, Any]:
    return call_bx("tasks.task.commentitem.add", {"taskId": int(task_id), "fields": {"POST_MESSAGE": text}})

def search_user_by_email(email: str):
    return call_bx("user.search", {"EMAIL": email}).get("result")

# ---------- УГОДИ
def list_deal_stages(category_id: int):
    # Bitrix: crm.dealcategory.stage.list(id) -> result: [{STATUS_ID/ID, NAME, SORT, ...}]
    return call_bx("crm.dealcategory.stage.list", {"id": int(category_id)}).get("result", [])

def list_deals(filt: Dict[str, Any], select: List[str], order: Optional[Dict[str, str]] = None, start: int = 0):
    payload = {"filter": filt, "select": select, "order": order or {"ID": "DESC"}, "start": start}
    return call_bx("crm.deal.list", payload)

def move_deal_to_stage(deal_id: int, stage_id: str):
    return call_bx("crm.deal.update", {"id": int(deal_id), "fields": {"STAGE_ID": stage_id}})

def comment_deal(deal_id: int, text: str):
    # спробуємо додати таймлайн-коментар; якщо портал не дозволяє — зробимо апдейт COMMENTS
    try:
        return call_bx("crm.timeline.comment.add", {"fields": {"ENTITY_TYPE": "deal", "ENTITY_ID": int(deal_id), "COMMENT": text}})
    except Exception:
        return call_bx("crm.deal.update", {"id": int(deal_id), "fields": {"COMMENTS": text}})

import aiohttp
import asyncio
from typing import Any, Dict, Optional

from shared.settings import settings


async def _fetch_json(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{settings.BITRIX_WEBHOOK_BASE}/{method}"
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25)) as sess:
        async with sess.post(url, json=payload) as r:
            r.raise_for_status()
            return await r.json()


def _call_sync(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    # використовується в async контексті через asyncio.run_in_executor — але простіше: loop.run_until_complete
    return asyncio.get_event_loop().run_until_complete(_fetch_json(method, payload))


def call_bx(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return _call_sync(method, payload)


# ---- Tasks
def list_tasks(filters: Dict[str, Any], select: Optional[list] = None) -> Dict[str, Any]:
    payload = {"filter": filters}
    if select:
        payload["select"] = select
    payload["order"] = {"DEADLINE": "ASC"}
    payload["start"] = 0
    return call_bx("tasks.task.list", payload)

def complete_task(task_id: int):
    return call_bx("tasks.task.complete", {"taskId": int(task_id)})

def add_comment(task_id: int, text: str):
    return call_bx("task.commentitem.add", {"TASKID": int(task_id), "FIELDS": {"POST_MESSAGE": text}})

def search_user_by_email(email: str):
    # працює на порталах із активною індексацією користувачів
    return call_bx("user.search", {"EMAIL": email}).get("result", [])


# ---- Deals
def list_deal_stages(category_id: int):
    return call_bx("crm.dealcategory.stage.list", {"id": int(category_id)}).get("result", [])

def list_deals(filters: Dict[str, Any], select: Optional[list] = None, order: Optional[Dict[str, str]] = None, start: Optional[int] = None):
    payload = {"filter": filters}
    if select:
        payload["select"] = select
    if order:
        payload["order"] = order
    if start is not None:
        payload["start"] = start
    return call_bx("crm.deal.list", payload)

def move_deal_to_stage(deal_id: int, stage_id: str):
    return call_bx("crm.deal.update", {"id": int(deal_id), "fields": {"STAGE_ID": stage_id}})

def comment_deal(deal_id: int, text: str):
    # коментар як Timeline Note
    return call_bx("crm.timeline.note.add", {"entityTypeId": 2, "entityId": int(deal_id), "text": text})

def get_deal(deal_id: int):
    return call_bx("crm.deal.get", {"id": int(deal_id)}).get("result", {})

def get_deal_products(deal_id: int):
    return call_bx("crm.deal.productrows.get", {"id": int(deal_id)}).get("result", [])

def get_deal_contacts(deal_id: int):
    return call_bx("crm.deal.contact.items.get", {"id": int(deal_id)}).get("result", [])

def get_contact(contact_id: int):
    return call_bx("crm.contact.get", {"id": int(contact_id)}).get("result", {})

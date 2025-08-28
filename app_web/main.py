# app_web/main.py
import asyncio
import html
import json
import logging
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import aiohttp
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    Update,
    CallbackQuery,
)

from shared.settings import settings

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app")

app = FastAPI()
bot = Bot(token=settings.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ----------------------------- Bitrix helpers -----------------------------

B24_BASE = settings.BITRIX_WEBHOOK_BASE.rstrip("/")
# Витягуємо портал, щоб будувати лінки без settings.B24_DOMAIN
_p = urlparse(B24_BASE)
PORTAL_BASE = f"{_p.scheme}://{_p.netloc}"

HTTP: aiohttp.ClientSession


async def b24(method: str, **params) -> Any:
    """Call Bitrix REST method."""
    url = f"{B24_BASE}/{method}.json"
    async with HTTP.post(url, json=params) as resp:
        data = await resp.json()
        if "error" in data:
            raise RuntimeError(f"B24 error: {data['error']}: {data.get('error_description')}")
        return data.get("result")


# ----------------------------- Caches -------------------------------------

_DEAL_TYPE_MAP: Optional[Dict[str, str]] = None
_ROUTER_ENUM_MAP: Optional[Dict[str, str]] = None   # UF_CRM_1602756048 optionId -> text


async def get_deal_type_map() -> Dict[str, str]:
    """
    Статуси-типи угод (ENTITY_ID=DEAL_TYPE). Адміни можуть перейменувати їх
    під 'підключення/ремонт/аварія' тощо — тоді цей мапінг все відобразить.
    """
    global _DEAL_TYPE_MAP
    if _DEAL_TYPE_MAP is None:
        items = await b24("crm.status.list", filter={"ENTITY_ID": "DEAL_TYPE"})
        # items: [{STATUS_ID:'SALE', NAME:'Підключення'}, ...]
        _DEAL_TYPE_MAP = {i["STATUS_ID"]: i["NAME"] for i in items}
    return _DEAL_TYPE_MAP


async def get_router_enum_map() -> Dict[str, str]:
    """
    Знаходимо користувацьке поле UF_CRM_1602756048 і підтягуємо LIST-опції.
    """
    global _ROUTER_ENUM_MAP
    if _ROUTER_ENUM_MAP is None:
        fields = await b24("crm.deal.userfield.list", order={"SORT": "ASC"})
        uf = next((f for f in fields if f.get("FIELD_NAME") == "UF_CRM_1602756048"), None)
        options: Dict[str, str] = {}
        if uf and isinstance(uf.get("LIST"), list):
            for o in uf["LIST"]:
                # o: {'ID': '5162', 'VALUE': 'TP-Link EC220-G5', ...}
                options[str(o["ID"])] = o["VALUE"]
        _ROUTER_ENUM_MAP = options
    return _ROUTER_ENUM_MAP


# ----------------------------- Formatting ----------------------------------

# ЖОДНИХ <br> — тільки \n, щоб не ловити "Unsupported start tag 'br'"
BR = "\n"


def _strip_bb(text: str) -> str:
    """Bitrix comments may come with [p]...[/p] etc."""
    if not text:
        return ""
    t = re.sub(r"\[/?p\]", "", text, flags=re.I)
    # інколи влітають <br> — гасимо їх у \n
    t = t.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    return t.strip()


def _money_pair(val: Optional[str]) -> Optional[str]:
    """
    Parse strings like '1700|UAH' -> '1700 UAH'
    """
    if not val:
        return None
    parts = str(val).split("|", 1)
    if len(parts) == 2:
        return f"{parts[0]} {parts[1]}"
    return val


def _deal_link(deal: Dict[str, Any]) -> str:
    return f"{PORTAL_BASE}/crm/deal/details/{deal.get('ID')}/"


async def render_deal_card(deal: Dict[str, Any]) -> str:
    deal_type_map = await get_deal_type_map()
    router_map = await get_router_enum_map()

    deal_id = deal.get("ID")
    title = deal.get("TITLE") or f"Deal #{deal_id}"
    amount = f"{deal.get('OPPORTUNITY', '0.00')} {deal.get('CURRENCY_ID', 'UAH')}"
    type_code = deal.get("TYPE_ID") or ""
    type_name = deal_type_map.get(type_code, type_code or "—")
    category = deal.get("CATEGORY_ID", "—")

    address = deal.get("UF_CRM_6009542BC647F") or "—"

    router_id_val = deal.get("UF_CRM_1602756048")
    # поле може бути числом або рядком — уніфікуємо до str, пустоту вважаємо «нема»
    router_id = str(router_id_val) if router_id_val not in (None, "") else ""
    router_name = router_map.get(router_id) if router_id else "—"

    router_price = _money_pair(deal.get("UF_CRM_1604468981320")) or "—"

    comments = _strip_bb(deal.get("COMMENTS") or "")

    contact_name = "—"
    contact_phone = ""
    if deal.get("CONTACT_ID"):
        try:
            c = await b24("crm.contact.get", id=deal["CONTACT_ID"])
            if c:
                contact_name = f"{c.get('NAME', '')} {c.get('SECOND_NAME', '')} {c.get('LAST_NAME', '')}".strip() or "—"
                # pick first phone
                phones = c.get("PHONE") or []
                if isinstance(phones, list) and phones:
                    contact_phone = phones[0].get("VALUE") or ""
        except Exception as e:
            log.warning("contact.get failed: %s", e)

    head = f"#{deal_id} • {html.escape(title)}"
    link = _deal_link(deal)
    body_lines = [
        f"<b>Сума:</b> {html.escape(amount)}",
        "",
        f"<b>Тип сделки:</b> {html.escape(type_name)}",
        f"<b>Категорія:</b> {html.escape(str(category))}",
        f"<b>Адреса:</b> {html.escape(address)}",
        f"<b>Роутер:</b> {html.escape(router_name)}",
        f"<b>Вартість роутера:</b> {html.escape(router_price)}",
        f"<b>Коментар:</b> {html.escape(comments) if comments else '—'}",
        "",
        f"<b>Контакт:</b> {html.escape(contact_name)}"
        + (f" • <a href=\"tel:{contact_phone}\">{html.escape(contact_phone)}</a>" if contact_phone else ""),
        "",
        f"<a href=\"{link}\">Відкрити в Bitrix24</a>",
    ]
    return f"<b>{head}</b>{BR*2}" + BR.join(body_lines)


def deal_keyboard(deal: Dict[str, Any]) -> InlineKeyboardMarkup:
    deal_id = str(deal.get("ID"))
    kb = [
        [
            InlineKeyboardButton(text="✅ Закрити угоду", callback_data=f"close:{deal_id}"),
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)


async def send_deal_card(chat_id: int, deal: Dict[str, Any]) -> None:
    text = await render_deal_card(deal)
    await bot.send_message(
        chat_id,
        text,
        reply_markup=deal_keyboard(deal),
        disable_web_page_preview=True,  # щоб не рендерився прев’ю-линк
    )


# ----------------------------- Handlers ------------------------------------

@dp.message(Command("start"))
async def cmd_start(m: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Мої задачі", callback_data="tasks")],
        [InlineKeyboardButton(text="📦 Мої угоди", callback_data="my_deals")],
    ])
    await m.answer("Готові працювати ✅", reply_markup=kb)


@dp.callback_query(F.data == "my_deals")
async def cb_my_deals(c: CallbackQuery):
    # відповідаємо якнайшвидше, щоб не було "query is too old..."
    try:
        await c.answer(cache_time=5)
    except Exception:
        pass

    # TODO: заміни на свій мапінг Telegram→Bitrix користувача
    await c.message.answer("📦 Завантажую угоди бригади…")

    deals: List[Dict[str, Any]] = await b24(
        "crm.deal.list",
        filter={"CLOSED": "N"},
        order={"DATE_CREATE": "DESC"},
        select=["*"]
    )
    for d in deals[:25]:
        await send_deal_card(c.message.chat.id, d)


@dp.message(Command("deal_dump"))
async def deal_dump(m: Message):
    # /deal_dump 1109122  або /deal_dump #1109122
    mtext = (m.text or "").strip()
    m2 = re.search(r"(\d+)", mtext)
    if not m2:
        await m.answer("Вкажіть ID угоди: /deal_dump 12345")
        return
    deal_id = m2.group(1)
    deal = await b24("crm.deal.get", id=deal_id)
    if not deal:
        await m.answer("Не знайшов угоду.")
        return

    # красиво відформатувати
    pretty = html.escape(json.dumps(deal, ensure_ascii=False, indent=2))
    await m.answer(f"<b>Dump угоди #{deal_id}</b>\n<pre>{pretty}</pre>")

    # і одразу картка
    await send_deal_card(m.chat.id, deal)


@dp.callback_query(F.data.startswith("close:"))
async def cb_close_deal(c: CallbackQuery):
    try:
        await c.answer(cache_time=5)
    except Exception:
        pass

    deal_id = c.data.split(":", 1)[1]
    deal = await b24("crm.deal.get", id=deal_id)
    if not deal:
        await c.message.answer("❗️Не знайшов угоду.")
        return
    category = str(deal.get("CATEGORY_ID") or "0")
    target_stage = f"C{category}:WON"
    await b24("crm.deal.update", id=deal_id, fields={"STAGE_ID": target_stage})
    await c.message.answer(f"✅ Угоду #{deal_id} закрито у статусі WON.")
    # оновлена картка
    deal2 = await b24("crm.deal.get", id=deal_id)
    await send_deal_card(c.message.chat.id, deal2)


# ----------------------------- Webhook plumbing ----------------------------

@app.on_event("startup")
async def on_startup():
    global HTTP
    HTTP = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))

    # реєстрація вебхука
    url = f"{settings.WEBHOOK_BASE.rstrip('/')}/webhook/{settings.WEBHOOK_SECRET}"
    log.info("[startup] portal base: %s", PORTAL_BASE)
    log.info("[startup] setting webhook to: %s", url)
    await bot.set_webhook(url)


@app.on_event("shutdown")
async def on_shutdown():
    await bot.delete_webhook()
    await HTTP.close()
    await bot.session.close()


@app.post("/webhook/{secret}")
async def telegram_webhook(secret: str, request: Request):
    if secret != settings.WEBHOOK_SECRET:
        return {"ok": False}
    update = Update.model_validate(await request.json())
    await dp.feed_update(bot, update)
    return {"ok": True}

# app_web/main.py
import html
import json
import logging
import re
from typing import Any, Dict, List, Optional

import aiohttp
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
    BotCommand,
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
_ROUTER_ENUM_MAP: Optional[Dict[str, str]] = None     # UF_CRM_1602756048 optionId -> text
_TARIFF_ENUM_MAP: Optional[Dict[str, str]] = None     # UF_CRM_1610558031277 optionId -> text


async def get_deal_type_map() -> Dict[str, str]:
    global _DEAL_TYPE_MAP
    if _DEAL_TYPE_MAP is None:
        items = await b24("crm.status.list", filter={"ENTITY_ID": "DEAL_TYPE"})
        _DEAL_TYPE_MAP = {i["STATUS_ID"]: i["NAME"] for i in items}
    return _DEAL_TYPE_MAP


async def _enum_map_for_userfield(field_name: str) -> Dict[str, str]:
    """Generic helper to fetch LIST options of a Deal UF enum."""
    fields = await b24("crm.deal.userfield.list", order={"SORT": "ASC"})
    uf = next((f for f in fields if f.get("FIELD_NAME") == field_name), None)
    options: Dict[str, str] = {}
    if uf and isinstance(uf.get("LIST"), list):
        for o in uf["LIST"]:
            # o: {'ID': '5162', 'VALUE': 'TP-Link EC220-G5', ...}
            options[str(o["ID"])] = o["VALUE"]
    return options


async def get_router_enum_map() -> Dict[str, str]:
    global _ROUTER_ENUM_MAP
    if _ROUTER_ENUM_MAP is None:
        _ROUTER_ENUM_MAP = await _enum_map_for_userfield("UF_CRM_1602756048")
    return _ROUTER_ENUM_MAP


async def get_tariff_enum_map() -> Dict[str, str]:
    global _TARIFF_ENUM_MAP
    if _TARIFF_ENUM_MAP is None:
        _TARIFF_ENUM_MAP = await _enum_map_for_userfield("UF_CRM_1610558031277")
    return _TARIFF_ENUM_MAP


# ----------------------------- Formatting ---------------------------------

BR = "\n"


def _strip_bb(text: str) -> str:
    """Bitrix comments may come with [p]...[/p] etc."""
    if not text:
        return ""
    t = re.sub(r"\[/?p\]", "", text, flags=re.I)
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


def main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📦 Мої угоди")],
            [KeyboardButton(text="📋 Мої задачі")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
        selective=False,
    )


async def render_deal_card(deal: Dict[str, Any]) -> str:
    deal_type_map = await get_deal_type_map()
    router_map = await get_router_enum_map()
    tariff_map = await get_tariff_enum_map()

    deal_id = deal.get("ID")
    title = deal.get("TITLE") or f"Deal #{deal_id}"
    type_code = deal.get("TYPE_ID") or ""
    type_name = deal_type_map.get(type_code, type_code or "—")
    category = deal.get("CATEGORY_ID", "—")

    # адреса з UF, якщо нема — стандартне поле ADDRESS
    address_value = deal.get("UF_CRM_6009542BC647F") or deal.get("ADDRESS") or "—"

    # роутер
    router_id = str(deal.get("UF_CRM_1602756048") or "")
    router_name = router_map.get(router_id) if router_id else "—"
    router_price = _money_pair(deal.get("UF_CRM_1604468981320")) or "—"

    # тариф (enum + price)
    tariff_id = str(deal.get("UF_CRM_1610558031277") or "")
    tariff_name = tariff_map.get(tariff_id) if tariff_id else "—"
    tariff_price = _money_pair(deal.get("UF_CRM_1611652685839")) or "—"

    # підключення (ціна)
    install_price = _money_pair(deal.get("UF_CRM_1609868447208")) or "—"

    comments = _strip_bb(deal.get("COMMENTS") or "")

    contact_name = "—"
    contact_phone = ""
    if deal.get("CONTACT_ID"):
        try:
            c = await b24("crm.contact.get", id=deal["CONTACT_ID"])
            if c:
                contact_name = f"{c.get('NAME', '')} {c.get('SECOND_NAME', '')} {c.get('LAST_NAME', '')}".strip() or "—"
                phones = c.get("PHONE") or []
                if isinstance(phones, list) and phones:
                    contact_phone = phones[0].get("VALUE") or ""
        except Exception as e:
            log.warning("contact.get failed: %s", e)

    head = f"#{deal_id} • {html.escape(title)}"
    link = f"https://{settings.B24_DOMAIN}/crm/deal/details/{deal_id}/"

    # телефону не робимо <a href="tel:"> — Telegram сам зробить клікабельним
    contact_line = f"<b>Контакт:</b> {html.escape(contact_name)}"
    if contact_phone:
        contact_line += f" • {html.escape(contact_phone)}"

    body_lines = [
        f"<b>Тип угоди:</b> {html.escape(type_name)}",
        f"<b>Категорія:</b> {html.escape(str(category))}",
        f"<b>Адреса:</b> {html.escape(address_value)}",
        "",
        f"<b>Роутер:</b> {html.escape(router_name)}",
        f"<b>Вартість роутера:</b> {html.escape(router_price)}",
        "",
        f"<b>Тариф:</b> {html.escape(tariff_name)}",
        f"<b>Вартість тарифу:</b> {html.escape(tariff_price)}",
        f"<b>Вартість підключення:</b> {html.escape(install_price)}",
        "",
        f"<b>Коментар:</b> {html.escape(comments) if comments else '—'}",
        "",
        contact_line,
        "",
        f"<a href=\"{link}\">Відкрити в Bitrix24</a>",
    ]
    return f"<b>{head}</b>{BR*2}" + BR.join(body_lines)


def deal_keyboard(deal: Dict[str, Any]) -> InlineKeyboardMarkup:
    deal_id = str(deal.get("ID"))
    kb = [[InlineKeyboardButton(text="✅ Закрити угоду", callback_data=f"close:{deal_id}")]]
    return InlineKeyboardMarkup(inline_keyboard=kb)


async def send_deal_card(chat_id: int, deal: Dict[str, Any]) -> None:
    text = await render_deal_card(deal)
    await bot.send_message(
        chat_id,
        text,
        reply_markup=deal_keyboard(deal),
        disable_web_page_preview=True,  # прибирає прев’ю посилань
    )


# ----------------------------- Simple binding storage ----------------------

_BINDINGS: Dict[int, Dict[str, Any]] = {}


async def get_binding(user_id: int) -> Optional[Dict[str, Any]]:
    return _BINDINGS.get(user_id)


async def set_binding(user_id: int, email: str, brigade: Optional[int] = None):
    _BINDINGS[user_id] = {"email": email, "brigade": brigade}


# ----------------------------- Handlers ------------------------------------

@dp.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(
        "Готові працювати ✅\n\n"
        "Спочатку виконайте:\n"
        "• /bind ваш_email\n"
        "• /set_brigade 1..5",
        reply_markup=main_menu_kb(),
    )


@dp.message(Command("menu"))
async def cmd_menu(m: Message):
    await m.answer("Меню відкрито 👇", reply_markup=main_menu_kb())


@dp.message(Command("bind"))
async def cmd_bind(m: Message):
    parts = (m.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await m.answer("Вкажіть email: /bind ваш_email", reply_markup=main_menu_kb())
        return
    email = parts[1].strip()
    await set_binding(m.from_user.id, email=email)
    await m.answer(f"Прив’язано email: <code>{html.escape(email)}</code> ✅", reply_markup=main_menu_kb())


@dp.message(Command("set_brigade"))
async def cmd_set_brigade(m: Message):
    parts = (m.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await m.answer("Вкажіть номер бригади: /set_brigade 1", reply_markup=main_menu_kb())
        return
    try:
        brigade = int(parts[1])
    except ValueError:
        await m.answer("Номер має бути числом: 1..5", reply_markup=main_menu_kb())
        return

    bind = await get_binding(m.from_user.id)
    if not bind:
        await m.answer("Спершу виконайте /bind ваш_email", reply_markup=main_menu_kb())
        return

    bind["brigade"] = brigade
    await m.answer(f"✅ Прив’язано до бригади №{brigade}", reply_markup=main_menu_kb())


# Текстова кнопка «📦 Мої угоди»
@dp.message(F.text == "📦 Мої угоди")
async def msg_my_deals(m: Message):
    # Віддзеркалюємо логіку з callback
    user_id = m.from_user.id
    bind = await get_binding(user_id)

    if not bind or not bind.get("brigade"):
        await m.answer(
            "Спершу прив’яжіть акаунт і бригаду:\n"
            "• /bind ваш_email\n"
            "• /set_brigade 1..5",
            reply_markup=main_menu_kb(),
        )
        return

    brigade = bind["brigade"]
    stage_code = {
        1: "UC_XF8O6V",
        2: "UC_0XLPCN",
        3: "UC_204CP3",
        4: "UC_TNEW3Z",
        5: "UC_RMBZ37",
    }.get(brigade)

    if not stage_code:
        await m.answer("Невірний номер бригади.", reply_markup=main_menu_kb())
        return

    await m.answer(f"📦 Завантажую угоди для бригади №{brigade}…", reply_markup=main_menu_kb())

    deals: List[Dict[str, Any]] = await b24(
        "crm.deal.list",
        filter={"CLOSED": "N", "STAGE_ID": f"C20:{stage_code}"},
        order={"DATE_CREATE": "DESC"},
        select=[
            "ID", "TITLE", "TYPE_ID", "CATEGORY_ID", "STAGE_ID",
            "COMMENTS", "CONTACT_ID",
            # адреса
            "UF_CRM_6009542BC647F", "ADDRESS",
            # роутер
            "UF_CRM_1602756048",     # enum id
            "UF_CRM_1604468981320",  # price
            # тариф
            "UF_CRM_1610558031277",  # enum id
            "UF_CRM_1611652685839",  # price
            # підключення (ціна)
            "UF_CRM_1609868447208",
        ],
    )

    if not deals:
        await m.answer("Немає активних угод.", reply_markup=main_menu_kb())
        return

    for d in deals[:25]:
        await send_deal_card(m.chat.id, d)


@dp.callback_query(F.data == "my_deals")
async def cb_my_deals(c: CallbackQuery):
    await c.answer()
    # тримаємо для сумісності (якщо десь ще є інлайн-кнопка)
    m = c.message
    await msg_my_deals(m)


@dp.message(F.text == "📋 Мої задачі")
async def msg_tasks(m: Message):
    await m.answer("Задачі ще в розробці 🛠️", reply_markup=main_menu_kb())


@dp.message(Command("deal_dump"))
async def deal_dump(m: Message):
    # /deal_dump 1109122  або /deal_dump #1109122
    mtext = (m.text or "").strip()
    m2 = re.search(r"(\d+)", mtext)
    if not m2:
        await m.answer("Вкажіть ID угоди: /deal_dump 12345", reply_markup=main_menu_kb())
        return
    deal_id = m2.group(1)
    deal = await b24("crm.deal.get", id=deal_id)
    if not deal:
        await m.answer("Не знайшов угоду.", reply_markup=main_menu_kb())
        return

    pretty = html.escape(json.dumps(deal, ensure_ascii=False, indent=2))
    await m.answer(f"<b>Dump угоди #{deal_id}</b>\n<pre>{pretty}</pre>", reply_markup=main_menu_kb())
    await send_deal_card(m.chat.id, deal)


@dp.callback_query(F.data.startswith("close:"))
async def cb_close_deal(c: CallbackQuery):
    await c.answer()
    deal_id = c.data.split(":", 1)[1]
    deal = await b24("crm.deal.get", id=deal_id)
    if not deal:
        await c.message.answer("❗️Не знайшов угоду.")
        return
    category = str(deal.get("CATEGORY_ID") or "0")
    target_stage = f"C{category}:WON"
    await b24("crm.deal.update", id=deal_id, fields={"STAGE_ID": target_stage})
    await c.message.answer(f"✅ Угоду #{deal_id} закрито у статусі WON.")
    deal2 = await b24("crm.deal.get", id=deal_id)
    await send_deal_card(c.message.chat.id, deal2)


# ----------------------------- Webhook plumbing ----------------------------

@app.on_event("startup")
async def on_startup():
    global HTTP
    HTTP = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))

    # Команди бота (видимі у Bot Menu)
    await bot.set_my_commands([
        BotCommand(command="start", description="Почати"),
        BotCommand(command="menu", description="Показати меню"),
        BotCommand(command="bind", description="Прив’язати email"),
        BotCommand(command="set_brigade", description="Вибрати бригаду"),
        BotCommand(command="deal_dump", description="Показати dump угоди"),
    ])

    # Реєстрація вебхука
    url = f"{settings.WEBHOOK_BASE.rstrip('/')}/webhook/{settings.WEBHOOK_SECRET}"
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

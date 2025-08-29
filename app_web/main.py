# app_web/main.py
import html
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

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
    url = f"{B24_BASE}/{method}.json"
    async with HTTP.post(url, json=params) as resp:
        data = await resp.json()
        if "error" in data:
            raise RuntimeError(f"B24 error: {data['error']}: {data.get('error_description')}")
        return data.get("result")


async def b24_list(method: str, **params) -> List[Dict[str, Any]]:
    """Посторінкова вибірка Bitrix (до ~1000 елементів достатньо для звіту)."""
    start = 0
    items: List[Dict[str, Any]] = []
    while True:
        chunk = await b24(method, start=start, **params)
        if isinstance(chunk, dict) and "items" in chunk:
            page_items = chunk["items"]
            next_start = chunk.get("next")  # іноді Bitrix так повертає
        else:
            page_items = chunk or []
            next_start = None
        items.extend(page_items)
        if not page_items:
            break
        if next_start is None:
            start += len(page_items)
        else:
            start = next_start
        if start >= 1000:
            break
    return items


# ----------------------------- Caches -------------------------------------

_DEAL_TYPE_MAP: Optional[Dict[str, str]] = None
_ROUTER_ENUM_MAP: Optional[Dict[str, str]] = None      # UF_CRM_1602756048
_TARIFF_ENUM_MAP: Optional[Dict[str, str]] = None      # UF_CRM_1610558031277
_FACT_ENUM_LIST: Optional[List[Tuple[str, str]]] = None  # list of (value_id, name)


async def get_deal_type_map() -> Dict[str, str]:
    global _DEAL_TYPE_MAP
    if _DEAL_TYPE_MAP is None:
        items = await b24("crm.status.list", filter={"ENTITY_ID": "DEAL_TYPE"})
        _DEAL_TYPE_MAP = {i["STATUS_ID"]: i["NAME"] for i in items}
    return _DEAL_TYPE_MAP


async def _enum_map_for_userfield(field_name: str) -> Dict[str, str]:
    fields = await b24("crm.deal.userfield.list", order={"SORT": "ASC"})
    uf = next((f for f in fields if f.get("FIELD_NAME") == field_name), None)
    options: Dict[str, str] = {}
    if uf and isinstance(uf.get("LIST"), list):
        for o in uf["LIST"]:
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


async def get_fact_enum_list() -> List[Tuple[str, str]]:
    """
    Для UF_CRM_1602766787968: повертає список (VALUE, NAME) виключаючи 'не выбрано'.
    Порядок як у Bitrix (SORT asc).
    """
    global _FACT_ENUM_LIST
    if _FACT_ENUM_LIST is None:
        fields = await b24("crm.deal.userfield.list", order={"SORT": "ASC"})
        uf = next((f for f in fields if f.get("FIELD_NAME") == "UF_CRM_1602766787968"), None)
        lst: List[Tuple[str, str]] = []
        if uf and isinstance(uf.get("LIST"), list):
            for o in uf["LIST"]:
                val = str(o.get("VALUE", ""))
                name = str(o.get("NAME", ""))
                if val == "":
                    continue
                lst.append((val, name))
        _FACT_ENUM_LIST = lst
    return _FACT_ENUM_LIST


# ----------------------------- UI helpers ---------------------------------

def main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📦 Мої угоди")],
            [KeyboardButton(text="📋 Мої задачі")],
            [KeyboardButton(text="📊 Звіт за сьогодні")],
            [KeyboardButton(text="📉 Звіт за вчора")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
        selective=False,
    )


def pick_brigade_inline_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=f"Бригада №{i}", callback_data=f"setbrig:{i}")]
        for i in (1, 2, 3, 4, 5)
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ----------------------------- Deal rendering ------------------------------

def _strip_bb(text: str) -> str:
    if not text:
        return ""
    t = re.sub(r"\[/?p\]", "", text, flags=re.I)
    return t.strip()


def _money_pair(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    parts = str(val).split("|", 1)
    if len(parts) == 2:
        return f"{parts[0]} {parts[1]}"
    return val


async def render_deal_card(deal: Dict[str, Any]) -> str:
    deal_type_map = await get_deal_type_map()
    router_map = await get_router_enum_map()
    tariff_map = await get_tariff_enum_map()

    deal_id = deal.get("ID")
    title = deal.get("TITLE") or f"Deal #{deal_id}"
    type_code = deal.get("TYPE_ID") or ""
    type_name = deal_type_map.get(type_code, type_code or "—")
    category = deal.get("CATEGORY_ID", "—")

    address_value = deal.get("UF_CRM_6009542BC647F") or deal.get("ADDRESS") or "—"

    router_id = str(deal.get("UF_CRM_1602756048") or "")
    router_name = router_map.get(router_id) if router_id else "—"
    router_price = _money_pair(deal.get("UF_CRM_1604468981320")) or "—"

    tariff_id = str(deal.get("UF_CRM_1610558031277") or "")
    tariff_name = tariff_map.get(tariff_id) if tariff_id else "—"
    tariff_price = _money_pair(deal.get("UF_CRM_1611652685839")) or "—"

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
    return f"<b>{head}</b>\n\n" + "\n".join(body_lines)


def deal_keyboard(deal: Dict[str, Any]) -> InlineKeyboardMarkup:
    deal_id = str(deal.get("ID"))
    kb = [[InlineKeyboardButton(text="✅ Закрити угоду", callback_data=f"close:{deal_id}")]]
    return InlineKeyboardMarkup(inline_keyboard=kb)


async def send_deal_card(chat_id: int, deal: Dict[str, Any]) -> None:
    text = await render_deal_card(deal)
    await bot.send_message(chat_id, text, reply_markup=deal_keyboard(deal), disable_web_page_preview=True)


# ----------------------------- Simple storage (brigade only) ---------------

_USER_BRIGADE: Dict[int, int] = {}


def get_user_brigade(user_id: int) -> Optional[int]:
    return _USER_BRIGADE.get(user_id)


def set_user_brigade(user_id: int, brigade: int) -> None:
    _USER_BRIGADE[user_id] = brigade


# mapping "brigade number" -> UF_CRM_1611995532420[] option IDs (brigade items)
_BRIGADE_EXEC_OPTION_ID = {
    1: 5494,
    2: 5496,
    3: 5498,
    4: 5500,
    5: 5502,
}

# mapping "brigade number" -> stage code in Канбані
_STAGE_BY_BRIGADE = {
    1: "UC_XF8O6V",
    2: "UC_0XLPCN",
    3: "UC_204CP3",
    4: "UC_TNEW3Z",
    5: "UC_RMBZ37",
}

# ----------------------------- Close wizard (fact + reason) ----------------

# user_id -> context
_PENDING_CLOSE: Dict[int, Dict[str, Any]] = {}

_FACTS_PER_PAGE = 8  # сторінка меню


def _facts_page_kb(deal_id: str, page: int, facts: List[Tuple[str, str]]) -> InlineKeyboardMarkup:
    """
    Меню фактів у СТОВПЧИК (по 1 в рядку) + навігація.
    facts: list of (value_id, name)
    """
    start = page * _FACTS_PER_PAGE
    chunk = facts[start:start + _FACTS_PER_PAGE]

    rows: List[List[InlineKeyboardButton]] = []
    for val, name in chunk:
        rows.append([InlineKeyboardButton(text=name[:40], callback_data=f"factsel:{deal_id}:{val}")])

    total_pages = max(1, (len(facts) + _FACTS_PER_PAGE - 1) // _FACTS_PER_PAGE)
    nav: List[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="« Назад", callback_data=f"factpage:{deal_id}:{page-1}"))
    nav.append(InlineKeyboardButton(text=f"Стор. {page+1}/{total_pages}", callback_data="noop"))
    if (page + 1) * _FACTS_PER_PAGE < len(facts):
        nav.append(InlineKeyboardButton(text="Вперед »", callback_data=f"factpage:{deal_id}:{page+1}"))
    rows.append(nav)

    rows.append([InlineKeyboardButton(text="❌ Скасувати", callback_data=f"cmtcancel:{deal_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _finalize_close(user_id: int, deal_id: str, fact_val: str, fact_name: str, reason_text: str) -> None:
    # get deal and category
    deal = await b24("crm.deal.get", id=deal_id)
    if not deal:
        raise RuntimeError("Deal not found")
    category = str(deal.get("CATEGORY_ID") or "0")
    target_stage = f"C{category}:WON"

    # build COMMENTS append
    prev_comments = _strip_bb(deal.get("COMMENTS") or "")
    block = f"[p]<b>Закриття:</b> {html.escape(fact_name)}[/p]"
    if reason_text:
        block += f"\n[p]<b>Причина ремонту:</b> {html.escape(reason_text)}[/p]"
    new_comments = block if not prev_comments else f"{prev_comments}\n\n{block}"

    # executors: set brigade option by user brigade
    brigade = get_user_brigade(user_id)
    exec_list = []
    if brigade and brigade in _BRIGADE_EXEC_OPTION_ID:
        exec_list = [_BRIGADE_EXEC_OPTION_ID[brigade]]

    # update fields
    fields = {
        "STAGE_ID": target_stage,
        "COMMENTS": new_comments,
        "UF_CRM_1602766787968": fact_val,            # Що по факту зроблено (enum value id)
        "UF_CRM_1702456465911": reason_text,         # Причина ремонту (free text)
    }
    if exec_list:
        fields["UF_CRM_1611995532420"] = exec_list   # Виконавець (multi)

    await b24("crm.deal.update", id=deal_id, fields=fields)


# ----------------------------- Handlers ------------------------------------

@dp.message(Command("start"))
async def cmd_start(m: Message):
    b = get_user_brigade(m.from_user.id)
    text = "Готові працювати ✅"
    if b:
        text += f"\nПоточна бригада: №{b}"
    else:
        text += "\nОберіть вашу бригаду нижче ⬇️"

    await m.answer(text, reply_markup=main_menu_kb())
    if not b:
        await m.answer("Швидкий вибір бригади:", reply_markup=pick_brigade_inline_kb())


@dp.message(Command("menu"))
async def cmd_menu(m: Message):
    await m.answer("Меню відкрито 👇", reply_markup=main_menu_kb())


@dp.message(Command("set_brigade"))
async def cmd_set_brigade(m: Message):
    parts = (m.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await m.answer("Вкажіть номер бригади: /set_brigade 1", reply_markup=main_menu_kb())
        await m.answer("Або натисніть кнопку:", reply_markup=pick_brigade_inline_kb())
        return
    try:
        brigade = int(parts[1])
    except ValueError:
        await m.answer("Номер має бути числом: 1..5", reply_markup=main_menu_kb())
        return

    if brigade not in (1, 2, 3, 4, 5):
        await m.answer("Доступні бригади: 1..5", reply_markup=main_menu_kb())
        return

    set_user_brigade(m.from_user.id, brigade)
    await m.answer(f"✅ Прив’язано до бригади №{brigade}", reply_markup=main_menu_kb())


@dp.callback_query(F.data.startswith("setbrig:"))
async def cb_setbrig(c: CallbackQuery):
    await c.answer()
    try:
        brigade = int(c.data.split(":", 1)[1])
    except Exception:
        await c.message.answer("Невірний номер бригади.", reply_markup=main_menu_kb())
        return
    if brigade not in (1, 2, 3, 4, 5):
        await c.message.answer("Доступні бригади: 1..5", reply_markup=main_menu_kb())
        return
    set_user_brigade(c.from_user.id, brigade)
    await c.message.answer(f"✅ Обрано бригаду №{brigade}", reply_markup=main_menu_kb())


@dp.message(F.text == "📦 Мої угоди")
async def msg_my_deals(m: Message):
    brigade = get_user_brigade(m.from_user.id)
    if not brigade:
        await m.answer("Спершу оберіть бригаду:", reply_markup=pick_brigade_inline_kb())
        return
    stage_code = _STAGE_BY_BRIGADE.get(brigade)
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
            "UF_CRM_6009542BC647F", "ADDRESS",
            "UF_CRM_1602756048", "UF_CRM_1604468981320",
            "UF_CRM_1610558031277", "UF_CRM_1611652685839",
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
    await msg_my_deals(c.message)


@dp.message(F.text == "📋 Мої задачі")
async def msg_tasks(m: Message):
    await m.answer("Задачі ще в розробці 🛠️", reply_markup=main_menu_kb())


@dp.message(Command("deal_dump"))
async def deal_dump(m: Message):
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


# ======== МАЙСТЕР ЗАКРИТТЯ: вибір факту + причина ==========

@dp.callback_query(F.data.startswith("close:"))
async def cb_close_deal_start(c: CallbackQuery):
    await c.answer()
    deal_id = c.data.split(":", 1)[1]
    facts = await get_fact_enum_list()
    _PENDING_CLOSE[c.from_user.id] = {"deal_id": deal_id, "stage": "pick_fact", "page": 0}
    await c.message.answer(
        f"Закриваємо угоду <a href=\"https://{settings.B24_DOMAIN}/crm/deal/details/{deal_id}/\">#{deal_id}</a>. Оберіть, що зроблено:",
        reply_markup=_facts_page_kb(deal_id, 0, facts),
    )


@dp.callback_query(F.data.startswith("factpage:"))
async def cb_fact_page(c: CallbackQuery):
    await c.answer()
    parts = c.data.split(":")
    if len(parts) < 3:
        return
    deal_id, page_s = parts[1], parts[2]
    try:
        page = int(page_s)
    except:
        page = 0
    facts = await get_fact_enum_list()
    await c.message.edit_reply_markup(reply_markup=_facts_page_kb(deal_id, page, facts))
    ctx = _PENDING_CLOSE.get(c.from_user.id)
    if ctx:
        ctx["page"] = page


@dp.callback_query(F.data.startswith("factsel:"))
async def cb_fact_select(c: CallbackQuery):
    await c.answer()
    parts = c.data.split(":")
    if len(parts) < 3:
        return
    deal_id, fact_val = parts[1], parts[2]
    facts = await get_fact_enum_list()
    fact_name = next((n for v, n in facts if v == fact_val), "")
    if not fact_name:
        await c.message.answer("Не вдалося обрати значення.")
        return
    _PENDING_CLOSE[c.from_user.id] = {
        "deal_id": deal_id,
        "stage": "await_reason",
        "fact_val": fact_val,
        "fact_name": fact_name,
    }
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Пропустити", callback_data=f"reason_skip:{deal_id}")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data=f"cmtcancel:{deal_id}")],
    ])
    await c.message.answer(
        f"Обрано: <b>{html.escape(fact_name)}</b>\nВведіть причину ремонту одним повідомленням, або натисніть «Пропустити».",
        reply_markup=kb,
    )


@dp.callback_query(F.data.startswith("reason_skip:"))
async def cb_reason_skip(c: CallbackQuery):
    await c.answer()
    ctx = _PENDING_CLOSE.get(c.from_user.id)
    if not ctx or ctx.get("stage") != "await_reason":
        await c.message.answer("Нема активного закриття.")
        return
    deal_id = ctx["deal_id"]
    fact_val = ctx["fact_val"]
    fact_name = ctx["fact_name"]
    try:
        await _finalize_close(c.from_user.id, deal_id, fact_val, fact_name, reason_text="")
        await c.message.answer(f"✅ Угоду #{deal_id} закрито. Дані записані.")
        deal2 = await b24("crm.deal.get", id=deal_id)
        await send_deal_card(c.message.chat.id, deal2)
    except Exception as e:
        log.exception("finalize close (skip reason) failed")
        await c.message.answer(f"❗️Помилка закриття: {e}")
    finally:
        _PENDING_CLOSE.pop(c.from_user.id, None)


@dp.callback_query(F.data.startswith("cmtcancel:"))
async def cb_close_cancel(c: CallbackQuery):
    await c.answer("Скасовано")
    _PENDING_CLOSE.pop(c.from_user.id, None)
    await c.message.answer("Скасовано. Угоду не змінено.", reply_markup=main_menu_kb())


# Текст причини ремонту — реагуємо ЛИШЕ коли чекаємо причину
@dp.message()
async def catch_free_text(m: Message):
    ctx = _PENDING_CLOSE.get(m.from_user.id)
    if not ctx or ctx.get("stage") != "await_reason":
        return
    deal_id = ctx["deal_id"]
    fact_val = ctx["fact_val"]
    fact_name = ctx["fact_name"]
    reason = (m.text or "").strip()
    try:
        await _finalize_close(m.from_user.id, deal_id, fact_val, fact_name, reason_text=reason)
        await m.answer(f"✅ Угоду #{deal_id} закрито. Дані записані.")
        deal2 = await b24("crm.deal.get", id=deal_id)
        await send_deal_card(m.chat.id, deal2)
    except Exception as e:
        log.exception("finalize close (reason text) failed")
        await m.answer(f"❗️Помилка закриття: {e}")
    finally:
        _PENDING_CLOSE.pop(m.from_user.id, None)


# ----------------------------- Reports -------------------------------------

def _day_range_utc(day: datetime) -> Tuple[str, str]:
    """
    Формуємо [>=CLOSEDATE, <CLOSEDATE_NEXT] у форматі 'YYYY-MM-DD HH:MM:SS'.
    Використовуємо локальний день без TZ — Bitrix це приймає коректно.
    """
    d0 = day.replace(hour=0, minute=0, second=0, microsecond=0)
    d1 = d0 + timedelta(days=1)
    fmt = "%Y-%m-%d %H:%M:%S"
    return d0.strftime(fmt), d1.strftime(fmt)


async def _report_for_day(user_id: int, day: datetime) -> str:
    brigade = get_user_brigade(user_id)
    if not brigade:
        return "Спершу оберіть бригаду:",

    exec_opt = _BRIGADE_EXEC_OPTION_ID.get(brigade)
    if not exec_opt:
        return "Невідомий номер бригади."

    start_s, end_s = _day_range_utc(day)

    # закриті за день (WON) цiєю бригадою
    deals = await b24_list(
        "crm.deal.list",
        filter={
            "CATEGORY_ID": 20,
            "STAGE_ID": "C20:WON",
            "UF_CRM_1611995532420": exec_opt,  # Виконавець = Бригада N
            ">=CLOSEDATE": start_s,
            "<CLOSEDATE": end_s,
        },
        select=["ID", "TITLE", "TYPE_ID", "CLOSEDATE"],
        order={"ID": "DESC"},
    )

    type_map = await get_deal_type_map()
    total = len(deals)
    by_type: Dict[str, int] = {}
    for d in deals:
        t = type_map.get(d.get("TYPE_ID") or "", "Інше")
        by_type[t] = by_type.get(t, 0) + 1

    # скільки активних у колонці бригади зараз
    stage_code = _STAGE_BY_BRIGADE.get(brigade)
    active = await b24_list(
        "crm.deal.list",
        filter={"CLOSED": "N", "STAGE_ID": f"C20:{stage_code}"},
        select=["ID"],
    )
    active_count = len(active)

    # гарний вивід
    date_str = day.strftime("%d.%m.%Y")
    lines = [
        f"<b>Бригада №{brigade}, {date_str}</b>",
        "",
        f"<b>Закрито задач:</b> {total}",
    ]
    # бажані категорії — просто підставимо, якщо є
    for name in ("Аварійні роботи", "Ремонт", "Підключення", "Сервісні роботи"):
        if name in by_type:
            lines.append(f"{name} — {by_type[name]}")
    # інші типи (яких не було у переліку)
    other_types = [k for k in by_type.keys() if k not in {"Аварійні роботи", "Ремонт", "Підключення", "Сервісні роботи"}]
    for k in sorted(other_types):
        lines.append(f"{k} — {by_type[k]}")

    lines += [
        "",
        f"<b>Задач у колонці бригади:</b> {active_count}",
    ]
    return "\n".join(lines)


@dp.message(F.text == "📊 Звіт за сьогодні")
async def report_today(m: Message):
    text = await _report_for_day(m.from_user.id, datetime.now())
    if isinstance(text, tuple):
        await m.answer(text[0], reply_markup=pick_brigade_inline_kb())
    else:
        await m.answer(text, reply_markup=main_menu_kb())


@dp.message(F.text == "📉 Звіт за вчора")
async def report_yesterday(m: Message):
    text = await _report_for_day(m.from_user.id, datetime.now() - timedelta(days=1))
    if isinstance(text, tuple):
        await m.answer(text[0], reply_markup=pick_brigade_inline_kb())
    else:
        await m.answer(text, reply_markup=main_menu_kb())


# ----------------------------- Webhook plumbing ----------------------------

@app.on_event("startup")
async def on_startup():
    global HTTP
    HTTP = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))

    await bot.set_my_commands([
        BotCommand(command="start", description="Почати"),
        BotCommand(command="menu", description="Показати меню"),
        BotCommand(command="set_brigade", description="Вибрати бригаду"),
        BotCommand(command="deal_dump", description="Показати dump угоди"),
    ])

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

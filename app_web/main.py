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


# ----------------------------- UI helpers ---------------------------------

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
    await bot.send_message(
        chat_id,
        text,
        reply_markup=deal_keyboard(deal),
        disable_web_page_preview=True,
    )


# ----------------------------- Simple storage (brigade only) ---------------

_USER_BRIGADE: Dict[int, int] = {}


def get_user_brigade(user_id: int) -> Optional[int]:
    return _USER_BRIGADE.get(user_id)


def set_user_brigade(user_id: int, brigade: int) -> None:
    _USER_BRIGADE[user_id] = brigade


# ----------------------------- Close wizard with comment -------------------

# Перелік “швидких причин” (кнопки). Ключ -> видимий текст.
_QUICK_REASONS: Dict[str, str] = {
    "done_ok": "Підключено та протестовано ✅",
    "hw_installed": "Обладнання встановлено, все працює",
    "client_moved": "Завдання виконано, клієнт задоволений",
    "no_issues": "Без зауважень",
    "other": "Ввести свій коментар…",
}

# Очікування текстового коментаря: user_id -> {"deal_id": str}
_PENDING_COMMENT: Dict[int, Dict[str, str]] = {}


def _close_wizard_kb(deal_id: str) -> InlineKeyboardMarkup:
    rows = []
    # по 2 в рядок
    pair = []
    for key, label in _QUICK_REASONS.items():
        pair.append(InlineKeyboardButton(text=label, callback_data=f"cmtsel:{deal_id}:{key}"))
        if len(pair) == 2:
            rows.append(pair)
            pair = []
    if pair:
        rows.append(pair)
    rows.append([InlineKeyboardButton(text="❌ Скасувати", callback_data=f"cmtcancel:{deal_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _finalize_close_with_comment(user_id: int, deal_id: str, comment_text: str) -> None:
    """Append comment to COMMENTS and close to WON."""
    # 1) отримати угоду
    deal = await b24("crm.deal.get", id=deal_id)
    if not deal:
        raise RuntimeError("Deal not found")

    # 2) зібрати оновлений COMMENTS (акуратно додаємо новий блок)
    prev_comments = _strip_bb(deal.get("COMMENTS") or "")
    author = f"@user_{user_id}"  # можна замінити на username, якщо він є у Telegram
    new_block = f"[p]<b>Коментар закриття</b>: {html.escape(comment_text)} (від {html.escape(author)})[/p]"

    new_comments = new_block if not prev_comments else f"{prev_comments}\n\n{new_block}"

    # 3) закрити угоду у WON + записати COMMENTS
    category = str(deal.get("CATEGORY_ID") or "0")
    target_stage = f"C{category}:WON"
    await b24(
        "crm.deal.update",
        id=deal_id,
        fields={"STAGE_ID": target_stage, "COMMENTS": new_comments},
    )


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


# Інлайн-вибір бригади
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


# Текстова кнопка «📦 Мої угоди»
@dp.message(F.text == "📦 Мої угоди")
async def msg_my_deals(m: Message):
    brigade = get_user_brigade(m.from_user.id)
    if not brigade:
        await m.answer("Спершу оберіть бригаду:", reply_markup=pick_brigade_inline_kb())
        return

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


# Сумісність зі старою інлайн-кнопкою (якщо десь залишиться)
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


# ======== МАЙСТЕР ЗАКРИТТЯ З КОМЕНТАРЕМ ========

@dp.callback_query(F.data.startswith("close:"))
async def cb_close_deal_start(c: CallbackQuery):
    """Початок майстра: показати варіанти коментарів/ввід свого."""
    await c.answer()
    deal_id = c.data.split(":", 1)[1]
    _PENDING_COMMENT[c.from_user.id] = {"deal_id": deal_id}
    await c.message.answer(
        f"Закриваємо угоду #{deal_id}. Оберіть коментар або введіть свій текст повідомлення одним наступним повідомленням:",
        reply_markup=_close_wizard_kb(deal_id),
    )


@dp.callback_query(F.data.startswith("cmtsel:"))
async def cb_close_deal_quick(c: CallbackQuery):
    """Клік по готовому варіанту (включно з «Ввести свій коментар…»)."""
    await c.answer()
    parts = c.data.split(":")
    # cmtsel:{deal_id}:{key}
    if len(parts) < 3:
        return
    deal_id, key = parts[1], parts[2]

    if key == "other":
        # просимо ввести текст
        _PENDING_COMMENT[c.from_user.id] = {"deal_id": deal_id}
        await c.message.answer("Введіть ваш коментар одним повідомленням ⌨️")
        return

    # беремо текст з пресетів і закриваємо
    text = _QUICK_REASONS.get(key, "").strip()
    if not text:
        await c.message.answer("Не вдалося прочитати коментар.")
        return

    try:
        await _finalize_close_with_comment(c.from_user.id, deal_id, text)
        await c.message.answer(f"✅ Угоду #{deal_id} закрито. Коментар додано.")
        deal2 = await b24("crm.deal.get", id=deal_id)
        await send_deal_card(c.message.chat.id, deal2)
    except Exception as e:
        log.exception("close with quick comment failed")
        await c.message.answer(f"❗️Помилка закриття: {e}")
    finally:
        _PENDING_COMMENT.pop(c.from_user.id, None)


@dp.callback_query(F.data.startswith("cmtcancel:"))
async def cb_close_deal_cancel(c: CallbackQuery):
    await c.answer("Скасовано")
    _PENDING_COMMENT.pop(c.from_user.id, None)
    await c.message.answer("Скасовано. Угоду не змінено.", reply_markup=main_menu_kb())


# Текст наступним повідомленням — це власний коментар користувача
@dp.message()
async def catch_free_text(m: Message):
    ctx = _PENDING_COMMENT.get(m.from_user.id)
    if not ctx:
        return  # це не коментар до закриття — інші хендлери вже спрацювали вище
    deal_id = ctx.get("deal_id")
    text = (m.text or "").strip()
    if not text:
        await m.answer("Порожній коментар. Введіть текст або натисніть «❌ Скасувати».")
        return

    try:
        await _finalize_close_with_comment(m.from_user.id, deal_id, text)
        await m.answer(f"✅ Угоду #{deal_id} закрито. Коментар додано.")
        deal2 = await b24("crm.deal.get", id=deal_id)
        await send_deal_card(m.chat.id, deal2)
    except Exception as e:
        log.exception("close with custom comment failed")
        await m.answer(f"❗️Помилка закриття: {e}")
    finally:
        _PENDING_COMMENT.pop(m.from_user.id, None)


# ----------------------------- Webhook plumbing ----------------------------

@app.on_event("startup")
async def on_startup():
    global HTTP
    HTTP = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))

    # Команди бота
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

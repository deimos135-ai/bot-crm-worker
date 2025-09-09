# app_web/main.py
import asyncio
import html
import json
import logging
import re
from datetime import datetime, timedelta, timezone
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
    Contact,
)

from shared.settings import settings

# ----------------------------- Logging -------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("app")

# ----------------------------- App / Bot -----------------------------------
app = FastAPI()
bot = Bot(token=settings.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ----------------------------- Bitrix helpers ------------------------------
B24_BASE = settings.BITRIX_WEBHOOK_BASE.rstrip("/")
HTTP: aiohttp.ClientSession

async def b24(method: str, **params) -> Any:
    """Single call to Bitrix REST method."""
    url = f"{B24_BASE}/{method}.json"
    async with HTTP.post(url, json=params) as resp:
        data = await resp.json()
        if "error" in data:
            raise RuntimeError(f"B24 error: {data['error']}: {data.get('error_description')}")
        return data.get("result")

async def b24_list(method: str, *, page_size: int = 200, throttle: float = 0.2, **params) -> List[Dict[str, Any]]:
    """Paginator for Bitrix list endpoints."""
    start = 0
    items: List[Dict[str, Any]] = []
    while True:
        payload = dict(params)
        payload["start"] = start
        res = await b24(method, **payload)
        chunk = res or []
        if isinstance(chunk, dict) and "items" in chunk:
            chunk = chunk.get("items", [])
        items.extend(chunk)
        log.info("[b24_list] %s got %s items (total %s) start=%s", method, len(chunk), len(items), start)
        if len(chunk) < page_size:
            break
        start += page_size
        if throttle:
            await asyncio.sleep(throttle)
    return items

# ----------------------------- AUTH (in-memory) ----------------------------
# Авторизація зберігається в оперативній пам'яті процеса
_AUTH_OK: Dict[int, bool] = {}         # tg_user_id -> authed?
# Бригада — також у пам'яті (як було у твоїй першій ревізії)
_USER_BRIGADE: Dict[int, int] = {}     # tg_user_id -> brigade number

def is_authed_sync(uid: int) -> bool:
    return _AUTH_OK.get(uid, False)

def mark_authed(uid: int) -> None:
    _AUTH_OK[uid] = True

def get_user_brigade(user_id: int) -> Optional[int]:
    return _USER_BRIGADE.get(user_id)

def set_user_brigade(user_id: int, brigade: int) -> None:
    _USER_BRIGADE[user_id] = brigade

def request_phone_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Поділитись номером", request_contact=True)]],
        resize_keyboard=True, one_time_keyboard=True, selective=False
    )
    return kb

# Нормалізація телефону з Telegram/Bitrix до цифр, без пробілів/-, зі збереженням країни.
def _digits_only(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

def normalize_phone(raw: str) -> Tuple[str, List[str]]:
    """
    Повертає (digits, variants) для пошуку у Bitrix user.search / user.get
    Приклади:

    """
    digits = _digits_only(raw)
    variants: List[str] = []
    if not digits:
        return "", variants

    # готуємо варіанти для різних полів і різного формату введення
    v_e164 = digits if digits.startswith("380") else f"38{digits}" if digits.startswith("0") else digits
    v_e164_plus = f"+{v_e164}"
    v_nat = digits[-10:] if len(digits) >= 10 else digits  # 0XXXXXXXXX або без 0
    if len(v_nat) == 10 and not v_nat.startswith("0"):
        v_nat = f"0{v_nat}" if len(v_nat) == 9 else v_nat
    v_tail9 = v_nat[-9:] if len(v_nat) >= 9 else v_nat

    # Порядок важливий — від більш точного до більш «вільного»
    variants = [v_e164, digits, v_e164_plus, v_nat, v_tail9]
    # унікалізуємо, зберігши порядок
    seen = set()
    uniq: List[str] = []
    for v in variants:
        if v and v not in seen:
            uniq.append(v)
            seen.add(v)
    return digits, uniq

async def b24_find_employee_by_phone(raw_phone: str) -> Optional[Dict[str, Any]]:
    """
    Шукаємо тільки серед користувачів Bitrix (співробітників).
    Повертаємо словник користувача, якщо знайдено.
    """
    digits, variants = normalize_phone(raw_phone)
    log.info("[contact] raw='%s' digits='%s' variants=%s", raw_phone, digits, variants)
    if not digits:
        return None

    # 1) user.search по FIND
    for v in variants:
        try:
            log.info("[b24.find] user.search FIND='%s'", v)
            users = await b24("user.search", FIND=v)
            log.info("[b24.find] user.search FIND='%s' -> %s users", v, len(users or []))
            if users:
                # Фільтруємо за полями телефонів для впевненості
                for u in users:
                    phones = [
                        (u.get("WORK_PHONE") or "").strip() or None,
                        (u.get("PERSONAL_PHONE") or "").strip() or None,
                        (u.get("PERSONAL_MOBILE") or "").strip() or None,
                    ]
                    phones = [p for p in phones if p]
                    if any(_digits_only(p).endswith(digits[-9:]) for p in phones):
                        log.info("[b24.find] MATCH(search) uid=%s name='%s' phones=%s raw='%s'",
                                 u.get("ID"), f"{u.get('NAME','')} {u.get('LAST_NAME','')}".strip(), phones, raw_phone)
                        return u
        except Exception as e:
            log.warning("[b24.find] user.search error for '%s': %s", v, e)

    # 2) user.get по конкретних полях (найтиповіші)
    for field in ("PERSONAL_MOBILE", "PERSONAL_PHONE", "WORK_PHONE"):
        for v in variants:
            try:
                # Bitrix user.get: FILTER={FIELD: 'value'}
                filt = {field: v}
                log.info("[b24.find] user.get FILTER=%s", filt)
                u = await b24("user.get", FILTER=filt)
                if isinstance(u, list) and u:
                    u = u[0]
                if u and isinstance(u, dict):
                    phones = [
                        (u.get("WORK_PHONE") or "").strip() or None,
                        (u.get("PERSONAL_PHONE") or "").strip() or None,
                        (u.get("PERSONAL_MOBILE") or "").strip() or None,
                    ]
                    phones = [p for p in phones if p]
                    log.info("[b24.find] MATCH(get) uid=%s name='%s' phones=%s raw='%s'",
                             u.get("ID"), f"{u.get('NAME','')} {u.get('LAST_NAME','')}".strip(), phones, raw_phone)
                    return u
            except Exception as e:
                log.warning("[b24.find] user.get error field=%s v='%s': %s", field, v, e)

    log.info("[b24.find] no matches for raw='%s'", raw_phone)
    return None

async def ensure_authed_or_ask(m: Message) -> bool:
    """Перевіряє авторизацію; якщо ні — просить поділитись номером. Повертає True якщо вже авторизований."""
    if is_authed_sync(m.from_user.id):
        return True
    await m.answer(
        "Щоб працювати з ботом, авторизуйтесь — поділіться номером телефону 👇",
        reply_markup=request_phone_kb()
    )
    return False

# ----------------------------- Caches --------------------------------------
_DEAL_TYPE_MAP: Optional[Dict[str, str]] = None
_ROUTER_ENUM_MAP: Optional[Dict[str, str]] = None      # UF_CRM_1602756048
_TARIFF_ENUM_MAP: Optional[Dict[str, str]] = None      # UF_CRM_1610558031277
_FACT_ENUM_LIST: Optional[List[Tuple[str, str]]] = None  # (VALUE, NAME)

async def get_deal_type_map() -> Dict[str, str]:
    global _DEAL_TYPE_MAP
    if _DEAL_TYPE_MAP is None:
        items = await b24("crm.status.list", filter={"ENTITY_ID": "DEAL_TYPE"})
        _DEAL_TYPE_MAP = {i["STATUS_ID"]: i["NAME"] for i in items}
        log.info("[cache] DEAL_TYPE map loaded: %s entries", len(_DEAL_TYPE_MAP))
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
    UF_CRM_1602766787968: повертає список (option_id, option_name).
    option_id = LIST[].ID, option_name = LIST[].VALUE
    """
    global _FACT_ENUM_LIST
    if _FACT_ENUM_LIST is None:
        fields = await b24("crm.deal.userfield.list", order={"SORT": "ASC"})
        uf = next((f for f in fields if f.get("FIELD_NAME") == "UF_CRM_1602766787968"), None)
        lst: List[Tuple[str, str]] = []
        if uf and isinstance(uf.get("LIST"), list):
            for o in uf["LIST"]:
                opt_id = str(o.get("ID") or "")
                opt_name = str(o.get("VALUE") or "")
                if not opt_id:
                    continue
                lst.append((opt_id, opt_name))
        _FACT_ENUM_LIST = lst
        log.info("[cache] FACT enum loaded: %s options", len(_FACT_ENUM_LIST))
    return _FACT_ENUM_LIST

# ----------------------------- UI helpers ----------------------------------
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

    # Що зроблено + Причина ремонту
    fact_val = str(deal.get("UF_CRM_1602766787968") or "")
    fact_name = "—"
    if fact_val:
        facts = await get_fact_enum_list()
        fact_name = next((name for val, name in facts if val == fact_val), fact_val)

    reason_text = (deal.get("UF_CRM_1702456465911") or "").strip() or "—"

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
        f"<b>Що зроблено:</b> {html.escape(fact_name)}",
        f"<b>Причина ремонту:</b> {html.escape(reason_text)}",
        "",
        contact_line,
        "",
        f"<a href=\"{link}\">Відкрити в CRM</a>",
    ]
    return f"<b>{head}</b>\n\n" + "\n".join(body_lines)

def deal_keyboard(deal: Dict[str, Any]) -> InlineKeyboardMarkup:
    deal_id = str(deal.get("ID"))
    kb = [[InlineKeyboardButton(text="✅ Закрити угоду", callback_data=f"close:{deal_id}")]]
    return InlineKeyboardMarkup(inline_keyboard=kb)

async def send_deal_card(chat_id: int, deal: Dict[str, Any]) -> None:
    text = await render_deal_card(deal)
    await bot.send_message(chat_id, text, reply_markup=deal_keyboard(deal), disable_web_page_preview=True)

# ----------------------------- Brigade mapping -----------------------------
# mapping "brigade number" -> UF_CRM_1611995532420[] option IDs (brigade items)
_BRIGADE_EXEC_OPTION_ID = {1: 5494, 2: 5496, 3: 5498, 4: 5500, 5: 5502}
# mapping brigade -> stage code in pipeline C20
_BRIGADE_STAGE = {1: "UC_XF8O6V", 2: "UC_0XLPCN", 3: "UC_204CP3", 4: "UC_TNEW3Z", 5: "UC_RMBZ37"}

# ----------------------------- Close wizard --------------------------------
_PENDING_CLOSE: Dict[int, Dict[str, Any]] = {}
_FACTS_PER_PAGE = 8  # 1 опція = 1 рядок; пагінація по 8

def _facts_page_kb(deal_id: str, page: int, facts: List[Tuple[str, str]]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    total_pages = max(1, (len(facts) + _FACTS_PER_PAGE - 1) // _FACTS_PER_PAGE)
    page = max(0, min(page, total_pages - 1))

    start = page * _FACTS_PER_PAGE
    chunk = facts[start:start + _FACTS_PER_PAGE]

    for val, name in chunk:
        rows.append([InlineKeyboardButton(text=name[:64], callback_data=f"factsel:{deal_id}:{val}")])

    if total_pages > 1:
        nav: List[InlineKeyboardButton] = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="« Назад", callback_data=f"factpage:{deal_id}:{page-1}"))
        nav.append(InlineKeyboardButton(text=f"Стор. {page+1}/{total_pages}", callback_data="noop"))
        if page + 1 < total_pages:
            nav.append(InlineKeyboardButton(text="Вперед »", callback_data=f"factpage:{deal_id}:{page+1}"))
        rows.append(nav)

    rows.append([InlineKeyboardButton(text="❌ Скасувати", callback_data=f"cmtcancel:{deal_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def _finalize_close(user_id: int, deal_id: str, fact_val: str, fact_name: str, reason_text: str) -> None:
    deal = await b24("crm.deal.get", id=deal_id)
    if not deal:
        raise RuntimeError("Deal not found")
    category = str(deal.get("CATEGORY_ID") or "0")
    target_stage = f"C{category}:WON"

    prev_comments = _strip_bb(deal.get("COMMENTS") or "")
    block = f"[p]<b>Закриття:</b> {html.escape(fact_name)}[/p]"
    if reason_text:
        block += f"\n[p]<b>Причина ремонту:</b> {html.escape(reason_text)}[/p]"
    new_comments = block if not prev_comments else f"{prev_comments}\n\n{block}"

    brigade = get_user_brigade(user_id)
    exec_list = []
    if brigade and brigade in _BRIGADE_EXEC_OPTION_ID:
        exec_list = [_BRIGADE_EXEC_OPTION_ID[brigade]]

    fields = {
        "STAGE_ID": target_stage,
        "COMMENTS": new_comments,
        "UF_CRM_1602766787968": fact_val,     # Що по факту зробили (enum VALUE)
        "UF_CRM_1702456465911": reason_text,  # Причина ремонту (free text)
    }
    if exec_list:
        fields["UF_CRM_1611995532420"] = exec_list  # Виконавець (multi)

    await b24("crm.deal.update", id=deal_id, fields=fields)

# ----------------------------- Report taxonomy -----------------------------
REPORT_CLASS_LABELS = {
    "connection": "Підключення",
    "repair": "Ремонти",
    "service": "Сервісні роботи",
    "reconnection": "Перепідключення",
    "accident": "Аварії",
    "construction": "Будівництво",
    "linework": "Роботи по лінії",
    "cc_request": "Звернення в КЦ",
    "other": "Інше",
}

REPORT_CLASS_ORDER = [
    "accident",
    "repair",
    "connection",
    "service",
    "reconnection",
    "construction",
    "linework",
    "cc_request",
    "other",
]

def normalize_type(type_name: str) -> str:
    """
    Мапимо назву типу угоди (Bitrix, будь-якою мовою) у наш клас звіту.
    """
    t = (type_name or "").strip().lower()

    mapping_exact = {
        "підключення": "connection",
        "подключение": "connection",

        "ремонт": "repair",

        "сервісні роботи": "service",
        "сервисные работы": "service",
        "сервіс": "service",
        "сервис": "service",

        "перепідключення": "reconnection",
        "переподключение": "reconnection",

        "аварія": "accident",
        "авария": "accident",

        "будівництво": "construction",
        "строительство": "construction",

        "роботи по лінії": "linework",
        "работы по линии": "linework",

        "звернення в кц": "cc_request",
        "обращение в кц": "cc_request",

        "не выбран": "other",
        "не вибрано": "other",
        "інше": "other",
        "прочее": "other",
    }
    if t in mapping_exact:
        return mapping_exact[t]

    # м'які правила
    if any(k in t for k in ("підключ", "подключ")):
        return "connection"
    if "ремонт" in t:
        return "repair"
    if any(k in t for k in ("сервіс", "сервис")):
        return "service"
    if any(k in t for k in ("перепідключ", "переподключ")):
        return "reconnection"
    if any(k in t for k in ("авар",)):
        return "accident"
    if any(k in t for k in ("будівниц", "строит")):
        return "construction"
    if any(k in t for k in ("ліні", "линии")):
        return "linework"
    if any(k in t for k in ("кц", "контакт-центр", "колл-центр", "call")):
        return "cc_request"
    return "other"

# ----------------------------- Report helpers ------------------------------
def _tz_ua_now() -> datetime:
    return datetime.now(timezone.utc)

def _day_bounds(offset_days: int = 0) -> Tuple[str, str, str]:
    now = _tz_ua_now()
    start = (now + timedelta(days=-offset_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    label = start.astimezone(timezone.utc).strftime("%d.%m.%Y")
    return label, start.isoformat(), end.isoformat()

async def build_daily_report(brigade: int, offset_days: int) -> Tuple[str, Dict[str, int], int]:
    if brigade not in _BRIGADE_STAGE:
        raise RuntimeError("Unknown brigade")

    label, frm, to = _day_bounds(offset_days)
    deal_type_map = await get_deal_type_map()

    exec_opt = _BRIGADE_EXEC_OPTION_ID.get(brigade)
    filter_closed = {"STAGE_ID": "C20:WON", ">=DATE_MODIFY": frm, "<DATE_MODIFY": to}
    if exec_opt:
        filter_closed["UF_CRM_1611995532420"] = exec_opt

    log.info("[report] closed filter: %s", filter_closed)

    closed_deals = await b24_list(
        "crm.deal.list",
        order={"DATE_MODIFY": "ASC"},
        filter=filter_closed,
        select=["ID", "TYPE_ID"],
        page_size=200,
    )
    log.info("[report] closed deals fetched: %s", len(closed_deals))

    counts: Dict[str, int] = {k: 0 for k in REPORT_CLASS_LABELS.keys()}
    for d in closed_deals:
        tcode = d.get("TYPE_ID") or ""
        tname = deal_type_map.get(tcode, tcode)
        cls = normalize_type(tname)
        counts[cls] = counts.get(cls, 0) + 1

    stage_code = _BRIGADE_STAGE[brigade]
    filter_active = {"CLOSED": "N", "STAGE_ID": f"C20:{stage_code}"}
    log.info("[report] active filter: %s", filter_active)

    active_deals = await b24_list(
        "crm.deal.list",
        order={"ID": "DESC"},
        filter=filter_active,
        select=["ID"],
        page_size=200,
    )
    active_left = len(active_deals)
    log.info("[report] active deals fetched: %s", active_left)

    return label, counts, active_left

def format_report(brigade: int, date_label: str, counts: Dict[str, int], active_left: int) -> str:
    total = sum(counts.values())
    lines = [
        f"<b>Бригада №{brigade} — {date_label}</b>",
        "",
        f"<b>Закрито задач:</b> {total}",
        "",
    ]
    # показуємо у фіксованому порядку, ховаємо нулі
    for key in REPORT_CLASS_ORDER:
        val = counts.get(key, 0)
        if val:
            lines.append(f"{REPORT_CLASS_LABELS.get(key, key)} — {val}")

    if len(lines) > 3 and lines[-1] != "":
        lines.append("")

    lines.append(f"<b>Активних задач на бригаді залишилось:</b> {active_left}")
    return "\n".join(lines)

# ----------------------------- Handlers ------------------------------------
@dp.message(Command("start"))
async def cmd_start(m: Message):
    # 1) авторизація
    if not is_authed_sync(m.from_user.id):
        await m.answer(
            "Готові працювати ✅\n\nЩоб продовжити, поділіться номером телефону .",
            reply_markup=request_phone_kb()
        )
        return

    # 2) як було
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
    if not is_authed_sync(m.from_user.id):
        await ensure_authed_or_ask(m)
        return
    await m.answer("Меню відкрито 👇", reply_markup=main_menu_kb())

@dp.message(Command("set_brigade"))
async def cmd_set_brigade(m: Message):
    if not is_authed_sync(m.from_user.id):
        await ensure_authed_or_ask(m)
        return
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
    if not is_authed_sync(c.from_user.id):
        await c.answer()
        await c.message.answer("Спершу авторизуйтесь — поділіться номером телефону:", reply_markup=request_phone_kb())
        return
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
    if not is_authed_sync(m.from_user.id):
        await ensure_authed_or_ask(m)
        return
    brigade = get_user_brigade(m.from_user.id)
    if not brigade:
        await m.answer("Спершу оберіть бригаду:", reply_markup=pick_brigade_inline_kb())
        return
    stage_code = _BRIGADE_STAGE.get(brigade)
    if not stage_code:
        await m.answer("Невірний номер бригади.", reply_markup=main_menu_kb())
        return

    await m.answer(f"📦 Завантажую угоди для бригади №{brigade}…", reply_markup=main_menu_kb())

    deals: List[Dict[str, Any]] = await b24_list(
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
            # для картки:
            "UF_CRM_1602766787968",     # Що зроблено
            "UF_CRM_1702456465911",     # Причина ремонту
        ],
        page_size=100,
    )
    if not deals:
        await m.answer("Немає активних угод.", reply_markup=main_menu_kb())
        return
    for d in deals[:25]:
        await send_deal_card(m.chat.id, d)

@dp.callback_query(F.data == "my_deals")
async def cb_my_deals(c: CallbackQuery):
    if not is_authed_sync(c.from_user.id):
        await c.answer()
        await c.message.answer("Спершу авторизуйтесь — поділіться номером телефону:", reply_markup=request_phone_kb())
        return
    await c.answer()
    await msg_my_deals(c.message)

@dp.message(F.text == "📋 Мої задачі")
async def msg_tasks(m: Message):
    if not is_authed_sync(m.from_user.id):
        await ensure_authed_or_ask(m)
        return
    await m.answer("Задачі ще в розробці 🛠️", reply_markup=main_menu_kb())

# --------- Закриття угоди: «що зроблено» + причина ------------------------
@dp.callback_query(F.data.startswith("close:"))
async def cb_close_deal_start(c: CallbackQuery):
    if not is_authed_sync(c.from_user.id):
        await c.answer()
        await c.message.answer("Спершу авторизуйтесь — поділіться номером телефону:", reply_markup=request_phone_kb())
        return
    await c.answer()
    deal_id = c.data.split(":", 1)[1]
    facts = await get_fact_enum_list()
    _PENDING_CLOSE[c.from_user.id] = {"deal_id": deal_id, "stage": "pick_fact", "page": 0}
    await c.message.answer(
        f"Закриваємо угоду <a href=\"https://{settings.B24_DOMAIN}/crm/deal/details/{deal_id}/\">#{deal_id}</a>. Оберіть, що зроблено:",
        reply_markup=_facts_page_kb(deal_id, 0, facts),
        disable_web_page_preview=True,
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

# ---------- приймаємо ТІЛЬКИ коли чекаємо текст причини -------------------
@dp.message(lambda m: _PENDING_CLOSE.get(m.from_user.id, {}).get("stage") == "await_reason")
async def catch_reason_text(m: Message):
    if not is_authed_sync(m.from_user.id):
        # теоретично не повинно статись, але про всяк
        await ensure_authed_or_ask(m)
        return
    ctx = _PENDING_CLOSE.get(m.from_user.id)
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
@dp.message(F.text == "📊 Звіт за сьогодні")
async def msg_report_today(m: Message):
    if not is_authed_sync(m.from_user.id):
        await ensure_authed_or_ask(m)
        return
    brigade = get_user_brigade(m.from_user.id)
    if not brigade:
        await m.answer("Спершу оберіть бригаду:", reply_markup=pick_brigade_inline_kb())
        return
    try:
        label, counts, active_left = await build_daily_report(brigade, offset_days=0)
        await m.answer(format_report(brigade, label, counts, active_left), reply_markup=main_menu_kb())
    except Exception as e:
        log.exception("report today failed")
        await m.answer(f"❗️Помилка формування звіту: {e}")

@dp.message(F.text == "📉 Звіт за вчора")
async def msg_report_yesterday(m: Message):
    if not is_authed_sync(m.from_user.id):
        await ensure_authed_or_ask(m)
        return
    brigade = get_user_brigade(m.from_user.id)
    if not brigade:
        await m.answer("Спершу оберіть бригаду:", reply_markup=pick_brigade_inline_kb())
        return
    try:
        label, counts, active_left = await build_daily_report(brigade, offset_days=1)
        await m.answer(format_report(brigade, label, counts, active_left), reply_markup=main_menu_kb())
    except Exception as e:
        log.exception("report yesterday failed")
        await m.answer(f"❗️Помилка формування звіту: {e}")

# ----------------------------- Dev helpers ---------------------------------
@dp.message(Command("deal_dump"))
async def deal_dump(m: Message):
    if not is_authed_sync(m.from_user.id):
        await ensure_authed_or_ask(m)
        return
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

# ----------------------------- AUTH handlers -------------------------------
@dp.message(F.contact)
async def on_contact(m: Message):
    """Обробляємо натискання кнопки 'Поділитись номером'."""
    c: Optional[Contact] = m.contact
    if not c or not c.phone_number:
        await m.answer("Не отримав номер телефону. Спробуйте ще раз:", reply_markup=request_phone_kb())
        return
    raw = c.phone_number
    digits, _ = normalize_phone(raw)
    log.info("[whoami_phone] user_id=%s username=%s", m.from_user.id, m.from_user.username or "-")
    log.info("[contact] from_user_id=%s raw='%s' digits='%s'", m.from_user.id, raw, digits)

    user = await b24_find_employee_by_phone(digits)
    if not user:
        await m.answer(
            "❌ Не знайшов співробітника з таким номером .\n"
            "Перевірте номер у профілі співробітника (поле «Мобільний»).",
            reply_markup=request_phone_kb(),
        )
        log.info("[auth] NOT FOUND in Bitrix for user_id=%s phone='%s'", m.from_user.id, digits)
        return

    # Ок — авторизуємо
    mark_authed(m.from_user.id)
    full_name = f"{user.get('NAME','')} {user.get('LAST_NAME','')}".strip() or "—"
    phone_dbg = (user.get("PERSONAL_MOBILE") or user.get("PERSONAL_PHONE") or user.get("WORK_PHONE") or "").strip()
    log.info("[auth] OK matched bx_user_id=%s name='%s' phone='%s' for tg_user_id=%s",
             user.get("ID"), full_name, phone_dbg, m.from_user.id)

    b = get_user_brigade(m.from_user.id)
    text = f"✅ Авторизація успішна. Вітаю, {html.escape(full_name)}!"
    if b:
        text += f"\nПоточна бригада: №{b}"
    else:
        text += "\nОберіть вашу бригаду нижче ⬇️"
    await m.answer(text, reply_markup=main_menu_kb())
    if not b:
        await m.answer("Швидкий вибір бригади:", reply_markup=pick_brigade_inline_kb())

# ----------------------------- Webhook plumbing ----------------------------
@app.on_event("startup")
async def on_startup():
    global HTTP
    HTTP = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))

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

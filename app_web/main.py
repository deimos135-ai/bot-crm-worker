import os
import json
import asyncio
import datetime as dt
from typing import Optional
from contextlib import suppress

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import CommandStart, Command
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

from fastapi import FastAPI, Request
from starlette.responses import JSONResponse
from starlette import status

from shared.settings import settings
from shared.team_names import TEAMS
from shared.tz import KYIV_TZ
from shared.repo import (
    connect, get_user, upsert_user_team,
    ensure_schema_and_seed, set_user_bitrix_id
)
from shared.bx import (
    list_tasks, complete_task, add_comment, search_user_by_email,
    get_task, get_deal, get_contact,
    list_deal_stages, list_deals, move_deal_to_stage, comment_deal
)
from worker.report_worker import daily_loop, build_full_report


# --------- Deals settings (env)
DEAL_CATEGORY_ID = int(os.getenv("DEAL_CATEGORY_ID", "0") or 0)
DEAL_DONE_STAGE_ID = os.getenv("DEAL_DONE_STAGE_ID", "").strip()
try:
    TEAM_STAGE_MAP = json.loads(os.getenv("TEAM_STAGE_MAP", "{}"))
except Exception:
    TEAM_STAGE_MAP = {}

def _normalize(s: str) -> str:
    return "".join(str(s).lower().replace("№", "").split())

async def _resolve_team_stage_id(team_id: int) -> str:
    sid = str(TEAM_STAGE_MAP.get(str(team_id), TEAM_STAGE_MAP.get(team_id, ""))).strip()
    if sid:
        return sid
    team_name = TEAMS.get(team_id, "")
    if not (DEAL_CATEGORY_ID and team_name):
        return ""
    try:
        stages = list_deal_stages(DEAL_CATEGORY_ID)
    except Exception:
        return ""
    tn = _normalize(team_name)
    for st in stages:
        name = st.get("NAME") or st.get("name") or ""
        code = st.get("STATUS_ID") or st.get("ID")
        nrm = _normalize(name)
        if tn in nrm or tn.replace("бригада", "brigada") in nrm:
            return str(code)
    return ""


# --------- Bot / Web
bot = Bot(settings.BOT_TOKEN)
dp = Dispatcher()
app = FastAPI()


# ========= START & TEAM =========
@dp.message(CommandStart())
async def start(m: types.Message):
    conn = await connect()
    row = await get_user(conn, m.from_user.id)
    await conn.close()

    if row and row["team_id"]:
        kb = InlineKeyboardBuilder()
        kb.button(text="📋 Мої задачі", callback_data="tasks:open:open")
        kb.button(text="🔁 Змінити бригаду", callback_data="team:change")
        kb.adjust(1, 1)
        await m.answer(
            f"Ви у бригаді: *{TEAMS.get(row['team_id'], '?')}*.\nГотові працювати ✅",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb.as_markup(),
        )
        return

    kb = InlineKeyboardBuilder()
    for tid, name in TEAMS.items():
        kb.button(text=name, callback_data=f"team:set:{tid}")
    kb.adjust(2, 3)
    await m.answer("Оберіть вашу бригаду:", reply_markup=kb.as_markup())


@dp.callback_query(F.data.startswith("team:change"))
async def change_team(c: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    for tid, name in TEAMS.items():
        kb.button(text=name, callback_data=f"team:set:{tid}")
    kb.adjust(2, 3)
    with suppress(Exception):
        await c.message.edit_text("Оберіть вашу бригаду:", reply_markup=kb.as_markup())
    await c.answer()


@dp.callback_query(F.data.startswith("team:set:"))
async def team_set(c: types.CallbackQuery):
    tid = int(c.data.split(":")[-1])
    full_name = f"{c.from_user.first_name or ''} {c.from_user.last_name or ''}".strip()
    conn = await connect()
    await upsert_user_team(conn, c.from_user.id, full_name, tid)
    await conn.close()
    text = f"Бригаду встановлено: *{TEAMS.get(tid, '—')}*. Готово ✅"
    kb = InlineKeyboardBuilder()
    kb.button(text="📋 Мої задачі", callback_data="tasks:open:open")
    with suppress(Exception):
        await c.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb.as_markup())
    await c.answer("Збережено ✅", show_alert=False)


# ========= DIAGNOSTICS =========
@dp.message(Command("whoami"))
async def whoami(m: types.Message):
    conn = await connect()
    u = await get_user(conn, m.from_user.id)
    await conn.close()
    team = TEAMS.get(u["team_id"]) if u and u.get("team_id") else "—"
    bx = u.get("bitrix_user_id") if u else None
    await m.answer(f"TG: {m.from_user.id}\nTeam: {team}\nBitrix ID: {bx or 'не прив’язано'}")


@dp.message(Command("bind"))
async def bind_email(m: types.Message):
    parts = (m.text or "").split()
    if len(parts) != 2 or "@" not in parts[1]:
        await m.answer("Приклад: `/bind user@company.com`", parse_mode=ParseMode.MARKDOWN)
        return
    email = parts[1].strip()
    try:
        res = search_user_by_email(email) or []
        user = res[0] if isinstance(res, list) and res else None
        bx_id = int(user.get("ID")) if user else None
        if not bx_id:
            await m.answer("Не знайшов користувача в Bitrix 🤔")
            return
        conn = await connect()
        await set_user_bitrix_id(conn, m.from_user.id, bx_id)
        await conn.close()
        await m.answer(f"Прив’язано Bitrix ID: {bx_id} ✅")
    except Exception as e:
        await m.answer(f"Не вдалось прив’язати: {e!s}")


# ========= TASKS =========
def _extract_deal_id_from_task(task: dict) -> Optional[int]:
    cand = task.get("UF_CRM_TASK") or []
    if isinstance(cand, str):
        cand = [cand]
    for s in cand:
        if str(s).startswith("D_"):
            try:
                return int(str(s).split("_")[1])
            except Exception:
                pass
    return None

def _format_deal_for_message(deal: dict, contact: Optional[dict]) -> str:
    type_id = deal.get("TYPE_ID") or "—"
    category = deal.get("CATEGORY_ID") or "—"
    comment = (deal.get("COMMENTS") or "").strip() or "—"
    address = deal.get("ADDRESS") or "—"
    router = deal.get("UF_CRM_ROUTER") or "—"
    router_sum = deal.get("UF_CRM_ROUTER_PRICE") or "—"
    contact_line = "—"
    if contact:
        name = " ".join(filter(None, [contact.get("NAME"), contact.get("LAST_NAME")]))
        phone = ""
        if contact.get("PHONE"):
            phone = contact["PHONE"][0].get("VALUE", "")
        contact_line = f"{name} {phone}"
    return "\n".join([
        f"Тип сделки: {type_id}",
        f"Категорія: {category}",
        f"Коментар: {comment}",
        f"Адреса: {address}",
        f"Роутер: {router}",
        f"Вартість роутера: {router_sum}",
        f"Контакт: {contact_line}",
    ])

def _task_line(t: dict, mode: str) -> str:
    tid = t.get("ID")
    title = t.get("TITLE")
    return f"• #{tid}: {title}"

def _build_task_row_kb(tid: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="ℹ️ Деталі", callback_data=f"task:details:{tid}")
    kb.button(text="✅ Закрити", callback_data=f"task:done:{tid}")
    kb.adjust(2)
    return kb.as_markup()


@dp.callback_query(F.data.startswith("tasks:open"))
async def tasks_open_cb(c: types.CallbackQuery):
    with suppress(Exception):
        await c.answer()
    try:
        await c.message.edit_text("📦 Завантажую задачі…")
    except TelegramBadRequest:
        pass

    conn = await connect()
    u = await get_user(conn, c.from_user.id)
    await conn.close()
    bx_id = u["bitrix_user_id"] if u else None
    if not bx_id:
        await bot.send_message(c.message.chat.id, "Спочатку прив’яжіть Bitrix: /bind email")
        return

    fields = ["ID","TITLE","DEADLINE","STATUS","UF_CRM_TASK"]
    filters = [{"RESPONSIBLE_ID": bx_id, "!STATUS": 5}]

    bag = {}
    for f in filters:
        try:
            res = list_tasks(f, fields)
            for t in res.get("result", []):
                bag[str(t["ID"])] = t
        except Exception:
            pass
    tasks = list(bag.values())
    if not tasks:
        await bot.send_message(c.message.chat.id, "Задач за запитом не знайдено 🙂")
        return

    await bot.send_message(c.message.chat.id, f"Задачі (до 20):")
    for t in tasks[:20]:
        tid = int(t["ID"])
        await bot.send_message(c.message.chat.id, _task_line(t, "open"), reply_markup=_build_task_row_kb(tid))
        await asyncio.sleep(0.05)


@dp.callback_query(F.data.startswith("task:details:"))
async def task_details(c: types.CallbackQuery):
    with suppress(Exception):
        await c.answer()
    tid = int(c.data.split(":")[-1])
    task = get_task(tid) or {}
    deal_id = _extract_deal_id_from_task(task)
    deal, contact = {}, None
    if deal_id:
        deal = get_deal(deal_id) or {}
        if deal.get("CONTACT_ID"):
            contact = get_contact(int(deal["CONTACT_ID"])) or None
    text = f"# {tid} • {task.get('TITLE','')}\n\n"
    text += _format_deal_for_message(deal, contact) if deal else "Прив’язану угоду не знайдено."
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Закрити", callback_data=f"task:done:{tid}")
    kb.button(text="🔙 Назад", callback_data="tasks:open:open")
    kb.adjust(2)
    with suppress(Exception):
        await c.message.edit_text(text, reply_markup=kb.as_markup())


@dp.callback_query(F.data.startswith("task:done:"))
async def task_done_cb(c: types.CallbackQuery):
    with suppress(Exception):
        await c.answer()
    tid = int(c.data.split(":")[-1])
    try:
        complete_task(tid)
        add_comment(tid, "Закрито через Telegram-бот ✅")
        await c.message.edit_reply_markup(reply_markup=None)
    except Exception as e:
        await bot.send_message(c.message.chat.id, f"Не вдалося закрити: {e!s}")


# ========= DEALS / REPORTS / WEBHOOK / STARTUP =========
# (залишаються без змін — ті, що в тебе вже працювали)

# app_web/main.py
import os
import json
import math
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
    # tasks
    list_tasks, complete_task, add_comment, search_user_by_email, get_task,
    # deals
    list_deal_stages, list_deals, move_deal_to_stage, comment_deal, get_deal, get_contact
)
from worker.report_worker import daily_loop, build_full_report


# --------- Deals settings (env)
DEAL_CATEGORY_ID = int(os.getenv("DEAL_CATEGORY_ID", "0") or 0)       # напр. 20
DEAL_DONE_STAGE_ID = os.getenv("DEAL_DONE_STAGE_ID", "").strip()      # напр. C20:WON або C20:UC_xxx
try:
    TEAM_STAGE_MAP = json.loads(os.getenv("TEAM_STAGE_MAP", "{}"))    # {"1":"C20:UC_..","2":"C20:UC_..",...}
except Exception:
    TEAM_STAGE_MAP = {}


# --------- Helpers (stages)
def _normalize(s: str) -> str:
    return "".join(str(s).lower().replace("№", "").split())

async def _resolve_team_stage_id(team_id: int) -> str:
    # 1) explicit map via env
    sid = str(TEAM_STAGE_MAP.get(str(team_id), TEAM_STAGE_MAP.get(team_id, ""))).strip()
    if sid:
        return sid
    # 2) try to find by stage name containing team name
    team_name = TEAMS.get(team_id, "")
    if not (DEAL_CATEGORY_ID and team_name):
        return ""
    try:
        stages = list_deal_stages(DEAL_CATEGORY_ID)  # [{STATUS_ID/ID, NAME, ...}]
    except Exception:
        return ""
    tn = _normalize(team_name)
    for st in stages:
        name = (st.get("NAME") or st.get("name") or "")
        code = (st.get("STATUS_ID") or st.get("STATUSID") or st.get("ID") or st.get("id"))
        nrm = _normalize(name)
        if tn in nrm or tn.replace("бригада", "brigada") in nrm:
            return str(code)
    return ""


# --------- Bot / Web (create BEFORE any @app.* decorators)
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
        kb.button(text="📋 Мої задачі", callback_data="tasks:list:open:1")
        kb.button(text="📦 Угоди бригади", callback_data="deals:list:1")
        kb.button(text="🔁 Змінити бригаду", callback_data="team:change")
        kb.adjust(1, 1, 1)
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
    kb.button(text="📋 Мої задачі", callback_data="tasks:list:open:1")
    kb.button(text="📦 Угоди бригади", callback_data="deals:list:1")
    kb.adjust(1, 1)
    with suppress(Exception):
        await c.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb.as_markup())
    await c.answer("Збережено ✅", show_alert=False)


# ========= DIAGNOSTICS & BIND =========
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


# ========= TASKS (helpers) =========
PAGE_SIZE = 8

def _extract_deal_id_from_task(task: dict) -> Optional[int]:
    cand = task.get("UF_CRM_TASK") or task.get("ufCrmTask") or []
    if isinstance(cand, str):
        cand = [cand]
    for s in cand:
        if str(s).startswith("D_"):
            with suppress(Exception):
                return int(str(s).split("_", 1)[1])
    return None

def _task_line(t: dict, mode: str) -> str:
    status_map = {1:"Нова",2:"В очікуванні",3:"В роботі",4:"Відкладена",5:"Завершена"}
    tid = t.get("ID") or t.get("id")
    title = (t.get("TITLE") or t.get("title") or "").strip()
    deadline = t.get("DEADLINE") or t.get("deadline")
    status = t.get("STATUS") or t.get("status")

    dl_str = ""
    if deadline:
        try:
            dl = dt.datetime.fromisoformat(str(deadline).replace("Z","+00:00")).astimezone(KYIV_TZ)
            dl_str = dl.strftime("%d.%m %H:%M")
        except Exception:
            dl_str = str(deadline)

    status_txt = status_map.get(int(status)) if str(status).isdigit() else (str(status) if status else "")
    suffix = f" • до {dl_str}" if dl_str else ""
    extra_s = f" ({status_txt})" if status_txt and mode not in ("closed_today",) else ""
    return f"• #{tid}: {title}{suffix}{extra_s}"

def _mode_header(mode: str) -> str:
    return {
        "today": "Завдання на сьогодні",
        "overdue": "Прострочені завдання",
        "closed_today": "Сьогодні закриті",
        "open": "Активні задачі",
    }.get(mode, "Завдання")

def _render_tasks_page(tasks: list[dict], page: int, mode: str) -> str:
    total = len(tasks)
    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(1, min(page, pages))
    start = (page - 1) * PAGE_SIZE
    chunk = tasks[start:start + PAGE_SIZE]

    lines = [f"{_mode_header(mode)} (стор. {page}/{pages}, всього: {total})"]
    for t in chunk:
        lines.append(_task_line(t, mode))
    return "\n".join(lines)

def _tasks_nav_kb(mode: str, page: int, total: int) -> types.InlineKeyboardMarkup:
    pages = max(1, math.ceil(max(0, total) / PAGE_SIZE))
    page = max(1, min(page, pages))
    prev_p = max(1, page - 1)
    next_p = min(pages, page + 1)

    kb = InlineKeyboardBuilder()
    kb.button(text="◀️", callback_data=f"tasks:list:{mode}:{prev_p}")
    kb.button(text="▶️", callback_data=f"tasks:list:{mode}:{next_p}")
    kb.button(text="🗓 Сьогодні", callback_data="tasks:list:today:1")
    kb.button(text="⏰ Прострочені", callback_data="tasks:list:overdue:1")
    kb.button(text="🟢 Відкриті", callback_data="tasks:list:open:1")
    kb.button(text="🔄 Оновити", callback_data=f"tasks:list:{mode}:{page}")
    kb.adjust(2, 2, 2)
    return kb.as_markup()


# ========= TASKS (commands & callbacks) =========
@dp.message(Command("tasks"))
async def my_tasks(m: types.Message):
    kb = InlineKeyboardBuilder()
    kb.button(text="📋 Відкрити список", callback_data="tasks:list:open:1")
    await m.answer("Натисніть, щоб побачити список задач:", reply_markup=kb.as_markup())


@dp.callback_query(F

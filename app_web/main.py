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
    list_tasks, complete_task, add_comment, search_user_by_email, get_task,
    list_deal_stages, list_deals, move_deal_to_stage, comment_deal, get_deal, get_contact
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
        name = (st.get("NAME") or st.get("name") or "")
        code = (st.get("STATUS_ID") or st.get("STATUSID") or st.get("ID") or st.get("id"))
        nrm = _normalize(name)
        if tn in nrm or tn.replace("бригада", "brigada") in nrm:
            return str(code)
    return ""


bot = Bot(settings.BOT_TOKEN)
dp = Dispatcher()
app = FastAPI()


# ========= START =========
@dp.message(CommandStart())
async def start(m: types.Message):
    conn = await connect()
    row = await get_user(conn, m.from_user.id)
    await conn.close()

    if row and row["team_id"]:
        kb = InlineKeyboardBuilder()
        kb.button(text="📋 Мої задачі", callback_data="tasks:list:open:1")
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
    kb.button(text="📋 Мої задачі", callback_data="tasks:list:open:1")
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
PAGE_SIZE = 8

async def _show_tasks_page(chat_id: int, mode: str, page: int,
                           edit_message: Optional[types.Message] = None,
                           user_id: Optional[int] = None):
    conn = await connect()
    try:
        key = user_id if user_id else chat_id
        u = await get_user(conn, key)
    finally:
        await conn.close()

    bx_id = u["bitrix_user_id"] if u else None
    if not bx_id:
        await bot.send_message(chat_id, "Спочатку прив’яжіть Bitrix: /bind email")
        return

    now = dt.datetime.now(KYIV_TZ)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end   = now.replace(hour=23, minute=59, second=59, microsecond=0)

    if mode in ("today","сьогодні"):
        extra = {">=DEADLINE": day_start.isoformat(), "<=DEADLINE": day_end.isoformat()}
    elif mode in ("overdue","прострочені","over"):
        extra = {"<DEADLINE": now.isoformat(), "!STATUS": 5}
    elif mode in ("closed_today","done_today"):
        extra = {">=CLOSED_DATE": day_start.isoformat(), "<=CLOSED_DATE": day_end.isoformat()}
    else:
        extra = {"REAL_STATUS": 2}

    fields = ["ID","TITLE","DEADLINE","STATUS","UF_CRM_TASK"]
    filters = [{"RESPONSIBLE_ID": bx_id, **extra}]

    bag = {}
    for f in filters:
        try:
            res = list_tasks(f, fields)
            arr = res.get("result") if isinstance(res, dict) else (res or [])
            for t in arr:
                tid = str(t.get("ID") or t.get("id"))
                if tid and tid not in bag:
                    bag[tid] = t
        except Exception:
            pass

    tasks = list(bag.values())
    text = f"Завдань: {len(tasks)}\n" + "\n".join([t.get("TITLE","") for t in tasks[:PAGE_SIZE]])
    kb = InlineKeyboardBuilder()
    kb.button(text="🟢 Оновити", callback_data=f"tasks:list:{mode}:{page}")
    kb.adjust(1)

    if edit_message:
        with suppress(TelegramBadRequest):
            await edit_message.edit_text(text, reply_markup=kb.as_markup())
    else:
        await bot.send_message(chat_id, text, reply_markup=kb.as_markup())


@dp.callback_query(F.data.startswith("tasks:list:"))
async def tasks_list_cb(c: types.CallbackQuery):
    with suppress(Exception):
        await c.answer()
    parts = c.data.split(":")
    mode = parts[2] if len(parts) > 2 else "open"
    page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 1
    await _show_tasks_page(c.message.chat.id, mode, page, edit_message=c.message, user_id=c.from_user.id)


@dp.callback_query(F.data.startswith("tasks:open"))
async def tasks_open_cb(c: types.CallbackQuery):
    with suppress(Exception):
        await c.answer()
    with suppress(TelegramBadRequest):
        await c.message.edit_text("📦 Завантажую …")
    await _show_tasks_page(c.message.chat.id, "open", 1, edit_message=c.message, user_id=c.from_user.id)


# ========= Webhook =========
OLD_SECRET = os.getenv("WEBHOOK_SECRET_OLD", "").strip()

@app.post("/webhook/{secret}")
async def telegram_webhook(secret: str, request: Request):
    valid = {settings.WEBHOOK_SECRET.strip()}
    if OLD_SECRET:
        valid.add(OLD_SECRET)
    if secret.strip() not in valid:
        return JSONResponse({"ok": False}, status_code=status.HTTP_404_NOT_FOUND)

    update = types.Update.model_validate(await request.json(), context={"bot": bot})
    try:
        await dp.feed_update(bot, update)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise
    return JSONResponse({"ok": True})


# ========= Startup =========
@app.on_event("startup")
async def on_startup():
    await ensure_schema_and_seed()
    await bot.delete_webhook(drop_pending_updates=True)
    url = f"{settings.WEBHOOK_BASE}/webhook/{settings.WEBHOOK_SECRET}"
    print("[startup] setting webhook to:", url)
    await bot.set_webhook(url=url, allowed_updates=["message","callback_query"])
    if getattr(settings, "RUN_WORKER_IN_APP", False):
        asyncio.create_task(daily_loop())

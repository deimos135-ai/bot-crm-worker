# app_web/main.py
import os
import json
import math
import asyncio
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
from shared.repo import connect, get_user, upsert_user_team, ensure_schema_and_seed, set_user_bitrix_id
from shared.bx import search_user_by_email, list_deal_stages, list_deals, move_deal_to_stage, comment_deal, get_deal, get_contact
from worker.report_worker import daily_loop, build_full_report

# --------- Deals settings (env)
DEAL_CATEGORY_ID = int(os.getenv("DEAL_CATEGORY_ID", "0") or 0)
DEAL_DONE_STAGE_ID = os.getenv("DEAL_DONE_STAGE_ID", "").strip()
try:
    TEAM_STAGE_MAP = json.loads(os.getenv("TEAM_STAGE_MAP", "{}"))
except Exception:
    TEAM_STAGE_MAP = {}

# --------- Helpers
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
        name = (st.get("NAME") or "")
        code = st.get("STATUS_ID") or st.get("ID")
        if tn in _normalize(name):
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
        kb.button(text="📂 Мої угоди", callback_data="deals:list:1")
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

@dp.callback_query(F.data.startswith("team:set:"))
async def team_set(c: types.CallbackQuery):
    tid = int(c.data.split(":")[-1])
    full_name = f"{c.from_user.first_name or ''} {c.from_user.last_name or ''}".strip()
    conn = await connect()
    await upsert_user_team(conn, c.from_user.id, full_name, tid)
    await conn.close()
    text = f"Бригаду встановлено: *{TEAMS.get(tid, '—')}* ✅"
    kb = InlineKeyboardBuilder()
    kb.button(text="📂 Мої угоди", callback_data="deals:list:1")
    with suppress(Exception):
        await c.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb.as_markup())
    await c.answer()

# ========= DEALS =========
DEALS_PAGE = 10

def _deal_brief_line(d: dict) -> str:
    did = d.get("ID")
    title = (d.get("TITLE") or "").strip()
    money = f" • {d['OPPORTUNITY']} {d.get('CURRENCY_ID','')}" if d.get("OPPORTUNITY") else ""
    return f"• #{did}: {title}{money}"

def _deals_nav_kb(page: int) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    prev_p = max(1, page - 1)
    next_p = page + 1
    kb.button(text="◀️", callback_data=f"deals:list:{prev_p}")
    kb.button(text="▶️", callback_data=f"deals:list:{next_p}")
    kb.button(text="🔄 Оновити", callback_data=f"deals:list:{page}")
    kb.adjust(2, 1)
    return kb.as_markup()

@dp.callback_query(F.data.startswith("deals:list:"))
async def deals_list_cb(c: types.CallbackQuery):
    with suppress(Exception):
        await c.answer()
    try:
        page = int(c.data.split(":")[-1])
    except Exception:
        page = 1
    start_offset = (page - 1) * DEALS_PAGE

    conn = await connect()
    u = await get_user(conn, c.from_user.id)
    await conn.close()
    if not u or not u.get("team_id"):
        await bot.send_message(c.message.chat.id, "Спочатку оберіть бригаду через /start.")
        return

    stage_id = await _resolve_team_stage_id(int(u["team_id"]))
    if not stage_id:
        await bot.send_message(c.message.chat.id, "Не знайшов етап для цієї бригади. Використай /stages.")
        return

    res = list_deals(
        {"CATEGORY_ID": DEAL_CATEGORY_ID, "STAGE_ID": stage_id},
        ["ID","TITLE","STAGE_ID","OPPORTUNITY","CURRENCY_ID","CONTACT_ID"],
        order={"ID":"DESC"}, start=start_offset
    )
    items = res.get("result") if isinstance(res, dict) else (res or [])
    if not items:
        await bot.send_message(c.message.chat.id, "Угод немає 🙂")
        return

    lines = [f"Угоди для *{TEAMS.get(int(u['team_id']),'?')}* (стор. {page}):"]
    for d in items:
        lines.append(_deal_brief_line(d))
    text = "\n".join(lines)
    kb = _deals_nav_kb(page)
    with suppress(TelegramBadRequest):
        await c.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

# ========= REPORTS =========
@dp.message(Command("report_now"))
async def report_now(m: types.Message):
    text = await build_full_report()
    await bot.send_message(settings.MASTER_REPORT_CHAT_ID, text)
    await m.answer("Звіт відправлено ✅")

# ========= WEBHOOK =========
OLD_SECRET = os.getenv("WEBHOOK_SECRET_OLD", "").strip()

@app.post("/webhook/{secret}")
async def telegram_webhook(secret: str, request: Request):
    if secret not in {settings.WEBHOOK_SECRET.strip(), OLD_SECRET}:
        return JSONResponse({"ok": False}, status_code=status.HTTP_404_NOT_FOUND)
    update = types.Update.model_validate(await request.json(), context={"bot": bot})
    await dp.feed_update(bot, update)
    return JSONResponse({"ok": True})

# ========= Startup =========
@app.on_event("startup")
async def on_startup():
    await ensure_schema_and_seed()
    await bot.delete_webhook(drop_pending_updates=True)
    url = f"{settings.WEBHOOK_BASE}/webhook/{settings.WEBHOOK_SECRET}"
    print("[startup] set webhook:", url)
    await bot.set_webhook(url=url, allowed_updates=["message","callback_query"])
    if getattr(settings, "RUN_WORKER_IN_APP", False):
        asyncio.create_task(daily_loop())

# app_web/main.py
import os
import json
import ast
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


# ===================== ENV / SETTINGS =====================
DEAL_CATEGORY_ID = int(os.getenv("DEAL_CATEGORY_ID", "0") or 0)       # напр. 20
DEAL_DONE_STAGE_ID = os.getenv("DEAL_DONE_STAGE_ID", "").strip()      # напр. C20:WON або C20:UC_xxx
try:
    TEAM_STAGE_MAP = json.loads(os.getenv("TEAM_STAGE_MAP", "{}"))    # {"1":"C20:UC_..","2":"C20:UC_..",...}
except Exception:
    TEAM_STAGE_MAP = {}

def _load_json_env(name: str, default: dict) -> dict:
    raw = os.getenv(name, "")
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        with suppress(Exception):
            return ast.literal_eval(raw)
    return default

# Мапа полів угоди (налаштовується у Secrets як JSON: DEAL_FIELD_MAP)
DEAL_FIELD_MAP = _load_json_env("DEAL_FIELD_MAP", {
    "type":         ["TYPE_ID"],
    "category":     ["CATEGORY_ID"],
    "address":      ["UF_CRM_ADDRESS", "ADDRESS"],
    "router":       ["UF_CRM_ROUTER"],
    "router_price": ["UF_CRM_ROUTER_PRICE", "UF_CRM_ROUTER_SUM"],
    "comment":      ["COMMENTS"],
})


# ===================== HELPERS =====================
def _normalize(s: str) -> str:
    return "".join(str(s).lower().replace("№", "").split())

async def _resolve_team_stage_id(team_id: int) -> str:
    # 1) explicit map via env
    sid = str(TEAM_STAGE_MAP.get(str(team_id), TEAM_STAGE_MAP.get(team_id, ""))).strip()
    if sid:
        return sid
    # 2) find by stage name containing team name
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

def _first_nonempty(src: dict, keys: list[str], default: str = "—"):
    for k in keys:
        v = src.get(k)
        if v not in (None, "", [], {}):
            return v
    return default


# ===================== BOT / WEB =====================
bot = Bot(settings.BOT_TOKEN)
dp = Dispatcher()
app = FastAPI()


# ===================== START & TEAM =====================
@dp.message(CommandStart())
async def start(m: types.Message):
    conn = await connect()
    row = await get_user(conn, m.from_user.id)
    await conn.close()

    kb = InlineKeyboardBuilder()
    if row and row.get("team_id"):
        kb.button(text="📋 Мої задачі", callback_data="tasks:list:open:1")
        kb.button(text="📦 Мої угоди", callback_data="deals:send_all")
        kb.button(text="🔁 Змінити бригаду", callback_data="team:change")
        kb.adjust(1, 1, 1)
        await m.answer(
            f"Ви у бригаді: *{TEAMS.get(row['team_id'], '?')}*.\nГотові працювати ✅",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb.as_markup(),
        )
        return

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
    kb.button(text="📦 Мої угоди", callback_data="deals:send_all")
    kb.adjust(1, 1)
    with suppress(Exception):
        await c.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb.as_markup())
    await c.answer("Збережено ✅", show_alert=False)


# ===================== DIAGNOSTICS & BIND =====================
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


# ===================== TASKS (короткий список з пагінацією) =====================
PAGE_SIZE = 8

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

@dp.message(Command("tasks"))
async def my_tasks(m: types.Message):
    kb = InlineKeyboardBuilder()
    kb.button(text="📋 Відкрити список", callback_data="tasks:list:open:1")
    await m.answer("Натисніть, щоб побачити список задач:", reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("tasks:list:"))
async def tasks_list_cb(c: types.CallbackQuery):
    with suppress(Exception):
        await c.answer()
    parts = c.data.split(":")  # tasks:list:<mode>:<page>
    mode = parts[2] if len(parts) > 2 else "open"
    page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 1
    await _show_tasks_page(c.message.chat.id, mode, page, edit_message=c.message)

@dp.callback_query(F.data.startswith("tasks:open"))
async def tasks_open_cb(c: types.CallbackQuery):
    # backward compatibility
    with suppress(Exception):
        await c.answer()
    with suppress(TelegramBadRequest):
        await c.message.edit_text("📦 Завантажую …")
    await _show_tasks_page(c.message.chat.id, "open", 1, edit_message=c.message)

async def _show_tasks_page(chat_id: int, mode: str, page: int, edit_message: Optional[types.Message] = None):
    # user
    conn = await connect()
    try:
        u = await get_user(conn, chat_id)   # приватні чати: tg_user_id == chat_id
    finally:
        await conn.close()
    bx_id = u["bitrix_user_id"] if u else None
    if not bx_id:
        await bot.send_message(chat_id, "Спочатку прив’яжіть Bitrix: /bind email")
        return

    # filters
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
        extra = {"REAL_STATUS": 2}   # активні

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
    text = _render_tasks_page(tasks, page, mode)
    kb = _tasks_nav_kb(mode, page, len(tasks))

    if edit_message:
        with suppress(TelegramBadRequest):
            await edit_message.edit_text(text, reply_markup=kb)
    else:
        await bot.send_message(chat_id, text, reply_markup=kb)


# ===================== DEALS (картки по одній + закриття) =====================
def _format_deal_card(deal: dict, contact: Optional[dict]) -> str:
    type_id    = _first_nonempty(deal, DEAL_FIELD_MAP.get("type", []))
    category   = _first_nonempty(deal, DEAL_FIELD_MAP.get("category", []))
    address    = _first_nonempty(deal, DEAL_FIELD_MAP.get("address", []))
    router     = _first_nonempty(deal, DEAL_FIELD_MAP.get("router", []))
    router_sum = _first_nonempty(deal, DEAL_FIELD_MAP.get("router_price", []))
    comment    = _first_nonempty(deal, DEAL_FIELD_MAP.get("comment", []))
    comment = (str(comment) or "—").strip() or "—"

    title = (deal.get("TITLE") or "").strip()
    did = deal.get("ID")
    money = ""
    if deal.get("OPPORTUNITY"):
        money = f"\nСума: {deal['OPPORTUNITY']} {deal.get('CURRENCY_ID','')}"

    contact_line = "—"
    if contact:
        name = " ".join(filter(None, [
            contact.get("NAME"),
            contact.get("SECOND_NAME"),
            contact.get("LAST_NAME"),
        ])).strip() or "Контакт"
        phone = ""
        if isinstance(contact.get("PHONE"), list) and contact["PHONE"]:
            phone = contact["PHONE"][0].get("VALUE") or ""
        contact_line = f"{name} {phone}".strip()

    return (
        f"#{did} • {title}{money}\n\n"
        f"Тип сделки: {type_id}\n"
        f"Категорія: {category}\n"
        f"Адреса: {address}\n"
        f"Роутер: {router}\n"
        f"Вартість роутера: {router_sum}\n"
        f"Коментар: {comment}\n"
        f"Контакт: {contact_line}"
    )

@dp.callback_query(F.data == "deals:send_all")
async def deals_send_all(c: types.CallbackQuery):
    with suppress(Exception):
        await c.answer()
    chat_id = c.message.chat.id

    # отримуємо бригаду
    conn = await connect()
    u = await get_user(conn, c.from_user.id)
    await conn.close()
    if not u or not u.get("team_id"):
        await bot.send_message(chat_id, "Спочатку оберіть бригаду через /start.")
        return

    if not DEAL_CATEGORY_ID:
        await bot.send_message(chat_id, "Задайте DEAL_CATEGORY_ID у Secrets.")
        return

    stage_id = await _resolve_team_stage_id(int(u["team_id"]))
    if not stage_id:
        await bot.send_message(chat_id, "Не знайшов етап для цієї бригади. Використайте /stages і налаштуйте TEAM_STAGE_MAP.")
        return

    await bot.send_message(chat_id, "📦 Завантажую угоди бригади…")

    try:
        res = list_deals(
            {"CATEGORY_ID": DEAL_CATEGORY_ID, "STAGE_ID": stage_id},
            ["ID","TITLE","STAGE_ID","ASSIGNED_BY_ID","DATE_CREATE","OPPORTUNITY","CURRENCY_ID","CONTACT_ID"],
            order={"ID": "DESC"}
        )
    except Exception as e:
        await bot.send_message(chat_id, f"Помилка запиту угод: {e!s}")
        return

    items = res.get("result") if isinstance(res, dict) else (res or [])
    if not items:
        await bot.send_message(chat_id, "Угод у цій колонці поки немає 🙂")
        return

    # надсилаємо картками по одній (до 30 щоб не засмічувати чат)
    for d in items[:30]:
        did = int(d.get("ID"))
        deal = {}
        contact = None
        with suppress(Exception):
            deal = get_deal(did) or {}
        with suppress(Exception):
            cid = int(deal.get("CONTACT_ID") or d.get("CONTACT_ID") or 0)
            if cid:
                contact = get_contact(cid) or None

        text = _format_deal_card(deal or d, contact)
        kb = InlineKeyboardBuilder()
        if DEAL_DONE_STAGE_ID:
            kb.button(text="✅ Закрити угоду", callback_data=f"deal:won:{did}")
        kb.adjust(1)
        await bot.send_message(chat_id, text, reply_markup=kb.as_markup())
        await asyncio.sleep(0.05)

@dp.callback_query(F.data.startswith("deal:won:"))
async def deal_won_cb(c: types.CallbackQuery):
    with suppress(Exception):
        await c.answer()
    if not DEAL_DONE_STAGE_ID:
        await bot.send_message(c.message.chat.id, "DEAL_DONE_STAGE_ID не налаштовано у Secrets.")
        return
    did = int(c.data.split(":")[-1])
    try:
        move_deal_to_stage(did, DEAL_DONE_STAGE_ID)
        with suppress(Exception):
            comment_deal(did, "Закрито через Telegram-бот ✅")
        await bot.send_message(c.message.chat.id, f"Угоду #{did} переведено в етап `{DEAL_DONE_STAGE_ID}` ✅", parse_mode=ParseMode.MARKDOWN)
        with suppress(TelegramBadRequest):
            await c.message.edit_reply_markup(reply_markup=None)
    except Exception as e:
        await bot.send_message(c.message.chat.id, f"Не вдалося оновити угоду #{did}: {e!s}")


# ===================== DEALS: /stages і /deals-список =====================
@dp.message(Command("stages"))
async def stages(m: types.Message):
    if not DEAL_CATEGORY_ID:
        await m.answer("Задайте DEAL_CATEGORY_ID у Secrets.")
        return
    try:
        sts = list_deal_stages(DEAL_CATEGORY_ID)
    except Exception as e:
        await m.answer(f"Помилка отримання етапів: {e!s}")
        return
    if not sts:
        await m.answer("Етапи не знайдено або нестачає прав.")
        return
    lines = [f"{(s.get('STATUS_ID') or s.get('ID'))}: {s.get('NAME')}" for s in sts]
    await m.answer("Етапи цієї воронки:\n" + "\n".join(lines))

@dp.message(Command("deals"))
async def deals_for_team(m: types.Message):
    # Залишаємо як короткий зведений список (без карток)
    if not DEAL_CATEGORY_ID:
        await m.answer("Задайте DEAL_CATEGORY_ID у Secrets (ID воронки, напр. 20).")
        return

    conn = await connect()
    u = await get_user(conn, m.from_user.id)
    await conn.close()
    if not u or not u.get("team_id"):
        await m.answer("Спочатку оберіть бригаду через /start.")
        return

    stage_id = await _resolve_team_stage_id(int(u["team_id"]))
    if not stage_id:
        await m.answer("Не знайшов етап для цієї бригади. Виведіть /stages і задайте TEAM_STAGE_MAP або назвіть етап як «Бригада N».")
        return

    try:
        res = list_deals(
            {"CATEGORY_ID": DEAL_CATEGORY_ID, "STAGE_ID": stage_id},
            ["ID","TITLE","STAGE_ID","ASSIGNED_BY_ID","DATE_CREATE","OPPORTUNITY","CURRENCY_ID"]
        )
    except Exception as e:
        await m.answer(f"Помилка запиту угод: {e!s}")
        return

    items = res.get("result") if isinstance(res, dict) else (res or [])
    if not items:
        await m.answer("Угод у цій колонці поки немає 🙂")
        return

    lines = []
    for d in items[:50]:
        did = d.get("ID")
        title = (d.get("TITLE") or "").strip()
        money = ""
        if d.get("OPPORTUNITY"):
            cur = d.get("CURRENCY_ID") or ""
            money = f" • {d['OPPORTUNITY']} {cur}"
        lines.append(f"• #{did}: {title}{money}")
    await m.answer(
        f"Угоди для *{TEAMS.get(int(u['team_id']), 'бригади')}*:\n" + "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN
    )


# ===================== DIAGNOSTИКА ПОЛІВ УГОДИ =====================
@dp.message(Command("deal_dump"))
async def deal_dump(m: types.Message):
    parts = (m.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        await m.answer("Приклад: `/deal_dump 1138378`", parse_mode=ParseMode.MARKDOWN)
        return
    did = int(parts[1])
    try:
        data = get_deal(did) or {}
    except Exception as e:
        await m.answer(f"Помилка отримання угоди: {e!s}")
        return
    if not data:
        await m.answer("Не знайшов угоду.")
        return

    keys = sorted(data.keys(), key=lambda k: (0 if str(k).startswith("UF_") else 1, str(k)))
    lines = []
    for k in keys:
        v = data[k]
        s = v
        if isinstance(v, (list, dict)):
            s = json.dumps(v, ensure_ascii=False)
        s = str(s)
        if len(s) > 300:
            s = s[:300] + "…"
        lines.append(f"{k}: {s}")
    text = "```\n" + "\n".join(lines) + "\n```"
    for chunk_start in range(0, len(text), 3500):
        await m.answer(text[chunk_start:chunk_start+3500], parse_mode=ParseMode.MARKDOWN)


# ===================== QUICK TASK ACTIONS (slash) =====================
@dp.message(Command("done"))
async def done(m: types.Message):
    parts = (m.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        await m.answer("Приклад: `/done 1234 коментар`", parse_mode=ParseMode.MARKDOWN)
        return
    task_id = int(parts[1])
    comment = " ".join(parts[2:]) or "Завершено через Telegram-бот"
    try:
        complete_task(task_id)
        add_comment(task_id, comment)
        await m.answer(f"Задачу #{task_id} завершено ✅")
    except Exception as e:
        await m.answer(f"Не вдалося завершити #{task_id}: {e!s}")

@dp.message(Command("chatid"))
async def chatid(m: types.Message):
    await m.answer(f"Chat ID: {m.chat.id}")


# ===================== REPORTS =====================
@dp.message(Command("report_now"))
async def report_now(m: types.Message):
    text = await build_full_report()
    await bot.send_message(settings.MASTER_REPORT_CHAT_ID, text)
    await m.answer("Звіт відправлено в майстер-групу ✅")


# ===================== WEBHOOK =====================
OLD_SECRET = os.getenv("WEBHOOK_SECRET_OLD", "").strip()

@app.post("/webhook/{secret}")
async def telegram_webhook(secret: str, request: Request):
    valid = {settings.WEBHOOK_SECRET.strip()}
    if OLD_SECRET:
        valid.add(OLD_SECRET)

    if secret.strip() not in valid:
        print("[webhook] WRONG SECRET got:", secret, "| expected one of:", list(valid))
        return JSONResponse({"ok": False}, status_code=status.HTTP_404_NOT_FOUND)

    update = types.Update.model_validate(await request.json(), context={"bot": bot})
    try:
        await dp.feed_update(bot, update)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise
    return JSONResponse({"ok": True})


# ===================== STARTUP =====================
@app.on_event("startup")
async def on_startup():
    await ensure_schema_and_seed()

    await bot.delete_webhook(drop_pending_updates=True)
    url = f"{settings.WEBHOOK_BASE}/webhook/{settings.WEBHOOK_SECRET}"
    print("[startup] setting webhook to:", url)
    await bot.set_webhook(
        url=url,
        allowed_updates=["message", "callback_query"],
    )

    if getattr(settings, "RUN_WORKER_IN_APP", False):
        asyncio.create_task(daily_loop())

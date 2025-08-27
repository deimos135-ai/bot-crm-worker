import os
import json
import asyncio
import datetime as dt
from typing import Optional

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

# --------- Helpers for stages
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
        name = st.get("NAME") or st.get("name") or ""
        code = st.get("STATUS_ID") or st.get("STATUSID") or st.get("ID") or st.get("id")
        nrm = _normalize(name)
        if tn in nrm:
            return str(code)
        # sometimes "brigada" transliteration
        if tn.replace("бригада", "brigada") in nrm:
            return str(code)
    return ""


# --------- Bot / Web
bot = Bot(settings.BOT_TOKEN)
dp = Dispatcher()
app = FastAPI()


# ========= BASIC FLOW: start & team selection =========
@dp.message(CommandStart())
async def start(m: types.Message):
    conn = await connect()
    row = await get_user(conn, m.from_user.id)
    await conn.close()

    if row and row["team_id"]:
        kb = InlineKeyboardBuilder()
        kb.button(text="📋 Мої задачі", callback_data="tasks:open:1")
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
    try:
        await c.message.edit_text("Оберіть вашу бригаду:", reply_markup=kb.as_markup())
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise
    finally:
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
    kb.button(text="📋 Мої задачі", callback_data="tasks:open:1")
    kb.adjust(1)
    try:
        await c.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb.as_markup())
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise
    finally:
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
            await m.answer("Не знайшов користувача в Bitrix за цим email 🤔")
            return
        conn = await connect()
        await set_user_bitrix_id(conn, m.from_user.id, bx_id)
        await conn.close()
        await m.answer(f"Прив’язано Bitrix ID: {bx_id} ✅")
    except Exception as e:
        await m.answer(f"Не вдалось прив’язати: {e!s}")


# ========= TASKS =========

def _normalize_tasks(res):
    if isinstance(res, dict) and "tasks" in res:
        return res["tasks"]
    if isinstance(res, dict) and isinstance(res.get("result"), list):
        return res["result"]
    if isinstance(res, list):
        return res
    return []

def _extract_deal_id_from_task(task: dict) -> Optional[int]:
    """
    Повертає ID угоди з UF_CRM_TASK (елементи типу 'D_<id>').
    """
    candidates = (
        task.get("UF_CRM_TASK")
        or task.get("ufCrmTask")
        or task.get("UF_CRM", {})
    )
    if not candidates:
        return None
    if isinstance(candidates, dict) and "task" in candidates:
        candidates = candidates["task"]
    if isinstance(candidates, str):
        candidates = [candidates]
    for s in candidates:
        s = str(s or "")
        if s.startswith("D_"):
            try:
                return int(s.split("_", 1)[1])
            except Exception:
                continue
    return None

def _format_deal_for_message(deal: dict, contact: Optional[dict]) -> str:
    # ⚠️ заміни UF_* на свої ключі, якщо вони відрізняються
    type_id    = deal.get("TYPE_ID") or "—"
    category   = deal.get("CATEGORY_ID") or deal.get("CATEGORY") or "—"
    comment    = (deal.get("COMMENTS") or "").strip() or "—"
    address    = deal.get("ADDRESS") or deal.get("UF_CRM_ADDRESS") or "—"

    router     = deal.get("UF_CRM_ROUTER") or "—"
    router_sum = deal.get("UF_CRM_ROUTER_PRICE") or deal.get("UF_CRM_ROUTER_SUM") or "—"

    contact_line = "—"
    if contact:
        name = " ".join(filter(None, [contact.get("NAME"), contact.get("SECOND_NAME"), contact.get("LAST_NAME")])).strip()
        phone = ""
        if isinstance(contact.get("PHONE"), list) and contact["PHONE"]:
            phone = contact["PHONE"][0].get("VALUE") or ""
        contact_line = f"{name or 'Контакт'} {phone}".strip()

    lines = [
        f"Тип сделки: {type_id}",
        f"Категорія: {category}",
        f"Коментар: {comment}",
        f"Адреса: {address}",
        f"Роутер: {router}",
        f"Вартість роутера: {router_sum}",
        f"Контакт: {contact_line}",
    ]
    return "\n".join(lines)

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

def _build_task_row_kb(tid: int) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="ℹ️ Деталі", callback_data=f"task:details:{tid}")
    kb.button(text="✅ Закрити", callback_data=f"task:done:{tid}")
    kb.adjust(2)
    return kb.as_markup()

@dp.message(Command("tasks"))
async def my_tasks(m: types.Message):
    # /tasks [open|today|overdue|closed_today]
    args = (m.text or "").split()[1:]
    mode = (args[0] if args else "open").lower()

    conn = await connect()
    u = await get_user(conn, m.from_user.id)
    await conn.close()
    bx_id = u["bitrix_user_id"] if u else None
    if not bx_id:
        await m.answer("Спочатку прив’яжіть Bitrix e-mail: `/bind user@company.com`", parse_mode=ParseMode.MARKDOWN)
        return

    now = dt.datetime.now(KYIV_TZ)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end   = now.replace(hour=23, minute=59, second=59, microsecond=0)

    extra = {}
    header = "Відкриті завдання"
    if mode in ("today","сьогодні"):
        extra = {">=DEADLINE": day_start.isoformat(), "<=DEADLINE": day_end.isoformat()}
        header = "Завдання на сьогодні"
    elif mode in ("overdue","прострочені","over"):
        extra = {"<DEADLINE": now.isoformat(), "!STATUS": 5}
        header = "Прострочені завдання"
    elif mode in ("closed_today","done_today"):
        extra = {">=CLOSED_DATE": day_start.isoformat(), "<=CLOSED_DATE": day_end.isoformat()}
        header = "Сьогодні закриті"
    else:
        extra = {"!STATUS": 5}

    # обов’язково беремо UF_CRM_TASK, щоб мати зв’язок із CRM
    fields = ["ID","TITLE","DEADLINE","STATUS","CLOSED_DATE","RESPONSIBLE_ID","CREATED_BY","UF_CRM_TASK"]
    filters = [
        {"RESPONSIBLE_ID": bx_id, **extra},
        {"ACCOMPLICE": bx_id, **extra},
        {"AUDITOR": bx_id, **extra},
        {"CREATED_BY": bx_id, **extra},
    ]

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
    if not tasks:
        await m.answer("Задач за запитом не знайдено 🙂")
        return

    await m.answer(f"{header} (до 20):")
    for t in tasks[:20]:
        tid = int(t.get("ID") or t.get("id"))
        await m.answer(_task_line(t, mode), reply_markup=_build_task_row_kb(tid))

@dp.callback_query(F.data.startswith("tasks:open"))
async def tasks_open_cb(c: types.CallbackQuery):
    # формати: tasks:open, tasks:open:1, tasks:today, tasks:overdue ...
    parts = c.data.split(":")
    mode = parts[1] if len(parts) > 1 else "open"
    # якщо передали сторінку (":open:1"), її поки ігноруємо
    mode = "open" if mode == "open" else mode

    conn = await connect()
    u = await get_user(conn, c.from_user.id)
    await conn.close()
    bx_id = u["bitrix_user_id"] if u else None
    if not bx_id:
        await c.answer("Спочатку прив’яжіть Bitrix: /bind email", show_alert=True)
        return

    now = dt.datetime.now(KYIV_TZ)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end   = now.replace(hour=23, minute=59, second=59, microsecond=0)

    extra = {"!STATUS": 5}
    header = "Відкриті завдання"
    if mode in ("today","сьогодні"):
        extra = {">=DEADLINE": day_start.isoformat(), "<=DEADLINE": day_end.isoformat()}
        header = "Завдання на сьогодні"
    elif mode in ("overdue","прострочені","over"):
        extra = {"<DEADLINE": now.isoformat(), "!STATUS": 5}
        header = "Прострочені завдання"
    elif mode in ("closed_today","done_today"):
        extra = {">=CLOSED_DATE": day_start.isoformat(), "<=CLOSED_DATE": day_end.isoformat()}
        header = "Сьогодні закриті"

    fields = ["ID","TITLE","DEADLINE","STATUS","CLOSED_DATE","RESPONSIBLE_ID","CREATED_BY","UF_CRM_TASK"]
    filters = [
        {"RESPONSIBLE_ID": bx_id, **extra},
        {"ACCOMPLICE": bx_id, **extra},
        {"AUDITOR": bx_id, **extra},
        {"CREATED_BY": bx_id, **extra},
    ]

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

    # відповідаємо на callback, щоб прибрати «крутилку»
    await c.answer()

    if not tasks:
        # редагуємо заголовок, або шлемо нове повідомлення
        try:
            await c.message.edit_text("Задач за запитом не знайдено 🙂")
        except TelegramBadRequest:
            await bot.send_message(c.message.chat.id, "Задач за запитом не знайдено 🙂")
        return

    # редагуємо поточне повідомлення заголовком
    try:
        await c.message.edit_text(f"{header} (до 20):")
    except TelegramBadRequest:
        await bot.send_message(c.message.chat.id, f"{header} (до 20):")

    # шлемо карточки з кнопками
    for t in tasks[:20]:
        tid = int(t.get("ID") or t.get("id"))
        await bot.send_message(
            c.message.chat.id,
            _task_line(t, mode),
            reply_markup=_build_task_row_kb(tid)
        )


# ======== CALLBACKS: details / done ========

@dp.callback_query(F.data.startswith("task:details:"))
async def task_details(c: types.CallbackQuery):
    tid = int(c.data.split(":")[-1])
    try:
        task = get_task(tid) or {}
        deal_id = _extract_deal_id_from_task(task)

        deal, contact = {}, None
        if deal_id:
            deal = get_deal(deal_id) or {}
            if deal.get("CONTACT_ID"):
                try:
                    contact = get_contact(int(deal["CONTACT_ID"])) or None
                except Exception:
                    contact = None

        title = (task.get("TITLE") or "").strip()
        head = f"# {tid} • {title}" if title else f"# {tid}"
        body = _format_deal_for_message(deal, contact) if deal else "Прив’язану угоду не знайдено."
        text = f"{head}\n\n{body}"

        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Закрити", callback_data=f"task:done:{tid}")
        kb.button(text="🔙 До списку", callback_data="tasks:open:1")
        kb.adjust(2)

        try:
            await c.message.edit_text(text, reply_markup=kb.as_markup())
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e):
                raise
        await c.answer()
    except Exception as e:
        await c.answer("Помилка завантаження деталей", show_alert=True)


@dp.callback_query(F.data.startswith("task:done:"))
async def task_done_cb(c: types.CallbackQuery):
    tid = int(c.data.split(":")[-1])
    try:
        complete_task(tid)
        try:
            add_comment(tid, "Закрито через Telegram-бот ✅")
        except Exception:
            pass
        await c.answer("Задачу закрито ✅", show_alert=False)
        try:
            await c.message.edit_reply_markup(reply_markup=None)
        except TelegramBadRequest:
            pass
    except Exception as e:
        await c.answer(f"Не вдалося закрити: {e!s}", show_alert=True)


# ========= DEALS (CRM) =========
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
    for d in items[:20]:
        did = d.get("ID")
        title = (d.get("TITLE") or "").strip()
        money = ""
        if d.get("OPPORTUNITY"):
            cur = d.get("CURRENCY_ID") or ""
            money = f" • {d['OPPORTUNITY']} {cur}"
        lines.append(f"• #{did}: {title}{money}")
    await m.answer(
        f"Угоди для *{TEAMS.get(int(u['team_id']), 'бригади')}* (до 20):\n" + "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN
    )


@dp.message(Command("won"))
async def deal_won(m: types.Message):
    parts = (m.text or "").split(maxsplit=2)
    if len(parts) < 2 or not parts[1].isdigit():
        await m.answer("Приклад: `/won 113776 Коментар`", parse_mode=ParseMode.MARKDOWN)
        return
    deal_id = int(parts[1])
    comment = parts[2] if len(parts) > 2 else "Закрито через Telegram-бот"

    stage_done = DEAL_DONE_STAGE_ID
    if not stage_done:
        await m.answer("Задайте DEAL_DONE_STAGE_ID у Secrets. Використайте /stages, щоб побачити список етапів.")
        return
    try:
        move_deal_to_stage(deal_id, stage_done)
        try:
            comment_deal(deal_id, comment)
        except Exception:
            pass
        await m.answer(f"Угоду #{deal_id} переведено в етап `{stage_done}` ✅", parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await m.answer(f"Не вдалося оновити угоду #{deal_id}: {e!s}")


# ========= REPORTS =========
@dp.message(Command("report_now"))
async def report_now(m: types.Message):
    text = await build_full_report()
    await bot.send_message(settings.MASTER_REPORT_CHAT_ID, text)
    await m.answer("Звіт відправлено в майстер-групу ✅")


# ========= Webhook (dynamic path with secret check) =========
@app.post("/webhook/{secret}")
async def telegram_webhook(secret: str, request: Request):
    if secret.strip() != settings.WEBHOOK_SECRET.strip():
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
    await bot.set_webhook(
        url=f"{settings.WEBHOOK_BASE}/webhook/{settings.WEBHOOK_SECRET}",
        allowed_updates=["message", "callback_query"],
    )

    if getattr(settings, "RUN_WORKER_IN_APP", False):
        asyncio.create_task(daily_loop())

import os
import json
import asyncio
import datetime as dt

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import CommandStart, Command
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

from fastapi import FastAPI, Request
from starlette.responses import JSONResponse, PlainTextResponse
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
    list_deal_stages, list_deals, move_deal_to_stage, comment_deal,
    get_deal, get_deal_products, get_deal_contacts, get_contact
)
from worker.report_worker import daily_loop, build_full_report


# ------------- Deals env
DEAL_CATEGORY_ID = int(os.getenv("DEAL_CATEGORY_ID", "0") or 0)
DEAL_DONE_STAGE_ID = os.getenv("DEAL_DONE_STAGE_ID", "").strip()
try:
    TEAM_STAGE_MAP = json.loads(os.getenv("TEAM_STAGE_MAP", "{}"))
except Exception:
    TEAM_STAGE_MAP = {}

PAGE_SIZE = 5  # deals per page


# -------- Helpers
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
        code = st.get("STATUS_ID") or st.get("STATUSID") or st.get("ID") or st.get("id")
        nrm = _normalize(name)
        if tn in nrm or tn.replace("бригада", "brigada") in nrm:
            return str(code)
    return ""

def _short(s: str, n: int = 40) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[: n - 1] + "…"

def _first(*vals):
    for v in vals:
        if v:
            return str(v)
    return ""

def _join_phone(phones):
    if isinstance(phones, list) and phones:
        return str(phones[0].get("VALUE") or "")
    return str(phones or "")

def _map_link(address: str):
    from urllib.parse import quote_plus
    return f"https://maps.google.com/?q={quote_plus(address)}" if address else None

def _deal_brief_info(d: dict):
    address = ""
    phone = ""
    comment = (d.get("COMMENTS") or d.get("COMMENTS") or "").strip()
    for k, v in d.items():
        if isinstance(k, str) and k.startswith("UF_CRM") and isinstance(v, str):
            if "АДРЕС" in k.upper() or "ADDRESS" in k.upper():
                address = v.strip() or address
    try:
        cids = get_deal_contacts(int(d.get("ID"))) or []
        if cids:
            contact = get_contact(int(cids[0].get("CONTACT_ID")))
            if not address:
                address = _first(
                    contact.get("ADDRESS"),
                    " ".join(filter(None, [
                        contact.get("ADDRESS_COUNTRY"),
                        contact.get("ADDRESS_REGION"),
                        contact.get("ADDRESS_PROVINCE"),
                        contact.get("ADDRESS_CITY"),
                        contact.get("ADDRESS_2"),
                        contact.get("ADDRESS_1"),
                    ])).strip()
                )
            phone = _join_phone(contact.get("PHONE"))
    except Exception:
        pass
    return (address.strip(), phone.strip(), comment)


# -------- Bot & Web
bot = Bot(settings.BOT_TOKEN)
dp = Dispatcher()
app = FastAPI()


# -------- Start & team
@dp.message(CommandStart())
async def start(m: types.Message):
    conn = await connect()
    row = await get_user(conn, m.from_user.id)
    await conn.close()

    if row and row.get("team_id"):
        kb = InlineKeyboardBuilder()
        kb.button(text="🔁 Змінити бригаду", callback_data="team:change")
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
    try:
        await c.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=None)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise
    finally:
        await c.answer("Збережено ✅", show_alert=False)


# -------- Bind & whoami
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


# -------- Tasks
@dp.message(Command("tasks"))
async def my_tasks(m: types.Message):
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

    fields = ["ID","TITLE","DEADLINE","STATUS","CLOSED_DATE","RESPONSIBLE_ID","CREATED_BY"]
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

    status_map = {1:"Нова",2:"В очікуванні",3:"В роботі",4:"Відкладена",5:"Завершена"}
    lines = []
    for t in tasks[:20]:
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
        lines.append(f"• #{tid}: {title}{suffix}{extra_s}")

    await m.answer(f"{header} (до 20):\n" + "\n".join(lines))


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

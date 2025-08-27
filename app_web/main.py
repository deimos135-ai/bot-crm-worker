# app_web/main.py
import asyncio
import datetime as dt
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
    connect, get_user, upsert_user_team, ensure_schema_and_seed, set_user_bitrix_id
)
from shared.bx import complete_task, add_comment, list_tasks, search_user_by_email
from worker.report_worker import daily_loop, build_full_report  # запуск планувальника у веб-процесі

bot = Bot(settings.BOT_TOKEN)
dp = Dispatcher()
app = FastAPI()


@dp.message(CommandStart())
async def start(m: types.Message):
    conn = await connect()
    row = await get_user(conn, m.from_user.id)
    await conn.close()

    if row and row["team_id"]:
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

    target_text = f"Бригаду встановлено: *{TEAMS.get(tid, '—')}*. Готово ✅"
    try:
        await c.message.edit_text(
            target_text, parse_mode=ParseMode.MARKDOWN, reply_markup=None
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise
    finally:
        await c.answer("Збережено ✅", show_alert=False)


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


# -------- Прив’язка Bitrix ID за e-mail
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


# -------- Список задач
def _normalize_tasks(res):
    if isinstance(res, dict) and "tasks" in res:
        return res["tasks"]
    if isinstance(res, dict) and isinstance(res.get("result"), list):
        return res["result"]
    if isinstance(res, list):
        return res
    return []

def _g(d, *keys):
    for k in keys:
        if isinstance(d, dict) and k in d:
            return d[k]
    return None

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

    if mode in ("today", "сьогодні"):
        filt = {"RESPONSIBLE_ID": bx_id, ">=DEADLINE": day_start.isoformat(), "<=DEADLINE": day_end.isoformat()}
        header = "Завдання на сьогодні"
    elif mode in ("overdue", "прострочені", "over"):
        filt = {"RESPONSIBLE_ID": bx_id, "<DEADLINE": now.isoformat(), "!STATUS": 5}
        header = "Прострочені завдання"
    elif mode in ("closed_today", "done_today"):
        filt = {"RESPONSIBLE_ID": bx_id, ">=CLOSED_DATE": day_start.isoformat(), "<=CLOSED_DATE": day_end.isoformat()}
        header = "Сьогодні закриті"
    else:
        # відкриті
        filt = {"RESPONSIBLE_ID": bx_id, "!STATUS": 5}
        header = "Відкриті завдання"

    try:
        res = list_tasks(filt, ["ID","TITLE","DEADLINE","STATUS","CLOSED_DATE"])
    except Exception as e:
        await m.answer(f"Не вдалося отримати задачі: {e!s}")
        return

    tasks = _normalize_tasks(res)
    if not tasks:
        await m.answer("Задач за запитом не знайдено 🙂")
        return

    status_map = {1:"Нова",2:"В очікуванні",3:"В роботі",4:"Відкладена",5:"Завершена"}

    lines = []
    for t in tasks[:20]:
        tid = _g(t,"id","ID")
        title = _g(t,"title","TITLE") or ""
        deadline = _g(t,"deadline","DEADLINE")
        status = _g(t,"status","STATUS")

        dl_str = ""
        if deadline:
            try:
                dl = dt.datetime.fromisoformat(str(deadline).replace("Z","+00:00")).astimezone(KYIV_TZ)
                dl_str = dl.strftime("%d.%m %H:%M")
            except Exception:
                dl_str = str(deadline)

        status_txt = status_map.get(int(status)) if str(status).isdigit() else (str(status) if status else "")
        suffix = f" • до {dl_str}" if dl_str else ""
        extra = f" ({status_txt})" if status_txt and mode not in ("closed_today",) else ""
        lines.append(f"• #{tid}: {title}{suffix}{extra}")

    await m.answer(f"{header} (до 20):\n" + "\n".join(lines))


# ---- Згенерувати звіт зараз
@dp.message(Command("report_now"))
async def report_now(m: types.Message):
    text = await build_full_report()
    await bot.send_message(settings.MASTER_REPORT_CHAT_ID, text)
    await m.answer("Звіт відправлено в майстер-групу ✅")


# ---- FastAPI webhook (динамічний шлях з перевіркою секрету)
@app.post("/webhook/{secret}")
async def telegram_webhook(secret: str, request: Request):
    if secret.strip() != settings.WEBHOOK_SECRET.strip():
        # маскуємося під 404, щоб не світити ендпоінт
        return JSONResponse({"ok": False}, status_code=status.HTTP_404_NOT_FOUND)

    update = types.Update.model_validate(await request.json(), context={"bot": bot})
    try:
        await dp.feed_update(bot, update)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise
    return JSONResponse({"ok": True})


@app.on_event("startup")
async def on_startup():
    # 1) БД та seed
    await ensure_schema_and_seed()

    # 2) Реєструємо Telegram webhook
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(
        url=f"{settings.WEBHOOK_BASE}/webhook/{settings.WEBHOOK_SECRET}",
        allowed_updates=["message", "callback_query"],
    )

    # 3) Планувальник звітів у цьому процесі (якщо RUN_WORKER_IN_APP=1)
    if getattr(settings, "RUN_WORKER_IN_APP", False):
        asyncio.create_task(daily_loop())

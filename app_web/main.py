import asyncio, datetime as dt
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import CommandStart, Command
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import InlineKeyboardBuilder
from fastapi import FastAPI, Request
from starlette.responses import JSONResponse

from shared.settings import settings
from shared.team_names import TEAMS
from shared.repo import connect, get_user, upsert_user_team, ensure_schema_and_seed
from shared.tz import KYIV_TZ
from shared.bx import complete_task, add_comment
from worker.report_worker import daily_loop  # для опції RUN_WORKER_IN_APP

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
        return await m.answer(
            f"Ви у бригаді: *{TEAMS.get(row['team_id'],'?')}*.\nГотові працювати ✅",
            parse_mode=ParseMode.MARKDOWN, reply_markup=kb.as_markup()
        )

    kb = InlineKeyboardBuilder()
    for tid, name in TEAMS.items():
        kb.button(text=name, callback_data=f"team:set:{tid}")
    kb.adjust(2,3)
    await m.answer("Оберіть вашу бригаду:", reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("team:change"))
async def team_change(c: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    for tid, name in TEAMS.items():
        kb.button(text=name, callback_data=f"team:set:{tid}")
    kb.adjust(2,3)
    await c.message.edit_text("Оберіть вашу бригаду:", reply_markup=kb.as_markup())
    await c.answer()

@dp.callback_query(F.data.startswith("team:set:"))
async def team_set(c: types.CallbackQuery):
    tid = int(c.data.split(":")[-1])
    full_name = f"{c.from_user.first_name or ''} {c.from_user.last_name or ''}".strip()
    conn = await connect()
    await upsert_user_team(conn, c.from_user.id, full_name, tid)
    await conn.close()
    await c.message.edit_text(f"Бригаду встановлено: *{TEAMS[tid]}*. Готово ✅", parse_mode=ParseMode.MARKDOWN)
    await c.answer()

@dp.message(Command("done"))
async def done(m: types.Message):
    parts = m.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await m.answer("Приклад: `/done 1234 коментар`", parse_mode=ParseMode.MARKDOWN)
    task_id = int(parts[1]); comment = " ".join(parts[2:]) or "Завершено через Telegram-бот"
    try:
        complete_task(task_id); add_comment(task_id, comment)
        await m.answer(f"Задачу #{task_id} завершено ✅")
    except Exception as e:
        await m.answer(f"Не вдалося завершити #{task_id}: {e!s}")

@dp.message(Command("chatid"))
async def chatid(m: types.Message):
    await m.answer(f"Chat ID: {m.chat.id}")

# --- FastAPI webhook endpoints
@app.post(f"/webhook/{settings.WEBHOOK_SECRET}")
async def telegram_webhook(request: Request):
    update = types.Update.model_validate(await request.json(), context={"bot": bot})
    await dp.feed_update(bot, update)
    return JSONResponse({"ok": True})

@app.on_event("startup")
async def on_startup():
    # 1) схеми БД
    await ensure_schema_and_seed()
    # 2) реєструємо вебхук
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(
        url=f"{settings.WEBHOOK_BASE}/webhook/{settings.WEBHOOK_SECRET}",
        allowed_updates=["message","callback_query"]
    )
    # 3) (опційно) запускаємо воркер у цьому ж процесі
    if settings.RUN_WORKER_IN_APP:
        asyncio.create_task(daily_loop())

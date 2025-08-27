# app_web/main.py
import asyncio
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import CommandStart, Command
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest
from fastapi import FastAPI, Request
from starlette.responses import JSONResponse

from shared.settings import settings
from shared.team_names import TEAMS
from shared.repo import connect, get_user, upsert_user_team, ensure_schema_and_seed
from shared.bx import complete_task, add_comment
from worker.report_worker import daily_loop  # запускаємо scheduler в app-процесі, якщо треба

bot = Bot(settings.BOT_TOKEN)
dp = Dispatcher()
app = FastAPI()


@dp.message(CommandStart())
async def start(m: types.Message):
    # якщо команда у групі — все одно дозволимо вибрати бригаду
    conn = await connect()
    row = await get_user(conn, m.from_user.id)
    await conn.close()

    # якщо вже вибрана бригада — запропонуємо змінити
    if row and row["team_id"]:
        kb = InlineKeyboardBuilder()
        kb.button(text="🔁 Змінити бригаду", callback_data="team:change")
        await m.answer(
            f"Ви у бригаді: *{TEAMS.get(row['team_id'], '?')}*.\nГотові працювати ✅",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb.as_markup(),
        )
        return

    # первинний вибір бригади
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
        # Якщо текст/markup ідентичні — ігноруємо, аби не падати 400
        if "message is not modified" not in str(e):
            raise
    finally:
        # Закриваємо "крутилку" на кнопці
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
        # Явно прибираємо клавіатуру, щоб Telegram точно вважав повідомлення зміненим
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


# ---- FastAPI webhook
@app.post(f"/webhook/{settings.WEBHOOK_SECRET}")
async def telegram_webhook(request: Request):
    # Парсимо апдейт і передаємо в aiogram
    update = types.Update.model_validate(await request.json(), context={"bot": bot})
    try:
        await dp.feed_update(bot, update)
    except TelegramBadRequest as e:
        # Страхуємо ще раз на рівні вебхука
        if "message is not modified" not in str(e):
            raise
    return JSONResponse({"ok": True})


@app.on_event("startup")
async def on_startup():
    # 1) БД та початкові дані
    await ensure_schema_and_seed()

    # 2) Реєструємо Telegram webhook
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(
        url=f"{settings.WEBHOOK_BASE}/webhook/{settings.WEBHOOK_SECRET}",
        allowed_updates=["message", "callback_query"],
    )

    # 3) Запускаємо добовий звіт у цьому ж процесі (якщо ввімкнено через секрет)
    #    Працює, коли встановлено RUN_WORKER_IN_APP=1
    try:
        if getattr(settings, "RUN_WORKER_IN_APP", False):
            asyncio.create_task(daily_loop())
    except Exception:
        # якщо поля немає/секрет не заданий — тихо ігноруємо
        pass

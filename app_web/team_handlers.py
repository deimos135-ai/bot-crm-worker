# app_web/team_handlers.py
from aiogram import Router, types, F
from shared import repo

router = Router(name="team_handlers")

@router.message(F.text.regexp(r"^/team\s+set\s+(\d+)$"))
async def team_set(message: types.Message, regexp: types.regexp):
    tid_str = regexp.group(1)  # завжди рядок
    chat_id = message.from_user.id
    # Можеш підставити дружню назву
    team_name = f"Бригада {tid_str}"
    await repo.upsert_user_team(chat_id, tid_str, team_name)
    await message.answer(f"Ок, зберіг команду: {team_name} (id={tid_str})")

@router.message(F.text == "/whoami")
async def whoami(message: types.Message):
    chat_id = message.from_user.id
    row = await repo.get_team(chat_id)
    if not row or not row.get("team_id"):
        await message.answer("Команду ще не вибрано.")
        return
    await message.answer(f"Твоя команда: {row['team_name'] or '—'} (id={row['team_id']})")

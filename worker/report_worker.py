import asyncio, datetime as dt
from aiogram import Bot
from shared.settings import settings
from shared.repo import connect, iter_team_users
from shared.tz import KYIV_TZ
from shared.bx import bx

bot = Bot(settings.BOT_TOKEN)

async def build_team_report(team_id: int) -> str:
    now = dt.datetime.now(KYIV_TZ)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end   = now.replace(hour=23, minute=59, second=59, microsecond=0)

    conn = await connect()
    users = await iter_team_users(conn, team_id)
    await conn.close()

    lines = [f"Звіт за {now.strftime('%d.%m.%Y')}\nБригада \"{team_id}\":"]
    closed_total = 0

    for u in users:
        # задачі, закриті сьогодні (за CLOSED_DATE)
        tasks = bx("tasks.task.list", {
            "filter": {
                "RESPONSIBLE_ID": u["bitrix_user_id"] or 0,
                ">=CLOSED_DATE": day_start.isoformat(),
                "<=CLOSED_DATE": day_end.isoformat(),
            },
            "select": ["ID","TITLE","CLOSED_DATE"]
        }) or {}

        closed = tasks.get("tasks", [])
        closed_total += len(closed)
        ids = [str(t["id"]) for t in closed]
        lines.append(f"• {u['full_name'] or u['tg_user_id']} — {len(closed)} задач(і): {', '.join(ids) if ids else '—'}")

    lines.append(f"\nВсього закрито: {closed_total}")
    return "\n".join(lines)

async def daily_loop():
    # простий цикл, що чекає до 18:00, потім шле звіти
    while True:
        now = dt.datetime.now(KYIV_TZ)
        target = now.replace(hour=18, minute=0, second=0, microsecond=0)
        if now >= target:
            target = target + dt.timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())

        # 18:00 настало → шлемо 5 звітів
        for team_id, chat_id in settings.LEADS_CHAT_BY_TEAM.items():
            if chat_id and chat_id != 0:
                try:
                    text = await build_team_report(team_id)
                    await bot.send_message(chat_id, text)
                except Exception as e:
                    # мінімальний лог у консоль
                    print(f"report error for team {team_id}: {e}")

async def main():
    await daily_loop()

if __name__ == "__main__":
    asyncio.run(main())

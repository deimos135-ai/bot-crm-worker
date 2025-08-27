import asyncio, datetime as dt
from aiogram import Bot
from shared.settings import settings
from shared.repo import connect, iter_team_users, ensure_schema_and_seed
from shared.team_names import TEAMS
from shared.tz import KYIV_TZ
from shared.bx import list_tasks

bot = Bot(settings.BOT_TOKEN)

async def build_full_report() -> str:
    now = dt.datetime.now(KYIV_TZ)
    day = now.strftime('%d.%m.%Y')
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end   = now.replace(hour=23, minute=59, second=59, microsecond=0)

    conn = await connect()
    lines = [f"Звіт за {day}\n"]

    total_closed = 0

    for team_id, team_name in TEAMS.items():
        users = await iter_team_users(conn, team_id)
        if not users:
            continue

        lines.append(f"Бригада “{team_name}”:")
        for u in users:
            bx_uid = u["bitrix_user_id"] or 0
            if not bx_uid:
                lines.append(f"• {u['full_name'] or u['tg_user_id']} — немає Bitrix ID")
                continue

            res = list_tasks(
                {
                    "RESPONSIBLE_ID": bx_uid,
                    ">=CLOSED_DATE": day_start.isoformat(),
                    "<=CLOSED_DATE": day_end.isoformat(),
                },
                ["ID","TITLE","CLOSED_DATE"]
            ) or {}
            closed = res.get("tasks") or res.get("result") or res or []
            # універсалізація під різні відповіді
            if isinstance(closed, dict) and "tasks" in closed:
                closed = closed["tasks"]

            total_closed += len(closed)
            ids = ", ".join(str(t.get("id") or t.get("ID")) for t in closed) if closed else "—"
            lines.append(f"• {u['full_name'] or u['tg_user_id']} — {len(closed)} задач(і): {ids}")
        lines.append("")  # порожній рядок між бригадами

    lines.append(f"Всього закрито за день: {total_closed}")
    await conn.close()
    return "\n".join(lines)

async def daily_loop():
    # забезпечити схеми (якщо воркер окремо)
    await ensure_schema_and_seed()

    while True:
        now = dt.datetime.now(KYIV_TZ)
        target = now.replace(hour=settings.REPORT_HOUR, minute=0, second=0, microsecond=0)
        if now >= target:
            target = target + dt.timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())

        try:
            text = await build_full_report()
            await bot.send_message(settings.MASTER_REPORT_CHAT_ID, text)
        except Exception as e:
            print("[worker] report error:", e)

async def main():
    await daily_loop()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

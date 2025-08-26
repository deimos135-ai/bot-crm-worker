import asyncio
import datetime as dt
from shared.tz import KYIV_TZ

# Можеш замінити на реальну агрегацію з Bitrix
async def build_full_report() -> str:
    now = dt.datetime.now(KYIV_TZ).strftime("%d.%m.%Y")
    return f"Звіт за {now}\n- (демо) Поки тут узагальнена статистика."

async def daily_loop():
    # заглушка — не запускаємо автоматично, доки не буде потреби
    while True:
        await asyncio.sleep(3600)

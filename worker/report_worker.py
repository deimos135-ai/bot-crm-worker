# worker/report_worker.py
from __future__ import annotations
import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone

from shared.repo import ensure_schema_and_seed

# === Налаштування шедулера (опційно) ===
REPORT_AT = os.getenv("REPORT_AT", "21:00")         # коли надсилати звіт (за замовч. 21:00)
REPORT_TZ_OFFSET = os.getenv("REPORT_TZ_OFFSET", "+03:00")  # зсув з UTC (Київ влітку +03:00, взимку +02:00)
ENABLE_DAILY_REPORT = os.getenv("ENABLE_DAILY_REPORT", "0") == "1"


def _seconds_until_next(hhmm: str, tz_offset: str) -> float:
    h, m = map(int, hhmm.split(":"))
    sign = 1 if tz_offset[0] == "+" else -1
    off_h, off_m = map(int, tz_offset[1:].split(":"))
    tz = timezone(sign * timedelta(hours=off_h, minutes=off_m))
    now = datetime.now(tz)
    target = now.replace(hour=h, minute=m, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


async def _send_daily_report() -> None:
    """
    Тут виклич свою реальну логіку формування/надсилання звіту.
    Якщо в тебе є готова функція, імпортуй її тут і виклич.
    Напр., з модуля worker.reporting:
        from worker.reporting import generate_and_send_daily_report
        await generate_and_send_daily_report()
    Поки для надійності просто логнемо (щоб процес не падав, якщо імпорту ще немає).
    """
    logging.info("Daily report tick: тут має бути виклик generate_and_send_daily_report()")


async def _scheduler_loop() -> None:
    # перше очікування до найближчого вікна REPORT_AT
    wait_first = _seconds_until_next(REPORT_AT, REPORT_TZ_OFFSET)
    logging.info("Scheduler armed: first run in %.0f sec (at %s, tz %s)", wait_first, REPORT_AT, REPORT_TZ_OFFSET)
    await asyncio.sleep(wait_first)
    while True:
        try:
            await _send_daily_report()
        except Exception:
            logging.exception("Daily report failed")
        # далі кожні 24 години
        await asyncio.sleep(24 * 60 * 60)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logging.info("report-worker starting...")
    # тримаємо схему в актуальному стані
    await ensure_schema_and_seed()

    # Запускаємо фоновий шедулер лише якщо увімкнено прапорцем
    if ENABLE_DAILY_REPORT:
        asyncio.create_task(_scheduler_loop())
        logging.info("Scheduler enabled (REPORT_AT=%s, REPORT_TZ_OFFSET=%s)", REPORT_AT, REPORT_TZ_OFFSET)
    else:
        logging.info("Scheduler disabled (set ENABLE_DAILY_REPORT=1 to enable)")

    # ГОЛОВНЕ: не завершувати процес
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())

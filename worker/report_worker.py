# worker/report_worker.py
import os
import json
import asyncio
import datetime as dt

from aiogram import Bot
from shared.settings import settings
from shared.team_names import TEAMS
from shared.tz import KYIV_TZ
from shared.bx import list_deal_stages, list_deals

DEAL_CATEGORY_ID = int(os.getenv("DEAL_CATEGORY_ID", "0") or 0)
try:
    TEAM_STAGE_MAP = json.loads(os.getenv("TEAM_STAGE_MAP", "{}"))
except Exception:
    TEAM_STAGE_MAP = {}

REPORT_HOUR = int(os.getenv("REPORT_HOUR", "19") or 19)
REPORT_MINUTE = int(os.getenv("REPORT_MINUTE", "0") or 0)

def _normalize(s: str) -> str:
    return "".join(str(s).lower().replace("№", "").split())

def _resolve_team_stage_id(team_id: int) -> str:
    # 1) з мапи, якщо задано
    sid = str(TEAM_STAGE_MAP.get(str(team_id), TEAM_STAGE_MAP.get(team_id, ""))).strip()
    if sid:
        return sid

    # 2) автоматичний підбір за назвою етапу
    team_name = TEAMS.get(team_id, "")
    if not (DEAL_CATEGORY_ID and team_name):
        return ""
    try:
        stages = list_deal_stages(DEAL_CATEGORY_ID) or []
    except Exception:
        return ""

    tn = _normalize(team_name)
    for st in stages:
        name = (st.get("NAME") or st.get("name") or "").strip()
        code = st.get("STATUS_ID") or st.get("STATUSID") or st.get("ID") or st.get("id")
        nrm = _normalize(name)
        if tn in nrm or tn.replace("бригада", "brigada") in nrm:
            return str(code)
    return ""

async def build_full_report() -> str:
    now = dt.datetime.now(KYIV_TZ)
    hdr = f"Щоденний звіт • {now.strftime('%d.%m %H:%M')}"

    if not DEAL_CATEGORY_ID:
        return f"{hdr}\n\nDEAL_CATEGORY_ID не задано в Secrets."

    lines = [hdr, ""]
    # По кожній бригаді підрахуємо кількість угод на її етапі
    for tid, name in TEAMS.items():
        sid = _resolve_team_stage_id(int(tid))
        if not sid:
            lines.append(f"• {name}: етап не знайдено (додайте в TEAM_STAGE_MAP)")
            continue

        total = 0
        start = 0
        has_next = True
        # Зберемо до 1000 для адекватного підрахунку, пагінація через `next`
        while has_next and total < 1000:
            try:
                res = list_deals(
                    {"CATEGORY_ID": DEAL_CATEGORY_ID, "STAGE_ID": sid},
                    ["ID"], {"ID": "DESC"},
                    start=start
                ) or {}
            except Exception:
                res = {}
            items = res.get("result", []) or []
            total += len(items)
            if "next" in res:
                start = int(res["next"])
                has_next = True
            else:
                has_next = False

        lines.append(f"• {name}: {total} угод на етапі")
    return "\n".join(lines)

async def daily_loop():
    """
    Нескінченний цикл: чекає до наступного REPORT_HOUR:REPORT_MINUTE (за Києвом),
    збирає звіт і надсилає в MASTER_REPORT_CHAT_ID.
    """
    bot = Bot(settings.BOT_TOKEN)
    # опціональний миттєвий стартовий репорт
    if os.getenv("REPORT_SEND_ON_START", "0") in ("1", "true", "True"):
        try:
            txt = await build_full_report()
            await bot.send_message(settings.MASTER_REPORT_CHAT_ID, txt)
        except Exception as e:
            await bot.send_message(settings.MASTER_REPORT_CHAT_ID, f"Не вдалося зібрати звіт: {e!s}")

    while True:
        now = dt.datetime.now(KYIV_TZ)
        target = now.replace(hour=REPORT_HOUR, minute=REPORT_MINUTE, second=0, microsecond=0)
        if now >= target:
            target += dt.timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())

        try:
            txt = await build_full_report()
            await bot.send_message(settings.MASTER_REPORT_CHAT_ID, txt)
        except Exception as e:
            # пробуємо повідомити про помилку, щоб не мовчати
            try:
                await bot.send_message(settings.MASTER_REPORT_CHAT_ID, f"Не вдалося зібрати звіт: {e!s}")
            except Exception:
                pass

        # щоб не відправити двічі в одну хвилину
        await asyncio.sleep(65)

if __name__ == "__main__":
    asyncio.run(daily_loop())

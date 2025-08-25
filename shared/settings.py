import os

class Settings:
    BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
    BITRIX_WEBHOOK_BASE = os.getenv("BITRIX_WEBHOOK_BASE")  # https://<portal>.bitrix24.ua/rest/<user>/<token>
    DATABASE_URL = os.getenv("DATABASE_URL")  # postgres://user:pass@host:port/db
    WEBHOOK_BASE = os.getenv("WEBHOOK_BASE")  # https://<your-app>.fly.dev
    WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")  # довільний секрет, також частина URL
    LEADS_CHAT_BY_TEAM = {  # map team_id -> Telegram chat id керівників
        1: int(os.getenv("TEAM1_CHAT_ID", "0")),
        2: int(os.getenv("TEAM2_CHAT_ID", "0")),
        3: int(os.getenv("TEAM3_CHAT_ID", "0")),
        4: int(os.getenv("TEAM4_CHAT_ID", "0")),
        5: int(os.getenv("TEAM5_CHAT_ID", "0")),
    }

settings = Settings()

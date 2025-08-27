import os

def _must(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env: {name}")
    return v

def _bool(name: str, default: str = "0") -> bool:
    return (os.getenv(name, default) or "").strip() in ("1", "true", "True", "YES", "yes")

class Settings:
    BOT_TOKEN = _must("TG_BOT_TOKEN")
    BITRIX_WEBHOOK_BASE = _must("BITRIX_WEBHOOK_BASE")  # https://portal.bitrix24.ua/rest/<user>/<token>
    DATABASE_URL = _must("DATABASE_URL")
    WEBHOOK_BASE = _must("WEBHOOK_BASE")               # https://<app>.fly.dev
    WEBHOOK_SECRET = _must("WEBHOOK_SECRET")

    # одна майстер-група для звітів
    MASTER_REPORT_CHAT_ID = int(_must("MASTER_REPORT_CHAT_ID"))

    # час відправлення щоденного звіту (година, Europe/Kyiv)
    REPORT_HOUR = int(os.getenv("REPORT_HOUR", "18"))

    # якщо у вас лише один інстанс на Fly, можете запустити воркер в тому ж процесі:
    RUN_WORKER_IN_APP = _bool("RUN_WORKER_IN_APP", "0")

settings = Settings()

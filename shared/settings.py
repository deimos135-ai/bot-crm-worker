import os


def _must(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env: {name}")
    return v


def _bool(name: str, default: str = "0") -> bool:
    return (os.getenv(name, default) or "").strip().lower() in ("1", "true", "yes", "y")


class Settings:
    # Telegram / infra
    BOT_TOKEN = _must("TG_BOT_TOKEN")
    WEBHOOK_BASE = _must("WEBHOOK_BASE")        # https://<app>.fly.dev
    WEBHOOK_SECRET = _must("WEBHOOK_SECRET")

    # Bitrix
    BITRIX_WEBHOOK_BASE = _must("BITRIX_WEBHOOK_BASE")  # https://portal.bitrix24.<tld>/rest/<user>/<token>
    B24_DOMAIN = os.getenv("B24_DOMAIN")  # опційно, напр.: fiberlink.bitrix24.eu

    # DB
    DATABASE_URL = _must("DATABASE_URL")

    # Reports
    MASTER_REPORT_CHAT_ID = int(_must("MASTER_REPORT_CHAT_ID"))
    REPORT_HOUR = int(os.getenv("REPORT_HOUR", "18"))

    # Worker
    RUN_WORKER_IN_APP = _bool("RUN_WORKER_IN_APP", "0")


# ВАЖЛИВО: інстанс має бути визначений на модульному рівні
settings = Settings()

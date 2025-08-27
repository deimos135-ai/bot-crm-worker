# shared/repo.py
from __future__ import annotations

import asyncpg
from typing import Optional, Dict, Any, Union
from shared.settings import settings


# ---------- Підключення та ініціалізація схеми ----------

async def connect() -> asyncpg.Connection:
    # Переконайся, що DATABASE_URL має sslmode=require, якщо це Fly Postgres
    # Додаємо таймаут, щоб уникнути зависань
    return await asyncpg.connect(settings.DATABASE_URL, timeout=15)

async def ensure_schema_and_seed() -> None:
    conn = await connect()
    try:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS tg_users (
            chat_id        BIGINT PRIMARY KEY,
            bitrix_user_id BIGINT,
            team_id        TEXT,
            team_name      TEXT,
            created_at     TIMESTAMPTZ DEFAULT now(),
            updated_at     TIMESTAMPTZ DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS idx_tg_users_team_id ON tg_users(team_id);
        """)
    finally:
        await conn.close()


# ---------- Хелпери ----------

def _as_text(v: Union[str, int, None]) -> Optional[str]:
    return str(v) if v is not None else None


# ---------- Операції з користувачем та командою ----------

async def get_user(chat_id: int) -> Optional[Dict[str, Any]]:
    conn = await connect()
    try:
        row = await conn.fetchrow(
            "SELECT chat_id, bitrix_user_id, team_id, team_name FROM tg_users WHERE chat_id=$1",
            chat_id,
        )
        return dict(row) if row else None
    finally:
        await conn.close()

async def upsert_user(chat_id: int, bitrix_user_id: Optional[int] = None) -> None:
    conn = await connect()
    try:
        await conn.execute(
            """
            INSERT INTO tg_users (chat_id, bitrix_user_id)
            VALUES ($1, $2)
            ON CONFLICT (chat_id) DO UPDATE
            SET bitrix_user_id = COALESCE(EXCLUDED.bitrix_user_id, tg_users.bitrix_user_id),
                updated_at = now()
            """,
            chat_id, bitrix_user_id
        )
    finally:
        await conn.close()

async def set_user_bitrix_id(chat_id: int, bitrix_user_id: int) -> None:
    """Саме цю назву імпортує main.py."""
    conn = await connect()
    try:
        await conn.execute(
            "UPDATE tg_users SET bitrix_user_id=$2, updated_at=now() WHERE chat_id=$1",
            chat_id, bitrix_user_id
        )
    finally:
        await conn.close()

# Синонім — якщо десь лишилась стара назва
set_bitrix_user = set_user_bitrix_id

async def set_team(chat_id: int, team_id: Union[str, int], team_name: Optional[str] = None) -> None:
    conn = await connect()
    try:
        await conn.execute(
            "UPDATE tg_users SET team_id=$2, team_name=$3, updated_at=now() WHERE chat_id=$1",
            chat_id, _as_text(team_id), team_name
        )
    finally:
        await conn.close()

async def upsert_user_team(chat_id: int, team_id: Union[str, int], team_name: Optional[str] = None) -> None:
    """
    Створює користувача, якщо його нема, та оновлює команду.
    """
    conn = await connect()
    try:
        await conn.execute(
            """
            INSERT INTO tg_users (chat_id, team_id, team_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (chat_id) DO UPDATE
            SET team_id   = EXCLUDED.team_id,
                team_name = COALESCE(EXCLUDED.team_name, tg_users.team_name),
                updated_at = now()
            """,
            chat_id, _as_text(team_id), team_name
        )
    finally:
        await conn.close()

async def get_team(chat_id: int) -> Optional[Dict[str, Any]]:
    conn = await connect()
    try:
        row = await conn.fetchrow(
            "SELECT team_id, team_name FROM tg_users WHERE chat_id=$1",
            chat_id,
        )
        return dict(row) if row else None
    finally:
        await conn.close()

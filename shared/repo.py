# shared/repo.py
from __future__ import annotations
import os
import asyncpg
from typing import Optional, Dict, Any
from shared.settings import settings


async def connect() -> asyncpg.Connection:
    # Підтримка sslmode=require в DATABASE_URL (рекомендовано для Fly Postgres)
    return await asyncpg.connect(settings.DATABASE_URL, timeout=10)


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


# ---- API, на яке зазвичай спирається main.py ----

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


async def set_bitrix_user(chat_id: int, bitrix_user_id: int) -> None:
    conn = await connect()
    try:
        await conn.execute(
            "UPDATE tg_users SET bitrix_user_id=$2, updated_at=now() WHERE chat_id=$1",
            chat_id, bitrix_user_id
        )
    finally:
        await conn.close()


async def set_team(chat_id: int, team_id: str, team_name: Optional[str] = None) -> None:
    conn = await connect()
    try:
        await conn.execute(
            "UPDATE tg_users SET team_id=$2, team_name=$3, updated_at=now() WHERE chat_id=$1",
            chat_id, team_id, team_name
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

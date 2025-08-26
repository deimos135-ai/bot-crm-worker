import asyncpg
from shared.settings import settings

_POOL = None


async def connect():
    global _POOL
    if _POOL is None:
        _POOL = await asyncpg.create_pool(dsn=settings.DATABASE_URL, min_size=1, max_size=5)
    return _POOL.acquire()


async def ensure_schema_and_seed():
    global _POOL
    if _POOL is None:
        _POOL = await asyncpg.create_pool(dsn=settings.DATABASE_URL, min_size=1, max_size=5)
    async with _POOL.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            tg_id BIGINT PRIMARY KEY,
            full_name TEXT,
            team_id INTEGER,
            bitrix_user_id BIGINT,
            created_at TIMESTAMPTZ DEFAULT now()
        );
        """)


async def upsert_user_team(conn, tg_id: int, full_name: str, team_id: int):
    await conn.execute("""
        INSERT INTO users (tg_id, full_name, team_id)
        VALUES ($1, $2, $3)
        ON CONFLICT (tg_id) DO UPDATE SET full_name=EXCLUDED.full_name, team_id=EXCLUDED.team_id
    """, tg_id, full_name, team_id)


async def set_user_bitrix_id(conn, tg_id: int, bx_id: int):
    await conn.execute("""
        UPDATE users SET bitrix_user_id=$2 WHERE tg_id=$1
    """, tg_id, bx_id)


async def get_user(conn, tg_id: int):
    row = await conn.fetchrow("SELECT * FROM users WHERE tg_id=$1", tg_id)
    return dict(row) if row else None

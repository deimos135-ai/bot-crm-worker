# shared/repo.py
import asyncio
import asyncpg
from typing import Optional
from .settings import settings
from .team_names import TEAMS

_POOL: Optional[asyncpg.Pool] = None

def _ensure_sslmode(url: str) -> str:
    # Supabase вимагає SSL; якщо не вказано – додаємо ?sslmode=require
    if "sslmode=" in (url or ""):
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}sslmode=require"

class _PooledConn:
    """
    Обгортка над з'єднанням з пулу, щоб зберегти старий інтерфейс:
    await conn.close() -> фактично release() у пул.
    Інші методи/атрибути делегуються реальному з'єднанню.
    """
    def __init__(self, pool: asyncpg.Pool, conn: asyncpg.Connection):
        self._pool = pool
        self._conn = conn

    async def close(self):
        # повертаємо з'єднання у пул
        await self._pool.release(self._conn)

    def __getattr__(self, name):
        return getattr(self._conn, name)

async def _get_pool() -> asyncpg.Pool:
    global _POOL
    if _POOL is not None:
        return _POOL
    url = _ensure_sslmode(settings.DATABASE_URL)
    # створюємо пул 1..5 конектів; таймаути скромні, щоб не висіло
    _POOL = await asyncpg.create_pool(
        dsn=url,
        min_size=1,
        max_size=5,
        timeout=10,            # час на встановлення конекту
        command_timeout=30,    # дефолт на виконання запитів
    )
    return _POOL

async def connect():
    """
    Сумісна з попереднім кодом функція.
    Повертає об'єкт, який виглядає як Connection, але .close() робить release() у пул.
    Містить ретраї на випадок «прокидання» БД / коротких збоїв мережі.
    """
    pool = await _get_pool()
    last_exc = None
    for attempt in range(3):  # 3 спроби з бекофом
        try:
            conn = await pool.acquire()
            return _PooledConn(pool, conn)
        except Exception as e:
            last_exc = e
            await asyncio.sleep(0.6 * (attempt + 1))
    # якщо не вдалося — піднімаємо останній ексепшн
    raise last_exc

async def ensure_schema_and_seed():
    conn = await connect()
    try:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
          id SERIAL PRIMARY KEY,
          tg_user_id BIGINT UNIQUE NOT NULL,
          bitrix_user_id INT,
          full_name TEXT,
          team_id INT,
          role TEXT DEFAULT 'worker',
          created_at TIMESTAMP DEFAULT now()
        )""")
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS teams (
          id INT PRIMARY KEY,
          name TEXT NOT NULL
        )""")
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS task_actions (
          id SERIAL PRIMARY KEY,
          bitrix_task_id INT,
          tg_user_id BIGINT,
          action TEXT,
          payload JSONB,
          created_at TIMESTAMP DEFAULT now()
        )""")
        # seed teams
        values = [(tid, TEAMS[tid]) for tid in TEAMS]
        await conn.executemany("""
          INSERT INTO teams (id, name)
          VALUES ($1,$2)
          ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
        """, values)
    finally:
        await conn.close()

# users
async def get_user(conn, tg_user_id: int):
    return await conn.fetchrow("SELECT * FROM users WHERE tg_user_id=$1", tg_user_id)

async def upsert_user_team(conn, tg_user_id: int, full_name: str, team_id: int):
    await conn.execute("""
      INSERT INTO users (tg_user_id, full_name, team_id)
      VALUES ($1,$2,$3)
      ON CONFLICT (tg_user_id) DO UPDATE SET full_name=EXCLUDED.full_name, team_id=EXCLUDED.team_id
    """, tg_user_id, full_name, team_id)

async def set_user_bitrix_id(conn, tg_user_id: int, bitrix_user_id: int):
    await conn.execute("UPDATE users SET bitrix_user_id=$1 WHERE tg_user_id=$2", bitrix_user_id, tg_user_id)

async def iter_team_users(conn, team_id: int):
    return await conn.fetch("SELECT * FROM users WHERE team_id=$1 ORDER BY full_name NULLS LAST", team_id)

async def log_action(conn, bitrix_task_id: int, tg_user_id: int, action: str, payload: dict):
    await conn.execute("""
      INSERT INTO task_actions (bitrix_task_id,tg_user_id,action,payload)
      VALUES ($1,$2,$3,$4)
    """, bitrix_task_id, tg_user_id, action, payload)

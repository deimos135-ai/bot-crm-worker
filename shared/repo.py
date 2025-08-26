# shared/repo.py
from __future__ import annotations
import asyncio
import asyncpg
from shared.settings import settings

_CONN: asyncpg.Connection | None = None
_LOCK = asyncio.Lock()

async def _open_connection() -> asyncpg.Connection:
    # 3 спроби з бекофом + таймаути на з’єднання/команди
    last_exc: Exception | None = None
    for attempt, delay in enumerate((0.5, 1.0, 2.0), start=1):
        try:
            return await asyncpg.connect(
                settings.DATABASE_URL,
                timeout=10,           # connect timeout
                command_timeout=10,   # per-query timeout
            )
        except Exception as e:       # noqa: BLE001 (контрольований ретрай)
            last_exc = e
            await asyncio.sleep(delay)
    assert last_exc is not None
    raise last_exc

async def connect() -> asyncpg.Connection:
    """
    Повертає один довгоживучий конект (реюз між запитами).
    Якщо з’єднання втрачено — відкриває заново.
    """
    global _CONN
    async with _LOCK:
        if _CONN is None or _CONN.is_closed():
            _CONN = await _open_connection()
    return _CONN

async def ensure_schema_and_seed() -> None:
    conn = await connect()
    await conn.execute("""
        create table if not exists chat_bindings (
            tg_user_id      bigint primary key,
            bitrix_email    text,
            bitrix_user_id  bigint
        );
    """)

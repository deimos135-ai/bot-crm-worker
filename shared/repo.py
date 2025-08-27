# shared/repo.py
import asyncpg
from .settings import settings
from .team_names import TEAMS
from .settings import settings

async def connect():
    return await asyncpg.connect(settings.DATABASE_URL)

async def ensure_schema_and_seed():
    conn = await connect()
    # tables
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
      name TEXT NOT NULL,
      lead_tg_chat_id BIGINT
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
    # seed teams (id, name, chat) з secrets і TEAMS
    values = [(tid, TEAMS[tid], settings.LEADS_CHAT_BY_TEAM.get(tid)) for tid in TEAMS]
    await conn.executemany("""
      INSERT INTO teams (id, name, lead_tg_chat_id)
      VALUES ($1,$2,$3)
      ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        lead_tg_chat_id = EXCLUDED.lead_tg_chat_id
    """, values)
    await conn.close()

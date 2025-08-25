import asyncpg
from .settings import settings
from .team_names import TEAMS

async def connect():
    return await asyncpg.connect(settings.DATABASE_URL)

async def ensure_schema_and_seed():
    conn = await connect()
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

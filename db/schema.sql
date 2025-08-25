-- Не обов'язково виконувати вручну (код робить ensure), але залишаю для зручності.
CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  tg_user_id BIGINT UNIQUE NOT NULL,
  bitrix_user_id INT,
  full_name TEXT,
  team_id INT,
  role TEXT DEFAULT 'worker',
  created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS teams (
  id INT PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS task_actions (
  id SERIAL PRIMARY KEY,
  bitrix_task_id INT,
  tg_user_id BIGINT,
  action TEXT,
  payload JSONB,
  created_at TIMESTAMP DEFAULT now()
);

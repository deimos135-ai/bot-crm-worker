@dp.message(Command("tasks"))
async def my_tasks(m: types.Message):
    # /tasks [open|today|overdue|closed_today]
    args = (m.text or "").split()[1:]
    mode = (args[0] if args else "open").lower()

    conn = await connect()
    u = await get_user(conn, m.from_user.id)
    await conn.close()

    bx_id = u["bitrix_user_id"] if u else None
    if not bx_id:
        await m.answer("Спочатку прив’яжіть Bitrix e-mail: `/bind user@company.com`", parse_mode=ParseMode.MARKDOWN)
        return

    now = dt.datetime.now(KYIV_TZ)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end   = now.replace(hour=23, minute=59, second=59, microsecond=0)

    # Які поля тягнемо
    fields = ["ID","TITLE","DEADLINE","STATUS","CLOSED_DATE","RESPONSIBLE_ID","CREATED_BY"]

    # Додаткові фільтри за режимом
    extra = {}
    header = "Відкриті завдання"
    if mode in ("today","сьогодні"):
        extra = {">=DEADLINE": day_start.isoformat(), "<=DEADLINE": day_end.isoformat()}
        header = "Завдання на сьогодні"
    elif mode in ("overdue","прострочені","over"):
        extra = {"<DEADLINE": now.isoformat(), "!STATUS": 5}  # не завершені
        header = "Прострочені завдання"
    elif mode in ("closed_today","done_today"):
        extra = {">=CLOSED_DATE": day_start.isoformat(), "<=CLOSED_DATE": day_end.isoformat()}
        header = "Сьогодні закриті"
    else:
        extra = {"!STATUS": 5}

    # 4 окремі запити по ролях — надійніше, ніж OR-логіка Bitrix
    filters = [
        {"RESPONSIBLE_ID": bx_id, **extra},
        {"ACCOMPLICE": bx_id, **extra},
        {"AUDITOR": bx_id, **extra},
        {"CREATED_BY": bx_id, **extra},
    ]

    # Тягнемо все і мерджимо по ID
    bag = {}
    for f in filters:
        try:
            res = list_tasks(f, fields)
            for t in (res.get("result") if isinstance(res, dict) else (res or [])):
                tid = str(t.get("ID") or t.get("id"))
                if tid and tid not in bag:
                    bag[tid] = t
        except Exception:
            # ігноруємо часткові помилки по окремих ролях
            pass

    tasks = list(bag.values())
    if not tasks:
        await m.answer("Задач за запитом не знайдено 🙂")
        return

    status_map = {1:"Нова",2:"В очікуванні",3:"В роботі",4:"Відкладена",5:"Завершена"}
    lines = []
    for t in tasks[:20]:
        tid = t.get("ID") or t.get("id")
        title = (t.get("TITLE") or t.get("title") or "").strip()
        deadline = t.get("DEADLINE") or t.get("deadline")
        status = t.get("STATUS") or t.get("status")

        dl_str = ""
        if deadline:
            try:
                dl = dt.datetime.fromisoformat(str(deadline).replace("Z","+00:00")).astimezone(KYIV_TZ)
                dl_str = dl.strftime("%d.%m %H:%M")
            except Exception:
                dl_str = str(deadline)

        status_txt = status_map.get(int(status)) if str(status).isdigit() else (str(status) if status else "")
        suffix = f" • до {dl_str}" if dl_str else ""
        extra = f" ({status_txt})" if status_txt and mode not in ("closed_today",) else ""
        lines.append(f"• #{tid}: {title}{suffix}{extra}")

    await m.answer(f"{header} (до 20):\n" + "\n".join(lines))

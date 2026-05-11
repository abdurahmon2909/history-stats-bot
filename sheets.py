from __future__ import annotations

import asyncio
import time
import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, WorksheetNotFound

from config import GOOGLE_CREDS, SHEET_ID

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

WS_USERS = "users"
WS_MESSAGES = "messages"

TASHKENT_TZ = ZoneInfo("Asia/Tashkent")

gc: gspread.Client | None = None
spreadsheet = None

# Worksheet cache
WS_CACHE: dict[str, gspread.Worksheet] = {}

# User row cache: {user_id: row_number}
USER_ROW_CACHE: dict[int, int] = {}

# User data cache
USER_DATA_CACHE: dict[int, dict[str, str]] = {}

# Message buffer
MESSAGE_BUFFER: list[list[str]] = []
BUFFER_LOCK = asyncio.Lock()

# Flush control
FLUSH_TASK: asyncio.Task | None = None
FLUSH_INTERVAL_SECONDS = 3
MAX_BUFFER_SIZE = 25

# Statistikadan chiqarib tashlanadigan userlar
EXCLUDED_USER_IDS = {
    159312129,  # Fazliddin Burxonov
}


def _connect_sync():
    global gc, spreadsheet
    creds = Credentials.from_service_account_info(GOOGLE_CREDS, scopes=SCOPES)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SHEET_ID)
    logging.info("Google Sheets ga ulandi")
    return spreadsheet


def _retry_sync(func, *args, **kwargs):
    delays = [1, 2, 4, 8, 16]
    last_error = None
    for delay in [0] + delays:
        try:
            if delay:
                time.sleep(delay + random.uniform(0, 1))
            return func(*args, **kwargs)
        except APIError as e:
            last_error = e
            err_text = str(e).lower()
            if "quota exceeded" in err_text or "429" in err_text or "rate limit" in err_text:
                logging.warning(f"API limit hit, retrying after {delay}s: {e}")
                continue
            raise
        except Exception as e:
            last_error = e
            raise
    raise last_error


def _ensure_ws_sync(title: str, headers: list[str]):
    global spreadsheet, WS_CACHE
    try:
        ws = spreadsheet.worksheet(title)
        logging.info(f"Worksheet '{title}' mavjud")
    except WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=1000, cols=max(10, len(headers) + 2))
        ws.append_row(headers)
        logging.info(f"Yangi worksheet '{title}' yaratildi")
    else:
        existing_headers = ws.row_values(1)
        if not existing_headers:
            ws.append_row(headers)
    WS_CACHE[title] = ws
    return ws


def _get_ws_sync(title: str):
    global WS_CACHE
    if title in WS_CACHE:
        return WS_CACHE[title]
    ws = _retry_sync(spreadsheet.worksheet, title)
    WS_CACHE[title] = ws
    return ws


def _warm_user_cache_sync():
    global USER_ROW_CACHE, USER_DATA_CACHE
    logging.info("Cache yuklanmoqda...")
    ws = _get_ws_sync(WS_USERS)
    values = _retry_sync(ws.get_all_values)
    USER_ROW_CACHE = {}
    USER_DATA_CACHE = {}
    for idx, row in enumerate(values[1:], start=2):
        if not row or len(row) < 1:
            continue
        try:
            user_id = int(str(row[0]).strip())
        except (ValueError, IndexError):
            continue
        full_name   = row[1].strip() if len(row) > 1 and row[1] else ""
        username    = row[2].strip() if len(row) > 2 and row[2] else ""
        is_subscribed = row[3].strip() if len(row) > 3 and row[3] else "0"
        first_seen  = row[4].strip() if len(row) > 4 else ""
        last_seen   = row[5].strip() if len(row) > 5 else ""
        USER_ROW_CACHE[user_id] = idx
        USER_DATA_CACHE[user_id] = {
            "full_name": full_name,
            "username": username,
            "is_subscribed": is_subscribed,
            "first_seen": first_seen,
            "last_seen": last_seen,
        }
    logging.info(f"Cache yuklandi: {len(USER_DATA_CACHE)} ta user")


async def init_sheets():
    await asyncio.to_thread(_connect_sync)
    await asyncio.to_thread(
        _ensure_ws_sync,
        WS_USERS,
        ["user_id", "full_name", "username", "is_subscribed", "first_seen", "last_seen"],
    )
    await asyncio.to_thread(
        _ensure_ws_sync,
        WS_MESSAGES,
        ["chat_id", "message_id", "user_id", "full_name", "username", "text", "sent_at"],
    )
    await asyncio.to_thread(_warm_user_cache_sync)
    logging.info("Sheets initializatsiyasi tugadi")


# ─────────────────────────────────────────────────────────────────────────────
# upsert_user  –  faqat ro'yxatga olish (ism bo'sh bo'lsa YOZMA)
# ─────────────────────────────────────────────────────────────────────────────

async def upsert_user(
    user_id: int,
    full_name: str,
    username: str | None,
    is_subscribed: int | None = None,
):
    await asyncio.to_thread(_upsert_user_sync, user_id, full_name, username, is_subscribed)


def _upsert_user_sync(
    user_id: int,
    full_name: str,
    username: str | None,
    is_subscribed: int | None = None,
):
    """
    Foydalanuvchini sheetga yozadi.
    MUHIM QOIDA: agar sheetda allaqachon HAQIQIY ism bo'lsa —
    uni hech qachon bo'sh yoki Telegram ismi bilan qayta yozmaydi.
    Haqiqiy ism faqat update_user_fullname() orqali o'zgartiriladi.
    """
    global USER_ROW_CACHE, USER_DATA_CACHE

    ws = _get_ws_sync(WS_USERS)
    now = datetime.now(timezone.utc).isoformat()
    row_num = USER_ROW_CACHE.get(user_id)

    # Sheetdagi mavjud ismni saqlaymiz — bo'sh argument bilan ezib tashlamaymiz
    cached = USER_DATA_CACHE.get(user_id, {})
    existing_name = cached.get("full_name", "").strip()

    # Yangi argument faqat sheetda ism YO'Q bo'lganda ishlatiladi
    # (ya'ni foydalanuvchi hali ism kirmagan bo'lsa)
    final_name = existing_name if existing_name else (full_name.strip() if full_name else "")

    if row_num:
        current_sub = cached.get("is_subscribed", "0")
        first_seen  = cached.get("first_seen", now) or now
        new_sub     = str(is_subscribed) if is_subscribed is not None else current_sub

        values = [[
            str(user_id),
            final_name,
            username or "",
            new_sub,
            first_seen,
            now,
        ]]
        _retry_sync(ws.update, range_name=f"A{row_num}:F{row_num}", values=values)
        USER_DATA_CACHE[user_id] = {
            "full_name": final_name,
            "username": username or "",
            "is_subscribed": new_sub,
            "first_seen": first_seen,
            "last_seen": now,
        }
        logging.info(f"upsert_user UPDATE: {user_id} | name='{final_name}' | sub={new_sub}")

    else:
        values = [
            str(user_id),
            final_name,
            username or "",
            str(is_subscribed or 0),
            now,
            now,
        ]
        _retry_sync(ws.append_row, values)
        current_rows = _retry_sync(lambda: len(ws.col_values(1)))
        USER_ROW_CACHE[user_id] = current_rows
        USER_DATA_CACHE[user_id] = {
            "full_name": final_name,
            "username": username or "",
            "is_subscribed": str(is_subscribed or 0),
            "first_seen": now,
            "last_seen": now,
        }
        logging.info(f"upsert_user INSERT: {user_id} | name='{final_name}'")


# ─────────────────────────────────────────────────────────────────────────────
# update_user_fullname  –  faqat bu funksiya ismni HAQIQIY o'zgartiradi
# ─────────────────────────────────────────────────────────────────────────────

async def update_user_fullname(user_id: int, full_name: str):
    if full_name and full_name.strip():
        logging.info(f"update_user_fullname: user={user_id}, name='{full_name}'")
        await asyncio.to_thread(_update_user_fullname_sync, user_id, full_name)
    else:
        logging.warning(f"update_user_fullname: bo'sh ism — user={user_id}")


def _update_user_fullname_sync(user_id: int, new_full_name: str):
    global USER_ROW_CACHE, USER_DATA_CACHE

    ws = _get_ws_sync(WS_USERS)
    row_num = USER_ROW_CACHE.get(user_id)
    cleaned = new_full_name.strip()

    if not cleaned:
        logging.warning(f"Bo'sh ism saqlanmadi: user={user_id}")
        return

    now = datetime.now(timezone.utc).isoformat()

    if not row_num:
        # Yangi foydalanuvchi — qator qo'shish
        values = [str(user_id), cleaned, "", "1", now, now]
        _retry_sync(ws.append_row, values)
        current_rows = _retry_sync(lambda: len(ws.col_values(1)))
        USER_ROW_CACHE[user_id] = current_rows
        USER_DATA_CACHE[user_id] = {
            "full_name": cleaned,
            "username": "",
            "is_subscribed": "1",
            "first_seen": now,
            "last_seen": now,
        }
        logging.info(f"update_fullname INSERT: {user_id} -> '{cleaned}'")
        return

    # Mavjud foydalanuvchi — faqat ism va last_seen yangilanadi
    cached = USER_DATA_CACHE.get(user_id, {})
    values = [[
        str(user_id),
        cleaned,
        cached.get("username", ""),
        cached.get("is_subscribed", "1"),
        cached.get("first_seen", now),
        now,
    ]]
    _retry_sync(ws.update, range_name=f"A{row_num}:F{row_num}", values=values)

    if user_id not in USER_DATA_CACHE:
        USER_DATA_CACHE[user_id] = {}
    USER_DATA_CACHE[user_id]["full_name"] = cleaned
    USER_DATA_CACHE[user_id]["last_seen"] = now
    logging.info(f"update_fullname UPDATE: {user_id} -> '{cleaned}'")


# ─────────────────────────────────────────────────────────────────────────────
# get_user_fullname / has_user_fullname
# ─────────────────────────────────────────────────────────────────────────────

async def get_user_fullname(user_id: int) -> str | None:
    if user_id in USER_DATA_CACHE:
        name = USER_DATA_CACHE[user_id].get("full_name", "").strip()
        if name:
            return name
    return await asyncio.to_thread(_get_user_fullname_sync, user_id)


def _get_user_fullname_sync(user_id: int) -> str | None:
    try:
        ws = _get_ws_sync(WS_USERS)
        cell = ws.find(str(user_id), in_column=1)
        if not cell:
            return None
        row = ws.row_values(cell.row)
        if len(row) > 1 and row[1].strip():
            name = row[1].strip()
            if user_id not in USER_DATA_CACHE:
                USER_DATA_CACHE[user_id] = {}
            USER_DATA_CACHE[user_id]["full_name"] = name
            return name
    except Exception as e:
        logging.error(f"get_user_fullname xato: {e}")
    return None


async def has_user_fullname(user_id: int) -> bool:
    name = await get_user_fullname(user_id)
    return bool(name and name.strip())


# ─────────────────────────────────────────────────────────────────────────────
# append_group_message  –  cache'dan haqiqiy ismni oladi
# ─────────────────────────────────────────────────────────────────────────────

async def append_group_message(
    chat_id: int,
    message_id: int,
    user_id: int,
    full_name: str,
    username: str | None,
    text: str | None,
    sent_at: datetime,
):
    sent_at_tashkent = sent_at.astimezone(TASHKENT_TZ)

    # Doim cache'dan haqiqiy ismni olamiz
    cached_name = USER_DATA_CACHE.get(user_id, {}).get("full_name", "").strip()
    display_name = cached_name if cached_name else "Ism kiritilmagan"

    row = [
        str(chat_id),
        str(message_id),
        str(user_id),
        display_name,
        username or "",
        (text or "")[:45000],
        sent_at_tashkent.isoformat(),
    ]

    async with BUFFER_LOCK:
        MESSAGE_BUFFER.append(row)
        need_flush_now = len(MESSAGE_BUFFER) >= MAX_BUFFER_SIZE

    if need_flush_now:
        await flush_message_buffer()


def _append_rows_sync(rows: list[list[str]]):
    if not rows:
        return
    ws = _get_ws_sync(WS_MESSAGES)
    _retry_sync(ws.append_rows, rows, value_input_option="RAW")


async def flush_message_buffer():
    async with BUFFER_LOCK:
        if not MESSAGE_BUFFER:
            return
        rows_to_write = MESSAGE_BUFFER.copy()
        MESSAGE_BUFFER.clear()
    try:
        await asyncio.to_thread(_append_rows_sync, rows_to_write)
        logging.debug(f"Flushed {len(rows_to_write)} messages to sheet")
    except Exception as e:
        logging.error(f"Flush error: {e}")
        async with BUFFER_LOCK:
            MESSAGE_BUFFER[:0] = rows_to_write
        raise


async def _periodic_flush_loop():
    while True:
        await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
        try:
            await flush_message_buffer()
        except Exception as e:
            logging.error(f"Periodic flush error: {e}")


async def start_background_flush():
    global FLUSH_TASK
    if FLUSH_TASK is None or FLUSH_TASK.done():
        FLUSH_TASK = asyncio.create_task(_periodic_flush_loop())
        logging.info("Background flush started")


async def stop_background_flush():
    global FLUSH_TASK
    if FLUSH_TASK and not FLUSH_TASK.done():
        FLUSH_TASK.cancel()
        try:
            await FLUSH_TASK
        except asyncio.CancelledError:
            pass
    await flush_message_buffer()
    logging.info("Background flush stopped")


# ─────────────────────────────────────────────────────────────────────────────
# Statistika
# ─────────────────────────────────────────────────────────────────────────────

def classify_activity(share_percent: float) -> str:
    if share_percent >= 5:
        return "Faol"
    if share_percent >= 3:
        return "Yaxshi"
    if share_percent >= 2:
        return "O'rtacha"
    return "Qoniqarli"


async def get_stats_for_hours(chat_id: int, hours: int) -> dict[str, Any]:
    await flush_message_buffer()
    return await asyncio.to_thread(_get_stats_for_hours_sync, chat_id, hours)


def _get_stats_for_hours_sync(chat_id: int, hours: int) -> dict[str, Any]:
    ws = _get_ws_sync(WS_MESSAGES)
    rows = _retry_sync(ws.get_all_records)
    now = datetime.now(timezone.utc)
    start_dt = now - timedelta(hours=hours)

    filtered = []
    for row in rows:
        try:
            if int(str(row.get("chat_id", "0")).strip()) != int(chat_id):
                continue
            user_id = int(str(row.get("user_id", "0")).strip())
            if user_id in EXCLUDED_USER_IDS:
                continue
            sent_at_raw = str(row.get("sent_at", "")).strip()
            if not sent_at_raw:
                continue
            sent_at = datetime.fromisoformat(sent_at_raw)
            if sent_at.tzinfo is None:
                sent_at = sent_at.replace(tzinfo=TASHKENT_TZ)
            if sent_at < start_dt:
                continue
            filtered.append(row)
        except Exception:
            continue

    return _build_stats(filtered, start_dt, now)


async def get_stats_for_range(chat_id: int, start_dt: datetime, end_dt: datetime) -> dict[str, Any]:
    await flush_message_buffer()
    return await asyncio.to_thread(_get_stats_for_range_sync, chat_id, start_dt, end_dt)


def _get_stats_for_range_sync(
    chat_id: int,
    start_dt: datetime,
    end_dt: datetime
) -> dict[str, Any]:

    ws = _get_ws_sync(WS_MESSAGES)
    rows = _retry_sync(ws.get_all_records)

    # ❗ start/end ni Tashkent timezone ga o'tkazamiz
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=TASHKENT_TZ)
    else:
        start_dt = start_dt.astimezone(TASHKENT_TZ)

    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=TASHKENT_TZ)
    else:
        end_dt = end_dt.astimezone(TASHKENT_TZ)

    filtered = []

    for row in rows:

        try:
            if int(str(row.get("chat_id", "0")).strip()) != int(chat_id):
                continue

            user_id = int(str(row.get("user_id", "0")).strip())

            if user_id in EXCLUDED_USER_IDS:
                continue

            sent_at_raw = str(row.get("sent_at", "")).strip()

            if not sent_at_raw:
                continue

            sent_at = datetime.fromisoformat(sent_at_raw)

            # ❗ timezone yo'q bo'lsa Tashkent deb olamiz
            if sent_at.tzinfo is None:
                sent_at = sent_at.replace(tzinfo=TASHKENT_TZ)
            else:
                sent_at = sent_at.astimezone(TASHKENT_TZ)

            # ❗ oralig'ni tekshirish
            if not (start_dt <= sent_at <= end_dt):
                continue

            filtered.append(row)

        except Exception as e:
            logging.error(f"Range stats error: {e}")
            continue

    return _build_stats(filtered, start_dt, end_dt)


def _build_stats(filtered: list, start_dt: datetime, end_dt: datetime) -> dict[str, Any]:
    """Filtrlangan qatorlardan statistika yasaydi."""
    total_messages = len(filtered)
    per_user: dict[int, dict[str, Any]] = {}

    for row in filtered:
        try:
            user_id = int(str(row.get("user_id", "0")).strip())
            if user_id in EXCLUDED_USER_IDS:
                continue
        except Exception:
            continue

        # MUHIM: cache'dan haqiqiy ismni olamiz, sheet qatoridagi eski ismni emas
        cached_name = USER_DATA_CACHE.get(user_id, {}).get("full_name", "").strip()
        full_name = cached_name if cached_name else (
            str(row.get("full_name", "")).strip() or "Ism kiritilmagan"
        )
        username = str(row.get("username", "")).strip()

        if user_id not in per_user:
            per_user[user_id] = {
                "user_id": user_id,
                "full_name": full_name,
                "username": username,
                "msg_count": 0,
            }
        per_user[user_id]["msg_count"] += 1

    result = []
    for item in per_user.values():
        share = (item["msg_count"] / total_messages * 100) if total_messages else 0.0
        item["share_percent"] = round(share, 2)
        item["category"] = classify_activity(share)
        result.append(item)

    result.sort(key=lambda x: (-x["msg_count"], x["full_name"].lower()))

    return {
        "start_dt": start_dt,
        "end_dt": end_dt,
        "total_messages": total_messages,
        "users": result,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Yordamchi funksiyalar
# ─────────────────────────────────────────────────────────────────────────────

async def get_all_users() -> list[dict[str, Any]]:
    users = []
    for user_id, data in USER_DATA_CACHE.items():
        users.append({
            "user_id": user_id,
            "full_name": data.get("full_name", ""),
            "username": data.get("username", ""),
            "is_subscribed": data.get("is_subscribed", "0"),
        })
    return users


async def get_user_count() -> int:
    return len(USER_DATA_CACHE)


async def refresh_user_cache():
    await asyncio.to_thread(_warm_user_cache_sync)
    logging.info("User cache yangilandi")

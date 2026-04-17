from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Any

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

gc: gspread.Client | None = None
spreadsheet = None

# Worksheet cache
WS_CACHE: dict[str, gspread.Worksheet] = {}

# User row cache: {user_id: row_number}
USER_ROW_CACHE: dict[int, int] = {}

# Message buffer
MESSAGE_BUFFER: list[list[str]] = []
BUFFER_LOCK = asyncio.Lock()

# Flush control
FLUSH_TASK: asyncio.Task | None = None
FLUSH_INTERVAL_SECONDS = 3
MAX_BUFFER_SIZE = 25


def _connect_sync():
    global gc, spreadsheet

    creds = Credentials.from_service_account_info(GOOGLE_CREDS, scopes=SCOPES)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SHEET_ID)
    return spreadsheet


def _retry_sync(func, *args, **kwargs):
    """
    Simple retry wrapper for transient Google Sheets API failures.
    """
    delays = [1, 2, 4, 8]
    last_error = None

    for attempt, delay in enumerate([0] + delays, start=1):
        try:
            if delay:
                time.sleep(delay)
            return func(*args, **kwargs)
        except APIError as e:
            last_error = e
            err_text = str(e).lower()
            if "quota exceeded" in err_text or "429" in err_text or "rate limit" in err_text:
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
    except WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=1000, cols=max(10, len(headers) + 2))
        ws.append_row(headers)
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
    """
    Reads the users worksheet once and caches user_id -> row_number.
    """
    global USER_ROW_CACHE

    ws = _get_ws_sync(WS_USERS)
    values = _retry_sync(ws.get_all_values)

    USER_ROW_CACHE = {}
    for idx, row in enumerate(values[1:], start=2):
        if not row:
            continue
        try:
            user_id = int(str(row[0]).strip())
            USER_ROW_CACHE[user_id] = idx
        except Exception:
            continue


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


def _find_row_by_user_id_sync(user_id: int) -> int | None:
    if user_id in USER_ROW_CACHE:
        return USER_ROW_CACHE[user_id]
    return None


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
    ws = _get_ws_sync(WS_USERS)
    now = datetime.now(timezone.utc).isoformat()
    row_num = _find_row_by_user_id_sync(user_id)

    if row_num:
        row = _retry_sync(ws.row_values, row_num)
        current_sub = row[3] if len(row) > 3 else "0"
        first_seen = row[4] if len(row) > 4 else now
        new_sub = str(is_subscribed) if is_subscribed is not None else current_sub

        _retry_sync(
            ws.update,
            range_name=f"A{row_num}:F{row_num}",
            values=[[
                str(user_id),
                full_name,
                username or "",
                new_sub,
                first_seen,
                now,
            ]]
        )
    else:
        _retry_sync(
            ws.append_row,
            [
                str(user_id),
                full_name,
                username or "",
                str(is_subscribed or 0),
                now,
                now,
            ]
        )

        # New row number = current worksheet row count after append
        # Better than reading all rows again.
        current_rows = _retry_sync(lambda: len(ws.col_values(1)))
        USER_ROW_CACHE[user_id] = current_rows


async def append_group_message(
    chat_id: int,
    message_id: int,
    user_id: int,
    full_name: str,
    username: str | None,
    text: str | None,
    sent_at: datetime,
):
    row = [
        str(chat_id),
        str(message_id),
        str(user_id),
        full_name,
        username or "",
        (text or "")[:45000],
        sent_at.isoformat(),
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
    except Exception:
        # If writing fails, return rows back to buffer to avoid data loss
        async with BUFFER_LOCK:
            MESSAGE_BUFFER[:0] = rows_to_write
        raise


async def _periodic_flush_loop():
    while True:
        await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
        try:
            await flush_message_buffer()
        except Exception:
            # suppress loop crash; next cycle will retry
            pass


async def start_background_flush():
    global FLUSH_TASK
    if FLUSH_TASK is None or FLUSH_TASK.done():
        FLUSH_TASK = asyncio.create_task(_periodic_flush_loop())


async def stop_background_flush():
    global FLUSH_TASK

    if FLUSH_TASK and not FLUSH_TASK.done():
        FLUSH_TASK.cancel()
        try:
            await FLUSH_TASK
        except asyncio.CancelledError:
            pass

    await flush_message_buffer()


def classify_activity(share_percent: float) -> str:
    if share_percent >= 15:
        return "Faol"
    if share_percent >= 8:
        return "Yaxshi"
    if share_percent >= 3:
        return "O'rtacha"
    return "Qoniqarli"


async def get_stats_for_hours(chat_id: int, hours: int) -> dict[str, Any]:
    # Before report, flush pending rows so report includes fresh data
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

            sent_at_raw = str(row.get("sent_at", "")).strip()
            if not sent_at_raw:
                continue

            sent_at = datetime.fromisoformat(sent_at_raw)
            if sent_at.tzinfo is None:
                sent_at = sent_at.replace(tzinfo=timezone.utc)

            if sent_at < start_dt:
                continue

            filtered.append(row)
        except Exception:
            continue

    total_messages = len(filtered)
    per_user: dict[int, dict[str, Any]] = {}

    for row in filtered:
        try:
            user_id = int(str(row.get("user_id", "0")).strip())
        except Exception:
            continue

        full_name = str(row.get("full_name", "")).strip() or "Noma'lum"
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
        "end_dt": now,
        "total_messages": total_messages,
        "users": result,
    }

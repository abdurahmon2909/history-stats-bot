from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

from config import GOOGLE_CREDS, SHEET_ID

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

gc: gspread.Client | None = None
spreadsheet = None

WS_USERS = "users"
WS_MESSAGES = "messages"


def _connect_sync():
    global gc, spreadsheet

    creds = Credentials.from_service_account_info(GOOGLE_CREDS, scopes=SCOPES)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SHEET_ID)
    return spreadsheet


def _ensure_ws_sync(title: str, headers: list[str]):
    global spreadsheet
    try:
        ws = spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=1000, cols=max(10, len(headers) + 2))
        ws.append_row(headers)
    else:
        existing_headers = ws.row_values(1)
        if not existing_headers:
            ws.append_row(headers)
    return ws


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


def _get_ws_sync(title: str):
    return spreadsheet.worksheet(title)


def _find_row_by_user_id_sync(user_id: int) -> int | None:
    ws = _get_ws_sync(WS_USERS)
    values = ws.get_all_values()
    for idx, row in enumerate(values[1:], start=2):
        if row and str(user_id) == str(row[0]).strip():
            return idx
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
        row = ws.row_values(row_num)
        current_sub = row[3] if len(row) > 3 else "0"
        first_seen = row[4] if len(row) > 4 else now
        new_sub = str(is_subscribed) if is_subscribed is not None else current_sub

        ws.update(
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
        ws.append_row([
            str(user_id),
            full_name,
            username or "",
            str(is_subscribed or 0),
            now,
            now,
        ])


async def append_group_message(
    chat_id: int,
    message_id: int,
    user_id: int,
    full_name: str,
    username: str | None,
    text: str | None,
    sent_at: datetime,
):
    await asyncio.to_thread(
        _append_group_message_sync,
        chat_id,
        message_id,
        user_id,
        full_name,
        username,
        text,
        sent_at.isoformat(),
    )


def _append_group_message_sync(
    chat_id: int,
    message_id: int,
    user_id: int,
    full_name: str,
    username: str | None,
    text: str | None,
    sent_at_iso: str,
):
    ws = _get_ws_sync(WS_MESSAGES)
    ws.append_row([
        str(chat_id),
        str(message_id),
        str(user_id),
        full_name,
        username or "",
        (text or "")[:45000],
        sent_at_iso,
    ])


def classify_activity(share_percent: float) -> str:
    if share_percent >= 15:
        return "Faol"
    if share_percent >= 8:
        return "Yaxshi"
    if share_percent >= 3:
        return "O'rtacha"
    return "Qoniqarli"


async def get_stats_for_hours(chat_id: int, hours: int) -> dict[str, Any]:
    return await asyncio.to_thread(_get_stats_for_hours_sync, chat_id, hours)


def _get_stats_for_hours_sync(chat_id: int, hours: int) -> dict[str, Any]:
    ws = _get_ws_sync(WS_MESSAGES)
    rows = ws.get_all_records()

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

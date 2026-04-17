import json
import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BOT_USERNAME = os.getenv("BOT_USERNAME", "").strip().lstrip("@")

GROUP_CHAT_ID = int(os.getenv("GROUP_CHAT_ID", "0").strip())

CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0").strip())
CHANNEL_LINK = os.getenv("CHANNEL_LINK", "").strip()

ADMIN_IDS = [
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
]

SHEET_ID = os.getenv("SHEET_ID", "").strip()
GOOGLE_CREDS_RAW = os.getenv("GOOGLE_CREDS", "").strip()

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN topilmadi")

if not GROUP_CHAT_ID:
    raise ValueError("GROUP_CHAT_ID topilmadi")

if not CHANNEL_ID:
    raise ValueError("CHANNEL_ID topilmadi")

if not CHANNEL_LINK:
    raise ValueError("CHANNEL_LINK topilmadi")

if not ADMIN_IDS:
    raise ValueError("ADMIN_IDS topilmadi")

if not SHEET_ID:
    raise ValueError("SHEET_ID topilmadi")

if not GOOGLE_CREDS_RAW:
    raise ValueError("GOOGLE_CREDS topilmadi")

try:
    GOOGLE_CREDS = json.loads(GOOGLE_CREDS_RAW)
except json.JSONDecodeError as e:
    raise ValueError(f"GOOGLE_CREDS noto'g'ri JSON: {e}")

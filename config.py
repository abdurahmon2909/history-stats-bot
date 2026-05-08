import json
import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BOT_USERNAME = os.getenv("BOT_USERNAME", "").strip().lstrip("@")

# ESKI VERSIYA BILAN HAM ISHLASH UCHUN (backward compatibility)
GROUP_CHAT_IDS_STR = os.getenv("GROUP_CHAT_IDS", "")
GROUP_CHAT_ID_OLD = os.getenv("GROUP_CHAT_ID", "")

if GROUP_CHAT_IDS_STR:
    # Yangi versiya: bir nechta guruh
    GROUP_CHAT_IDS = [
        int(x.strip()) 
        for x in GROUP_CHAT_IDS_STR.split(",") 
        if x.strip().isdigit()
    ]
elif GROUP_CHAT_ID_OLD:
    # Eski versiya: bitta guruh
    GROUP_CHAT_IDS = [int(GROUP_CHAT_ID_OLD.strip())]
else:
    GROUP_CHAT_IDS = []

# Asosiy guruh (birinchi guruh yoki eski guruh)
MAIN_GROUP_CHAT_ID = GROUP_CHAT_IDS[0] if GROUP_CHAT_IDS else 0

# Guruh nomlari (ixtiyoriy)
GROUP_NAMES = {}
for group_id in GROUP_CHAT_IDS:
    group_name = os.getenv(f"GROUP_NAME_{abs(group_id)}", f"Guruh {GROUP_CHAT_IDS.index(group_id) + 1}")
    GROUP_NAMES[group_id] = group_name

CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0").strip())
CHANNEL_LINK = os.getenv("CHANNEL_LINK", "").strip()

ADMIN_IDS = [
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
]

SHEET_ID = os.getenv("SHEET_ID", "").strip()
GOOGLE_CREDS_RAW = os.getenv("GOOGLE_CREDS", "").strip()

# Validatsiya
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN topilmadi")

if not GROUP_CHAT_IDS:
    # Aniq xatolik va yechim
    print("=" * 50)
    print("XATOLIK: GROUP_CHAT_IDS yoki GROUP_CHAT_ID topilmadi!")
    print("Iltimos, .env fayliga quyidagilardan birini qo'shing:")
    print("1. GROUP_CHAT_IDS=-1001234567890,-1009876543210  (bir nechta guruh)")
    print("2. GROUP_CHAT_ID=-1001234567890  (bitta guruh)")
    print("=" * 50)
    raise ValueError("GROUP_CHAT_IDS topilmadi (kamida bitta guruh ID kerak). .env faylini tekshiring!")

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

# Logging uchun
print(f"Yuklangan guruhlar: {len(GROUP_CHAT_IDS)} ta")
for gid in GROUP_CHAT_IDS:
    print(f"  - {GROUP_NAMES[gid]}: {gid}")

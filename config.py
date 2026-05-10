import json
import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BOT_USERNAME = os.getenv("BOT_USERNAME", "").strip().lstrip("@")

# ============ GURUH ID LARNI O'QISH ============
GROUP_CHAT_IDS = []

# 1. Yangi usul: GROUP_CHAT_IDS (vergul bilan ajratilgan bir nechta ID)
group_chat_ids_str = os.getenv("GROUP_CHAT_IDS", "").strip()
if group_chat_ids_str:
    for part in group_chat_ids_str.split(","):
        part = part.strip()
        if part:
            try:
                GROUP_CHAT_IDS.append(int(part))
            except ValueError:
                print(f"WARNING: '{part}' noto'g'ri guruh ID, tashlab ketilmoqda")

# 2. Eski usul: GROUP_CHAT_ID (bitta ID)
if not GROUP_CHAT_IDS:
    group_chat_id_old = os.getenv("GROUP_CHAT_ID", "").strip()
    if group_chat_id_old:
        # Agar vergul bo'lsa, xatolik chiqarmaslik uchun
        if "," in group_chat_id_old:
            print("WARNING: GROUP_CHAT_ID da vergul bor! GROUP_CHAT_IDS dan foydalaning.")
            # Vergul bilan ajratilgan bo'lsa, ham qabul qilamiz
            for part in group_chat_id_old.split(","):
                part = part.strip()
                if part:
                    try:
                        GROUP_CHAT_IDS.append(int(part))
                    except ValueError:
                        print(f"WARNING: '{part}' noto'g'ri guruh ID")
        else:
            try:
                GROUP_CHAT_IDS.append(int(group_chat_id_old))
            except ValueError:
                print(f"WARNING: '{group_chat_id_old}' noto'g'ri guruh ID")

# 3. Agar hali ham bo'sh bo'lsa, xatolik
if not GROUP_CHAT_IDS:
    print("=" * 60)
    print("XATOLIK: Hech qanday guruh ID topilmadi!")
    print("Iltimos, .env fayliga quyidagilardan birini qo'shing:")
    print("  - GROUP_CHAT_IDS=-1001234567890,-1009876543210  (bir nechta guruh)")
    print("  - GROUP_CHAT_ID=-1001234567890  (bitta guruh)")
    print("=" * 60)
    raise ValueError("Guruh ID topilmadi! .env faylini tekshiring.")

# Asosiy guruh (birinchi guruh)
MAIN_GROUP_CHAT_ID = GROUP_CHAT_IDS[0]

# Guruh nomlari (ixtiyoriy)
GROUP_NAMES = {-1001716132943: "Toshkent Attestatsiya",
    -1003903298311: "Yangi Guruh",
              -1003967312441: "DARS"}
for idx, group_id in enumerate(GROUP_CHAT_IDS, 1):
    # Avval maxsus nomni qidirish
    custom_name = os.getenv(f"GROUP_NAME_{abs(group_id)}", "")
    if custom_name:
        GROUP_NAMES[group_id] = custom_name
    else:
        GROUP_NAMES[group_id] = f"Guruh {idx}"

# ============ QOLGAN SOZLAMALAR ============
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

# Debug ma'lumot
print("=" * 40)
print("Config yuklandi:")
print(f"  Bot token: {'✓' if BOT_TOKEN else '✗'}")
print(f"  Guruhlar soni: {len(GROUP_CHAT_IDS)}")
for gid in GROUP_CHAT_IDS:
    print(f"    - {GROUP_NAMES[gid]}: {gid}")
print(f"  Kanal ID: {CHANNEL_ID}")
print(f"  Adminlar: {len(ADMIN_IDS)} ta")
print("=" * 40)

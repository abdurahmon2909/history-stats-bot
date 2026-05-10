# ============ bot.py (TO'LIQ TUZATILGAN) ============

from __future__ import annotations

import asyncio
import logging
import os
import calendar as cal_module
from datetime import datetime, timedelta, timezone
from typing import Optional
from aiogram.types import (
    BotCommand,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeAllGroupChats,
)
from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ChatMemberStatus, ChatType
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    FSInputFile,
    BotCommand,
)

from config import (
    BOT_TOKEN,
    GROUP_CHAT_IDS,
    GROUP_NAMES,
    MAIN_GROUP_CHAT_ID,
    CHANNEL_ID,
    CHANNEL_LINK,
    ADMIN_IDS,
)
from sheets import (
    init_sheets,
    upsert_user,
    update_user_fullname,
    append_group_message,
    get_stats_for_range,
    get_stats_for_hours,
    has_user_fullname,
    get_user_fullname,
    start_background_flush,
    stop_background_flush,
    get_all_users,
    get_user_count,
)
from pdf_report import build_pdf_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

from group_events import router as group_events_router

bot = Bot(BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)
dp.include_router(group_events_router)


# ─────────────────────────────────────────────────────────────────────────────
# STATE GURUHLARI
# ─────────────────────────────────────────────────────────────────────────────

class RegisterState(StatesGroup):
    waiting_for_fullname = State()


class BroadcastState(StatesGroup):
    waiting_for_message = State()


class AdminReportState(StatesGroup):
    waiting_for_start_date = State()
    waiting_for_start_time = State()
    waiting_for_end_date   = State()
    waiting_for_end_time   = State()


# ─────────────────────────────────────────────────────────────────────────────
# YORDAMCHI FUNKSIYALAR
# ─────────────────────────────────────────────────────────────────────────────

BOT_ID = None


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def join_channel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Kanalga o'tish ➡️", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="Tekshirish ✅", callback_data="check_sub")],
        ]
    )


def admin_main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Tez hisobot",            callback_data="admin:quick")],
            [InlineKeyboardButton(text="📅 Qo'lda vaqt tanlash",   callback_data="admin:custom")],
        ]
    )


async def check_subscription(user_id: int) -> tuple[bool, str]:
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        is_sub = member.status in {
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.CREATOR,
        }
        return is_sub, "ok"
    except TelegramBadRequest as e:
        err = str(e).lower()
        if "member list is inaccessible" in err:
            return False, "inaccessible"
        return False, "error"
    except Exception:
        return False, "error"


# ─────────────────────────────────────────────────────────────────────────────
# /start  →  obuna tekshirish  →  ism so'rash
# ─────────────────────────────────────────────────────────────────────────────

@router.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    user = message.from_user
    if not user:
        return

    # 1. Obunani tekshirish
    subscribed, status = await check_subscription(user.id)

    if status == "inaccessible":
        await message.answer(
            "⚠️ Kanal a'zoligini tekshirib bo'lmadi.\n"
            "Admin botni kanalga to'liq ulab chiqishi kerak."
        )
        return

    if status == "error":
        await message.answer("❌ Tekshiruvda xatolik. Keyinroq qayta urinib ko'ring.")
        return

    if not subscribed:
        await message.answer(
            "📢 Botdan foydalanish uchun avval kanalga a'zo bo'ling.",
            reply_markup=join_channel_kb(),
        )
        return

    # 2. Foydalanuvchini sheetga yozamiz (ism bo'sh — bu yerda muhim emas,
    #    chunki upsert_user haqiqiy ismni ezib tashlamaydi)
    await upsert_user(
        user_id=user.id,
        full_name="",          # bo'sh — sheets.py ichida mavjud ism saqlanadi
        username=user.username,
        is_subscribed=1,
    )

    # 3. Har doim ism so'raymiz (yangilash uchun)
    await state.set_state(RegisterState.waiting_for_fullname)
    await message.answer(
        "✅ <b>Obuna tasdiqlandi!</b>\n\n"
        "📝 Iltimos, <b>to'liq ism va familiyangizni</b> lotin harflarida kiriting:\n"
        "<i>Masalan: Murodjonov Asilbek</i>\n\n"
        "❌ Bekor qilish: /cancel",
        parse_mode="HTML",
    )


# ─────────────────────────────────────────────────────────────────────────────
# check_sub callback  →  obuna tasdiqlangach ism so'rash
# ─────────────────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "check_sub")
async def check_subscription_callback(callback: CallbackQuery, state: FSMContext):
    user = callback.from_user
    if not user:
        await callback.answer("Foydalanuvchi topilmadi", show_alert=True)
        return

    subscribed, status = await check_subscription(user.id)

    if status == "inaccessible":
        await callback.answer(
            "Bot kanal a'zoligini tekshira olmayapti. Admin botni kanalga admin qilishi kerak.",
            show_alert=True,
        )
        return

    if status == "error":
        await callback.answer("Tekshiruvda xatolik. Keyinroq qayta urinib ko'ring.", show_alert=True)
        return

    if not subscribed:
        await callback.answer("Siz hali kanalga a'zo bo'lmagansiz ❌", show_alert=True)
        return

    await upsert_user(
        user_id=user.id,
        full_name="",
        username=user.username,
        is_subscribed=1,
    )

    await callback.answer("Obuna tasdiqlandi ✅")
    await callback.message.delete()

    await state.set_state(RegisterState.waiting_for_fullname)
    await callback.message.answer(
        "✅ <b>Obuna tasdiqlandi!</b>\n\n"
        "📝 Iltimos, <b>to'liq ism va familiyangizni</b> lotin harflarida kiriting:\n"
        "<i>Masalan: Murodjonov Asilbek</i>\n\n"
        "❌ Bekor qilish: /cancel",
        parse_mode="HTML",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Ism kiritish  →  sheetga yozish  (BITTA handler)
# ─────────────────────────────────────────────────────────────────────────────

@router.message(RegisterState.waiting_for_fullname)
async def register_fullname_handler(message: Message, state: FSMContext):
    user = message.from_user
    if not user:
        return

    text = (message.text or "").strip()

    # Validatsiya: kamida 3 belgi
    if len(text) < 3:
        await message.answer(
            "❌ Ism va familiya kamida 3 harfdan iborat bo'lishi kerak.\n"
            "Qaytadan kiriting:"
        )
        return

    # Validatsiya: faqat harf, bo'sh joy, tire
    if not all(c.isalpha() or c.isspace() or c == "-" for c in text):
        await message.answer(
            "❌ Faqat harflar, bo'sh joy va '-' belgisi ruxsat etiladi.\n"
            "<i>Masalan: Murodjonov Asilbek</i>\n\n"
            "Qaytadan kiriting:",
            parse_mode="HTML",
        )
        return

    # ✅ Ismni sheetga yozamiz — bu yagona to'g'ri joy
    await update_user_fullname(user.id, text)
    await state.clear()

    logging.info(f"✅ Ism saqlandi: user={user.id} → '{text}'")

    if is_admin(user.id):
        await message.answer(
            f"✅ <b>Assalomu alaykum, {text}!</b>\n\n"
            "🔽 <b>Admin panel:</b> /admin\n"
            "📢 <b>Xabar yuborish:</b> /broadcast\n"
            "✏️ <b>Ism o'zgartirish:</b> /editname",
            parse_mode="HTML",
            reply_markup=admin_main_menu_kb(),
        )
    else:
        await message.answer(
            f"✅ <b>Assalomu alaykum, {text}!</b>\n\n"
            "🎉 Xush kelibsiz!\n\n"
            "✏️ Bu yerda siz ustoz Fazliddin Burxonovga savol yo'llashingiz mumkin va test savollariga javoblar yuborishingiz mumkin ",
            parse_mode="HTML",
        )


# ─────────────────────────────────────────────────────────────────────────────
# /editname  →  ism o'zgartirish (xuddi start oqimi, lekin obunasiz)
# ─────────────────────────────────────────────────────────────────────────────

@router.message(Command("editname"))
async def edit_fullname_handler(message: Message, state: FSMContext):
    user = message.from_user
    if not user:
        return

    subscribed, status = await check_subscription(user.id)
    if not subscribed:
        await message.answer("Avval kanalga a'zo bo'ling.", reply_markup=join_channel_kb())
        return

    await state.set_state(RegisterState.waiting_for_fullname)
    await message.answer(
        "📝 <b>Ism familiyangizni o'zgartirish</b>\n\n"
        "Yangi to'liq ism va familiyangizni kiriting:\n"
        "<i>Masalan: Murodjonov Asilbek</i>\n\n"
        "❌ Bekor qilish: /cancel",
        parse_mode="HTML",
    )

# ─────────────────────────────────────────────────────────────────────────────
# USER → ADMIN SUPPORT
# ─────────────────────────────────────────────────────────────────────────────

# Forward qilingan message_id -> user_id
SUPPORT_REPLY_MAP: dict[int, int] = {}


@router.message(
    F.chat.type == ChatType.PRIVATE,
    F.reply_to_message
)
async def admin_reply_to_user(message: Message):
    """
    Admin reply qilsa userga yuboradi
    """
    user = message.from_user

    if not user or not is_admin(user.id):
        return

    replied = message.reply_to_message

    if not replied:
        return

    target_user_id = SUPPORT_REPLY_MAP.get(replied.message_id)

    if not target_user_id:
        return

    try:
        # TEXT
        if message.text:
            await bot.send_message(
                target_user_id,
                f"📨 <b>Admin javobi:</b>\n\n{message.text}",
                parse_mode="HTML",
            )

        # PHOTO
        elif message.photo:
            await bot.send_photo(
                target_user_id,
                photo=message.photo[-1].file_id,
                caption=message.caption or "📨 Admin javobi",
            )

        # VIDEO
        elif message.video:
            await bot.send_video(
                target_user_id,
                video=message.video.file_id,
                caption=message.caption or "📨 Admin javobi",
            )

        # DOCUMENT
        elif message.document:
            await bot.send_document(
                target_user_id,
                document=message.document.file_id,
                caption=message.caption or "📨 Admin javobi",
            )

        await message.reply("✅ Javob foydalanuvchiga yuborildi.")

    except Exception as e:
        logging.error(f"Admin reply error: {e}")
        await message.reply(f"❌ Xatolik: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# /cancel
# ─────────────────────────────────────────────────────────────────────────────

@router.message(Command("cancel"))
async def cancel_handler(message: Message, state: FSMContext):
    current = await state.get_state()
    if current:
        await state.clear()
        await message.answer("❌ Bekor qilindi.")
    else:
        await message.answer("Hech narsa bajarilmayapti.")


# ─────────────────────────────────────────────────────────────────────────────
# BROADCAST
# ─────────────────────────────────────────────────────────────────────────────

def get_confirm_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ BOT ORQALI YUBORISH", callback_data="confirm_send"),
                InlineKeyboardButton(text="👥 GURUHGA YUBORISH",   callback_data="send_to_group"),
            ],
            [
                InlineKeyboardButton(text="📋 XABARNI KO'RISH",   callback_data="preview_message"),
                InlineKeyboardButton(text="❌ BEKOR QILISH",        callback_data="cancel_broadcast"),
            ],
        ]
    )


def get_result_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📢 YANGI XABAR YUBORISH", callback_data="new_broadcast")]
        ]
    )


@router.message(Command("broadcast"))
async def broadcast_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.reply("❌ Bu buyruq faqat adminlar uchun!")
        return
    await state.set_state(BroadcastState.waiting_for_message)
    await message.answer(
        "📢 <b>ELON YUBORISH</b>\n\n"
        "Yubormoqchi bo'lgan xabaringizni kiriting:\n\n"
        "• 📝 Matn  • 🖼 Rasm  • 🎥 Video  • 📎 Hujjat\n\n"
        "❌ Bekor qilish: /cancel",
        parse_mode="HTML",
    )


@router.message(BroadcastState.waiting_for_message)
async def get_broadcast_message(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return

    if message.text:
        msg_data = {"type": "text",     "content": message.text,               "caption": ""}
    elif message.photo:
        msg_data = {"type": "photo",    "content": message.photo[-1].file_id,  "caption": message.caption or ""}
    elif message.video:
        msg_data = {"type": "video",    "content": message.video.file_id,      "caption": message.caption or ""}
    elif message.document:
        msg_data = {"type": "document", "content": message.document.file_id,   "caption": message.caption or ""}
    else:
        await message.answer("❌ Faqat matn, rasm, video yoki hujjat yuboring!")
        return

    await state.update_data(message_data=msg_data)

    total = await get_user_count()
    preview = (
        f"📢 <b>XABAR TAYYOR</b>\n\n"
        f"Tur: <b>{msg_data['type'].upper()}</b>\n"
        f"👥 Botdagi userlar: <b>{total}</b>\n"
        f"👥 Guruhlar: <b>{len(GROUP_CHAT_IDS)}</b>\n\n"
        f"Qayerga yuboramiz?"
    )
    await message.answer(preview, parse_mode="HTML", reply_markup=get_confirm_keyboard())


@router.callback_query(F.data == "preview_message")
async def preview_callback(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    msg = data.get("message_data")
    if not msg:
        await callback.answer("Xabar topilmadi!", show_alert=True)
        return
    if msg["type"] == "text":
        await callback.message.answer(f"📋 Xabar matni:\n\n{msg['content']}")
    elif msg["type"] == "photo":
        await callback.message.answer_photo(msg["content"], caption=msg["caption"] or None)
    elif msg["type"] == "video":
        await callback.message.answer_video(msg["content"], caption=msg["caption"] or None)
    elif msg["type"] == "document":
        await callback.message.answer_document(msg["content"], caption=msg["caption"] or None)
    await callback.answer()


@router.callback_query(F.data == "send_to_group")
async def send_to_group_cb(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    msg = data.get("message_data")
    if not msg:
        await callback.answer("Xabar topilmadi!", show_alert=True)
        return
    await callback.message.edit_text("📤 Guruhga yuborilmoqda...")
    try:
        if msg["type"] == "text":
            await bot.send_message(MAIN_GROUP_CHAT_ID, msg["content"])
        elif msg["type"] == "photo":
            await bot.send_photo(MAIN_GROUP_CHAT_ID, msg["content"], caption=msg.get("caption"))
        elif msg["type"] == "video":
            await bot.send_video(MAIN_GROUP_CHAT_ID, msg["content"], caption=msg.get("caption"))
        elif msg["type"] == "document":
            await bot.send_document(MAIN_GROUP_CHAT_ID, msg["content"], caption=msg.get("caption"))
        await state.clear()
        await callback.message.edit_text(
            "✅ <b>Xabar guruhga yuborildi!</b>",
            parse_mode="HTML",
            reply_markup=get_result_keyboard(),
        )
    except Exception as e:
        await callback.message.edit_text(f"❌ Xatolik: {e}")
    await callback.answer()


@router.callback_query(F.data == "confirm_send")
async def confirm_send_cb(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    msg = data.get("message_data")
    if not msg:
        await callback.answer("Xabar topilmadi!", show_alert=True)
        return
    await callback.message.edit_text("📤 Barcha foydalanuvchilarga yuborilmoqda...\n⏳ Biroz kuting...")

    users = await get_all_users()
    success = fail = 0

    for user in users:
        uid = user.get("user_id")
        if not uid:
            continue
        try:
            if msg["type"] == "text":
                await bot.send_message(uid, msg["content"])
            elif msg["type"] == "photo":
                await bot.send_photo(uid, msg["content"], caption=msg.get("caption"))
            elif msg["type"] == "video":
                await bot.send_video(uid, msg["content"], caption=msg.get("caption"))
            elif msg["type"] == "document":
                await bot.send_document(uid, msg["content"], caption=msg.get("caption"))
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            fail += 1
            logging.error(f"Yuborilmadi {uid}: {e}")

    await state.clear()
    total = len(users)
    pct = f"{success/total*100:.1f}%" if total else "–"
    await callback.message.edit_text(
        f"✅ <b>Yuborish yakunlandi!</b>\n\n"
        f"✅ Muvaffaqiyatli: {success}\n"
        f"❌ Muvaffaqiyatsiz: {fail}\n"
        f"📊 Jami: {total}\n"
        f"📈 Muvaffaqiyat: {pct}\n\n"
        f"Tur: {msg['type'].upper()}",
        parse_mode="HTML",
        reply_markup=get_result_keyboard(),
    )
    await callback.answer()


@router.callback_query(F.data == "cancel_broadcast")
async def cancel_broadcast_cb(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("❌ Xabar yuborish bekor qilindi!")
    await callback.answer()


@router.callback_query(F.data == "new_broadcast")
async def new_broadcast_cb(callback: CallbackQuery, state: FSMContext):
    await state.set_state(BroadcastState.waiting_for_message)
    await callback.message.edit_text(
        "📢 <b>YANGI XABAR KIRITING</b>\n\n❌ Bekor qilish: /cancel",
        parse_mode="HTML",
    )
    await callback.answer()


# ─────────────────────────────────────────────────────────────────────────────
# ADMIN PANEL
# ─────────────────────────────────────────────────────────────────────────────

@router.message(Command("admin"))
async def admin_command(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer("👋 Admin panelga xush kelibsiz!", reply_markup=admin_main_menu_kb())


@router.callback_query(F.data == "admin:back_to_main")
async def back_to_main_cb(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return
    await callback.message.edit_text("👋 Admin panel", reply_markup=admin_main_menu_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:cancel_report")
async def cancel_report_cb(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return
    await state.clear()
    await callback.message.edit_text("❌ Hisobot yaratish bekor qilindi.", reply_markup=admin_main_menu_kb())
    await callback.answer()


# ─────────────── Tez hisobot ───────────────

@router.callback_query(F.data == "admin:quick")
async def quick_report_menu(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return

    keyboard = []
    for group_id in GROUP_CHAT_IDS:
        name = GROUP_NAMES.get(group_id, f"Guruh {group_id}")
        keyboard.append([InlineKeyboardButton(text=f"📢 {name}", callback_data=f"quick_group:{group_id}")])
    keyboard.append([InlineKeyboardButton(text="🔙 Ortga", callback_data="admin:back_to_main")])

    await callback.message.edit_text(
        "📊 <b>TEZ HISOBOT</b>\n\nGuruhni tanlang:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("quick_group:"))
async def quick_group_selected(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return

    group_id = int(callback.data.split(":")[1])
    group_name = GROUP_NAMES.get(group_id, f"Guruh {group_id}")

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="2 soat",  callback_data=f"quick_final:{group_id}:2"),
                InlineKeyboardButton(text="4 soat",  callback_data=f"quick_final:{group_id}:4"),
                InlineKeyboardButton(text="8 soat",  callback_data=f"quick_final:{group_id}:8"),
            ],
            [
                InlineKeyboardButton(text="1 kun",   callback_data=f"quick_final:{group_id}:24"),
                InlineKeyboardButton(text="3 kun",   callback_data=f"quick_final:{group_id}:72"),
                InlineKeyboardButton(text="1 hafta", callback_data=f"quick_final:{group_id}:168"),
            ],
            [
                InlineKeyboardButton(text="1 oy",    callback_data=f"quick_final:{group_id}:720"),
            ],
            [
                InlineKeyboardButton(text="🔙 Guruh tanlash", callback_data="admin:quick"),
                InlineKeyboardButton(text="🏠 Menyu",          callback_data="admin:back_to_main"),
            ],
        ]
    )
    await callback.message.edit_text(
        f"✅ <b>Guruh:</b> {group_name}\n\n⏰ Vaqt oralig'ini tanlang:",
        parse_mode="HTML",
        reply_markup=keyboard,
    )
    await callback.answer()


@router.callback_query(F.data.startswith("quick_final:"))
async def quick_report_final(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return

    parts = callback.data.split(":")
    group_id = int(parts[1])
    hours    = int(parts[2])
    group_name = GROUP_NAMES.get(group_id, f"Guruh {group_id}")

    labels = {2: "2 soat", 4: "4 soat", 8: "8 soat",
              24: "1 kun", 72: "3 kun", 168: "1 hafta", 720: "1 oy"}
    period_label = labels.get(hours, f"{hours} soat")

    await callback.message.edit_text(
        f"📊 <b>Hisobot tayyorlanmoqda...</b>\n\n"
        f"🏢 {group_name} | ⏰ So'nggi {period_label}",
        parse_mode="HTML",
    )

    try:
        stats = await get_stats_for_hours(group_id, hours)
        stats["group_name"] = group_name
        stats["group_id"]   = group_id

        os.makedirs("reports", exist_ok=True)
        filename = f"reports/report_{group_name.replace(' ','_')}_{hours}h_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        await asyncio.to_thread(build_pdf_report, stats, period_label, filename)

        await callback.message.answer(
            f"🎯 <b>Hisobot tayyor!</b>\n\n"
            f"🏢 {group_name}\n"
            f"📊 So'nggi {period_label}\n"
            f"💬 Xabarlar: {stats['total_messages']}\n"
            f"👤 Faol userlar: {len(stats['users'])}",
            parse_mode="HTML",
        )
        await callback.message.answer_document(
            FSInputFile(filename),
            caption=f"📊 {group_name} — So'nggi {period_label}",
        )
        await callback.message.answer("👋 Admin panel", reply_markup=admin_main_menu_kb())
    except Exception as e:
        await callback.message.answer(f"❌ Xatolik: {e}")

    await callback.answer()


# ─────────────── Qo'lda vaqt tanlash ───────────────

def cancel_report_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Bekor qilish", callback_data="admin:cancel_report")],
            [InlineKeyboardButton(text="🔙 Ortga",        callback_data="admin:back_to_main")],
        ]
    )


def time_select_kb() -> InlineKeyboardMarkup:
    keyboard = []
    for i in range(0, 24, 4):
        row = [InlineKeyboardButton(text=f"{h:02d}", callback_data=f"time:hour:{h}")
               for h in range(i, min(i + 4, 24))]
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton(text="✏️ Soatni o'zingiz kiriting", callback_data="time:manual_hour")])
    keyboard.append([InlineKeyboardButton(text="❌ Bekor qilish", callback_data="admin:cancel_report")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def minute_select_kb(hour: int) -> InlineKeyboardMarkup:
    minute_row = [
        InlineKeyboardButton(text=f"{m:02d}", callback_data=f"time:minute:{hour}:{m}")
        for m in [0, 15, 30, 45]
    ]
    return InlineKeyboardMarkup(
        inline_keyboard=[
            minute_row,
            [InlineKeyboardButton(text="✏️ Daqiqani o'zingiz kiriting", callback_data="time:manual_minute")],
            [InlineKeyboardButton(text="🔙 Soatga qaytish", callback_data="time:back_to_hour")],
            [InlineKeyboardButton(text="❌ Bekor qilish",   callback_data="admin:cancel_report")],
        ]
    )


def create_calendar_kb(year: int, month: int) -> InlineKeyboardMarkup:
    months_uz = ["Yanvar","Fevral","Mart","Aprel","May","Iyun",
                 "Iyul","Avgust","Sentyabr","Oktyabr","Noyabr","Dekabr"]
    week_days = ["Du","Se","Ch","Pa","Ju","Sh","Ya"]
    first_weekday, days_in_month = cal_module.monthrange(year, month)
    prev_m = month - 1 if month > 1 else 12
    prev_y = year if month > 1 else year - 1
    next_m = month + 1 if month < 12 else 1
    next_y = year if month < 12 else year + 1

    keyboard = [
        [
            InlineKeyboardButton(text="◀️", callback_data=f"cal:prev:{prev_y}:{prev_m}"),
            InlineKeyboardButton(text=f"{months_uz[month-1]} {year}", callback_data="cal:ignore"),
            InlineKeyboardButton(text="▶️", callback_data=f"cal:next:{next_y}:{next_m}"),
        ],
        [InlineKeyboardButton(text=d, callback_data="cal:ignore") for d in week_days],
    ]

    row = [InlineKeyboardButton(text=" ", callback_data="cal:ignore") for _ in range(first_weekday)]
    for day in range(1, days_in_month + 1):
        row.append(InlineKeyboardButton(text=str(day), callback_data=f"cal:day:{year}:{month}:{day}"))
        if len(row) == 7:
            keyboard.append(row)
            row = []
    if row:
        while len(row) < 7:
            row.append(InlineKeyboardButton(text=" ", callback_data="cal:ignore"))
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton(text="❌ Bekor qilish", callback_data="admin:cancel_report")])
    keyboard.append([InlineKeyboardButton(text="🏠 Asosiy menyu", callback_data="admin:back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


@router.callback_query(F.data == "admin:custom")
async def custom_report_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return

    keyboard = []
    for group_id in GROUP_CHAT_IDS:
        name = GROUP_NAMES.get(group_id, f"Guruh {group_id}")
        keyboard.append([InlineKeyboardButton(text=f"📢 {name}", callback_data=f"custom_group:{group_id}")])
    keyboard.append([InlineKeyboardButton(text="🔙 Ortga", callback_data="admin:back_to_main")])

    await callback.message.edit_text(
        "📅 <b>QO'LDA VAQT BILAN HISOBOT</b>\n\nGuruhni tanlang:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("custom_group:"))
async def custom_group_selected(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return

    group_id = int(callback.data.split(":")[1])
    group_name = GROUP_NAMES.get(group_id, f"Guruh {group_id}")
    await state.update_data(selected_group_id=group_id)
    await state.set_state(AdminReportState.waiting_for_start_date)

    now = datetime.now()
    await callback.message.edit_text(
        f"✅ <b>Guruh:</b> {group_name}\n\n📅 BOSHLANG'ICH SANANI tanlang:",
        parse_mode="HTML",
        reply_markup=create_calendar_kb(now.year, now.month),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("cal:"))
async def calendar_handler(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return

    parts = callback.data.split(":")
    action = parts[1]
    current_state = await state.get_state()

    if action in ("prev", "next"):
        year, month = int(parts[2]), int(parts[3])
        await callback.message.edit_reply_markup(reply_markup=create_calendar_kb(year, month))

    elif action == "day":
        year, month, day = int(parts[2]), int(parts[3]), int(parts[4])
        selected_date = datetime(year, month, day)

        if current_state == AdminReportState.waiting_for_start_date:
            await state.update_data(start_date=selected_date)
            await state.set_state(AdminReportState.waiting_for_start_time)
            await callback.message.edit_text(
                f"✅ Boshlang'ich sana: <b>{selected_date.strftime('%Y-%m-%d')}</b>\n\n"
                "⏰ BOSHLANG'ICH SOATNI tanlang:",
                parse_mode="HTML",
                reply_markup=time_select_kb(),
            )

        elif current_state == AdminReportState.waiting_for_end_date:
            data = await state.get_data()
            start_date = data.get("start_date")
            if selected_date < start_date:
                await callback.answer("❌ Tugash sanasi boshlang'ich sanadan oldin bo'lmasin!", show_alert=True)
                return
            await state.update_data(end_date=selected_date)
            await state.set_state(AdminReportState.waiting_for_end_time)
            await callback.message.edit_text(
                f"✅ Boshlanish: <b>{start_date.strftime('%Y-%m-%d')}</b>\n"
                f"✅ Tugash:     <b>{selected_date.strftime('%Y-%m-%d')}</b>\n\n"
                "⏰ TUGASH SOATINI tanlang:",
                parse_mode="HTML",
                reply_markup=time_select_kb(),
            )

    await callback.answer()


@router.callback_query(F.data.startswith("time:hour:"))
async def select_hour_cb(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return
    hour = int(callback.data.split(":")[2])
    await state.update_data(selected_hour=hour)
    await callback.message.edit_text(
        f"✅ Tanlangan soat: <b>{hour:02d}</b>\n\n⏰ DAQIQANI tanlang:",
        parse_mode="HTML",
        reply_markup=minute_select_kb(hour),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("time:minute:"))
async def select_minute_cb(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return

    parts  = callback.data.split(":")
    hour   = int(parts[2])
    minute = int(parts[3])
    current_state = await state.get_state()
    data = await state.get_data()

    if current_state == AdminReportState.waiting_for_start_time:
        start_date = data.get("start_date")
        start_dt = datetime.combine(start_date, datetime.min.time().replace(hour=hour, minute=minute))
        await state.update_data(start_datetime=start_dt)
        await state.set_state(AdminReportState.waiting_for_end_date)
        await callback.message.edit_text(
            f"✅ Boshlanish vaqti: <b>{start_dt.strftime('%Y-%m-%d %H:%M')}</b>\n\n"
            "📅 TUGASH SANASINI tanlang:",
            parse_mode="HTML",
            reply_markup=create_calendar_kb(start_date.year, start_date.month),
        )

    elif current_state == AdminReportState.waiting_for_end_time:
        end_date   = data.get("end_date")
        start_dt   = data.get("start_datetime")
        group_id   = data.get("selected_group_id")
        end_dt     = datetime.combine(end_date, datetime.min.time().replace(hour=hour, minute=minute))

        if end_dt < start_dt:
            await callback.answer("❌ Tugash vaqti boshlang'ich vaqtdan oldin bo'lmasin!", show_alert=True)
            return

        group_name = GROUP_NAMES.get(group_id, f"Guruh {group_id}")
        await state.clear()

        await callback.message.edit_text(
            f"📊 <b>Hisobot tayyorlanmoqda...</b>\n\n"
            f"🏢 {group_name}\n"
            f"📅 {start_dt.strftime('%Y-%m-%d %H:%M')} → {end_dt.strftime('%Y-%m-%d %H:%M')}",
            parse_mode="HTML",
        )

        await _generate_and_send_report(callback.message, group_id, group_name, start_dt, end_dt)

    await callback.answer()


@router.callback_query(F.data == "time:back_to_hour")
async def back_to_hour_cb(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return
    data = await state.get_data()
    current_state = await state.get_state()
    if current_state == AdminReportState.waiting_for_start_time:
        await callback.message.edit_text(
            f"✅ Sana: <b>{data['start_date'].strftime('%Y-%m-%d')}</b>\n\n⏰ SOATNI tanlang:",
            parse_mode="HTML", reply_markup=time_select_kb()
        )
    elif current_state == AdminReportState.waiting_for_end_time:
        await callback.message.edit_text(
            f"✅ Tugash sanasi: <b>{data['end_date'].strftime('%Y-%m-%d')}</b>\n\n⏰ SOATNI tanlang:",
            parse_mode="HTML", reply_markup=time_select_kb()
        )
    await callback.answer()


@router.callback_query(F.data == "time:manual_hour")
async def manual_hour_cb(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "⏰ Soatni kiriting (0–23):\nMasalan: 14\n\n❌ Bekor qilish uchun /cancel",
        reply_markup=cancel_report_kb(),
    )
    await callback.answer()


@router.callback_query(F.data == "time:manual_minute")
async def manual_minute_cb(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "⏰ Daqiqani kiriting (0–59):\nMasalan: 30\n\n❌ Bekor qilish uchun /cancel",
        reply_markup=cancel_report_kb(),
    )
    await callback.answer()


@router.message(AdminReportState.waiting_for_start_time)
async def manual_start_time_msg(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        hour, minute = _parse_time_text(message.text)
        data = await state.get_data()
        start_date = data.get("start_date")
        start_dt = datetime.combine(start_date, datetime.min.time().replace(hour=hour, minute=minute))
        await state.update_data(start_datetime=start_dt)
        await state.set_state(AdminReportState.waiting_for_end_date)
        await message.answer(
            f"✅ Boshlanish: <b>{start_dt.strftime('%Y-%m-%d %H:%M')}</b>\n\n📅 TUGASH SANASINI tanlang:",
            parse_mode="HTML",
            reply_markup=create_calendar_kb(start_date.year, start_date.month),
        )
    except ValueError:
        await message.answer("❌ Noto'g'ri format. Masalan: 14 yoki 14:30", reply_markup=cancel_report_kb())


@router.message(AdminReportState.waiting_for_end_time)
async def manual_end_time_msg(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        hour, minute = _parse_time_text(message.text)
        data = await state.get_data()
        end_date = data.get("end_date")
        start_dt = data.get("start_datetime")
        group_id = data.get("selected_group_id")
        end_dt = datetime.combine(end_date, datetime.min.time().replace(hour=hour, minute=minute))

        if end_dt < start_dt:
            await message.answer("❌ Tugash vaqti boshlang'ich vaqtdan oldin bo'lmasin!", reply_markup=cancel_report_kb())
            return

        group_name = GROUP_NAMES.get(group_id, f"Guruh {group_id}")
        await state.clear()
        await message.answer(
            f"📊 <b>Hisobot tayyorlanmoqda...</b>\n\n🏢 {group_name}",
            parse_mode="HTML",
        )
        await _generate_and_send_report(message, group_id, group_name, start_dt, end_dt)
    except ValueError:
        await message.answer("❌ Noto'g'ri format. Masalan: 14 yoki 14:30", reply_markup=cancel_report_kb())


def _parse_time_text(text: str) -> tuple[int, int]:
    text = (text or "").strip()
    if ":" in text:
        h, m = map(int, text.split(":"))
    else:
        h, m = int(text), 0
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise ValueError
    return h, m


async def _generate_and_send_report(target, group_id: int, group_name: str,
                                    start_dt: datetime, end_dt: datetime):
    """PDF yaratib yuboradi. target = Message."""
    period_label = f"{start_dt.strftime('%Y-%m-%d %H:%M')} – {end_dt.strftime('%Y-%m-%d %H:%M')}"
    stats = await get_stats_for_range(group_id, start_dt, end_dt)
    stats["group_name"] = group_name
    stats["group_id"]   = group_id

    os.makedirs("reports", exist_ok=True)
    filename = (
        f"reports/report_{group_name.replace(' ','_')}_"
        f"{start_dt.strftime('%Y%m%d_%H%M')}_{end_dt.strftime('%Y%m%d_%H%M')}.pdf"
    )
    await asyncio.to_thread(build_pdf_report, stats, period_label, filename)

    await target.answer(
        f"🎯 <b>Hisobot tayyor!</b>\n\n"
        f"🏢 {group_name}\n"
        f"📊 {period_label}\n"
        f"💬 Xabarlar: {stats['total_messages']}\n"
        f"👤 Faol userlar: {len(stats['users'])}",
        parse_mode="HTML",
    )
    await target.answer_document(
        FSInputFile(filename),
        caption=f"📊 {group_name} — {period_label}",
    )
    await target.answer("👋 Admin panel", reply_markup=admin_main_menu_kb())


# ─────────────────────────────────────────────────────────────────────────────
# GURUH XABARLARINI KUZATISH
# ─────────────────────────────────────────────────────────────────────────────

@router.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def group_message_tracker(message: Message):
    if message.chat.id not in GROUP_CHAT_IDS:
        return
    if not message.from_user or message.from_user.is_bot:
        return

    text = message.text or message.caption or ""

    # Cache'dan haqiqiy ismni olamiz
    from sheets import USER_DATA_CACHE
    cached_name = USER_DATA_CACHE.get(message.from_user.id, {}).get("full_name", "").strip()
    display_name = cached_name if cached_name else "Ism kiritilmagan"

    await append_group_message(
        chat_id=message.chat.id,
        message_id=message.message_id,
        user_id=message.from_user.id,
        full_name=display_name,
        username=message.from_user.username,
        text=text,
        sent_at=message.date.astimezone(timezone.utc),
    )


# ─────────────────────────────────────────────────────────────────────────────
# PRIVATE XABARLAR
# ─────────────────────────────────────────────────────────────────────────────

@router.message(F.chat.type == ChatType.PRIVATE)
async def private_message_router(message: Message, state: FSMContext):
    user = message.from_user

    if not user:
        return

    # State yoki command bo‘lsa skip
    current_state = await state.get_state()

    if current_state is not None:
        return

    if message.text and message.text.startswith("/"):
        return

    subscribed, status = await check_subscription(user.id)

    if status in ("inaccessible", "error"):
        await message.answer(
            "❌ Tekshiruvda xatolik. Keyinroq urinib ko‘ring."
        )
        return

    if not subscribed:
        await message.answer(
            "Avval kanalga a'zo bo‘ling.",
            reply_markup=join_channel_kb()
        )
        return

    # Ism kiritmagan bo‘lsa
    if not await has_user_fullname(user.id):
        await state.set_state(RegisterState.waiting_for_fullname)

        await message.answer(
            "📝 Iltimos, ism va familiyangizni kiriting:\n"
            "<i>Masalan: Murodjonov Asilbek</i>\n\n"
            "❌ Bekor qilish: /cancel",
            parse_mode="HTML",
        )
        return

    # USER XABARINI ADMINGA FORWARD
    for admin_id in ADMIN_IDS:
        try:
            forwarded = await message.forward(chat_id=admin_id)

            # mapping saqlash
            SUPPORT_REPLY_MAP[forwarded.message_id] = user.id

        except Exception as e:
            logging.exception(f"Forward error: {e}")

    await message.answer("✅ Xabaringiz adminga yuborildi.")
# ─────────────────────────────────────────────────────────────────────────────
# /id
# ─────────────────────────────────────────────────────────────────────────────

@router.message(Command("id"))
async def get_id(message: Message):
    await message.answer(f"Chat ID: <code>{message.chat.id}</code>", parse_mode="HTML")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def set_commands():

    commands = [
        BotCommand(command="start",     description="Botni ishga tushirish"),
        BotCommand(command="admin",     description="Admin panel"),
        BotCommand(command="broadcast", description="Elon yuborish"),
        BotCommand(command="editname",  description="Ism familiyani o'zgartirish"),
        BotCommand(command="id",        description="Chat ID ni ko'rish"),
        BotCommand(command="cancel",    description="Bekor qilish"),
    ]

    # PRIVATE uchun commandlar
    await bot.set_my_commands(
        commands,
        scope=BotCommandScopeAllPrivateChats()
    )

    # GROUP uchun commandlarni tozalash
    await bot.set_my_commands(
        [],
        scope=BotCommandScopeAllGroupChats()
    )

async def main():
    global BOT_ID
    await set_commands()
    await init_sheets()

    bot_info = await bot.get_me()
    BOT_ID = bot_info.id

    await start_background_flush()
    logging.info(f"Bot ishga tushdi. ID: {BOT_ID}")
    logging.info(f"Kuzatiladigan guruhlar: {len(GROUP_CHAT_IDS)} ta")

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await stop_background_flush()


if __name__ == "__main__":
    asyncio.run(main())

from __future__ import annotations

import asyncio
import logging
import os
import calendar as cal_module
from datetime import datetime, timedelta, timezone
from typing import Optional

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
)

from config import (
    BOT_TOKEN,
    GROUP_CHAT_ID,
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
    get_user_fullname,
    start_background_flush,
    stop_background_flush,
)
from pdf_report import build_pdf_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

bot = Bot(BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)


class RegisterState(StatesGroup):
    waiting_for_fullname = State()


class AdminReportState(StatesGroup):
    waiting_for_start_date = State()
    waiting_for_start_time = State()
    waiting_for_end_date = State()
    waiting_for_end_time = State()


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def admin_main_menu_kb() -> InlineKeyboardMarkup:
    """Asosiy admin menyusi"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Tez hisobot", callback_data="admin:quick")],
            [InlineKeyboardButton(text="📅 Qo'lda vaqt tanlash", callback_data="admin:custom")],
            [InlineKeyboardButton(text="📈 Statistika", callback_data="admin:stats")],
        ]
    )


def quick_report_kb() -> InlineKeyboardMarkup:
    """Tez hisobot menyusi"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="2 soat", callback_data="quick:2"),
                InlineKeyboardButton(text="4 soat", callback_data="quick:4"),
                InlineKeyboardButton(text="8 soat", callback_data="quick:8"),
            ],
            [
                InlineKeyboardButton(text="1 kun", callback_data="quick:24"),
                InlineKeyboardButton(text="3 kun", callback_data="quick:72"),
                InlineKeyboardButton(text="1 hafta", callback_data="quick:168"),
            ],
            [
                InlineKeyboardButton(text="1 oy", callback_data="quick:720"),
            ],
            [
                InlineKeyboardButton(text="🔙 Ortga", callback_data="admin:back_to_main"),
            ],
        ]
    )


def time_select_kb() -> InlineKeyboardMarkup:
    """Soat tanlash menyusi (00-23)"""
    keyboard = []
    
    # Soatlar 0-23 qatorlarga bo'lib (4 tadan)
    for i in range(0, 24, 4):
        row = []
        for hour in range(i, i+4):
            if hour < 24:
                row.append(InlineKeyboardButton(text=f"{hour:02d}", callback_data=f"time:hour:{hour}"))
        keyboard.append(row)
    
    # Qo'lda kiritish va navigatsiya tugmalari
    keyboard.append([InlineKeyboardButton(text="✏️ Soatni o'zingiz kiriting", callback_data="time:manual_hour")])
    keyboard.append([InlineKeyboardButton(text="🔙 Ortga", callback_data="time:back_to_date")])
    keyboard.append([InlineKeyboardButton(text="❌ Bekor qilish", callback_data="admin:cancel_report")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def minute_select_kb(selected_hour: int) -> InlineKeyboardMarkup:
    """Daqiqa tanlash menyusi"""
    keyboard = []
    
    # Daqiqalar qatori
    minute_row = []
    for minute in [0, 15, 30, 45]:
        minute_row.append(InlineKeyboardButton(text=f"{minute:02d}", callback_data=f"time:minute:{selected_hour}:{minute}"))
    keyboard.append(minute_row)
    
    # Qo'lda kiritish va navigatsiya tugmalari
    keyboard.append([InlineKeyboardButton(text="✏️ Daqiqani o'zingiz kiriting", callback_data="time:manual_minute")])
    keyboard.append([InlineKeyboardButton(text="🔙 Ortga (soatga)", callback_data="time:back_to_hour")])
    keyboard.append([InlineKeyboardButton(text="🏠 Asosiy menyu", callback_data="admin:back_to_main")])
    keyboard.append([InlineKeyboardButton(text="❌ Bekor qilish", callback_data="admin:cancel_report")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def cancel_report_kb() -> InlineKeyboardMarkup:
    """Bekor qilish menyusi"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Bekor qilish", callback_data="admin:cancel_report")],
            [InlineKeyboardButton(text="🔙 Ortga", callback_data="admin:back_to_main")],
        ]
    )


def join_channel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Kanalga o'tish", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="Qayta tekshirish", callback_data="check_sub")],
        ]
    )


def create_calendar_kb(year: int, month: int, selected_day: int = None) -> InlineKeyboardMarkup:
    """Kalendar menyusi"""
    
    months_uz = ["Yanvar", "Fevral", "Mart", "Aprel", "May", "Iyun", 
                 "Iyul", "Avgust", "Sentyabr", "Oktyabr", "Noyabr", "Dekabr"]
    
    week_days = ["Du", "Se", "Ch", "Pa", "Ju", "Sh", "Ya"]
    
    first_weekday, days_in_month = cal_module.monthrange(year, month)
    
    keyboard = []
    
    prev_month = month - 1 if month > 1 else 12
    prev_year = year if month > 1 else year - 1
    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1
    
    keyboard.append([
        InlineKeyboardButton(text="◀️", callback_data=f"cal:prev:{prev_year}:{prev_month}"),
        InlineKeyboardButton(text=f"{months_uz[month-1]} {year}", callback_data="cal:ignore"),
        InlineKeyboardButton(text="▶️", callback_data=f"cal:next:{next_year}:{next_month}")
    ])
    
    week_row = []
    for day in week_days:
        week_row.append(InlineKeyboardButton(text=day, callback_data="cal:ignore"))
    keyboard.append(week_row)
    
    row = []
    for _ in range(first_weekday):
        row.append(InlineKeyboardButton(text=" ", callback_data="cal:ignore"))
    
    for day in range(1, days_in_month + 1):
        if selected_day == day:
            row.append(InlineKeyboardButton(text=f"✅{day}", callback_data=f"cal:day:{year}:{month}:{day}"))
        else:
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


async def check_subscription(user_id: int) -> tuple[bool, str]:
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        is_subscribed = member.status in {
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.CREATOR,
        }
        return is_subscribed, "ok"

    except TelegramBadRequest as e:
        err = str(e).lower()

        if "member list is inaccessible" in err:
            logging.error(
                "Kanal a'zoligini tekshirib bo'lmadi: bot kanalga qo'shilmagan yoki admin emas"
            )
            return False, "inaccessible"

        logging.exception("Obunani tekshirishda Telegram xatosi: %s", e)
        return False, "error"

    except Exception as e:
        logging.exception("Obunani tekshirishda xato: %s", e)
        return False, "error"


@router.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    user = message.from_user
    if not user:
        return

    subscribed, status = await check_subscription(user.id)

    await upsert_user(
        user_id=user.id,
        full_name=user.full_name,
        username=user.username,
        is_subscribed=1 if subscribed else 0,
    )

    if status == "inaccessible":
        await message.answer(
            "Hozircha kanal obunasini avtomatik tekshirib bo'lmadi.\n"
            "Admin botni kanalga to'liq ulab chiqishi kerak."
        )
        return

    if status == "error":
        await message.answer(
            "Tekshiruvda xatolik bo'ldi. Keyinroq qayta urinib ko'ring."
        )
        return

    if not subscribed:
        await message.answer(
            "Botdan foydalanish uchun avval kanalga a'zo bo'ling.",
            reply_markup=join_channel_kb(),
        )
        return

    await state.set_state(RegisterState.waiting_for_fullname)
    await message.answer(
        "✅ Obuna tasdiqlandi!\n\n"
        "Iltimos, to'liq ismingiz va familiyangizni kiriting:\n"
        "Masalan: Alisher Navoiy\n\n"
        "Bu ma'lumot hisobotlarda ko'rsatiladi."
    )


@router.message(RegisterState.waiting_for_fullname)
async def register_fullname(message: Message, state: FSMContext):
    user = message.from_user
    if not user:
        return

    full_name = message.text.strip()
    
    if len(full_name) < 3:
        await message.answer("Ism va familiya kamida 3 harfdan iborat bo'lishi kerak. Qaytadan kiriting:")
        return

    await update_user_fullname(user.id, full_name)
    
    await state.clear()
    
    if is_admin(user.id):
        await message.answer(
            f"✅ Assalomu alaykum, {full_name}!\n\n"
            "Admin panelga xush kelibsiz!",
            reply_markup=admin_main_menu_kb(),
        )
    else:
        await message.answer(
            f"✅ Assalomu alaykum, {full_name}!\n\n"
            "Xush kelibsiz.\n"
            "Siz yuborgan xabarlar adminlarga yetkaziladi."
        )


@router.callback_query(F.data == "check_sub")
async def check_subscription_callback(callback: CallbackQuery, state: FSMContext):
    user = callback.from_user
    if not user:
        await callback.answer("Foydalanuvchi topilmadi", show_alert=True)
        return

    subscribed, status = await check_subscription(user.id)

    await upsert_user(
        user_id=user.id,
        full_name=user.full_name,
        username=user.username,
        is_subscribed=1 if subscribed else 0,
    )

    if status == "inaccessible":
        await callback.answer(
            "Bot kanal a'zoligini tekshira olmayapti. Admin botni kanalga admin qilishi kerak.",
            show_alert=True,
        )
        return

    if status == "error":
        await callback.answer(
            "Tekshiruvda xatolik bo'ldi. Keyinroq qayta urinib ko'ring.",
            show_alert=True,
        )
        return

    if not subscribed:
        await callback.answer("Siz hali kanalga a'zo bo'lmagansiz", show_alert=True)
        return

    await callback.answer("Obuna tasdiqlandi ✅")

    await state.set_state(RegisterState.waiting_for_fullname)
    await callback.message.edit_text(
        "✅ Obuna tasdiqlandi!\n\n"
        "Iltimos, to'liq ismingiz va familiyangizni kiriting:\n"
        "Masalan: Alisher Navoiy\n\n"
        "Bu ma'lumot hisobotlarda ko'rsatiladi."
    )


# ============ ADMIN PANEL ============

@router.message(Command("admin"))
async def admin_command(message: Message):
    user = message.from_user
    if not user or not is_admin(user.id):
        return

    await message.answer(
        "👋 Admin panelga xush kelibsiz!",
        reply_markup=admin_main_menu_kb(),
    )


@router.callback_query(F.data == "admin:back_to_main")
async def back_to_main(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return
    
    await callback.message.edit_text(
        "👋 Admin panelga xush kelibsiz!",
        reply_markup=admin_main_menu_kb(),
    )
    await callback.answer()


@router.callback_query(F.data == "admin:cancel_report")
async def cancel_report(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return
    
    await state.clear()
    await callback.message.edit_text(
        "❌ Hisobot yaratish bekor qilindi.",
        reply_markup=admin_main_menu_kb(),
    )
    await callback.answer()


@router.callback_query(F.data == "admin:quick")
async def quick_report_menu(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📊 Vaqt oralig'ini tanlang:",
        reply_markup=quick_report_kb(),
    )
    await callback.answer()


@router.callback_query(F.data == "admin:stats")
async def stats_menu(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return
    
    await callback.answer("Statistika tayyorlanmoqda...")
    
    await callback.message.edit_text(
        "📈 Statistika funksiyasi ishlab chiqilmoqda.\n"
        "Hozircha hisobot olish uchun 'Tez hisobot' yoki 'Qo'lda vaqt tanlash' bo'limlaridan foydalaning.",
        reply_markup=admin_main_menu_kb(),
    )


@router.callback_query(F.data == "admin:custom")
async def custom_report_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return
    
    await state.set_state(AdminReportState.waiting_for_start_date)
    
    now = datetime.now()
    await callback.message.edit_text(
        "📅 BOSHLANG'ICH SANANI tanlang:",
        reply_markup=create_calendar_kb(now.year, now.month)
    )
    await callback.answer()


# Time handlers
@router.callback_query(F.data == "time:manual_hour")
async def manual_hour_input(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⏰ Soatni kiriting (0-23):\n"
        "Masalan: 14\n\n"
        "❌ Bekor qilish tugmasini bosing.",
        reply_markup=cancel_report_kb(),
    )
    await callback.answer()


@router.callback_query(F.data == "time:manual_minute")
async def manual_minute_input(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⏰ Daqiqani kiriting (0-59):\n"
        "Masalan: 30\n\n"
        "❌ Bekor qilish tugmasini bosing.",
        reply_markup=cancel_report_kb(),
    )
    await callback.answer()


@router.callback_query(F.data == "time:back_to_date")
async def back_to_date(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return
    
    current_state = await state.get_state()
    
    if current_state == AdminReportState.waiting_for_start_time:
        await state.set_state(AdminReportState.waiting_for_start_date)
        now = datetime.now()
        await callback.message.edit_text(
            "📅 BOSHLANG'ICH SANANI qayta tanlang:",
            reply_markup=create_calendar_kb(now.year, now.month)
        )
    elif current_state == AdminReportState.waiting_for_end_time:
        data = await state.get_data()
        start_date = data.get("start_date")
        await state.set_state(AdminReportState.waiting_for_end_date)
        await callback.message.edit_text(
            f"✅ Boshlang'ich sana: {start_date.strftime('%Y-%m-%d')}\n\n"
            "📅 TUGASH SANASINI qayta tanlang:",
            reply_markup=create_calendar_kb(start_date.year, start_date.month)
        )
    
    await callback.answer()


@router.callback_query(F.data == "time:back_to_hour")
async def back_to_hour(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return
    
    current_state = await state.get_state()
    
    if current_state == AdminReportState.waiting_for_start_time:
        data = await state.get_data()
        selected_date = data.get("start_date")
        await callback.message.edit_text(
            f"✅ Boshlang'ich sana: {selected_date.strftime('%Y-%m-%d')}\n\n"
            "⏰ BOSHLANG'ICH SOATNI tanlang:",
            reply_markup=time_select_kb()
        )
    elif current_state == AdminReportState.waiting_for_end_time:
        data = await state.get_data()
        start_date = data.get("start_date")
        end_date = data.get("end_date")
        await callback.message.edit_text(
            f"✅ Boshlang'ich sana: {start_date.strftime('%Y-%m-%d')}\n"
            f"✅ Tugash sanasi: {end_date.strftime('%Y-%m-%d')}\n\n"
            "⏰ TUGASH SOATINI tanlang:",
            reply_markup=time_select_kb()
        )
    
    await callback.answer()


@router.callback_query(F.data.startswith("time:hour:"))
async def select_hour(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return
    
    hour = int(callback.data.split(":")[2])
    await state.update_data(selected_hour=hour)
    
    await callback.message.edit_text(
        f"✅ Tanlangan soat: {hour:02d}\n\n"
        "⏰ DAQIQA NI tanlang:",
        reply_markup=minute_select_kb(hour)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("time:minute:"))
async def select_minute(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return
    
    parts = callback.data.split(":")
    hour = int(parts[2])
    minute = int(parts[3])
    
    selected_time = datetime.now().replace(hour=hour, minute=minute, second=0, microsecond=0)
    await state.update_data(selected_time=selected_time)
    
    current_state = await state.get_state()
    
    if current_state == AdminReportState.waiting_for_start_time:
        data = await state.get_data()
        start_date = data.get("start_date")
        start_datetime = datetime.combine(start_date, selected_time.time())
        
        await state.update_data(start_datetime=start_datetime)
        await state.set_state(AdminReportState.waiting_for_end_date)
        
        await callback.message.edit_text(
            f"✅ Boshlang'ich vaqt: {start_datetime.strftime('%Y-%m-%d %H:%M')}\n\n"
            "📅 TUGASH SANASINI tanlang:",
            reply_markup=create_calendar_kb(start_date.year, start_date.month)
        )
        
    elif current_state == AdminReportState.waiting_for_end_time:
        data = await state.get_data()
        end_date = data.get("end_date")
        start_datetime = data.get("start_datetime")
        end_datetime = datetime.combine(end_date, selected_time.time())
        
        if end_datetime < start_datetime:
            await callback.answer("❌ Tugash vaqti boshlang'ich vaqtdan oldin bo'lishi mumkin emas!", show_alert=True)
            return
        
        await state.clear()
        
        await callback.message.edit_text(
            f"📊 Hisobot tayyorlanmoqda...\n\n"
            f"📅 Boshlanish: {start_datetime.strftime('%Y-%m-%d %H:%M')}\n"
            f"📅 Tugash: {end_datetime.strftime('%Y-%m-%d %H:%M')}"
        )
        
        stats = await get_stats_for_range(GROUP_CHAT_ID, start_datetime, end_datetime)
        
        period_label = f"{start_datetime.strftime('%Y-%m-%d %H:%M')} dan {end_datetime.strftime('%Y-%m-%d %H:%M')} gacha"
        
        os.makedirs("reports", exist_ok=True)
        filename = f"reports/report_{start_datetime.strftime('%Y%m%d_%H%M')}_{end_datetime.strftime('%Y%m%d_%H%M')}_{datetime.now().strftime('%H%M%S')}.pdf"
        
        await asyncio.to_thread(build_pdf_report, stats, period_label, filename)
        
        short_text = (
            f"{period_label} bo'yicha natija tayyor.\n"
            f"Jami xabarlar: {stats['total_messages']}\n"
            f"Faol foydalanuvchilar: {len(stats['users'])}"
        )
        
        await callback.message.answer(short_text)
        await callback.message.answer_document(
            FSInputFile(filename),
            caption=f"📊 {period_label} uchun PDF hisobot",
        )
        
        # HISOBOTDAN KEYIN ASOSIY MENYU
        await callback.message.answer(
            "👋 Admin panelga xush kelibsiz!",
            reply_markup=admin_main_menu_kb(),
        )
    
    await callback.answer()


# Calendar callback handler
@router.callback_query(F.data.startswith("cal:"))
async def calendar_handler(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return
    
    parts = callback.data.split(":")
    action = parts[1]
    
    current_state = await state.get_state()
    
    if action == "prev":
        year = int(parts[2])
        month = int(parts[3])
        
        await callback.message.edit_reply_markup(
            reply_markup=create_calendar_kb(year, month)
        )
        await callback.answer()
        
    elif action == "next":
        year = int(parts[2])
        month = int(parts[3])
        
        await callback.message.edit_reply_markup(
            reply_markup=create_calendar_kb(year, month)
        )
        await callback.answer()
        
    elif action == "day":
        year = int(parts[2])
        month = int(parts[3])
        day = int(parts[4])
        selected_date = datetime(year, month, day)
        
        if current_state == AdminReportState.waiting_for_start_date:
            await state.update_data(start_date=selected_date)
            await state.set_state(AdminReportState.waiting_for_start_time)
            
            await callback.message.edit_text(
                f"✅ Boshlang'ich sana: {selected_date.strftime('%Y-%m-%d')}\n\n"
                "⏰ BOSHLANG'ICH SOATNI tanlang:",
                reply_markup=time_select_kb()
            )
            
        elif current_state == AdminReportState.waiting_for_end_date:
            await state.update_data(end_date=selected_date)
            await state.set_state(AdminReportState.waiting_for_end_time)
            
            data = await state.get_data()
            start_date = data.get("start_date")
            
            if selected_date < start_date:
                await callback.answer("❌ Tugash sanasi boshlang'ich sanadan oldin bo'lishi mumkin emas!", show_alert=True)
                return
            
            await callback.message.edit_text(
                f"✅ Boshlang'ich sana: {start_date.strftime('%Y-%m-%d')}\n"
                f"✅ Tugash sanasi: {selected_date.strftime('%Y-%m-%d')}\n\n"
                "⏰ TUGASH SOATINI tanlang:",
                reply_markup=time_select_kb()
            )
            
        await callback.answer()
    
    elif action == "ignore":
        await callback.answer()


# Manual time input handlers (text)
@router.message(AdminReportState.waiting_for_start_time)
async def manual_time_input(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    
    try:
        text = message.text.strip()
        
        if ":" in text:
            hour, minute = map(int, text.split(":"))
        else:
            hour = int(text)
            minute = 0
        
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError
        
        selected_time = datetime.now().replace(hour=hour, minute=minute, second=0, microsecond=0)
        
        data = await state.get_data()
        selected_date = data.get("start_date")
        start_datetime = datetime.combine(selected_date, selected_time.time())
        
        await state.update_data(start_datetime=start_datetime)
        await state.set_state(AdminReportState.waiting_for_end_date)
        
        await message.answer(
            f"✅ Boshlang'ich vaqt: {start_datetime.strftime('%Y-%m-%d %H:%M')}\n\n"
            "📅 TUGASH SANASINI tanlang:",
            reply_markup=create_calendar_kb(selected_date.year, selected_date.month)
        )
        
    except ValueError:
        await message.answer(
            "❌ Noto'g'ri format! Iltimos, soatni 0-23 oralig'ida kiriting.\n"
            "Masalan: 14 yoki 14:30\n\n"
            "Qaytadan kiriting yoki bekor qiling:",
            reply_markup=cancel_report_kb(),
        )


@router.message(AdminReportState.waiting_for_end_time)
async def manual_end_time_input(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    
    try:
        text = message.text.strip()
        
        if ":" in text:
            hour, minute = map(int, text.split(":"))
        else:
            hour = int(text)
            minute = 0
        
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError
        
        selected_time = datetime.now().replace(hour=hour, minute=minute, second=0, microsecond=0)
        
        data = await state.get_data()
        end_date = data.get("end_date")
        start_datetime = data.get("start_datetime")
        end_datetime = datetime.combine(end_date, selected_time.time())
        
        if end_datetime < start_datetime:
            await message.answer(
                "❌ Tugash vaqti boshlang'ich vaqtdan oldin bo'lishi mumkin emas!\n"
                "Qaytadan kiriting:",
                reply_markup=cancel_report_kb(),
            )
            return
        
        await state.clear()
        
        await message.answer(
            f"📊 Hisobot tayyorlanmoqda...\n\n"
            f"📅 Boshlanish: {start_datetime.strftime('%Y-%m-%d %H:%M')}\n"
            f"📅 Tugash: {end_datetime.strftime('%Y-%m-%d %H:%M')}"
        )
        
        stats = await get_stats_for_range(GROUP_CHAT_ID, start_datetime, end_datetime)
        
        period_label = f"{start_datetime.strftime('%Y-%m-%d %H:%M')} dan {end_datetime.strftime('%Y-%m-%d %H:%M')} gacha"
        
        os.makedirs("reports", exist_ok=True)
        filename = f"reports/report_{start_datetime.strftime('%Y%m%d_%H%M')}_{end_datetime.strftime('%Y%m%d_%H%M')}_{datetime.now().strftime('%H%M%S')}.pdf"
        
        await asyncio.to_thread(build_pdf_report, stats, period_label, filename)
        
        short_text = (
            f"{period_label} bo'yicha natija tayyor.\n"
            f"Jami xabarlar: {stats['total_messages']}\n"
            f"Faol foydalanuvchilar: {len(stats['users'])}"
        )
        
        await message.answer(short_text)
        await message.answer_document(
            FSInputFile(filename),
            caption=f"📊 {period_label} uchun PDF hisobot",
        )
        
        # HISOBOTDAN KEYIN ASOSIY MENYU
        await message.answer(
            "👋 Admin panelga xush kelibsiz!",
            reply_markup=admin_main_menu_kb(),
        )
        
    except ValueError:
        await message.answer(
            "❌ Noto'g'ri format! Iltimos, soatni 0-23 oralig'ida kiriting.\n"
            "Masalan: 14 yoki 14:30\n\n"
            "Qaytadan kiriting yoki bekor qiling:",
            reply_markup=cancel_report_kb(),
        )


# Tez hisobotlar uchun handler
@router.callback_query(F.data.startswith("quick:"))
async def quick_report_handler(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return
    
    try:
        hours = int(callback.data.split(":")[1])
    except Exception:
        await callback.answer("Noto'g'ri so'rov", show_alert=True)
        return
    
    await callback.answer("PDF tayyorlanmoqda...")
    
    stats = await get_stats_for_hours(GROUP_CHAT_ID, hours)
    
    labels = {
        2: "2 soat",
        4: "4 soat",
        8: "8 soat",
        24: "1 kun",
        72: "3 kun",
        168: "1 hafta",
        720: "1 oy",
    }
    period_label = labels.get(hours, f"{hours} soat")
    
    os.makedirs("reports", exist_ok=True)
    filename = f"reports/report_{hours}h_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    
    await asyncio.to_thread(build_pdf_report, stats, period_label, filename)
    
    short_text = (
        f"So'nggi {period_label} bo'yicha natija tayyor.\n"
        f"Jami xabarlar: {stats['total_messages']}\n"
        f"Faol foydalanuvchilar: {len(stats['users'])}"
    )
    
    await callback.message.answer(short_text)
    await callback.message.answer_document(
        FSInputFile(filename),
        caption=f"So'nggi {period_label} bo'yicha PDF hisobot",
    )
    
    # HISOBOTDAN KEYIN ASOSIY MENYU
    await callback.message.answer(
        "👋 Admin panelga xush kelibsiz!",
        reply_markup=admin_main_menu_kb(),
    )


@router.message(Command("id"))
async def get_id(message: Message):
    await message.answer(f"Chat ID: {message.chat.id}")


@router.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def group_message_tracker(message: Message):
    if message.chat.id != GROUP_CHAT_ID:
        return

    if not message.from_user:
        return

    if message.from_user.is_bot:
        return

    text = message.text or message.caption or ""

    full_name = await get_user_fullname(message.from_user.id)
    if not full_name:
        full_name = message.from_user.full_name

    await append_group_message(
        chat_id=message.chat.id,
        message_id=message.message_id,
        user_id=message.from_user.id,
        full_name=full_name,
        username=message.from_user.username,
        text=text,
        sent_at=message.date.astimezone(timezone.utc),
    )


@router.message(F.chat.type == ChatType.PRIVATE)
async def private_message_router(message: Message, state: FSMContext):
    user = message.from_user
    if not user:
        return

    current_state = await state.get_state()
    if current_state is not None:
        return

    if message.text and message.text.startswith("/"):
        return

    subscribed, status = await check_subscription(user.id)

    await upsert_user(
        user_id=user.id,
        full_name=user.full_name,
        username=user.username,
        is_subscribed=1 if subscribed else 0,
    )

    if status == "inaccessible":
        await message.answer(
            "Hozircha kanal obunasini avtomatik tekshirib bo'lmadi.\n"
            "Keyinroq urinib ko'ring."
        )
        return

    if status == "error":
        await message.answer(
            "Tekshiruvda xatolik bo'ldi. Keyinroq qayta urinib ko'ring."
        )
        return

    if not subscribed:
        await message.answer(
            "Avval kanalga a'zo bo'ling.",
            reply_markup=join_channel_kb(),
        )
        return

    for admin_id in ADMIN_IDS:
        try:
            await message.forward(chat_id=admin_id)
        except Exception as e:
            logging.exception("Adminga forward qilishda xato: %s", e)

    await message.answer("✅ Xabaringiz adminga yuborildi.")


async def main():
    await init_sheets()
    await start_background_flush()
    logging.info("Google Sheets ga ulanildi")
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await stop_background_flush()


if __name__ == "__main__":
    asyncio.run(main())

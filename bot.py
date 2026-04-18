from __future__ import annotations

import asyncio
import logging
import os
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

# Calendar imports
from aiogram_calendar import SimpleCalendar, SimpleCalendarCallback, get_user_locale
from aiogram_calendar.schemas import DialogCalendar, DialogCalendarCallback

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
    waiting_for_end_date = State()


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def admin_main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Tez hisobot", callback_data="admin:quick")],
            [InlineKeyboardButton(text="📅 Qo'lda vaqt tanlash", callback_data="admin:custom")],
            [InlineKeyboardButton(text="📈 Statistika", callback_data="admin:stats")],
        ]
    )


def quick_report_kb() -> InlineKeyboardMarkup:
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
                InlineKeyboardButton(text="🔙 Ortga", callback_data="admin:back_to_main"),
            ],
        ]
    )


def cancel_report_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Bekor qilish", callback_data="admin:cancel_report")]
        ]
    )


def join_channel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Kanalga o'tish", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="Qayta tekshirish", callback_data="check_sub")],
        ]
    )


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
    
    # Kalendarni ko'rsatamiz
    calendar = SimpleCalendar(
        locale='uz',
        show_alerts=True,
        cancel_button=True
    )
    await callback.message.edit_text(
        "📅 BOSHLANG'ICH SANANI tanlang:",
        reply_markup=await calendar.start_calendar()
    )
    await callback.answer()


@router.callback_query(SimpleCalendarCallback.filter(), AdminReportState.waiting_for_start_date)
async def process_start_date_calendar(
    callback: CallbackQuery,
    callback_data: SimpleCalendarCallback,
    state: FSMContext
):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return
    
    calendar = SimpleCalendar(
        locale='uz',
        show_alerts=True,
        cancel_button=True
    )
    
    selected, date = await calendar.process_selection(callback, callback_data)
    
    if selected:
        # Sanani saqlaymiz
        await state.update_data(start_date=date)
        await state.set_state(AdminReportState.waiting_for_end_date)
        
        # End sana uchun kalendar
        await callback.message.edit_text(
            f"✅ Boshlang'ich sana: {date.strftime('%Y-%m-%d')}\n\n"
            "📅 TUGASH SANASINI tanlang:",
            reply_markup=await calendar.start_calendar()
        )
    else:
        # Bekor qilindi
        await state.clear()
        await callback.message.edit_text(
            "❌ Hisobot yaratish bekor qilindi.",
            reply_markup=admin_main_menu_kb(),
        )


@router.callback_query(SimpleCalendarCallback.filter(), AdminReportState.waiting_for_end_date)
async def process_end_date_calendar(
    callback: CallbackQuery,
    callback_data: SimpleCalendarCallback,
    state: FSMContext
):
    if not is_admin(callback.from_user.id):
        await callback.answer("Siz admin emassiz", show_alert=True)
        return
    
    calendar = SimpleCalendar(
        locale='uz',
        show_alerts=True,
        cancel_button=True
    )
    
    selected, date = await calendar.process_selection(callback, callback_data)
    
    if selected:
        data = await state.get_data()
        start_date = data.get("start_date")
        end_date = date
        
        # Tugash sanasi boshlang'ichdan oldin emasligini tekshiramiz
        if end_date < start_date:
            await callback.answer("❌ Tugash sanasi boshlang'ich sanadan oldin bo'lishi mumkin emas!", show_alert=True)
            return
        
        # Vaqt oralig'ini soatga o'tkazamiz
        start_datetime = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        end_datetime = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=timezone.utc)
        
        await state.clear()
        
        await callback.message.edit_text(
            f"📊 Hisobot tayyorlanmoqda...\n\n"
            f"📅 Boshlanish: {start_date.strftime('%Y-%m-%d')}\n"
            f"📅 Tugash: {end_date.strftime('%Y-%m-%d')}"
        )
        
        # Statistikani olish
        stats = await get_stats_for_range(GROUP_CHAT_ID, start_datetime, end_datetime)
        
        period_label = f"{start_date.strftime('%Y-%m-%d')} dan {end_date.strftime('%Y-%m-%d')} gacha"
        
        os.makedirs("reports", exist_ok=True)
        filename = f"reports/report_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{datetime.now().strftime('%H%M%S')}.pdf"
        
        await asyncio.to_thread(build_pdf_report, stats, period_label, filename)
        
        short_text = (
            f"So'nggi {period_label} bo'yicha natija tayyor.\n"
            f"Jami xabarlar: {stats['total_messages']}\n"
            f"Faol foydalanuvchilar: {len(stats['users'])}"
        )
        
        await callback.message.answer(short_text)
        await callback.message.answer_document(
            FSInputFile(filename),
            caption=f"📊 {period_label} uchun PDF hisobot",
        )
        
        # Asosiy menyuga qaytish
        await callback.message.answer(
            "👋 Admin panelga xush kelibsiz!",
            reply_markup=admin_main_menu_kb(),
        )
        
    else:
        # Bekor qilindi
        await state.clear()
        await callback.message.edit_text(
            "❌ Hisobot yaratish bekor qilindi.",
            reply_markup=admin_main_menu_kb(),
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
        672: "1 oy"
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

    await append_group_message(
        chat_id=message.chat.id,
        message_id=message.message_id,
        user_id=message.from_user.id,
        full_name=message.from_user.full_name,
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

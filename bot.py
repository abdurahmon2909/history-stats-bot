from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone

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


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def admin_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Oxirgi 2 soat", callback_data="report:2"),
                InlineKeyboardButton(text="Oxirgi 4 soat", callback_data="report:4"),
            ],
            [
                InlineKeyboardButton(text="Oxirgi 8 soat", callback_data="report:8"),
                InlineKeyboardButton(text="1 kun", callback_data="report:24"),
            ],
            [
                InlineKeyboardButton(text="3 kun", callback_data="report:72"),
                InlineKeyboardButton(text="1 hafta", callback_data="report:168"),
            ],
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
            "Admin panel ochildi. Quyidan statistika periodini tanlang:",
            reply_markup=admin_menu_kb(),
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


@router.message(Command("admin"))
async def admin_command(message: Message):
    user = message.from_user
    if not user or not is_admin(user.id):
        return

    await message.answer(
        "Statistika periodini tanlang:",
        reply_markup=admin_menu_kb(),
    )


@router.message(Command("id"))
async def get_id(message: Message):
    await message.answer(f"Chat ID: {message.chat.id}")


@router.callback_query(F.data.startswith("report:"))
async def report_callback(callback: CallbackQuery):
    user = callback.from_user
    if not is_admin(user.id):
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

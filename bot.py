from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ChatMemberStatus, ChatType
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart, Command
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
    CHANNEL_USERNAME,
    ADMIN_IDS,
)
from sheets import (
    init_sheets,
    upsert_user,
    append_group_message,
    get_stats_for_hours,
)
from pdf_report import build_pdf_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)


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
    channel_url = f"https://t.me/{CHANNEL_USERNAME.lstrip('@')}"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Kanalga o'tish", url=channel_url)],
            [InlineKeyboardButton(text="Qayta tekshirish", callback_data="check_sub")],
        ]
    )


async def check_subscription(user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_USERNAME, user_id=user_id)
        return member.status in {
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.OWNER,
        }
    except TelegramBadRequest as e:
        err = str(e).lower()
        if "member list is inaccessible" in err:
            logging.error(
                "Kanal a'zoligini tekshirib bo'lmadi: bot kanalga qo'shilmagan yoki admin emas"
            )
            return False
        logging.exception("Obunani tekshirishda Telegram xatosi: %s", e)
        return False
    except Exception as e:
        logging.exception("Obunani tekshirishda xato: %s", e)
        return False


@router.message(CommandStart())
async def start_handler(message: Message):
    user = message.from_user
    if not user:
        return

    subscribed = await check_subscription(user.id)

    await upsert_user(
        user_id=user.id,
        full_name=user.full_name,
        username=user.username,
        is_subscribed=1 if subscribed else 0,
    )

    if not subscribed:
        await message.answer(
            f"Botdan foydalanish uchun avval {CHANNEL_USERNAME} kanaliga a'zo bo'ling.",
            reply_markup=join_channel_kb(),
        )
        return

    if is_admin(user.id):
        await message.answer(
            "Admin panel ochildi. Quyidan statistika periodini tanlang:",
            reply_markup=admin_menu_kb(),
        )
        return

    await message.answer(
        "Xush kelibsiz.\n\n"
        "Siz yuborgan xabarlar adminlarga yetkaziladi."
    )


@router.callback_query(F.data == "check_sub")
async def check_subscription_callback(callback: CallbackQuery):
    user = callback.from_user
    if not user:
        await callback.answer("Foydalanuvchi topilmadi", show_alert=True)
        return

    subscribed = await check_subscription(user.id)

    await upsert_user(
        user_id=user.id,
        full_name=user.full_name,
        username=user.username,
        is_subscribed=1 if subscribed else 0,
    )

    if not subscribed:
        await callback.answer("Siz hali kanalga a'zo bo'lmagansiz", show_alert=True)
        return

    await callback.answer("Obuna tasdiqlandi ✅")

    if is_admin(user.id):
        await callback.message.edit_text(
            "Admin panel ochildi. Quyidan statistika periodini tanlang:",
            reply_markup=admin_menu_kb(),
        )
    else:
        await callback.message.edit_text(
            "Obuna tasdiqlandi ✅\n\n"
            "Xush kelibsiz.\n"
            "Siz yuborgan xabarlar adminlarga yetkaziladi."
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

    await upsert_user(
        user_id=message.from_user.id,
        full_name=message.from_user.full_name,
        username=message.from_user.username,
        is_subscribed=None,
    )


@router.message(F.chat.type == ChatType.PRIVATE)
async def private_message_router(message: Message):
    user = message.from_user
    if not user:
        return

    if message.text and message.text.startswith("/"):
        return

    subscribed = await check_subscription(user.id)

    await upsert_user(
        user_id=user.id,
        full_name=user.full_name,
        username=user.username,
        is_subscribed=1 if subscribed else 0,
    )

    if not subscribed:
        await message.answer(
            f"Avval {CHANNEL_USERNAME} kanaliga a'zo bo'ling.",
            reply_markup=join_channel_kb(),
        )
        return

    sender_info = (
        f"Yangi murojaat\n\n"
        f"Ism: {user.full_name}\n"
        f"Username: @{user.username}" if user.username
        else f"Yangi murojaat\n\nIsm: {user.full_name}\nUsername: yo'q"
    )
    sender_info += f"\nUser ID: {user.id}"

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, sender_info)
            await bot.copy_message(
                chat_id=admin_id,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
            )
        except Exception as e:
            logging.exception("Adminga yuborishda xato: %s", e)

    await message.answer("Xabaringiz adminlarga yuborildi.")


async def main():
    await init_sheets()
    logging.info("Google Sheets ga ulanildi")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())

from __future__ import annotations

import logging
from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from config import ADMIN_IDS

router = Router()


class BCState(StatesGroup):
    waiting_msg = State()


@router.message(Command("broadcast"))
async def bc_start(message: Message, state: FSMContext):
    """Broadcast boshlash"""
    user_id = message.from_user.id
    
    if user_id not in ADMIN_IDS:
        await message.reply("❌ Bu buyruq faqat adminlar uchun!")
        return
    
    await state.set_state(BCState.waiting_msg)
    await message.answer(
        "📢 **ELON MATNINI KIRITING**\n\n"
        "Yubormoqchi bo'lgan xabaringizni yozing.\n\n"
        "❌ Bekor qilish: /cancel",
        parse_mode="Markdown"
    )
    logging.info(f"Admin {user_id} broadcast boshlandi")


@router.message(Command("cancel"))
async def bc_cancel(message: Message, state: FSMContext):
    """Bekor qilish"""
    await state.clear()
    await message.answer("❌ Bekor qilindi!")


@router.message(BCState.waiting_msg)
async def bc_get_msg(message: Message, state: FSMContext):
    """Xabarni olish"""
    user_id = message.from_user.id
    
    if user_id not in ADMIN_IDS:
        await message.reply("❌ Siz admin emassiz!")
        await state.clear()
        return
    
    text = message.text
    
    if not text:
        await message.answer("❌ Iltimos, matn kiriting!")
        return
    
    await state.update_data(msg_text=text)
    
    # Preview
    await message.answer(
        f"📋 **XABARINGIZ:**\n\n"
        f"```\n{text[:500]}\n```\n\n"
        f"✅ Yuborish uchun: /send\n"
        f"❌ Bekor qilish: /cancel",
        parse_mode="Markdown"
    )


@router.message(Command("send"))
async def bc_send(message: Message, state: FSMContext):
    """Xabarni yuborish"""
    user_id = message.from_user.id
    
    if user_id not in ADMIN_IDS:
        return
    
    data = await state.get_data()
    text = data.get("msg_text")
    
    if not text:
        await message.answer("❌ Xabar topilmadi! Avval /broadcast bilan xabar yozing.")
        return
    
    await message.answer(f"✅ Xabar yuborildi!\n\nMatn: {text[:100]}")
    logging.info(f"Admin {user_id} xabar yubordi: {text[:50]}")
    
    await state.clear()


# Test uchun oddiy echo
@router.message(F.text)
async def echo_all(message: Message):
    """Barcha xabarlarni qaytarish (test uchun)"""
    user_id = message.from_user.id
    if user_id in ADMIN_IDS:
        await message.answer(f"Echo: {message.text}")

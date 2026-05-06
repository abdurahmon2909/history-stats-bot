from __future__ import annotations

import asyncio
import logging

from aiogram import Bot, Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, 
    CallbackQuery, 
    InlineKeyboardMarkup, 
    InlineKeyboardButton,
)

from config import ADMIN_IDS, GROUP_CHAT_ID
from sheets import get_all_users

router = Router()


class BroadcastState(StatesGroup):
    waiting_for_message = State()


def broadcast_kb() -> InlineKeyboardMarkup:
    """Xabarni tasdiqlash uchun keyboard"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Ha, yuborish", callback_data="broadcast:confirm"),
                InlineKeyboardButton(text="👥 Faqat guruhga", callback_data="broadcast:to_group"),
            ],
            [
                InlineKeyboardButton(text="📋 Xabarni ko'rish", callback_data="broadcast:preview"),
                InlineKeyboardButton(text="❌ Bekor qilish", callback_data="broadcast:cancel"),
            ]
        ]
    )


async def start_broadcast(message: Message, state: FSMContext):
    """Broadcast boshlash"""
    user_id = message.from_user.id
    if user_id not in ADMIN_IDS:
        await message.reply("❌ Bu buyruq faqat adminlar uchun!")
        return
    
    await state.set_state(BroadcastState.waiting_for_message)
    await message.answer(
        "📢 **ELON YUBORISH**\n\n"
        "Yubormoqchi bo'lgan xabaringizni kiriting:\n\n"
        "• Matn - oddiy matn\n"
        "• Rasm - rasm + caption\n"
        "• Video - video + caption\n"
        "• Hujjat - fayl + caption\n\n"
        "⚠️ Xabar BARCHA foydalanuvchilarga yuboriladi!\n\n"
        "❌ Bekor qilish uchun /cancel yozing",
        parse_mode="Markdown"
    )


@router.message(Command("broadcast"))
async def broadcast_command(message: Message, state: FSMContext):
    """Adminlarga xabar yuborish imkoniyati"""
    await start_broadcast(message, state)


@router.message(Command("cancel"))
async def cancel_broadcast(message: Message, state: FSMContext):
    """Xabar yuborishni bekor qilish"""
    await state.clear()
    await message.answer("❌ Xabar yuborish bekor qilindi.")


@router.message(BroadcastState.waiting_for_message)
async def get_broadcast_message(message: Message, state: FSMContext):
    """Admin xabarini saqlash"""
    user_id = message.from_user.id
    if user_id not in ADMIN_IDS:
        await message.reply("❌ Siz admin emassiz!")
        await state.clear()
        return
    
    # Xabar turini aniqlash
    message_data = {}
    
    if message.text:
        message_data = {"type": "text", "text": message.text}
    elif message.photo:
        message_data = {
            "type": "photo", 
            "content": message.photo[-1].file_id,
            "caption": message.caption or ""
        }
    elif message.video:
        message_data = {
            "type": "video",
            "content": message.video.file_id,
            "caption": message.caption or ""
        }
    elif message.document:
        message_data = {
            "type": "document",
            "content": message.document.file_id,
            "caption": message.caption or ""
        }
    else:
        await message.answer(
            "❌ Faqat matn, rasm, video yoki hujjat yuboring!\n"
            "Qaytadan kiriting yoki /cancel yozing:"
        )
        return
    
    await state.update_data(broadcast_message=message_data)
    
    # Xabarni oldindan ko'rish
    preview_text = "📢 **XABAR OLDINDAN KO'RISH**\n\n"
    
    if message_data["type"] == "text":
        preview_text += f"📝 Matn:\n```\n{message_data['text'][:300]}\n```"
    elif message_data["type"] == "photo":
        preview_text += "🖼 Rasm yuboriladi\n"
        if message_data.get("caption"):
            preview_text += f"📝 Sarlavha: {message_data['caption'][:100]}"
    elif message_data["type"] == "video":
        preview_text += "🎥 Video yuboriladi\n"
        if message_data.get("caption"):
            preview_text += f"📝 Sarlavha: {message_data['caption'][:100]}"
    elif message_data["type"] == "document":
        preview_text += "📎 Hujjat yuboriladi\n"
        if message_data.get("caption"):
            preview_text += f"📝 Sarlavha: {message_data['caption'][:100]}"
    
    preview_text += "\n\n✅ Xabar yuborilsinmi?"
    
    await message.answer(preview_text, reply_markup=broadcast_kb(), parse_mode="Markdown")


@router.callback_query(F.data == "broadcast:preview")
async def preview_message(callback: CallbackQuery, state: FSMContext):
    """Xabarni oldindan ko'rish"""
    data = await state.get_data()
    msg = data.get("broadcast_message")
    
    if not msg:
        await callback.answer("Xabar topilmadi!", show_alert=True)
        return
    
    if msg["type"] == "text":
        await callback.message.answer(f"📋 **MATN:**\n```\n{msg['text']}\n```", parse_mode="Markdown")
    elif msg["type"] == "photo":
        await callback.message.answer_photo(msg["content"], caption=msg.get("caption", "📷 Rasm"))
    elif msg["type"] == "video":
        await callback.message.answer_video(msg["content"], caption=msg.get("caption", "🎥 Video"))
    elif msg["type"] == "document":
        await callback.message.answer_document(msg["content"], caption=msg.get("caption", "📎 Hujjat"))
    
    await callback.answer()


@router.callback_query(F.data == "broadcast:to_group")
async def send_to_group(callback: CallbackQuery, state: FSMContext, bot: Bot):
    """Faqat guruhga yuborish"""
    data = await state.get_data()
    msg = data.get("broadcast_message")
    
    if not msg:
        await callback.answer("Xabar topilmadi!", show_alert=True)
        return
    
    await callback.message.edit_text("📤 Guruhga yuborilmoqda...")
    
    try:
        if msg["type"] == "text":
            await bot.send_message(GROUP_CHAT_ID, msg["text"])
        elif msg["type"] == "photo":
            await bot.send_photo(GROUP_CHAT_ID, msg["content"], caption=msg.get("caption"))
        elif msg["type"] == "video":
            await bot.send_video(GROUP_CHAT_ID, msg["content"], caption=msg.get("caption"))
        elif msg["type"] == "document":
            await bot.send_document(GROUP_CHAT_ID, msg["content"], caption=msg.get("caption"))
        
        await state.clear()
        await callback.message.edit_text("✅ Xabar guruhga yuborildi!")
    except Exception as e:
        await callback.message.edit_text(f"❌ Xatolik: {e}")
    
    await callback.answer()


@router.callback_query(F.data == "broadcast:confirm")
async def send_to_all(callback: CallbackQuery, state: FSMContext, bot: Bot):
    """Barcha foydalanuvchilarga yuborish"""
    data = await state.get_data()
    msg = data.get("broadcast_message")
    
    if not msg:
        await callback.answer("Xabar topilmadi!", show_alert=True)
        return
    
    await callback.message.edit_text("📤 Barcha foydalanuvchilarga yuborilmoqda...\n⏳ Bu biroz vaqt olishi mumkin...")
    
    users = await get_all_users()
    
    if not users:
        await callback.message.edit_text("❌ Hech qanday foydalanuvchi topilmadi!")
        return
    
    success = 0
    fail = 0
    
    for user in users:
        try:
            user_id = user.get("user_id")
            if not user_id:
                continue
            
            if msg["type"] == "text":
                await bot.send_message(user_id, msg["text"])
            elif msg["type"] == "photo":
                await bot.send_photo(user_id, msg["content"], caption=msg.get("caption"))
            elif msg["type"] == "video":
                await bot.send_video(user_id, msg["content"], caption=msg.get("caption"))
            elif msg["type"] == "document":
                await bot.send_document(user_id, msg["content"], caption=msg.get("caption"))
            
            success += 1
            await asyncio.sleep(0.05)
        except Exception:
            fail += 1
    
    await state.clear()
    await callback.message.edit_text(
        f"✅ **Xabar yuborish yakunlandi!**\n\n"
        f"✅ Muvaffaqiyatli: {success}\n"
        f"❌ Muvaffaqiyatsiz: {fail}\n"
        f"👥 Jami: {len(users)}"
    )
    await callback.answer()


@router.callback_query(F.data == "broadcast:cancel")
async def cancel_broadcast_callback(callback: CallbackQuery, state: FSMContext):
    """Bekor qilish"""
    await state.clear()
    await callback.message.edit_text("❌ Xabar yuborish bekor qilindi.")
    await callback.answer()

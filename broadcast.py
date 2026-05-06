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

router = Router(name="broadcast_router")  # Routerga nom bering


class BroadcastState(StatesGroup):
    waiting_for_message = State()


def broadcast_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Ha, barchaga yuborish", callback_data="broadcast:confirm"),
                InlineKeyboardButton(text="👥 Faqat guruhga", callback_data="broadcast:to_group"),
            ],
            [
                InlineKeyboardButton(text="📋 Xabarni ko'rish", callback_data="broadcast:preview"),
                InlineKeyboardButton(text="❌ Bekor qilish", callback_data="broadcast:cancel"),
            ]
        ]
    )


# ============ /broadcast KOMANDASI ============
@router.message(Command("broadcast"))
async def broadcast_command(message: Message, state: FSMContext):
    """Adminlarga xabar yuborish imkoniyati"""
    user_id = message.from_user.id
    
    # Admin tekshiruvi
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
    logging.info(f"Admin {user_id} broadcast boshladi")


@router.message(Command("cancel"))
async def cancel_command(message: Message, state: FSMContext):
    """Xabar yuborishni bekor qilish"""
    await state.clear()
    await message.answer("❌ Xabar yuborish bekor qilindi.")


# ============ XABARNI QABUL QILISH ============
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
        message_data = {
            "type": "text", 
            "text": message.text
        }
    elif message.photo:
        message_data = {
            "type": "photo", 
            "file_id": message.photo[-1].file_id,
            "caption": message.caption or ""
        }
    elif message.video:
        message_data = {
            "type": "video",
            "file_id": message.video.file_id,
            "caption": message.caption or ""
        }
    elif message.document:
        message_data = {
            "type": "document",
            "file_id": message.document.file_id,
            "caption": message.caption or ""
        }
    else:
        await message.answer(
            "❌ Faqat matn, rasm, video yoki hujjat yuboring!\n"
            "Qaytadan kiriting yoki /cancel yozing:"
        )
        return
    
    await state.update_data(broadcast_message=message_data)
    
    # Preview
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
    
    await message.answer(
        preview_text, 
        reply_markup=broadcast_kb(), 
        parse_mode="Markdown"
    )
    logging.info(f"Admin {user_id} xabar tayyor, tasdiqlanishi kutilmoqda")


# ============ PREVIEW ============
@router.callback_query(F.data == "broadcast:preview")
async def preview_callback(callback: CallbackQuery, state: FSMContext):
    """Xabarni oldindan ko'rish"""
    data = await state.get_data()
    msg = data.get("broadcast_message")
    
    if not msg:
        await callback.answer("Xabar topilmadi!", show_alert=True)
        return
    
    if msg["type"] == "text":
        await callback.message.answer(
            f"📋 **MATN PREVIEW:**\n```\n{msg['text']}\n```", 
            parse_mode="Markdown"
        )
    elif msg["type"] == "photo":
        await callback.message.answer_photo(
            msg["file_id"], 
            caption=msg.get("caption", "📷 Rasm preview")
        )
    elif msg["type"] == "video":
        await callback.message.answer_video(
            msg["file_id"], 
            caption=msg.get("caption", "🎥 Video preview")
        )
    elif msg["type"] == "document":
        await callback.message.answer_document(
            msg["file_id"], 
            caption=msg.get("caption", "📎 Hujjat preview")
        )
    
    await callback.answer()


# ============ FAQAT GURUHGA ============
@router.callback_query(F.data == "broadcast:to_group")
async def to_group_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
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
            await bot.send_photo(GROUP_CHAT_ID, msg["file_id"], caption=msg.get("caption"))
        elif msg["type"] == "video":
            await bot.send_video(GROUP_CHAT_ID, msg["file_id"], caption=msg.get("caption"))
        elif msg["type"] == "document":
            await bot.send_document(GROUP_CHAT_ID, msg["file_id"], caption=msg.get("caption"))
        
        await state.clear()
        await callback.message.edit_text("✅ Xabar guruhga yuborildi!")
    except Exception as e:
        await callback.message.edit_text(f"❌ Xatolik: {e}")
    
    await callback.answer()


# ============ BARCHAGA YUBORISH ============
@router.callback_query(F.data == "broadcast:confirm")
async def to_all_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
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
                await bot.send_photo(user_id, msg["file_id"], caption=msg.get("caption"))
            elif msg["type"] == "video":
                await bot.send_video(user_id, msg["file_id"], caption=msg.get("caption"))
            elif msg["type"] == "document":
                await bot.send_document(user_id, msg["file_id"], caption=msg.get("caption"))
            
            success += 1
            await asyncio.sleep(0.05)  # Rate limit
        except Exception as e:
            fail += 1
            logging.error(f"Yuborilmadi user {user.get('user_id')}: {e}")
    
    await state.clear()
    await callback.message.edit_text(
        f"✅ **Xabar yuborish yakunlandi!**\n\n"
        f"✅ Muvaffaqiyatli: {success}\n"
        f"❌ Muvaffaqiyatsiz: {fail}\n"
        f"👥 Jami: {len(users)}"
    )
    await callback.answer()


# ============ BEKOR QILISH ============
@router.callback_query(F.data == "broadcast:cancel")
async def cancel_callback(callback: CallbackQuery, state: FSMContext):
    """Bekor qilish"""
    await state.clear()
    await callback.message.edit_text("❌ Xabar yuborish bekor qilindi.")
    await callback.answer()

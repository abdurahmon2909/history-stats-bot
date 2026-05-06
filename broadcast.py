from __future__ import annotations

import asyncio
import logging
from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from config import ADMIN_IDS, GROUP_CHAT_ID
from sheets import get_all_users

router = Router()


class BroadcastStates(StatesGroup):
    waiting_for_message = State()


def get_confirm_keyboard():
    """Tasdiqlash tugmalari"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ HA, yuborish", callback_data="bc_confirm"),
                InlineKeyboardButton(text="❌ YO'Q", callback_data="bc_cancel"),
            ],
            [
                InlineKeyboardButton(text="👥 FAQAT GURUHGA", callback_data="bc_group"),
                InlineKeyboardButton(text="📋 PREVIEW", callback_data="bc_preview"),
            ]
        ]
    )


@router.message(Command("broadcast"))
async def broadcast_start(message: Message, state: FSMContext):
    """Broadcastni boshlash"""
    user_id = message.from_user.id
    
    if user_id not in ADMIN_IDS:
        await message.reply("❌ Bu buyruq faqat adminlar uchun!")
        return
    
    await state.set_state(BroadcastStates.waiting_for_message)
    await message.answer(
        "📢 **ELON YUBORISH**\n\n"
        "Yubormoqchi bo'lgan xabaringizni kiriting:\n\n"
        "• Matn - oddiy matn\n"
        "• Rasm - rasm + caption\n"
        "• Video - video + caption\n"
        "• Hujjat - fayl + caption\n\n"
        "❌ Bekor qilish: /cancel",
        parse_mode="Markdown"
    )


@router.message(Command("cancel"))
async def broadcast_cancel(message: Message, state: FSMContext):
    """Bekor qilish"""
    await state.clear()
    await message.answer("❌ Xabar yuborish bekor qilindi!")


@router.message(BroadcastStates.waiting_for_message)
async def broadcast_get_message(message: Message, state: FSMContext):
    """Xabarni qabul qilish"""
    user_id = message.from_user.id
    
    if user_id not in ADMIN_IDS:
        await message.reply("❌ Siz admin emassiz!")
        await state.clear()
        return
    
    msg_data = {}
    
    if message.text:
        msg_data = {"type": "text", "content": message.text}
    elif message.photo:
        msg_data = {
            "type": "photo", 
            "content": message.photo[-1].file_id,
            "caption": message.caption or ""
        }
    elif message.video:
        msg_data = {
            "type": "video",
            "content": message.video.file_id,
            "caption": message.caption or ""
        }
    elif message.document:
        msg_data = {
            "type": "document",
            "content": message.document.file_id,
            "caption": message.caption or ""
        }
    else:
        await message.answer("❌ Faqat matn, rasm, video yoki hujjat yuboring!")
        return
    
    await state.update_data(message_data=msg_data)
    
    preview = "📢 **XABAR PREVIEW:**\n\n"
    if msg_data["type"] == "text":
        preview += f"```\n{msg_data['content'][:300]}\n```"
    else:
        preview += f"Tur: {msg_data['type'].upper()}\n"
        if msg_data.get("caption"):
            preview += f"Sarlavha: {msg_data['caption'][:100]}"
    
    preview += "\n\n✅ Yuborilsinmi?"
    
    await message.answer(preview, reply_markup=get_confirm_keyboard(), parse_mode="Markdown")


@router.callback_query(F.data == "bc_preview")
async def preview_callback(callback: CallbackQuery, state: FSMContext):
    """Xabarni oldindan ko'rish"""
    data = await state.get_data()
    msg = data.get("message_data")
    
    if not msg:
        await callback.answer("Xabar topilmadi!", show_alert=True)
        return
    
    if msg["type"] == "text":
        await callback.message.answer(f"📋 MATN:\n```\n{msg['content']}\n```", parse_mode="Markdown")
    elif msg["type"] == "photo":
        await callback.message.answer_photo(msg["content"], caption=msg.get("caption", "Preview"))
    elif msg["type"] == "video":
        await callback.message.answer_video(msg["content"], caption=msg.get("caption", "Preview"))
    elif msg["type"] == "document":
        await callback.message.answer_document(msg["content"], caption=msg.get("caption", "Preview"))
    
    await callback.answer()


@router.callback_query(F.data == "bc_group")
async def group_only_callback(callback: CallbackQuery, state: FSMContext, bot):
    """Faqat guruhga yuborish"""
    data = await state.get_data()
    msg = data.get("message_data")
    
    if not msg:
        await callback.answer("Xabar topilmadi!", show_alert=True)
        return
    
    await callback.message.edit_text("📤 Guruhga yuborilmoqda...")
    
    try:
        if msg["type"] == "text":
            await bot.send_message(GROUP_CHAT_ID, msg["content"])
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


@router.callback_query(F.data == "bc_confirm")
async def confirm_callback(callback: CallbackQuery, state: FSMContext, bot):
    """Barcha foydalanuvchilarga yuborish"""
    data = await state.get_data()
    msg = data.get("message_data")
    
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
            uid = user.get("user_id")
            if not uid:
                continue
            
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
            logging.error(f"Yuborilmadi {user.get('user_id')}: {e}")
    
    await state.clear()
    await callback.message.edit_text(
        f"✅ **Xabar yuborish yakunlandi!**\n\n"
        f"✅ Muvaffaqiyatli: {success}\n"
        f"❌ Muvaffaqiyatsiz: {fail}\n"
        f"👥 Jami: {len(users)}"
    )
    await callback.answer()


@router.callback_query(F.data == "bc_cancel")
async def cancel_callback(callback: CallbackQuery, state: FSMContext):
    """Bekor qilish"""
    await state.clear()
    await callback.message.edit_text("❌ Xabar yuborish bekor qilindi!")
    await callback.answer()b

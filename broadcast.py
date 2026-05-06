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
from sheets import get_all_users  # Faqat get_all_users import qilindi

router = Router()


class BroadcastState(StatesGroup):
    waiting_for_message = State()
    waiting_for_confirmation = State()


def confirm_broadcast_kb() -> InlineKeyboardMarkup:
    """Xabarni tasdiqlash uchun keyboard"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Ha, yuborish", callback_data="broadcast:confirm"),
                InlineKeyboardButton(text="❌ Yo'q", callback_data="broadcast:cancel"),
            ],
            [
                InlineKeyboardButton(text="📋 Xabarni ko'rish", callback_data="broadcast:preview"),
                InlineKeyboardButton(text="👥 Faqat guruhga", callback_data="broadcast:to_group"),
            ]
        ]
    )


async def start_broadcast(message: Message, state: FSMContext):
    """Broadcast boshlash"""
    await state.set_state(BroadcastState.waiting_for_message)
    await message.answer(
        "📢 **ELON YUBORISH**\n\n"
        "Yubormoqchi bo'lgan xabaringizni kiriting.\n\n"
        "📝 **Matn** - oddiy matn yuborish\n"
        "🖼 **Rasm** - rasm + caption yuborish\n"
        "🎥 **Video** - video + caption yuborish\n"
        "📎 **Hujjat** - fayl + caption yuborish\n\n"
        "⚠️ Xabar BARCHA foydalanuvchilarga yuboriladi!\n\n"
        "❌ Bekor qilish uchun /cancel buyrug'ini yozing.",
        parse_mode="Markdown"
    )


@router.message(Command("cancel"))
async def cancel_broadcast(message: Message, state: FSMContext):
    """Xabar yuborishni bekor qilish"""
    current_state = await state.get_state()
    if current_state is not None:
        await state.clear()
        await message.answer("❌ Xabar yuborish bekor qilindi.")
    else:
        await message.answer("Hech qanday jarayon yo'q.")


@router.message(BroadcastState.waiting_for_message)
async def get_broadcast_message(message: Message, state: FSMContext):
    """Admin xabarini saqlash"""
    user_id = message.from_user.id
    if user_id not in ADMIN_IDS:
        await message.reply("❌ Siz admin emassiz!")
        await state.clear()
        return
    
    # Xabar turini aniqlash
    message_data = {
        "type": None,
        "content": None,
        "caption": None,
        "text": None,
    }
    
    if message.text:
        message_data["type"] = "text"
        message_data["text"] = message.text
    elif message.photo:
        message_data["type"] = "photo"
        message_data["content"] = message.photo[-1].file_id
        message_data["caption"] = message.caption or ""
    elif message.video:
        message_data["type"] = "video"
        message_data["content"] = message.video.file_id
        message_data["caption"] = message.caption or ""
    elif message.document:
        message_data["type"] = "document"
        message_data["content"] = message.document.file_id
        message_data["caption"] = message.caption or ""
    else:
        await message.answer(
            "❌ Faqat matn, rasm, video yoki hujjat yuborishingiz mumkin!\n"
            "Qaytadan kiriting yoki /cancel bilan bekor qiling:"
        )
        return
    
    await state.update_data(broadcast_message=message_data)
    
    # Xabarni oldindan ko'rish
    preview_text = f"📢 **XABAR OLDINDAN KO'RISH**\n\n"
    preview_text += f"📍 **Yuboriladigan joy:** Barcha foydalanuvchilar\n\n"
    
    if message_data["type"] == "text":
        preview_text += f"📝 Matn:\n\n`{message_data['text'][:500]}`"
        if len(message_data['text']) > 500:
            preview_text += "..."
    elif message_data["type"] == "photo":
        preview_text += "🖼 Rasm\n"
        if message_data["caption"]:
            preview_text += f"\n📝 Sarlavha: `{message_data['caption'][:200]}`"
    elif message_data["type"] == "video":
        preview_text += "🎥 Video\n"
        if message_data["caption"]:
            preview_text += f"\n📝 Sarlavha: `{message_data['caption'][:200]}`"
    elif message_data["type"] == "document":
        preview_text += "📎 Hujjat\n"
        if message_data["caption"]:
            preview_text += f"\n📝 Sarlavha: `{message_data['caption'][:200]}`"
    
    preview_text += f"\n\n✅ Xabar yuborilsinmi?"
    
    await message.answer(
        preview_text,
        reply_markup=confirm_broadcast_kb(),
        parse_mode="Markdown"
    )


@router.callback_query(F.data == "broadcast:preview")
async def preview_message(callback: CallbackQuery, state: FSMContext):
    """Xabarni oldindan ko'rish"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS:
        await callback.answer("Siz admin emassiz!", show_alert=True)
        return
    
    data = await state.get_data()
    broadcast_message = data.get("broadcast_message")
    
    if not broadcast_message:
        await callback.answer("Xabar topilmadi!", show_alert=True)
        return
    
    try:
        # Xabarni oldindan ko'rish uchun yuborish
        if broadcast_message["type"] == "text":
            await callback.message.answer(
                f"📋 **XABAR MATNI**\n\n{broadcast_message['text']}",
                parse_mode="Markdown"
            )
        elif broadcast_message["type"] == "photo":
            await callback.message.answer_photo(
                broadcast_message["content"],
                caption=broadcast_message["caption"] or "📷 Rasm"
            )
        elif broadcast_message["type"] == "video":
            await callback.message.answer_video(
                broadcast_message["content"],
                caption=broadcast_message["caption"] or "🎥 Video"
            )
        elif broadcast_message["type"] == "document":
            await callback.message.answer_document(
                broadcast_message["content"],
                caption=broadcast_message["caption"] or "📎 Hujjat"
            )
        
        await callback.answer("Xabar yuqorida ko'rsatildi ✅")
    except Exception as e:
        await callback.answer(f"Xatolik: {e}", show_alert=True)


@router.callback_query(F.data == "broadcast:to_group")
async def send_to_group_only(callback: CallbackQuery, state: FSMContext, bot: Bot):
    """Faqat guruhga xabar yuborish"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS:
        await callback.answer("Siz admin emassiz!", show_alert=True)
        return
    
    data = await state.get_data()
    broadcast_message = data.get("broadcast_message")
    
    if not broadcast_message:
        await callback.answer("Xabar topilmadi!", show_alert=True)
        return
    
    await callback.message.edit_text("📤 Xabar guruhga yuborilmoqda...")
    
    try:
        # Faqat guruhga yuborish
        if broadcast_message["type"] == "text":
            await bot.send_message(GROUP_CHAT_ID, broadcast_message["text"])
        elif broadcast_message["type"] == "photo":
            await bot.send_photo(
                GROUP_CHAT_ID, 
                broadcast_message["content"], 
                caption=broadcast_message["caption"]
            )
        elif broadcast_message["type"] == "video":
            await bot.send_video(
                GROUP_CHAT_ID, 
                broadcast_message["content"], 
                caption=broadcast_message["caption"]
            )
        elif broadcast_message["type"] == "document":
            await bot.send_document(
                GROUP_CHAT_ID, 
                broadcast_message["content"], 
                caption=broadcast_message["caption"]
            )
        
        await state.clear()
        
        await callback.message.edit_text(
            f"✅ **Xabar guruhga yuborildi!**\n\n"
            f"👥 Guruh ID: `{GROUP_CHAT_ID}`\n"
        )
        
    except Exception as e:
        await callback.message.edit_text(
            f"❌ **Xatolik!**\n\n"
            f"Xabar yuborilmadi: {str(e)}\n\n"
            f"Bot guruhga qo'shilganligini tekshiring."
        )
    
    await callback.answer()


@router.callback_query(F.data == "broadcast:confirm")
async def confirm_broadcast_to_all(callback: CallbackQuery, state: FSMContext, bot: Bot):
    """Xabarni BARCHA foydalanuvchilarga yuborish"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS:
        await callback.answer("Siz admin emassiz!", show_alert=True)
        return
    
    data = await state.get_data()
    broadcast_message = data.get("broadcast_message")
    
    if not broadcast_message:
        await callback.answer("Xabar topilmadi!", show_alert=True)
        return
    
    await callback.message.edit_text("📤 Xabar barcha foydalanuvchilarga yuborilmoqda...\n⏳ Bu bir necha daqiqa vaqt olishi mumkin...")
    
    # Barcha foydalanuvchilarni olish
    all_users = await get_all_users()
    
    if not all_users:
        await callback.message.edit_text("❌ Hech qanday foydalanuvchi topilmadi!")
        return
    
    success_count = 0
    fail_count = 0
    
    # Har bir foydalanuvchiga xabar yuborish
    for user_data in all_users:
        try:
            user_id = user_data.get("user_id")
            if not user_id:
                continue
            
            if broadcast_message["type"] == "text":
                await bot.send_message(user_id, broadcast_message["text"])
            elif broadcast_message["type"] == "photo":
                await bot.send_photo(
                    user_id, 
                    broadcast_message["content"], 
                    caption=broadcast_message["caption"]
                )
            elif broadcast_message["type"] == "video":
                await bot.send_video(
                    user_id, 
                    broadcast_message["content"], 
                    caption=broadcast_message["caption"]
                )
            elif broadcast_message["type"] == "document":
                await bot.send_document(
                    user_id, 
                    broadcast_message["content"], 
                    caption=broadcast_message["caption"]
                )
            
            success_count += 1
            
            # Rate limitga rioya qilish (sekundiga 20 ta xabar)
            await asyncio.sleep(0.05)
            
        except Exception as e:
            fail_count += 1
            logging.error(f"Xabar yuborilmadi (user_id={user_id}): {e}")
    
    await state.clear()
    
    await callback.message.edit_text(
        f"✅ **Xabar yuborish yakunlandi!**\n\n"
        f"📊 **Statistika:**\n"
        f"✅ Muvaffaqiyatli: {success_count}\n"
        f"❌ Muvaffaqiyatsiz: {fail_count}\n"
        f"👥 Jami foydalanuvchilar: {len(all_users)}"
    )
    
    await callback.answer()


@router.callback_query(F.data == "broadcast:cancel")
async def cancel_broadcast_callback(callback: CallbackQuery, state: FSMContext):
    """Xabar yuborishni bekor qilish"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS:
        await callback.answer("Siz admin emassiz!", show_alert=True)
        return
    
    await state.clear()
    await callback.message.edit_text("❌ Xabar yuborish bekor qilindi.")
    await callback.answer()

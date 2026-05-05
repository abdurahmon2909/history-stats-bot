# ============ group_events.py (YANGI FAYL) ============
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from aiogram import Bot, Router, F
from aiogram.enums import ChatType, ChatMemberStatus
from aiogram.types import ChatMemberUpdated, Message

from config import ADMIN_IDS

router = Router()

# O'chirilgan xabarlar logini saqlash (ixtiyoriy)
deleted_log = []


@router.chat_member()
async def auto_delete_join_leave(event: ChatMemberUpdated):
    """
    Guruhga odam kirganda yoki chiqanda avtomatik o'chiradi
    Hech qanday /del buyrug'i kerak emas
    """
    # Faqat guruh ekanligini tekshirish
    if event.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return
    
    # Botning o'zgarishlarini filter qilish
    if event.new_chat_member.user.is_bot:
        return
    
    old_status = event.old_chat_member.status
    new_status = event.new_chat_member.status
    user = event.new_chat_member.user
    
    # Kirish yoki chiqish holatini aniqlash
    should_delete = False
    action = None
    
    # ODAM KIRDI
    if old_status in [ChatMemberStatus.LEFT, ChatMemberStatus.KICKED] and \
       new_status in [ChatMemberStatus.MEMBER, ChatMemberStatus.RESTRICTED]:
        should_delete = True
        action = "joined"
    
    # ODAM CHIQDI
    elif new_status in [ChatMemberStatus.LEFT, ChatMemberStatus.KICKED] and \
         old_status not in [ChatMemberStatus.LEFT, ChatMemberStatus.KICKED]:
        should_delete = True
        action = "left"
    
    if not should_delete:
        return
    
    # O'chirishdan oldin biroz kutish (xabar kelishi uchun)
    await asyncio.sleep(0.5)
    
    try:
        # So'nggi xabarlarni tekshirish va service message ni o'chirish
        # Telegram'da kirish/chiqish xabarlari service message sifatida keladi
        # Ularni o'chirishning eng ishonchli usuli - event'dan keyin darhol o'chirish
        
        # Service message ni o'chirish uchun maxsus usul
        # Afsuski, bot API to'g'ridan-to'g'ri service message ni o'chirish imkonini bermaydi
        # Lekin quyidagi usul bilan ishlaydi:
        
        # 1. Guruhdagi so'nggi 5 ta xabarni olish
        # 2. Agar service message bo'lsa, o'chirish
        
        async for message in event.chat.history(limit=5):
            # Agar service message bo'lsa (new_chat_members yoki left_chat_member)
            if message.new_chat_members or message.left_chat_member:
                await message.delete()
                
                # Adminlarga log yuborish (ixtiyoriy)
                action_text = "qo'shildi" if action == "joined" else "chiqdi"
                log_text = f"🗑 Avtomatik o'chirildi: {user.full_name} ({user.id}) {action_text}"
                
                for admin_id in ADMIN_IDS:
                    try:
                        await event.bot.send_message(admin_id, log_text)
                    except:
                        pass
                
                break
                
    except Exception as e:
        logging.error(f"Kirish/chiqish xabarini o'chirishda xato: {e}")


@router.message(F.new_chat_members | F.left_chat_member)
async def auto_delete_service_message(message: Message):
    """
    Service message (new_chat_members, left_chat_member) larni avtomatik o'chiradi
    Bu to'g'ridan-to'g'ri xabar kelganda ishlaydi
    """
    try:
        await message.delete()
        
        # Adminlarga log yuborish
        if message.new_chat_members:
            for user in message.new_chat_members:
                if not user.is_bot:
                    log_text = f"🗑 Avtomatik o'chirildi: {user.full_name} ({user.id}) guruhga qo'shildi"
                    for admin_id in ADMIN_IDS:
                        try:
                            await message.bot.send_message(admin_id, log_text)
                        except:
                            pass
        
        elif message.left_chat_member:
            user = message.left_chat_member
            if not user.is_bot:
                log_text = f"🗑 Avtomatik o'chirildi: {user.full_name} ({user.id}) guruhni tark etdi"
                for admin_id in ADMIN_IDS:
                    try:
                        await message.bot.send_message(admin_id, log_text)
                    except:
                        pass
                        
    except Exception as e:
        logging.error(f"Service message ni o'chirishda xato: {e}")
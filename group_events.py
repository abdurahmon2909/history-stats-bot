from __future__ import annotations

import asyncio
import logging

from aiogram import Router, F
from aiogram.enums import ChatType
from aiogram.types import Message

router = Router()


@router.message(F.new_chat_members)
async def auto_delete_join_message(message: Message):
    """
    Guruhga odam qo'shilganda chiqadigan xabarni avtomatik o'chiradi
    Hech qanday xabar yuborilmaydi
    """
    # Faqat guruh ekanligini tekshirish
    if message.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return
    
    # Botning o'zini qo'shilishini filter qilish
    for user in message.new_chat_members:
        if user.is_bot:
            return
    
    try:
        # Xabarni o'chirish (hech qanday xabar yuborilmaydi)
        await message.delete()
        logging.info(f"Kirish xabari o'chirildi: {message.chat.id}")
    except Exception as e:
        logging.error(f"Kirish xabarini o'chirishda xato: {e}")


@router.message(F.left_chat_member)
async def auto_delete_leave_message(message: Message):
    """
    Guruhdan odam chiqqanda chiqadigan xabarni avtomatik o'chiradi
    Hech qanday xabar yuborilmaydi
    """
    # Faqat guruh ekanligini tekshirish
    if message.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return
    
    # Botning o'zini chiqishini filter qilish
    if message.left_chat_member.is_bot:
        return
    
    try:
        # Xabarni o'chirish (hech qanday xabar yuborilmaydi)
        await message.delete()
        logging.info(f"Chiqish xabari o'chirildi: {message.chat.id}")
    except Exception as e:
        logging.error(f"Chiqish xabarini o'chirishda xato: {e}")

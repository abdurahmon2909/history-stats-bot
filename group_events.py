from __future__ import annotations

import logging

from aiogram import Router, F, Bot
from aiogram.enums import ChatType
from aiogram.types import Message

router = Router()


@router.message(F.new_chat_members)
async def auto_delete_join_message(message: Message, bot: Bot):
    """
    Guruhga odam qo'shilganda chiqadigan xabarni avtomatik o'chiradi.
    Hech qanday xabar yuborilmaydi.
    """
    # Faqat guruh ekanligini tekshirish
    if message.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return

    # Faqat botning O'ZINI qo'shilishini filter qilish
    for user in message.new_chat_members:
        if user.is_bot and user.id == bot.id:
            return

    try:
        await message.delete()
        logging.info(f"Kirish xabari o'chirildi: {message.chat.id}")
    except Exception as e:
        logging.error(f"Kirish xabarini o'chirishda xato: {e}")


@router.message(F.left_chat_member)
async def auto_delete_leave_message(message: Message, bot: Bot):
    """
    Guruhdan odam chiqqanda chiqadigan xabarni avtomatik o'chiradi.
    Hech qanday xabar yuborilmaydi.
    """
    # Faqat guruh ekanligini tekshirish
    if message.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        return

    # Botning o'zini chiqishini filter qilish
    if message.left_chat_member.is_bot and message.left_chat_member.id == bot.id:
        return

    try:
        await message.delete()
        logging.info(f"Chiqish xabari o'chirildi: {message.chat.id}")
    except Exception as e:
        logging.error(f"Chiqish xabarini o'chirishda xato: {e}")

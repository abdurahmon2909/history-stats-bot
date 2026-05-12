from __future__ import annotations

import logging

from aiogram import Router, F
from aiogram.enums import ChatType
from aiogram.types import Message
import re

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
    
    # Faqat botning O'ZINI qo'shilishini filter qilish
    for user in message.new_chat_members:
        if user.is_bot and user.id == message.bot.id:
            return
    
    try:
        # Xabarni o'chirish
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
    if message.left_chat_member.is_bot and message.left_chat_member.id == message.bot.id:
        return
    
    try:
        # Xabarni o'chirish
        await message.delete()
        logging.info(f"Chiqish xabari o'chirildi: {message.chat.id}")
    except Exception as e:
        logging.error(f"Chiqish xabarini o'chirishda xato: {e}")

LINK_REGEX = re.compile(
    r"(https?://\S+|www\.\S+|t\.me/\S+|telegram\.me/\S+)",
    re.IGNORECASE
)


@router.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def block_links_handler(message: Message):
    """
    Guruhga link tashlashni bloklaydi
    """

    if not message.text:
        return

    try:
        # Adminlarni tekshirish
        member = await message.bot.get_chat_member(
            chat_id=message.chat.id,
            user_id=message.from_user.id
        )

        # Adminlarga ruxsat
        if member.status in ["administrator", "creator"]:
            return

        # Link tekshirish
        if LINK_REGEX.search(message.text):

            # Xabarni o‘chirish
            await message.delete()

            # Ogohlantirish
            await message.answer(
                f"{message.from_user.full_name}, guruhga link tashlamang!"
            )

            logging.info(
                f"Link o‘chirildi | User: {message.from_user.id} | Chat: {message.chat.id}"
            )

    except Exception as e:
        logging.error(f"Link bloklashda xato: {e}")

from __future__ import annotations

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

router = Router()


@router.message(Command("broadcast"))
async def broadcast_command(message: Message):
    """Oddiy test broadcast"""
    await message.answer("✅ Broadcast komandasi ishlayapti! Test muvaffaqiyatli!")

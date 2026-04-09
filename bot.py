import asyncio
import json
import logging
import os
import re
from datetime import datetime
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, BusinessMessagesDeleted, BusinessConnection
)
from aiogram.filters import Command

# ─── НАСТРОЙКИ ───────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
MY_USER_ID = int(os.getenv("MY_USER_ID", "0"))
# ─────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ─── ХРАНИЛИЩА ───────────────────────────────────────────────
cache: dict[tuple, dict] = {}
connections: dict[str, dict] = {}

MONITORS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "monitors.json")
monitors: dict[str, dict] = {}


def load_monitors():
    global monitors
    try:
        with open(MONITORS_FILE, "r", encoding="utf-8") as f:
            monitors = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        monitors = {}


def save_monitors():
    with open(MONITORS_FILE, "w", encoding="utf-8") as f:
        json.dump(monitors, f, ensure_ascii=False, indent=2)


load_monitors()


def fmt(dt: datetime) -> str:
    return dt.strftime("%d.%m.%Y %H:%M:%S")


@dp.business_connection()
async def on_business_connection(conn: BusinessConnection):
    connections[conn.id] = {
        "user_id": conn.user.id,
        "user_name": conn.user.full_name,
        "username": (conn.user.username or "").lower(),
    }
    logging.info(f"Business connection {conn.id} -> user {conn.user.id} (@{conn.user.username})")


def get_owner(conn_id: str) -> dict | None:
    return connections.get(conn_id)


async def send_media(user_id: int, data: dict, header: str):
    try:
        if data.get("photo"):
            cap = header + (f"\n\n📝 {data['text']}" if data["text"] else "")
            await bot.send_photo(user_id, data["photo"], caption=cap, parse_mode="HTML")
        elif data.get("video"):
            cap = header + (f"\n\n📝 {data['text']}" if data["text"] else "")
            await bot.send_video(user_id, data["video"], caption=cap, parse_mode="HTML")
        elif data.get("voice"):
            await bot.send_message(user_id, header, parse_mode="HTML")
            await bot.send_voice(user_id, data["voice"])
        elif data.get("sticker"):
            await bot.send_message(user_id, header, parse_mode="HTML")
            await bot.send_sticker(user_id, data["sticker"])
        elif data.get("document"):
            cap = header + (f"\n\n📝 {data['text']}" if data["text"] else "")
            await bot.send_document(user_id, data["document"], caption=cap, parse_mode="HTML")
        elif data.get("animation"):
            await bot.send_message(user_id, header, parse_mode="HTML")
            await bot.send_animation(user_id, data["animation"])
        elif data.get("video_note"):
            await bot.send_message(user_id, header, parse_mode="HTML")
            await bot.send_video_note(user_id, data["video_note"])
        else:
            body = f"\n\n💬 {data['text']}" if data.get("text") else "\n\n(пустое сообщение)"
            await bot.send_message(user_id, header + body, parse_mode="HTML")
    except Exception as e:
        await bot.send_message(user_id, f"{header}\n\n⚠️ Ошибка отправки: {e}", parse_mode="HTML")


async def send_live_media(user_id: int, message: Message, header: str):
    try:
        msg_text = message.text or message.caption or ""
        if message.photo:
            cap = header + (f"\n\n💬 {msg_text}" if msg_text else "")
            await bot.send_photo(user_id, message.photo[-1].file_id, caption=cap, parse_mode="HTML")
        elif message.video:
            cap = header + (f"\n\n💬 {msg_text}" if msg_text else "")
            await bot.send_video(user_id, message.video.file_id, caption=cap, parse_mode="HTML")
        elif message.voice:
            await bot.send_message(user_id, header, parse_mode="HTML")
            await bot.send_voice(user_id, message.voice.file_id)
        elif message.sticker:
            await bot.send_message(user_id, header, parse_mode="HTML")
            await bot.send_sticker(user_id, message.sticker.file_id)
        elif message.document:
            cap = header + (f"\n\n💬 {msg_text}" if msg_text else "")
            await bot.send_document(user_id, message.document.file_id, caption=cap, parse_mode="HTML")
        elif message.animation:
            await bot.send_message(user_id, header, parse_mode="HTML")
            await bot.send_animation(user_id, message.animation.file_id)
        elif message.video_note:
            await bot.send_message(user_id, header, parse_mode="HTML")
            await bot.send_video_note(user_id, message.video_note.file_id)
        else:
            body = f"\n\n💬 {msg_text}" if msg_text else ""
            if body:
                await bot.send_message(user_id, header + body, parse_mode="HTML")
    except Exception as e:
        await bot.send_message(user_id, f"{header}\n\n⚠️ Ошибка: {e}", parse_mode="HTML")


@dp.business_message()
async def on_business_message(message: Message):
    if not message.business_connection_id:
        return

    conn_id = message.business_connection_id
    key = (conn_id, message.message_id)
    owner = get_owner(conn_id)
    owner_id = owner["user_id"] if owner else None
    owner_username = owner["username"] if owner else ""

    if message.from_user:
        sender_name = message.from_user.full_name
        sender_username = f"@{message.from_user.username}" if message.from_user.username else ""
        sender_id = message.from_user.id
    else:
        sender_name = "Неизвестно"
        sender_username = ""
        sender_id = None

    chat_name = message.chat.first_name or ""
    chat_uname = f" (@{message.chat.username})" if message.chat.username else ""

    fwd_info = ""
    fwd = getattr(message, 'forward_origin', None)
    if fwd:
        fwd_type = getattr(fwd, 'type', '')
        if fwd_type == 'user':
            fu = fwd.sender_user
            fn = fu.full_name if fu else "Неизвестно"
            fu_name = f" (@{fu.username})" if fu and fu.username else ""
            fwd_info = f"🔄 Переслано от: {fn}{fu_name}"
        elif fwd_type == 'hidden_user':
            fwd_info = f"🔄 Переслано от: {fwd.sender_user_name} (скрыт)"
        elif fwd_type == 'chat':
            ch = fwd.sender_chat
            fwd_info = f"🔄 Переслано из: {ch.title if ch else 'чат'}"
        elif fwd_type == 'channel':
            ch = fwd.chat
            fwd_info = f"🔄 Переслано из канала: {ch.title if ch else 'канал'}"

    cache[key] = {
        "sender_name": sender_name,
        "sender_username": sender_username,
        "sender_id": sender_id,
        "owner_id": owner_id,
        "chat_name": chat_name,
        "chat_uname": chat_uname,
        "fwd_info": fwd_info,
        "sent_at": datetime.now(),
        "text": message.text or message.caption or "",
        "photo": message.photo[-1].file_id if message.photo else None,
        "video": message.video.file_id if message.video else None,
        "voice": message.voice.file_id if message.voice else None,
        "sticker": message.sticker.file_id if message.sticker else None,
        "document": message.document.file_id if message.document else None,
        "animation": message.animation.file_id if message.animation else None,
        "video_note": message.video_note.file_id if message.video_note else None,
    }

    if owner_id == MY_USER_ID and (message.photo or message.video):
        sender = sender_name + (f" ({sender_username})" if sender_username else "")
        header = (
            f"📷 <b>Фото/видео из ЛС</b>\n"
            f"├ Чат с: <b>{chat_name}{chat_uname}</b>\n"
            f"├ От: <b>{sender}</b>\n"
            f"└ Время: <b>{fmt(datetime.now())}</b>"
        )
        await send_live_media(MY_USER_ID, message, header)
        cache[key]["media_forwarded"] = True

    if owner_username and owner_username in monitors and owner_id != MY_USER_ID:
        sender = sender_name + (f" ({sender_username})" if sender_username else "")
        owner_display = owner["user_name"] + (f" (@{owner_username})" if owner_username else "")
        fwd_line = f"\n├ <b>{fwd_info}</b>" if fwd_info else ""
        header_m = (
            f"📨 <b>Мониторинг</b>: {owner_display}\n"
            f"├ Чат с: <b>{chat_name}{chat_uname}</b>\n"
            f"├ От: <b>{sender}</b>"
            f"{fwd_line}\n"
            f"└ Время: <b>{fmt(datetime.now())}</b>"
        )
        await send_live_media(MY_USER_ID, message, header_m)


@dp.deleted_business_messages()
async def on_deleted_business(event: BusinessMessagesDeleted):
    deleted_at = fmt(datetime.now())
    conn_id = event.business_connection_id
    owner = get_owner(conn_id)
    owner_id = owner["user_id"] if owner else None

    for msg_id in event.message_ids:
        key = (conn_id, msg_id)
        data = cache.pop(key, None)

        if not data:
            if owner_id:
                await bot.send_message(
                    owner_id,
                    f"🗑 <b>Удалено сообщение</b>\n"
                    f"├ Удалено: <b>{deleted_at}</b>\n"
                    f"└ ⚠️ Содержимое не в кеше (бот не видел это сообщение)",
                    parse_mode="HTML"
                )
            continue

        sender = data["sender_name"]
        if data["sender_username"]:
            sender += f" ({data['sender_username']})"

        if data.get("sender_id") == owner_id:
            continue

        fwd_line = f"\n├ <b>{data['fwd_info']}</b>" if data.get("fwd_info") else ""

        header = (
            f"🗑 <b>Удалено сообщение</b>\n"
            f"├ От: <b>{sender}</b>"
            f"{fwd_line}\n"
            f"├ Отправлено: <b>{fmt(data['sent_at'])}</b>\n"
            f"└ Удалено: <b>{deleted_at}</b>"
        )

        if owner_id:
            if data.get("media_forwarded") and owner_id == MY_USER_ID and (data.get("photo") or data.get("video")):
                await bot.send_message(
                    MY_USER_ID,
                    f"🗑 <b>Удалено фото/видео</b>\n"
                    f"├ От: <b>{sender}</b>\n"
                    f"└ Удалено: <b>{deleted_at}</b>\n\n"
                    f"✅ Уже было переслано при получении",
                    parse_mode="HTML"
                )
            else:
                await send_media(owner_id, data, header)


@dp.message(Command("check"))
async def cmd_check(message: Message):
    if message.from_user.id != MY_USER_ID:
        return
    text = message.text or ""
    match = re.search(r'@(\w+)', text)
    if not match:
        await message.answer("📋 <code>/check @username</code>", parse_mode="HTML")
        return
    username = match.group(1).lower()
    monitors[username] = {"added_at": fmt(datetime.now())}
    save_monitors()
    await message.answer(f"✅ <b>Мониторинг @{username} включён</b>", parse_mode="HTML")


@dp.message(Command("uncheck"))
async def cmd_uncheck(message: Message):
    if message.from_user.id != MY_USER_ID:
        return
    text = message.text or ""
    match = re.search(r'@(\w+)', text)
    if not match:
        await message.answer("📋 <code>/uncheck @username</code>", parse_mode="HTML")
        return
    username = match.group(1).lower()
    if username in monitors:
        del monitors[username]
        save_monitors()
        await message.answer(f"🛑 Мониторинг @{username} отключён.", parse_mode="HTML")
    else:
        await message.answer(f"⚠️ @{username} не в списке.", parse_mode="HTML")


@dp.message(Command("monitors"))
async def cmd_monitors(message: Message):
    if message.from_user.id != MY_USER_ID:
        return
    if not monitors:
        await message.answer("Нет активных мониторингов.")
        return
    lines = ["📋 <b>Мониторинг:</b>\n"]
    for acc, info in monitors.items():
        lines.append(f"• @{acc} — с {info['added_at']}")
    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("users"))
async def cmd_users(message: Message):
    if message.from_user.id != MY_USER_ID:
        return
    if not connections:
        await message.answer("Нет активных подключений.")
        return
    lines = ["👥 <b>Подключённые:</b>\n"]
    for conn_id, info in connections.items():
        uname = f"@{info['username']}" if info['username'] else "без username"
        lines.append(f"• {info['user_name']} ({uname}) — ID: <code>{info['user_id']}</code>")
    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("start"))
async def cmd_start(message: Message):
    if message.from_user.id == MY_USER_ID:
        await message.answer(
            "👁 Бот запущен.\n\n"
            "<b>Команды:</b>\n"
            "/check @user — мониторить ЛС\n"
            "/uncheck @user — убрать\n"
            "/monitors — список\n"
            "/users — подключённые",
            parse_mode="HTML"
        )
    else:
        await message.answer(
            "👁 Бот активен.\n\n"
            "Подключи в <b>Настройки → Telegram Business → Чат-боты</b> "
            "и я буду пересылать тебе удалённые сообщения.",
            parse_mode="HTML"
        )


async def main():
    print("Бот запущен (Business API)")
    await dp.start_polling(
        bot,
        allowed_updates=[
            "message",
            "business_message",
            "deleted_business_messages",
            "business_connection",
        ]
    )


if __name__ == "__main__":
    asyncio.run(main())

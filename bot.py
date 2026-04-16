import asyncio
import hashlib
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, BusinessMessagesDeleted, BusinessConnection
)
from aiogram.filters import Command
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

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
    if not message.business_connection_id:
        return

    conn_id = message.business_connection_id

    # ─── .type команда ───────────────────────────────────────
    raw_text = message.text or ""
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
            f"└ Время: <b>{fmt(datetime.now())}</b>"
        )
        await send_live_media(owner_id, message, spoiler_header)
        cache[key]["media_forwarded"] = True

    if owner_id == MY_USER_ID and (message.photo or message.video) and not has_spoiler:
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
            f"└ Время: <b>{fmt(datetime.now())}</b>"
        )
        await send_live_media(MY_USER_ID, message, header_m)


@dp.deleted_business_messages()
async def on_deleted_business(event: BusinessMessagesDeleted):
    deleted_at = fmt(datetime.now())
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
        monitors[username] = {"added_at": fmt(datetime.now()), "excludes": []}
    else:
        monitors[username]["added_at"] = fmt(datetime.now())
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

    allowed = [
        "message",
        "business_message",
        "deleted_business_messages",
        "business_connection",
    ]

    if domain and port_str:
        # ─── Railway: webhook ─────────────────────────────
        webhook_path = "/webhook"
        webhook_url = f"https://{domain}{webhook_path}"
        secret = hashlib.sha256(BOT_TOKEN.encode()).hexdigest()[:32]

        await bot.set_webhook(
            webhook_url,
            secret_token=secret,
            allowed_updates=allowed,
        )
        print(f"Webhook: {webhook_url}")

        app = web.Application()

        async def health(request):
            return web.Response(text="OK")
        app.router.add_get("/", health)

        handler = SimpleRequestHandler(dispatcher=dp, bot=bot, secret_token=secret)
        handler.register(app, path=webhook_path)
        setup_application(app, dp, bot=bot)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", int(port_str))
        await site.start()
        print(f"Listening on :{port_str}")

        await asyncio.Event().wait()
    else:
        # ─── Локально: polling ────────────────────────────
        await bot.delete_webhook()
        print("Бот запущен (polling)")
        await dp.start_polling(bot, allowed_updates=allowed)


if __name__ == "__main__":
    asyncio.run(main())

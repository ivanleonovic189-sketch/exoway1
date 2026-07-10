import asyncio
import base64
import hashlib
import hmac
import html as html_mod
import json
import logging
import os
import random
import re
import secrets
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    BufferedInputFile, Message, BusinessMessagesDeleted, BusinessConnection
)
from aiogram.filters import Command, BaseFilter
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
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")   # если пусто — веб-админка отключена
# ─────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
MSK = timezone(timedelta(hours=3))

# ─── Премиум-эмодзи ──────────────────────────────────────────
EMOJI_WARNING_ID = "5420323339723881652"
EMOJI_EDIT_ID = "5375338737028841420"
EMOJI_TRASH_ID = "5445267414562389170"
EMOJI_EXPORT_PROGRESS_ID = "5282843764451195532"
EMOJI_EXPORT_DONE_ID = "5253742260054409879"
EMOJI_INFO_LOVE_ID = "5255861796350224063"
EMOJI_INFO_BIRTHDAY_ID = "5404431410972864937"
EMOJI_INFO_AGE_ID = "5395444514028529554"
EMOJI_INFO_WANT_ID = "5397782960512444700"
EMOJI_INFO_DISLIKE_ID = "5210952531676504517"
EMOJI_INFO_LOCATION_ID = "5416041192905265756"
EMOJI_INFO_JOB_ID = "5451882707875276247"
EMOJI_KEY_MOMENTS_ID = "5456140674028019486"


def pemoji(emoji_id: str, fallback: str) -> str:
    """Премиум tg-emoji с обычным эмодзи как фолбэк для не-Premium."""
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'


WARNING = pemoji(EMOJI_WARNING_ID, "⚠️")
EDIT_ICON = pemoji(EMOJI_EDIT_ID, "✏️")
TRASH_ICON = pemoji(EMOJI_TRASH_ID, "🗑")
EXPORT_PROGRESS = pemoji(EMOJI_EXPORT_PROGRESS_ID, "⏳")
EXPORT_DONE = pemoji(EMOJI_EXPORT_DONE_ID, "✅")
INFO_LOVE = pemoji(EMOJI_INFO_LOVE_ID, "💘")
INFO_BIRTHDAY = pemoji(EMOJI_INFO_BIRTHDAY_ID, "🎉")
INFO_AGE = pemoji(EMOJI_INFO_AGE_ID, "🎂")
INFO_WANT = pemoji(EMOJI_INFO_WANT_ID, "🎯")
INFO_DISLIKE = pemoji(EMOJI_INFO_DISLIKE_ID, "🚫")
INFO_LOCATION = pemoji(EMOJI_INFO_LOCATION_ID, "📍")
INFO_JOB = pemoji(EMOJI_INFO_JOB_ID, "💼")
KEY_MOMENTS = pemoji(EMOJI_KEY_MOMENTS_ID, "🔎")
INFO_FAMILY = "👪"
INFO_HOBBY = "🎮"
INFO_FEAR = "😨"


def quote_block(text: str, expandable: bool = True) -> str:
    """Оборачивает текст в нативную тг-цитату (blockquote)."""
    if not text:
        return ""
    escaped = html_mod.escape(text)
    attr = " expandable" if expandable else ""
    return f'\n\n<blockquote{attr}>{escaped}</blockquote>'


# ─── Массовое удаление / экспорт переписки в HTML ─────────────
BULK_DELETE_THRESHOLD = 3                    # от скольки удалённых сообщений разом считаем это "снесли всю переписку"
MAX_EMBED_BYTES = 18 * 1024 * 1024           # общий бюджет на вшиваемые медиа в одном файле (сырых байт, до base64)

MEDIA_LABELS = {
    "photo": "📷 Фото",
    "video": "🎥 Видео",
    "voice": "🎤 Голосовое",
    "sticker": "😀 Стикер",
    "document": "📄 Документ",
    "animation": "🎬 GIF",
    "video_note": "⚫ Кружочек",
}


async def _download_b64_budgeted(file_id: str, budget: list[int]) -> str | None:
    """Скачивает файл и кодирует в base64, если укладывается в оставшийся бюджет байт."""
    if budget[0] <= 0:
        return None
    try:
        file_info = await bot.get_file(file_id)
        if file_info.file_size and file_info.file_size > budget[0]:
            return None
        buf = await bot.download_file(file_info.file_path)
        raw = buf.read()
        if len(raw) > budget[0]:
            return None
        budget[0] -= len(raw)
        return base64.b64encode(raw).decode()
    except Exception as e:
        logging.warning(f"Не удалось скачать медиа {file_id}: {e}")
        return None


async def _bubble_html(msg_id: int, data: dict | None, owner_id: int | None, budget: list[int]) -> str:
    if not data:
        return f'<div class="row system"><span>Сообщение #{msg_id} — нет данных в кеше</span></div>'

    is_owner = owner_id is not None and data.get("sender_id") == owner_id
    sender = html_mod.escape(data.get("sender_name") or "Неизвестно")
    if data.get("sender_username"):
        sender += f" ({html_mod.escape(data['sender_username'])})"
    time_str = fmt(data["sent_at"])
    text = html_mod.escape(data.get("text", ""))

    media_html = ""
    if data.get("photo"):
        b64 = await _download_b64_budgeted(data["photo"], budget)
        media_html = (
            f'<img class="media-img" src="data:image/jpeg;base64,{b64}" alt="photo">'
            if b64 else '<div class="media-tag">📷 Фото</div>'
        )
    elif data.get("video"):
        b64 = await _download_b64_budgeted(data["video"], budget)
        media_html = (
            f'<video class="media-video" controls src="data:video/mp4;base64,{b64}"></video>'
            if b64 else '<div class="media-tag">🎥 Видео</div>'
        )
    elif data.get("video_note"):
        b64 = await _download_b64_budgeted(data["video_note"], budget)
        media_html = (
            f'<video class="media-video round" controls src="data:video/mp4;base64,{b64}"></video>'
            if b64 else '<div class="media-tag">⚫ Кружочек</div>'
        )
    elif data.get("voice"):
        b64 = await _download_b64_budgeted(data["voice"], budget)
        media_html = (
            f'<audio class="media-audio" controls src="data:audio/ogg;base64,{b64}"></audio>'
            if b64 else '<div class="media-tag">🎤 Голосовое</div>'
        )
    elif data.get("sticker"):
        if data.get("sticker_is_animated"):
            # .tgs (Lottie) — без JS-плеера не отрисовать, показываем статичный превью-thumbnail
            thumb = data.get("sticker_thumb")
            b64 = await _download_b64_budgeted(thumb, budget) if thumb else None
            if b64:
                media_html = (
                    f'<img class="media-img sticker" src="data:image/webp;base64,{b64}" alt="sticker">'
                    f'<div class="media-tag">✨ Анимированный стикер</div>'
                )
            else:
                media_html = '<div class="media-tag">✨ Анимированный стикер</div>'
        elif data.get("sticker_is_video"):
            b64 = await _download_b64_budgeted(data["sticker"], budget)
            media_html = (
                f'<video class="media-video sticker" controls loop muted autoplay '
                f'src="data:video/webm;base64,{b64}"></video>'
                if b64 else '<div class="media-tag">😀 Видео-стикер</div>'
            )
        else:
            b64 = await _download_b64_budgeted(data["sticker"], budget)
            media_html = (
                f'<img class="media-img sticker" src="data:image/webp;base64,{b64}" alt="sticker">'
                if b64 else '<div class="media-tag">😀 Стикер</div>'
            )
    elif data.get("document"):
        media_html = '<div class="media-tag">📄 Документ</div>'
    elif data.get("animation"):
        b64 = await _download_b64_budgeted(data["animation"], budget)
        media_html = (
            f'<video class="media-video" controls loop muted autoplay src="data:video/mp4;base64,{b64}"></video>'
            if b64 else '<div class="media-tag">🎬 GIF</div>'
        )

    text_html = f'<div class="text">{text}</div>' if text else ""
    if not text_html and not media_html:
        text_html = '<div class="text empty">(пусто)</div>'

    side = "out" if is_owner else "in"
    return (
        f'<div class="row {side}"><div class="bubble">'
        f'<div class="meta">{sender} · {time_str}</div>'
        f'{media_html}{text_html}'
        f'</div></div>'
    )


CHAT_ROWS_CSS = """
  .chat { max-width: 720px; margin: 0 auto; display: flex; flex-direction: column; gap: 8px; }
  .row { display: flex; }
  .row.in { justify-content: flex-start; }
  .row.out { justify-content: flex-end; }
  .row.system { justify-content: center; }
  .row.system span {
    background: #17212b; color: #8a97a3; font-size: 12px;
    padding: 6px 12px; border-radius: 8px;
  }
  .bubble {
    max-width: 72%; padding: 8px 12px; border-radius: 14px;
    background: #182533; word-wrap: break-word; white-space: pre-wrap;
  }
  .row.out .bubble { background: #2b5278; }
  .meta { font-size: 12px; color: #8a97a3; margin-bottom: 4px; }
  .row.out .meta { color: #a9c6e0; }
  .text { font-size: 15px; line-height: 1.4; }
  .text.empty { color: #8a97a3; font-style: italic; }
  .media-tag {
    display: inline-block; font-size: 13px; padding: 4px 8px;
    background: rgba(255,255,255,0.06); border-radius: 8px; margin-bottom: 4px;
  }
  .media-img { max-width: 100%; border-radius: 10px; display: block; margin-bottom: 4px; }
  .media-img.sticker { max-width: 160px; background: transparent; }
  .media-video { max-width: 100%; border-radius: 10px; display: block; margin-bottom: 4px; }
  .media-video.round { border-radius: 50%; max-width: 220px; aspect-ratio: 1 / 1; object-fit: cover; }
  .media-video.sticker { max-width: 160px; border-radius: 0; }
  .media-audio { width: 100%; margin-bottom: 4px; }
  @media (prefers-color-scheme: light) {
    .row.system span { background: #ffffff; }
    .bubble { background: #ffffff; }
    .row.out .bubble { background: #dcf0ff; }
    .row.out .meta { color: #4a7ba6; }
  }
"""


async def build_transcript_rows(entries: list[tuple[int, dict | None]], owner_id: int | None) -> str:
    budget = [MAX_EMBED_BYTES]
    rows = []
    for msg_id, data in entries:
        rows.append(await _bubble_html(msg_id, data, owner_id, budget))
    return "\n".join(rows)


async def build_transcript_html(chat_title: str, entries: list[tuple[int, dict | None]], owner_id: int | None) -> str:
    body = await build_transcript_rows(entries, owner_id)
    generated = fmt(datetime.now(MSK))
    title_esc = html_mod.escape(chat_title)
    return f"""<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Переписка — {title_esc}</title>
<style>
  :root {{ color-scheme: dark; }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0; padding: 24px 12px 48px;
    background: #0e1621; color: #e9edf1;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  }}
  .header {{
    max-width: 720px; margin: 0 auto 20px; padding: 16px 20px;
    background: #17212b; border-radius: 12px;
  }}
  .header h1 {{ margin: 0 0 4px; font-size: 18px; }}
  .header p {{ margin: 0; color: #8a97a3; font-size: 13px; }}
  {CHAT_ROWS_CSS}
  @media (prefers-color-scheme: light) {{
    body {{ background: #f4f4f5; color: #1a1a1a; }}
    .header {{ background: #ffffff; }}
  }}
</style>
</head>
<body>
  <div class="header">
    <h1>💬 {title_esc}</h1>
    <p>Экспортировано {generated} · сообщений: {len(entries)}</p>
  </div>
  <div class="chat">
    {body}
  </div>
</body>
</html>"""


async def send_transcript_document(target_chat_id: int, chat_title: str, entries: list[tuple[int, dict | None]],
                                    owner_id: int | None, caption: str):
    html_doc = await build_transcript_html(chat_title, entries, owner_id)
    safe_title = re.sub(r'[^0-9A-Za-zА-Яа-яЁё_-]+', '_', chat_title)[:40].strip('_') or "chat"
    filename = f"chat_{safe_title}_{datetime.now(MSK).strftime('%Y%m%d_%H%M%S')}.html"
    try:
        await bot.send_document(
            target_chat_id,
            BufferedInputFile(html_doc.encode("utf-8"), filename=filename),
            caption=caption,
            parse_mode="HTML",
        )
    except Exception as e:
        await bot.send_message(
            target_chat_id,
            f"{caption}\n\n{WARNING} Не удалось отправить файл: {html_mod.escape(str(e))}",
            parse_mode="HTML",
        )


async def send_bulk_deleted_transcript(conn_id: str, owner_id: int, message_ids: list[int], deleted_at: str):
    entries = []
    chat_title = ""
    for msg_id in sorted(message_ids):
        key = (conn_id, msg_id)
        data = cache.pop(key, None)
        if data and not chat_title:
            chat_title = (data.get("chat_name") or "") + (data.get("chat_uname") or "")
        entries.append((msg_id, data))

    chat_title = chat_title or "Переписка"
    known = sum(1 for _, d in entries if d)

    caption = (
        f"{TRASH_ICON} <b>Переписка удалена целиком</b>\n"
        f"├ Чат с: <b>{html_mod.escape(chat_title)}</b>\n"
        f"├ Сообщений: <b>{len(entries)}</b> (в кеше: {known})\n"
        f"└ Удалено: <b>{deleted_at}</b>\n\n"
        f"📎 Полная переписка сохранена во вложении"
    )
    await send_transcript_document(owner_id, chat_title, entries, owner_id, caption)

# ─── ХРАНИЛИЩА ───────────────────────────────────────────────
cache: dict[tuple, dict] = {}
connections: dict[str, dict] = {}
active_modes: dict[str, str] = {}   # conn_id -> "kawaii" | "bydlo" | "crazy"
custom_emoji_love: list[str] = []   # LoveDayEmoji
custom_emoji_mad: list[str] = []    # MadEmoji
user_numbers: dict[int, int] = {}   # user_id -> #N
user_counter: int = 0
msg_counter: int = 0

CACHE_MAX_AGE_DAYS = int(os.getenv("CACHE_MAX_AGE_DAYS", "30"))
CACHE_CLEANUP_INTERVAL_SEC = 6 * 3600  # раз в 6 часов


async def cache_cleanup_loop():
    """Фоновая авточистка: убирает из памяти сообщения старше CACHE_MAX_AGE_DAYS."""
    while True:
        await asyncio.sleep(CACHE_CLEANUP_INTERVAL_SEC)
        cutoff = datetime.now(MSK) - timedelta(days=CACHE_MAX_AGE_DAYS)
        stale_keys = [key for key, data in cache.items() if data.get("sent_at") and data["sent_at"] < cutoff]
        for key in stale_keys:
            cache.pop(key, None)
        if stale_keys:
            logging.info(f"cache_cleanup: удалено {len(stale_keys)} сообщений старше {CACHE_MAX_AGE_DAYS} дней")

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


def get_user_num(uid: int) -> int:
    global user_counter
    if uid not in user_numbers:
        user_counter += 1
        user_numbers[uid] = user_counter
    return user_numbers[uid]


def next_msg_num() -> int:
    global msg_counter
    msg_counter += 1
    return msg_counter


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
    unum = get_user_num(conn.user.id)
    connections[conn.id] = {
        "user_id": conn.user.id,
        "user_name": conn.user.full_name,
        "username": (conn.user.username or "").lower(),
        "num": unum,
    }
    logging.info(f"Business connection {conn.id} -> user #{unum} {conn.user.id} (@{conn.user.username})")


async def get_owner(conn_id: str) -> dict | None:
    if conn_id in connections:
        return connections[conn_id]
    try:
        conn = await bot.get_business_connection(conn_id)
        unum = get_user_num(conn.user.id)
        connections[conn_id] = {
            "user_id": conn.user.id,
            "user_name": conn.user.full_name,
            "username": (conn.user.username or "").lower(),
            "num": unum,
        }
        logging.info(f"Recovered connection {conn_id} -> user #{unum} {conn.user.id}")
        return connections[conn_id]
    except Exception as e:
        logging.warning(f"Failed to get connection {conn_id}: {e}")
        return None


async def send_media(user_id: int, data: dict, header: str):
    quote = quote_block(data.get("text", ""))
    try:
        if data.get("photo"):
            cap = header + quote
            await bot.send_photo(user_id, data["photo"], caption=cap, parse_mode="HTML")
        elif data.get("video"):
            cap = header + quote
            await bot.send_video(user_id, data["video"], caption=cap, parse_mode="HTML")
        elif data.get("voice"):
            await bot.send_message(user_id, header, parse_mode="HTML")
            await bot.send_voice(user_id, data["voice"])
        elif data.get("sticker"):
            await bot.send_message(user_id, header, parse_mode="HTML")
            await bot.send_sticker(user_id, data["sticker"])
        elif data.get("document"):
            cap = header + quote
            await bot.send_document(user_id, data["document"], caption=cap, parse_mode="HTML")
        elif data.get("animation"):
            await bot.send_message(user_id, header, parse_mode="HTML")
            await bot.send_animation(user_id, data["animation"])
        elif data.get("video_note"):
            await bot.send_message(user_id, header, parse_mode="HTML")
            await bot.send_video_note(user_id, data["video_note"])
        else:
            body = quote if data.get("text") else "\n\n<i>(пустое сообщение)</i>"
            await bot.send_message(user_id, header + body, parse_mode="HTML")
    except Exception as e:
        await bot.send_message(user_id, f"{header}\n\n{WARNING} Ошибка отправки: {html_mod.escape(str(e))}", parse_mode="HTML")


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
        await bot.send_message(user_id, f"{header}\n\n{WARNING} Ошибка: {html_mod.escape(str(e))}", parse_mode="HTML")


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
            
            frames = [
                "❤️",
                "   💗\n  ❤️❤️",
                "  💖💖\n 💗❤️💗\n  💖💖 ",
                """ 💕💕💕
💗❤️❤️💗
 💕💕💕""",
                
                # Большое пульсирующее сердце 1
                """      ❤️❤️❤️❤️❤️
   ❤️❤️❤️❤️❤️❤️❤️
  ❤️❤️❤️❤️❤️❤️❤️❤️
 ❤️❤️❤️❤️❤️❤️❤️❤️❤️
❤️❤️❤️❤️❤️❤️❤️❤️❤️❤️
 ❤️❤️❤️❤️❤️❤️❤️❤️❤️
  ❤️❤️❤️❤️❤️❤️❤️❤️
   ❤️❤️❤️❤️❤️❤️❤️
      ❤️❤️❤️❤️❤️""",
                
                # Пульс — ярче с 💖
                """     💖💖💖💖💖💖
   💖💗💗💗💗💗💗💖
  💖💗❤️❤️❤️❤️❤️💗💖
 💖💗❤️💖💖💖💖❤️💗💖
💖💗❤️💖💖💖💖💖❤️💗💖
 💖💗❤️💖💖💖💖❤️💗💖
  💖💗❤️❤️❤️❤️❤️💗💖
   💖💗💗💗💗💗💗💖
     💖💖💖💖💖💖""",
                
                # Пульс 2 + ✨
                """✨   💖💖💖💖💖💖   ✨
   💖💗💗💗💗💗💗💖
  💖💗❤️❤️❤️❤️❤️💗💖
 💖💗❤️💖     💖❤️💗💖
💖💗❤️💖  💗💗  💖❤️💗💖
 💖💗❤️💖     💖❤️💗💖
  💖💗❤️❤️❤️❤️❤️💗💖
   💖💗💗💗💗💗💗💖
✨   💖💖💖💖💖💖   ✨""",
                
                # Пульс 3 (чуть меньше)
                """    💗💗💗💗💗
   💖❤️❤️❤️❤️❤️💖
  💗❤️💖💖💖💖❤️💗
 💖❤️💖💖💖💖💖❤️💖
 💖❤️💖💖💖💖💖❤️💖
  💗❤️💖💖💖💖❤️💗
   💖❤️❤️❤️❤️❤️💖
    💗💗💗💗💗""",
                
                # Романтический кадр
                """       🌹 💖 🌹
     💗 Я ТЕБЯ 💗
       ❤️ ЛЮБЛЮ ❤️
     💖 ВСЕГДА 💖
       🌹 💕 🌹""",
                
                # Финальный шикарный кадр
                """✨✨✨   Я ТЕБЯ ЛЮБЛЮ   ✨✨✨
      💖💖💖💖💖💖💖💖
    💗❤️❤️❤️❤️❤️❤️❤️💗
   ❤️💖💖💖💖💖💖💖❤️
  💖❤️💖💖💖💖💖❤️💖
   ❤️💖💖💖💖💖💖💖❤️
    💗❤️❤️❤️❤️❤️❤️❤️💗
      💖💖💖💖💖💖💖💖
         💕   💕   💕"""
            ]
            
            for i, frame in enumerate(frames):
                try:
                    await bot.edit_message_text(
                        text=frame,
                        chat_id=message.chat.id,
                        message_id=message.message_id,
                        business_connection_id=conn_id,
                    )
                except Exception:
                    pass
                
                # Чуть быстрее в начале, медленнее в конце
                if i < 4:
                    await asyncio.sleep(0.25)
                else:
                    await asyncio.sleep(0.45)
            
            await asyncio.sleep(4)  # держим финальный кадр подольше
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
        "msg_num": next_msg_num(),
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
        "sticker_is_animated": bool(message.sticker.is_animated) if message.sticker else False,
        "sticker_is_video": bool(message.sticker.is_video) if message.sticker else False,
        "sticker_thumb": (
            message.sticker.thumbnail.file_id
            if message.sticker and message.sticker.thumbnail else None
        ),
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
        unum_tag = f" [юзер #{get_user_num(message.from_user.id)}]" if owner_id == MY_USER_ID and message.from_user else ""
        num_tag = f" [#{cache[key]['msg_num']}]" if owner_id == MY_USER_ID else ""
        spoiler_header = (
            f"🔥 <b>Скрытое медиа (спойлер)</b>{num_tag}"
            f"\n├ Чат с: <b>{chat_name}{chat_uname}</b>"
            f"\n├ От: <b>{sender}</b>{unum_tag}"
            f"\n└ Время: <b>{fmt(datetime.now(MSK))}</b>"
        )
        await send_live_media(owner_id, message, spoiler_header)
        cache[key]["media_forwarded"] = True

    if owner_id == MY_USER_ID and (message.photo or message.video) and not has_spoiler:
        sender = sender_name + (f" ({sender_username})" if sender_username else "")
        unum = get_user_num(message.from_user.id) if message.from_user else 0
        header = (
            f"📷 <b>Фото/видео из ЛС</b> [#{cache[key]['msg_num']}]"
            f"\n├ Чат с: <b>{chat_name}{chat_uname}</b>"
            f"\n├ От: <b>{sender}</b> [юзер #{unum}]"
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
        unum = get_user_num(message.from_user.id) if message.from_user else 0
        fwd_line = f"\n├ <b>{fwd_info}</b>" if fwd_info else ""
        reply_line = f"\n├ ↩️ Ответ на: <i>{cache[key].get('reply_text', '')}</i>" if cache[key].get('reply_text') else ""
        header_m = (
            f"📨 <b>Мониторинг</b>: {owner_display} [#{cache[key]['msg_num']}]\n"
            f"├ Чат с: <b>{chat_name}{chat_uname}</b>\n"
            f"├ От: <b>{sender}</b> [юзер #{unum}]"
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

    if not owner_id:
        logging.warning(f"deleted_business_messages: не удалось определить владельца conn={conn_id}, ids={event.message_ids}")
        if MY_USER_ID:
            await bot.send_message(
                MY_USER_ID,
                f"{WARNING} <b>Не удалось обработать удаление</b>\n"
                f"├ conn_id: <code>{html_mod.escape(conn_id or '')}</code>\n"
                f"├ Удалено сообщений: <b>{len(event.message_ids)}</b>\n"
                f"└ Причина: не резолвится владелец подключения (get_business_connection упал или соединение неизвестно)",
                parse_mode="HTML"
            )
        return

    if len(event.message_ids) >= BULK_DELETE_THRESHOLD:
        await send_bulk_deleted_transcript(conn_id, owner_id, event.message_ids, deleted_at)
        return

    for msg_id in event.message_ids:
        key = (conn_id, msg_id)
        data = cache.pop(key, None)

        if not data:
            if owner_id:
                await bot.send_message(
                    owner_id,
                    f"{TRASH_ICON} <b>Удалено сообщение</b>\n"
                    f"├ Удалено: <b>{deleted_at}</b>\n"
                    f"└ {WARNING} Содержимое не в кеше (бот не видел это сообщение)",
                    parse_mode="HTML"
                )
            continue

        msg_num = data.get("msg_num", "?")
        sender = data["sender_name"]
        if data["sender_username"]:
            sender += f" ({data['sender_username']})"

        if data.get("sender_id") == owner_id:
            continue

        fwd_line = f"\n├ <b>{data['fwd_info']}</b>" if data.get("fwd_info") else ""
        reply_line = f"\n├ ↩️ Ответ на: <i>{data['reply_text']}</i>" if data.get("reply_text") else ""
        unum_tag = f" [юзер #{get_user_num(data['sender_id'])}]" if data.get("sender_id") and owner_id == MY_USER_ID else ""
        num_tag = f" [#{msg_num}]" if owner_id == MY_USER_ID else ""

        header = (
            f"{TRASH_ICON} <b>Удалено сообщение</b>{num_tag}\n"
            f"├ От: <b>{sender}</b>{unum_tag}"
            f"{fwd_line}"
            f"{reply_line}\n"
            f"├ Отправлено: <b>{fmt(data['sent_at'])}</b>\n"
            f"└ Удалено: <b>{deleted_at}</b>"
        )

        if owner_id:
            if data.get("media_forwarded") and owner_id == MY_USER_ID and (data.get("photo") or data.get("video")):
                await bot.send_message(
                    MY_USER_ID,
                    f"{TRASH_ICON} <b>Удалено фото/видео</b>\n"
                    f"├ От: <b>{sender}</b>\n"
                    f"└ Удалено: <b>{deleted_at}</b>\n\n"
                    f"✅ Уже было переслано при получении",
                    parse_mode="HTML"
                )
            else:
                await send_media(owner_id, data, header)


@dp.edited_business_message()
async def on_edited_business_message(message: Message):
    if not message.business_connection_id:
        return
    conn_id = message.business_connection_id
    key = (conn_id, message.message_id)
    old_data = cache.get(key)
    owner = await get_owner(conn_id)
    owner_id = owner["user_id"] if owner else None

    new_text = message.text or message.caption or ""

    if message.from_user:
        sender_name = message.from_user.full_name
        sender_username = f"@{message.from_user.username}" if message.from_user.username else ""
        sender_id = message.from_user.id
    else:
        sender_name = "Неизвестно"
        sender_username = ""
        sender_id = None

    sender = sender_name + (f" ({sender_username})" if sender_username else "")
    unum = get_user_num(sender_id) if sender_id else 0
    owner_username = owner["username"] if owner else ""
    is_monitored = owner_username and owner_username in monitors and owner_id != MY_USER_ID

    if old_data:
        msg_num = old_data.get("msg_num", "?")
        old_text = old_data.get("text", "")
        old_data["text"] = new_text

        if old_text != new_text:
            # Чужое сообщение — шлём владельцу подключения (тому, кто подключил бота)
            if sender_id != owner_id and owner_id:
                chat_name = old_data.get("chat_name", "")
                chat_uname = old_data.get("chat_uname", "")
                num_tag = f" [#{msg_num}]" if owner_id == MY_USER_ID else ""
                unum_tag = f" [юзер #{unum}]" if owner_id == MY_USER_ID else ""
                await bot.send_message(
                    owner_id,
                    f"{EDIT_ICON} <b>Сообщение изменено</b>{num_tag}\n"
                    f"├ Чат с: <b>{chat_name}{chat_uname}</b>\n"
                    f"├ От: <b>{sender}</b>{unum_tag}\n"
                    f"├ Было: <i>{html_mod.escape(old_text[:200]) or '(пусто)'}</i>\n"
                    f"├ Стало: <i>{html_mod.escape(new_text[:200]) or '(пусто)'}</i>\n"
                    f"└ Время: <b>{fmt(datetime.now(MSK))}</b>",
                    parse_mode="HTML"
                )
            # Владелец сам редактирует — шлём если он в мониторинге
            elif is_monitored:
                chat_name = old_data.get("chat_name", "")
                chat_uname = old_data.get("chat_uname", "")
                owner_display = owner["user_name"] + (f" (@{owner_username})" if owner_username else "")
                await bot.send_message(
                    MY_USER_ID,
                    f"{EDIT_ICON} <b>Мониторинг — сообщение изменено</b> [#{msg_num}]\n"
                    f"├ Аккаунт: <b>{owner_display}</b>\n"
                    f"├ Чат с: <b>{chat_name}{chat_uname}</b>\n"
                    f"├ Было: <i>{html_mod.escape(old_text[:200]) or '(пусто)'}</i>\n"
                    f"├ Стало: <i>{html_mod.escape(new_text[:200]) or '(пусто)'}</i>\n"
                    f"└ Время: <b>{fmt(datetime.now(MSK))}</b>",
                    parse_mode="HTML"
                )
    else:
        # Не было в кеше — всё равно уведомим владельца подключения
        if sender_id != owner_id and owner_id:
            chat_name = message.chat.first_name or ""
            chat_uname = f" (@{message.chat.username})" if message.chat.username else ""
            unum_tag = f" [юзер #{unum}]" if owner_id == MY_USER_ID else ""
            await bot.send_message(
                owner_id,
                f"{EDIT_ICON} <b>Сообщение изменено</b>\n"
                f"├ Чат с: <b>{chat_name}{chat_uname}</b>\n"
                f"├ От: <b>{sender}</b>{unum_tag}\n"
                f"├ Новый текст: <i>{html_mod.escape(new_text[:200]) or '(пусто)'}</i>\n"
                f"└ Время: <b>{fmt(datetime.now(MSK))}</b>",
                parse_mode="HTML"
            )
        elif is_monitored:
            chat_name = message.chat.first_name or ""
            chat_uname = f" (@{message.chat.username})" if message.chat.username else ""
            owner_display = owner["user_name"] + (f" (@{owner_username})" if owner_username else "")
            await bot.send_message(
                MY_USER_ID,
                f"{EDIT_ICON} <b>Мониторинг — сообщение изменено</b>\n"
                f"├ Аккаунт: <b>{owner_display}</b>\n"
                f"├ Чат с: <b>{chat_name}{chat_uname}</b>\n"
                f"├ Новый текст: <i>{html_mod.escape(new_text[:200]) or '(пусто)'}</i>\n"
                f"└ Время: <b>{fmt(datetime.now(MSK))}</b>",
                parse_mode="HTML"
            )


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
        await message.answer(f"{WARNING} @{username} не в списке.", parse_mode="HTML")


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
        unum = info.get('num', '?')
        lines.append(f"• <b>#{unum}</b> {info['user_name']} ({uname}) — ID: <code>{info['user_id']}</code>")
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


def find_own_conversation(requester_id: int, username: str) -> tuple[list[tuple[int, dict]], str]:
    """Сообщения из СОБСТВЕННОГО бизнес-подключения запросившего с конкретным собеседником @username."""
    entries = []
    chat_title = ""
    for (conn_id, msg_id), data in cache.items():
        if data.get("owner_id") != requester_id:
            continue
        chat_uname_raw = (data.get("chat_uname") or "").strip(" ()@").lower()
        if chat_uname_raw != username:
            continue
        entries.append((msg_id, data))
        if not chat_title:
            chat_title = (data.get("chat_name") or "") + (data.get("chat_uname") or "")
    entries.sort(key=lambda item: item[1]["sent_at"])
    return entries, (chat_title or f"@{username}")


@dp.message(Command("export"))
async def cmd_export(message: Message):
    text = message.text or ""
    match = re.search(r'@(\w+)', text)
    if not match:
        await message.answer(
            "📋 <code>/export @username</code>\nСохранит переписку с этим собеседником в HTML-файл на память.",
            parse_mode="HTML"
        )
        return
    username = match.group(1).lower()

    entries, chat_title = find_own_conversation(message.from_user.id, username)
    if not entries:
        await message.answer(f"📭 Нет сообщений с @{username} в кеше.")
        return

    await message.answer(
        f"{EXPORT_PROGRESS} Готовлю переписку с @{username} ({len(entries)} сообщений)…",
        parse_mode="HTML"
    )

    caption = (
        f"{EXPORT_DONE} <b>Экспорт переписки готов</b>\n"
        f"├ Чат с: <b>{html_mod.escape(chat_title)}</b>\n"
        f"├ Сообщений: <b>{len(entries)}</b>\n"
        f"└ Сформировано: <b>{fmt(datetime.now(MSK))}</b>\n\n"
        f"📎 Полная переписка во вложении"
    )
    await send_transcript_document(message.chat.id, chat_title, entries, message.from_user.id, caption)


# ─── /info — ключевые моменты по ключевым словам ──────────────
_MONTHS_RU = r'(?:январ\w*|феврал\w*|март\w*|апрел\w*|ма[йя]\w*|июн\w*|июл\w*|август\w*|сентябр\w*|октябр\w*|ноябр\w*|декабр\w*)'

INFO_PATTERNS = [
    (f"{INFO_AGE} Возраст", [
        r'мне\s+(?:уже\s+|будет\s+|сейчас\s+)?(\d{1,3})\s*(?:лет|года|год)\b',
        r'(\d{1,3})\s*(?:лет|года|год)\s+мне\b',
        r'исполнилось\s+(\d{1,3})\s*(?:лет|года|год)?',
        r'стукну(?:ло|ет)\s+(\d{1,3})',
        r'в\s+свои\s+(\d{1,3})',
        r'возраст[:\s]+(\d{1,3})\b',
    ]),
    (f"{INFO_BIRTHDAY} Дата рождения", [
        r'(?:день\s*рождения|днюх[аи]|др)\D{0,20}(\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?)',
        r'(?:день\s*рождения|днюх[аи]|др)\D{0,20}(\d{1,2}\s+' + _MONTHS_RU + r')',
        r'родил[а]?сь?\D{0,20}(\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?)',
        r'родил[а]?сь?\D{0,20}(\d{1,2}\s+' + _MONTHS_RU + r')',
        r'родил[а]?сь?\s+в\s+(\d{4})\s*(?:году)?',
    ]),
    (f"{INFO_LOVE} Симпатия / любовь", [
        r'(?:люблю|обожаю|нравится|нравишься|симпатия\s+к|влюблен[а]?|влюбилась|влюбился|'
        r'втюрилась|втюрился|втрескал[а]?сь|запал[а]?|увлечен[а]?|неровно\s+дышу|'
        r'неравнодушн[а-я]*|схожу\s+с\s+ума\s+по|без\s+ума\s+от)\s+([^\n.!?,]{1,60})',
    ]),
    (f"{INFO_WANT} Хочет / мечтает", [
        r'(?:хочу|мечтаю|надеюсь|планирую|стремлюсь|жажду|было\s+бы\s+круто|'
        r'было\s+бы\s+здорово|я\s+бы\s+хотел[а]?|мне\s+бы\s+хотелось|спит\s+и\s+видит)\s+([^\n.!?,]{1,60})',
    ]),
    (f"{INFO_DISLIKE} Не любит / бесит", [
        r'(?:ненавиж[у]|бесит|терпеть\s+не\s+могу|не\s+люблю|раздражает|достало|'
        r'надоело|не\s+выношу|достал[а]?|злит|выводит\s+из\s+себя|тошнит\s+от)\s+([^\n.!?,]{1,60})',
    ]),
    (f"{INFO_LOCATION} Место / город", [
        r'(?:я\s+из|живу\s+в|переехал[а]?\s+в|родом\s+из|прописан[а]?\s+в|'
        r'обитаю\s+в|проживаю\s+в)\s+([^\n.,!?]{1,40})',
    ]),
    (f"{INFO_JOB} Работа / учёба", [
        r'(?:работаю|учусь|подрабатываю|устроил[а]?сь|моя\s+профессия|'
        r'моя\s+специальность|я\s+фрилансер[а-я]*)\s*(?:в|на|как)?\s*([^\n.,!?]{1,50})',
    ]),
    (f"{INFO_FAMILY} Семья / отношения", [
        r'(?:у\s+меня\s+есть\s+)?(?:муж|жена|парень|девушка)\s*[—-]?\s*([^\n.,!?]{0,40})',
        r'(?:женат|замужем|разведен[а]?|помолвлен[а]?|встречаемся)\b',
        r'(?:моя\s+мама|мой\s+папа|мои\s+родители|моя\s+сестра|мой\s+брат|мой\s+сын|моя\s+дочь)\s*([^\n.,!?]{0,40})',
    ]),
    (f"{INFO_HOBBY} Хобби / увлечения", [
        r'(?:увлекаюсь|занимаюсь|коллекционирую|моё\s+хобби|мое\s+хобби|люблю\s+играть\s+в)\s+([^\n.,!?]{1,50})',
    ]),
    (f"{INFO_FEAR} Страхи / фобии", [
        r'(?:боюсь|страшно\s+когда|моя\s+фобия|панически\s+боюсь|до\s+ужаса\s+боюсь)\s+([^\n.!?,]{1,60})',
    ]),
]


def scan_info(entries: list[tuple[int, dict]]) -> dict[str, list[tuple[str, str, str]]]:
    """Для каждой категории отдаёт (время, отправитель, ПОЛНЫЙ текст сообщения) — без дублей на одно сообщение."""
    found: dict[str, list[tuple[str, str, str]]] = {label: [] for label, _ in INFO_PATTERNS}
    seen: dict[str, set] = {label: set() for label, _ in INFO_PATTERNS}
    for msg_id, data in entries:
        text = data.get("text", "")
        if not text:
            continue
        time_str = fmt(data["sent_at"])
        sender = data.get("sender_name", "?")
        for label, patterns in INFO_PATTERNS:
            if msg_id in seen[label]:
                continue
            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    found[label].append((time_str, sender, text))
                    seen[label].add(msg_id)
                    break
    return found


@dp.message(Command("info"))
async def cmd_info(message: Message):
    text = message.text or ""
    match = re.search(r'@(\w+)', text)
    if not match:
        await message.answer(
            "📋 <code>/info @username</code>\n"
            "Поищет в переписке возраст, дату рождения, симпатии, желания и т.п. по ключевым словам.",
            parse_mode="HTML"
        )
        return
    username = match.group(1).lower()

    entries, chat_title = find_own_conversation(message.from_user.id, username)
    if not entries:
        await message.answer(f"📭 Нет сообщений с @{username} в кеше.")
        return

    found = scan_info(entries)
    lines = [f"{KEY_MOMENTS} <b>Ключевые моменты — {html_mod.escape(chat_title)}</b>"]
    total = 0
    for label, items in found.items():
        if not items:
            continue
        lines.append(f"\n<b>{label}</b>")
        for time_str, sender, full_text in items[:6]:
            total += 1
            shown = full_text[:3000] + ("…" if len(full_text) > 3000 else "")
            lines.append(f"· {time_str} — <b>{html_mod.escape(sender)}</b>{quote_block(shown)}")

    if total == 0:
        await message.answer(f"🤷 По @{username} ничего не нашлось — совпадений по ключевым словам нет.")
        return

    lines.append(f"\n{WARNING} Это просто поиск по ключевым словам в тексте, не реальный анализ — проверяйте сами.")

    chunks = []
    current = ""
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
        await message.answer(f"{WARNING} @{username} не в мониторинге.", parse_mode="HTML")
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
        await message.answer(f"{WARNING} @{username} не в мониторинге.", parse_mode="HTML")
        return
    excludes = monitors[username].get("excludes", [])
    if chat_incl in excludes:
        excludes.remove(chat_incl)
        save_monitors()
        await message.answer(f"✅ Чат @{chat_incl} снова мониторится для @{username}", parse_mode="HTML")
    else:
        await message.answer(f"{WARNING} @{chat_incl} не в исключениях @{username}", parse_mode="HTML")


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
        f"RENDER_EXTERNAL_HOSTNAME: <code>{os.getenv('RENDER_EXTERNAL_HOSTNAME', '(не задан)')}</code>",
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
        domain = os.getenv("RAILWAY_PUBLIC_DOMAIN") or os.getenv("RENDER_EXTERNAL_HOSTNAME") or ""
        admin_line = f"\n\n🌐 Веб-админка: https://{domain}/admin" if domain and ADMIN_PASSWORD else ""
        await message.answer(
            "👁 Бот запущен.\n\n"
            "<b>Команды:</b>\n"
            "/check @user — мониторить ЛС\n"
            "/uncheck @user — убрать\n"
            "/exclude @user @chat — исключить чат\n"
            "/include @user @chat — вернуть чат\n"
            "/last @user 10 — последние сообщения\n"
            "/export @user — вся переписка в HTML-файл\n"
            "/monitors — список\n"
            "/users — подключённые"
            f"{admin_line}",
            parse_mode="HTML"
        )
    else:
        await message.answer(
            "👁 Бот активен.\n\n"
            "Подключи в <b>Настройки → Telegram Business → Чат-боты</b> "
            "и я буду пересылать тебе удалённые и изменённые сообщения.\n\n"
            "<b>Команды:</b>\n"
            "/export @user — сохранить переписку с человеком в HTML-файл на память\n"
            "/info @user — найти в переписке ключевые моменты (возраст, симпатии, желания и т.п.)",
            parse_mode="HTML"
        )


# ─── Веб-админка ───────────────────────────────────────────────
ADMIN_COOKIE_NAME = "exoway_admin"
ADMIN_SESSION_TOKEN = secrets.token_hex(32)   # генерируется заново при каждом запуске процесса

ADMIN_CSS = """
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { margin:0; padding: 24px 16px 48px; background:#0e1621; color:#e9edf1;
         font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
  a { color:#6ab0f3; text-decoration: none; }
  a:hover { text-decoration: underline; }
  .top { max-width:900px; margin: 0 auto 16px; display:flex; justify-content:space-between; align-items:center; }
  .card { max-width: 900px; margin: 0 auto 16px; background:#17212b; border-radius:12px; padding:16px 20px; }
  h1 { font-size: 20px; margin: 0 0 8px; }
  h2 { font-size: 16px; margin: 0 0 12px; }
  table { width:100%; border-collapse: collapse; font-size: 14px; }
  th, td { text-align:left; padding: 6px 10px; border-bottom: 1px solid #223042; }
  input, button { background:#182533; color:#e9edf1; border:1px solid #223042; border-radius:8px;
                  padding:8px 10px; font-size:14px; }
  button { cursor:pointer; background:#2b5278; border:none; }
  form { display:flex; gap:8px; flex-wrap:wrap; align-items:center; margin: 8px 0; }
  .muted { color:#8a97a3; font-size: 13px; }
  @media (prefers-color-scheme: light) {
    body { background:#f4f4f5; color:#1a1a1a; }
    .card, .top { background: transparent; }
    .card { background:#ffffff; }
    th, td { border-bottom: 1px solid #e2e2e2; }
    input, button { background:#f0f0f0; color:#1a1a1a; border:1px solid #ddd; }
  }
"""


def _admin_page(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html_mod.escape(title)}</title>
<style>{ADMIN_CSS}</style></head><body>
<div class="top"><div><b>👁 Exoway admin</b></div><div><a href="/admin/logout">Выйти</a></div></div>
{body}
</body></html>"""


def _admin_login_page(error: str = "") -> str:
    err = f'<p style="color:#e57373">{html_mod.escape(error)}</p>' if error else ""
    return f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Вход — Exoway admin</title>
<style>
  body {{ margin:0; min-height:100vh; display:flex; align-items:center; justify-content:center;
         background:#0e1621; color:#e9edf1; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }}
  form {{ background:#17212b; padding:24px 28px; border-radius:12px; width:280px; }}
  input {{ width:100%; padding:10px; margin:8px 0; border-radius:8px; border:1px solid #223042;
           background:#182533; color:#e9edf1; box-sizing:border-box; }}
  button {{ width:100%; padding:10px; border-radius:8px; border:none; background:#2b5278; color:#fff; cursor:pointer; }}
</style></head><body>
<form method="post" action="/admin/login">
  <h2>👁 Вход в админку</h2>
  {err}
  <input type="password" name="password" placeholder="Пароль" autofocus>
  <button type="submit">Войти</button>
</form>
</body></html>"""


def _is_authed(request: web.Request) -> bool:
    if not ADMIN_PASSWORD:
        return False
    cookie = request.cookies.get(ADMIN_COOKIE_NAME, "")
    return hmac.compare_digest(cookie, ADMIN_SESSION_TOKEN)


async def admin_login_get(request: web.Request):
    if not ADMIN_PASSWORD:
        return web.Response(text="Веб-админка отключена (не задан ADMIN_PASSWORD).", status=503)
    if _is_authed(request):
        return web.HTTPFound("/admin")
    return web.Response(text=_admin_login_page(), content_type="text/html")


async def admin_login_post(request: web.Request):
    if not ADMIN_PASSWORD:
        return web.Response(text="Веб-админка отключена.", status=503)
    data = await request.post()
    password = data.get("password", "")
    if hmac.compare_digest(password, ADMIN_PASSWORD):
        resp = web.HTTPFound("/admin")
        resp.set_cookie(
            ADMIN_COOKIE_NAME, ADMIN_SESSION_TOKEN,
            httponly=True, samesite="Strict", secure=(request.scheme == "https"),
            max_age=7 * 24 * 3600,
        )
        return resp
    return web.Response(text=_admin_login_page("Неверный пароль"), content_type="text/html", status=401)


async def admin_logout(request: web.Request):
    resp = web.HTTPFound("/admin/login")
    resp.del_cookie(ADMIN_COOKIE_NAME)
    return resp


async def admin_dashboard(request: web.Request):
    if not _is_authed(request):
        return web.HTTPFound("/admin/login")

    rows = []
    for conn_id, info in connections.items():
        uname = f"@{info['username']}" if info['username'] else "—"
        count = sum(1 for (cid, _mid) in cache if cid == conn_id)
        rows.append(
            f"<tr><td>#{info.get('num', '?')}</td><td>{html_mod.escape(info['user_name'])}</td>"
            f"<td>{html_mod.escape(uname)}</td><td><code>{info['user_id']}</code></td>"
            f"<td>{count}</td>"
            f'<td><a href="/admin/connection/{conn_id}">Открыть</a></td></tr>'
        )
    conn_table = (
        "<table><tr><th>#</th><th>Имя</th><th>Username</th><th>ID</th><th>Сообщений</th><th></th></tr>"
        + "".join(rows) + "</table>"
    ) if rows else '<p class="muted">Нет подключений.</p>'

    mon_rows = []
    for acc, info in monitors.items():
        excl = ", ".join(f"@{e}" for e in info.get("excludes", [])) or "—"
        mon_rows.append(
            f"<tr><td>@{html_mod.escape(acc)}</td><td>{info.get('added_at', '?')}</td><td>{excl}</td>"
            f'<td><form method="post" action="/admin/monitors/remove/{acc}">'
            f'<button type="submit">Убрать</button></form></td></tr>'
        )
    mon_table = (
        "<table><tr><th>Аккаунт</th><th>С</th><th>Исключения</th><th></th></tr>" + "".join(mon_rows) + "</table>"
    ) if mon_rows else '<p class="muted">Мониторинг не настроен.</p>'

    body = f"""
    <div class="card"><h1>📊 Статус</h1>
      <p class="muted">Подключений: {len(connections)} · Сообщений в кеше: {len(cache)} · Мониторингов: {len(monitors)}</p>
      <p><a href="/admin/search">🔍 Поиск по всем чатам</a> · <a href="/admin/stats">📈 Статистика</a></p>
    </div>
    <div class="card"><h2>👥 Подключения</h2>{conn_table}</div>
    <div class="card"><h2>📋 Мониторинг</h2>{mon_table}
      <form method="post" action="/admin/monitors/add">
        <input name="username" placeholder="username без @">
        <button type="submit">Добавить</button>
      </form>
    </div>
    """
    return web.Response(text=_admin_page("Exoway — admin", body), content_type="text/html")


async def admin_connection(request: web.Request):
    if not _is_authed(request):
        return web.HTTPFound("/admin/login")
    conn_id = request.match_info["conn_id"]
    owner = connections.get(conn_id)
    if not owner:
        return web.Response(text="Подключение не найдено", status=404)

    chats: dict[str, dict] = {}
    for (cid, msg_id), data in cache.items():
        if cid != conn_id:
            continue
        chat_uname_raw = (data.get("chat_uname") or "").strip(" ()@").lower()
        key = chat_uname_raw or f"noname_{data.get('chat_name', '')}"
        entry = chats.setdefault(key, {
            "title": (data.get("chat_name") or "") + (data.get("chat_uname") or ""),
            "count": 0,
            "uname": chat_uname_raw,
        })
        entry["count"] += 1

    rows = []
    for key, info in sorted(chats.items(), key=lambda x: -x[1]["count"]):
        if info["uname"]:
            link = (
                f'<a href="/admin/connection/{conn_id}/view?chat={info["uname"]}">Просмотр</a> · '
                f'<a href="/admin/connection/{conn_id}/info?chat={info["uname"]}">Инфо</a> · '
                f'<a href="/admin/connection/{conn_id}/export?chat={info["uname"]}">Скачать HTML</a>'
            )
        else:
            link = '<span class="muted">нет username, недоступно</span>'
        rows.append(f"<tr><td>{html_mod.escape(info['title'])}</td><td>{info['count']}</td><td>{link}</td></tr>")

    table = (
        "<table><tr><th>Собеседник</th><th>Сообщений</th><th></th></tr>" + "".join(rows) + "</table>"
    ) if rows else '<p class="muted">В кеше пока ничего нет для этого подключения.</p>'

    body = f"""
    <p><a href="/admin">← Назад</a></p>
    <div class="card">
      <h1>{html_mod.escape(owner['user_name'])} {f"(@{html_mod.escape(owner['username'])})" if owner['username'] else ""}</h1>
      <p class="muted">ID: <code>{owner['user_id']}</code> · #{owner.get('num', '?')}</p>
      {table}
    </div>
    """
    return web.Response(text=_admin_page(f"{owner['user_name']} — Exoway admin", body), content_type="text/html")


def _conn_chat_entries(conn_id: str, username: str) -> tuple[list[tuple[int, dict]], str]:
    entries = []
    chat_title = ""
    for (cid, msg_id), data in cache.items():
        if cid != conn_id:
            continue
        chat_uname_raw = (data.get("chat_uname") or "").strip(" ()@").lower()
        if chat_uname_raw != username:
            continue
        entries.append((msg_id, data))
        if not chat_title:
            chat_title = (data.get("chat_name") or "") + (data.get("chat_uname") or "")
    entries.sort(key=lambda item: item[1]["sent_at"])
    return entries, (chat_title or f"@{username}")


async def admin_export(request: web.Request):
    if not _is_authed(request):
        return web.HTTPFound("/admin/login")
    conn_id = request.match_info["conn_id"]
    owner = connections.get(conn_id)
    if not owner:
        return web.Response(text="Подключение не найдено", status=404)
    username = request.query.get("chat", "").strip(" @").lower()
    if not username:
        return web.Response(text="Не указан собеседник (?chat=username)", status=400)

    entries, chat_title = _conn_chat_entries(conn_id, username)
    if not entries:
        return web.Response(text="Нет сообщений с этим собеседником в кеше", status=404)

    html_doc = await build_transcript_html(chat_title, entries, owner["user_id"])
    return web.Response(
        text=html_doc,
        content_type="text/html",
        headers={"Content-Disposition": f'inline; filename="chat_{username}.html"'},
    )


CHAT_PAGE_SIZE = 150


async def admin_view(request: web.Request):
    if not _is_authed(request):
        return web.HTTPFound("/admin/login")
    conn_id = request.match_info["conn_id"]
    owner = connections.get(conn_id)
    if not owner:
        return web.Response(text="Подключение не найдено", status=404)
    username = request.query.get("chat", "").strip(" @").lower()
    if not username:
        return web.Response(text="Не указан собеседник (?chat=username)", status=400)

    entries, chat_title = _conn_chat_entries(conn_id, username)
    if not entries:
        return web.Response(text="Нет сообщений с этим собеседником в кеше", status=404)

    total_pages = max(1, (len(entries) + CHAT_PAGE_SIZE - 1) // CHAT_PAGE_SIZE)
    page_param = request.query.get("page")
    page = int(page_param) if page_param and page_param.isdigit() else total_pages
    page = max(1, min(page, total_pages))
    start = (page - 1) * CHAT_PAGE_SIZE
    page_entries = entries[start:start + CHAT_PAGE_SIZE]

    rows = await build_transcript_rows(page_entries, owner["user_id"])

    nav = []
    if page > 1:
        nav.append(f'<a href="/admin/connection/{conn_id}/view?chat={username}&page={page - 1}">← Раньше</a>')
    else:
        nav.append('<span></span>')
    nav.append(f'<span class="muted">Страница {page} из {total_pages} · сообщений: {len(entries)}</span>')
    if page < total_pages:
        nav.append(f'<a href="/admin/connection/{conn_id}/view?chat={username}&page={page + 1}">Позже →</a>')
    else:
        nav.append('<span></span>')

    body = f"""
    <p><a href="/admin/connection/{conn_id}">← Назад</a></p>
    <style>{CHAT_ROWS_CSS}</style>
    <div class="card">
      <h1>💬 {html_mod.escape(chat_title)}</h1>
      <div style="display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap; margin-bottom:12px;">{"".join(nav)}</div>
      <div class="chat">{rows}</div>
    </div>
    """
    return web.Response(text=_admin_page(f"{chat_title} — Exoway admin", body), content_type="text/html")


async def admin_info_view(request: web.Request):
    if not _is_authed(request):
        return web.HTTPFound("/admin/login")
    conn_id = request.match_info["conn_id"]
    owner = connections.get(conn_id)
    if not owner:
        return web.Response(text="Подключение не найдено", status=404)
    username = request.query.get("chat", "").strip(" @").lower()
    if not username:
        return web.Response(text="Не указан собеседник (?chat=username)", status=400)

    entries, chat_title = _conn_chat_entries(conn_id, username)
    if not entries:
        return web.Response(text="Нет сообщений с этим собеседником в кеше", status=404)

    found = scan_info(entries)
    sections = []
    total = 0
    for label, items in found.items():
        if not items:
            continue
        item_rows = []
        for time_str, sender, full_text in items[:10]:
            total += 1
            shown = full_text[:3000] + ("…" if len(full_text) > 3000 else "")
            item_rows.append(
                f'<div style="margin:10px 0;"><div class="muted">{time_str} — <b>{html_mod.escape(sender)}</b></div>'
                f'<blockquote style="margin:4px 0 0; padding:8px 12px; background:rgba(255,255,255,0.05); '
                f'border-left:3px solid #2b5278; border-radius:6px; white-space:pre-wrap;">{html_mod.escape(shown)}</blockquote></div>'
            )
        sections.append(f'<div class="card"><h2>{label}</h2>{"".join(item_rows)}</div>')

    sections_html = "".join(sections) if total else '<div class="card"><p class="muted">Совпадений по ключевым словам нет.</p></div>'

    body = f"""
    <p><a href="/admin/connection/{conn_id}">← Назад</a></p>
    <div class="card"><h1>{KEY_MOMENTS} Ключевые моменты — {html_mod.escape(chat_title)}</h1>
      <p class="muted">{WARNING} Поиск по ключевым словам в тексте, не реальный анализ.</p>
    </div>
    {sections_html}
    """
    return web.Response(text=_admin_page(f"Info {chat_title} — Exoway admin", body), content_type="text/html")


async def admin_search(request: web.Request):
    if not _is_authed(request):
        return web.HTTPFound("/admin/login")
    query = request.query.get("q", "").strip()
    results = []
    if query:
        q_lower = query.lower()
        for (conn_id, msg_id), data in cache.items():
            text = data.get("text", "")
            if text and q_lower in text.lower():
                owner = connections.get(conn_id)
                results.append((data["sent_at"], conn_id, owner, data))
        results.sort(key=lambda x: x[0], reverse=True)

    rows = []
    for sent_at, conn_id, owner, data in results[:200]:
        chat = (data.get("chat_name") or "") + (data.get("chat_uname") or "")
        sender = data.get("sender_name", "?")
        owner_label = owner["user_name"] if owner else "?"
        snippet = data.get("text", "")
        idx = snippet.lower().find(query.lower())
        start = max(0, idx - 40)
        excerpt = ("…" if start > 0 else "") + snippet[start:start + 120] + ("…" if start + 120 < len(snippet) else "")
        rows.append(
            f"<tr><td>{fmt(sent_at)}</td><td>{html_mod.escape(owner_label)}</td>"
            f"<td>{html_mod.escape(chat)}</td><td>{html_mod.escape(sender)}</td>"
            f"<td>{html_mod.escape(excerpt)}</td>"
            f'<td><a href="/admin/connection/{conn_id}">Открыть</a></td></tr>'
        )
    table = (
        "<table><tr><th>Время</th><th>Аккаунт</th><th>Чат</th><th>Отправитель</th><th>Текст</th><th></th></tr>"
        + "".join(rows) + "</table>"
    ) if rows else '<p class="muted">Ничего не найдено.</p>'

    body = f"""
    <p><a href="/admin">← Назад</a></p>
    <div class="card"><h1>🔍 Поиск по всем чатам</h1>
      <form method="get" action="/admin/search">
        <input name="q" value="{html_mod.escape(query)}" placeholder="текст для поиска" style="min-width:240px">
        <button type="submit">Искать</button>
      </form>
      <p class="muted">{f"Найдено: {len(results)}" if query else "Введите текст, чтобы искать по всем подключениям сразу"}</p>
      {table}
    </div>
    """
    return web.Response(text=_admin_page("Поиск — Exoway admin", body), content_type="text/html")


async def admin_stats(request: web.Request):
    if not _is_authed(request):
        return web.HTTPFound("/admin/login")

    now = datetime.now(MSK)
    days = [(now - timedelta(days=i)).date() for i in range(13, -1, -1)]
    per_day = Counter()
    top_chats = Counter()
    for data in cache.values():
        sent_at = data.get("sent_at")
        if sent_at:
            per_day[sent_at.date()] += 1
        chat = (data.get("chat_name") or "") + (data.get("chat_uname") or "")
        if chat:
            top_chats[chat] += 1

    max_count = max((per_day.get(d, 0) for d in days), default=0) or 1
    bars = []
    for d in days:
        count = per_day.get(d, 0)
        height = int(4 + (count / max_count) * 120)
        bars.append(
            f'<div style="display:flex; flex-direction:column; align-items:center; gap:4px; flex:1;">'
            f'<div title="{count}" style="width:100%; max-width:28px; height:{height}px; '
            f'background:#2b5278; border-radius:4px 4px 0 0;"></div>'
            f'<div class="muted" style="font-size:10px;">{d.strftime("%d.%m")}</div>'
            f'</div>'
        )
    chart = f'<div style="display:flex; align-items:flex-end; gap:6px; height:150px;">{"".join(bars)}</div>'

    top_rows = "".join(
        f"<tr><td>{html_mod.escape(chat)}</td><td>{count}</td></tr>"
        for chat, count in top_chats.most_common(10)
    )
    top_table = (
        f"<table><tr><th>Собеседник</th><th>Сообщений</th></tr>{top_rows}</table>"
        if top_rows else '<p class="muted">Пусто.</p>'
    )

    body = f"""
    <p><a href="/admin">← Назад</a></p>
    <div class="card"><h1>📈 Статистика</h1>
      <p class="muted">Сообщений в кеше: {len(cache)} · авточистка старше {CACHE_MAX_AGE_DAYS} дн.</p>
    </div>
    <div class="card"><h2>Сообщений по дням (14 дней)</h2>{chart}</div>
    <div class="card"><h2>Топ собеседников</h2>{top_table}</div>
    """
    return web.Response(text=_admin_page("Статистика — Exoway admin", body), content_type="text/html")


async def admin_monitor_add(request: web.Request):
    if not _is_authed(request):
        return web.HTTPFound("/admin/login")
    data = await request.post()
    username = data.get("username", "").strip(" @").lower()
    if username:
        if username not in monitors:
            monitors[username] = {"added_at": fmt(datetime.now(MSK)), "excludes": []}
        else:
            monitors[username]["added_at"] = fmt(datetime.now(MSK))
        save_monitors()
    return web.HTTPFound("/admin")


async def admin_monitor_remove(request: web.Request):
    if not _is_authed(request):
        return web.HTTPFound("/admin/login")
    username = request.match_info["username"]
    if username in monitors:
        del monitors[username]
        save_monitors()
    return web.HTTPFound("/admin")


async def main():
    domain = (
        os.getenv("RAILWAY_PUBLIC_DOMAIN")
        or os.getenv("RENDER_EXTERNAL_HOSTNAME")
        or ""
    )
    port_str = os.getenv("PORT", "")
    port = int(port_str) if port_str else 0

    allowed = [
        "message",
        "business_message",
        "edited_business_message",
        "deleted_business_messages",
        "business_connection",
    ]

    await bot.delete_webhook()
    asyncio.create_task(cache_cleanup_loop())

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
    app.router.add_get("/admin", admin_dashboard)
    app.router.add_get("/admin/login", admin_login_get)
    app.router.add_post("/admin/login", admin_login_post)
    app.router.add_get("/admin/logout", admin_logout)
    app.router.add_get("/admin/connection/{conn_id}", admin_connection)
    app.router.add_get("/admin/connection/{conn_id}/export", admin_export)
    app.router.add_get("/admin/connection/{conn_id}/view", admin_view)
    app.router.add_get("/admin/connection/{conn_id}/info", admin_info_view)
    app.router.add_get("/admin/search", admin_search)
    app.router.add_get("/admin/stats", admin_stats)
    app.router.add_post("/admin/monitors/add", admin_monitor_add)
    app.router.add_post("/admin/monitors/remove/{username}", admin_monitor_remove)

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

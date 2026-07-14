import asyncio
import base64
import hashlib
import html as html_mod
import json
import logging
import os
import random
import re
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    BufferedInputFile, Message, BusinessMessagesDeleted, BusinessConnection,
    ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery,
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
EMOJI_INFO_JOB_ID = "5445221832074483553"
EMOJI_KEY_MOMENTS_ID = "5456140674028019486"
EMOJI_INFO_FAMILY_ID = "5235470249406512502"
EMOJI_INFO_HOBBY_ID = "5197371802136892976"
EMOJI_INFO_FEAR_ID = "5330403668191618210"


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
INFO_FAMILY = pemoji(EMOJI_INFO_FAMILY_ID, "👪")
INFO_HOBBY = pemoji(EMOJI_INFO_HOBBY_ID, "🎮")
INFO_FEAR = pemoji(EMOJI_INFO_FEAR_ID, "😨")


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

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
REDIS_URL = os.getenv("REDIS_URL", "")
redis_client = None
if REDIS_URL:
    import redis as _redis
    redis_client = _redis.from_url(REDIS_URL, decode_responses=True)
    try:
        redis_client.ping()
        logging.info("storage_backend: Redis (данные переживут перезапуск контейнера)")
    except Exception as e:
        logging.error(f"storage_backend: не удалось подключиться к Redis ({e}), падаю обратно на локальные JSON-файлы")
        redis_client = None
else:
    logging.warning("storage_backend: REDIS_URL не задан, используются локальные JSON-файлы (данные будут теряться при пересоздании контейнера)")


def _load_store(key: str, filename: str, default):
    """Читает JSON-блоб из Redis (если настроен REDIS_URL), иначе из локального файла."""
    if redis_client:
        raw = redis_client.get(key)
        if raw is None:
            return default
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return default
    try:
        with open(os.path.join(DATA_DIR, filename), "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def _save_store(key: str, filename: str, value) -> None:
    if redis_client:
        redis_client.set(key, json.dumps(value, ensure_ascii=False))
        return
    with open(os.path.join(DATA_DIR, filename), "w", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False, indent=2)


monitors: dict[str, dict] = {}


def load_monitors():
    global monitors
    monitors = _load_store("monitors", "monitors.json", {})


def save_monitors():
    _save_store("monitors", "monitors.json", monitors)


load_monitors()

reminders: list[dict] = []
reminder_counter: int = 0


def load_reminders():
    global reminders, reminder_counter
    reminders = _load_store("reminders", "reminders.json", [])
    reminder_counter = max((r["id"] for r in reminders), default=0)


def save_reminders():
    _save_store("reminders", "reminders.json", reminders)


def next_reminder_id() -> int:
    global reminder_counter
    reminder_counter += 1
    return reminder_counter


load_reminders()

DIGEST_HOUR_MSK = int(os.getenv("DIGEST_HOUR_MSK", "21"))  # во сколько слать ежедневную сводку, по МСК

digest_disabled: set[int] = set()


def load_digest_disabled():
    global digest_disabled
    digest_disabled = set(_load_store("digest_disabled", "digest_disabled.json", []))


def save_digest_disabled():
    _save_store("digest_disabled", "digest_disabled.json", list(digest_disabled))


load_digest_disabled()

# Кто уже писал раньше (по conn_id) — чтобы "новые контакты" в сводке не считались
# заново после каждого перезапуска контейнера (in-memory cache для этого не годится).
known_senders: dict[str, list[int]] = {}


def load_known_senders():
    global known_senders
    known_senders = _load_store("known_senders", "known_senders.json", {})


def save_known_senders():
    _save_store("known_senders", "known_senders.json", known_senders)


load_known_senders()

# "Ключевые моменты" для /info (сообщения, совпавшие с INFO_PATTERNS) — хранится отдельно
# от cache и переживает рестарты, в отличие от полной переписки (та остаётся только в памяти).
# Ключ: "{owner_id}:{username собеседника}".
info_history: dict[str, list[dict]] = {}


def load_info_history():
    global info_history
    info_history = _load_store("info_history", "info_history.json", {})


def save_info_history():
    _save_store("info_history", "info_history.json", info_history)


load_info_history()

def load_active_modes():
    global active_modes
    active_modes = _load_store("active_modes", "active_modes.json", {})


def save_active_modes():
    _save_store("active_modes", "active_modes.json", active_modes)


load_active_modes()

muted_connections: set[str] = set()


def load_muted_connections():
    global muted_connections
    muted_connections = set(_load_store("muted_connections", "muted_connections.json", []))


def save_muted_connections():
    _save_store("muted_connections", "muted_connections.json", list(muted_connections))


load_muted_connections()

# Сообщения, удалённые самим ботом через .mute — чтобы on_deleted_business
# не слал владельцу уведомление об их удалении. In-memory: живёт секунды,
# ровно до прихода вебхука об удалении, персистентность не нужна.
muted_deleted_ids: set[tuple[str, int]] = set()


def fmt(dt: datetime) -> str:
    return dt.astimezone(MSK).strftime("%d.%m.%Y %H:%M:%S")


# ─── Парсер времени для /remind (всё по МСК) ──────────────────
_WEEKDAYS_RU = {
    "понедельник": 0, "вторник": 1, "среду": 2, "четверг": 3,
    "пятницу": 4, "субботу": 5, "воскресенье": 6,
}


def parse_remind_time(text: str, now: datetime) -> tuple[datetime | None, str]:
    """Разбирает ведущее время в тексте (по МСК). Возвращает (due_at, остаток_текста) или (None, text)."""
    text = text.strip()

    m = re.match(r'^через\s+(\d+)\s*(минут\w*|мин\.?|час(?:а|ов)?|недел\w*|день|дн\w*)\s+(.*)$', text, re.IGNORECASE)
    if m:
        amount = int(m.group(1))
        unit = m.group(2).lower()
        rest = m.group(3)
        if unit.startswith("мин"):
            delta = timedelta(minutes=amount)
        elif unit.startswith("час"):
            delta = timedelta(hours=amount)
        elif unit.startswith("недел"):
            delta = timedelta(weeks=amount)
        else:
            delta = timedelta(days=amount)
        return now + delta, rest

    m = re.match(r'^(сегодня|завтра|послезавтра)\s+(?:в\s+)?(\d{1,2})[:.](\d{2})\s+(.*)$', text, re.IGNORECASE)
    if m:
        day_word, hh, mm, rest = m.group(1).lower(), int(m.group(2)), int(m.group(3)), m.group(4)
        offset = {"сегодня": 0, "завтра": 1, "послезавтра": 2}[day_word]
        try:
            target_date = (now + timedelta(days=offset)).date()
            due = datetime.combine(target_date, datetime.min.time()).replace(hour=hh, minute=mm, tzinfo=MSK)
        except ValueError:
            return None, text
        return due, rest

    m = re.match(
        r'^в\s+(понедельник|вторник|среду|четверг|пятницу|субботу|воскресенье)'
        r'(?:\s+(?:в\s+)?(\d{1,2})[:.](\d{2}))?\s+(.*)$',
        text, re.IGNORECASE
    )
    if m:
        weekday_name, hh, mm, rest = m.group(1).lower(), m.group(2), m.group(3), m.group(4)
        target_wd = _WEEKDAYS_RU[weekday_name]
        days_ahead = (target_wd - now.weekday()) % 7
        if days_ahead == 0:
            days_ahead = 7
        try:
            target_date = (now + timedelta(days=days_ahead)).date()
            hour = int(hh) if hh else 9
            minute = int(mm) if mm else 0
            due = datetime.combine(target_date, datetime.min.time()).replace(hour=hour, minute=minute, tzinfo=MSK)
        except ValueError:
            return None, text
        return due, rest

    m = re.match(r'^в\s+(\d{1,2})[:.](\d{2})\s+(.*)$', text, re.IGNORECASE)
    if m:
        hh, mm, rest = int(m.group(1)), int(m.group(2)), m.group(3)
        try:
            due = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        except ValueError:
            return None, text
        if due <= now:
            due += timedelta(days=1)
        return due, rest

    # голое "HH:MM текст" без предлога "в"
    m = re.match(r'^(\d{1,2})[:.](\d{2})\s+(.*)$', text)
    if m:
        hh, mm, rest = int(m.group(1)), int(m.group(2)), m.group(3)
        try:
            due = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        except ValueError:
            return None, text
        if due <= now:
            due += timedelta(days=1)
        return due, rest

    m = re.match(r'^(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?\s+(?:в\s+)?(\d{1,2})[:.](\d{2})\s+(.*)$', text)
    if m:
        day, month, year, hh, mm, rest = m.groups()
        year = int(year) if year else now.year
        if year < 100:
            year += 2000
        try:
            due = datetime(year, int(month), int(day), int(hh), int(mm), tzinfo=MSK)
        except ValueError:
            return None, text
        if due <= now:
            due = due.replace(year=due.year + 1)
        return due, rest

    m = re.match(r'^(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?\s+(.*)$', text)
    if m:
        day, month, year, rest = m.groups()
        year = int(year) if year else now.year
        if year < 100:
            year += 2000
        try:
            due = datetime(year, int(month), int(day), 9, 0, tzinfo=MSK)
        except ValueError:
            return None, text
        if due <= now:
            due = due.replace(year=due.year + 1)
        return due, rest

    return None, text


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

    # ─── .mute — молча удаляет входящие от собеседника ────
    if conn_id in muted_connections and message.from_user:
        owner_mute = await get_owner(conn_id)
        if owner_mute and message.from_user.id != owner_mute["user_id"]:
            try:
                await bot.delete_business_messages(
                    business_connection_id=conn_id,
                    message_ids=[message.message_id],
                )
                muted_deleted_ids.add((conn_id, message.message_id))
            except Exception as e:
                logging.warning(f".mute: не удалось удалить сообщение: {e}")
            return

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

    # ─── .mute / .unmute ──────────────────────────────────
    cmd_lower = raw_text.lower().strip()
    if cmd_lower in (".mute", ".unmute"):
        owner = await get_owner(conn_id)
        if owner and message.from_user and message.from_user.id == owner["user_id"]:
            if cmd_lower == ".mute":
                muted_connections.add(conn_id)
                status_text = "shut up!"
            else:
                muted_connections.discard(conn_id)
                status_text = "🔊 Мут отключён"
            save_muted_connections()
            try:
                await bot.edit_message_text(
                    text=status_text,
                    chat_id=message.chat.id,
                    message_id=message.message_id,
                    business_connection_id=conn_id,
                )
            except Exception:
                pass
            return

    # ─── .kawaii / .bydlo / .crazy (режимы речи) ─────────
    if cmd_lower in (".kawaii", ".bydlo", ".crazy"):
        mode_name = cmd_lower[1:]  # "kawaii" / "bydlo" / "crazy"
        owner = await get_owner(conn_id)
        if owner and message.from_user and message.from_user.id == owner["user_id"]:
            emoji, label = MODE_INFO[mode_name]
            if active_modes.get(conn_id) == mode_name:
                del active_modes[conn_id]
                save_active_modes()
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
                save_active_modes()
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

    if (
        owner_id and sender_id != owner_id and message.chat.username
        and text_matches_info_pattern(cache[key]["text"])
    ):
        remember_info_moment(
            owner_id, message.chat.username, message.message_id,
            sender_name, cache[key]["text"], cache[key]["sent_at"],
        )

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

    # Сообщения, которые удалил сам .mute — владельцу об этом не сообщаем
    message_ids = []
    for msg_id in event.message_ids:
        if (conn_id, msg_id) in muted_deleted_ids:
            muted_deleted_ids.discard((conn_id, msg_id))
        else:
            message_ids.append(msg_id)
    if not message_ids:
        return

    owner = await get_owner(conn_id)
    owner_id = owner["user_id"] if owner else None

    if not owner_id:
        logging.warning(f"deleted_business_messages: не удалось определить владельца conn={conn_id}, ids={message_ids}")
        if MY_USER_ID:
            await bot.send_message(
                MY_USER_ID,
                f"{WARNING} <b>Не удалось обработать удаление</b>\n"
                f"├ conn_id: <code>{html_mod.escape(conn_id or '')}</code>\n"
                f"├ Удалено сообщений: <b>{len(message_ids)}</b>\n"
                f"└ Причина: не резолвится владелец подключения (get_business_connection упал или соединение неизвестно)",
                parse_mode="HTML"
            )
        return

    if len(message_ids) >= BULK_DELETE_THRESHOLD:
        await send_bulk_deleted_transcript(conn_id, owner_id, message_ids, deleted_at)
        return

    for msg_id in message_ids:
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


def find_own_conversation(
    requester_id: int, username: str, since: datetime | None = None
) -> tuple[list[tuple[int, dict]], str]:
    """Сообщения из СОБСТВЕННОГО бизнес-подключения запросившего с конкретным собеседником @username."""
    entries = []
    chat_title = ""
    for (conn_id, msg_id), data in cache.items():
        if data.get("owner_id") != requester_id:
            continue
        chat_uname_raw = (data.get("chat_uname") or "").strip(" ()@").lower()
        if chat_uname_raw != username:
            continue
        if since and data.get("sent_at") and data["sent_at"] < since:
            continue
        entries.append((msg_id, data))
        if not chat_title:
            chat_title = (data.get("chat_name") or "") + (data.get("chat_uname") or "")
    entries.sort(key=lambda item: item[1]["sent_at"])
    return entries, (chat_title or f"@{username}")


def parse_since_token(text: str) -> tuple[datetime | None, str | None]:
    """Ищет в тексте период вида 7d / 2w / 24h (по МСК) и возвращает (since, исходный токен)."""
    m = re.search(r'\b(\d+)\s*([dhw])\b', text, re.IGNORECASE)
    if not m:
        return None, None
    amount, unit = int(m.group(1)), m.group(2).lower()
    delta = {"h": timedelta(hours=amount), "d": timedelta(days=amount), "w": timedelta(weeks=amount)}[unit]
    return datetime.now(MSK) - delta, m.group(0)


async def run_export(message: Message, text: str):
    match = re.search(r'@(\w+)', text)
    if not match:
        await message.answer(
            "📋 <code>/export @username [7d]</code>\n"
            "Сохранит переписку с этим собеседником в HTML-файл на память.\n"
            "Можно ограничить период: <code>24h</code> / <code>7d</code> / <code>2w</code>.",
            parse_mode="HTML"
        )
        return
    username = match.group(1).lower()
    since, since_token = parse_since_token(text)

    entries, chat_title = find_own_conversation(message.from_user.id, username, since)
    if not entries:
        period_note = f" за последние {since_token}" if since_token else ""
        await message.answer(f"📭 Нет сообщений с @{username}{period_note} в кеше.")
        return

    await message.answer(
        f"{EXPORT_PROGRESS} Готовлю переписку с @{username} ({len(entries)} сообщений)…",
        parse_mode="HTML"
    )

    period_line = f"├ Период: <b>последние {since_token}</b>\n" if since_token else ""
    caption = (
        f"{EXPORT_DONE} <b>Экспорт переписки готов</b>\n"
        f"├ Чат с: <b>{html_mod.escape(chat_title)}</b>\n"
        f"{period_line}"
        f"├ Сообщений: <b>{len(entries)}</b>\n"
        f"└ Сформировано: <b>{fmt(datetime.now(MSK))}</b>\n\n"
        f"📎 Полная переписка во вложении"
    )
    await send_transcript_document(message.chat.id, chat_title, entries, message.from_user.id, caption)


@dp.message(Command("export"))
async def cmd_export(message: Message):
    await run_export(message, message.text or "")


# ─── /remind — напоминания (время всегда по МСК) ──────────────
async def run_remind(message: Message, body: str):
    if not body.strip():
        await message.answer(
            "📋 <code>/remind завтра в 18:00 позвонить другу</code>\n"
            "Понимаю: <code>через 20 минут</code>, <code>завтра в 9:00</code>, "
            "<code>в пятницу в 15:00</code>, <code>25.12 в 10:00</code>.\n"
            "Время всегда по МСК.",
            parse_mode="HTML"
        )
        return

    due_at, reminder_text = parse_remind_time(body, datetime.now(MSK))
    if not due_at:
        await message.answer(
            f"{WARNING} Не понял время. Примеры: <code>через час отдохнуть</code>, "
            f"<code>завтра в 9:00 звонок</code>",
            parse_mode="HTML"
        )
        return

    reminder_text = reminder_text.strip() or "⏰ Напоминание"
    rid = next_reminder_id()
    reminders.append({
        "id": rid,
        "user_id": message.from_user.id,
        "chat_id": message.chat.id,
        "text": reminder_text,
        "due_at": due_at.isoformat(),
        "created_at": datetime.now(MSK).isoformat(),
    })
    save_reminders()
    await message.answer(
        f"{EXPORT_DONE} Напомню <b>{fmt(due_at)}</b> (МСК) [#{rid}]:\n«{html_mod.escape(reminder_text)}»",
        parse_mode="HTML"
    )


@dp.message(Command("remind"))
async def cmd_remind(message: Message):
    text = message.text or ""
    body = re.sub(r'^/remind(@\w+)?\s*', '', text, flags=re.IGNORECASE)
    await run_remind(message, body)


async def run_reminders(message: Message, user_id: int):
    kb = reminders_keyboard(user_id)
    if not kb:
        await message.answer("📭 Нет активных напоминаний.")
        return
    await message.answer(REMINDERS_LIST_TEXT, parse_mode="HTML", reply_markup=kb)


@dp.message(Command("reminders"))
async def cmd_reminders(message: Message):
    await run_reminders(message, message.from_user.id)


async def run_cancel_reminder(message: Message, text: str, user_id: int):
    m = re.search(r'(\d+)', text)
    if not m:
        await message.answer("📋 <code>/cancelreminder ID</code>", parse_mode="HTML")
        return
    rid = int(m.group(1))
    before = len(reminders)
    reminders[:] = [r for r in reminders if not (r["id"] == rid and r["user_id"] == user_id)]
    if len(reminders) < before:
        save_reminders()
        await message.answer(f"✅ Напоминание #{rid} отменено.")
    else:
        await message.answer(f"{WARNING} Напоминание #{rid} не найдено.", parse_mode="HTML")


@dp.message(Command("cancelreminder"))
async def cmd_cancel_reminder(message: Message):
    await run_cancel_reminder(message, message.text or "", message.from_user.id)


async def reminder_loop():
    while True:
        await asyncio.sleep(30)
        now = datetime.now(MSK)
        due = [r for r in reminders if datetime.fromisoformat(r["due_at"]) <= now]
        for r in due:
            try:
                await bot.send_message(
                    r["chat_id"],
                    f"⏰ <b>Напоминание</b>\n{html_mod.escape(r['text'])}",
                    parse_mode="HTML"
                )
            except Exception as e:
                logging.warning(f"reminder send failed: {e}")
            reminders.remove(r)
        if due:
            save_reminders()


# ─── Ежедневная сводка (МСК) ───────────────────────────────────
async def send_daily_digest(conn_id: str, owner_id: int, now: datetime):
    today = now.date()
    todays_entries = [
        data for (cid, _mid), data in cache.items()
        if cid == conn_id and data.get("sent_at") and data["sent_at"].date() == today
    ]
    if not todays_entries:
        return

    incoming = [d for d in todays_entries if d.get("sender_id") != owner_id]
    if not incoming:
        return
    senders = {d["sender_id"] for d in incoming if d.get("sender_id")}

    earlier_senders = set(known_senders.get(conn_id, []))
    new_senders = senders - earlier_senders

    known_senders[conn_id] = list(earlier_senders | senders)
    save_known_senders()

    lines = [
        f"📅 <b>Итоги дня</b> — {now.strftime('%d.%m.%Y')} (МСК)\n",
        f"├ Сообщений получено: <b>{len(incoming)}</b>",
        f"├ Собеседников сегодня: <b>{len(senders)}</b>",
    ]
    if new_senders:
        names = []
        for sid in new_senders:
            match_data = next((d for d in incoming if d.get("sender_id") == sid), None)
            if match_data:
                nm = match_data["sender_name"]
                if match_data.get("sender_username"):
                    nm += f" ({match_data['sender_username']})"
                names.append(nm)
        lines.append(f"└ 🆕 Новые контакты: <b>{len(new_senders)}</b> — {html_mod.escape(', '.join(names))}")
    else:
        lines.append("└ 🆕 Новых контактов нет")

    try:
        await bot.send_message(owner_id, "\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logging.warning(f"digest send failed for {owner_id}: {e}")


async def digest_loop():
    sent_dates: dict[int, str] = {}
    while True:
        await asyncio.sleep(60)
        now = datetime.now(MSK)
        if now.hour != DIGEST_HOUR_MSK:
            continue
        today_str = now.strftime("%Y-%m-%d")
        for conn_id, owner in list(connections.items()):
            owner_id = owner["user_id"]
            if owner_id in digest_disabled:
                continue
            if sent_dates.get(owner_id) == today_str:
                continue
            await send_daily_digest(conn_id, owner_id, now)
            sent_dates[owner_id] = today_str


# ─── /info — ключевые моменты по ключевым словам ──────────────
_MONTHS_RU = r'(?:январ\w*|феврал\w*|март\w*|апрел\w*|ма[йя]\w*|июн\w*|июл\w*|август\w*|сентябр\w*|октябр\w*|ноябр\w*|декабр\w*)'

INFO_PATTERNS = [
    (f"{INFO_AGE} Возраст", [
        r'мне\s+(?:уже\s+|будет\s+|сейчас\s+|через\s+\w+\s+будет\s+)?(\d{1,3})\s*(?:лет|года|год)\b',
        r'(\d{1,3})\s*(?:лет|года|год)\s+мне\b',
        r'исполнилось\s+(\d{1,3})\s*(?:лет|года|год)?',
        r'исполнится\s+(\d{1,3})',
        r'стукну(?:ло|ет)\s+(\d{1,3})',
        r'в\s+свои\s+(\d{1,3})',
        r'возраст[:\s]+(\d{1,3})\b',
        r'(\d{1,3})[-\s]?летн(?:ий|яя|его|ей|ему|им)\b',
        r'\d{1,3}\s+лет\s+от\s+роду',
    ]),
    (f"{INFO_BIRTHDAY} Дата рождения", [
        r'(?:день\s*рождения|днюх[аи]|\bдр\b)\D{0,20}(\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?)',
        r'(?:день\s*рождения|днюх[аи]|\bдр\b)\D{0,20}(\d{1,2}\s+' + _MONTHS_RU + r')',
        r'родил[а]?сь?\D{0,20}(\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?)',
        r'родил[а]?сь?\D{0,20}(\d{1,2}\s+' + _MONTHS_RU + r')',
        r'родил[а]?сь?\s+в\s+(\d{4})\s*(?:году)?',
        r'дата\s+рождения\D{0,10}(\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?)',
        r'мо[йе]\s+днюх[аи]',
    ]),
    (f"{INFO_LOVE} Симпатия / любовь", [
        r'\b(?:люблю|обожаю|нравится|нравишься|симпатия\s+к|симпатизирую|влюблен[а]?|'
        r'влюбилась|влюбился|влюбилась\s+по\s+уши|влюбился\s+по\s+уши|влюбляюсь|'
        r'втюрилась|втюрился|втюрилась\s+по\s+уши|втюрился\s+по\s+уши|втрескал[а]?сь|'
        r'запал[а]?|запала\s+на|запал\s+на|увлечен[а]?|неровно\s+дышу|'
        r'неравнодушн[а-я]*|схожу\s+с\s+ума\s+по|без\s+ума\s+от|вздыхаю\s+по|сохну\s+по|'
        r'млею\s+от|тянет\s+к|привязан[а]?\s+к|влечёт\s+к|воспылал[а]?\s+чувствами|'
        r'испытываю\s+чувства\s+к|испытываю\s+симпатию|сердце\s+бьется\s+чаще|'
        r'нравится\s+до\s+чёртиков|обожаю\s+до\s+безумия|есть\s+чувства\s+к)\b',
    ]),
    (f"{INFO_WANT} Хочет / мечтает", [
        r'\b(?:хочу|мечтаю|надеюсь|планирую|стремлюсь|жажду|было\s+бы\s+круто|'
        r'было\s+бы\s+здорово|я\s+бы\s+хотел[а]?|мне\s+бы\s+хотелось|спит\s+и\s+видит|'
        r'заветное\s+желание|заветная\s+мечта|жду\s+не\s+дождусь|вот\s+бы|'
        r'если\s+бы\s+только|размышляю\s+о\s+том\s+чтобы|подумываю|загадал[а]?\s+желание|'
        r'в\s+планах|всей\s+душой\s+хочу|очень\s+хочется|ужасно\s+хочется|'
        r'до\s+смерти\s+хочу|мечта\s+всей\s+жизни)\b',
    ]),
    (f"{INFO_DISLIKE} Не любит / бесит", [
        r'\b(?:ненавиж[у]|бесит|терпеть\s+не\s+могу|не\s+люблю|раздражает|достало|'
        r'надоело|не\s+выношу|достал[а]?|злит|выводит\s+из\s+себя|тошнит\s+от|'
        r'воротит\s+от|тошно\s+от|коробит\s+от|вымораживает|триггерит|выбешивает|'
        r'кровь\s+закипает\s+от|зубы\s+сводит\s+от|презираю|вызывает\s+отвращение|'
        r'не\s+перевариваю|бесит\s+до\s+чёртиков|конкретно\s+бесит|ужасно\s+раздражает)\b',
    ]),
    (f"{INFO_LOCATION} Место / город", [
        r'\b(?:я\s+из|живу\s+в|переехал[а]?\s+в|родом\s+из|прописан[а]?\s+в|'
        r'обитаю\s+в|проживаю\s+в|обосновал[а]?сь\s+в|осел[а]?\s+в|корни\s+из|'
        r'выросл?[аи]?\s+в|детство\s+прошло\s+в|коренн[а-я]+\s+\w+|в\s+родном\s+городе|'
        r'местн[а-я]+\s+из)\b',
    ]),
    (f"{INFO_JOB} Работа / учёба", [
        r'\b(?:работаю|учусь|подрабатываю|устроил[а]?сь|моя\s+профессия|'
        r'моя\s+специальность|я\s+фрилансер[а-я]*|тружусь|вкалываю|зарабатываю|'
        r'руковожу|занимаю\s+должность|изучаю|поступил[а]?\s+в|окончил[а]?|'
        r'получаю\s+образование|преподаю|веду\s+занятия|фрилансю|моя\s+работа|'
        r'по\s+профессии\s+я|работаю\s+в\s+сфере)\b',
    ]),
    (f"{INFO_FAMILY} Семья / отношения", [
        r'\b(?:муж|жена|парень|девушка|женат|замужем|разведен[а]?|помолвлен[а]?|'
        r'обручен[а]?|встречаемся|в\s+отношениях|свободн[а-я]+\s+(?:сейчас|давно)?|'
        r'холост|не\s+замужем|бывш[а-я]+\s+(?:парень|девушка|муж|жена)|'
        r'моя\s+мама|мой\s+папа|мои\s+родители|моя\s+сестра|мой\s+брат|мой\s+сын|'
        r'моя\s+дочь|мои\s+дети|племянни[кц]а?|бабушка|дедушка|двоюродн[а-я]+)\b',
    ]),
    (f"{INFO_HOBBY} Хобби / увлечения", [
        r'\b(?:увлекаюсь|занимаюсь|коллекционирую|моё\s+хобби|мое\s+хобби|'
        r'люблю\s+играть\s+в|балуюсь|практикую|фанатею\s+от|подсел[а]?\s+на|'
        r'играю\s+на|рисую|вышиваю|вяжу|люблю\s+готовить|люблю\s+путешествовать|'
        r'катаюсь\s+на|тренируюсь\s+в|снимаю\s+на\s+камеру)\b',
    ]),
    (f"{INFO_FEAR} Страхи / фобии", [
        r'\b(?:боюсь|страшно\s+когда|моя\s+фобия|панически\s+боюсь|до\s+ужаса\s+боюсь|'
        r'жуть\s+как\s+боюсь|ужасно\s+боюсь|до\s+дрожи\s+боюсь|мурашки\s+от|'
        r'кошмар\s+для\s+меня|вызывает\s+панику|паническая\s+атака\s+от|содрогаюсь\s+от|'
        r'до\s+чёртиков\s+боюсь|не\s+переношу\s+вид)\b',
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


def text_matches_info_pattern(text: str) -> bool:
    if not text:
        return False
    return any(
        re.search(pattern, text, re.IGNORECASE)
        for _, patterns in INFO_PATTERNS
        for pattern in patterns
    )


def remember_info_moment(owner_id: int, chat_username: str, msg_id: int, sender_name: str, text: str, sent_at: datetime) -> None:
    """Сохраняет сообщение, совпавшее с INFO_PATTERNS, чтобы /info видел его и после рестарта."""
    hist_key = f"{owner_id}:{chat_username.lower()}"
    entries = info_history.setdefault(hist_key, [])
    if any(e["msg_id"] == msg_id for e in entries):
        return
    entries.append({
        "msg_id": msg_id,
        "sent_at": sent_at.isoformat(),
        "sender_name": sender_name,
        "text": text,
    })
    save_info_history()


async def run_info(message: Message, text: str):
    match = re.search(r'@(\w+)', text)
    if not match:
        await message.answer(
            "📋 <code>/info @username [7d]</code>\n"
            "Поищет в переписке возраст, дату рождения, симпатии, желания и т.п. по ключевым словам.\n"
            "Можно ограничить период: <code>24h</code> / <code>7d</code> / <code>2w</code>.",
            parse_mode="HTML"
        )
        return
    username = match.group(1).lower()
    since, since_token = parse_since_token(text)

    entries, chat_title = find_own_conversation(message.from_user.id, username, since)
    # Ключевые моменты — только по словам собеседника, не по своим же сообщениям
    entries = [(msg_id, data) for msg_id, data in entries if data.get("sender_id") != message.from_user.id]

    # Подмешиваем персистентную историю ключевых моментов — она переживает рестарты,
    # в отличие от cache, поэтому старые совпадения не теряются.
    seen_ids = {msg_id for msg_id, _ in entries}
    for item in info_history.get(f"{message.from_user.id}:{username}", []):
        if item["msg_id"] in seen_ids:
            continue
        sent_at = datetime.fromisoformat(item["sent_at"])
        if since and sent_at < since:
            continue
        entries.append((item["msg_id"], {
            "sent_at": sent_at,
            "sender_name": item["sender_name"],
            "text": item["text"],
        }))
        seen_ids.add(item["msg_id"])
    entries.sort(key=lambda e: e[1]["sent_at"])

    if not entries:
        period_note = f" за последние {since_token}" if since_token else ""
        await message.answer(f"📭 Нет сообщений с @{username}{period_note} в кеше.")
        return

    found = scan_info(entries)
    period_line = f" (последние {since_token})" if since_token else ""
    lines = [f"{KEY_MOMENTS} <b>Ключевые моменты — {html_mod.escape(chat_title)}{period_line}</b>"]
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


@dp.message(Command("info"))
async def cmd_info(message: Message):
    await run_info(message, message.text or "")


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


# ─── Кнопочное меню (без команд) ──────────────────────────────
MENU_BUTTON_TEXT = "☰ Меню"
pending_action: dict[int, str] = {}   # user_id -> "export" | "info" | "remind" | "cancel"


def main_reply_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=MENU_BUTTON_TEXT)]],
        resize_keyboard=True,
        is_persistent=True,
    )


def menu_inline_keyboard(user_id: int) -> InlineKeyboardMarkup:
    digest_label = "🔔 Включить дневную сводку" if user_id in digest_disabled else "🔕 Отключить дневную сводку"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Экспорт переписки", callback_data="menu_export",
            icon_custom_emoji_id=EMOJI_EXPORT_DONE_ID,
        )],
        [InlineKeyboardButton(
            text="Инфо по человеку", callback_data="menu_info",
            icon_custom_emoji_id=EMOJI_KEY_MOMENTS_ID,
        )],
        [InlineKeyboardButton(
            text="Поставить напоминание", callback_data="menu_remind",
            icon_custom_emoji_id=EMOJI_EXPORT_PROGRESS_ID,
        )],
        [InlineKeyboardButton(text="📋 Мои напоминания", callback_data="menu_reminders")],
        [InlineKeyboardButton(text=digest_label, callback_data="menu_toggle_digest")],
    ])


def reminders_keyboard(user_id: int) -> InlineKeyboardMarkup | None:
    mine = sorted((r for r in reminders if r["user_id"] == user_id), key=lambda r: r["due_at"])
    if not mine:
        return None
    rows = []
    for r in mine:
        due = datetime.fromisoformat(r["due_at"])
        preview = r["text"][:25] + ("…" if len(r["text"]) > 25 else "")
        label = f"🗑 {due.strftime('%d.%m %H:%M')} — {preview}"
        rows.append([InlineKeyboardButton(
            text=label, callback_data=f"cancelrem_{r['id']}", icon_custom_emoji_id=EMOJI_TRASH_ID,
        )])
    return InlineKeyboardMarkup(inline_keyboard=rows)


REMINDERS_LIST_TEXT = "⏰ <b>Твои напоминания (МСК):</b>\nНажми на напоминание, чтобы отменить его."


@dp.message(F.text == MENU_BUTTON_TEXT)
async def on_menu_button(message: Message):
    pending_action.pop(message.from_user.id, None)
    await message.answer(
        f"{KEY_MOMENTS} <b>Что сделать?</b>\nВыбери ниже — набирать команды не нужно.",
        parse_mode="HTML",
        reply_markup=menu_inline_keyboard(message.from_user.id),
    )


@dp.callback_query(F.data.startswith("menu_"))
async def on_menu_callback(callback: CallbackQuery):
    uid = callback.from_user.id
    action = callback.data
    toast = ""

    if action == "menu_export":
        pending_action[uid] = "export"
        await callback.message.answer(
            f"{EXPORT_PROGRESS} Напиши <code>@username</code> собеседника "
            f"(можно добавить период: <code>7d</code> / <code>2w</code> / <code>24h</code>)",
            parse_mode="HTML",
        )
    elif action == "menu_info":
        pending_action[uid] = "info"
        await callback.message.answer(
            f"{KEY_MOMENTS} Напиши <code>@username</code> собеседника "
            f"(можно добавить период: <code>7d</code> / <code>2w</code> / <code>24h</code>)",
            parse_mode="HTML",
        )
    elif action == "menu_remind":
        pending_action[uid] = "remind"
        await callback.message.answer(
            f"{EXPORT_DONE} Напиши, когда и что напомнить, например:\n"
            f"<code>завтра в 18:00 позвонить другу</code>\n"
            f"<code>через 20 минут отдохнуть</code>\n"
            f"<code>в пятницу в 15:00 встреча</code>\n"
            f"Время по МСК.",
            parse_mode="HTML",
        )
    elif action == "menu_reminders":
        pending_action.pop(uid, None)
        await run_reminders(callback.message, uid)
    elif action == "menu_toggle_digest":
        if uid in digest_disabled:
            digest_disabled.discard(uid)
            toast = "🔔 Дневная сводка включена"
        else:
            digest_disabled.add(uid)
            toast = "🔕 Дневная сводка отключена"
        save_digest_disabled()
        try:
            await callback.message.edit_reply_markup(reply_markup=menu_inline_keyboard(uid))
        except Exception:
            pass

    await callback.answer(toast)


@dp.callback_query(F.data.startswith("cancelrem_"))
async def on_cancel_reminder_button(callback: CallbackQuery):
    uid = callback.from_user.id
    try:
        rid = int(callback.data.split("_", 1)[1])
    except (ValueError, IndexError):
        await callback.answer()
        return

    before = len(reminders)
    reminders[:] = [r for r in reminders if not (r["id"] == rid and r["user_id"] == uid)]
    if len(reminders) == before:
        await callback.answer("⚠️ Не найдено")
        return
    save_reminders()

    kb = reminders_keyboard(uid)
    try:
        if kb:
            await callback.message.edit_text(REMINDERS_LIST_TEXT, parse_mode="HTML", reply_markup=kb)
        else:
            await callback.message.edit_text("📭 Нет активных напоминаний.")
    except Exception:
        pass
    await callback.answer("✅ Отменено")


@dp.message((F.chat.type == "private") & F.text & ~F.text.startswith("/"))
async def on_pending_input(message: Message):
    uid = message.from_user.id
    action = pending_action.get(uid)
    if not action:
        return
    pending_action.pop(uid, None)
    text = (message.text or "").strip()

    if action == "export":
        arg = text if text.startswith("@") else f"@{text}"
        await run_export(message, arg)
    elif action == "info":
        arg = text if text.startswith("@") else f"@{text}"
        await run_info(message, arg)
    elif action == "remind":
        await run_remind(message, text)


@dp.message(Command("start"))
async def cmd_start(message: Message):
    if message.from_user.id == MY_USER_ID:
        domain = os.getenv("RAILWAY_PUBLIC_DOMAIN") or os.getenv("RENDER_EXTERNAL_HOSTNAME") or ""
        await message.answer(
            "👁 Бот запущен.\n\n"
            "Нажми «☰ Меню» снизу — команды набирать не обязательно.\n\n"
            "<b>Команды (для админа):</b>\n"
            "/check @user — мониторить ЛС\n"
            "/uncheck @user — убрать\n"
            "/exclude @user @chat — исключить чат\n"
            "/include @user @chat — вернуть чат\n"
            "/last @user 10 — последние сообщения\n"
            "/export @user [7d] — вся переписка в HTML-файл\n"
            "/remind — напоминание (МСК)\n"
            "/reminders — список напоминаний\n"
            "/monitors — список\n"
            "/users — подключённые",
            parse_mode="HTML",
            reply_markup=main_reply_keyboard(),
        )
    else:
        await message.answer(
            "👁 Бот активен.\n\n"
            "Подключи в <b>Настройки → Telegram Business → Чат-боты</b> "
            "и я буду пересылать тебе удалённые и изменённые сообщения.\n\n"
            "Нажми «☰ Меню» снизу — всё делается кнопками, команды не нужны.\n\n"
            "Раз в день (в 21:00 МСК) присылаю сводку за день по каждому подключённому чату.",
            parse_mode="HTML",
            reply_markup=main_reply_keyboard(),
        )




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
        "callback_query",
        "business_message",
        "edited_business_message",
        "deleted_business_messages",
        "business_connection",
    ]

    await bot.delete_webhook()
    asyncio.create_task(cache_cleanup_loop())
    asyncio.create_task(reminder_loop())
    asyncio.create_task(digest_loop())

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

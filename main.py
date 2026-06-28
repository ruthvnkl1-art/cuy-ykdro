"""
Netflix Clone - Telegram Streaming Proxy
Backend Server: FastAPI + Telethon + SearchGram Bot

pip install fastapi uvicorn telethon python-dotenv
"""

import os
import asyncio
import logging
import re
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse

from telethon import TelegramClient, events
from telethon.tl.types import (
    DocumentAttributeFilename,
    DocumentAttributeVideo,
    MessageMediaDocument,
    ReplyInlineMarkup,
    KeyboardButtonCallback,
)
from telethon.tl.functions.messages import GetBotCallbackAnswerRequest

# ─────────────────────────────────────────────
# הגדרות בסיסיות
# ─────────────────────────────────────────────

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Telegram credentials ──────────────────────
API_ID   = int(os.getenv("TG_API_ID", "20819357"))
API_HASH = os.getenv("TG_API_HASH", "10ed98ddb3b635bac90d9c9a943dd5f5")
PHONE    = os.getenv("TG_PHONE", "+972502840086")
SESSION  = os.getenv("TG_SESSION", "session")

# ── SearchGram Bot ────────────────────────────
SEARCHGRAM_BOT = "searchgram"   # username של הבוט (ללא @)
BOT_TIMEOUT    = 20             # שניות המתנה לתגובת הבוט

# גדלי chunk לסטרימינג (512 KB)
CHUNK_SIZE = 512 * 1024

# ─────────────────────────────────────────────
# ניהול ה-Telegram Client
# ─────────────────────────────────────────────

client: TelegramClient | None = None


async def start_client() -> None:
    """מאתחל את ה-Telegram client ומנהל authentication."""
    global client
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.connect()

    if not await client.is_user_authorized():
       pass
        logger.info("✅ התחברות הצליחה – session נשמר ב-%s.session", SESSION)
    else:
        logger.info("✅ Client מחובר (session קיים)")


async def stop_client() -> None:
    global client
    if client and client.is_connected():
        await client.disconnect()
        logger.info("🔌 Client נותק")


# ─────────────────────────────────────────────
# Lifespan
# ─────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    await start_client()
    yield
    await stop_client()


# ─────────────────────────────────────────────
# יצירת האפליקציה
# ─────────────────────────────────────────────

app = FastAPI(
    title="Telegram Streaming Proxy",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
    expose_headers=["Content-Range", "Accept-Ranges", "Content-Length", "Content-Type"],
)


# ─────────────────────────────────────────────
# כלי עזר
# ─────────────────────────────────────────────

def get_video_filename(message) -> str | None:
    if not message.media or not isinstance(message.media, MessageMediaDocument):
        return None
    for attr in message.media.document.attributes:
        if isinstance(attr, DocumentAttributeFilename):
            return attr.file_name
    return None


def is_video_message(message) -> bool:
    if not message.media or not isinstance(message.media, MessageMediaDocument):
        return False
    doc = message.media.document
    if doc.mime_type and doc.mime_type.startswith("video/"):
        return True
    for attr in doc.attributes:
        if isinstance(attr, DocumentAttributeVideo):
            return True
    return False


def parse_size_from_label(label: str) -> float:
    """מחלץ גודל בMB מתווית כמו '[546.11 MB]' או '[1.28 GB]'."""
    match = re.search(r"\[([0-9.]+)\s*(MB|GB)\]", label, re.IGNORECASE)
    if not match:
        return 0.0
    size  = float(match.group(1))
    unit  = match.group(2).upper()
    return size * 1024 if unit == "GB" else size


def parse_buttons(markup) -> list[dict]:
    """
    מפרסר את כפתורי הבוט ומחזיר רשימה של תוצאות.
    כל כפתור = קובץ עם שם + גודל + callback_data.
    """
    results = []
    if not markup or not isinstance(markup, ReplyInlineMarkup):
        return results

    for row in markup.rows:
        for btn in row.buttons:
            if not isinstance(btn, KeyboardButtonCallback):
                continue
            label   = btn.text.strip()
            size_mb = parse_size_from_label(label)

            # סינון כפתורי ניווט (חצים, עמודים)
            if any(x in label for x in ["⬅️", "➡️", "◀️", "▶️", "עמוד", "הבא", "קודם"]):
                continue

            results.append({
                "label":         label,
                "callback_data": btn.data,
                "size_mb":       round(size_mb, 2),
            })

    return results


# ─────────────────────────────────────────────
# לוגיקת SearchGram
# ─────────────────────────────────────────────

async def search_via_searchgram(query: str) -> list[dict]:
    """
    שולח שאילתה לבוט SearchGram וממתין לתגובה עם כפתורים.
    מחזיר רשימת תוצאות עם callback_data לכל קובץ.
    """
    bot_entity = await client.get_entity(SEARCHGRAM_BOT)

    # ── שלח את שם הסרט ──────────────────────
    sent = await client.send_message(bot_entity, query)
    logger.info("📤 נשלח ל-SearchGram: '%s'", query)

    # ── המתן לתגובה עם כפתורים ──────────────
    response_msg = None
    deadline     = asyncio.get_event_loop().time() + BOT_TIMEOUT

    while asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(1.5)
        msgs = await client.get_messages(bot_entity, limit=5)
        for msg in msgs:
            # תגובה חדשה מהבוט (אחרי ההודעה שלנו) עם כפתורים
            if msg.id > sent.id and msg.reply_markup:
                response_msg = msg
                break
        if response_msg:
            break

    if not response_msg:
        logger.warning("⏰ SearchGram לא הגיב תוך %ds", BOT_TIMEOUT)
        return []

    # ── פרסר את הכפתורים ────────────────────
    buttons = parse_buttons(response_msg.reply_markup)
    logger.info("🎬 נמצאו %d תוצאות ל-'%s'", len(buttons), query)

    # ── צרף את message_id של תגובת הבוט ─────
    for btn in buttons:
        btn["bot_msg_id"]  = response_msg.id
        btn["bot_chat_id"] = bot_entity.id

    return buttons


async def click_button_and_get_file(bot_chat_id: int, bot_msg_id: int, callback_data: bytes):
    """
    לוחץ על כפתור הבוט ומחמתין לקובץ שהוא שולח.
    מחזיר את ה-message עם הקובץ.
    """
    bot_entity = await client.get_entity(bot_chat_id)
    msg        = await client.get_messages(bot_entity, ids=bot_msg_id)

    # ── לחץ על הכפתור ───────────────────────
    await client(GetBotCallbackAnswerRequest(
        peer    = bot_entity,
        msg_id  = bot_msg_id,
        data    = callback_data,
    ))
    logger.info("👆 לחצנו על כפתור – ממתינים לקובץ…")

    # ── המתן לקובץ שהבוט ישלח ───────────────
    deadline = asyncio.get_event_loop().time() + BOT_TIMEOUT
    while asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(1.5)
        msgs = await client.get_messages(bot_entity, limit=5)
        for m in msgs:
            if m.id > bot_msg_id and is_video_message(m):
                logger.info("✅ קיבלנו קובץ וידאו (msg_id=%d)", m.id)
                return m
    return None


# ─────────────────────────────────────────────
# Endpoint 1: Health Check
# ─────────────────────────────────────────────

@app.get("/health")
async def health() -> JSONResponse:
    connected = client is not None and client.is_connected()
    return JSONResponse({"status": "ok", "telegram_connected": connected})


# ─────────────────────────────────────────────
# Endpoint 2: חיפוש דרך SearchGram
# ─────────────────────────────────────────────

@app.get("/search")
async def search_movies(query: str) -> JSONResponse:
    """
    שולח שאילתה לבוט SearchGram ומחזיר את רשימת הכפתורים כתוצאות.
    """
    if not client or not client.is_connected():
        raise HTTPException(503, "Telegram client אינו מחובר")
    if not query.strip():
        raise HTTPException(400, "שאילתת החיפוש ריקה")

    try:
        buttons = await search_via_searchgram(query.strip())
    except Exception as e:
        logger.error("שגיאה בחיפוש: %s", e)
        raise HTTPException(500, f"שגיאה בחיפוש: {e}")

    results = []
    for btn in buttons:
        # callback_data שמור כ-bytes – נקודד ל-hex להעברה ב-URL
        cb_hex = btn["callback_data"].hex()
        results.append({
            "filename":      btn["label"],
            "size_mb":       btn["size_mb"],
            "bot_chat_id":   btn["bot_chat_id"],
            "bot_msg_id":    btn["bot_msg_id"],
            "callback_hex":  cb_hex,
            "stream_url":    f"/stream-bot/{btn['bot_chat_id']}/{btn['bot_msg_id']}/{cb_hex}",
        })

    return JSONResponse({
        "query":   query,
        "count":   len(results),
        "results": results,
    })


# ─────────────────────────────────────────────
# Endpoint 3: סטרימינג דרך SearchGram
# ─────────────────────────────────────────────

@app.get("/stream-bot/{bot_chat_id}/{bot_msg_id}/{callback_hex}")
async def stream_via_bot(
    bot_chat_id:  int,
    bot_msg_id:   int,
    callback_hex: str,
    request:      Request,
) -> StreamingResponse:
    """
    לוחץ על כפתור ב-SearchGram, מקבל את הקובץ, ומזרים אותו לדפדפן.
    תומך ב-HTTP Range Requests.
    """
    if not client or not client.is_connected():
        raise HTTPException(503, "Telegram client אינו מחובר")

    # ── פענוח callback_data ──────────────────
    try:
        callback_data = bytes.fromhex(callback_hex)
    except ValueError:
        raise HTTPException(400, "callback_hex לא תקין")

    # ── לחיצה על כפתור + קבלת קובץ ─────────
    try:
        file_msg = await click_button_and_get_file(bot_chat_id, bot_msg_id, callback_data)
    except Exception as e:
        logger.error("שגיאה בלחיצה על כפתור: %s", e)
        raise HTTPException(500, f"שגיאה: {e}")

    if not file_msg:
        raise HTTPException(404, "הבוט לא שלח קובץ – נסה שוב")

    doc       = file_msg.media.document
    file_size = doc.size
    mime_type = doc.mime_type or "video/mp4"

    # ── פענוח Range header ───────────────────
    range_header = request.headers.get("Range")
    start = 0
    end   = file_size - 1

    if range_header:
        try:
            rv = range_header.strip().replace("bytes=", "")
            rs, re_ = rv.split("-")
            start = int(rs)  if rs  else 0
            end   = int(re_) if re_ else file_size - 1
        except Exception:
            raise HTTPException(416, "Range header לא תקין")

    if start > end or start >= file_size:
        raise HTTPException(416, "Range לא בתחום הקובץ")

    content_length = end - start + 1

    # ── Streaming Generator ──────────────────
    async def video_streamer() -> AsyncGenerator[bytes, None]:
        remaining = content_length
        try:
            async for chunk in client.iter_download(
                file_msg.media,
                offset       = start,
                request_size = CHUNK_SIZE,
            ):
                if remaining <= 0:
                    break
                if len(chunk) > remaining:
                    chunk = chunk[:remaining]
                remaining -= len(chunk)
                yield chunk
        except asyncio.CancelledError:
            logger.info("🛑 סטרימינג בוטל")
        except Exception as e:
            logger.error("❌ שגיאה בסטרימינג: %s", e)

    status_code = 206 if range_header else 200
    headers = {
        "Content-Range":  f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges":  "bytes",
        "Content-Length": str(content_length),
        "Content-Type":   mime_type,
        "Cache-Control":  "no-cache",
    }

    return StreamingResponse(
        video_streamer(),
        status_code = status_code,
        headers     = headers,
        media_type  = mime_type,
    )


# ─────────────────────────────────────────────
# הרצה ישירה
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)

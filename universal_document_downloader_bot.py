#!/usr/bin/env python3
"""
Universal Document Downloader & Viewer Bot (single-file edition)

Dependencies (Python 3.11+):
  pip install -U \
    python-telegram-bot aiohttp beautifulsoup4 aiofiles aiosqlite Pillow fpdf2

Environment:
  export BOT_TOKEN="123456:ABCDEF..."

Optional:
  export RESTRICT_ACCESS="1"          # 1=allow only OWNER_ID + approved users
  export HTTP_PROXY="http://127.0.0.1:8080"
"""

# ============================================================
# 1) imports
# ============================================================
from __future__ import annotations

import asyncio
import logging
import os
import re
import tempfile
import time
import zipfile
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urldefrag, urljoin, urlparse

import aiofiles
import aiohttp
import aiosqlite
from bs4 import BeautifulSoup
from fpdf import FPDF
from PIL import Image, UnidentifiedImageError
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Update
from telegram.error import BadRequest, Forbidden, RetryAfter, TelegramError, TimedOut
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)


# ============================================================
# 2) configuration
# ============================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

# Hardcoded owner/admin ID as requested.
# Change this value before production use.
OWNER_ID = 123456789

RESTRICT_ACCESS = os.getenv("RESTRICT_ACCESS", "1") == "1"
DB_PATH = Path("bot_data.sqlite3")
LOG_PATH = Path("bot.log")

HTTP_TIMEOUT = aiohttp.ClientTimeout(total=60, connect=15, sock_read=60)
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    )
}

WAITING_FOR_URL = 1
HISTORY_PAGE_SIZE = 5
MAX_CONCURRENT_JOBS = 3
MAX_ASSETS_PER_REQUEST = 40
MAX_TEXT_CHARS = 800_000
CACHE_TTL_SECONDS = 15 * 60
CACHE_MAX_ITEMS = 300
RATE_LIMIT_WINDOW_SECONDS = 15
RATE_LIMIT_MAX_EVENTS = 6
DOWNLOAD_CHUNK_SIZE = 64 * 1024
RETRY_ATTEMPTS = 3


def setup_logging() -> None:
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    root.addHandler(sh)

    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(formatter)
    root.addHandler(fh)


logger = logging.getLogger("universal_downloader_bot")


# ============================================================
# 3) helper utilities
# ============================================================
@dataclass(slots=True)
class LinkAnalysis:
    source_url: str
    final_url: str
    title: str = "Untitled"
    html: str = ""
    extracted_text: str = ""
    pdf_links: list[str] = field(default_factory=list)
    image_links: list[str] = field(default_factory=list)
    doc_links: list[str] = field(default_factory=list)
    txt_links: list[str] = field(default_factory=list)
    embedded_links: list[str] = field(default_factory=list)

    @property
    def total_assets(self) -> int:
        return (
            len(self.pdf_links)
            + len(self.image_links)
            + len(self.doc_links)
            + len(self.txt_links)
            + len(self.embedded_links)
        )


class TTLCache:
    """Small LRU + TTL cache for URL analysis results."""

    def __init__(self, ttl_seconds: int, max_items: int) -> None:
        self.ttl_seconds = ttl_seconds
        self.max_items = max_items
        self._store: OrderedDict[str, tuple[float, LinkAnalysis]] = OrderedDict()

    def get(self, key: str) -> LinkAnalysis | None:
        item = self._store.get(key)
        if not item:
            return None
        ts, value = item
        if (time.time() - ts) > self.ttl_seconds:
            self._store.pop(key, None)
            return None
        self._store.move_to_end(key)
        return value

    def set(self, key: str, value: LinkAnalysis) -> None:
        self._store[key] = (time.time(), value)
        self._store.move_to_end(key)
        while len(self._store) > self.max_items:
            self._store.popitem(last=False)

    def size(self) -> int:
        return len(self._store)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_filename(name: str, fallback: str = "file.bin") -> str:
    cleaned = re.sub(r"[^\w.\-]+", "_", name.strip())
    if not cleaned:
        return fallback
    return cleaned[:180]


def human_bytes(size: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f}{unit}"
        value /= 1024
    return f"{size}B"


def dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def normalize_url(url: str) -> str:
    return urldefrag(url.strip())[0]


def extract_first_url(text: str) -> str | None:
    match = re.search(r"(https?://[^\s]+)", text, re.IGNORECASE)
    return match.group(1).strip() if match else None


def is_valid_http_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def extension_of(url: str) -> str:
    path = urlparse(url).path.lower()
    if "." not in path:
        return ""
    return path.rsplit(".", 1)[-1]


def is_image_url(url: str) -> bool:
    return extension_of(url) in {"jpg", "jpeg", "png", "webp", "gif", "bmp", "tiff"}


def is_pdf_url(url: str) -> bool:
    return extension_of(url) == "pdf"


def is_txt_url(url: str) -> bool:
    return extension_of(url) in {"txt", "md", "csv", "log"}


def is_docx_url(url: str) -> bool:
    return extension_of(url) in {"doc", "docx"}


def build_main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📥 Download Document", callback_data="menu:download")],
            [InlineKeyboardButton("📂 My Downloads", callback_data="menu:history")],
            [InlineKeyboardButton("📚 Extract Text", callback_data="menu:extract")],
            [InlineKeyboardButton("🌐 Website Snapshot", callback_data="menu:snapshot")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="menu:settings")],
            [InlineKeyboardButton("ℹ️ Help", callback_data="menu:help")],
        ]
    )


def build_url_options() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("⬇ Download as PDF", callback_data="op:pdf")],
            [InlineKeyboardButton("🖼 Download Images", callback_data="op:images")],
            [InlineKeyboardButton("📄 Extract Text", callback_data="op:text")],
            [InlineKeyboardButton("🌐 Save HTML", callback_data="op:html")],
            [InlineKeyboardButton("🔙 Back", callback_data="menu:back")],
        ]
    )


def build_history_keyboard(page: int, total_pages: int) -> InlineKeyboardMarkup:
    row: list[InlineKeyboardButton] = []
    if page > 0:
        row.append(
            InlineKeyboardButton("⬅ Prev Page", callback_data=f"hist:page:{page - 1}")
        )
    if page + 1 < total_pages:
        row.append(
            InlineKeyboardButton("➡ Next Page", callback_data=f"hist:page:{page + 1}")
        )
    buttons: list[list[InlineKeyboardButton]] = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data="menu:back")])
    return InlineKeyboardMarkup(buttons)


class AsyncDatabase:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.conn: aiosqlite.Connection | None = None
        self.lock = asyncio.Lock()

    async def connect(self) -> None:
        self.conn = await aiosqlite.connect(self.db_path)
        self.conn.row_factory = aiosqlite.Row
        await self._migrate()

    async def close(self) -> None:
        if self.conn:
            await self.conn.close()
            self.conn = None

    async def _migrate(self) -> None:
        assert self.conn is not None
        await self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                added_by INTEGER,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS stats (
                user_id INTEGER PRIMARY KEY,
                total_requests INTEGER NOT NULL DEFAULT 0,
                total_success INTEGER NOT NULL DEFAULT 0,
                total_failed INTEGER NOT NULL DEFAULT 0,
                last_seen TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                action TEXT NOT NULL,
                status TEXT NOT NULL,
                file_name TEXT,
                size_bytes INTEGER,
                error TEXT,
                created_at TEXT NOT NULL
            );
            """
        )
        await self.conn.commit()

    async def add_user(self, user_id: int, added_by: int) -> None:
        assert self.conn is not None
        async with self.lock:
            await self.conn.execute(
                """
                INSERT INTO users (user_id, added_by, is_active, created_at)
                VALUES (?, ?, 1, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    is_active=1,
                    added_by=excluded.added_by
                """,
                (user_id, added_by, utc_now()),
            )
            await self.conn.execute(
                """
                INSERT INTO stats (user_id, last_seen)
                VALUES (?, ?)
                ON CONFLICT(user_id) DO NOTHING
                """,
                (user_id, utc_now()),
            )
            await self.conn.commit()

    async def remove_user(self, user_id: int) -> None:
        assert self.conn is not None
        async with self.lock:
            await self.conn.execute(
                "UPDATE users SET is_active=0 WHERE user_id=?", (user_id,)
            )
            await self.conn.commit()

    async def is_allowed(self, user_id: int) -> bool:
        assert self.conn is not None
        cur = await self.conn.execute(
            "SELECT is_active FROM users WHERE user_id=?", (user_id,)
        )
        row = await cur.fetchone()
        return bool(row and row["is_active"] == 1)

    async def list_users(self) -> list[int]:
        assert self.conn is not None
        cur = await self.conn.execute(
            "SELECT user_id FROM users WHERE is_active=1 ORDER BY user_id ASC"
        )
        rows = await cur.fetchall()
        return [int(r["user_id"]) for r in rows]

    async def touch_user(self, user_id: int) -> None:
        assert self.conn is not None
        async with self.lock:
            await self.conn.execute(
                """
                INSERT INTO stats (user_id, last_seen)
                VALUES (?, ?)
                ON CONFLICT(user_id) DO UPDATE SET last_seen=excluded.last_seen
                """,
                (user_id, utc_now()),
            )
            await self.conn.commit()

    async def inc_request(self, user_id: int) -> None:
        assert self.conn is not None
        async with self.lock:
            await self.conn.execute(
                """
                INSERT INTO stats (user_id, total_requests, last_seen)
                VALUES (?, 1, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    total_requests=total_requests+1,
                    last_seen=excluded.last_seen
                """,
                (user_id, utc_now()),
            )
            await self.conn.commit()

    async def inc_success(self, user_id: int) -> None:
        assert self.conn is not None
        async with self.lock:
            await self.conn.execute(
                "UPDATE stats SET total_success=total_success+1 WHERE user_id=?",
                (user_id,),
            )
            await self.conn.commit()

    async def inc_failed(self, user_id: int) -> None:
        assert self.conn is not None
        async with self.lock:
            await self.conn.execute(
                "UPDATE stats SET total_failed=total_failed+1 WHERE user_id=?",
                (user_id,),
            )
            await self.conn.commit()

    async def add_download(
        self,
        user_id: int,
        url: str,
        action: str,
        status: str,
        file_name: str | None = None,
        size_bytes: int | None = None,
        error: str | None = None,
    ) -> None:
        assert self.conn is not None
        async with self.lock:
            await self.conn.execute(
                """
                INSERT INTO downloads (user_id, url, action, status, file_name, size_bytes, error, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (user_id, url, action, status, file_name, size_bytes, error, utc_now()),
            )
            await self.conn.commit()

    async def get_download_page(
        self, user_id: int, page: int, page_size: int
    ) -> tuple[list[aiosqlite.Row], int]:
        assert self.conn is not None
        cur = await self.conn.execute(
            "SELECT COUNT(*) AS cnt FROM downloads WHERE user_id=?", (user_id,)
        )
        total = int((await cur.fetchone())["cnt"])
        offset = page * page_size
        cur = await self.conn.execute(
            """
            SELECT id, action, status, file_name, size_bytes, created_at
            FROM downloads
            WHERE user_id=?
            ORDER BY id DESC
            LIMIT ? OFFSET ?
            """,
            (user_id, page_size, offset),
        )
        rows = await cur.fetchall()
        return rows, total

    async def get_user_stats(self, user_id: int) -> dict[str, Any]:
        assert self.conn is not None
        cur = await self.conn.execute(
            """
            SELECT total_requests, total_success, total_failed, last_seen
            FROM stats WHERE user_id=?
            """,
            (user_id,),
        )
        row = await cur.fetchone()
        if not row:
            return {
                "total_requests": 0,
                "total_success": 0,
                "total_failed": 0,
                "last_seen": "N/A",
            }
        return dict(row)


# ============================================================
# 4) downloader engine
# ============================================================
class DownloaderEngine:
    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session

    async def fetch_html(self, url: str) -> tuple[str, str]:
        last_exc: Exception | None = None
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                async with self.session.get(
                    url, headers=HTTP_HEADERS, allow_redirects=True
                ) as resp:
                    resp.raise_for_status()
                    content_type = resp.headers.get("Content-Type", "").lower()
                    if "text/html" not in content_type and "application/xhtml" not in content_type:
                        raise ValueError(
                            f"URL is not an HTML page (content-type: {content_type})"
                        )
                    text = await resp.text(errors="ignore")
                    return str(resp.url), text
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                last_exc = exc
                await asyncio.sleep(0.7 * attempt)
        assert last_exc is not None
        raise last_exc

    async def fetch_text_document(self, url: str) -> str:
        last_exc: Exception | None = None
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                async with self.session.get(
                    url, headers=HTTP_HEADERS, allow_redirects=True
                ) as resp:
                    resp.raise_for_status()
                    return await resp.text(errors="ignore")
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = exc
                await asyncio.sleep(0.7 * attempt)
        assert last_exc is not None
        raise last_exc

    async def download_file(
        self,
        url: str,
        out_path: Path,
        progress_cb: Any | None = None,
    ) -> tuple[Path, int]:
        last_exc: Exception | None = None
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                async with self.session.get(
                    url, headers=HTTP_HEADERS, allow_redirects=True
                ) as resp:
                    resp.raise_for_status()
                    total = int(resp.headers.get("Content-Length") or 0)
                    downloaded = 0
                    async with aiofiles.open(out_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(DOWNLOAD_CHUNK_SIZE):
                            if not chunk:
                                continue
                            downloaded += len(chunk)
                            await f.write(chunk)
                            if progress_cb:
                                await progress_cb(downloaded, total)
                    return out_path, downloaded
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = exc
                await asyncio.sleep(0.7 * attempt)
        assert last_exc is not None
        raise last_exc

    async def make_zip(self, file_paths: list[Path], out_zip: Path) -> Path:
        def _zip() -> None:
            with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                for p in file_paths:
                    if p.exists():
                        zf.write(p, arcname=p.name)

        await asyncio.to_thread(_zip)
        return out_zip

    async def images_to_pdf(self, image_paths: list[Path], out_pdf: Path) -> Path:
        def _convert() -> None:
            images: list[Image.Image] = []
            for p in image_paths:
                try:
                    img = Image.open(p)
                    if img.mode != "RGB":
                        img = img.convert("RGB")
                    images.append(img)
                except (UnidentifiedImageError, OSError):
                    continue
            if not images:
                raise ValueError("No valid images to convert.")
            first, rest = images[0], images[1:]
            first.save(out_pdf, save_all=True, append_images=rest)
            for im in images:
                im.close()

        await asyncio.to_thread(_convert)
        return out_pdf

    async def text_to_pdf(self, text: str, out_pdf: Path, title: str) -> Path:
        safe_text = text.encode("latin-1", errors="replace").decode("latin-1")
        safe_title = title.encode("latin-1", errors="replace").decode("latin-1")

        def _build() -> None:
            pdf = FPDF()
            pdf.set_auto_page_break(auto=True, margin=15)
            pdf.add_page()
            pdf.set_font("Helvetica", "B", 14)
            pdf.multi_cell(0, 8, safe_title)
            pdf.ln(2)
            pdf.set_font("Helvetica", size=11)
            for line in safe_text.splitlines():
                pdf.multi_cell(0, 6, line[:1800])
            pdf.output(str(out_pdf))

        await asyncio.to_thread(_build)
        return out_pdf


# ============================================================
# 5) parser engine
# ============================================================
class ParserEngine:
    @staticmethod
    def analyze_html(source_url: str, final_url: str, html: str) -> LinkAnalysis:
        soup = BeautifulSoup(html, "html.parser")
        title = (soup.title.string or "Untitled").strip() if soup.title else "Untitled"

        links: list[str] = []
        image_links: list[str] = []
        embedded: list[str] = []

        for tag, attr in (
            ("a", "href"),
            ("img", "src"),
            ("iframe", "src"),
            ("embed", "src"),
            ("object", "data"),
            ("source", "src"),
        ):
            for node in soup.find_all(tag):
                raw = (node.get(attr) or "").strip()
                if not raw:
                    continue
                absolute = normalize_url(urljoin(final_url, raw))
                if not is_valid_http_url(absolute):
                    continue
                links.append(absolute)
                if tag == "img":
                    image_links.append(absolute)
                if tag in {"iframe", "embed", "object"}:
                    embedded.append(absolute)

        links = dedupe_keep_order(links)
        image_links = dedupe_keep_order(image_links)
        embedded = dedupe_keep_order(embedded)

        pdf_links = [u for u in links if is_pdf_url(u)]
        txt_links = [u for u in links if is_txt_url(u)]
        doc_links = [u for u in links if is_docx_url(u)]
        image_by_ext = [u for u in links if is_image_url(u)]
        image_links = dedupe_keep_order(image_links + image_by_ext)

        # Remove noisy tags before text extraction.
        for t in soup(["script", "style", "noscript", "svg", "canvas"]):
            t.decompose()
        text = soup.get_text(separator="\n", strip=True)[:MAX_TEXT_CHARS]

        return LinkAnalysis(
            source_url=source_url,
            final_url=final_url,
            title=title[:180],
            html=html,
            extracted_text=text,
            pdf_links=pdf_links[:MAX_ASSETS_PER_REQUEST],
            image_links=image_links[:MAX_ASSETS_PER_REQUEST],
            doc_links=doc_links[:MAX_ASSETS_PER_REQUEST],
            txt_links=txt_links[:MAX_ASSETS_PER_REQUEST],
            embedded_links=embedded[:MAX_ASSETS_PER_REQUEST],
        )


# ============================================================
# 6) telegram handlers
# ============================================================
class UniversalDownloaderBot:
    def __init__(self) -> None:
        self.db = AsyncDatabase(DB_PATH)
        self.cache = TTLCache(ttl_seconds=CACHE_TTL_SECONDS, max_items=CACHE_MAX_ITEMS)
        self.rate_windows: dict[int, deque[float]] = defaultdict(deque)
        self.job_semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)
        self.session: aiohttp.ClientSession | None = None
        self.downloader: DownloaderEngine | None = None

    async def post_init(self, _: Application) -> None:
        connector = aiohttp.TCPConnector(limit=100)
        self.session = aiohttp.ClientSession(timeout=HTTP_TIMEOUT, connector=connector)
        self.downloader = DownloaderEngine(self.session)
        await self.db.connect()
        await self.db.add_user(OWNER_ID, OWNER_ID)
        logger.info("Bot initialized")

    async def post_shutdown(self, _: Application) -> None:
        if self.session:
            await self.session.close()
            self.session = None
        await self.db.close()
        logger.info("Bot shutdown complete")

    async def _safe_edit(
        self, message, text: str, reply_markup: InlineKeyboardMarkup | None = None
    ) -> None:
        try:
            await message.edit_text(text, reply_markup=reply_markup)
        except BadRequest as exc:
            if "Message is not modified" in str(exc):
                return
            logger.warning("Edit message failed: %s", exc)

    async def _reply(
        self,
        update: Update,
        text: str,
        reply_markup: InlineKeyboardMarkup | None = None,
    ) -> None:
        if update.callback_query and update.callback_query.message:
            await update.callback_query.message.reply_text(text, reply_markup=reply_markup)
            return
        if update.effective_message:
            await update.effective_message.reply_text(text, reply_markup=reply_markup)

    def _consume_rate_limit(self, user_id: int) -> float:
        now = time.monotonic()
        bucket = self.rate_windows[user_id]
        while bucket and (now - bucket[0]) > RATE_LIMIT_WINDOW_SECONDS:
            bucket.popleft()
        if len(bucket) >= RATE_LIMIT_MAX_EVENTS:
            return RATE_LIMIT_WINDOW_SECONDS - (now - bucket[0])
        bucket.append(now)
        return 0.0

    async def _authorize(
        self, update: Update, *, apply_rate_limit: bool = True
    ) -> bool:
        user = update.effective_user
        if not user:
            return False
        uid = int(user.id)

        await self.db.touch_user(uid)

        if RESTRICT_ACCESS and uid != OWNER_ID and not await self.db.is_allowed(uid):
            msg = (
                "🚫 Access denied.\n"
                f"Your user ID: `{uid}`\n"
                "Contact bot owner to get access."
            )
            if update.callback_query:
                await update.callback_query.answer("Access denied", show_alert=True)
            await self._reply(update, msg)
            return False

        if apply_rate_limit and uid != OWNER_ID:
            wait_s = self._consume_rate_limit(uid)
            if wait_s > 0:
                await self._reply(
                    update, f"⏳ Anti-spam active. Retry in {wait_s:.1f}s."
                )
                return False

        return True

    async def _analysis_for_url(self, url: str) -> LinkAnalysis:
        assert self.downloader is not None
        cached = self.cache.get(url)
        if cached:
            return cached

        # Fast-path for direct assets.
        ext = extension_of(url)
        if ext in {"pdf", "jpg", "jpeg", "png", "gif", "webp", "bmp", "doc", "docx", "txt", "md", "csv", "log"}:
            analysis = LinkAnalysis(source_url=url, final_url=url, title="Direct File Link")
            if is_pdf_url(url):
                analysis.pdf_links.append(url)
            elif is_image_url(url):
                analysis.image_links.append(url)
            elif is_docx_url(url):
                analysis.doc_links.append(url)
            elif is_txt_url(url):
                analysis.txt_links.append(url)
            self.cache.set(url, analysis)
            return analysis

        final_url, html = await self.downloader.fetch_html(url)
        analysis = ParserEngine.analyze_html(url, final_url, html)
        self.cache.set(url, analysis)
        return analysis

    async def _update_progress(self, msg, text: str) -> None:
        await self._safe_edit(msg, text)

    async def start(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._authorize(update, apply_rate_limit=False):
            return
        text = (
            "👋 *Universal Document Downloader Bot*\n\n"
            "Send any public webpage/document URL and I will detect downloadable assets.\n"
            "Use the menu below to begin."
        )
        await update.effective_message.reply_text(
            text, reply_markup=build_main_menu(), parse_mode="Markdown"
        )

    async def help_cmd(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._authorize(update, apply_rate_limit=False):
            return
        text = (
            "*Help*\n"
            "• Send a URL directly, or use menu buttons.\n"
            "• Supported: PDF, images, HTML, TXT, DOCX links.\n"
            "• /cancel cancels URL waiting mode.\n\n"
            "*Admin commands*\n"
            "• /add_user <id>\n"
            "• /remove_user <id>\n"
            "• /users\n"
            "• /broadcast <message>"
        )
        await update.effective_message.reply_text(text, parse_mode="Markdown")

    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        context.user_data.pop("pending_action", None)
        context.user_data.pop("current_url", None)
        await update.effective_message.reply_text(
            "✅ Cancelled.", reply_markup=build_main_menu()
        )
        return ConversationHandler.END

    async def menu_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        if not await self._authorize(update):
            return ConversationHandler.END
        query = update.callback_query
        assert query is not None
        await query.answer()

        action = query.data.split(":", 1)[1]
        if action in {"download", "extract", "snapshot"}:
            context.user_data["pending_action"] = action
            await query.message.reply_text(
                "🔗 Send a public URL.\nExample: https://example.com/article"
            )
            return WAITING_FOR_URL

        if action == "history":
            await self._render_history(query.message, query.from_user.id, 0)
            return ConversationHandler.END

        if action == "settings":
            stats = await self.db.get_user_stats(query.from_user.id)
            text = (
                "⚙️ Settings & Stats\n\n"
                f"• Restrict access: {RESTRICT_ACCESS}\n"
                f"• Cache items: {self.cache.size()}\n"
                f"• Queue limit: {MAX_CONCURRENT_JOBS}\n"
                f"• Requests: {stats['total_requests']}\n"
                f"• Success: {stats['total_success']}\n"
                f"• Failed: {stats['total_failed']}"
            )
            await self._safe_edit(query.message, text, reply_markup=build_main_menu())
            return ConversationHandler.END

        if action == "help":
            await self._safe_edit(
                query.message,
                "ℹ️ Use menu buttons or send a URL directly.\n"
                "If many records exist, My Downloads supports pagination.",
                reply_markup=build_main_menu(),
            )
            return ConversationHandler.END

        if action == "back":
            await self._safe_edit(
                query.message, "🏠 Main menu", reply_markup=build_main_menu()
            )
            return ConversationHandler.END

        return ConversationHandler.END

    async def url_from_conversation(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        if not await self._authorize(update):
            return ConversationHandler.END
        await self.db.inc_request(update.effective_user.id)

        raw = update.effective_message.text.strip()
        url = extract_first_url(raw)
        if not url:
            await update.effective_message.reply_text("❌ Please send a valid URL.")
            return WAITING_FOR_URL

        url = normalize_url(url)
        if not is_valid_http_url(url):
            await update.effective_message.reply_text("❌ Invalid URL scheme/host.")
            return WAITING_FOR_URL

        pending = context.user_data.get("pending_action", "download")
        status_msg = await update.effective_message.reply_text("⚡ Processing link")

        try:
            analysis = await self._analysis_for_url(url)
            context.user_data["current_url"] = url
            context.user_data["last_title"] = analysis.title
        except Exception as exc:  # noqa: BLE001
            await self.db.inc_failed(update.effective_user.id)
            await self._safe_edit(status_msg, f"❌ Error: {exc}")
            return ConversationHandler.END

        if pending == "extract":
            await self._do_extract_text(update, context, analysis, status_msg)
        elif pending == "snapshot":
            await self._do_save_html(update, context, analysis, status_msg)
        else:
            preview = (
                f"🔎 Link Preview\n"
                f"• Title: {analysis.title}\n"
                f"• PDFs: {len(analysis.pdf_links)}\n"
                f"• Images: {len(analysis.image_links)}\n"
                f"• DOC/DOCX: {len(analysis.doc_links)}\n"
                f"• TXT links: {len(analysis.txt_links)}\n"
                f"• Embedded: {len(analysis.embedded_links)}"
            )
            await self._safe_edit(status_msg, preview, reply_markup=build_url_options())
        return ConversationHandler.END

    async def url_direct_message(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not await self._authorize(update):
            return
        raw = update.effective_message.text.strip()
        url = extract_first_url(raw)
        if not url:
            return
        await self.db.inc_request(update.effective_user.id)
        url = normalize_url(url)
        if not is_valid_http_url(url):
            await update.effective_message.reply_text("❌ Invalid URL.")
            return

        status_msg = await update.effective_message.reply_text("⚡ Processing link")
        try:
            analysis = await self._analysis_for_url(url)
        except Exception as exc:  # noqa: BLE001
            await self.db.inc_failed(update.effective_user.id)
            await self._safe_edit(status_msg, f"❌ Error: {exc}")
            return

        context.user_data["current_url"] = url
        context.user_data["last_title"] = analysis.title
        preview = (
            f"🔎 Link Preview\n"
            f"• Title: {analysis.title}\n"
            f"• PDFs: {len(analysis.pdf_links)}\n"
            f"• Images: {len(analysis.image_links)}\n"
            f"• DOC/DOCX: {len(analysis.doc_links)}\n"
            f"• TXT links: {len(analysis.txt_links)}\n"
            f"• Embedded: {len(analysis.embedded_links)}"
        )
        await self._safe_edit(status_msg, preview, reply_markup=build_url_options())

    async def op_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._authorize(update):
            return
        query = update.callback_query
        assert query is not None
        await query.answer()

        await self.db.inc_request(query.from_user.id)
        op = query.data.split(":", 1)[1]
        url = context.user_data.get("current_url")
        if not url:
            await query.message.reply_text(
                "❌ No URL in session. Send a link first.", reply_markup=build_main_menu()
            )
            return

        status_msg = await query.message.reply_text("🚀 Starting download")

        try:
            analysis = await self._analysis_for_url(url)
            async with self.job_semaphore:
                if op == "pdf":
                    await self._do_download_pdf(update, context, analysis, status_msg)
                elif op == "images":
                    await self._do_download_images(update, context, analysis, status_msg)
                elif op == "text":
                    await self._do_extract_text(update, context, analysis, status_msg)
                elif op == "html":
                    await self._do_save_html(update, context, analysis, status_msg)
                else:
                    await self._safe_edit(status_msg, "❌ Unknown operation.")
        except Exception as exc:  # noqa: BLE001
            uid = query.from_user.id
            await self.db.inc_failed(uid)
            await self.db.add_download(
                user_id=uid,
                url=url,
                action=op,
                status="error",
                error=str(exc)[:500],
            )
            await self._safe_edit(status_msg, f"❌ Error: {exc}")

    async def _stream_progress_cb(self, msg, prefix: str):
        last_emit = {"time": 0.0}

        async def _cb(downloaded: int, total: int) -> None:
            now = time.monotonic()
            if (now - last_emit["time"]) < 1.8:
                return
            last_emit["time"] = now
            if total > 0:
                pct = int((downloaded / total) * 100)
                text = f"{prefix}\n{pct}% ({human_bytes(downloaded)}/{human_bytes(total)})"
            else:
                text = f"{prefix}\n{human_bytes(downloaded)}"
            await self._safe_edit(msg, text)

        return _cb

    async def _do_download_pdf(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        analysis: LinkAnalysis,
        status_msg,
    ) -> None:
        assert self.downloader is not None
        uid = update.effective_user.id
        url = analysis.source_url

        await self._update_progress(status_msg, "⚡ Processing link")
        with tempfile.TemporaryDirectory(prefix=f"udd_pdf_{uid}_") as tmp:
            tmp_dir = Path(tmp)
            out_file = tmp_dir / "document.pdf"
            downloaded_assets: list[Path] = []

            if analysis.pdf_links:
                source = analysis.pdf_links[0]
                await self._update_progress(status_msg, "📥 Downloading assets")
                cb = await self._stream_progress_cb(status_msg, "📥 Downloading assets")
                await self.downloader.download_file(source, out_file, progress_cb=cb)
            elif analysis.image_links:
                await self._update_progress(status_msg, "📥 Downloading assets")
                for idx, img_url in enumerate(analysis.image_links[:MAX_ASSETS_PER_REQUEST], start=1):
                    ext = extension_of(img_url) or "jpg"
                    target = tmp_dir / f"img_{idx:03d}.{ext}"
                    cb = await self._stream_progress_cb(
                        status_msg, f"📥 Downloading assets ({idx}/{len(analysis.image_links[:MAX_ASSETS_PER_REQUEST])})"
                    )
                    try:
                        await self.downloader.download_file(img_url, target, progress_cb=cb)
                        downloaded_assets.append(target)
                    except Exception:
                        continue
                await self._update_progress(status_msg, "📦 Packaging files")
                await self.downloader.images_to_pdf(downloaded_assets, out_file)
            else:
                await self._update_progress(status_msg, "📦 Packaging files")
                text = analysis.extracted_text
                if not text and is_txt_url(url):
                    text = await self.downloader.fetch_text_document(url)
                if not text:
                    text = "No readable text found for PDF conversion."
                await self.downloader.text_to_pdf(text, out_file, analysis.title)

            size = out_file.stat().st_size
            await self._update_progress(status_msg, "📤 Uploading to Telegram")
            with out_file.open("rb") as f:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=InputFile(f, filename="document.pdf"),
                    caption=f"✅ PDF ready ({human_bytes(size)})",
                )
            await self._update_progress(status_msg, "✅ Completed")
            await self.db.inc_success(uid)
            await self.db.add_download(
                user_id=uid,
                url=url,
                action="pdf",
                status="ok",
                file_name="document.pdf",
                size_bytes=size,
            )

    async def _do_download_images(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        analysis: LinkAnalysis,
        status_msg,
    ) -> None:
        assert self.downloader is not None
        uid = update.effective_user.id
        url = analysis.source_url
        if not analysis.image_links:
            await self.db.inc_failed(uid)
            await self._safe_edit(status_msg, "❌ No images detected.")
            return

        await self._update_progress(status_msg, "⚡ Processing link")
        with tempfile.TemporaryDirectory(prefix=f"udd_img_{uid}_") as tmp:
            tmp_dir = Path(tmp)
            files: list[Path] = []

            img_urls = analysis.image_links[:MAX_ASSETS_PER_REQUEST]
            await self._update_progress(status_msg, "📥 Downloading assets")
            for idx, img_url in enumerate(img_urls, start=1):
                ext = extension_of(img_url) or "jpg"
                target = tmp_dir / f"img_{idx:03d}.{ext}"
                cb = await self._stream_progress_cb(
                    status_msg, f"📥 Downloading assets ({idx}/{len(img_urls)})"
                )
                try:
                    await self.downloader.download_file(img_url, target, progress_cb=cb)
                    files.append(target)
                except Exception:
                    continue

            if not files:
                await self.db.inc_failed(uid)
                await self._safe_edit(status_msg, "❌ Failed to download detected images.")
                return

            await self._update_progress(status_msg, "📦 Packaging files")
            out_zip = tmp_dir / "images.zip"
            await self.downloader.make_zip(files, out_zip)
            size = out_zip.stat().st_size

            await self._update_progress(status_msg, "📤 Uploading to Telegram")
            with out_zip.open("rb") as f:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=InputFile(f, filename="images.zip"),
                    caption=f"✅ Images package ready ({len(files)} files, {human_bytes(size)})",
                )
            await self._update_progress(status_msg, "✅ Completed")
            await self.db.inc_success(uid)
            await self.db.add_download(
                user_id=uid,
                url=url,
                action="images",
                status="ok",
                file_name="images.zip",
                size_bytes=size,
            )

    async def _do_extract_text(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        analysis: LinkAnalysis,
        status_msg,
    ) -> None:
        assert self.downloader is not None
        uid = update.effective_user.id
        url = analysis.source_url
        await self._update_progress(status_msg, "⚡ Processing link")

        text = analysis.extracted_text
        if not text and is_txt_url(url):
            text = await self.downloader.fetch_text_document(url)
        if not text:
            text = "No readable text extracted from this source."

        with tempfile.TemporaryDirectory(prefix=f"udd_txt_{uid}_") as tmp:
            out_txt = Path(tmp) / "extracted_text.txt"
            await self._update_progress(status_msg, "📦 Packaging files")
            async with aiofiles.open(out_txt, "w", encoding="utf-8") as f:
                await f.write(text)
            size = out_txt.stat().st_size

            await self._update_progress(status_msg, "📤 Uploading to Telegram")
            snippet = text[:900].strip() or "No preview."
            with out_txt.open("rb") as f:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=InputFile(f, filename="extracted_text.txt"),
                    caption=f"✅ Text extracted ({human_bytes(size)})\n\nPreview:\n{snippet}",
                )
            await self._update_progress(status_msg, "✅ Completed")
            await self.db.inc_success(uid)
            await self.db.add_download(
                user_id=uid,
                url=url,
                action="text",
                status="ok",
                file_name="extracted_text.txt",
                size_bytes=size,
            )

    async def _do_save_html(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        analysis: LinkAnalysis,
        status_msg,
    ) -> None:
        uid = update.effective_user.id
        url = analysis.source_url
        await self._update_progress(status_msg, "⚡ Processing link")
        html = analysis.html or f"<html><body><a href='{url}'>{url}</a></body></html>"

        with tempfile.TemporaryDirectory(prefix=f"udd_html_{uid}_") as tmp:
            out_html = Path(tmp) / "snapshot.html"
            await self._update_progress(status_msg, "📦 Packaging files")
            async with aiofiles.open(out_html, "w", encoding="utf-8") as f:
                await f.write(html)
            size = out_html.stat().st_size

            await self._update_progress(status_msg, "📤 Uploading to Telegram")
            with out_html.open("rb") as f:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=InputFile(f, filename="snapshot.html"),
                    caption=f"✅ HTML snapshot saved ({human_bytes(size)})",
                )
            await self._update_progress(status_msg, "✅ Completed")
            await self.db.inc_success(uid)
            await self.db.add_download(
                user_id=uid,
                url=url,
                action="html",
                status="ok",
                file_name="snapshot.html",
                size_bytes=size,
            )

    async def _render_history(self, message, user_id: int, page: int) -> None:
        rows, total = await self.db.get_download_page(user_id, page, HISTORY_PAGE_SIZE)
        total_pages = max(1, (total + HISTORY_PAGE_SIZE - 1) // HISTORY_PAGE_SIZE)
        clamped_page = max(0, min(page, total_pages - 1))
        if clamped_page != page:
            page = clamped_page
            rows, total = await self.db.get_download_page(user_id, page, HISTORY_PAGE_SIZE)
            total_pages = max(1, (total + HISTORY_PAGE_SIZE - 1) // HISTORY_PAGE_SIZE)
        else:
            page = clamped_page

        if not rows:
            text = "📂 No downloads yet."
            await self._safe_edit(message, text, reply_markup=build_main_menu())
            return

        lines = [f"📂 My Downloads (page {page + 1}/{total_pages})", ""]
        for row in rows:
            status_emoji = "✅" if row["status"] == "ok" else "❌"
            size_txt = (
                human_bytes(int(row["size_bytes"])) if row["size_bytes"] is not None else "-"
            )
            lines.append(
                f"{status_emoji} #{row['id']} • {row['action']} • "
                f"{row['file_name'] or 'N/A'} • {size_txt}"
            )
        text = "\n".join(lines)
        await self._safe_edit(
            message, text, reply_markup=build_history_keyboard(page, total_pages)
        )

    async def history_callback(
        self, update: Update, _: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not await self._authorize(update):
            return
        query = update.callback_query
        assert query is not None
        await query.answer()
        parts = query.data.split(":")
        if len(parts) != 3 or parts[1] != "page":
            return
        try:
            page = int(parts[2])
        except ValueError:
            page = 0
        await self._render_history(query.message, query.from_user.id, page)

    def _is_owner(self, update: Update) -> bool:
        user = update.effective_user
        return bool(user and user.id == OWNER_ID)

    async def add_user_cmd(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not self._is_owner(update):
            await self._reply(update, "🚫 Owner only command.")
            return
        if not context.args:
            await update.effective_message.reply_text("Usage: /add_user <id>")
            return
        try:
            user_id = int(context.args[0])
        except ValueError:
            await update.effective_message.reply_text("Invalid user id.")
            return
        await self.db.add_user(user_id, OWNER_ID)
        await update.effective_message.reply_text(f"✅ Added user {user_id}")

    async def remove_user_cmd(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not self._is_owner(update):
            await self._reply(update, "🚫 Owner only command.")
            return
        if not context.args:
            await update.effective_message.reply_text("Usage: /remove_user <id>")
            return
        try:
            user_id = int(context.args[0])
        except ValueError:
            await update.effective_message.reply_text("Invalid user id.")
            return
        await self.db.remove_user(user_id)
        await update.effective_message.reply_text(f"✅ Removed user {user_id}")

    async def users_cmd(
        self, update: Update, _: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not self._is_owner(update):
            await self._reply(update, "🚫 Owner only command.")
            return
        users = await self.db.list_users()
        if not users:
            await update.effective_message.reply_text("No approved users.")
            return
        payload = "\n".join(str(uid) for uid in users)
        await update.effective_message.reply_text(f"👥 Approved users:\n{payload}")

    async def broadcast_cmd(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not self._is_owner(update):
            await self._reply(update, "🚫 Owner only command.")
            return
        msg = " ".join(context.args).strip()
        if not msg:
            await update.effective_message.reply_text("Usage: /broadcast <message>")
            return

        users = await self.db.list_users()
        sent = 0
        failed = 0
        for uid in users:
            try:
                await context.bot.send_message(chat_id=uid, text=f"📢 Broadcast:\n{msg}")
                sent += 1
                await asyncio.sleep(0.03)
            except (Forbidden, BadRequest, TimedOut, RetryAfter, TelegramError):
                failed += 1

        await update.effective_message.reply_text(
            f"Broadcast completed.\n✅ Sent: {sent}\n❌ Failed: {failed}"
        )

    async def error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        logger.exception("Unhandled exception", exc_info=context.error)
        if isinstance(update, Update) and update.effective_message:
            try:
                await update.effective_message.reply_text(
                    "❌ Internal error occurred. Please try again."
                )
            except TelegramError:
                pass


# ============================================================
# 8) main async runner
# ============================================================
def main() -> None:
    setup_logging()

    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is required. Export BOT_TOKEN and restart.")
    if OWNER_ID == 123456789:
        logger.warning("OWNER_ID is still default. Set it before production usage.")

    bot = UniversalDownloaderBot()

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(bot.post_init)
        .post_shutdown(bot.post_shutdown)
        .build()
    )

    application.add_handler(CommandHandler("add_user", bot.add_user_cmd))
    application.add_handler(CommandHandler("remove_user", bot.remove_user_cmd))
    application.add_handler(CommandHandler("users", bot.users_cmd))
    application.add_handler(CommandHandler("broadcast", bot.broadcast_cmd))

    application.add_handler(CommandHandler("start", bot.start))
    application.add_handler(CommandHandler("help", bot.help_cmd))
    application.add_handler(CommandHandler("cancel", bot.cancel))

    conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(bot.menu_callback, pattern=r"^menu:")],
        states={
            WAITING_FOR_URL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, bot.url_from_conversation)
            ]
        },
        fallbacks=[CommandHandler("cancel", bot.cancel)],
        allow_reentry=True,
    )
    application.add_handler(conv)

    application.add_handler(CallbackQueryHandler(bot.op_callback, pattern=r"^op:"))
    application.add_handler(CallbackQueryHandler(bot.history_callback, pattern=r"^hist:"))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, bot.url_direct_message)
    )
    application.add_error_handler(bot.error_handler)

    logger.info("Starting polling...")
    application.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

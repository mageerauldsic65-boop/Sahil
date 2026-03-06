#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Universal Document Downloader & Viewer — Telegram Bot
======================================================
Production-ready, single-file async Telegram bot built with:
  - python-telegram-bot v20+  (async, ApplicationBuilder)
  - aiohttp                   (non-blocking HTTP + streaming downloads)
  - BeautifulSoup / lxml      (HTML parsing & asset extraction)
  - Pillow + ReportLab        (image → PDF conversion)
  - SQLite                    (persistent storage — zero external DB)
  - aiofiles                  (async file I/O)

Supports:  PDF · Images (ZIP/PDF) · TXT · HTML snapshots · DOCX
Runs on:   Python 3.11+ · Termux · Ubuntu VPS · Any POSIX system

=======================================================================
QUICK SETUP
=======================================================================
1. pip install -r requirements.txt
2. Set BOT_TOKEN and OWNER_ID below (or via env vars).
3. python bot.py
=======================================================================
"""

# ═══════════════════════════════════════════════════════════════════════
# §1  IMPORTS
# ═══════════════════════════════════════════════════════════════════════

from __future__ import annotations

import asyncio
import hashlib
import html as html_mod
import io
import json
import logging
import os
import re
import sqlite3
import tempfile
import time
import uuid
import zipfile
from collections import defaultdict
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, unquote

import aiofiles
import aiohttp
import html2text
import validators
from bs4 import BeautifulSoup
from PIL import Image
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas as rl_canvas

from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ═══════════════════════════════════════════════════════════════════════
# §2  CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
OWNER_ID: int  = int(os.getenv("OWNER_ID", "123456789"))

# Paths
BASE_DIR  = Path(__file__).parent
TEMP_DIR  = BASE_DIR / "tmp"
DB_PATH   = BASE_DIR / "bot_data.db"
LOG_PATH  = BASE_DIR / "bot.log"

# Limits
MAX_FILE_SIZE          = 50 * 1024 * 1024   # 50 MB (Telegram cap)
MAX_IMAGES_PER_JOB     = 40
MAX_ASSETS_PER_ZIP     = 30
MAX_CONCURRENT_DL      = 5
MAX_QUEUE_PER_USER     = 3
DOWNLOAD_TIMEOUT_SEC   = 120
CONNECT_TIMEOUT_SEC    = 15

# Anti-spam
RATE_LIMIT_CALLS       = 5
RATE_LIMIT_WINDOW_SEC  = 30

# Cache
CACHE_TTL_SEC          = 3600   # 1 hour

# Retry
MAX_RETRIES            = 3
RETRY_BASE_DELAY_SEC   = 2.0

# Pagination
ITEMS_PER_PAGE         = 5

# ═══════════════════════════════════════════════════════════════════════
# §3  LOGGING
# ═══════════════════════════════════════════════════════════════════════

def _build_logger() -> logging.Logger:
    logger = logging.getLogger("DocBot")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger

log = _build_logger()

# ═══════════════════════════════════════════════════════════════════════
# §4  DATABASE
# ═══════════════════════════════════════════════════════════════════════

class Database:
    """
    Thread-safe SQLite wrapper.  All writes are funnelled through
    context-manager connections so WAL mode keeps reads non-blocking.
    """

    _SCHEMA = """
    PRAGMA journal_mode = WAL;
    PRAGMA foreign_keys = ON;

    CREATE TABLE IF NOT EXISTS users (
        user_id    INTEGER PRIMARY KEY,
        username   TEXT    DEFAULT '',
        first_name TEXT    DEFAULT '',
        joined_at  TEXT    DEFAULT (datetime('now')),
        is_allowed INTEGER DEFAULT 1,
        downloads  INTEGER DEFAULT 0,
        last_seen  TEXT    DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS downloads (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id    INTEGER NOT NULL,
        url        TEXT    NOT NULL,
        file_type  TEXT    NOT NULL,
        file_name  TEXT    DEFAULT '',
        file_size  INTEGER DEFAULT 0,
        status     TEXT    DEFAULT 'pending',
        created_at TEXT    DEFAULT (datetime('now')),
        FOREIGN KEY (user_id) REFERENCES users(user_id)
    );

    CREATE TABLE IF NOT EXISTS url_cache (
        hash       TEXT PRIMARY KEY,
        url        TEXT NOT NULL,
        payload    TEXT NOT NULL,
        cached_at  TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS settings (
        user_id   INTEGER PRIMARY KEY,
        auto_pdf  INTEGER DEFAULT 1,
        notify    INTEGER DEFAULT 1,
        max_imgs  INTEGER DEFAULT 20
    );
    """

    def __init__(self, path: Path) -> None:
        self._path = str(path)
        with self._conn() as c:
            c.executescript(self._SCHEMA)
        log.info("Database ready at %s", path)

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    # ------------------------------------------------------------------
    # User management
    # ------------------------------------------------------------------

    def upsert_user(self, uid: int, username: str, first_name: str) -> None:
        with self._conn() as c:
            c.execute(
                """
                INSERT INTO users (user_id, username, first_name)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username   = excluded.username,
                    first_name = excluded.first_name,
                    last_seen  = datetime('now')
                """,
                (uid, username or "", first_name or ""),
            )

    def is_allowed(self, uid: int) -> bool:
        if uid == OWNER_ID:
            return True
        with self._conn() as c:
            row = c.execute(
                "SELECT is_allowed FROM users WHERE user_id = ?", (uid,)
            ).fetchone()
            return bool(row and row["is_allowed"])

    def all_users(self) -> List[Dict]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM users WHERE is_allowed = 1"
            ).fetchall()
            return [dict(r) for r in rows]

    def set_allowed(self, uid: int, allowed: bool) -> bool:
        with self._conn() as c:
            # Ensure user row exists
            c.execute(
                "INSERT OR IGNORE INTO users (user_id) VALUES (?)", (uid,)
            )
            res = c.execute(
                "UPDATE users SET is_allowed = ? WHERE user_id = ?",
                (int(allowed), uid),
            )
            return res.rowcount > 0

    # ------------------------------------------------------------------
    # Download log
    # ------------------------------------------------------------------

    def log_dl(
        self,
        uid: int,
        url: str,
        ftype: str,
        fname: str,
        fsize: int,
        status: str,
    ) -> None:
        with self._conn() as c:
            c.execute(
                """
                INSERT INTO downloads
                    (user_id, url, file_type, file_name, file_size, status)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (uid, url, ftype, fname, fsize, status),
            )
            c.execute(
                "UPDATE users SET downloads = downloads + 1, last_seen = datetime('now')"
                " WHERE user_id = ?",
                (uid,),
            )

    def user_downloads(self, uid: int, limit: int = 50) -> List[Dict]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM downloads WHERE user_id = ?"
                " ORDER BY created_at DESC LIMIT ?",
                (uid, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Cache
    # ------------------------------------------------------------------

    def cache_get(self, url: str) -> Optional[Dict]:
        h = hashlib.md5(url.encode()).hexdigest()
        with self._conn() as c:
            row = c.execute(
                "SELECT payload FROM url_cache"
                " WHERE hash = ?"
                " AND (julianday('now') - julianday(cached_at)) * 86400 < ?",
                (h, CACHE_TTL_SEC),
            ).fetchone()
            return json.loads(row["payload"]) if row else None

    def cache_set(self, url: str, data: Dict) -> None:
        h = hashlib.md5(url.encode()).hexdigest()
        with self._conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO url_cache (hash, url, payload)"
                " VALUES (?, ?, ?)",
                (h, url, json.dumps(data, ensure_ascii=False)),
            )

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def global_stats(self) -> Dict[str, int]:
        with self._conn() as c:
            tu  = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            td  = c.execute("SELECT COUNT(*) FROM downloads").fetchone()[0]
            act = c.execute(
                "SELECT COUNT(*) FROM users"
                " WHERE (julianday('now') - julianday(last_seen)) < 1"
            ).fetchone()[0]
            return {"total_users": tu, "total_downloads": td, "active_today": act}

    # ------------------------------------------------------------------
    # Settings
    # ------------------------------------------------------------------

    def get_settings(self, uid: int) -> Dict:
        with self._conn() as c:
            c.execute(
                "INSERT OR IGNORE INTO settings (user_id) VALUES (?)", (uid,)
            )
            row = c.execute(
                "SELECT * FROM settings WHERE user_id = ?", (uid,)
            ).fetchone()
            return dict(row)

    def toggle_setting(self, uid: int, key: str) -> Dict:
        settings = self.get_settings(uid)
        new_val = 0 if settings.get(key) else 1
        with self._conn() as c:
            c.execute(
                f"UPDATE settings SET {key} = ? WHERE user_id = ?",
                (new_val, uid),
            )
        settings[key] = new_val
        return settings


# ═══════════════════════════════════════════════════════════════════════
# §5  HELPER UTILITIES
# ═══════════════════════════════════════════════════════════════════════

class RateLimiter:
    """Sliding-window rate limiter keyed by user_id."""

    def __init__(self) -> None:
        self._log: Dict[int, List[float]] = defaultdict(list)

    def check(self, uid: int) -> bool:
        now = time.monotonic()
        cutoff = now - RATE_LIMIT_WINDOW_SEC
        bucket = [t for t in self._log[uid] if t > cutoff]
        self._log[uid] = bucket
        if len(bucket) >= RATE_LIMIT_CALLS:
            return False
        self._log[uid].append(now)
        return True


class DownloadQueue:
    """Global semaphore + per-user slot counter."""

    def __init__(self) -> None:
        self._sem = asyncio.Semaphore(MAX_CONCURRENT_DL)
        self._slots: Dict[int, int] = defaultdict(int)

    async def acquire(self, uid: int) -> bool:
        if self._slots[uid] >= MAX_QUEUE_PER_USER:
            return False
        self._slots[uid] += 1
        await self._sem.acquire()
        return True

    def release(self, uid: int) -> None:
        self._sem.release()
        self._slots[uid] = max(0, self._slots[uid] - 1)


# ── URL helpers ────────────────────────────────────────────────────────

def validate_url(url: str) -> bool:
    try:
        if not validators.url(url):
            return False
        p = urlparse(url)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def truncate_url(url: str, max_len: int = 55) -> str:
    return url if len(url) <= max_len else url[: max_len - 3] + "…"


def safe_filename(name: str, max_len: int = 80) -> str:
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    return name[:max_len].strip("._") or "file"


def url_filename(url: str) -> str:
    raw = unquote(Path(urlparse(url).path).name)
    return safe_filename(raw) if raw else "download"


def paginate(
    items: List, page: int, per: int = ITEMS_PER_PAGE
) -> Tuple[List, int, int]:
    total_pages = max(1, (len(items) + per - 1) // per)
    page = max(0, min(page, total_pages - 1))
    return items[page * per : (page + 1) * per], page, total_pages


# ═══════════════════════════════════════════════════════════════════════
# §6  DOWNLOADER ENGINE
# ═══════════════════════════════════════════════════════════════════════

class DownloaderEngine:
    """
    Async HTTP client wrapper.
    - Persistent aiohttp session (created lazily).
    - Retry with exponential back-off.
    - Streaming download with size guard and optional progress callback.
    """

    _HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT":             "1",
    }

    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(
                total=DOWNLOAD_TIMEOUT_SEC, connect=CONNECT_TIMEOUT_SEC
            )
            connector = aiohttp.TCPConnector(limit=30, ssl=False, ttl_dns_cache=300)
            self._session = aiohttp.ClientSession(
                headers=self._HEADERS,
                timeout=timeout,
                connector=connector,
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # ------------------------------------------------------------------

    async def fetch_text(self, url: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Fetch HTML/text from *url*.
        Returns (body_text, final_url) or (None, None) on failure.
        """
        session = await self._get_session()
        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(url, allow_redirects=True) as r:
                    r.raise_for_status()
                    text = await r.text(errors="replace")
                    return text, str(r.url)
            except Exception as exc:
                log.warning("fetch_text attempt %d: %s — %s", attempt + 1, url, exc)
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_BASE_DELAY_SEC * (attempt + 1))
        return None, None

    async def fetch_bytes(self, url: str) -> Optional[bytes]:
        """Fetch raw bytes (images, small files)."""
        session = await self._get_session()
        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(url) as r:
                    r.raise_for_status()
                    if int(r.headers.get("Content-Length", 0)) > MAX_FILE_SIZE:
                        return None
                    return await r.read()
            except Exception as exc:
                log.warning("fetch_bytes attempt %d: %s — %s", attempt + 1, url, exc)
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_BASE_DELAY_SEC * (attempt + 1))
        return None

    async def stream_to_file(
        self,
        url: str,
        dest: Path,
        progress_cb=None,
    ) -> Tuple[bool, int]:
        """
        Stream-download *url* into *dest* with optional progress callback.
        Returns (success, bytes_written).
        """
        session = await self._get_session()
        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(url) as r:
                    r.raise_for_status()
                    total = int(r.headers.get("Content-Length", 0))
                    written = 0
                    async with aiofiles.open(dest, "wb") as fh:
                        async for chunk in r.content.iter_chunked(65_536):
                            await fh.write(chunk)
                            written += len(chunk)
                            if written > MAX_FILE_SIZE:
                                log.warning("File exceeded size limit — aborting")
                                return False, written
                            if progress_cb and total:
                                await progress_cb(written, total)
                    return True, written
            except Exception as exc:
                log.warning(
                    "stream_to_file attempt %d: %s — %s", attempt + 1, url, exc
                )
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_BASE_DELAY_SEC * (attempt + 1))
        return False, 0

    async def content_type(self, url: str) -> str:
        session = await self._get_session()
        try:
            async with session.head(url, allow_redirects=True) as r:
                return r.headers.get("Content-Type", "").lower()
        except Exception:
            return ""


# ═══════════════════════════════════════════════════════════════════════
# §7  PARSER ENGINE
# ═══════════════════════════════════════════════════════════════════════

_IMG_EXTS  = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff"}
_DOC_EXTS  = {".docx", ".doc", ".odt", ".xlsx", ".pptx", ".epub"}


class ParserEngine:
    """
    BeautifulSoup-powered content analyzer and converter.
    Extracts assets, produces PDFs, ZIPs, and plain text.
    """

    def __init__(self, dl: DownloaderEngine) -> None:
        self._dl = dl

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    @staticmethod
    def parse(body: str) -> BeautifulSoup:
        return BeautifulSoup(body, "lxml")

    @staticmethod
    def title(soup: BeautifulSoup) -> str:
        tag = soup.find("title")
        if tag and tag.get_text(strip=True):
            return tag.get_text(strip=True)[:200]
        h = soup.find("h1")
        return h.get_text(strip=True)[:200] if h else "Untitled"

    @staticmethod
    def meta_desc(soup: BeautifulSoup) -> str:
        m = soup.find("meta", attrs={"name": "description"})
        return (m.get("content", "") or "")[:400] if m else ""

    @staticmethod
    def extract_text(soup: BeautifulSoup) -> str:
        for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()
        h = html2text.HTML2Text()
        h.ignore_links = True
        h.ignore_images = True
        h.body_width = 0
        return h.handle(str(soup)).strip()

    # ------------------------------------------------------------------
    # Link extraction
    # ------------------------------------------------------------------

    def extract_links(
        self, soup: BeautifulSoup, base_url: str
    ) -> Dict[str, List[str]]:
        result: Dict[str, List[str]] = {
            "pdfs": [], "images": [], "docs": [], "links": []
        }
        seen: set = set()

        def _add(url: str) -> None:
            if url in seen or not url.startswith("http"):
                return
            seen.add(url)
            ext = Path(urlparse(url).path).suffix.lower()
            if ext == ".pdf":
                result["pdfs"].append(url)
            elif ext in _IMG_EXTS:
                result["images"].append(url)
            elif ext in _DOC_EXTS:
                result["docs"].append(url)
            else:
                result["links"].append(url)

        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            if href and not href.startswith(("#", "javascript:", "mailto:", "tel:")):
                _add(urljoin(base_url, href))

        for tag in soup.find_all("img", src=True):
            _add(urljoin(base_url, tag["src"].strip()))

        for tag in soup.find_all(["embed", "iframe", "object"], True):
            src = tag.get("src") or tag.get("data") or ""
            if src:
                _add(urljoin(base_url, src.strip()))

        # Deduplicate preserving order
        for key in result:
            result[key] = list(dict.fromkeys(result[key]))

        return result

    # ------------------------------------------------------------------
    # Converters
    # ------------------------------------------------------------------

    async def images_to_pdf(
        self, image_urls: List[str], output: Path
    ) -> int:
        """
        Download up to MAX_IMAGES_PER_JOB images and stitch into a PDF.
        Returns the number of pages written (0 on failure).
        """
        pil_images: List[Image.Image] = []

        for url in image_urls[:MAX_IMAGES_PER_JOB]:
            data = await self._dl.fetch_bytes(url)
            if not data:
                continue
            try:
                img = Image.open(io.BytesIO(data)).convert("RGB")
                pil_images.append(img)
            except Exception as exc:
                log.debug("Skip image %s: %s", url, exc)

        if not pil_images:
            return 0

        try:
            c = rl_canvas.Canvas(str(output), pagesize=A4)
            pw, ph = A4
            for img in pil_images:
                iw, ih = img.size
                ratio = min(pw / iw, ph / ih)
                dw, dh = iw * ratio, ih * ratio
                buf = io.BytesIO()
                img.save(buf, format="JPEG", quality=85)
                buf.seek(0)
                c.drawImage(
                    ImageReader(buf),
                    (pw - dw) / 2, (ph - dh) / 2,
                    dw, dh,
                )
                c.showPage()
            c.save()
            return len(pil_images)
        except Exception as exc:
            log.error("images_to_pdf failed: %s", exc)
            return 0

    async def create_zip(
        self, asset_urls: List[str], output: Path
    ) -> int:
        """
        Download up to MAX_ASSETS_PER_ZIP assets and bundle into a ZIP.
        Returns the count of files added.
        """
        added = 0
        try:
            with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as zf:
                for i, url in enumerate(asset_urls[:MAX_ASSETS_PER_ZIP]):
                    data = await self._dl.fetch_bytes(url)
                    if data:
                        fname = url_filename(url) or f"asset_{i}"
                        if fname in zf.namelist():
                            fname = f"{i}_{fname}"
                        zf.writestr(fname, data)
                        added += 1
        except Exception as exc:
            log.error("create_zip failed: %s", exc)
        return added


# ═══════════════════════════════════════════════════════════════════════
# §8  SESSION STATE
# ═══════════════════════════════════════════════════════════════════════

class SessionStore:
    """Ephemeral per-user dict store (lives in RAM)."""

    def __init__(self) -> None:
        self._data: Dict[int, Dict[str, Any]] = {}

    def set(self, uid: int, key: str, value: Any) -> None:
        self._data.setdefault(uid, {})[key] = value

    def get(self, uid: int, key: str, default: Any = None) -> Any:
        return self._data.get(uid, {}).get(key, default)

    def clear(self, uid: int) -> None:
        self._data.pop(uid, None)


# ═══════════════════════════════════════════════════════════════════════
# §9  KEYBOARD BUILDERS
# ═══════════════════════════════════════════════════════════════════════

def kb_main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📥 Download Document", callback_data="menu:download"),
            InlineKeyboardButton("📂 My Downloads",      callback_data="menu:history"),
        ],
        [
            InlineKeyboardButton("📚 Extract Text",      callback_data="menu:extract"),
            InlineKeyboardButton("🌐 Website Snapshot",  callback_data="menu:snapshot"),
        ],
        [
            InlineKeyboardButton("⚙️ Settings",          callback_data="menu:settings"),
            InlineKeyboardButton("ℹ️ Help",              callback_data="menu:help"),
        ],
    ])


def kb_url_actions() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("⬇ Download as PDF",  callback_data="dl:pdf"),
            InlineKeyboardButton("🖼 Download Images",  callback_data="dl:images"),
        ],
        [
            InlineKeyboardButton("📄 Extract Text",     callback_data="dl:text"),
            InlineKeyboardButton("🌐 Save HTML",        callback_data="dl:html"),
        ],
        [
            InlineKeyboardButton("📦 All Assets (ZIP)", callback_data="dl:zip"),
            InlineKeyboardButton("🔙 Main Menu",        callback_data="menu:main"),
        ],
    ])


def kb_back() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back to Menu", callback_data="menu:main")]
    ])


def kb_pagination(page: int, total: int, prefix: str) -> InlineKeyboardMarkup:
    nav: List[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅ Prev", callback_data=f"{prefix}:{page-1}"))
    nav.append(InlineKeyboardButton(f"📄 {page+1}/{total}", callback_data="noop"))
    if page < total - 1:
        nav.append(InlineKeyboardButton("➡ Next", callback_data=f"{prefix}:{page+1}"))
    return InlineKeyboardMarkup([nav, [InlineKeyboardButton("🔙 Menu", callback_data="menu:main")]])


def kb_settings(s: Dict) -> InlineKeyboardMarkup:
    a = "✅" if s.get("auto_pdf") else "❌"
    n = "✅" if s.get("notify")   else "❌"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Auto PDF  {a}", callback_data="cfg:auto_pdf")],
        [InlineKeyboardButton(f"Notify    {n}", callback_data="cfg:notify")],
        [InlineKeyboardButton("🔙 Back",         callback_data="menu:main")],
    ])


# ═══════════════════════════════════════════════════════════════════════
# §10  GLOBAL SINGLETONS
# ═══════════════════════════════════════════════════════════════════════

db      = Database(DB_PATH)
rl      = RateLimiter()
dlq     = DownloadQueue()
store   = SessionStore()
fetcher = DownloaderEngine()
parser  = ParserEngine(fetcher)


def _ensure_dirs() -> None:
    TEMP_DIR.mkdir(parents=True, exist_ok=True)


def _register(update: Update) -> None:
    u = update.effective_user
    if u:
        db.upsert_user(u.id, u.username or "", u.first_name or "")


def _uid(update: Update) -> int:
    return update.effective_user.id  # type: ignore[union-attr]


# ═══════════════════════════════════════════════════════════════════════
# §11  SHARED HELPERS
# ═══════════════════════════════════════════════════════════════════════

async def _edit(query, text: str, markup=None) -> None:
    kwargs: Dict[str, Any] = {"text": text, "parse_mode": ParseMode.HTML}
    if markup:
        kwargs["reply_markup"] = markup
    try:
        await query.edit_message_text(**kwargs)
    except TelegramError:
        pass


async def _send_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str = "") -> None:
    user = update.effective_user
    name = html_mod.escape(user.first_name or "there") if user else "there"
    body = text or (
        f"👋 Welcome, <b>{name}</b>!\n\n"
        "🤖 <b>Universal Document Downloader</b>\n"
        "Drop any public URL and I'll fetch, convert, or extract its content.\n\n"
        "Choose an option:"
    )
    kwargs: Dict[str, Any] = {
        "text": body,
        "reply_markup": kb_main_menu(),
        "parse_mode": ParseMode.HTML,
    }
    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(**kwargs)
        except TelegramError:
            await update.callback_query.message.reply_text(**kwargs)
    elif update.message:
        await update.message.reply_text(**kwargs)


async def _upload_file(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    path: Path,
    caption: str,
) -> None:
    async with aiofiles.open(path, "rb") as fh:
        data = await fh.read()
    await context.bot.send_document(
        chat_id=chat_id,
        document=io.BytesIO(data),
        filename=path.name,
        caption=caption,
        parse_mode=ParseMode.HTML,
    )


# ═══════════════════════════════════════════════════════════════════════
# §12  COMMAND HANDLERS
# ═══════════════════════════════════════════════════════════════════════

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    _register(update)
    if not db.is_allowed(_uid(update)):
        await update.message.reply_text("⛔ Access denied. Contact the owner.")
        return
    await _send_menu(update, ctx)


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    _register(update)
    text = (
        "📖 <b>How to use</b>\n\n"
        "1️⃣ Send any public URL (webpage, PDF, image page …)\n"
        "2️⃣ Bot analyses the link and detects all downloadable assets\n"
        "3️⃣ Pick your preferred output format\n\n"
        "<b>Download options</b>\n"
        "  ⬇ <b>PDF</b>  — direct PDF, or images stitched into PDF\n"
        "  🖼 <b>Images</b> — ZIP archive of all found images\n"
        "  📄 <b>Text</b>   — clean readable text from the page\n"
        "  🌐 <b>HTML</b>   — full HTML snapshot file\n"
        "  📦 <b>ZIP</b>    — all assets bundled together\n\n"
        "<b>Commands</b>\n"
        "/start   — Main menu\n"
        "/help    — This message\n"
        "/cancel  — Reset current session\n"
        "/history — Your last 50 downloads\n"
        "/stats   — Your personal stats\n\n"
        "<b>Admin commands</b>\n"
        "/add_user [id]       — Grant access\n"
        "/remove_user [id]    — Revoke access\n"
        "/users               — List all users\n"
        "/broadcast [message] — Message every user\n"
        "/admin_stats         — Bot-wide statistics\n"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_back())


async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    store.clear(_uid(update))
    await update.message.reply_text("❌ Session cleared.", reply_markup=kb_main_menu())


async def cmd_history(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    _register(update)
    uid = _uid(update)
    rows = db.user_downloads(uid)
    if not rows:
        await update.message.reply_text(
            "📂 <b>History is empty.</b>\nSend a URL to get started!",
            parse_mode=ParseMode.HTML, reply_markup=kb_main_menu(),
        )
        return
    store.set(uid, "hist", rows)
    await _render_history(update, ctx, uid, 0)


async def _render_history(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE, uid: int, page: int
) -> None:
    rows = store.get(uid, "hist", [])
    page_rows, cur, total = paginate(rows, page)
    lines = [f"📂 <b>Download History</b> — {len(rows)} entries\n"]
    for r in page_rows:
        icon = "✅" if r["status"] == "success" else "❌"
        lines.append(
            f"{icon} <code>{r['file_type'].upper()}</code>"
            f" · {fmt_size(r['file_size'] or 0)}\n"
            f"   🔗 {html_mod.escape(truncate_url(r['url']))}\n"
            f"   📅 {r['created_at'][:16]}\n"
        )
    text = "\n".join(lines)
    markup = kb_pagination(cur, total, "hist")
    if update.callback_query:
        await _edit(update.callback_query, text, markup)
    else:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)


async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    _register(update)
    uid = _uid(update)
    rows = db.user_downloads(uid)
    ok = sum(1 for r in rows if r["status"] == "success")
    text = (
        f"📊 <b>Your Statistics</b>\n\n"
        f"📥 Total requests : <b>{len(rows)}</b>\n"
        f"✅ Successful     : <b>{ok}</b>\n"
        f"❌ Failed         : <b>{len(rows) - ok}</b>\n"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_back())


# ═══════════════════════════════════════════════════════════════════════
# §13  URL MESSAGE HANDLER
# ═══════════════════════════════════════════════════════════════════════

async def handle_url(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Entry point for any text message that contains a URL."""
    uid = _uid(update)
    _register(update)

    if not db.is_allowed(uid):
        await update.message.reply_text("⛔ Access denied.")
        return

    if not rl.check(uid):
        await update.message.reply_text(
            f"⏳ Rate limit: max {RATE_LIMIT_CALLS} requests per "
            f"{RATE_LIMIT_WINDOW_SEC}s. Please wait."
        )
        return

    url = update.message.text.strip()

    if not validate_url(url):
        await update.message.reply_text(
            "❌ <b>Invalid URL</b>\n\n"
            "Please send a valid link starting with <code>http://</code> or "
            "<code>https://</code>.",
            parse_mode=ParseMode.HTML,
        )
        return

    store.set(uid, "url", url)
    prog = await update.message.reply_text("⚡ <b>Analysing link…</b>", parse_mode=ParseMode.HTML)

    # Check cache
    cached = db.cache_get(url)
    if cached:
        scan = cached
        note = " <i>(cached)</i>"
    else:
        html_body, final_url = await fetcher.fetch_text(url)
        if not html_body:
            await prog.edit_text("❌ Could not fetch the page. Check the URL and try again.")
            return

        soup   = parser.parse(html_body)
        links  = parser.extract_links(soup, final_url or url)
        pg_title = parser.title(soup)
        desc   = parser.meta_desc(soup)

        scan = {
            "url":       url,
            "final_url": final_url or url,
            "title":     pg_title,
            "desc":      desc,
            "links":     links,
            "html":      html_body,   # not cached (too large)
        }
        db.cache_set(url, {k: v for k, v in scan.items() if k != "html"})
        note = ""

    store.set(uid, "scan", scan)

    pdfs  = scan["links"].get("pdfs",   [])
    imgs  = scan["links"].get("images", [])
    docs  = scan["links"].get("docs",   [])

    summary = (
        f"🔍 <b>Analysis Complete</b>{note}\n\n"
        f"📄 <b>{html_mod.escape(scan.get('title', 'Untitled'))}</b>\n"
        f"🔗 <code>{html_mod.escape(truncate_url(url))}</code>\n\n"
        f"<b>Assets Found:</b>\n"
        f"   📕 PDFs       : <b>{len(pdfs)}</b>\n"
        f"   🖼 Images     : <b>{len(imgs)}</b>\n"
        f"   📁 Documents  : <b>{len(docs)}</b>\n\n"
        f"<i>Select an action:</i>"
    )
    await prog.edit_text(summary, parse_mode=ParseMode.HTML, reply_markup=kb_url_actions())


# ═══════════════════════════════════════════════════════════════════════
# §14  CALLBACK ROUTER
# ═══════════════════════════════════════════════════════════════════════

async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    q   = update.callback_query
    uid = _uid(update)
    await q.answer()

    if not db.is_allowed(uid):
        await q.answer("⛔ Access denied.", show_alert=True)
        return

    data = q.data or ""

    # ── menu navigation ────────────────────────────────────────────────
    if data == "noop":
        return

    if data == "menu:main":
        await _send_menu(update, ctx)
        return

    if data == "menu:download":
        await _edit(
            q,
            "📥 <b>Send me a URL</b>\n\nPaste any public link:",
            kb_back(),
        )
        return

    if data == "menu:help":
        await cmd_help(update, ctx)
        return

    if data == "menu:history":
        rows = db.user_downloads(uid)
        if not rows:
            await _edit(q, "📂 <b>History is empty.</b>", kb_back())
            return
        store.set(uid, "hist", rows)
        await _render_history(update, ctx, uid, 0)
        return

    if data == "menu:extract":
        scan = store.get(uid, "scan")
        if not scan:
            await _edit(q, "❌ No active URL. Send a URL first.", kb_back())
            return
        await _do_text(update, ctx, uid, scan)
        return

    if data == "menu:snapshot":
        scan = store.get(uid, "scan")
        if not scan:
            await _edit(q, "❌ No active URL. Send a URL first.", kb_back())
            return
        await _do_html(update, ctx, uid, scan)
        return

    if data == "menu:settings":
        s = db.get_settings(uid)
        await _edit(q, "⚙️ <b>Settings</b>\nToggle your preferences:", kb_settings(s))
        return

    # ── settings toggles ───────────────────────────────────────────────
    if data.startswith("cfg:"):
        key = data[4:]
        s = db.toggle_setting(uid, key)
        await _edit(q, "⚙️ <b>Settings updated!</b>", kb_settings(s))
        return

    # ── download actions ───────────────────────────────────────────────
    if data.startswith("dl:"):
        dl_type = data[3:]
        scan = store.get(uid, "scan")
        if not scan:
            await _edit(q, "❌ Session expired. Please send the URL again.", kb_back())
            return
        acquired = await dlq.acquire(uid)
        if not acquired:
            await _edit(q, "⏳ Your queue is full. Wait for current downloads to finish.")
            return
        try:
            dispatch = {
                "pdf":    _do_pdf,
                "images": _do_images,
                "text":   _do_text,
                "html":   _do_html,
                "zip":    _do_zip,
            }
            handler = dispatch.get(dl_type)
            if handler:
                await handler(update, ctx, uid, scan)
            else:
                await _edit(q, "❌ Unknown action.")
        finally:
            dlq.release(uid)
        return

    # ── history pagination ─────────────────────────────────────────────
    if data.startswith("hist:"):
        page = int(data.split(":")[-1])
        await _render_history(update, ctx, uid, page)
        return


# ═══════════════════════════════════════════════════════════════════════
# §15  DOWNLOAD PROCESSORS
# ═══════════════════════════════════════════════════════════════════════

async def _do_pdf(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE, uid: int, scan: Dict
) -> None:
    q   = update.callback_query
    url = scan["url"]
    pdfs  = scan["links"].get("pdfs", [])
    imgs  = scan["links"].get("images", [])
    _ensure_dirs()

    try:
        # ── Direct PDF download ────────────────────────────────────────
        if pdfs:
            await _edit(q, "🚀 <b>Starting PDF download…</b>")
            fname = safe_filename(url_filename(pdfs[0]))
            if not fname.endswith(".pdf"):
                fname += ".pdf"
            dest = TEMP_DIR / f"{uid}_{uuid.uuid4().hex[:8]}_{fname}"

            async def _progress(done: int, total: int) -> None:
                pct = int(done / total * 100)
                if pct % 20 == 0:
                    await _edit(q, f"📥 <b>Downloading PDF…</b> {pct}% ({fmt_size(done)})")

            ok, size = await fetcher.stream_to_file(pdfs[0], dest, _progress)
            if ok and dest.exists():
                await _edit(q, "📤 <b>Uploading to Telegram…</b>")
                await _upload_file(ctx, q.message.chat_id, dest,
                                   f"📕 <b>{html_mod.escape(fname)}</b>\n📦 {fmt_size(size)}")
                db.log_dl(uid, url, "pdf", fname, size, "success")
                await _edit(q, f"✅ <b>Done!</b> <code>{html_mod.escape(fname)}</code>")
                dest.unlink(missing_ok=True)
                return
            await _edit(q, "❌ PDF download failed. Trying image conversion…")

        # ── Images → PDF ───────────────────────────────────────────────
        if imgs:
            await _edit(q, f"🖼 <b>Converting {min(len(imgs), MAX_IMAGES_PER_JOB)} images to PDF…</b>")
            fname = f"images_{uuid.uuid4().hex[:8]}.pdf"
            dest  = TEMP_DIR / fname
            pages = await parser.images_to_pdf(imgs, dest)
            if pages and dest.exists():
                size = dest.stat().st_size
                await _edit(q, "📤 <b>Uploading PDF…</b>")
                await _upload_file(ctx, q.message.chat_id, dest,
                                   f"📕 <b>Image PDF</b>\n🖼 {pages} pages · 📦 {fmt_size(size)}")
                db.log_dl(uid, url, "pdf", fname, size, "success")
                await _edit(q, "✅ <b>PDF created from images!</b>")
                dest.unlink(missing_ok=True)
                return

        await _edit(q, "❌ No PDF or convertible images found on this page.", kb_back())
        db.log_dl(uid, url, "pdf", "", 0, "failed")

    except Exception as exc:
        log.error("_do_pdf uid=%d: %s", uid, exc, exc_info=True)
        await _edit(q, f"❌ <b>Error:</b> {html_mod.escape(str(exc))}", kb_back())
        db.log_dl(uid, url, "pdf", "", 0, "failed")


async def _do_images(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE, uid: int, scan: Dict
) -> None:
    q    = update.callback_query
    url  = scan["url"]
    imgs = scan["links"].get("images", [])
    _ensure_dirs()

    if not imgs:
        await _edit(q, "❌ No images found on this page.", kb_back())
        return

    try:
        count = min(len(imgs), MAX_ASSETS_PER_ZIP)
        await _edit(q, f"📦 <b>Packaging {count} images into ZIP…</b>")
        fname = f"images_{uuid.uuid4().hex[:8]}.zip"
        dest  = TEMP_DIR / fname
        added = await parser.create_zip(imgs, dest)

        if added and dest.exists():
            size = dest.stat().st_size
            await _edit(q, "📤 <b>Uploading ZIP…</b>")
            await _upload_file(ctx, q.message.chat_id, dest,
                               f"🖼 <b>Image Collection</b>\n📁 {added} files · 📦 {fmt_size(size)}")
            db.log_dl(uid, url, "zip", fname, size, "success")
            await _edit(q, "✅ <b>Images downloaded!</b>")
            dest.unlink(missing_ok=True)
        else:
            await _edit(q, "❌ Failed to create image archive.", kb_back())
            db.log_dl(uid, url, "zip", "", 0, "failed")

    except Exception as exc:
        log.error("_do_images uid=%d: %s", uid, exc, exc_info=True)
        await _edit(q, f"❌ <b>Error:</b> {html_mod.escape(str(exc))}", kb_back())


async def _do_text(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE, uid: int, scan: Dict
) -> None:
    q   = update.callback_query if update.callback_query else None
    url = scan["url"]
    _ensure_dirs()

    try:
        html_body = scan.get("html")
        if not html_body:
            if q:
                await _edit(q, "⚡ <b>Fetching page content…</b>")
            html_body, _ = await fetcher.fetch_text(url)

        if not html_body:
            msg = "❌ Could not fetch page content."
            if q:
                await _edit(q, msg, kb_back())
            return

        soup = parser.parse(html_body)
        text = parser.extract_text(soup)
        pg_title = parser.title(soup)

        if not text.strip():
            msg = "❌ No readable text found on this page."
            if q:
                await _edit(q, msg, kb_back())
            return

        chat_id = (
            q.message.chat_id if q
            else (update.message.chat_id if update.message else None)
        )
        if chat_id is None:
            return

        # Short text: show inline
        if len(text) <= 3500:
            preview = (
                f"📄 <b>{html_mod.escape(pg_title)}</b>\n\n"
                f"<pre>{html_mod.escape(text[:3000])}</pre>"
            )
            if q:
                await _edit(q, preview, kb_back())
            else:
                await update.message.reply_text(preview, parse_mode=ParseMode.HTML, reply_markup=kb_back())
        else:
            if q:
                await _edit(q, "📤 <b>Sending text file…</b>")
            fname = f"text_{uuid.uuid4().hex[:8]}.txt"
            dest  = TEMP_DIR / fname
            async with aiofiles.open(dest, "w", encoding="utf-8") as fh:
                await fh.write(f"Source : {url}\nTitle  : {pg_title}\n\n{text}")
            size = dest.stat().st_size
            await _upload_file(ctx, chat_id, dest,
                               f"📄 <b>{html_mod.escape(pg_title)}</b>\n📦 {fmt_size(size)}")
            db.log_dl(uid, url, "txt", fname, size, "success")
            if q:
                await _edit(q, "✅ <b>Text extracted and sent!</b>")
            dest.unlink(missing_ok=True)

    except Exception as exc:
        log.error("_do_text uid=%d: %s", uid, exc, exc_info=True)
        msg = f"❌ <b>Error:</b> {html_mod.escape(str(exc))}"
        if q:
            await _edit(q, msg, kb_back())


async def _do_html(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE, uid: int, scan: Dict
) -> None:
    q   = update.callback_query if update.callback_query else None
    url = scan["url"]
    _ensure_dirs()

    try:
        html_body = scan.get("html")
        if not html_body:
            if q:
                await _edit(q, "⚡ <b>Fetching HTML…</b>")
            html_body, _ = await fetcher.fetch_text(url)

        if not html_body:
            if q:
                await _edit(q, "❌ Could not fetch HTML.", kb_back())
            return

        pg_title = scan.get("title", "snapshot")
        fname = f"snapshot_{safe_filename(pg_title[:30])}_{uuid.uuid4().hex[:6]}.html"
        dest  = TEMP_DIR / fname
        async with aiofiles.open(dest, "w", encoding="utf-8") as fh:
            await fh.write(html_body)

        size    = dest.stat().st_size
        chat_id = (
            q.message.chat_id if q
            else (update.message.chat_id if update.message else None)
        )
        if chat_id is None:
            return

        await _upload_file(
            ctx, chat_id, dest,
            f"🌐 <b>HTML Snapshot</b>\n📄 {html_mod.escape(pg_title)}\n📦 {fmt_size(size)}",
        )
        db.log_dl(uid, url, "html", fname, size, "success")
        if q:
            await _edit(q, "✅ <b>HTML snapshot saved!</b>")
        dest.unlink(missing_ok=True)

    except Exception as exc:
        log.error("_do_html uid=%d: %s", uid, exc, exc_info=True)
        if q:
            await _edit(q, f"❌ <b>Error:</b> {html_mod.escape(str(exc))}", kb_back())


async def _do_zip(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE, uid: int, scan: Dict
) -> None:
    q    = update.callback_query
    url  = scan["url"]
    lnks = scan["links"]
    _ensure_dirs()

    all_assets = lnks.get("pdfs", []) + lnks.get("images", []) + lnks.get("docs", [])
    if not all_assets:
        await _edit(q, "❌ No downloadable assets found on this page.", kb_back())
        return

    try:
        count = min(len(all_assets), MAX_ASSETS_PER_ZIP)
        await _edit(q, f"📦 <b>Packaging {count} assets…</b>")
        fname = f"bundle_{uuid.uuid4().hex[:8]}.zip"
        dest  = TEMP_DIR / fname
        added = await parser.create_zip(all_assets, dest)

        if added and dest.exists():
            size = dest.stat().st_size
            await _edit(q, "📤 <b>Uploading bundle…</b>")
            cap = (
                f"📦 <b>Asset Bundle</b>\n"
                f"   📕 PDFs   : {len(lnks.get('pdfs',  []))}\n"
                f"   🖼 Images : {len(lnks.get('images', []))}\n"
                f"   📁 Docs   : {len(lnks.get('docs',   []))}\n"
                f"   📦 Size   : {fmt_size(size)}"
            )
            await _upload_file(ctx, q.message.chat_id, dest, cap)
            db.log_dl(uid, url, "zip", fname, size, "success")
            await _edit(q, "✅ <b>Bundle ready!</b>")
            dest.unlink(missing_ok=True)
        else:
            await _edit(q, "❌ Failed to create asset bundle.", kb_back())
            db.log_dl(uid, url, "zip", "", 0, "failed")

    except Exception as exc:
        log.error("_do_zip uid=%d: %s", uid, exc, exc_info=True)
        await _edit(q, f"❌ <b>Error:</b> {html_mod.escape(str(exc))}", kb_back())


# ═══════════════════════════════════════════════════════════════════════
# §16  ADMIN COMMANDS
# ═══════════════════════════════════════════════════════════════════════

def owner_only(fn):
    """Decorator — restricts handler to OWNER_ID."""
    @wraps(fn)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user and update.effective_user.id != OWNER_ID:
            await update.message.reply_text("⛔ Owner-only command.")
            return
        return await fn(update, ctx)
    return wrapper


@owner_only
async def cmd_add_user(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not ctx.args:
        await update.message.reply_text("Usage: /add_user <user_id>")
        return
    try:
        tid = int(ctx.args[0])
        db.set_allowed(tid, True)
        await update.message.reply_text(f"✅ User <code>{tid}</code> granted access.",
                                        parse_mode=ParseMode.HTML)
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")


@owner_only
async def cmd_remove_user(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not ctx.args:
        await update.message.reply_text("Usage: /remove_user <user_id>")
        return
    try:
        tid = int(ctx.args[0])
        if tid == OWNER_ID:
            await update.message.reply_text("❌ Cannot remove the owner.")
            return
        db.set_allowed(tid, False)
        await update.message.reply_text(f"✅ User <code>{tid}</code> blocked.",
                                        parse_mode=ParseMode.HTML)
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")


@owner_only
async def cmd_users(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    users = db.all_users()
    if not users:
        await update.message.reply_text("No users registered yet.")
        return
    lines = [f"👥 <b>Allowed Users ({len(users)})</b>\n"]
    for u in users[:40]:
        name = html_mod.escape(u.get("first_name") or "—")
        uname = f"@{u['username']}" if u.get("username") else "—"
        lines.append(
            f"• <code>{u['user_id']}</code>  {name}  {uname}\n"
            f"  DLs: {u['downloads']}  |  Last: {str(u.get('last_seen',''))[:10]}"
        )
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


@owner_only
async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not ctx.args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    msg   = " ".join(ctx.args)
    users = db.all_users()
    sent, failed = 0, 0
    prog = await update.message.reply_text(f"📡 Broadcasting to {len(users)} users…")
    for u in users:
        try:
            await ctx.bot.send_message(
                chat_id=u["user_id"],
                text=f"📢 <b>Broadcast</b>\n\n{html_mod.escape(msg)}",
                parse_mode=ParseMode.HTML,
            )
            sent += 1
            await asyncio.sleep(0.05)           # flood guard
        except TelegramError:
            failed += 1
    await prog.edit_text(
        f"📡 <b>Broadcast complete</b>\n✅ Sent: {sent}  ❌ Failed: {failed}",
        parse_mode=ParseMode.HTML,
    )


@owner_only
async def cmd_admin_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    s = db.global_stats()
    text = (
        f"📊 <b>Bot Statistics</b>\n\n"
        f"👥 Total users    : <b>{s['total_users']}</b>\n"
        f"📥 Total downloads: <b>{s['total_downloads']}</b>\n"
        f"🟢 Active today   : <b>{s['active_today']}</b>\n"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


# ═══════════════════════════════════════════════════════════════════════
# §17  GLOBAL ERROR HANDLER
# ═══════════════════════════════════════════════════════════════════════

async def error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    log.error("Unhandled exception: %s", ctx.error, exc_info=True)
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "⚠️ An unexpected error occurred. Use /cancel to reset."
            )
        except TelegramError:
            pass


# ═══════════════════════════════════════════════════════════════════════
# §18  POST-INIT & MAIN RUNNER
# ═══════════════════════════════════════════════════════════════════════

async def post_init(app: Application) -> None:
    await app.bot.set_my_commands([
        BotCommand("start",   "Main menu"),
        BotCommand("help",    "Usage guide"),
        BotCommand("history", "Download history"),
        BotCommand("stats",   "Your statistics"),
        BotCommand("cancel",  "Reset session"),
    ])
    log.info("Bot commands registered.")


def main() -> None:
    _ensure_dirs()

    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        log.error("BOT_TOKEN is not set! Edit bot.py or set the BOT_TOKEN env variable.")
        raise SystemExit(1)

    log.info("Starting Universal Document Downloader Bot (owner=%d)…", OWNER_ID)

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    # ── Command handlers ───────────────────────────────────────────────
    app.add_handler(CommandHandler("start",        cmd_start))
    app.add_handler(CommandHandler("help",         cmd_help))
    app.add_handler(CommandHandler("cancel",       cmd_cancel))
    app.add_handler(CommandHandler("history",      cmd_history))
    app.add_handler(CommandHandler("stats",        cmd_stats))

    # ── Admin handlers ─────────────────────────────────────────────────
    app.add_handler(CommandHandler("add_user",     cmd_add_user))
    app.add_handler(CommandHandler("remove_user",  cmd_remove_user))
    app.add_handler(CommandHandler("users",        cmd_users))
    app.add_handler(CommandHandler("broadcast",    cmd_broadcast))
    app.add_handler(CommandHandler("admin_stats",  cmd_admin_stats))

    # ── Interaction handlers ───────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_url))

    # ── Global error handler ───────────────────────────────────────────
    app.add_error_handler(error_handler)

    log.info("Bot is running. Press Ctrl+C to stop.")
    app.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES,
    )


if __name__ == "__main__":
    main()

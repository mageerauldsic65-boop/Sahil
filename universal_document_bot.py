#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Universal Document Downloader & Viewer — Telegram Bot (v1.0)
============================================================
Production-ready async Telegram bot for downloading documents from any public URL.
Supports PDF, images, HTML, TXT, DOCX detection with BeautifulSoup parsing.

Dependencies (pip install -r requirements.txt):
    python-telegram-bot[ext]>=20.0
    aiohttp>=3.9.0
    beautifulsoup4>=4.12.0
    Pillow>=10.0.0
    lxml>=5.0.0

Runs on: Python 3.11+, Termux, Ubuntu VPS
"""

from __future__ import annotations

import asyncio
import hashlib
import html as html_mod
import io
import json
import logging
import os
import re
import time
import zipfile
from collections import deque
from dataclasses import dataclass, field
from functools import wraps
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup
from PIL import Image
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ChatAction, ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ═══════════════════════════════════════════════════════════════════════════
# 1. IMPORTS (complete)
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
# 2. CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))  # Hardcoded: set your Telegram user ID

DATA_DIR = Path("bot_data")
USERS_FILE = DATA_DIR / "users.json"
HISTORY_FILE = DATA_DIR / "download_history.json"
CACHE_DIR = DATA_DIR / "cache"
TEMP_DIR = DATA_DIR / "temp"

# Performance
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=90, connect=15)
MAX_CONCURRENT_DOWNLOADS = 4
MAX_CACHE_SIZE_MB = 50
MAX_FILE_SIZE_MB = 50
ITEMS_PER_PAGE = 5
RATE_LIMIT_REQUESTS = 5
RATE_LIMIT_WINDOW = 60  # seconds

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# URL validation
URL_PATTERN = re.compile(
    r"https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+[^\s]*",
    re.IGNORECASE,
)
SUPPORTED_EXTENSIONS = (".pdf", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".html", ".htm", ".txt", ".docx")

HTML_PARSER = "lxml"
try:
    BeautifulSoup("", HTML_PARSER)
except Exception:
    HTML_PARSER = "html.parser"

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("UniversalDocBot")


# ═══════════════════════════════════════════════════════════════════════════
# 3. HELPER UTILITIES
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class ParsedDocument:
    """Holds parsed document metadata and assets."""

    url: str
    title: str
    pdf_links: list[str] = field(default_factory=list)
    image_urls: list[str] = field(default_factory=list)
    text_content: str = ""
    html_content: str = ""
    docx_links: list[str] = field(default_factory=list)
    raw_html: str = ""
    soup: BeautifulSoup | None = None


def _safe_filename(name: str, max_len: int = 50) -> str:
    """Sanitize string for use as filename."""
    safe = re.sub(r"[^\w\s\-.]", "", name)
    return safe.strip()[:max_len] or "document"


def _progress_bar(pct: int, width: int = 10) -> str:
    """Generate progress bar string."""
    filled = int(width * pct / 100)
    return "█" * filled + "░" * (width - filled)


def _cache_key(url: str) -> str:
    """Generate cache key from URL."""
    return hashlib.sha256(url.encode()).hexdigest()[:16]


# ═══════════════════════════════════════════════════════════════════════════
# USER MANAGER
# ═══════════════════════════════════════════════════════════════════════════


class UserManager:
    """Manages authorized users with persistent JSON storage."""

    def __init__(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self._users: set[int] = set()
        self._load()

    def _load(self) -> None:
        if USERS_FILE.exists():
            try:
                data = json.loads(USERS_FILE.read_text())
                self._users = set(data.get("users", []))
            except (json.JSONDecodeError, KeyError):
                self._users = set()
        if OWNER_ID:
            self._users.add(OWNER_ID)
        self._save()

    def _save(self) -> None:
        USERS_FILE.write_text(json.dumps({"users": sorted(self._users)}, indent=2))

    def add(self, uid: int) -> bool:
        if uid in self._users:
            return False
        self._users.add(uid)
        self._save()
        return True

    def remove(self, uid: int) -> bool:
        if uid == OWNER_ID or uid not in self._users:
            return False
        self._users.discard(uid)
        self._save()
        return True

    def is_authorized(self, uid: int) -> bool:
        return uid in self._users

    @property
    def all_ids(self) -> list[int]:
        return sorted(self._users)


# ═══════════════════════════════════════════════════════════════════════════
# RATE LIMITER & ANTI-SPAM
# ═══════════════════════════════════════════════════════════════════════════


class RateLimiter:
    """Per-user rate limiting."""

    def __init__(self, max_requests: int = RATE_LIMIT_REQUESTS, window: int = RATE_LIMIT_WINDOW):
        self._requests: dict[int, deque[float]] = {}
        self._max = max_requests
        self._window = window

    def check(self, user_id: int) -> bool:
        now = time.time()
        if user_id not in self._requests:
            self._requests[user_id] = deque()
        q = self._requests[user_id]
        while q and now - q[0] > self._window:
            q.popleft()
        if len(q) >= self._max:
            return False
        q.append(now)
        return True


# ═══════════════════════════════════════════════════════════════════════════
# DOWNLOAD HISTORY & STATS
# ═══════════════════════════════════════════════════════════════════════════


class DownloadHistory:
    """Tracks download history and user statistics."""

    def __init__(self) -> None:
        self._history: list[dict] = []
        self._stats: dict[int, int] = {}
        self._load()

    def _load(self) -> None:
        if HISTORY_FILE.exists():
            try:
                data = json.loads(HISTORY_FILE.read_text())
                self._history = data.get("history", [])[-500:]
                self._stats = {int(k): v for k, v in data.get("stats", {}).items()}
            except Exception:
                pass

    def _save(self) -> None:
        HISTORY_FILE.write_text(
            json.dumps(
                {"history": self._history[-500:], "stats": self._stats},
                indent=2,
            )
        )

    def add(self, user_id: int, url: str, format_type: str, success: bool) -> None:
        self._history.append(
            {"user_id": user_id, "url": url[:200], "format": format_type, "success": success, "ts": time.time()}
        )
        self._stats[user_id] = self._stats.get(user_id, 0) + 1
        self._save()

    def get_recent(self, user_id: int, limit: int = 10) -> list[dict]:
        return [h for h in reversed(self._history) if h["user_id"] == user_id][:limit]


# ═══════════════════════════════════════════════════════════════════════════
# SIMPLE CACHE (URL -> ParsedDocument metadata)
# ═══════════════════════════════════════════════════════════════════════════


class URLCache:
    """In-memory cache for parsed document metadata (TTL-based)."""

    def __init__(self, max_entries: int = 100, ttl_seconds: int = 600):
        self._cache: dict[str, tuple[ParsedDocument, float]] = {}
        self._max = max_entries
        self._ttl = ttl_seconds

    def get(self, url: str) -> ParsedDocument | None:
        key = _cache_key(url)
        if key not in self._cache:
            return None
        doc, ts = self._cache[key]
        if time.time() - ts > self._ttl:
            del self._cache[key]
            return None
        return doc

    def set(self, url: str, doc: ParsedDocument) -> None:
        key = _cache_key(url)
        if len(self._cache) >= self._max:
            oldest = min(self._cache.items(), key=lambda x: x[1][1])
            del self._cache[oldest[0]]
        self._cache[key] = (doc, time.time())


user_mgr = UserManager()
rate_limiter = RateLimiter()
download_history = DownloadHistory()
url_cache = URLCache()

# Pending URL sessions: user_id -> {url, doc, page}
_pending: dict[int, dict[str, Any]] = {}
_download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)


# ═══════════════════════════════════════════════════════════════════════════
# 4. DOWNLOADER ENGINE
# ═══════════════════════════════════════════════════════════════════════════


class DownloadEngine:
    """Async download engine with retry logic and streaming."""

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session

    async def fetch_text(self, url: str, extra_headers: dict | None = None) -> str:
        """Fetch URL as text with retry."""
        hdrs = {**BROWSER_HEADERS, **(extra_headers or {})}
        for attempt in range(3):
            try:
                async with self.session.get(
                    url,
                    headers=hdrs,
                    timeout=REQUEST_TIMEOUT,
                    allow_redirects=True,
                ) as r:
                    r.raise_for_status()
                    return await r.text()
            except Exception as e:
                if attempt == 2:
                    raise
                await asyncio.sleep(2 ** attempt)
        return ""

    async def fetch_bytes(self, url: str, extra_headers: dict | None = None) -> bytes:
        """Fetch URL as bytes with retry."""
        hdrs = {**BROWSER_HEADERS, **(extra_headers or {})}
        for attempt in range(3):
            try:
                async with self.session.get(
                    url,
                    headers=hdrs,
                    timeout=REQUEST_TIMEOUT,
                    allow_redirects=True,
                ) as r:
                    r.raise_for_status()
                    return await r.read()
            except Exception as e:
                if attempt == 2:
                    raise
                await asyncio.sleep(2 ** attempt)
        return b""

    async def download_images(
        self, urls: list[str], progress_cb=None
    ) -> list[bytes]:
        """Download multiple images concurrently."""
        sem = asyncio.Semaphore(6)
        results: list[tuple[int, bytes | None]] = []

        async def _grab(idx: int, url: str) -> None:
            async with sem:
                for attempt in range(3):
                    try:
                        data = await self.fetch_bytes(url)
                        if len(data) > 100:
                            results.append((idx, data))
                            if progress_cb:
                                await progress_cb(idx + 1, len(urls))
                            return
                    except Exception:
                        await asyncio.sleep(1.5 * (attempt + 1))
                results.append((idx, None))

        await asyncio.gather(*(_grab(i, u) for i, u in enumerate(urls)))
        results.sort(key=lambda x: x[0])
        return [d for _, d in results if d]

    @staticmethod
    def images_to_pdf(blobs: list[bytes]) -> bytes:
        """Convert image bytes to single PDF."""
        imgs: list[Image.Image] = []
        for b in blobs:
            try:
                im = Image.open(io.BytesIO(b))
                if im.mode in ("RGBA", "P", "LA"):
                    im = im.convert("RGB")
                imgs.append(im)
            except Exception:
                pass
        if not imgs:
            raise ValueError("No valid images for PDF")
        buf = io.BytesIO()
        first, *rest = imgs
        first.save(buf, "PDF", save_all=True, append_images=rest, resolution=150)
        return buf.getvalue()

    @staticmethod
    def images_to_zip(blobs: list[bytes], title: str = "document") -> bytes:
        """Pack images into ZIP."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for idx, data in enumerate(blobs, 1):
                ext = "png" if data[:4] == b"\x89PNG" else "jpg"
                zf.writestr(f"{title}_page_{idx:04d}.{ext}", data)
        return buf.getvalue()


# ═══════════════════════════════════════════════════════════════════════════
# 5. PARSER ENGINE
# ═══════════════════════════════════════════════════════════════════════════


class ParserEngine:
    """Parse webpages to detect documents and extract content."""

    PDF_RE = re.compile(r"\.pdf(?:\?|$)", re.I)
    IMG_RE = re.compile(r"\.(?:jpg|jpeg|png|gif|webp)(?:\?|$)", re.I)
    DOCX_RE = re.compile(r"\.docx(?:\?|$)", re.I)

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session
        self.downloader = DownloadEngine(session)

    async def parse_url(self, url: str, use_cache: bool = True) -> ParsedDocument:
        """Fetch and parse URL, returning ParsedDocument."""
        if use_cache:
            cached = url_cache.get(url)
            if cached:
                return cached

        html = await self.downloader.fetch_text(url)
        soup = BeautifulSoup(html, HTML_PARSER)

        title = "Untitled"
        if soup.title and soup.title.string:
            title = soup.title.string.strip()[:200]
        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content"):
            title = og_title["content"].strip()[:200]

        # Check if URL itself is a direct document
        pdf_links: list[str] = []
        image_urls: list[str] = []
        docx_links: list[str] = []

        if self.PDF_RE.search(url):
            pdf_links.append(url)
        elif self.IMG_RE.search(url):
            image_urls.append(url)
        elif self.DOCX_RE.search(url):
            docx_links.append(url)

        # Extract links from page
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("/"):
                href = urljoin(url, href)
            elif not href.startswith("http"):
                continue
            if self.PDF_RE.search(href) and href not in pdf_links:
                pdf_links.append(href)
            elif self.IMG_RE.search(href) and href not in image_urls:
                image_urls.append(href)
            elif self.DOCX_RE.search(href) and href not in docx_links:
                docx_links.append(href)

        # Extract img src
        for img in soup.find_all("img", src=True):
            src = img["src"].strip()
            if src.startswith("//"):
                src = "https:" + src
            elif src.startswith("/"):
                src = urljoin(url, src)
            if src.startswith("http") and self.IMG_RE.search(src) and src not in image_urls:
                image_urls.append(src)

        # Extract data-src, data-url for lazy images
        for img in soup.find_all(attrs={"data-src": True}):
            src = img["data-src"].strip()
            if src.startswith("http") and self.IMG_RE.search(src) and src not in image_urls:
                image_urls.append(src)

        # Extract text content
        text_parts: list[str] = []
        for tag in ["article", "main", "[role='main']", ".content", ".post", ".article", "body"]:
            el = soup.select_one(tag) if tag.startswith(("[", ".")) else soup.find(tag)
            if el:
                t = el.get_text("\n", strip=True)
                if t and len(t) > 100:
                    text_parts.append(t)
                    break
        if not text_parts:
            text_parts.append(soup.get_text("\n", strip=True))

        text_content = "\n\n".join(text_parts) if text_parts else ""
        # Clean HTML for snapshot (remove scripts, styles)
        for tag in soup(["script", "style"]):
            tag.decompose()
        html_content = str(soup) if soup else html

        doc = ParsedDocument(
            url=url,
            title=title,
            pdf_links=pdf_links[:20],
            image_urls=image_urls[:50],
            text_content=text_content[:500_000],
            html_content=html_content[:2_000_000],
            docx_links=docx_links[:10],
            raw_html=html,
            soup=soup,
        )
        url_cache.set(url, doc)
        return doc


# ═══════════════════════════════════════════════════════════════════════════
# DECORATORS
# ═══════════════════════════════════════════════════════════════════════════


def auth_required(func):
    @wraps(func)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id if update.effective_user else 0
        if not user_mgr.is_authorized(uid):
            await update.message.reply_text(
                "❌ <b>Access Denied</b>\n\nContact the bot owner for access.",
                parse_mode=ParseMode.HTML,
            )
            return
        return await func(update, ctx)
    return wrapper


def owner_only(func):
    @wraps(func)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id if update.effective_user else 0
        if uid != OWNER_ID:
            await update.message.reply_text("❌ Owner-only command.")
            return
        return await func(update, ctx)
    return wrapper


# ═══════════════════════════════════════════════════════════════════════════
# 6. TELEGRAM HANDLERS
# ═══════════════════════════════════════════════════════════════════════════


def main_menu_keyboard() -> InlineKeyboardMarkup:
    """Main menu inline keyboard."""
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📥 Download Document", callback_data="menu:download")],
            [InlineKeyboardButton("📂 My Downloads", callback_data="menu:history")],
            [InlineKeyboardButton("📚 Extract Text", callback_data="menu:extract")],
            [InlineKeyboardButton("🌐 Website Snapshot", callback_data="menu:snapshot")],
            [
                InlineKeyboardButton("⚙️ Settings", callback_data="menu:settings"),
                InlineKeyboardButton("ℹ️ Help", callback_data="menu:help"),
            ],
        ]
    )


def link_options_keyboard(doc: ParsedDocument, page: int = 0) -> InlineKeyboardMarkup:
    """Options when user has submitted a link."""
    buttons: list[list[InlineKeyboardButton]] = []

    has_pdf = bool(doc.pdf_links)
    has_images = bool(doc.image_urls)
    has_text = len(doc.text_content.strip()) > 50

    if has_pdf:
        buttons.append([InlineKeyboardButton("⬇ Download as PDF", callback_data="fmt:pdf:0")])
    if has_images:
        buttons.append([InlineKeyboardButton("🖼 Download Images", callback_data="fmt:img:0")])
    if has_text:
        buttons.append([InlineKeyboardButton("📄 Extract Text", callback_data="fmt:txt:0")])
    buttons.append([InlineKeyboardButton("🌐 Save HTML", callback_data="fmt:html:0")])

    # If multiple PDFs/images, add pagination
    all_items = doc.pdf_links or doc.image_urls or [doc.url]
    total_pages = max(1, (len(all_items) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅ Prev Page", callback_data=f"page:{page-1}"))
    nav.append(InlineKeyboardButton("🔙 Back", callback_data="menu:main"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("➡ Next Page", callback_data=f"page:{page+1}"))
    buttons.append(nav)

    return InlineKeyboardMarkup(buttons)


def history_keyboard(page: int, total: int) -> InlineKeyboardMarkup:
    """Pagination for download history."""
    row = []
    if page > 0:
        row.append(InlineKeyboardButton("⬅ Prev", callback_data=f"hist:{page-1}"))
    row.append(InlineKeyboardButton("🔙 Back", callback_data="menu:main"))
    if page < total - 1:
        row.append(InlineKeyboardButton("➡ Next", callback_data=f"hist:{page+1}"))
    return InlineKeyboardMarkup([row])


async def cmd_start(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start command."""
    u = update.effective_user
    if not u:
        return
    if not user_mgr.is_authorized(u.id) and OWNER_ID:
        await update.message.reply_text(
            "❌ <b>Access Denied</b>\n\nContact the bot owner for access.",
            parse_mode=ParseMode.HTML,
        )
        return
    await update.message.reply_text(
        f"🚀 <b>Universal Document Downloader</b>\n\n"
        f"Welcome, {html_mod.escape(u.first_name or 'User')}!\n\n"
        f"📥 Send any public URL to download:\n"
        f"  • PDF documents\n"
        f"  • Images (converted to PDF/ZIP)\n"
        f"  • Webpage text & HTML snapshots\n\n"
        f"<i>Your ID: <code>{u.id}</code></i>",
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_keyboard(),
    )


async def cmd_help(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /help command."""
    is_owner = update.effective_user and update.effective_user.id == OWNER_ID
    owner_block = ""
    if is_owner:
        owner_block = (
            "\n\n👑 <b>Admin Commands:</b>\n"
            "  /add_user [id] — Add user\n"
            "  /remove_user [id] — Remove user\n"
            "  /users — List users\n"
            "  /broadcast [msg] — Broadcast to all"
        )
    await update.message.reply_text(
        "📖 <b>Help — Universal Document Downloader</b>\n\n"
        "1. Send a URL (e.g. https://example.com/doc.pdf)\n"
        "2. Choose format: PDF, Images, Text, or HTML\n"
        "3. Bot fetches, parses, and sends the file\n\n"
        "📥 <b>Supported:</b> PDF, images, HTML, TXT, DOCX links\n"
        "🖼 Images on a page → converted to PDF or ZIP\n"
        "📄 Text extraction from articles\n"
        "🌐 Full HTML snapshot of the page"
        f"{owner_block}",
        parse_mode=ParseMode.HTML,
    )


async def cmd_cancel(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /cancel command."""
    uid = update.effective_user.id if update.effective_user else 0
    _pending.pop(uid, None)
    await update.message.reply_text("✅ Cancelled.", reply_markup=main_menu_keyboard())


@auth_required
async def handle_url_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle user sending a URL."""
    if not update.message or not update.message.text:
        return
    uid = update.effective_user.id if update.effective_user else 0
    if not rate_limiter.check(uid):
        await update.message.reply_text("⏳ Rate limit exceeded. Please wait a minute.")
        return

    text = update.message.text.strip()
    match = URL_PATTERN.search(text)
    if not match:
        return
    url = match.group(0)
    if " " in url:
        url = url.split()[0]

    status = await update.message.reply_text("🚀 Starting download\n⚡ Processing link…")

    try:
        async with aiohttp.ClientSession() as sess:
            parser = ParserEngine(sess)
            doc = await parser.parse_url(url)

        _pending[uid] = {"url": url, "doc": doc, "page": 0}

        summary = []
        if doc.pdf_links:
            summary.append(f"📕 {len(doc.pdf_links)} PDF(s)")
        if doc.image_urls:
            summary.append(f"🖼 {len(doc.image_urls)} image(s)")
        if doc.text_content.strip():
            summary.append(f"📄 {len(doc.text_content)} chars text")
        summary_str = " · ".join(summary) if summary else "📄 Content detected"

        await status.edit_text(
            f"✅ <b>Link Analyzed</b>\n\n"
            f"📖 {html_mod.escape(doc.title[:80])}\n"
            f"{summary_str}\n\n"
            f"Select option:",
            parse_mode=ParseMode.HTML,
            reply_markup=link_options_keyboard(doc),
        )
    except Exception as exc:
        logger.error("Parse failed: %s", exc, exc_info=True)
        await status.edit_text(
            f"❌ Error\n<code>{html_mod.escape(str(exc)[:300])}</code>",
            parse_mode=ParseMode.HTML,
        )


async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle inline keyboard callbacks."""
    query = update.callback_query
    if not query:
        return
    await query.answer()
    uid = query.from_user.id if query.from_user else 0
    if not user_mgr.is_authorized(uid):
        await query.answer("❌ Not authorized", show_alert=True)
        return

    data = query.data or ""
    parts = data.split(":")

    # Menu navigation
    if parts[0] == "menu":
        if parts[1] == "main":
            await query.edit_message_text(
                "📂 <b>Main Menu</b>\n\nSelect an option:",
                parse_mode=ParseMode.HTML,
                reply_markup=main_menu_keyboard(),
            )
        elif parts[1] == "download":
            await query.edit_message_text(
                "📥 <b>Download Document</b>\n\nSend any public URL to download.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="menu:main")]]
                ),
            )
        elif parts[1] == "history":
            await show_history(query, 0)
        elif parts[1] == "extract":
            await query.edit_message_text(
                "📚 <b>Extract Text</b>\n\nSend a URL to extract readable text.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="menu:main")]]
                ),
            )
        elif parts[1] == "snapshot":
            await query.edit_message_text(
                "🌐 <b>Website Snapshot</b>\n\nSend a URL to save as HTML.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="menu:main")]]
                ),
            )
        elif parts[1] == "settings":
            await query.edit_message_text(
                "⚙️ <b>Settings</b>\n\nNo settings available.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="menu:main")]]
                ),
            )
        elif parts[1] == "help":
            await query.edit_message_text(
                "ℹ️ <b>Help</b>\n\nSend a URL to get started. Use the buttons to choose format.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="menu:main")]]
                ),
            )
        return

    # Pagination
    if parts[0] == "page":
        page = int(parts[1]) if len(parts) > 1 else 0
        info = _pending.get(uid)
        if info:
            info["page"] = page
            doc = info["doc"]
            await query.edit_message_reply_markup(reply_markup=link_options_keyboard(doc, page))
        return

    if parts[0] == "hist":
        page = int(parts[1]) if len(parts) > 1 else 0
        await show_history(query, page)
        return

    # Format selection -> trigger download
    if parts[0] == "fmt" and len(parts) >= 2:
        fmt = parts[1]
        info = _pending.get(uid)
        if not info:
            await query.edit_message_text("❌ Session expired. Send the URL again.")
            return
        await execute_download(query, ctx, info, fmt)
        return


async def show_history(query, page: int) -> None:
    """Show download history with pagination."""
    uid = query.from_user.id if query.from_user else 0
    recent = download_history.get_recent(uid, limit=50)
    total_pages = max(1, (len(recent) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    start = page * ITEMS_PER_PAGE
    items = recent[start : start + ITEMS_PER_PAGE]

    lines = []
    for i, h in enumerate(items, start + 1):
        status = "✅" if h.get("success", True) else "❌"
        fmt = h.get("format", "?")
        url_short = (h.get("url", "")[:40] + "…") if len(h.get("url", "")) > 40 else h.get("url", "")
        lines.append(f"{i}. {status} {fmt} — {url_short}")

    text = "📂 <b>My Downloads</b>\n\n" + ("\n".join(lines) if lines else "No downloads yet.")
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=history_keyboard(page, total_pages),
    )


async def execute_download(
    query, ctx: ContextTypes.DEFAULT_TYPE, info: dict, fmt: str
) -> None:
    """Execute the actual download and send file."""
    uid = query.from_user.id if query.from_user else 0
    url = info["url"]
    doc = info["doc"]
    chat_id = query.message.chat_id if query.message else 0

    async with _download_semaphore:
        try:
            await ctx.bot.send_chat_action(chat_id, ChatAction.UPLOAD_DOCUMENT)
            await query.edit_message_text(
                f"📥 Downloading assets\n⚡ Processing…",
                parse_mode=ParseMode.HTML,
            )

            async with aiohttp.ClientSession() as sess:
                engine = DownloadEngine(sess)
                parser = ParserEngine(sess)

                if fmt == "pdf":
                    if doc.pdf_links:
                        data = await engine.fetch_bytes(doc.pdf_links[0])
                    elif doc.image_urls:
                        await query.edit_message_text("📦 Downloading images…")
                        blobs = await engine.download_images(
                            doc.image_urls,
                            progress_cb=lambda c, t: None,
                        )
                        if not blobs:
                            raise ValueError("No images downloaded")
                        data = engine.images_to_pdf(blobs)
                    else:
                        raise ValueError("No PDF or images found")
                    filename = f"{_safe_filename(doc.title)}.pdf"

                elif fmt == "img":
                    if not doc.image_urls:
                        raise ValueError("No images found")
                    await query.edit_message_text("📦 Packaging files…")
                    blobs = await engine.download_images(doc.image_urls)
                    if not blobs:
                        raise ValueError("Image download failed")
                    data = engine.images_to_zip(blobs, _safe_filename(doc.title))
                    filename = f"{_safe_filename(doc.title)}_images.zip"

                elif fmt == "txt":
                    text = doc.text_content.strip()
                    if len(text) < 20:
                        raise ValueError("Insufficient text content")
                    header = f"Source: {url}\nTitle: {doc.title}\n{'='*60}\n\n"
                    data = (header + text).encode()
                    filename = f"{_safe_filename(doc.title)}.txt"

                elif fmt == "html":
                    data = doc.html_content.encode() if doc.html_content else doc.raw_html.encode()
                    filename = f"{_safe_filename(doc.title)}.html"

                else:
                    raise ValueError(f"Unknown format: {fmt}")

                # Size check
                size_mb = len(data) / 1024 / 1024
                if size_mb > MAX_FILE_SIZE_MB:
                    raise ValueError(f"File too large ({size_mb:.1f} MB). Max: {MAX_FILE_SIZE_MB} MB")

                await query.edit_message_text("📤 Uploading to Telegram…")
                await ctx.bot.send_document(
                    chat_id=chat_id,
                    document=io.BytesIO(data),
                    filename=filename,
                    caption=f"✅ {html_mod.escape(doc.title[:100])}\n📥 Universal Document Bot",
                    parse_mode=ParseMode.HTML,
                )
                await query.edit_message_text("✅ Completed")
                download_history.add(uid, url, fmt, True)

        except Exception as exc:
            logger.error("Download failed: %s", exc, exc_info=True)
            await query.edit_message_text(
                f"❌ Error\n<code>{html_mod.escape(str(exc)[:300])}</code>",
                parse_mode=ParseMode.HTML,
            )
            download_history.add(uid, url, fmt, False)
        finally:
            _pending.pop(uid, None)


# ═══════════════════════════════════════════════════════════════════════════
# 7. ADMIN COMMANDS
# ═══════════════════════════════════════════════════════════════════════════


@owner_only
async def cmd_add_user(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Add user by ID."""
    if not ctx.args:
        await update.message.reply_text("⚠️ Usage: /add_user [id]")
        return
    lines = []
    for arg in ctx.args:
        try:
            uid = int(arg)
            if user_mgr.add(uid):
                lines.append(f"✅ {uid} added")
            else:
                lines.append(f"ℹ️ {uid} already exists")
        except ValueError:
            lines.append(f"❌ Invalid: {arg}")
    await update.message.reply_text("\n".join(lines))


@owner_only
async def cmd_remove_user(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove user by ID."""
    if not ctx.args:
        await update.message.reply_text("⚠️ Usage: /remove_user [id]")
        return
    lines = []
    for arg in ctx.args:
        try:
            uid = int(arg)
            if user_mgr.remove(uid):
                lines.append(f"✅ {uid} removed")
            else:
                lines.append(f"❌ {uid} — owner or not found")
        except ValueError:
            lines.append(f"❌ Invalid: {arg}")
    await update.message.reply_text("\n".join(lines))


@owner_only
async def cmd_users(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """List all users."""
    ids = user_mgr.all_ids
    listing = "\n".join(f"  {'👑' if u == OWNER_ID else '👤'} {u}" for u in ids)
    await update.message.reply_text(f"👥 Users ({len(ids)}):\n\n{listing}")


@owner_only
async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Broadcast message to all users."""
    if not ctx.args:
        await update.message.reply_text("⚠️ Usage: /broadcast [message]")
        return
    msg = " ".join(ctx.args)
    sent = failed = 0
    for uid in user_mgr.all_ids:
        try:
            await ctx.bot.send_message(
                uid,
                f"📢 <b>Broadcast</b>\n\n{html_mod.escape(msg)}",
                parse_mode=ParseMode.HTML,
            )
            sent += 1
        except Exception:
            failed += 1
    await update.message.reply_text(f"📢 Done — ✅ {sent} sent, ❌ {failed} failed")


# ═══════════════════════════════════════════════════════════════════════════
# 8. MAIN ASYNC RUNNER
# ═══════════════════════════════════════════════════════════════════════════


def main() -> None:
    """Entry point."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    if BOT_TOKEN in ("YOUR_BOT_TOKEN_HERE", ""):
        logger.error("Set BOT_TOKEN environment variable.")
        return
    if OWNER_ID == 0:
        logger.warning("OWNER_ID is 0 — admin commands disabled.")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("add_user", cmd_add_user))
    app.add_handler(CommandHandler("remove_user", cmd_remove_user))
    app.add_handler(CommandHandler("users", cmd_users))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))

    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_url_message,
        )
    )
    app.add_handler(CallbackQueryHandler(handle_callback))

    logger.info("🚀 Universal Document Bot starting (owner=%s)", OWNER_ID)
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()

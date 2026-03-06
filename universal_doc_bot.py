#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Universal Document Downloader & Viewer — Telegram Bot (v1.0)
============================================================
Production-ready async Telegram bot that fetches any public webpage,
detects downloadable assets (PDF, images, HTML, TXT, DOCX), extracts
readable text, converts image sequences to PDF, and packages downloads.

Optimized for Termux / Ubuntu VPS — Python 3.11+.

Dependencies:
    pip install python-telegram-bot[ext] aiohttp beautifulsoup4 Pillow lxml
"""

# ═══════════════════════════════════════════════════════════════════════════════
# 1. IMPORTS
# ═══════════════════════════════════════════════════════════════════════════════

from __future__ import annotations

import asyncio
import hashlib
import html as html_mod
import io
import json
import logging
import math
import os
import re
import shutil
import sys
import tempfile
import time
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup
from PIL import Image
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ═══════════════════════════════════════════════════════════════════════════════
# 2. CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
OWNER_ID: int = int(os.getenv("OWNER_ID", "0"))

DATA_DIR = Path("bot_data")
USERS_FILE = DATA_DIR / "users.json"
HISTORY_FILE = DATA_DIR / "history.json"
STATS_FILE = DATA_DIR / "stats.json"
CACHE_DIR = DATA_DIR / "cache"
TEMP_DIR = DATA_DIR / "temp"

TELEGRAM_FILE_LIMIT = 50 * 1024 * 1024  # 50 MB
MAX_IMAGES = 200
MAX_CONCURRENT = 5
RATE_WINDOW = 60  # seconds
RATE_MAX = 10  # requests per window
CACHE_TTL = 3600  # 1 hour
PAGE_SIZE = 5

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=120, connect=30)
DOWNLOAD_TIMEOUT = aiohttp.ClientTimeout(total=300, connect=30)

BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

DOC_EXTENSIONS = frozenset({
    ".pdf", ".doc", ".docx", ".xls", ".xlsx",
    ".ppt", ".pptx", ".odt", ".ods", ".odp",
    ".txt", ".csv", ".rtf", ".epub",
})

IMG_EXTENSIONS = frozenset({
    ".jpg", ".jpeg", ".png", ".gif", ".webp",
    ".bmp", ".tiff", ".ico",
})

URL_RE = re.compile(r"https?://[^\s<>\"']+", re.I)

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
logger = logging.getLogger("UnivDocBot")

# ═══════════════════════════════════════════════════════════════════════════════
# 3. HELPER UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════


def _ensure_dirs() -> None:
    for d in (DATA_DIR, CACHE_DIR, TEMP_DIR):
        d.mkdir(parents=True, exist_ok=True)


def fmt_size(n: int | float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024.0:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} TB"


def sanitize(name: str, limit: int = 60) -> str:
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    name = re.sub(r"_+", "_", name).strip("_. ")
    return name[:limit] or "document"


def valid_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def ext_of(url: str) -> str:
    ext = Path(urlparse(url).path).suffix.lower()
    return ext if len(ext) <= 10 else ""


def esc(text: str) -> str:
    return html_mod.escape(text)


# ── Rate limiter ──────────────────────────────────────────────────────────────


class RateLimiter:
    def __init__(self, window: int = RATE_WINDOW, cap: int = RATE_MAX) -> None:
        self._w = window
        self._c = cap
        self._log: dict[int, list[float]] = defaultdict(list)

    def allow(self, uid: int) -> bool:
        now = time.monotonic()
        self._log[uid] = [t for t in self._log[uid] if now - t < self._w]
        if len(self._log[uid]) >= self._c:
            return False
        self._log[uid].append(now)
        return True

    def remaining(self, uid: int) -> int:
        now = time.monotonic()
        recent = sum(1 for t in self._log.get(uid, []) if now - t < self._w)
        return max(0, self._c - recent)


rate_limiter = RateLimiter()

# ── In-memory cache ──────────────────────────────────────────────────────────


class TTLCache:
    def __init__(self, ttl: int = CACHE_TTL) -> None:
        self._ttl = ttl
        self._data: dict[str, tuple[float, Any]] = {}

    def get(self, key: str) -> Any | None:
        if key in self._data:
            ts, val = self._data[key]
            if time.monotonic() - ts < self._ttl:
                return val
            del self._data[key]
        return None

    def put(self, key: str, val: Any) -> None:
        self._data[key] = (time.monotonic(), val)

    def purge(self) -> int:
        now = time.monotonic()
        stale = [k for k, (ts, _) in self._data.items() if now - ts >= self._ttl]
        for k in stale:
            del self._data[k]
        return len(stale)

    @property
    def size(self) -> int:
        return len(self._data)


page_cache = TTLCache()

# ═══════════════════════════════════════════════════════════════════════════════
# 4. USER MANAGER — persistent JSON storage with history & stats
# ═══════════════════════════════════════════════════════════════════════════════


class UserManager:
    def __init__(self) -> None:
        _ensure_dirs()
        self._users: set[int] = set()
        self._history: dict[str, list[dict]] = {}
        self._stats: dict[str, dict] = {}
        self._load()

    # ── persistence ───────────────────────────────────────────────────────

    def _load(self) -> None:
        self._users = self._read_json(USERS_FILE, "users", set, lambda d: set(d.get("users", [])))
        if OWNER_ID:
            self._users.add(OWNER_ID)
        self._flush_users()
        self._history = self._read_json(HISTORY_FILE, "history", dict, lambda d: d)
        self._stats = self._read_json(STATS_FILE, "stats", dict, lambda d: d)

    @staticmethod
    def _read_json(path: Path, label: str, default_factory, transform):
        if path.exists():
            try:
                return transform(json.loads(path.read_text()))
            except Exception:
                logger.warning("Corrupt %s file — resetting", label)
        return default_factory()

    def _flush_users(self) -> None:
        USERS_FILE.write_text(json.dumps({"users": sorted(self._users)}, indent=2))

    def _flush_history(self) -> None:
        HISTORY_FILE.write_text(json.dumps(self._history, indent=2, default=str))

    def _flush_stats(self) -> None:
        STATS_FILE.write_text(json.dumps(self._stats, indent=2, default=str))

    # ── user CRUD ─────────────────────────────────────────────────────────

    def add(self, uid: int) -> bool:
        if uid in self._users:
            return False
        self._users.add(uid)
        self._flush_users()
        return True

    def remove(self, uid: int) -> bool:
        if uid == OWNER_ID or uid not in self._users:
            return False
        self._users.discard(uid)
        self._flush_users()
        return True

    def authorized(self, uid: int) -> bool:
        return uid in self._users

    @property
    def all_ids(self) -> list[int]:
        return sorted(self._users)

    # ── download history & stats ──────────────────────────────────────────

    def record(self, uid: int, url: str, fmt: str, title: str) -> None:
        key = str(uid)
        self._history.setdefault(key, []).append({
            "url": url, "format": fmt,
            "title": title[:80],
            "time": datetime.now(timezone.utc).isoformat(),
        })
        self._history[key] = self._history[key][-50:]
        self._flush_history()

        s = self._stats.setdefault(key, {
            "downloads": 0,
            "first_seen": datetime.now(timezone.utc).isoformat(),
        })
        s["downloads"] = s.get("downloads", 0) + 1
        s["last_active"] = datetime.now(timezone.utc).isoformat()
        self._flush_stats()

    def history(self, uid: int) -> list[dict]:
        return list(reversed(self._history.get(str(uid), [])))

    def stats(self, uid: int) -> dict:
        return self._stats.get(str(uid), {"downloads": 0})

    def global_summary(self) -> str:
        total_dl = sum(s.get("downloads", 0) for s in self._stats.values())
        return f"Users: {len(self._users)} | Downloads: {total_dl}"


users = UserManager()

# ═══════════════════════════════════════════════════════════════════════════════
# 5. DOWNLOAD ENGINE — async fetch, streaming, retry, concurrency
# ═══════════════════════════════════════════════════════════════════════════════


class Downloader:
    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session
        self._sem = asyncio.Semaphore(MAX_CONCURRENT)

    async def text(self, url: str, *, retries: int = 3) -> str:
        return (await self._get(url, retries=retries, as_bytes=False))[0]

    async def raw(self, url: str, *, retries: int = 3, cap: int = TELEGRAM_FILE_LIMIT) -> bytes:
        return (await self._get(url, retries=retries, as_bytes=True, cap=cap))[0]

    async def _get(
        self, url: str, *, retries: int, as_bytes: bool, cap: int = 0
    ) -> tuple[Any, int]:
        last: Exception | None = None
        for attempt in range(retries):
            try:
                async with self._sem, self.session.get(
                    url, headers=BROWSER_HEADERS,
                    timeout=DOWNLOAD_TIMEOUT if as_bytes else REQUEST_TIMEOUT,
                    allow_redirects=True, ssl=False,
                ) as r:
                    r.raise_for_status()
                    if as_bytes:
                        if cap and r.content_length and r.content_length > cap:
                            raise ValueError(f"File too large ({fmt_size(r.content_length)})")
                        chunks, total = [], 0
                        async for ch in r.content.iter_chunked(65_536):
                            total += len(ch)
                            if cap and total > cap:
                                raise ValueError(f"Exceeds {fmt_size(cap)} limit")
                            chunks.append(ch)
                        return b"".join(chunks), r.status
                    return await r.text(errors="replace"), r.status
            except ValueError:
                raise
            except Exception as exc:
                last = exc
                if attempt < retries - 1:
                    await asyncio.sleep(1.5 ** attempt)
        raise last or RuntimeError("download failed")

    async def to_file(self, url: str, dest: Path, *, cap: int = TELEGRAM_FILE_LIMIT) -> int:
        async with self._sem, self.session.get(
            url, headers=BROWSER_HEADERS,
            timeout=DOWNLOAD_TIMEOUT, allow_redirects=True, ssl=False,
        ) as r:
            r.raise_for_status()
            total = 0
            with open(dest, "wb") as f:
                async for ch in r.content.iter_chunked(65_536):
                    total += len(ch)
                    if total > cap:
                        raise ValueError(f"Exceeds {fmt_size(cap)}")
                    f.write(ch)
            return total

    async def batch(self, urls: list[str], dest: Path, prefix: str = "file") -> list[Path]:
        async def _one(i: int, u: str) -> Path | None:
            ext = ext_of(u) or ".jpg"
            p = dest / f"{prefix}_{i:04d}{ext}"
            try:
                await self.to_file(u, p)
                return p
            except Exception as exc:
                logger.warning("Batch download %d failed: %s", i, exc)
                return None

        results = await asyncio.gather(
            *(_one(i, u) for i, u in enumerate(urls[:MAX_IMAGES]))
        )
        return [p for p in results if p is not None]

# ═══════════════════════════════════════════════════════════════════════════════
# 6. PARSER ENGINE — detect PDFs, images, documents, text from any page
# ═══════════════════════════════════════════════════════════════════════════════


class ParseResult:
    __slots__ = (
        "url", "title", "description",
        "pdfs", "images", "docs",
        "text", "html",
    )

    def __init__(self, url: str) -> None:
        self.url = url
        self.title: str = ""
        self.description: str = ""
        self.pdfs: list[str] = []
        self.images: list[str] = []
        self.docs: list[str] = []
        self.text: str = ""
        self.html: str = ""

    def summary_text(self) -> str:
        lines = [f"<b>{esc(self.title or 'Untitled Page')}</b>"]
        if self.description:
            lines.append(f"<i>{esc(self.description[:200])}</i>")
        lines.append("")
        lines.append(f"PDFs found: <b>{len(self.pdfs)}</b>")
        lines.append(f"Images found: <b>{len(self.images)}</b>")
        lines.append(f"Other documents: <b>{len(self.docs)}</b>")
        lines.append(f"Text length: <b>{len(self.text):,}</b> chars")
        return "\n".join(lines)


class Parser:
    def __init__(self, dl: Downloader) -> None:
        self.dl = dl

    async def analyse(self, url: str) -> ParseResult:
        cached = page_cache.get(url)
        if cached is not None:
            return cached

        html_src = await self.dl.text(url)
        soup = BeautifulSoup(html_src, HTML_PARSER)
        r = ParseResult(url)
        r.html = html_src

        # ── title / description ───────────────────────────────────────────
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            r.title = og["content"]
        elif soup.title and soup.title.string:
            r.title = soup.title.string.strip()

        for tag in (
            soup.find("meta", property="og:description"),
            soup.find("meta", attrs={"name": "description"}),
        ):
            if tag and tag.get("content"):
                r.description = tag["content"][:300]
                break

        # ── links ─────────────────────────────────────────────────────────
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = urljoin(url, a["href"])
            if href in seen:
                continue
            seen.add(href)
            e = ext_of(href)
            if e == ".pdf":
                r.pdfs.append(href)
            elif e in DOC_EXTENSIONS - {".pdf"}:
                r.docs.append(href)

        # embedded objects / iframes that point to PDFs
        for tag_name, attr in (("embed", "src"), ("object", "data"), ("iframe", "src")):
            for tag in soup.find_all(tag_name, attrs={attr: True}):
                src = urljoin(url, tag[attr])
                if src not in seen and (
                    ".pdf" in src.lower()
                    or "pdf" in (tag.get("type") or "").lower()
                ):
                    seen.add(src)
                    r.pdfs.append(src)

        # ── images ────────────────────────────────────────────────────────
        img_seen: set[str] = set()

        for img in soup.find_all("img", src=True):
            src = urljoin(url, img["src"])
            if src not in img_seen and ext_of(src) in IMG_EXTENSIONS:
                img_seen.add(src)
                r.images.append(src)

        for tag in soup.find_all(attrs={"srcset": True}):
            for part in tag["srcset"].split(","):
                src = urljoin(url, part.strip().split()[0])
                if src not in img_seen and ext_of(src) in IMG_EXTENSIONS:
                    img_seen.add(src)
                    r.images.append(src)

        for tag in soup.find_all(style=True):
            for m in re.finditer(r'url\(["\']?([^"\')\s]+)["\']?\)', tag["style"]):
                src = urljoin(url, m.group(1))
                if src not in img_seen and ext_of(src) in IMG_EXTENSIONS:
                    img_seen.add(src)
                    r.images.append(src)

        # ── readable text ─────────────────────────────────────────────────
        for tag in soup(["script", "style", "nav", "header", "footer", "noscript"]):
            tag.decompose()
        raw_text = soup.get_text(separator="\n", strip=True)
        r.text = "\n".join(ln.strip() for ln in raw_text.splitlines() if ln.strip())

        page_cache.put(url, r)
        return r

# ═══════════════════════════════════════════════════════════════════════════════
# 7. FILE PROCESSOR — images→PDF, ZIP packaging, snapshots
# ═══════════════════════════════════════════════════════════════════════════════


class FileProc:

    @staticmethod
    async def imgs_to_pdf(paths: list[Path]) -> bytes:
        if not paths:
            raise ValueError("No images provided")
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, FileProc._sync_img_pdf, paths)

    @staticmethod
    def _sync_img_pdf(paths: list[Path]) -> bytes:
        imgs: list[Image.Image] = []
        for p in paths:
            try:
                im = Image.open(p)
                if im.mode in ("RGBA", "P", "LA"):
                    im = im.convert("RGB")
                imgs.append(im)
            except Exception as exc:
                logger.warning("Skip bad image %s: %s", p.name, exc)
        if not imgs:
            raise ValueError("No valid images after filtering")
        buf = io.BytesIO()
        imgs[0].save(buf, "PDF", save_all=True, append_images=imgs[1:], resolution=150)
        for im in imgs:
            im.close()
        return buf.getvalue()

    @staticmethod
    async def make_zip(files: list[Path]) -> bytes:
        buf = io.BytesIO()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, FileProc._sync_zip, files, buf)
        return buf.getvalue()

    @staticmethod
    def _sync_zip(files: list[Path], buf: io.BytesIO) -> None:
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in files:
                zf.write(f, f.name)

    @staticmethod
    def snapshot_bytes(html: str, url: str) -> bytes:
        header = (
            f"<!-- Snapshot of {url} -->\n"
            f"<!-- Captured: {datetime.now(timezone.utc).isoformat()} -->\n"
        )
        return (header + html).encode("utf-8", errors="replace")

# ═══════════════════════════════════════════════════════════════════════════════
# 8. TELEGRAM HANDLERS — commands, inline keyboards, pagination
# ═══════════════════════════════════════════════════════════════════════════════

# ── decorators ────────────────────────────────────────────────────────────────


def auth(fn):
    @wraps(fn)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE, *a, **kw):
        uid = update.effective_user.id
        if not users.authorized(uid):
            msg = "You are not authorized. Contact the admin for access."
            if update.callback_query:
                await update.callback_query.answer(msg, show_alert=True)
            elif update.message:
                await update.message.reply_text(msg)
            return
        return await fn(update, ctx, *a, **kw)
    return wrapper


def owner(fn):
    @wraps(fn)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE, *a, **kw):
        if update.effective_user.id != OWNER_ID:
            await update.message.reply_text("Owner-only command.")
            return
        return await fn(update, ctx, *a, **kw)
    return wrapper


# ── shared helpers ────────────────────────────────────────────────────────────

def _session(ctx: ContextTypes.DEFAULT_TYPE) -> aiohttp.ClientSession:
    s = ctx.bot_data.get("session")
    if s is None or s.closed:
        s = aiohttp.ClientSession()
        ctx.bot_data["session"] = s
    return s


MAIN_KB = InlineKeyboardMarkup([
    [
        InlineKeyboardButton("Download Document", callback_data="m:dl"),
        InlineKeyboardButton("My Downloads", callback_data="m:hist"),
    ],
    [
        InlineKeyboardButton("Extract Text", callback_data="m:txt"),
        InlineKeyboardButton("Website Snapshot", callback_data="m:snap"),
    ],
    [
        InlineKeyboardButton("Settings", callback_data="m:cfg"),
        InlineKeyboardButton("Help", callback_data="m:help"),
    ],
])

BACK_KB = InlineKeyboardMarkup(
    [[InlineKeyboardButton("Back", callback_data="m:back")]]
)

WELCOME = (
    "<b>Universal Document Downloader</b>\n\n"
    "Send me any public URL and I will detect downloadable assets.\n"
    "Choose an option below or paste a link to get started."
)

# ── /start, /help, /cancel, /mystats ─────────────────────────────────────────


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    if uid == OWNER_ID:
        users.add(uid)
    if not users.authorized(uid):
        await update.message.reply_text(
            "Welcome! You are not yet authorized.\n"
            "Contact the bot owner to request access."
        )
        return
    await update.message.reply_text(WELCOME, parse_mode=ParseMode.HTML, reply_markup=MAIN_KB)


async def cmd_help(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "<b>How to use this bot</b>\n\n"
        "1. Send any public URL.\n"
        "2. The bot scans for downloadable content.\n"
        "3. Choose your preferred download format.\n\n"
        "<b>Supported:</b> PDF, Images, HTML, TXT, DOCX\n\n"
        "<b>Commands</b>\n"
        "/start — Main menu\n"
        "/help — This message\n"
        "/cancel — Cancel current operation\n"
        "/mystats — Your download statistics\n\n"
        "<b>Admin</b>\n"
        "/add_user &lt;id&gt; — Authorize user\n"
        "/remove_user &lt;id&gt; — Revoke access\n"
        "/users — List authorized users\n"
        "/broadcast &lt;msg&gt; — Message all users\n"
        "/botstats — Global statistics",
        parse_mode=ParseMode.HTML,
    )


async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    ctx.user_data.clear()
    await update.message.reply_text("Operation cancelled.", reply_markup=MAIN_KB)


@auth
async def cmd_mystats(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    s = users.stats(uid)
    await update.message.reply_text(
        f"<b>Your Statistics</b>\n\n"
        f"Downloads: <b>{s.get('downloads', 0)}</b>\n"
        f"First seen: {s.get('first_seen', 'N/A')}\n"
        f"Last active: {s.get('last_active', 'N/A')}",
        parse_mode=ParseMode.HTML,
    )


# ── main menu callbacks ──────────────────────────────────────────────────────


@auth
async def cb_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    tag = q.data  # m:dl, m:hist, …

    if tag == "m:dl":
        ctx.user_data["mode"] = "download"
        await q.edit_message_text(
            "Send me a URL and I will find all downloadable files.",
            reply_markup=BACK_KB,
        )
    elif tag == "m:txt":
        ctx.user_data["mode"] = "extract"
        await q.edit_message_text(
            "Send me a URL and I will extract readable text.",
            reply_markup=BACK_KB,
        )
    elif tag == "m:snap":
        ctx.user_data["mode"] = "snapshot"
        await q.edit_message_text(
            "Send me a URL and I will save an HTML snapshot.",
            reply_markup=BACK_KB,
        )
    elif tag == "m:hist":
        await _show_history(q, ctx, 0)
    elif tag == "m:cfg":
        rem = rate_limiter.remaining(update.effective_user.id)
        await q.edit_message_text(
            f"<b>Settings</b>\n\n"
            f"Rate limit: {RATE_MAX} req / {RATE_WINDOW}s\n"
            f"Remaining: {rem}\n"
            f"Max file size: {fmt_size(TELEGRAM_FILE_LIMIT)}\n"
            f"Cache TTL: {CACHE_TTL}s\n"
            f"Cached pages: {page_cache.size}",
            parse_mode=ParseMode.HTML,
            reply_markup=BACK_KB,
        )
    elif tag == "m:help":
        await q.edit_message_text(
            "<b>Quick Help</b>\n\n"
            "1. Paste any public URL.\n"
            "2. Bot scans for PDFs, images, documents.\n"
            "3. Pick a download format.\n\n"
            "Supported: PDF, Images, HTML, TXT, DOCX",
            parse_mode=ParseMode.HTML,
            reply_markup=BACK_KB,
        )
    elif tag == "m:back":
        ctx.user_data.pop("mode", None)
        await q.edit_message_text(WELCOME, parse_mode=ParseMode.HTML, reply_markup=MAIN_KB)


# ── history with pagination ──────────────────────────────────────────────────


async def _show_history(q, ctx: ContextTypes.DEFAULT_TYPE, page: int) -> None:
    items = users.history(q.from_user.id)
    if not items:
        await q.edit_message_text("No download history yet.", reply_markup=BACK_KB)
        return

    pages = math.ceil(len(items) / PAGE_SIZE)
    page = max(0, min(page, pages - 1))
    sl = items[page * PAGE_SIZE : (page + 1) * PAGE_SIZE]

    lines = [f"<b>Download History</b>  (page {page + 1}/{pages})\n"]
    for i, it in enumerate(sl, page * PAGE_SIZE + 1):
        lines.append(
            f"<b>{i}.</b> {esc(it.get('title', '?'))}\n"
            f"    {it.get('format', '?')} | {it.get('time', '')[:10]}"
        )

    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton("Prev Page", callback_data=f"hist:{page - 1}"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("Next Page", callback_data=f"hist:{page + 1}"))
    kb: list[list[InlineKeyboardButton]] = []
    if nav:
        kb.append(nav)
    kb.append([InlineKeyboardButton("Back", callback_data="m:back")])
    await q.edit_message_text(
        "\n".join(lines), parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(kb),
    )


@auth
async def cb_history(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    page = int(q.data.split(":")[1])
    await _show_history(q, ctx, page)


# ── URL message handler ──────────────────────────────────────────────────────


@auth
async def handle_url(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    m = URL_RE.search(update.message.text or "")
    if not m:
        return
    url = m.group(0)
    if not valid_url(url):
        await update.message.reply_text("That doesn't look like a valid URL.")
        return

    uid = update.effective_user.id
    if not rate_limiter.allow(uid):
        await update.message.reply_text(
            f"Rate limit reached. Try again in {RATE_WINDOW}s."
        )
        return

    mode = ctx.user_data.get("mode")
    if mode == "extract":
        await _do_extract(update, ctx, url)
        return
    if mode == "snapshot":
        await _do_snapshot(update, ctx, url)
        return

    status = await update.message.reply_text("Scanning URL ...")

    try:
        dl = Downloader(_session(ctx))
        result = await Parser(dl).analyse(url)
        ctx.user_data["pr"] = result
        ctx.user_data["url"] = url

        kb: list[list[InlineKeyboardButton]] = []
        if result.pdfs:
            kb.append([InlineKeyboardButton(
                f"Download PDFs ({len(result.pdfs)})", callback_data="a:pdfs:0",
            )])
        if result.images:
            n = min(len(result.images), MAX_IMAGES)
            kb.append([InlineKeyboardButton(
                f"Download Images ({n})", callback_data="a:imgs",
            )])
            kb.append([InlineKeyboardButton(
                "Images as PDF", callback_data="a:i2p",
            )])
        if result.docs:
            kb.append([InlineKeyboardButton(
                f"Download Documents ({len(result.docs)})", callback_data="a:docs:0",
            )])
        if result.text:
            kb.append([InlineKeyboardButton(
                "Extract Text", callback_data="a:txt",
            )])
        kb.append([InlineKeyboardButton("Save HTML Snapshot", callback_data="a:html")])
        kb.append([InlineKeyboardButton("Back to Menu", callback_data="m:back")])

        await status.edit_text(
            f"Scan complete!\n\n{result.summary_text()}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(kb),
        )
    except Exception as exc:
        logger.error("Scan failed for %s: %s", url, exc)
        await status.edit_text(
            f"Failed to scan URL.\n\nError: {esc(str(exc)[:200])}",
            parse_mode=ParseMode.HTML,
        )


# ── quick-mode helpers ────────────────────────────────────────────────────────


async def _do_extract(update: Update, ctx: ContextTypes.DEFAULT_TYPE, url: str) -> None:
    msg = await update.message.reply_text("Extracting text ...")
    try:
        dl = Downloader(_session(ctx))
        r = await Parser(dl).analyse(url)
        if not r.text:
            await msg.edit_text("No readable text found on this page.")
            return
        if len(r.text) <= 4000:
            await msg.edit_text(
                f"<b>Extracted Text</b>\n\n<pre>{esc(r.text[:3900])}</pre>",
                parse_mode=ParseMode.HTML,
            )
        else:
            buf = io.BytesIO(r.text.encode("utf-8"))
            fname = f"{sanitize(r.title)}.txt"
            await msg.edit_text("Uploading text file ...")
            await update.message.reply_document(
                document=buf, filename=fname,
                caption=f"Extracted text from {url[:100]}",
            )
        users.record(update.effective_user.id, url, "TXT", r.title)
    except Exception as exc:
        logger.error("Text extract failed: %s", exc)
        await msg.edit_text(f"Error: {esc(str(exc)[:200])}", parse_mode=ParseMode.HTML)


async def _do_snapshot(update: Update, ctx: ContextTypes.DEFAULT_TYPE, url: str) -> None:
    msg = await update.message.reply_text("Saving HTML snapshot ...")
    try:
        dl = Downloader(_session(ctx))
        r = await Parser(dl).analyse(url)
        data = FileProc.snapshot_bytes(r.html, url)
        fname = f"{sanitize(r.title)}.html"
        await msg.edit_text("Uploading snapshot ...")
        await update.message.reply_document(
            document=io.BytesIO(data), filename=fname,
            caption=f"HTML snapshot of {url[:100]}",
        )
        users.record(update.effective_user.id, url, "HTML", r.title)
    except Exception as exc:
        logger.error("Snapshot failed: %s", exc)
        await msg.edit_text(f"Error: {esc(str(exc)[:200])}", parse_mode=ParseMode.HTML)


# ── action callbacks (download / convert) ─────────────────────────────────────


@auth
async def cb_action(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":")
    act = parts[1]

    pr: ParseResult | None = ctx.user_data.get("pr")
    url: str = ctx.user_data.get("url", "")
    if pr is None:
        await q.edit_message_text("Session expired — please resend the URL.", reply_markup=MAIN_KB)
        return

    dl = Downloader(_session(ctx))

    try:
        if act == "pdfs":
            await _list_pdfs(q, ctx, pr, int(parts[2]) if len(parts) > 2 else 0)
        elif act == "docs":
            await _list_docs(q, ctx, pr, int(parts[2]) if len(parts) > 2 else 0)
        elif act == "gpdf":
            await _dl_single(q, ctx, dl, pr.pdfs, int(parts[2]), url, "PDF")
        elif act == "gdoc":
            await _dl_single(q, ctx, dl, pr.docs, int(parts[2]), url, "DOC")
        elif act == "imgs":
            await _dl_images_zip(q, ctx, dl, pr, url)
        elif act == "i2p":
            await _dl_images_pdf(q, ctx, dl, pr, url)
        elif act == "txt":
            await _dl_text(q, ctx, pr, url)
        elif act == "html":
            await _dl_html(q, ctx, pr, url)
        elif act == "txtf":
            await _dl_text_file(q, ctx, url)
    except Exception as exc:
        logger.error("Action %s failed: %s", act, exc)
        try:
            await q.edit_message_text(
                f"Operation failed.\nError: {esc(str(exc)[:200])}",
                parse_mode=ParseMode.HTML,
                reply_markup=BACK_KB,
            )
        except Exception:
            pass


# ── paginated PDF / doc lists ─────────────────────────────────────────────────


async def _list_pdfs(q, ctx, pr: ParseResult, page: int) -> None:
    await _paginated_list(q, pr.pdfs, page, "PDF Files", "a:pdfs", "a:gpdf")


async def _list_docs(q, ctx, pr: ParseResult, page: int) -> None:
    await _paginated_list(q, pr.docs, page, "Documents", "a:docs", "a:gdoc")


async def _paginated_list(
    q, links: list[str], page: int,
    heading: str, nav_prefix: str, dl_prefix: str,
) -> None:
    total = len(links)
    pages = math.ceil(total / PAGE_SIZE)
    page = max(0, min(page, pages - 1))
    start = page * PAGE_SIZE
    sl = links[start : start + PAGE_SIZE]

    lines = [f"<b>{heading}</b>  (page {page + 1}/{pages})\n"]
    kb: list[list[InlineKeyboardButton]] = []
    for i, link in enumerate(sl):
        idx = start + i
        name = unquote(Path(urlparse(link).path).name)[:50] or f"file_{idx}"
        lines.append(f"<b>{idx + 1}.</b> {esc(name)}")
        kb.append([InlineKeyboardButton(
            f"Download: {name[:30]}", callback_data=f"{dl_prefix}:{idx}",
        )])

    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton("Prev Page", callback_data=f"{nav_prefix}:{page - 1}"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("Next Page", callback_data=f"{nav_prefix}:{page + 1}"))
    if nav:
        kb.append(nav)
    kb.append([InlineKeyboardButton("Back", callback_data="m:back")])

    await q.edit_message_text(
        "\n".join(lines), parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(kb),
    )


# ── single file download ─────────────────────────────────────────────────────


async def _dl_single(
    q, ctx, dl: Downloader,
    links: list[str], idx: int, page_url: str, label: str,
) -> None:
    if idx >= len(links):
        await q.edit_message_text("Invalid selection.", reply_markup=BACK_KB)
        return

    link = links[idx]
    name = unquote(Path(urlparse(link).path).name)[:60] or f"file_{idx}"

    await q.edit_message_text(f"Starting download ...")

    data = await dl.raw(link)
    await q.edit_message_text(f"Uploading to Telegram ...")
    await ctx.bot.send_document(
        chat_id=q.message.chat_id,
        document=io.BytesIO(data), filename=name,
        caption=f"{label}: {name}\nSize: {fmt_size(len(data))}",
    )
    users.record(q.from_user.id, link, label, name)
    await q.edit_message_text("Download complete!", reply_markup=BACK_KB)


# ── image ZIP ─────────────────────────────────────────────────────────────────


async def _dl_images_zip(q, ctx, dl: Downloader, pr: ParseResult, url: str) -> None:
    imgs = pr.images[:MAX_IMAGES]
    if not imgs:
        await q.edit_message_text("No images found.", reply_markup=BACK_KB)
        return

    await q.edit_message_text(f"Downloading {len(imgs)} images ...")

    tmp = Path(tempfile.mkdtemp(dir=TEMP_DIR))
    try:
        paths = await dl.batch(imgs, tmp, "img")
        if not paths:
            await q.edit_message_text("Failed to download images.", reply_markup=BACK_KB)
            return

        await q.edit_message_text("Packaging ZIP ...")
        zdata = await FileProc.make_zip(paths)
        if len(zdata) > TELEGRAM_FILE_LIMIT:
            await q.edit_message_text(
                f"ZIP too large ({fmt_size(len(zdata))}). Try a page with fewer images.",
                reply_markup=BACK_KB,
            )
            return

        fname = f"{sanitize(pr.title)}_images.zip"
        await q.edit_message_text("Uploading to Telegram ...")
        await ctx.bot.send_document(
            chat_id=q.message.chat_id,
            document=io.BytesIO(zdata), filename=fname,
            caption=f"{len(paths)} images | {fmt_size(len(zdata))}",
        )
        users.record(q.from_user.id, url, "ZIP", pr.title)
        await q.edit_message_text("Download complete!", reply_markup=BACK_KB)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# ── images → PDF ──────────────────────────────────────────────────────────────


async def _dl_images_pdf(q, ctx, dl: Downloader, pr: ParseResult, url: str) -> None:
    imgs = pr.images[:MAX_IMAGES]
    if not imgs:
        await q.edit_message_text("No images found.", reply_markup=BACK_KB)
        return

    await q.edit_message_text(f"Downloading {len(imgs)} images ...")

    tmp = Path(tempfile.mkdtemp(dir=TEMP_DIR))
    try:
        paths = await dl.batch(imgs, tmp, "img")
        if not paths:
            await q.edit_message_text("Failed to download images.", reply_markup=BACK_KB)
            return

        await q.edit_message_text("Converting to PDF ...")
        pdf = await FileProc.imgs_to_pdf(sorted(paths))
        if len(pdf) > TELEGRAM_FILE_LIMIT:
            await q.edit_message_text(
                f"PDF too large ({fmt_size(len(pdf))}). Try fewer images.",
                reply_markup=BACK_KB,
            )
            return

        fname = f"{sanitize(pr.title)}.pdf"
        await q.edit_message_text("Uploading to Telegram ...")
        await ctx.bot.send_document(
            chat_id=q.message.chat_id,
            document=io.BytesIO(pdf), filename=fname,
            caption=f"{len(paths)} pages | {fmt_size(len(pdf))}",
        )
        users.record(q.from_user.id, url, "PDF", pr.title)
        await q.edit_message_text("Download complete!", reply_markup=BACK_KB)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# ── text extraction ───────────────────────────────────────────────────────────


async def _dl_text(q, ctx, pr: ParseResult, url: str) -> None:
    if not pr.text:
        await q.edit_message_text("No readable text found.", reply_markup=BACK_KB)
        return

    if len(pr.text) <= 4000:
        ctx.user_data["_txt"] = pr.text
        ctx.user_data["_ttl"] = pr.title
        await q.edit_message_text(
            f"<b>Extracted Text</b>\n\n<pre>{esc(pr.text[:3900])}</pre>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Download as TXT", callback_data="a:txtf")],
                [InlineKeyboardButton("Back", callback_data="m:back")],
            ]),
        )
    else:
        fname = f"{sanitize(pr.title)}.txt"
        await q.edit_message_text("Uploading text file ...")
        await ctx.bot.send_document(
            chat_id=q.message.chat_id,
            document=io.BytesIO(pr.text.encode("utf-8")), filename=fname,
            caption=f"Extracted text ({len(pr.text):,} chars)",
        )
        users.record(q.from_user.id, url, "TXT", pr.title)
        await q.edit_message_text("Text extraction complete!", reply_markup=BACK_KB)


async def _dl_text_file(q, ctx, url: str) -> None:
    txt = ctx.user_data.get("_txt", "")
    title = ctx.user_data.get("_ttl", "document")
    if not txt:
        await q.edit_message_text("No text cached — resend the URL.", reply_markup=BACK_KB)
        return
    fname = f"{sanitize(title)}.txt"
    await ctx.bot.send_document(
        chat_id=q.message.chat_id,
        document=io.BytesIO(txt.encode("utf-8")), filename=fname,
        caption=f"Text ({len(txt):,} chars)",
    )
    users.record(q.from_user.id, url, "TXT", title)


# ── HTML snapshot ─────────────────────────────────────────────────────────────


async def _dl_html(q, ctx, pr: ParseResult, url: str) -> None:
    data = FileProc.snapshot_bytes(pr.html, url)
    fname = f"{sanitize(pr.title)}.html"
    await q.edit_message_text("Uploading snapshot ...")
    await ctx.bot.send_document(
        chat_id=q.message.chat_id,
        document=io.BytesIO(data), filename=fname,
        caption=f"HTML snapshot | {fmt_size(len(data))}",
    )
    users.record(q.from_user.id, url, "HTML", pr.title)
    await q.edit_message_text("Snapshot saved!", reply_markup=BACK_KB)


# ═══════════════════════════════════════════════════════════════════════════════
# 9. ADMIN COMMANDS
# ═══════════════════════════════════════════════════════════════════════════════


@owner
async def cmd_add_user(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not ctx.args:
        await update.message.reply_text("Usage: /add_user &lt;user_id&gt;", parse_mode=ParseMode.HTML)
        return
    try:
        uid = int(ctx.args[0])
    except ValueError:
        await update.message.reply_text("Invalid user ID.")
        return
    if users.add(uid):
        await update.message.reply_text(f"User <code>{uid}</code> authorized.", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(f"User <code>{uid}</code> already authorized.", parse_mode=ParseMode.HTML)


@owner
async def cmd_remove_user(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not ctx.args:
        await update.message.reply_text("Usage: /remove_user &lt;user_id&gt;", parse_mode=ParseMode.HTML)
        return
    try:
        uid = int(ctx.args[0])
    except ValueError:
        await update.message.reply_text("Invalid user ID.")
        return
    if users.remove(uid):
        await update.message.reply_text(f"User <code>{uid}</code> removed.", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(f"Cannot remove user <code>{uid}</code>.", parse_mode=ParseMode.HTML)


@owner
async def cmd_users(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    ids = users.all_ids
    lines = ["<b>Authorized Users</b>\n"]
    for uid in ids:
        tag = " (owner)" if uid == OWNER_ID else ""
        lines.append(f"<code>{uid}</code>{tag}")
    lines.append(f"\nTotal: {len(ids)}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


@owner
async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not ctx.args:
        await update.message.reply_text("Usage: /broadcast &lt;message&gt;", parse_mode=ParseMode.HTML)
        return
    text = " ".join(ctx.args)
    sent = failed = 0
    for uid in users.all_ids:
        try:
            await ctx.bot.send_message(chat_id=uid, text=text)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    await update.message.reply_text(f"Broadcast done. Sent: {sent} | Failed: {failed}")


@owner
async def cmd_botstats(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    summary = users.global_summary()
    await update.message.reply_text(
        f"<b>Bot Statistics</b>\n\n"
        f"{summary}\n"
        f"Cached pages: {page_cache.size}",
        parse_mode=ParseMode.HTML,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# 10. BACKGROUND TASKS — cache cleanup
# ═══════════════════════════════════════════════════════════════════════════════


async def _periodic_cleanup(app: Application) -> None:
    while True:
        await asyncio.sleep(600)
        n = page_cache.purge()
        if n:
            logger.info("Cache cleanup: purged %d expired entries", n)
        if TEMP_DIR.exists():
            for child in TEMP_DIR.iterdir():
                if child.is_dir():
                    try:
                        shutil.rmtree(child, ignore_errors=True)
                    except Exception:
                        pass


# ═══════════════════════════════════════════════════════════════════════════════
# 11. APPLICATION LIFECYCLE & MAIN
# ═══════════════════════════════════════════════════════════════════════════════


async def post_init(app: Application) -> None:
    _ensure_dirs()
    app.bot_data["session"] = aiohttp.ClientSession()
    app.bot_data["cleanup_task"] = asyncio.create_task(_periodic_cleanup(app))
    logger.info("Bot initialized — owner=%s", OWNER_ID)


async def post_shutdown(app: Application) -> None:
    task = app.bot_data.get("cleanup_task")
    if task and not task.done():
        task.cancel()
    session = app.bot_data.get("session")
    if session and not session.closed:
        await session.close()
    if TEMP_DIR.exists():
        shutil.rmtree(TEMP_DIR, ignore_errors=True)
    logger.info("Bot shutdown complete")


def main() -> None:
    if BOT_TOKEN in ("YOUR_BOT_TOKEN_HERE", ""):
        logger.error(
            "BOT_TOKEN not set. Export it as an environment variable or "
            "edit the BOT_TOKEN constant at the top of this file."
        )
        sys.exit(1)

    _ensure_dirs()

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .concurrent_updates(True)
        .build()
    )

    # Commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("mystats", cmd_mystats))

    # Admin
    app.add_handler(CommandHandler("add_user", cmd_add_user))
    app.add_handler(CommandHandler("remove_user", cmd_remove_user))
    app.add_handler(CommandHandler("users", cmd_users))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    app.add_handler(CommandHandler("botstats", cmd_botstats))

    # Inline-keyboard callbacks
    app.add_handler(CallbackQueryHandler(cb_menu, pattern=r"^m:"))
    app.add_handler(CallbackQueryHandler(cb_history, pattern=r"^hist:"))
    app.add_handler(CallbackQueryHandler(cb_action, pattern=r"^a:"))

    # URL messages
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.Regex(URL_RE),
        handle_url,
    ))

    logger.info("Starting Universal Document Downloader Bot ...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()

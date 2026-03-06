#!/usr/bin/env python3
"""
Universal Document Downloader & Viewer Bot (Single File)
========================================================

Dependencies (Python 3.11+):
  pip install -U "python-telegram-bot[ext]" aiohttp beautifulsoup4 Pillow lxml

Optimized for:
  - Termux
  - Ubuntu VPS
  - Low memory + async I/O
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import html
import json
import logging
import os
import re
import shutil
import tempfile
import textwrap
import time
import zipfile
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass, field
from functools import wraps
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup
from PIL import Image, ImageDraw, ImageFont
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Update
from telegram.constants import ChatAction, ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

# ============================================================================
# 1) IMPORTS (done above)
# ============================================================================
# 2) CONFIGURATION
# ============================================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")

# Hardcoded owner ID as requested. Change this to your Telegram numeric ID.
OWNER_ID = 6512242172

DATA_DIR = Path("bot_data")
TMP_DIR = DATA_DIR / "tmp"
STORAGE_FILE = DATA_DIR / "storage.json"

WAITING_URL = 1
PAGE_SIZE = 6
MAX_HISTORY_PER_USER = 100
MAX_TEXT_SIZE = 2_000_000
MAX_HTML_BYTES = 6_000_000
MAX_DOWNLOAD_BYTES = 180 * 1024 * 1024
MAX_IMAGES_FOR_PDF = 120
CACHE_TTL_SECONDS = 900
CACHE_MAX_ITEMS = 128
TEMP_CLEANUP_MAX_AGE = 3600
DOWNLOAD_CONCURRENCY = 3
MAX_REQUESTS_PER_MIN = 8
MAX_LINKS_TO_SHOW = 120
HTTP_RETRIES = 3

URL_RE = re.compile(r"(https?://[^\s<>\"']+)", re.IGNORECASE)
SAFE_FILENAME_RE = re.compile(r"[^a-zA-Z0-9._ -]+")
SUPPORTED_DOC_EXT = (".pdf", ".txt", ".docx", ".html", ".htm")
SUPPORTED_IMG_EXT = (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp")

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=120, connect=25, sock_read=90)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("UniversalDownloaderBot")


# ============================================================================
# 3) HELPER UTILITIES
# ============================================================================


def now_ts() -> float:
    return time.time()


def human_bytes(size: int) -> str:
    units = ["B", "KB", "MB", "GB"]
    val = float(size)
    for unit in units:
        if val < 1024 or unit == units[-1]:
            return f"{val:.1f} {unit}" if unit != "B" else f"{int(val)} B"
        val /= 1024
    return f"{size} B"


def sanitize_filename(name: str, fallback: str = "download") -> str:
    cleaned = SAFE_FILENAME_RE.sub("", name).strip().replace(" ", "_")
    cleaned = cleaned[:90].strip("._")
    return cleaned or fallback


def valid_public_url(url: str) -> bool:
    try:
        p = urlparse(url)
        if p.scheme not in {"http", "https"}:
            return False
        if not p.netloc or "." not in p.netloc:
            return False
        host = p.hostname or ""
        blocked = {"localhost", "127.0.0.1", "0.0.0.0"}
        if host in blocked or host.endswith(".local"):
            return False
        return True
    except Exception:
        return False


def unique_keep_order(items: list[str]) -> list[str]:
    return list(dict.fromkeys(items))


async def read_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return default

    def _read() -> dict[str, Any]:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    try:
        return await asyncio.to_thread(_read)
    except Exception:
        logger.exception("Failed reading JSON storage; using default")
        return default


async def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    tmp = path.with_suffix(".tmp")

    def _write() -> None:
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        tmp.replace(path)

    await asyncio.to_thread(_write)


def menu_keyboard() -> InlineKeyboardMarkup:
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


def link_action_keyboard(token: str, include_assets: bool) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("⬇ Download as PDF", callback_data=f"act:{token}:pdf"),
            InlineKeyboardButton("🖼 Download Images", callback_data=f"act:{token}:img"),
        ],
        [
            InlineKeyboardButton("📄 Extract Text", callback_data=f"act:{token}:txt"),
            InlineKeyboardButton("🌐 Save HTML", callback_data=f"act:{token}:html"),
        ],
    ]
    if include_assets:
        rows.append([InlineKeyboardButton("📋 View Assets", callback_data=f"assets:{token}:0")])
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="menu:back")])
    return InlineKeyboardMarkup(rows)


def progress_stage(stage: str) -> str:
    stages = {
        "start": "🚀 Starting download",
        "process": "⚡ Processing link",
        "download": "📥 Downloading assets",
        "package": "📦 Packaging files",
        "upload": "📤 Uploading to Telegram",
        "done": "✅ Completed",
        "error": "❌ Error",
    }
    return stages.get(stage, stage)


class RateLimiter:
    def __init__(self, per_minute: int) -> None:
        self.per_minute = per_minute
        self.events: dict[int, deque[float]] = defaultdict(deque)

    def allow(self, user_id: int) -> tuple[bool, int]:
        q = self.events[user_id]
        current = now_ts()
        cutoff = current - 60
        while q and q[0] < cutoff:
            q.popleft()
        if len(q) >= self.per_minute:
            wait_for = max(1, int(60 - (current - q[0])))
            return False, wait_for
        q.append(current)
        return True, 0


class TTLCache:
    def __init__(self, ttl_seconds: int, max_items: int) -> None:
        self.ttl = ttl_seconds
        self.max_items = max_items
        self._items: OrderedDict[str, tuple[float, Any]] = OrderedDict()

    def get(self, key: str) -> Any | None:
        item = self._items.get(key)
        if not item:
            return None
        ts, val = item
        if now_ts() - ts > self.ttl:
            self._items.pop(key, None)
            return None
        self._items.move_to_end(key)
        return val

    def set(self, key: str, value: Any) -> None:
        self._items[key] = (now_ts(), value)
        self._items.move_to_end(key)
        while len(self._items) > self.max_items:
            self._items.popitem(last=False)


class DownloadQueue:
    def __init__(self, max_concurrency: int) -> None:
        self.sem = asyncio.Semaphore(max_concurrency)
        self._active = 0
        self._lock = asyncio.Lock()

    @property
    def active(self) -> int:
        return self._active

    async def run(self, coro):
        async with self.sem:
            async with self._lock:
                self._active += 1
            try:
                return await coro
            finally:
                async with self._lock:
                    self._active -= 1


class DataStore:
    """Persistent storage for users, history, settings and statistics."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock = asyncio.Lock()
        self.data: dict[str, Any] = {
            "users": [OWNER_ID],
            "history": {},
            "stats": {},
            "settings": {},
        }

    async def load(self) -> None:
        async with self.lock:
            self.data = await read_json(self.path, self.data)
            users = set(int(x) for x in self.data.get("users", []))
            users.add(OWNER_ID)
            self.data["users"] = sorted(users)
            self.data.setdefault("history", {})
            self.data.setdefault("stats", {})
            self.data.setdefault("settings", {})
            await write_json_atomic(self.path, self.data)

    async def save(self) -> None:
        async with self.lock:
            await write_json_atomic(self.path, self.data)

    def is_authorized(self, user_id: int) -> bool:
        return int(user_id) in set(int(x) for x in self.data.get("users", []))

    async def add_user(self, user_id: int) -> bool:
        uid = int(user_id)
        users = set(int(x) for x in self.data.get("users", []))
        if uid in users:
            return False
        users.add(uid)
        self.data["users"] = sorted(users)
        await self.save()
        return True

    async def remove_user(self, user_id: int) -> bool:
        uid = int(user_id)
        if uid == OWNER_ID:
            return False
        users = set(int(x) for x in self.data.get("users", []))
        if uid not in users:
            return False
        users.remove(uid)
        self.data["users"] = sorted(users)
        await self.save()
        return True

    def list_users(self) -> list[int]:
        return sorted(int(x) for x in self.data.get("users", []))

    async def add_history(
        self,
        user_id: int,
        url: str,
        action: str,
        status: str,
        file_name: str = "",
        size: int = 0,
    ) -> None:
        uid = str(user_id)
        rows = self.data["history"].setdefault(uid, [])
        rows.insert(
            0,
            {
                "ts": int(now_ts()),
                "url": url,
                "action": action,
                "status": status,
                "file": file_name,
                "size": size,
            },
        )
        self.data["history"][uid] = rows[:MAX_HISTORY_PER_USER]
        await self.save()

    def get_history(self, user_id: int) -> list[dict[str, Any]]:
        return list(self.data.get("history", {}).get(str(user_id), []))

    async def bump_stat(self, user_id: int, key: str, amount: int = 1) -> None:
        uid = str(user_id)
        u = self.data["stats"].setdefault(uid, {"requests": 0, "downloads": 0, "errors": 0})
        u[key] = int(u.get(key, 0)) + amount
        await self.save()

    def get_stats(self, user_id: int) -> dict[str, int]:
        return dict(self.data.get("stats", {}).get(str(user_id), {}))

    def get_setting(self, user_id: int, key: str, default: Any) -> Any:
        return self.data.get("settings", {}).get(str(user_id), {}).get(key, default)

    async def set_setting(self, user_id: int, key: str, value: Any) -> None:
        uid = str(user_id)
        self.data["settings"].setdefault(uid, {})[key] = value
        await self.save()


@dataclass(slots=True)
class ParsedPage:
    url: str
    final_url: str
    status: int
    content_type: str
    title: str
    description: str
    html: str
    text: str
    pdf_links: list[str]
    image_links: list[str]
    doc_links: list[str]
    fetched_at: float

    def preview(self) -> str:
        host = urlparse(self.final_url).netloc
        return (
            f"🌐 <b>{html.escape(self.title or 'Untitled')}</b>\n"
            f"🔗 {html.escape(host)}\n"
            f"📄 PDFs: <b>{len(self.pdf_links)}</b> | "
            f"🖼 Images: <b>{len(self.image_links)}</b> | "
            f"📎 Docs: <b>{len(self.doc_links)}</b>\n"
            f"🧾 Content-Type: <code>{html.escape(self.content_type or 'unknown')}</code>"
        )


@dataclass(slots=True)
class SessionPayload:
    parsed: ParsedPage
    assets: list[tuple[str, str]]
    created_at: float = field(default_factory=now_ts)


class DownloaderEngine:
    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session

    async def _request_with_retry(self, method: str, url: str, **kwargs):
        error: Exception | None = None
        for attempt in range(1, HTTP_RETRIES + 1):
            try:
                return await self.session.request(method, url, **kwargs)
            except Exception as exc:
                error = exc
                await asyncio.sleep(0.8 * attempt)
        raise RuntimeError(f"HTTP request failed after retries: {error}")

    async def fetch_and_parse(
        self,
        url: str,
    ) -> ParsedPage:
        async with await self._request_with_retry(
            "GET",
            url,
            headers=BROWSER_HEADERS,
            allow_redirects=True,
            timeout=REQUEST_TIMEOUT,
            ssl=False,
        ) as resp:
            status = resp.status
            content_type = resp.headers.get("Content-Type", "").split(";")[0].strip().lower()
            final_url = str(resp.url)
            title = Path(urlparse(final_url).path).name or "document"
            description = ""
            html_data = ""
            text_data = ""
            pdf_links: list[str] = []
            image_links: list[str] = []
            doc_links: list[str] = []

            low_url = final_url.lower()
            if (
                "application/pdf" in content_type
                or low_url.endswith(".pdf")
            ):
                pdf_links.append(final_url)
            elif content_type.startswith("image/") or low_url.endswith(SUPPORTED_IMG_EXT):
                image_links.append(final_url)
            elif (
                "text/plain" in content_type
                or low_url.endswith(".txt")
                or low_url.endswith(".docx")
            ):
                doc_links.append(final_url)
                raw = await resp.content.read(MAX_TEXT_SIZE)
                try:
                    text_data = raw.decode(resp.charset or "utf-8", errors="ignore")
                except Exception:
                    text_data = raw.decode("utf-8", errors="ignore")
            else:
                raw = await resp.content.read(MAX_HTML_BYTES)
                html_data = raw.decode(resp.charset or "utf-8", errors="ignore")
                soup = BeautifulSoup(html_data, "lxml")
                title = self._extract_title(soup, fallback=title)
                description = self._extract_description(soup)
                text_data = self._extract_readable_text(soup)
                pdf_links, image_links, doc_links = self._extract_assets(soup, final_url)

            return ParsedPage(
                url=url,
                final_url=final_url,
                status=status,
                content_type=content_type,
                title=title[:200],
                description=description[:400],
                html=html_data,
                text=text_data[:MAX_TEXT_SIZE],
                pdf_links=unique_keep_order(pdf_links)[:MAX_LINKS_TO_SHOW],
                image_links=unique_keep_order(image_links)[:MAX_LINKS_TO_SHOW],
                doc_links=unique_keep_order(doc_links)[:MAX_LINKS_TO_SHOW],
                fetched_at=now_ts(),
            )

    @staticmethod
    def _extract_title(soup: BeautifulSoup, fallback: str = "Untitled") -> str:
        og = soup.find("meta", attrs={"property": "og:title"})
        if og and og.get("content"):
            return str(og["content"]).strip()
        if soup.title and soup.title.string:
            return soup.title.string.strip()
        h1 = soup.find("h1")
        if h1:
            t = h1.get_text(strip=True)
            if t:
                return t
        return fallback

    @staticmethod
    def _extract_description(soup: BeautifulSoup) -> str:
        for attrs in (
            {"name": "description"},
            {"property": "og:description"},
        ):
            m = soup.find("meta", attrs=attrs)
            if m and m.get("content"):
                return str(m["content"]).strip()
        return ""

    @staticmethod
    def _extract_readable_text(soup: BeautifulSoup) -> str:
        for bad in soup(["script", "style", "noscript", "svg"]):
            bad.extract()
        target = soup.select_one("article") or soup.select_one("main") or soup.body or soup
        text = target.get_text(separator="\n", strip=True)
        lines = [ln.strip() for ln in text.splitlines() if len(ln.strip()) > 2]
        return "\n".join(lines)

    @staticmethod
    def _extract_assets(
        soup: BeautifulSoup, base_url: str
    ) -> tuple[list[str], list[str], list[str]]:
        pdf_links: list[str] = []
        image_links: list[str] = []
        doc_links: list[str] = []

        for a in soup.find_all("a", href=True):
            href = urljoin(base_url, a.get("href", "").strip())
            if not href.startswith(("http://", "https://")):
                continue
            low = href.lower().split("#", 1)[0]
            if low.endswith(".pdf"):
                pdf_links.append(href)
            elif low.endswith(SUPPORTED_IMG_EXT):
                image_links.append(href)
            elif low.endswith((".txt", ".docx", ".html", ".htm", ".zip")):
                doc_links.append(href)

        for img in soup.find_all("img"):
            candidates: list[str] = []
            for key in ("src", "data-src", "data-original"):
                val = img.get(key)
                if val:
                    candidates.append(val)
            srcset = img.get("srcset")
            if srcset:
                for part in srcset.split(","):
                    candidates.append(part.strip().split(" ")[0])
            for c in candidates:
                if not c:
                    continue
                full = urljoin(base_url, c.strip())
                if full.startswith(("http://", "https://")):
                    image_links.append(full)

        for tag in soup.find_all(["iframe", "embed", "object"]):
            src = tag.get("src") or tag.get("data")
            if not src:
                continue
            full = urljoin(base_url, src.strip())
            low = full.lower()
            if ".pdf" in low:
                pdf_links.append(full)
            elif low.endswith(SUPPORTED_DOC_EXT):
                doc_links.append(full)

        return (
            unique_keep_order(pdf_links),
            unique_keep_order(image_links),
            unique_keep_order(doc_links),
        )

    async def stream_download(
        self,
        url: str,
        out_path: Path,
        *,
        max_bytes: int = MAX_DOWNLOAD_BYTES,
        progress_cb=None,
    ) -> int:
        downloaded = 0
        async with await self._request_with_retry(
            "GET",
            url,
            headers=BROWSER_HEADERS,
            allow_redirects=True,
            timeout=REQUEST_TIMEOUT,
            ssl=False,
        ) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("Content-Length", "0") or 0)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with out_path.open("wb") as f:
                async for chunk in resp.content.iter_chunked(64 * 1024):
                    if not chunk:
                        continue
                    downloaded += len(chunk)
                    if downloaded > max_bytes:
                        raise RuntimeError("File too large (limit reached).")
                    await asyncio.to_thread(f.write, chunk)
                    if progress_cb:
                        await progress_cb(downloaded, total)
        return downloaded

    async def download_many(
        self,
        urls: list[str],
        out_dir: Path,
        progress_cb=None,
        max_count: int = MAX_IMAGES_FOR_PDF,
    ) -> list[Path]:
        urls = urls[:max_count]
        sem = asyncio.Semaphore(6)
        result: list[tuple[int, Path | None]] = []

        async def worker(idx: int, url: str) -> None:
            async with sem:
                ext = Path(urlparse(url).path).suffix.lower() or ".jpg"
                target = out_dir / f"asset_{idx+1:04d}{ext[:6]}"
                for attempt in range(1, HTTP_RETRIES + 1):
                    try:
                        await self.stream_download(url, target)
                        result.append((idx, target))
                        if progress_cb:
                            await progress_cb(len(result), len(urls))
                        return
                    except Exception:
                        if attempt == HTTP_RETRIES:
                            result.append((idx, None))
                        else:
                            await asyncio.sleep(0.4 * attempt)

        await asyncio.gather(*(worker(i, u) for i, u in enumerate(urls)))
        result.sort(key=lambda x: x[0])
        return [p for _, p in result if p and p.exists()]

    async def images_to_pdf(self, image_paths: list[Path], out_pdf: Path) -> None:
        def _convert() -> None:
            images: list[Image.Image] = []
            for p in image_paths:
                try:
                    im = Image.open(p)
                    if im.mode != "RGB":
                        im = im.convert("RGB")
                    images.append(im)
                except Exception:
                    continue
            if not images:
                raise RuntimeError("No valid images to build PDF.")
            first, *rest = images
            first.save(out_pdf, "PDF", save_all=True, append_images=rest, resolution=150)

        await asyncio.to_thread(_convert)

    async def text_to_pdf(self, text: str, title: str, out_pdf: Path) -> None:
        def _build() -> None:
            if not text.strip():
                raise RuntimeError("No text available to create PDF.")
            w, h = 1240, 1754
            margin = 80
            max_chars = 95
            line_height = 28
            max_lines = (h - margin * 2) // line_height
            font = ImageFont.load_default()
            paragraphs = text.splitlines()
            lines: list[str] = []
            lines.append(title[:120])
            lines.append("-" * 60)
            for para in paragraphs:
                wrapped = textwrap.wrap(para, width=max_chars) or [""]
                lines.extend(wrapped)
            pages: list[Image.Image] = []
            for i in range(0, len(lines), max_lines):
                page = Image.new("RGB", (w, h), "white")
                draw = ImageDraw.Draw(page)
                y = margin
                for ln in lines[i : i + max_lines]:
                    draw.text((margin, y), ln, fill="black", font=font)
                    y += line_height
                pages.append(page)
            first, *rest = pages
            first.save(out_pdf, "PDF", save_all=True, append_images=rest, resolution=150)

        await asyncio.to_thread(_build)

    async def zip_files(self, files: list[Path], out_zip: Path) -> None:
        def _zip() -> None:
            with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
                for p in files:
                    if p.exists():
                        zf.write(p, arcname=p.name)

        await asyncio.to_thread(_zip)


@dataclass
class Runtime:
    store: DataStore
    limiter: RateLimiter
    cache: TTLCache
    queue: DownloadQueue
    session: aiohttp.ClientSession | None = None
    sessions: dict[str, SessionPayload] = field(default_factory=dict)


def get_runtime(ctx: ContextTypes.DEFAULT_TYPE) -> Runtime:
    return ctx.application.bot_data["runtime"]


def owner_only(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != OWNER_ID:
            await update.effective_message.reply_text("❌ Owner-only command.")
            return
        return await func(update, context)

    return wrapper


async def ensure_auth(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    rt = get_runtime(context)
    uid = update.effective_user.id
    if rt.store.is_authorized(uid):
        return True
    await update.effective_message.reply_text(
        "❌ Access denied.\n"
        "Send your ID to the owner for approval:\n"
        f"<code>{uid}</code>",
        parse_mode=ParseMode.HTML,
    )
    return False


async def safe_edit(query, text: str, reply_markup: InlineKeyboardMarkup | None = None) -> None:
    try:
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    except Exception:
        await query.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)


def mk_session_token(user_id: int, url: str) -> str:
    raw = f"{user_id}:{url}:{now_ts()}".encode("utf-8", "ignore")
    return hashlib.sha1(raw).hexdigest()[:12]


def build_assets(payload: SessionPayload) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    rows.extend([("PDF", x) for x in payload.parsed.pdf_links])
    rows.extend([("IMG", x) for x in payload.parsed.image_links])
    rows.extend([("DOC", x) for x in payload.parsed.doc_links])
    return rows[:MAX_LINKS_TO_SHOW]


def render_assets_page(token: str, items: list[tuple[str, str]], page: int) -> tuple[str, InlineKeyboardMarkup]:
    total_pages = max(1, (len(items) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * PAGE_SIZE
    chunk = items[start : start + PAGE_SIZE]
    lines = [f"📑 <b>Detected Assets</b> (Page {page + 1}/{total_pages})", ""]
    if not chunk:
        lines.append("No assets found.")
    else:
        for idx, (typ, url) in enumerate(chunk, start=start + 1):
            label = html.escape(url[:90] + ("..." if len(url) > 90 else ""))
            lines.append(f"{idx}. <b>{typ}</b> - <a href=\"{html.escape(url)}\">{label}</a>")

    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅ Prev Page", callback_data=f"assets:{token}:{page-1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("➡ Next Page", callback_data=f"assets:{token}:{page+1}"))
    rows: list[list[InlineKeyboardButton]] = []
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"backopt:{token}")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def render_history_page(history: list[dict[str, Any]], page: int) -> tuple[str, InlineKeyboardMarkup]:
    total_pages = max(1, (len(history) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * PAGE_SIZE
    chunk = history[start : start + PAGE_SIZE]
    lines = [f"📂 <b>My Downloads</b> (Page {page + 1}/{total_pages})", ""]
    if not chunk:
        lines.append("No download history yet.")
    else:
        for row in chunk:
            ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(row.get("ts", 0)))
            action = row.get("action", "-")
            status = row.get("status", "-")
            size = int(row.get("size", 0))
            url = row.get("url", "")
            lines.append(
                f"• <b>{html.escape(action.upper())}</b> [{html.escape(status)}]\n"
                f"  {ts} | {html.escape(human_bytes(size))}\n"
                f"  <code>{html.escape(url[:70])}</code>"
            )

    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅ Prev Page", callback_data=f"hist:{page-1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("➡ Next Page", callback_data=f"hist:{page+1}"))
    rows: list[list[InlineKeyboardButton]] = []
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="menu:back")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


async def cleanup_temp_dir() -> None:
    if not TMP_DIR.exists():
        return
    cutoff = now_ts() - TEMP_CLEANUP_MAX_AGE
    for p in TMP_DIR.iterdir():
        with contextlib.suppress(Exception):
            if p.is_dir() and p.stat().st_mtime < cutoff:
                shutil.rmtree(p, ignore_errors=True)


# ============================================================================
# 4) DOWNLOADER ENGINE (implemented in DownloaderEngine)
# ============================================================================
# 5) PARSER ENGINE (implemented in DownloaderEngine.fetch_and_parse)
# ============================================================================
# 6) TELEGRAM HANDLERS
# ============================================================================


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    rt = get_runtime(context)
    user = update.effective_user
    if not rt.store.is_authorized(user.id):
        await update.effective_message.reply_text(
            "👋 Welcome.\n"
            "You are not authorized yet.\n"
            "Send this ID to owner:\n"
            f"<code>{user.id}</code>",
            parse_mode=ParseMode.HTML,
        )
        return ConversationHandler.END

    stats = rt.store.get_stats(user.id)
    await update.effective_message.reply_text(
        "🤖 <b>Universal Document Downloader & Viewer</b>\n\n"
        "Send a public URL or use menu actions below.\n"
        f"📊 Requests: {stats.get('requests', 0)} | Downloads: {stats.get('downloads', 0)}",
        parse_mode=ParseMode.HTML,
        reply_markup=menu_keyboard(),
    )
    return ConversationHandler.END


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_auth(update, context):
        return
    owner_block = ""
    if update.effective_user.id == OWNER_ID:
        owner_block = (
            "\n\n👑 <b>Admin Commands</b>\n"
            "/add_user [id]\n"
            "/remove_user [id]\n"
            "/users\n"
            "/broadcast [message]"
        )
    await update.effective_message.reply_text(
        "ℹ️ <b>Help</b>\n\n"
        "1) Tap <b>Download Document</b> and send a URL.\n"
        "2) Bot detects PDFs, images, embedded docs and text.\n"
        "3) Pick output:\n"
        "   • ⬇ Download as PDF\n"
        "   • 🖼 Download Images (ZIP)\n"
        "   • 📄 Extract Text (TXT)\n"
        "   • 🌐 Save HTML snapshot\n\n"
        "Anti-spam and queueing are enabled for stability.\n"
        "Use /cancel anytime to reset current flow."
        f"{owner_block}",
        parse_mode=ParseMode.HTML,
    )


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.effective_user:
        context.user_data.pop("await_mode", None)
    await update.effective_message.reply_text("✅ Cancelled.", reply_markup=menu_keyboard())
    return ConversationHandler.END


async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    rt = get_runtime(context)
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id

    if not rt.store.is_authorized(uid):
        await query.answer("Not authorized", show_alert=True)
        return ConversationHandler.END

    action = query.data.split(":", 1)[1]
    if action in {"download", "extract", "snapshot"}:
        context.user_data["await_mode"] = action
        await safe_edit(
            query,
            "🔗 Send a public URL now.\n\n"
            "Supported examples:\n"
            "• webpage\n• PDF link\n• image gallery page\n• DOCX/TXT link\n\n"
            "Use /cancel to stop.",
            InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="menu:back")]]),
        )
        return WAITING_URL

    if action == "history":
        history = rt.store.get_history(uid)
        text, kb = render_history_page(history, 0)
        await safe_edit(query, text, kb)
        return ConversationHandler.END

    if action == "settings":
        p = rt.store.get_setting(uid, "progress_updates", True)
        text = (
            "⚙️ <b>Settings</b>\n\n"
            f"Progress updates: <b>{'ON' if p else 'OFF'}</b>"
        )
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "Toggle Progress",
                        callback_data=f"set:progress:{0 if p else 1}",
                    )
                ],
                [InlineKeyboardButton("🔙 Back", callback_data="menu:back")],
            ]
        )
        await safe_edit(query, text, kb)
        return ConversationHandler.END

    if action == "help":
        await safe_edit(
            query,
            "ℹ️ Tap one of the main actions, then send URL.\n"
            "This bot supports PDF/images/text/HTML extraction and download packaging.\n"
            "Use /help for full details.",
            InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="menu:back")]]),
        )
        return ConversationHandler.END

    if action == "back":
        await safe_edit(
            query,
            "🏠 <b>Main Menu</b>\nChoose an option:",
            menu_keyboard(),
        )
        return ConversationHandler.END

    return ConversationHandler.END


async def history_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rt = get_runtime(context)
    query = update.callback_query
    await query.answer()
    if not rt.store.is_authorized(query.from_user.id):
        return
    try:
        page = int(query.data.split(":")[1])
    except Exception:
        page = 0
    text, kb = render_history_page(rt.store.get_history(query.from_user.id), page)
    await safe_edit(query, text, kb)


async def settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rt = get_runtime(context)
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    if not rt.store.is_authorized(uid):
        return

    _, key, value = query.data.split(":")
    if key == "progress":
        await rt.store.set_setting(uid, "progress_updates", bool(int(value)))

    enabled = rt.store.get_setting(uid, "progress_updates", True)
    text = (
        "⚙️ <b>Settings</b>\n\n"
        f"Progress updates: <b>{'ON' if enabled else 'OFF'}</b>"
    )
    kb = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Toggle Progress", callback_data=f"set:progress:{0 if enabled else 1}")],
            [InlineKeyboardButton("🔙 Back", callback_data="menu:back")],
        ]
    )
    await safe_edit(query, text, kb)


async def parse_url_and_show_actions(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    url: str,
) -> int:
    rt = get_runtime(context)
    uid = update.effective_user.id
    await rt.store.bump_stat(uid, "requests")

    allowed, wait_seconds = rt.limiter.allow(uid)
    if not allowed:
        await update.effective_message.reply_text(
            f"⏳ Slow down. Try again in ~{wait_seconds}s."
        )
        return WAITING_URL

    status_msg = await update.effective_message.reply_text(
        f"{progress_stage('start')}\n{progress_stage('process')}",
    )

    parsed = rt.cache.get(url)
    if not parsed:
        try:
            engine = DownloaderEngine(rt.session)
            parsed = await engine.fetch_and_parse(url)
            rt.cache.set(url, parsed)
        except Exception as exc:
            await rt.store.bump_stat(uid, "errors")
            await status_msg.edit_text(f"{progress_stage('error')}\n<code>{html.escape(str(exc)[:350])}</code>", parse_mode=ParseMode.HTML)
            return WAITING_URL

    token = mk_session_token(uid, parsed.final_url)
    payload = SessionPayload(parsed=parsed, assets=[])
    payload.assets = build_assets(payload)
    rt.sessions[f"{uid}:{token}"] = payload

    desc = parsed.description or "No description"
    message = (
        "✅ <b>Link analyzed</b>\n\n"
        f"{parsed.preview()}\n\n"
        f"📝 {html.escape(desc[:200])}\n\n"
        "Choose output format:"
    )
    await status_msg.edit_text(
        message,
        parse_mode=ParseMode.HTML,
        reply_markup=link_action_keyboard(token, include_assets=bool(payload.assets)),
        disable_web_page_preview=False,
    )
    return ConversationHandler.END


async def handle_url_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_auth(update, context):
        return ConversationHandler.END

    text = (update.effective_message.text or "").strip()
    m = URL_RE.search(text)
    if not m:
        await update.effective_message.reply_text(
            "❌ No valid URL found in your message.\nPlease send a public http/https link."
        )
        return WAITING_URL

    url = m.group(1).strip()
    if not valid_public_url(url):
        await update.effective_message.reply_text("❌ Invalid or private URL.")
        return WAITING_URL

    return await parse_url_and_show_actions(update, context, url)


async def assets_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rt = get_runtime(context)
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    if not rt.store.is_authorized(uid):
        return
    _, token, page_raw = query.data.split(":")
    payload = rt.sessions.get(f"{uid}:{token}")
    if not payload:
        await safe_edit(query, "Session expired. Send URL again.", menu_keyboard())
        return
    page = int(page_raw)
    text, kb = render_assets_page(token, payload.assets, page)
    await safe_edit(query, text, kb)


async def back_to_options_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rt = get_runtime(context)
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    token = query.data.split(":")[1]
    payload = rt.sessions.get(f"{uid}:{token}")
    if not payload:
        await safe_edit(query, "Session expired. Send URL again.", menu_keyboard())
        return
    desc = payload.parsed.description or "No description"
    await safe_edit(
        query,
        "✅ <b>Link analyzed</b>\n\n"
        f"{payload.parsed.preview()}\n\n"
        f"📝 {html.escape(desc[:200])}\n\n"
        "Choose output format:",
        link_action_keyboard(token, include_assets=bool(payload.assets)),
    )


async def _send_result_document(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    path: Path,
    caption: str,
) -> int:
    size = path.stat().st_size
    with path.open("rb") as f:
        await context.bot.send_document(
            chat_id=chat_id,
            document=InputFile(f, filename=path.name),
            caption=caption,
            parse_mode=ParseMode.HTML,
        )
    return size


async def _run_download_action(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    payload: SessionPayload,
    action: str,
) -> tuple[str, int]:
    rt = get_runtime(context)
    query = update.callback_query
    uid = query.from_user.id
    chat_id = query.message.chat_id
    engine = DownloaderEngine(rt.session)
    show_progress = rt.store.get_setting(uid, "progress_updates", True)
    title = sanitize_filename(payload.parsed.title, "document")

    temp_root = Path(tempfile.mkdtemp(prefix=f"job_{uid}_", dir=str(TMP_DIR)))
    try:
        if show_progress:
            await safe_edit(query, f"{progress_stage('start')}\n{progress_stage('download')}")

        if action == "pdf":
            out_pdf = temp_root / f"{title}.pdf"
            if payload.parsed.pdf_links:
                source = payload.parsed.pdf_links[0]
                await engine.stream_download(source, out_pdf)
            elif payload.parsed.image_links:
                if show_progress:
                    await safe_edit(query, f"{progress_stage('download')}\nDownloading images for PDF...")
                imgs = await engine.download_many(payload.parsed.image_links, temp_root / "images")
                if not imgs:
                    raise RuntimeError("No images could be downloaded for PDF conversion.")
                if show_progress:
                    await safe_edit(query, f"{progress_stage('package')}\nConverting images to PDF...")
                await engine.images_to_pdf(imgs, out_pdf)
            else:
                if show_progress:
                    await safe_edit(query, f"{progress_stage('package')}\nBuilding text-based PDF...")
                await engine.text_to_pdf(payload.parsed.text, payload.parsed.title, out_pdf)

            if show_progress:
                await safe_edit(query, f"{progress_stage('upload')}")
            await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.UPLOAD_DOCUMENT)
            size = await _send_result_document(
                context,
                chat_id,
                out_pdf,
                f"✅ <b>{html.escape(payload.parsed.title)}</b>\n📕 PDF",
            )
            return out_pdf.name, size

        if action == "img":
            if not payload.parsed.image_links:
                raise RuntimeError("No downloadable images detected.")
            if show_progress:
                await safe_edit(query, f"{progress_stage('download')}\nDownloading image assets...")
            imgs = await engine.download_many(payload.parsed.image_links, temp_root / "images")
            if not imgs:
                raise RuntimeError("Image download failed.")
            zip_path = temp_root / f"{title}_images.zip"
            if show_progress:
                await safe_edit(query, f"{progress_stage('package')}\nCreating ZIP archive...")
            await engine.zip_files(imgs, zip_path)
            if show_progress:
                await safe_edit(query, progress_stage("upload"))
            await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.UPLOAD_DOCUMENT)
            size = await _send_result_document(
                context,
                chat_id,
                zip_path,
                f"✅ <b>{html.escape(payload.parsed.title)}</b>\n🖼 Images ZIP ({len(imgs)} files)",
            )
            return zip_path.name, size

        if action == "txt":
            out_txt = temp_root / f"{title}.txt"
            text_content = payload.parsed.text.strip()
            if not text_content:
                raise RuntimeError("No readable text found.")
            body = (
                f"Title: {payload.parsed.title}\n"
                f"Source: {payload.parsed.final_url}\n"
                f"{'=' * 60}\n\n{text_content}"
            )
            await asyncio.to_thread(out_txt.write_text, body, "utf-8")
            if show_progress:
                await safe_edit(query, progress_stage("upload"))
            await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.UPLOAD_DOCUMENT)
            size = await _send_result_document(
                context,
                chat_id,
                out_txt,
                f"✅ <b>{html.escape(payload.parsed.title)}</b>\n📄 TXT",
            )
            return out_txt.name, size

        if action == "html":
            out_html = temp_root / f"{title}.html"
            content = payload.parsed.html
            if not content:
                # Build fallback HTML snapshot for non-HTML resources.
                content = (
                    "<!doctype html><html><head><meta charset='utf-8'>"
                    f"<title>{html.escape(payload.parsed.title)}</title></head><body>"
                    f"<h1>{html.escape(payload.parsed.title)}</h1>"
                    f"<p>Original URL: <a href='{html.escape(payload.parsed.final_url)}'>"
                    f"{html.escape(payload.parsed.final_url)}</a></p>"
                    "<p>No HTML content available. Resource appears to be a direct file.</p>"
                    "</body></html>"
                )
            await asyncio.to_thread(out_html.write_text, content, "utf-8")
            if show_progress:
                await safe_edit(query, progress_stage("upload"))
            await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.UPLOAD_DOCUMENT)
            size = await _send_result_document(
                context,
                chat_id,
                out_html,
                f"✅ <b>{html.escape(payload.parsed.title)}</b>\n🌐 HTML snapshot",
            )
            return out_html.name, size

        raise RuntimeError("Unsupported action.")
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


async def action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rt = get_runtime(context)
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    if not rt.store.is_authorized(uid):
        await query.answer("Not authorized", show_alert=True)
        return

    _, token, action = query.data.split(":")
    session_key = f"{uid}:{token}"
    payload = rt.sessions.get(session_key)
    if not payload:
        await safe_edit(query, "❌ Session expired. Please send URL again.", menu_keyboard())
        return

    async def _work():
        try:
            file_name, size = await _run_download_action(update, context, payload, action)
            await rt.store.bump_stat(uid, "downloads")
            await rt.store.add_history(
                uid,
                payload.parsed.final_url,
                action,
                "success",
                file_name=file_name,
                size=size,
            )
            await safe_edit(
                query,
                f"{progress_stage('done')}\n"
                f"📁 File: <code>{html.escape(file_name)}</code>\n"
                f"📦 Size: <b>{html.escape(human_bytes(size))}</b>",
                InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="menu:back")]]),
            )
        except Exception as exc:
            await rt.store.bump_stat(uid, "errors")
            await rt.store.add_history(
                uid,
                payload.parsed.final_url,
                action,
                "failed",
                file_name="",
                size=0,
            )
            await safe_edit(
                query,
                f"{progress_stage('error')}\n<code>{html.escape(str(exc)[:350])}</code>",
                InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="menu:back")]]),
            )

    await rt.queue.run(_work())
    rt.sessions.pop(session_key, None)


# ============================================================================
# 7) ADMIN COMMANDS
# ============================================================================


@owner_only
async def cmd_add_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rt = get_runtime(context)
    if not context.args:
        await update.effective_message.reply_text("Usage: /add_user [id]")
        return
    lines: list[str] = []
    for arg in context.args:
        try:
            uid = int(arg)
        except ValueError:
            lines.append(f"❌ Invalid ID: {arg}")
            continue
        added = await rt.store.add_user(uid)
        lines.append(f"{'✅ Added' if added else 'ℹ️ Already exists'}: {uid}")
    await update.effective_message.reply_text("\n".join(lines))


@owner_only
async def cmd_remove_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rt = get_runtime(context)
    if not context.args:
        await update.effective_message.reply_text("Usage: /remove_user [id]")
        return
    lines: list[str] = []
    for arg in context.args:
        try:
            uid = int(arg)
        except ValueError:
            lines.append(f"❌ Invalid ID: {arg}")
            continue
        removed = await rt.store.remove_user(uid)
        lines.append(f"{'✅ Removed' if removed else 'ℹ️ Not found/owner'}: {uid}")
    await update.effective_message.reply_text("\n".join(lines))


@owner_only
async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rt = get_runtime(context)
    ids = rt.store.list_users()
    lines = [f"{'👑' if uid == OWNER_ID else '👤'} <code>{uid}</code>" for uid in ids]
    await update.effective_message.reply_text(
        f"👥 <b>Authorized Users ({len(ids)})</b>\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML,
    )


@owner_only
async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rt = get_runtime(context)
    if not context.args:
        await update.effective_message.reply_text("Usage: /broadcast [message]")
        return
    message = " ".join(context.args).strip()
    sent = 0
    failed = 0
    for uid in rt.store.list_users():
        try:
            await context.bot.send_message(
                chat_id=uid,
                text=f"📢 <b>Broadcast</b>\n\n{html.escape(message)}",
                parse_mode=ParseMode.HTML,
            )
            sent += 1
            await asyncio.sleep(0.03)
        except Exception:
            failed += 1
    await update.effective_message.reply_text(f"Broadcast done. ✅ {sent} | ❌ {failed}")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_auth(update, context):
        return
    rt = get_runtime(context)
    uid = update.effective_user.id
    stats = rt.store.get_stats(uid)
    await update.effective_message.reply_text(
        "✅ <b>Status</b>\n"
        f"👥 Users: {len(rt.store.list_users())}\n"
        f"🧵 Active downloads: {rt.queue.active}\n"
        f"🧠 Cache items: {len(rt.cache._items)}\n"
        f"📊 You: requests={stats.get('requests', 0)}, "
        f"downloads={stats.get('downloads', 0)}, errors={stats.get('errors', 0)}",
        parse_mode=ParseMode.HTML,
    )


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled error: %s", context.error)
    if isinstance(update, Update) and update.effective_message:
        with contextlib.suppress(Exception):
            await update.effective_message.reply_text(
                "❌ Internal error occurred. Please try again."
            )


# ============================================================================
# 8) MAIN ASYNC RUNNER
# ============================================================================


async def app_post_init(app: Application) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    await cleanup_temp_dir()
    runtime = Runtime(
        store=DataStore(STORAGE_FILE),
        limiter=RateLimiter(MAX_REQUESTS_PER_MIN),
        cache=TTLCache(CACHE_TTL_SECONDS, CACHE_MAX_ITEMS),
        queue=DownloadQueue(DOWNLOAD_CONCURRENCY),
    )
    await runtime.store.load()
    runtime.session = aiohttp.ClientSession(timeout=REQUEST_TIMEOUT)
    app.bot_data["runtime"] = runtime
    logger.info("Runtime initialized (owner=%s)", OWNER_ID)


async def app_post_shutdown(app: Application) -> None:
    runtime: Runtime | None = app.bot_data.get("runtime")
    if runtime and runtime.session:
        await runtime.session.close()
    await cleanup_temp_dir()


def build_application() -> Application:
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)
        .post_init(app_post_init)
        .post_shutdown(app_post_shutdown)
        .build()
    )

    convo = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(menu_callback, pattern=r"^menu:(download|extract|snapshot)$"),
        ],
        states={
            WAITING_URL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_url_message),
            ],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
        allow_reentry=True,
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("status", cmd_status))

    app.add_handler(CommandHandler("add_user", cmd_add_user))
    app.add_handler(CommandHandler("remove_user", cmd_remove_user))
    app.add_handler(CommandHandler("users", cmd_users))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))

    app.add_handler(convo)
    app.add_handler(CallbackQueryHandler(menu_callback, pattern=r"^menu:"))
    app.add_handler(CallbackQueryHandler(history_page_callback, pattern=r"^hist:\d+$"))
    app.add_handler(CallbackQueryHandler(settings_callback, pattern=r"^set:"))
    app.add_handler(CallbackQueryHandler(action_callback, pattern=r"^act:[a-f0-9]{12}:(pdf|img|txt|html)$"))
    app.add_handler(CallbackQueryHandler(assets_callback, pattern=r"^assets:[a-f0-9]{12}:\d+$"))
    app.add_handler(CallbackQueryHandler(back_to_options_callback, pattern=r"^backopt:[a-f0-9]{12}$"))

    # Also accept URL directly without entering conversation.
    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND & filters.Regex(URL_RE),
            handle_url_message,
        )
    )

    app.add_error_handler(on_error)
    return app


def main() -> None:
    if BOT_TOKEN in {"", "YOUR_BOT_TOKEN_HERE"}:
        raise SystemExit("Set BOT_TOKEN environment variable first.")
    app = build_application()
    logger.info("Starting Universal Document Downloader bot...")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()

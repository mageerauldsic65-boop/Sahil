#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Universal Document Downloader & Viewer Bot
==========================================

Dependencies:
    pip install python-telegram-bot[ext] aiohttp aiofiles beautifulsoup4 Pillow lxml

Runtime targets:
    - Python 3.11+
    - Termux
    - Ubuntu / Debian VPS

Environment variables:
    BOT_TOKEN          Telegram bot token (required)
    OWNER_ID           Telegram owner ID (optional, overrides hardcoded value)
    BOT_DATA_DIR       Directory for persistent state (optional)
    BOT_TEMP_ROOT      Directory for temporary files (optional)
    MAX_DOWNLOAD_BYTES Maximum export size in bytes (optional)
    MAX_IMAGES_PER_JOB Maximum number of images to package per job (optional)

This file intentionally contains the full bot implementation in one module while
keeping the internals modular through classes and helper functions.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import hashlib
import html
import io
import ipaddress
import json
import logging
import os
import re
import shutil
import tempfile
import textwrap
import time
import zipfile
from collections import Counter, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import aiofiles
import aiohttp
from bs4 import BeautifulSoup
from PIL import Image, ImageDraw, ImageFont
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Update
from telegram.constants import ChatAction, ParseMode
from telegram.error import BadRequest, Forbidden, RetryAfter, TimedOut
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
# 1. IMPORTS / CONFIGURATION
# ============================================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "REPLACE_WITH_YOUR_BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "123456789"))  # Replace with your Telegram ID.

DATA_DIR = Path(os.getenv("BOT_DATA_DIR", "bot_data"))
TEMP_ROOT = Path(os.getenv("BOT_TEMP_ROOT", tempfile.gettempdir())) / "universal_document_bot"
LOG_FILE = DATA_DIR / "universal_document_bot.log"
USERS_FILE = DATA_DIR / "users.json"
HISTORY_FILE = DATA_DIR / "history.json"
SETTINGS_FILE = DATA_DIR / "settings.json"
STATS_FILE = DATA_DIR / "stats.json"

MENU_PAGE_SIZE = 5
HISTORY_PAGE_SIZE = 5
CACHE_TTL_SECONDS = 600
CACHE_MAX_ITEMS = 128
ANTI_SPAM_WINDOW_SECONDS = 10
ANTI_SPAM_MAX_EVENTS = 5
PER_USER_COOLDOWN_SECONDS = 3.0
QUEUE_WORKERS = max(1, int(os.getenv("QUEUE_WORKERS", "2")))
HTTP_CONNECTION_LIMIT = 40
ASSET_CONCURRENCY = max(1, int(os.getenv("ASSET_CONCURRENCY", "4")))
MAX_ANALYSIS_BYTES = 3 * 1024 * 1024
MAX_TEXT_EXPORT_CHARS = 120_000
MAX_IMAGES_PER_JOB = max(1, int(os.getenv("MAX_IMAGES_PER_JOB", "40")))
MAX_DOWNLOAD_BYTES = int(os.getenv("MAX_DOWNLOAD_BYTES", str(45 * 1024 * 1024)))
TEMP_RETENTION_SECONDS = 6 * 60 * 60

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=120, connect=20, sock_connect=20, sock_read=90)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
}

HTML_PARSER = "lxml"
try:
    BeautifulSoup("", HTML_PARSER)
except Exception:
    HTML_PARSER = "html.parser"

WAITING_FOR_URL = 1

HELP_TEXT = (
    "ℹ️ <b>Universal Document Downloader & Viewer Bot</b>\n\n"
    "Send any <b>public</b> webpage or document link and the bot will analyze it for:\n"
    "• PDF files\n• image sequences\n• embedded documents\n• readable text\n\n"
    "<b>Workflow</b>\n"
    "1. Send a link\n"
    "2. Review the preview\n"
    "3. Pick an output format\n\n"
    "<b>Exports</b>\n"
    "• ⬇ Download as PDF\n"
    "• 🖼 Download Images (ZIP)\n"
    "• 📄 Extract Text (TXT)\n"
    "• 🌐 Save HTML snapshot\n\n"
    "<b>Commands</b>\n"
    "/start - open menu\n"
    "/help - show help\n"
    "/cancel - cancel URL prompt or queued job\n"
    "/stats - personal statistics\n\n"
    "<b>Owner commands</b>\n"
    "/add_user [id]\n"
    "/remove_user [id]\n"
    "/users\n"
    "/broadcast [message]"
)


def setup_logging() -> logging.Logger:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    TEMP_ROOT.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("UniversalDocumentBot")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


logger = setup_logging()


# ============================================================================
# 2. HELPER UTILITIES
# ============================================================================


def now_ts() -> float:
    return time.time()


def format_ts(value: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value))


def sanitize_filename(name: str, default: str = "document", limit: int = 80) -> str:
    cleaned = re.sub(r"[^\w\s.-]", "", name or "", flags=re.ASCII).strip().replace(" ", "_")
    cleaned = re.sub(r"_+", "_", cleaned)
    return (cleaned[:limit].strip("._") or default)


def format_bytes(size: int | float) -> str:
    size = float(max(0, size))
    units = ["B", "KB", "MB", "GB"]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{size:.1f} GB"


def short_text(text: str, limit: int = 300) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def extract_first_url(text: str) -> str | None:
    match = re.search(r"(?i)\b((?:https?://|www\.)[^\s<>{}|\\^`]+)", text or "")
    if not match:
        return None
    url = match.group(1).rstrip("),.;]>")
    if url.startswith("www."):
        url = f"https://{url}"
    return url


def normalize_url(raw_url: str) -> str:
    parsed = urlparse(raw_url.strip())
    scheme = parsed.scheme.lower() or "https"
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    if not netloc and parsed.path:
        reparsed = urlparse(f"{scheme}://{parsed.path}")
        scheme, netloc, path = reparsed.scheme, reparsed.netloc.lower(), reparsed.path or "/"
        parsed = reparsed
    return parsed._replace(scheme=scheme, netloc=netloc, fragment="").geturl()


def validate_public_url(raw_url: str) -> tuple[bool, str]:
    try:
        url = normalize_url(raw_url)
        parsed = urlparse(url)
    except Exception:
        return False, "Invalid URL."

    if parsed.scheme not in {"http", "https"}:
        return False, "Only http/https URLs are supported."

    host = parsed.hostname or ""
    if not host:
        return False, "URL is missing a hostname."

    if host in {"localhost"} or host.endswith(".local"):
        return False, "Local or private hosts are not allowed."

    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None

    if ip and (
        ip.is_private
        or ip.is_loopback
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
        or ip.is_link_local
    ):
        return False, "Private or reserved IP addresses are blocked."

    return True, url


def build_progress_bar(current: int, total: int, width: int = 10) -> str:
    if total <= 0:
        return "░" * width
    ratio = min(1.0, max(0.0, current / total))
    filled = int(ratio * width)
    return "█" * filled + "░" * (width - filled)


def mime_without_charset(value: str) -> str:
    return (value or "").split(";", 1)[0].strip().lower()


def guess_extension(url: str, mime_type: str = "") -> str:
    parsed = urlparse(url)
    suffix = Path(parsed.path).suffix.lower()
    if suffix in {".pdf", ".jpg", ".jpeg", ".png", ".webp", ".gif", ".txt", ".html", ".htm", ".docx", ".doc"}:
        return suffix
    mime = mime_without_charset(mime_type)
    return {
        "application/pdf": ".pdf",
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "image/gif": ".gif",
        "text/plain": ".txt",
        "text/html": ".html",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
        "application/msword": ".doc",
    }.get(mime, "")


def classify_url_kind(url: str, mime_type: str = "", type_hint: str = "") -> str | None:
    candidate = f"{url} {mime_type} {type_hint}".lower()
    if ".pdf" in candidate or "application/pdf" in candidate:
        return "pdf"
    if any(ext in candidate for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif")) or mime_without_charset(mime_type).startswith("image/"):
        return "image"
    if ".docx" in candidate or "wordprocessingml" in candidate:
        return "docx"
    if ".doc" in candidate or "application/msword" in candidate:
        return "doc"
    if ".txt" in candidate or "text/plain" in candidate:
        return "txt"
    if ".html" in candidate or ".htm" in candidate or "text/html" in candidate:
        return "html"
    return None


def action_label(action: str) -> str:
    return {
        "pdf": "PDF",
        "images": "Images ZIP",
        "txt": "TXT",
        "html": "HTML Snapshot",
    }.get(action, action.upper())


def asset_label(kind: str) -> str:
    return {
        "pdf": "PDF",
        "image": "Image",
        "docx": "DOCX",
        "doc": "DOC",
        "txt": "TXT",
        "html": "HTML",
    }.get(kind, kind.upper())


async def safe_reply_text(target: Any, text: str, **kwargs: Any) -> None:
    with contextlib.suppress(BadRequest, TimedOut):
        await target.reply_text(text, **kwargs)


async def safe_edit_text(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )
    except BadRequest as exc:
        if "Message is not modified" not in str(exc):
            logger.debug("safe_edit_text ignored: %s", exc)
    except (TimedOut, RetryAfter):
        logger.debug("Temporary Telegram edit issue for chat=%s msg=%s", chat_id, message_id)


async def cleanup_path(path: Path) -> None:
    if not path.exists():
        return
    await asyncio.to_thread(shutil.rmtree, path, True)


async def cleanup_old_temp_dirs() -> None:
    TEMP_ROOT.mkdir(parents=True, exist_ok=True)
    cutoff = now_ts() - TEMP_RETENTION_SECONDS
    for child in TEMP_ROOT.iterdir():
        with contextlib.suppress(FileNotFoundError):
            if child.is_dir() and child.stat().st_mtime < cutoff:
                await cleanup_path(child)


class ProgressReporter:
    """Throttles Telegram status edits to avoid noisy updates."""

    def __init__(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        chat_id: int,
        message_id: int,
        min_interval: float = 1.2,
    ) -> None:
        self.context = context
        self.chat_id = chat_id
        self.message_id = message_id
        self.min_interval = min_interval
        self._last_text = ""
        self._last_sent = 0.0

    async def send(
        self,
        text: str,
        reply_markup: InlineKeyboardMarkup | None = None,
        force: bool = False,
    ) -> None:
        if not force and text == self._last_text:
            return
        if not force and now_ts() - self._last_sent < self.min_interval:
            return
        self._last_text = text
        self._last_sent = now_ts()
        await safe_edit_text(self.context, self.chat_id, self.message_id, text, reply_markup=reply_markup)


@dataclass(slots=True)
class ParsedAsset:
    url: str
    kind: str
    label: str
    source: str
    mime_type: str = ""


@dataclass(slots=True)
class LinkAnalysis:
    requested_url: str
    final_url: str
    content_type: str
    title: str
    description: str
    readable_text: str
    text_excerpt: str
    html_snapshot: str
    assets: list[ParsedAsset] = field(default_factory=list)
    fetched_at: float = field(default_factory=now_ts)

    @property
    def domain(self) -> str:
        return urlparse(self.final_url).hostname or "unknown-host"

    @property
    def counts(self) -> Counter[str]:
        return Counter(asset.kind for asset in self.assets)

    @property
    def has_images(self) -> bool:
        return any(asset.kind == "image" for asset in self.assets)

    @property
    def direct_pdf(self) -> ParsedAsset | None:
        for asset in self.assets:
            if asset.kind == "pdf" and asset.url == self.final_url:
                return asset
        for asset in self.assets:
            if asset.kind == "pdf":
                return asset
        return None


@dataclass(slots=True)
class ExportJob:
    job_id: str
    user_id: int
    action: str
    analysis: LinkAnalysis
    chat_id: int
    message_id: int
    requested_at: float = field(default_factory=now_ts)
    cancelled: bool = False


class TTLCache:
    def __init__(self, ttl_seconds: int, max_items: int = 128) -> None:
        self.ttl_seconds = ttl_seconds
        self.max_items = max_items
        self._items: dict[str, tuple[float, Any]] = {}

    def _purge(self) -> None:
        cutoff = now_ts()
        expired = [key for key, (expires, _) in self._items.items() if expires <= cutoff]
        for key in expired:
            self._items.pop(key, None)
        if len(self._items) > self.max_items:
            for key in sorted(self._items, key=lambda item: self._items[item][0])[: len(self._items) - self.max_items]:
                self._items.pop(key, None)

    def get(self, key: str) -> Any | None:
        self._purge()
        item = self._items.get(key)
        if not item:
            return None
        expires, value = item
        if expires <= now_ts():
            self._items.pop(key, None)
            return None
        return copy.deepcopy(value)

    def set(self, key: str, value: Any) -> None:
        self._purge()
        self._items[key] = (now_ts() + self.ttl_seconds, copy.deepcopy(value))


class SpamGuard:
    def __init__(self) -> None:
        self._events: dict[int, deque[float]] = {}
        self._cooldowns: dict[int, float] = {}

    def allow(self, user_id: int) -> tuple[bool, str | None]:
        ts = now_ts()
        last = self._cooldowns.get(user_id, 0.0)
        if ts - last < PER_USER_COOLDOWN_SECONDS:
            wait_time = PER_USER_COOLDOWN_SECONDS - (ts - last)
            return False, f"Please wait {wait_time:.1f}s before sending another request."

        events = self._events.setdefault(user_id, deque())
        while events and ts - events[0] > ANTI_SPAM_WINDOW_SECONDS:
            events.popleft()
        if len(events) >= ANTI_SPAM_MAX_EVENTS:
            return False, "Too many requests. Slow down for a moment."

        events.append(ts)
        self._cooldowns[user_id] = ts
        return True, None


class PersistentStore:
    """Small JSON-backed persistence layer for users, history, settings and stats."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._users: set[int] = set()
        self._history: dict[str, list[dict[str, Any]]] = {}
        self._settings: dict[str, dict[str, Any]] = {}
        self._stats: dict[str, Any] = {"global": {}, "users": {}}

    async def initialize(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self._users = set(int(item) for item in (await self._load_json(USERS_FILE, {"users": []})).get("users", []))
        self._users.add(OWNER_ID)
        self._history = await self._load_json(HISTORY_FILE, {})
        self._settings = await self._load_json(SETTINGS_FILE, {})
        self._stats = await self._load_json(
            STATS_FILE,
            {"global": {"analyzed": 0, "completed": 0, "failed": 0}, "users": {}},
        )
        await self._save_users()

    async def _load_json(self, path: Path, default: Any) -> Any:
        if not path.exists():
            return default
        try:
            async with aiofiles.open(path, "r", encoding="utf-8") as handle:
                raw = await handle.read()
            return json.loads(raw) if raw.strip() else default
        except Exception:
            logger.warning("Failed to load %s, using defaults.", path.name)
            return default

    async def _atomic_write(self, path: Path, payload: Any) -> None:
        temp_path = path.with_suffix(path.suffix + ".tmp")
        serialized = json.dumps(payload, indent=2, ensure_ascii=True)
        async with aiofiles.open(temp_path, "w", encoding="utf-8") as handle:
            await handle.write(serialized)
        await asyncio.to_thread(os.replace, temp_path, path)

    async def _save_users(self) -> None:
        await self._atomic_write(USERS_FILE, {"users": sorted(self._users)})

    async def _save_history(self) -> None:
        await self._atomic_write(HISTORY_FILE, self._history)

    async def _save_settings(self) -> None:
        await self._atomic_write(SETTINGS_FILE, self._settings)

    async def _save_stats(self) -> None:
        await self._atomic_write(STATS_FILE, self._stats)

    async def is_authorized(self, user_id: int) -> bool:
        return user_id in self._users

    async def add_user(self, user_id: int) -> bool:
        async with self._lock:
            if user_id in self._users:
                return False
            self._users.add(user_id)
            await self._save_users()
            return True

    async def remove_user(self, user_id: int) -> bool:
        async with self._lock:
            if user_id == OWNER_ID or user_id not in self._users:
                return False
            self._users.remove(user_id)
            await self._save_users()
            return True

    async def list_users(self) -> list[int]:
        return sorted(self._users)

    async def mark_analyzed(self, user_id: int) -> None:
        async with self._lock:
            self._stats.setdefault("global", {}).setdefault("analyzed", 0)
            self._stats["global"]["analyzed"] += 1
            user_stats = self._stats.setdefault("users", {}).setdefault(str(user_id), {})
            user_stats["analyzed"] = user_stats.get("analyzed", 0) + 1
            await self._save_stats()

    async def record_job(self, user_id: int, entry: dict[str, Any]) -> None:
        async with self._lock:
            bucket = self._history.setdefault(str(user_id), [])
            bucket.insert(0, entry)
            self._history[str(user_id)] = bucket[:100]

            global_stats = self._stats.setdefault("global", {})
            user_stats = self._stats.setdefault("users", {}).setdefault(str(user_id), {})

            if entry.get("status") == "completed":
                global_stats["completed"] = global_stats.get("completed", 0) + 1
                user_stats["completed"] = user_stats.get("completed", 0) + 1
            else:
                global_stats["failed"] = global_stats.get("failed", 0) + 1
                user_stats["failed"] = user_stats.get("failed", 0) + 1

            user_stats["last_title"] = entry.get("title", "")
            user_stats["last_action"] = entry.get("action", "")
            user_stats["last_run_at"] = entry.get("timestamp", now_ts())

            await self._save_history()
            await self._save_stats()

    async def get_history(self, user_id: int) -> list[dict[str, Any]]:
        return list(self._history.get(str(user_id), []))

    async def get_settings(self, user_id: int) -> dict[str, Any]:
        current = self._settings.get(str(user_id), {})
        return {"preview_enabled": current.get("preview_enabled", True)}

    async def toggle_preview(self, user_id: int) -> dict[str, Any]:
        async with self._lock:
            current = self._settings.setdefault(str(user_id), {})
            current["preview_enabled"] = not current.get("preview_enabled", True)
            await self._save_settings()
            return {"preview_enabled": current["preview_enabled"]}

    async def get_stats(self, user_id: int | None = None) -> dict[str, Any]:
        if user_id is None:
            return copy.deepcopy(self._stats.get("global", {}))
        base = copy.deepcopy(self._stats.get("users", {}).get(str(user_id), {}))
        base.setdefault("analyzed", 0)
        base.setdefault("completed", 0)
        base.setdefault("failed", 0)
        return base


# ============================================================================
# 3. DOWNLOADER ENGINE
# ============================================================================


class DownloaderEngine:
    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session

    async def _request(self, method: str, url: str, **kwargs: Any) -> aiohttp.ClientResponse:
        extra_headers = kwargs.pop("headers", {})
        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                response = await self.session.request(
                    method,
                    url,
                    headers={**DEFAULT_HEADERS, **extra_headers},
                    timeout=REQUEST_TIMEOUT,
                    allow_redirects=True,
                    **kwargs,
                )
                if response.status >= 500:
                    raise aiohttp.ClientResponseError(
                        response.request_info,
                        response.history,
                        status=response.status,
                        message=f"Server error {response.status}",
                        headers=response.headers,
                    )
                return response
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = exc
                if attempt < 3:
                    await asyncio.sleep(0.8 * attempt)
                else:
                    break
        raise last_error or RuntimeError("Unknown request failure")

    async def fetch_for_analysis(self, url: str) -> dict[str, Any]:
        async with await self._request("GET", url) as response:
            response.raise_for_status()
            final_url = str(response.url)
            content_type = mime_without_charset(response.headers.get("Content-Type", ""))
            content_length = int(response.headers.get("Content-Length", "0") or 0)
            filename = sanitize_filename(Path(urlparse(final_url).path).name or "document")

            if content_type.startswith("text/") or content_type in {"application/xhtml+xml", "application/xml"}:
                chunks: list[bytes] = []
                received = 0
                async for chunk in response.content.iter_chunked(64 * 1024):
                    received += len(chunk)
                    if received > MAX_ANALYSIS_BYTES:
                        break
                    chunks.append(chunk)
                charset = response.charset or "utf-8"
                body_text = b"".join(chunks).decode(charset, errors="replace")
                return {
                    "final_url": final_url,
                    "content_type": content_type or "text/plain",
                    "content_length": content_length,
                    "filename": filename,
                    "text": body_text,
                }

            return {
                "final_url": final_url,
                "content_type": content_type or "application/octet-stream",
                "content_length": content_length,
                "filename": filename,
                "text": "",
            }

    async def stream_to_file(
        self,
        url: str,
        target: Path,
        *,
        progress_cb: Any | None = None,
        max_bytes: int = MAX_DOWNLOAD_BYTES,
    ) -> tuple[Path, int]:
        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                async with await self._request("GET", url) as response:
                    response.raise_for_status()
                    total = int(response.headers.get("Content-Length", "0") or 0)
                    written = 0
                    async with aiofiles.open(target, "wb") as handle:
                        async for chunk in response.content.iter_chunked(64 * 1024):
                            written += len(chunk)
                            if written > max_bytes:
                                raise ValueError(
                                    f"Downloaded file exceeds configured limit ({format_bytes(max_bytes)})."
                                )
                            await handle.write(chunk)
                            if progress_cb:
                                await progress_cb(written, total)
                    return target, written
            except Exception as exc:
                last_error = exc
                with contextlib.suppress(FileNotFoundError):
                    target.unlink()
                if attempt < 3:
                    await asyncio.sleep(0.8 * attempt)
                else:
                    break
        raise last_error or RuntimeError("Download failed")

    async def download_assets(
        self,
        assets: list[ParsedAsset],
        workdir: Path,
        *,
        progress_cb: Any | None = None,
    ) -> list[tuple[ParsedAsset, Path, int]]:
        semaphore = asyncio.Semaphore(ASSET_CONCURRENCY)
        completed = 0
        results: list[tuple[int, ParsedAsset, Path, int]] = []
        lock = asyncio.Lock()

        async def _download_one(index: int, asset: ParsedAsset) -> None:
            nonlocal completed
            async with semaphore:
                extension = guess_extension(asset.url, asset.mime_type) or (".jpg" if asset.kind == "image" else "")
                filename = sanitize_filename(asset.label or f"{asset.kind}_{index + 1}", default=f"{asset.kind}_{index + 1}")
                target = workdir / f"{index + 1:03d}_{filename}{extension}"
                path, size = await self.stream_to_file(asset.url, target)
                async with lock:
                    results.append((index, asset, path, size))
                    completed += 1
                    if progress_cb:
                        await progress_cb(completed, len(assets))

        await asyncio.gather(*(_download_one(index, asset) for index, asset in enumerate(assets)))
        results.sort(key=lambda item: item[0])
        return [(asset, path, size) for _, asset, path, size in results]

    async def save_text_file(self, target: Path, content: str) -> Path:
        async with aiofiles.open(target, "w", encoding="utf-8") as handle:
            await handle.write(content)
        return target

    async def save_html_file(self, target: Path, content: str) -> Path:
        async with aiofiles.open(target, "w", encoding="utf-8") as handle:
            await handle.write(content)
        return target

    async def build_zip(self, members: list[Path], target: Path) -> Path:
        await asyncio.to_thread(self._build_zip_sync, members, target)
        return target

    @staticmethod
    def _build_zip_sync(members: list[Path], target: Path) -> None:
        with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as archive:
            for path in members:
                archive.write(path, arcname=path.name)

    async def images_to_pdf(self, images: list[Path], target: Path) -> Path:
        await asyncio.to_thread(self._images_to_pdf_sync, images, target)
        return target

    @staticmethod
    def _images_to_pdf_sync(images: list[Path], target: Path) -> None:
        converted: list[Image.Image] = []
        try:
            for path in images:
                with Image.open(path) as source:
                    image = source.convert("RGB")
                    converted.append(image.copy())
            if not converted:
                raise ValueError("No valid images were downloaded.")
            first, *rest = converted
            first.save(target, "PDF", save_all=True, append_images=rest, resolution=150)
        finally:
            for image in converted:
                image.close()

    async def text_to_pdf(self, title: str, text: str, target: Path) -> Path:
        await asyncio.to_thread(self._text_to_pdf_sync, title, text, target)
        return target

    @staticmethod
    def _text_to_pdf_sync(title: str, text: str, target: Path) -> None:
        width, height = 1240, 1754
        margin = 72
        line_height = 24
        font = ImageFont.load_default()

        raw_lines: list[str] = []
        wrapper = textwrap.TextWrapper(width=95, break_long_words=True, replace_whitespace=False)
        for paragraph in (text or "").splitlines():
            if not paragraph.strip():
                raw_lines.append("")
                continue
            raw_lines.extend(wrapper.wrap(paragraph))

        if not raw_lines:
            raw_lines = ["No readable text was available for this page."]

        pages: list[Image.Image] = []
        cursor = 0
        while cursor < len(raw_lines):
            canvas = Image.new("RGB", (width, height), "white")
            draw = ImageDraw.Draw(canvas)
            y = margin

            if not pages:
                draw.text((margin, y), title[:110] or "Document Export", fill="black", font=font)
                y += line_height * 2

            while cursor < len(raw_lines) and y < height - margin:
                line = raw_lines[cursor]
                draw.text((margin, y), line, fill="black", font=font)
                y += line_height
                cursor += 1

            pages.append(canvas)

        first, *rest = pages
        first.save(target, "PDF", save_all=True, append_images=rest, resolution=150)
        for page in pages:
            page.close()


# ============================================================================
# 4. PARSER ENGINE
# ============================================================================


class ParserEngine:
    def parse(self, requested_url: str, fetched: dict[str, Any]) -> LinkAnalysis:
        content_type = fetched["content_type"]
        final_url = fetched["final_url"]
        text = fetched.get("text", "")

        if content_type.startswith("text/html") or final_url.lower().endswith((".html", ".htm", "/")):
            return self._parse_html(requested_url, final_url, content_type, text)
        if content_type.startswith("text/plain"):
            return self._parse_text(requested_url, final_url, content_type, text)
        return self._parse_binary(requested_url, final_url, content_type, fetched.get("filename", "document"))

    def _parse_text(self, requested_url: str, final_url: str, content_type: str, text: str) -> LinkAnalysis:
        title = sanitize_filename(Path(urlparse(final_url).path).name or "text_document").replace("_", " ")
        readable_text = self._normalize_text(text)
        description = short_text(readable_text, 240)
        html_snapshot = self._html_from_text(title, description, readable_text)
        assets = [ParsedAsset(url=final_url, kind="txt", label=title, source="direct", mime_type=content_type)]
        return LinkAnalysis(
            requested_url=requested_url,
            final_url=final_url,
            content_type=content_type,
            title=title,
            description=description,
            readable_text=readable_text[:MAX_TEXT_EXPORT_CHARS],
            text_excerpt=short_text(readable_text, 320),
            html_snapshot=html_snapshot,
            assets=assets,
        )

    def _parse_binary(self, requested_url: str, final_url: str, content_type: str, filename: str) -> LinkAnalysis:
        kind = classify_url_kind(final_url, content_type) or "file"
        title = sanitize_filename(filename or kind).replace("_", " ")
        description = f"Direct downloadable asset detected ({content_type or 'binary file'})."
        html_snapshot = self._simple_html_snapshot(
            title=title,
            description=description,
            readable_text="This link points to a downloadable document rather than a webpage.",
            source_url=final_url,
            assets=[ParsedAsset(url=final_url, kind=kind, label=title, source="direct", mime_type=content_type)],
        )
        return LinkAnalysis(
            requested_url=requested_url,
            final_url=final_url,
            content_type=content_type,
            title=title,
            description=description,
            readable_text="",
            text_excerpt=description,
            html_snapshot=html_snapshot,
            assets=[ParsedAsset(url=final_url, kind=kind, label=title, source="direct", mime_type=content_type)],
        )

    def _parse_html(self, requested_url: str, final_url: str, content_type: str, raw_html: str) -> LinkAnalysis:
        soup = BeautifulSoup(raw_html or "", HTML_PARSER)
        title = self._extract_title(soup, final_url)
        description = self._extract_description(soup)
        assets = self._collect_assets(soup, final_url)
        readable_text = self._extract_text(soup)
        excerpt = short_text(readable_text or description or title, 320)
        return LinkAnalysis(
            requested_url=requested_url,
            final_url=final_url,
            content_type=content_type or "text/html",
            title=title,
            description=description,
            readable_text=readable_text[:MAX_TEXT_EXPORT_CHARS],
            text_excerpt=excerpt,
            html_snapshot=raw_html,
            assets=assets,
        )

    def _extract_title(self, soup: BeautifulSoup, fallback_url: str) -> str:
        for selector in (
            lambda: soup.find("meta", property="og:title"),
            lambda: soup.find("meta", attrs={"name": "twitter:title"}),
            lambda: soup.title,
            lambda: soup.find("h1"),
        ):
            tag = selector()
            if not tag:
                continue
            text = tag.get("content") if hasattr(tag, "get") else None
            text = text or getattr(tag, "string", None) or tag.get_text(" ", strip=True)
            if text and text.strip():
                return short_text(text.strip(), 120)
        fallback = Path(urlparse(fallback_url).path).name or urlparse(fallback_url).hostname or "Document"
        return fallback.replace("-", " ").replace("_", " ").title()

    def _extract_description(self, soup: BeautifulSoup) -> str:
        for attrs in (
            {"name": "description"},
            {"property": "og:description"},
            {"name": "twitter:description"},
        ):
            tag = soup.find("meta", attrs=attrs)
            if tag and tag.get("content"):
                return short_text(tag["content"], 240)
        first_para = soup.find("p")
        return short_text(first_para.get_text(" ", strip=True) if first_para else "", 240)

    def _collect_assets(self, soup: BeautifulSoup, base_url: str) -> list[ParsedAsset]:
        results: list[ParsedAsset] = []
        seen: set[str] = set()

        def add_asset(candidate_url: str, kind: str | None, label: str, source: str, mime_type: str = "") -> None:
            if not candidate_url or candidate_url.startswith(("data:", "javascript:", "mailto:")):
                return
            absolute = urljoin(base_url, candidate_url)
            absolute = absolute.split("#", 1)[0]
            if absolute in seen:
                return
            if not absolute.startswith(("http://", "https://")):
                return
            inferred_kind = kind or classify_url_kind(absolute, mime_type) or "file"
            seen.add(absolute)
            results.append(
                ParsedAsset(
                    url=absolute,
                    kind=inferred_kind,
                    label=short_text(label or Path(urlparse(absolute).path).name or inferred_kind.title(), 80),
                    source=source,
                    mime_type=mime_type,
                )
            )

        for tag in soup.find_all(["a", "iframe", "embed", "object", "img", "source", "meta"]):
            if tag.name == "meta":
                prop = (tag.get("property") or tag.get("name") or "").lower()
                if prop in {"og:image", "twitter:image"} and tag.get("content"):
                    add_asset(tag["content"], "image", "Preview image", "meta")
                continue

            attr = "href" if tag.name == "a" else "src"
            if tag.name == "object":
                attr = "data"

            url = tag.get(attr, "")
            mime_type = tag.get("type", "")
            label = tag.get("title") or tag.get("alt") or tag.get_text(" ", strip=True)

            if tag.name in {"img", "source"}:
                srcset = tag.get("srcset", "")
                if srcset:
                    candidate = srcset.split(",")[-1].strip().split(" ")[0]
                    if candidate:
                        url = candidate
                width = int(tag.get("width") or 0)
                height = int(tag.get("height") or 0)
                lowered = (url or "").lower()
                if (width and width < 120) and (height and height < 120) and any(
                    token in lowered for token in ("icon", "logo", "avatar", "sprite")
                ):
                    continue
                add_asset(url, "image", label or "Image asset", tag.name, mime_type)
                continue

            kind = classify_url_kind(url, mime_type, tag.get("class", ""))
            if tag.name in {"iframe", "embed", "object"} and not kind:
                kind = "html"
            if kind:
                add_asset(url, kind, label or f"{kind.upper()} asset", tag.name, mime_type)

        return results[:150]

    def _extract_text(self, soup: BeautifulSoup) -> str:
        candidate_html = BeautifulSoup(str(soup), HTML_PARSER)
        for tag in candidate_html(["script", "style", "noscript", "svg", "canvas", "form", "header", "footer", "nav"]):
            tag.decompose()

        best_text = ""
        for selector in ("article", "main", "[role='main']", ".content", "#content", "body"):
            node = candidate_html.select_one(selector)
            if not node:
                continue
            text = self._normalize_text(node.get_text("\n", strip=True))
            if len(text) > len(best_text):
                best_text = text
        return best_text

    @staticmethod
    def _normalize_text(text: str) -> str:
        text = text or ""
        text = re.sub(r"\r\n?", "\n", text)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _html_from_text(self, title: str, description: str, readable_text: str) -> str:
        escaped_title = html.escape(title)
        escaped_description = html.escape(description or "")
        paragraphs = "".join(
            f"<p>{html.escape(line)}</p>\n" for line in readable_text.splitlines() if line.strip()
        ) or "<p><em>No readable text was available.</em></p>"
        return (
            "<!DOCTYPE html>\n"
            "<html lang='en'><head><meta charset='utf-8'>"
            "<meta name='viewport' content='width=device-width, initial-scale=1'>"
            f"<title>{escaped_title}</title>"
            "<style>body{font-family:Arial,sans-serif;max-width:860px;margin:2rem auto;padding:0 1rem;line-height:1.7;color:#222}"
            "h1{font-size:1.8rem} .meta{color:#555;margin-bottom:1.5rem}</style></head><body>"
            f"<h1>{escaped_title}</h1><div class='meta'>{escaped_description}</div>{paragraphs}</body></html>"
        )

    def _simple_html_snapshot(
        self,
        *,
        title: str,
        description: str,
        readable_text: str,
        source_url: str,
        assets: list[ParsedAsset],
    ) -> str:
        links = "".join(
            f"<li><a href='{html.escape(asset.url)}'>{html.escape(asset.label)} ({html.escape(asset.kind.upper())})</a></li>"
            for asset in assets
        ) or "<li>No embedded assets detected.</li>"
        return (
            "<!DOCTYPE html><html lang='en'><head><meta charset='utf-8'>"
            f"<title>{html.escape(title)}</title></head><body>"
            f"<h1>{html.escape(title)}</h1>"
            f"<p>{html.escape(description)}</p>"
            f"<p><strong>Source:</strong> <a href='{html.escape(source_url)}'>{html.escape(source_url)}</a></p>"
            f"<pre>{html.escape(readable_text)}</pre>"
            f"<ul>{links}</ul></body></html>"
        )


# ============================================================================
# 5. TELEGRAM RUNTIME / QUEUE
# ============================================================================


class BotRuntime:
    def __init__(self) -> None:
        self.application: Application | None = None
        self.session: aiohttp.ClientSession | None = None
        self.store = PersistentStore()
        self.cache = TTLCache(CACHE_TTL_SECONDS, CACHE_MAX_ITEMS)
        self.spam_guard = SpamGuard()
        self.parser = ParserEngine()
        self.downloader: DownloaderEngine | None = None
        self.queue = DownloadCoordinator(self)
        self.sessions: dict[int, LinkAnalysis] = {}

    async def startup(self, application: Application) -> None:
        self.application = application
        connector = aiohttp.TCPConnector(limit=HTTP_CONNECTION_LIMIT, ttl_dns_cache=300)
        self.session = aiohttp.ClientSession(timeout=REQUEST_TIMEOUT, connector=connector)
        self.downloader = DownloaderEngine(self.session)
        await self.store.initialize()
        await cleanup_old_temp_dirs()
        await self.queue.start()
        logger.info("Runtime initialized. Queue workers=%s owner=%s", QUEUE_WORKERS, OWNER_ID)

    async def shutdown(self) -> None:
        await self.queue.stop()
        if self.session:
            await self.session.close()
        self.session = None
        logger.info("Runtime shut down cleanly.")

    def require_downloader(self) -> DownloaderEngine:
        if not self.downloader:
            raise RuntimeError("Runtime is not initialized.")
        return self.downloader

    async def analyze_url(self, user_id: int, url: str) -> LinkAnalysis:
        ok, normalized_or_reason = validate_public_url(url)
        if not ok:
            raise ValueError(normalized_or_reason)

        normalized_url = normalized_or_reason
        cached = self.cache.get(normalized_url)
        if cached:
            logger.info("Cache hit for %s", normalized_url)
            self.sessions[user_id] = cached
            return cached

        fetched = await self.require_downloader().fetch_for_analysis(normalized_url)
        analysis = self.parser.parse(normalized_url, fetched)
        self.cache.set(normalized_url, analysis)
        self.sessions[user_id] = analysis
        await self.store.mark_analyzed(user_id)
        return analysis

    async def process_export(self, job: ExportJob, context: Any) -> None:
        reporter = ProgressReporter(context, job.chat_id, job.message_id)
        workdir = Path(tempfile.mkdtemp(prefix="uddb_", dir=str(TEMP_ROOT)))
        title = sanitize_filename(job.analysis.title or "document")
        result_path: Path | None = None
        result_size = 0

        try:
            if job.cancelled:
                await reporter.send("❌ <b>Error</b>\n\nThis job was cancelled before it started.", force=True)
                return

            await reporter.send(
                "🚀 <b>Starting download</b>\n\n"
                f"📄 <b>{html.escape(job.analysis.title)}</b>\n"
                f"⚡ Format: {html.escape(action_label(job.action))}",
                force=True,
            )

            await context.bot.send_chat_action(job.chat_id, ChatAction.TYPING)
            await reporter.send(
                "⚡ <b>Processing link</b>\n\n"
                f"🌐 Source: <code>{html.escape(job.analysis.domain)}</code>\n"
                f"📦 Export: {html.escape(action_label(job.action))}",
                force=True,
            )

            if job.action == "pdf":
                result_path, result_size = await self._export_pdf(job, workdir, reporter)
            elif job.action == "images":
                result_path, result_size = await self._export_images_zip(job, workdir, reporter)
            elif job.action == "txt":
                result_path, result_size = await self._export_txt(job, workdir, reporter)
            elif job.action == "html":
                result_path, result_size = await self._export_html(job, workdir, reporter)
            else:
                raise ValueError("Unsupported export action.")

            await reporter.send(
                "📤 <b>Uploading to Telegram</b>\n\n"
                f"📦 File: <code>{html.escape(result_path.name)}</code>\n"
                f"📏 Size: {html.escape(format_bytes(result_size))}",
                force=True,
            )

            await context.bot.send_chat_action(job.chat_id, ChatAction.UPLOAD_DOCUMENT)
            caption = (
                f"✅ <b>{html.escape(job.analysis.title)}</b>\n"
                f"📤 {html.escape(action_label(job.action))}\n"
                f"🌐 {html.escape(job.analysis.domain)}\n"
                f"📏 {html.escape(format_bytes(result_size))}"
            )

            with result_path.open("rb") as handle:
                await context.bot.send_document(
                    chat_id=job.chat_id,
                    document=InputFile(handle, filename=result_path.name),
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    read_timeout=120,
                    write_timeout=120,
                )

            await reporter.send(
                "✅ <b>Completed</b>\n\n"
                f"📄 <b>{html.escape(job.analysis.title)}</b>\n"
                f"📦 {html.escape(action_label(job.action))} sent successfully.",
                reply_markup=main_menu_keyboard(),
                force=True,
            )

            await self.store.record_job(
                job.user_id,
                {
                    "timestamp": now_ts(),
                    "status": "completed",
                    "action": job.action,
                    "title": job.analysis.title,
                    "url": job.analysis.requested_url,
                    "file_name": result_path.name,
                    "size_bytes": result_size,
                },
            )
        except Exception as exc:
            logger.exception("Export failed for user=%s action=%s", job.user_id, job.action)
            await reporter.send(
                "❌ <b>Error</b>\n\n"
                f"{html.escape(short_text(str(exc), 300))}\n\n"
                "Try another format or send a different link.",
                reply_markup=main_menu_keyboard(),
                force=True,
            )
            await self.store.record_job(
                job.user_id,
                {
                    "timestamp": now_ts(),
                    "status": "failed",
                    "action": job.action,
                    "title": job.analysis.title,
                    "url": job.analysis.requested_url,
                    "error": short_text(str(exc), 300),
                },
            )
        finally:
            await cleanup_path(workdir)

    async def _export_pdf(
        self,
        job: ExportJob,
        workdir: Path,
        reporter: ProgressReporter,
    ) -> tuple[Path, int]:
        downloader = self.require_downloader()
        direct_pdf = job.analysis.direct_pdf
        if direct_pdf:
            target = workdir / f"{sanitize_filename(job.analysis.title)}.pdf"

            async def _byte_progress(current: int, total: int) -> None:
                if total > 0:
                    pct = int((current / total) * 100)
                    bar = build_progress_bar(current, total)
                    await reporter.send(
                        "📥 <b>Downloading assets</b>\n\n"
                        f"[{bar}] {pct}%\n"
                        f"📄 Direct PDF • {html.escape(format_bytes(current))}/{html.escape(format_bytes(total))}"
                    )

            path, size = await downloader.stream_to_file(direct_pdf.url, target, progress_cb=_byte_progress)
            return path, size

        image_assets = [asset for asset in job.analysis.assets if asset.kind == "image"][:MAX_IMAGES_PER_JOB]
        if image_assets:
            await reporter.send(
                "📥 <b>Downloading assets</b>\n\n"
                f"🖼 Preparing {len(image_assets)} image assets for PDF conversion.",
                force=True,
            )

            async def _count_progress(current: int, total: int) -> None:
                await reporter.send(
                    "📥 <b>Downloading assets</b>\n\n"
                    f"[{build_progress_bar(current, total)}] {int(current / total * 100)}%\n"
                    f"🖼 Images: {current}/{total}"
                )

            downloaded = await downloader.download_assets(image_assets, workdir, progress_cb=_count_progress)
            if not downloaded:
                raise ValueError("No image assets could be downloaded.")

            await reporter.send("📦 <b>Packaging files</b>\n\nConverting images into a single PDF.", force=True)
            pdf_path = workdir / f"{sanitize_filename(job.analysis.title)}.pdf"
            await downloader.images_to_pdf([path for _, path, _ in downloaded], pdf_path)
            return pdf_path, pdf_path.stat().st_size

        if not job.analysis.readable_text.strip():
            raise ValueError("No direct PDF, image sequence, or readable text was available for PDF export.")

        await reporter.send("📦 <b>Packaging files</b>\n\nRendering extracted text into a PDF document.", force=True)
        pdf_path = workdir / f"{sanitize_filename(job.analysis.title)}.pdf"
        await downloader.text_to_pdf(job.analysis.title, job.analysis.readable_text, pdf_path)
        return pdf_path, pdf_path.stat().st_size

    async def _export_images_zip(
        self,
        job: ExportJob,
        workdir: Path,
        reporter: ProgressReporter,
    ) -> tuple[Path, int]:
        downloader = self.require_downloader()
        assets = [asset for asset in job.analysis.assets if asset.kind in {"image", "pdf", "docx", "doc"}][:MAX_IMAGES_PER_JOB]
        if not assets:
            raise ValueError("No image or downloadable document assets were detected for ZIP export.")

        await reporter.send(
            "📥 <b>Downloading assets</b>\n\n"
            f"📦 Collecting {len(assets)} assets for ZIP packaging.",
            force=True,
        )

        async def _count_progress(current: int, total: int) -> None:
            await reporter.send(
                "📥 <b>Downloading assets</b>\n\n"
                f"[{build_progress_bar(current, total)}] {int(current / total * 100)}%\n"
                f"📦 Assets: {current}/{total}"
            )

        downloaded = await downloader.download_assets(assets, workdir, progress_cb=_count_progress)
        if not downloaded:
            raise ValueError("No assets could be downloaded for ZIP export.")

        await reporter.send("📦 <b>Packaging files</b>\n\nCreating ZIP archive.", force=True)
        zip_path = workdir / f"{sanitize_filename(job.analysis.title)}_assets.zip"
        await downloader.build_zip([path for _, path, _ in downloaded], zip_path)
        return zip_path, zip_path.stat().st_size

    async def _export_txt(
        self,
        job: ExportJob,
        workdir: Path,
        reporter: ProgressReporter,
    ) -> tuple[Path, int]:
        text = job.analysis.readable_text.strip()
        if not text:
            raise ValueError("Readable text could not be extracted from this link.")

        await reporter.send("📦 <b>Packaging files</b>\n\nPreparing TXT export.", force=True)
        header = (
            f"{'=' * 72}\n"
            f"{job.analysis.title}\n"
            f"Source: {job.analysis.final_url}\n"
            f"Generated: {format_ts(now_ts())}\n"
            f"{'=' * 72}\n\n"
        )
        txt_path = workdir / f"{sanitize_filename(job.analysis.title)}.txt"
        await self.require_downloader().save_text_file(txt_path, header + text)
        return txt_path, txt_path.stat().st_size

    async def _export_html(
        self,
        job: ExportJob,
        workdir: Path,
        reporter: ProgressReporter,
    ) -> tuple[Path, int]:
        await reporter.send("📦 <b>Packaging files</b>\n\nSaving HTML snapshot.", force=True)
        snapshot = job.analysis.html_snapshot.strip()
        if not snapshot:
            snapshot = ParserEngine()._simple_html_snapshot(
                title=job.analysis.title,
                description=job.analysis.description,
                readable_text=job.analysis.readable_text,
                source_url=job.analysis.final_url,
                assets=job.analysis.assets,
            )
        html_path = workdir / f"{sanitize_filename(job.analysis.title)}.html"
        await self.require_downloader().save_html_file(html_path, snapshot)
        return html_path, html_path.stat().st_size


class DownloadCoordinator:
    """FIFO export queue with worker tasks and per-user de-duplication."""

    def __init__(self, runtime: BotRuntime) -> None:
        self.runtime = runtime
        self.queue: asyncio.Queue[ExportJob | None] = asyncio.Queue()
        self.workers: list[asyncio.Task[Any]] = []
        self.pending_jobs: dict[int, ExportJob] = {}
        self.running_users: set[int] = set()
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        if self.workers:
            return
        for index in range(QUEUE_WORKERS):
            self.workers.append(asyncio.create_task(self._worker_loop(index), name=f"queue-worker-{index}"))

    async def stop(self) -> None:
        for _ in self.workers:
            await self.queue.put(None)
        if self.workers:
            await asyncio.gather(*self.workers, return_exceptions=True)
        self.workers.clear()
        self.pending_jobs.clear()
        self.running_users.clear()

    async def enqueue(self, job: ExportJob) -> int:
        async with self._lock:
            if job.user_id in self.pending_jobs or job.user_id in self.running_users:
                raise ValueError("You already have a queued or active export job.")
            self.pending_jobs[job.user_id] = job
            await self.queue.put(job)
            position = self.queue.qsize() + len(self.running_users)
            return position

    async def cancel_user(self, user_id: int) -> bool:
        async with self._lock:
            job = self.pending_jobs.get(user_id)
            if not job:
                return False
            job.cancelled = True
            self.pending_jobs.pop(user_id, None)
            return True

    def is_busy(self, user_id: int) -> bool:
        return user_id in self.pending_jobs or user_id in self.running_users

    async def _worker_loop(self, worker_id: int) -> None:
        logger.info("Queue worker %s started.", worker_id)
        while True:
            job = await self.queue.get()
            if job is None:
                self.queue.task_done()
                break

            if job.cancelled:
                self.queue.task_done()
                continue

            async with self._lock:
                self.pending_jobs.pop(job.user_id, None)
                self.running_users.add(job.user_id)

            try:
                if not self.runtime.application:
                    raise RuntimeError("Application context not initialized.")
                proxy_context = type("BotProxyContext", (), {"bot": self.runtime.application.bot})()
                await self.runtime.process_export(job, proxy_context)
            except Exception:
                logger.exception("Unhandled queue worker failure for user=%s", job.user_id)
            finally:
                async with self._lock:
                    self.running_users.discard(job.user_id)
                self.queue.task_done()
        logger.info("Queue worker %s stopped.", worker_id)


# ============================================================================
# 6. TELEGRAM HANDLERS
# ============================================================================


def get_runtime(context: ContextTypes.DEFAULT_TYPE) -> BotRuntime:
    runtime = context.application.bot_data.get("runtime")
    if not isinstance(runtime, BotRuntime):
        raise RuntimeError("Bot runtime is unavailable.")
    return runtime


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📥 Download Document", callback_data="menu:download"),
                InlineKeyboardButton("📂 My Downloads", callback_data="menu:history:0"),
            ],
            [
                InlineKeyboardButton("📚 Extract Text", callback_data="menu:extract"),
                InlineKeyboardButton("🌐 Website Snapshot", callback_data="menu:snapshot"),
            ],
            [
                InlineKeyboardButton("⚙️ Settings", callback_data="menu:settings"),
                InlineKeyboardButton("ℹ️ Help", callback_data="menu:help"),
            ],
        ]
    )


def preview_keyboard(page: int, total_pages: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton("⬇ Download as PDF", callback_data="action:pdf"),
            InlineKeyboardButton("🖼 Download Images", callback_data="action:images"),
        ],
        [
            InlineKeyboardButton("📄 Extract Text", callback_data="action:txt"),
            InlineKeyboardButton("🌐 Save HTML", callback_data="action:html"),
        ],
    ]
    nav: list[InlineKeyboardButton] = []
    if total_pages > 1:
        if page > 0:
            nav.append(InlineKeyboardButton("⬅ Prev Page", callback_data=f"analysis:page:{page - 1}"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton("➡ Next Page", callback_data=f"analysis:page:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="menu:back")])
    return InlineKeyboardMarkup(rows)


def history_keyboard(page: int, total_pages: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅ Prev Page", callback_data=f"history:page:{page - 1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("➡ Next Page", callback_data=f"history:page:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="menu:back")])
    return InlineKeyboardMarkup(rows)


def settings_keyboard(preview_enabled: bool) -> InlineKeyboardMarkup:
    label = "✅ Link Preview: ON" if preview_enabled else "☑️ Link Preview: OFF"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(label, callback_data="settings:toggle_preview")],
            [InlineKeyboardButton("🔙 Back", callback_data="menu:back")],
        ]
    )


def render_analysis_text(analysis: LinkAnalysis, page: int, preview_enabled: bool) -> tuple[str, int]:
    assets = analysis.assets
    total_pages = max(1, (len(assets) + MENU_PAGE_SIZE - 1) // MENU_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    counts = analysis.counts
    header = (
        "⚡ <b>Processing link</b>\n\n"
        f"📄 <b>{html.escape(analysis.title)}</b>\n"
        f"🌐 <code>{html.escape(analysis.domain)}</code>\n"
        f"🧾 Content-Type: <code>{html.escape(analysis.content_type or 'unknown')}</code>\n"
        f"📚 Assets: PDF={counts.get('pdf', 0)} • Images={counts.get('image', 0)} • "
        f"DOCX/DOC={counts.get('docx', 0) + counts.get('doc', 0)} • TXT={counts.get('txt', 0)}\n"
        f"📝 Preview: {html.escape(analysis.text_excerpt or analysis.description or 'No text preview available.')}"
    )

    if not preview_enabled or not assets:
        return header, total_pages

    start = page * MENU_PAGE_SIZE
    current_assets = assets[start : start + MENU_PAGE_SIZE]
    lines = [f"\n\n<b>Detected assets • page {page + 1}/{total_pages}</b>"]
    for index, asset in enumerate(current_assets, start=1 + start):
        lines.append(
            f"{index}. <b>{html.escape(asset_label(asset.kind))}</b> - "
            f"{html.escape(short_text(asset.label, 52))}"
        )
    return header + "\n" + "\n".join(lines), total_pages


def render_history_text(history: list[dict[str, Any]], page: int, user_stats: dict[str, Any]) -> tuple[str, int]:
    total_pages = max(1, (len(history) + HISTORY_PAGE_SIZE - 1) // HISTORY_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * HISTORY_PAGE_SIZE
    entries = history[start : start + HISTORY_PAGE_SIZE]

    lines = [
        "📂 <b>My Downloads</b>\n",
        f"📊 Analyzed: {user_stats.get('analyzed', 0)}",
        f"✅ Completed: {user_stats.get('completed', 0)}",
        f"❌ Failed: {user_stats.get('failed', 0)}",
        "",
    ]

    if not entries:
        lines.append("No downloads recorded yet.")
    else:
        for idx, entry in enumerate(entries, start=1 + start):
            lines.append(
                f"{idx}. <b>{html.escape(short_text(entry.get('title', 'Untitled'), 45))}</b>\n"
                f"   {html.escape(action_label(entry.get('action', 'file')))} • "
                f"{'✅' if entry.get('status') == 'completed' else '❌'} • "
                f"{html.escape(format_ts(entry.get('timestamp', now_ts())))}"
            )

    lines.append(f"\nPage {page + 1}/{total_pages}")
    return "\n".join(lines), total_pages


async def ensure_authorized(update: Update, context: ContextTypes.DEFAULT_TYPE, *, alert: bool = False) -> bool:
    runtime = get_runtime(context)
    user_id = update.effective_user.id if update.effective_user else 0
    if await runtime.store.is_authorized(user_id):
        return True
    message = "❌ You are not authorized to use this bot. Ask the owner to approve your Telegram ID."
    if update.callback_query:
        with contextlib.suppress(BadRequest):
            await update.callback_query.answer(message, show_alert=alert)
    elif update.effective_message:
        await safe_reply_text(update.effective_message, message)
    return False


async def ensure_owner(update: Update) -> bool:
    user_id = update.effective_user.id if update.effective_user else 0
    return user_id == OWNER_ID


async def send_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    allowed = await get_runtime(context).store.is_authorized(user.id)
    access_line = "✅ Access granted." if allowed else "🔒 Private mode is enabled. Ask the owner to add your ID."
    text = (
        f"🚀 <b>Universal Document Downloader & Viewer Bot</b>\n\n"
        f"Hello, {html.escape(user.first_name or 'there')}.\n"
        f"{access_line}\n\n"
        "Send a public URL or use the menu below."
    )
    if update.effective_message:
        await update.effective_message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(),
            disable_web_page_preview=True,
        )


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await send_welcome(update, context)


async def cmd_help(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_message:
        await update.effective_message.reply_text(
            HELP_TEXT,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=main_menu_keyboard(),
        )


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    runtime = get_runtime(context)
    user_id = update.effective_user.id
    context.user_data.pop("prompt_mode", None)
    cancelled = await runtime.queue.cancel_user(user_id)
    message = "Cancelled the URL prompt." if not cancelled else "Cancelled your queued download job."
    if update.effective_message:
        await update.effective_message.reply_text(message, reply_markup=main_menu_keyboard())
    return ConversationHandler.END


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update, context):
        return
    runtime = get_runtime(context)
    user_id = update.effective_user.id
    own_stats = await runtime.store.get_stats(user_id)
    global_stats = await runtime.store.get_stats(None) if user_id == OWNER_ID else None

    lines = [
        "📊 <b>Your Statistics</b>",
        f"🔎 Links analyzed: {own_stats.get('analyzed', 0)}",
        f"✅ Completed jobs: {own_stats.get('completed', 0)}",
        f"❌ Failed jobs: {own_stats.get('failed', 0)}",
    ]
    if global_stats is not None:
        lines.extend(
            [
                "",
                "👑 <b>Global</b>",
                f"🔎 Total analyzed: {global_stats.get('analyzed', 0)}",
                f"✅ Total completed: {global_stats.get('completed', 0)}",
                f"❌ Total failed: {global_stats.get('failed', 0)}",
            ]
        )

    await update.effective_message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def prompt_for_url(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_authorized(update, context, alert=True):
        return ConversationHandler.END
    query = update.callback_query
    await query.answer()
    action = query.data.split(":", 1)[1]
    context.user_data["prompt_mode"] = action
    await query.edit_message_text(
        "🔗 Send the public URL you want me to analyze.\n\n"
        "Supported: webpages, PDFs, images, TXT files, and accessible DOC/DOCX links.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="menu:back")]]),
    )
    return WAITING_FOR_URL


async def analyze_message_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["_last_processed_message_id"] = update.effective_message.message_id
    if not await ensure_authorized(update, context):
        return ConversationHandler.END

    runtime = get_runtime(context)
    user_id = update.effective_user.id
    allowed, reason = runtime.spam_guard.allow(user_id)
    if not allowed:
        await safe_reply_text(update.effective_message, f"❌ {reason}")
        return WAITING_FOR_URL if "prompt_mode" in context.user_data else ConversationHandler.END

    if runtime.queue.is_busy(user_id):
        await safe_reply_text(update.effective_message, "⏳ You already have a queued or active job.")
        return WAITING_FOR_URL if "prompt_mode" in context.user_data else ConversationHandler.END

    raw_url = extract_first_url(update.effective_message.text or "")
    if not raw_url:
        await safe_reply_text(update.effective_message, "❌ No valid URL detected. Please send a public http/https link.")
        return WAITING_FOR_URL if "prompt_mode" in context.user_data else ConversationHandler.END

    status = await update.effective_message.reply_text(
        "⚡ <b>Processing link</b>\n\nPlease wait while I analyze the URL.",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )

    try:
        analysis = await runtime.analyze_url(user_id, raw_url)
        settings = await runtime.store.get_settings(user_id)
        preview_text, total_pages = render_analysis_text(analysis, page=0, preview_enabled=settings["preview_enabled"])
        await status.edit_text(
            preview_text,
            parse_mode=ParseMode.HTML,
            reply_markup=preview_keyboard(0, total_pages),
            disable_web_page_preview=True,
        )
    except Exception as exc:
        logger.exception("Link analysis failed for user=%s", user_id)
        await status.edit_text(
            "❌ <b>Error</b>\n\n"
            f"{html.escape(short_text(str(exc), 300))}",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(),
            disable_web_page_preview=True,
        )

    context.user_data.pop("prompt_mode", None)
    return ConversationHandler.END


async def handle_direct_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if context.user_data.get("_last_processed_message_id") == update.effective_message.message_id:
        return
    if extract_first_url(update.effective_message.text or ""):
        await analyze_message_link(update, context)


async def show_history_page(query: Any, context: ContextTypes.DEFAULT_TYPE, page: int) -> None:
    runtime = get_runtime(context)
    user_id = query.from_user.id
    history = await runtime.store.get_history(user_id)
    stats = await runtime.store.get_stats(user_id)
    text, total_pages = render_history_text(history, page, stats)
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=history_keyboard(max(0, min(page, total_pages - 1)), total_pages),
    )


async def show_settings(query: Any, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = get_runtime(context)
    settings = await runtime.store.get_settings(query.from_user.id)
    stats = await runtime.store.get_stats(query.from_user.id)
    text = (
        "⚙️ <b>Settings</b>\n\n"
        f"Link preview: <b>{'Enabled' if settings['preview_enabled'] else 'Disabled'}</b>\n"
        f"Queue workers: <b>{QUEUE_WORKERS}</b>\n"
        f"Max job size: <b>{format_bytes(MAX_DOWNLOAD_BYTES)}</b>\n\n"
        "📊 <b>Quick stats</b>\n"
        f"Analyzed: {stats.get('analyzed', 0)}\n"
        f"Completed: {stats.get('completed', 0)}\n"
        f"Failed: {stats.get('failed', 0)}"
    )
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=settings_keyboard(settings["preview_enabled"]),
    )


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    data = query.data or ""
    runtime = get_runtime(context)

    if data == "menu:back":
        await query.edit_message_text(
            "🏠 <b>Main Menu</b>\n\nChoose an option below or send a public URL directly.",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(),
        )
        return

    if data == "menu:help":
        await query.edit_message_text(
            HELP_TEXT,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="menu:back")]]),
            disable_web_page_preview=True,
        )
        return

    if not await ensure_authorized(update, context, alert=True):
        return

    if data.startswith("menu:history:"):
        page = int(data.rsplit(":", 1)[1])
        await show_history_page(query, context, page)
        return

    if data.startswith("history:page:"):
        page = int(data.rsplit(":", 1)[1])
        await show_history_page(query, context, page)
        return

    if data == "menu:settings":
        await show_settings(query, context)
        return

    if data == "settings:toggle_preview":
        settings = await runtime.store.toggle_preview(query.from_user.id)
        await show_settings(query, context)
        with contextlib.suppress(BadRequest):
            await query.answer(
                f"Link preview {'enabled' if settings['preview_enabled'] else 'disabled'}.",
                show_alert=False,
            )
        return

    if data.startswith("analysis:page:"):
        analysis = runtime.sessions.get(query.from_user.id)
        if not analysis:
            await query.edit_message_text("❌ This preview session expired. Send the URL again.", reply_markup=main_menu_keyboard())
            return
        page = int(data.rsplit(":", 1)[1])
        settings = await runtime.store.get_settings(query.from_user.id)
        text, total_pages = render_analysis_text(analysis, page, settings["preview_enabled"])
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=preview_keyboard(max(0, min(page, total_pages - 1)), total_pages),
            disable_web_page_preview=True,
        )
        return

    if data.startswith("action:"):
        action = data.split(":", 1)[1]
        analysis = runtime.sessions.get(query.from_user.id)
        if not analysis:
            await query.edit_message_text("❌ This preview session expired. Send the URL again.", reply_markup=main_menu_keyboard())
            return
        if runtime.queue.is_busy(query.from_user.id):
            await query.answer("You already have a queued or active job.", show_alert=True)
            return

        job = ExportJob(
            job_id=hashlib.sha1(f"{query.from_user.id}:{action}:{now_ts()}".encode()).hexdigest()[:12],
            user_id=query.from_user.id,
            action=action,
            analysis=analysis,
            chat_id=query.message.chat.id,
            message_id=query.message.message_id,
        )
        position = await runtime.queue.enqueue(job)
        await query.edit_message_text(
            "🚀 <b>Starting download</b>\n\n"
            f"📄 <b>{html.escape(analysis.title)}</b>\n"
            f"📦 Requested format: {html.escape(action_label(action))}\n"
            f"🧵 Queue position: <b>{position}</b>",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        return

    if data in {"menu:download", "menu:extract", "menu:snapshot"}:
        await query.answer("Send the URL in chat after selecting this option.", show_alert=False)
        return


# ============================================================================
# 7. ADMIN COMMANDS
# ============================================================================


async def cmd_add_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_owner(update):
        await safe_reply_text(update.effective_message, "❌ Owner-only command.")
        return
    runtime = get_runtime(context)
    if not context.args:
        await safe_reply_text(update.effective_message, "Usage: /add_user [telegram_id]")
        return

    lines: list[str] = []
    for value in context.args:
        try:
            user_id = int(value)
        except ValueError:
            lines.append(f"❌ Invalid ID: <code>{html.escape(value)}</code>")
            continue
        added = await runtime.store.add_user(user_id)
        lines.append(f"{'✅ Added' if added else 'ℹ️ Already allowed'} <code>{user_id}</code>")

    await update.effective_message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_remove_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_owner(update):
        await safe_reply_text(update.effective_message, "❌ Owner-only command.")
        return
    runtime = get_runtime(context)
    if not context.args:
        await safe_reply_text(update.effective_message, "Usage: /remove_user [telegram_id]")
        return

    lines: list[str] = []
    for value in context.args:
        try:
            user_id = int(value)
        except ValueError:
            lines.append(f"❌ Invalid ID: <code>{html.escape(value)}</code>")
            continue
        removed = await runtime.store.remove_user(user_id)
        lines.append(f"{'✅ Removed' if removed else '❌ Not removable'} <code>{user_id}</code>")

    await update.effective_message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_owner(update):
        await safe_reply_text(update.effective_message, "❌ Owner-only command.")
        return
    runtime = get_runtime(context)
    users = await runtime.store.list_users()
    lines = [f"👥 <b>Authorized users ({len(users)})</b>", ""]
    for user_id in users:
        badge = "👑" if user_id == OWNER_ID else "👤"
        lines.append(f"{badge} <code>{user_id}</code>")
    await update.effective_message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_owner(update):
        await safe_reply_text(update.effective_message, "❌ Owner-only command.")
        return
    runtime = get_runtime(context)
    if not context.args:
        await safe_reply_text(update.effective_message, "Usage: /broadcast [message]")
        return

    message = " ".join(context.args)
    sent = 0
    failed = 0
    for user_id in await runtime.store.list_users():
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=f"📢 <b>Broadcast</b>\n\n{html.escape(message)}",
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            sent += 1
            await asyncio.sleep(0.05)
        except (Forbidden, BadRequest):
            failed += 1
        except Exception:
            logger.exception("Broadcast send failed for user=%s", user_id)
            failed += 1

    await update.effective_message.reply_text(f"📢 Broadcast complete — ✅ {sent} sent, ❌ {failed} failed.")


# ============================================================================
# 8. MAIN ASYNC RUNNER
# ============================================================================


async def post_init(application: Application) -> None:
    runtime = BotRuntime()
    await runtime.startup(application)
    application.bot_data["runtime"] = runtime


async def post_shutdown(application: Application) -> None:
    runtime = application.bot_data.get("runtime")
    if isinstance(runtime, BotRuntime):
        await runtime.shutdown()


def build_application() -> Application:
    if BOT_TOKEN in {"", "REPLACE_WITH_YOUR_BOT_TOKEN"}:
        raise RuntimeError("BOT_TOKEN is not configured. Set the BOT_TOKEN environment variable.")
    if OWNER_ID == 123456789:
        logger.warning("OWNER_ID is still the placeholder value. Replace it before deploying.")

    url_conversation = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(prompt_for_url, pattern=r"^menu:(download|extract|snapshot)$"),
        ],
        states={
            WAITING_FOR_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, analyze_message_link)],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
        name="url_prompt_conversation",
        persistent=False,
    )

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("add_user", cmd_add_user))
    app.add_handler(CommandHandler("remove_user", cmd_remove_user))
    app.add_handler(CommandHandler("users", cmd_users))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))

    app.add_handler(url_conversation)
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_direct_text))
    return app


def main() -> None:
    logger.info("Starting Universal Document Downloader bot (owner=%s)", OWNER_ID)
    application = build_application()
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
        close_loop=False,
    )


if __name__ == "__main__":
    main()

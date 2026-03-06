#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Universal Document Downloader & Viewer Telegram Bot
==================================================

Dependencies (Python 3.11+):
  pip install "python-telegram-bot[ext]>=20.0" aiohttp beautifulsoup4 Pillow lxml

This bot is designed for Termux and Ubuntu VPS deployments and focuses on:
  - asyncio + aiohttp networking
  - resilient parsing/downloading pipeline
  - low-memory streaming downloads
  - inline keyboard UX with pagination
  - admin access control + broadcast
  - caching, anti-spam, queue workers, and persistent history/stats
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import json
import logging
import os
import re
import shutil
import tempfile
import time
import zipfile
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urljoin, urlparse, urlsplit

import aiohttp
from bs4 import BeautifulSoup
from PIL import Image
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ChatAction, ParseMode
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

# =============================================================================
# 1) CONFIGURATION
# =============================================================================

# Hardcoded owner id (can still be overridden by env for convenience).
OWNER_ID = int(os.getenv("OWNER_ID", "6512242172"))
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

DATA_DIR = Path(os.getenv("BOT_DATA_DIR", "bot_data"))
STATE_FILE = DATA_DIR / "state.json"
LOG_FILE = DATA_DIR / "bot.log"

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=180, connect=25, sock_read=120)
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

DOWNLOAD_CHUNK_SIZE = 256 * 1024
MAX_HTML_BYTES = 8 * 1024 * 1024
MAX_TEXT_CHARS = 350_000
MAX_ASSETS_PER_PAGE = 8
MAX_TELEGRAM_UPLOAD_BYTES = 49 * 1024 * 1024
MAX_HISTORY_PER_USER = 200

CACHE_TTL_SECONDS = 15 * 60
CACHE_MAX_ITEMS = 200

SPAM_WINDOW_SECONDS = 60
SPAM_MAX_REQUESTS = 8

DOWNLOAD_WORKERS = 3
PER_USER_CONCURRENCY = 1
GLOBAL_CONCURRENCY = 5

STATE_WAITING_URL = 1

URL_RE = re.compile(r"https?://[^\s<>()]+", re.IGNORECASE)
EXT_PDF_RE = re.compile(r"\.pdf(?:$|\?)", re.IGNORECASE)
EXT_IMAGE_RE = re.compile(r"\.(?:jpg|jpeg|png|webp|gif|bmp|tiff)(?:$|\?)", re.IGNORECASE)
EXT_TXT_RE = re.compile(r"\.txt(?:$|\?)", re.IGNORECASE)
EXT_DOCX_RE = re.compile(r"\.docx?(?:$|\?)", re.IGNORECASE)
EXT_HTML_RE = re.compile(r"\.(?:html?|xhtml)(?:$|\?)", re.IGNORECASE)
FILENAME_SANITIZE_RE = re.compile(r"[^\w.\- ]+")


# =============================================================================
# 2) HELPER UTILITIES
# =============================================================================

def setup_logging() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    handlers = [logging.StreamHandler()]
    try:
        handlers.append(logging.FileHandler(LOG_FILE, encoding="utf-8"))
    except OSError:
        pass
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=handlers,
    )


logger = logging.getLogger("UniversalDocBot")


def now_ts() -> int:
    return int(time.time())


def extract_first_url(text: str) -> str | None:
    m = URL_RE.search(text or "")
    return m.group(0) if m else None


def is_valid_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def normalize_url(url: str) -> str:
    return url.strip()


def sha_short(value: str, length: int = 12) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:length]


def sanitize_filename(name: str, fallback: str = "document") -> str:
    cleaned = FILENAME_SANITIZE_RE.sub("", name or "").strip().replace("\n", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .")
    if not cleaned:
        return fallback
    return cleaned[:80]


def dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def human_bytes(num: int) -> str:
    if num < 1024:
        return f"{num} B"
    if num < 1024**2:
        return f"{num / 1024:.1f} KB"
    if num < 1024**3:
        return f"{num / 1024**2:.1f} MB"
    return f"{num / 1024**3:.2f} GB"


async def retry(
    operation: Callable[[], Any],
    retries: int = 3,
    base_delay: float = 1.0,
    exc_types: tuple[type[BaseException], ...] = (Exception,),
) -> Any:
    last_exc: BaseException | None = None
    for attempt in range(retries):
        try:
            return await operation()
        except exc_types as exc:  # type: ignore[misc]
            last_exc = exc
            if attempt == retries - 1:
                break
            await asyncio.sleep(base_delay * (2**attempt))
    if last_exc:
        raise last_exc
    raise RuntimeError("retry() ended without result or exception")


def escape_html(text: str) -> str:
    return html.escape(text or "")


class StatusReporter:
    """Safely edits a Telegram status message with lightweight throttling."""

    def __init__(self, bot, chat_id: int, message_id: int) -> None:
        self.bot = bot
        self.chat_id = chat_id
        self.message_id = message_id
        self._last_text: str = ""
        self._last_ts = 0.0

    async def edit(
        self,
        text: str,
        *,
        reply_markup: InlineKeyboardMarkup | None = None,
        force: bool = False,
    ) -> None:
        now = time.monotonic()
        if not force and text == self._last_text:
            return
        if not force and (now - self._last_ts) < 0.9:
            return
        try:
            await self.bot.edit_message_text(
                chat_id=self.chat_id,
                message_id=self.message_id,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=reply_markup,
            )
            self._last_text = text
            self._last_ts = now
        except BadRequest as exc:
            if "message is not modified" not in str(exc).lower():
                logger.debug("Status edit ignored: %s", exc)
        except Exception as exc:
            logger.debug("Status edit failed: %s", exc)


# =============================================================================
# 3) PERSISTENT STATE, CACHE, SPAM GUARD
# =============================================================================

class PersistentState:
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
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            await self.save()
            return
        try:
            raw = await asyncio.to_thread(self.path.read_text, "utf-8")
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                self.data.update(parsed)
        except Exception as exc:
            logger.warning("State load failed, using defaults: %s", exc)
        # owner is always authorized
        users = set(int(x) for x in self.data.get("users", []))
        users.add(OWNER_ID)
        self.data["users"] = sorted(users)
        await self.save()

    async def save(self) -> None:
        payload = json.dumps(self.data, ensure_ascii=False, indent=2)
        await asyncio.to_thread(self.path.write_text, payload, "utf-8")

    async def is_authorized(self, user_id: int) -> bool:
        async with self.lock:
            return user_id in set(self.data.get("users", []))

    async def add_user(self, user_id: int) -> bool:
        async with self.lock:
            users = set(int(x) for x in self.data.get("users", []))
            if user_id in users:
                return False
            users.add(user_id)
            self.data["users"] = sorted(users)
            await self.save()
            return True

    async def remove_user(self, user_id: int) -> bool:
        if user_id == OWNER_ID:
            return False
        async with self.lock:
            users = set(int(x) for x in self.data.get("users", []))
            if user_id not in users:
                return False
            users.remove(user_id)
            self.data["users"] = sorted(users)
            await self.save()
            return True

    async def list_users(self) -> list[int]:
        async with self.lock:
            return sorted(int(x) for x in self.data.get("users", []))

    async def get_user_settings(self, user_id: int) -> dict[str, Any]:
        key = str(user_id)
        async with self.lock:
            settings = self.data.setdefault("settings", {}).setdefault(
                key, {"link_preview": True}
            )
            return dict(settings)

    async def toggle_link_preview(self, user_id: int) -> bool:
        key = str(user_id)
        async with self.lock:
            user_settings = self.data.setdefault("settings", {}).setdefault(
                key, {"link_preview": True}
            )
            user_settings["link_preview"] = not bool(user_settings.get("link_preview", True))
            await self.save()
            return bool(user_settings["link_preview"])

    async def add_history(self, user_id: int, entry: dict[str, Any]) -> None:
        key = str(user_id)
        async with self.lock:
            history = self.data.setdefault("history", {}).setdefault(key, [])
            history.append(entry)
            if len(history) > MAX_HISTORY_PER_USER:
                del history[: len(history) - MAX_HISTORY_PER_USER]
            await self.save()

    async def get_history(self, user_id: int) -> list[dict[str, Any]]:
        key = str(user_id)
        async with self.lock:
            return list(self.data.setdefault("history", {}).get(key, []))

    async def bump_stats(
        self,
        user_id: int,
        *,
        requests: int = 0,
        completed: int = 0,
        failed: int = 0,
        bytes_sent: int = 0,
    ) -> dict[str, int]:
        key = str(user_id)
        async with self.lock:
            stats = self.data.setdefault("stats", {}).setdefault(
                key,
                {"requests": 0, "completed": 0, "failed": 0, "bytes_sent": 0},
            )
            stats["requests"] += requests
            stats["completed"] += completed
            stats["failed"] += failed
            stats["bytes_sent"] += bytes_sent
            await self.save()
            return dict(stats)

    async def get_stats(self, user_id: int) -> dict[str, int]:
        key = str(user_id)
        async with self.lock:
            stats = self.data.setdefault("stats", {}).setdefault(
                key,
                {"requests": 0, "completed": 0, "failed": 0, "bytes_sent": 0},
            )
            return dict(stats)


class TTLCache:
    def __init__(self, ttl_seconds: int, max_items: int) -> None:
        self.ttl = ttl_seconds
        self.max_items = max_items
        self.lock = asyncio.Lock()
        self.store: dict[str, tuple[float, Any]] = {}

    async def get(self, key: str) -> Any | None:
        async with self.lock:
            item = self.store.get(key)
            if not item:
                return None
            exp, value = item
            if exp < time.time():
                self.store.pop(key, None)
                return None
            return value

    async def set(self, key: str, value: Any) -> None:
        async with self.lock:
            if len(self.store) >= self.max_items:
                # Drop oldest expiration first.
                oldest_key = min(self.store.items(), key=lambda kv: kv[1][0])[0]
                self.store.pop(oldest_key, None)
            self.store[key] = (time.time() + self.ttl, value)

    async def cleanup(self) -> None:
        async with self.lock:
            now = time.time()
            stale = [k for k, (exp, _) in self.store.items() if exp < now]
            for k in stale:
                self.store.pop(k, None)


class SpamGuard:
    def __init__(self, max_requests: int, window_seconds: int) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.events: dict[int, deque[float]] = defaultdict(deque)

    def check(self, user_id: int) -> tuple[bool, int]:
        now = time.monotonic()
        q = self.events[user_id]
        while q and (now - q[0]) > self.window_seconds:
            q.popleft()
        if len(q) >= self.max_requests:
            wait_for = int(self.window_seconds - (now - q[0])) + 1
            return False, max(wait_for, 1)
        q.append(now)
        return True, 0


# =============================================================================
# 4) DOWNLOADER ENGINE
# =============================================================================

class DownloaderEngine:
    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session
        self.global_sem = asyncio.Semaphore(GLOBAL_CONCURRENCY)

    async def _request_get(self, url: str):
        return self.session.get(
            url,
            allow_redirects=True,
            headers=DEFAULT_HEADERS,
            timeout=REQUEST_TIMEOUT,
        )

    async def stream_download(
        self,
        url: str,
        destination: Path,
        progress_cb: Callable[[int, int | None], Any] | None = None,
    ) -> tuple[Path, str, int]:
        """
        Streams a URL to file with retries.
        Returns: (path, content_type, bytes_written)
        """

        async def _op() -> tuple[Path, str, int]:
            async with self.global_sem:
                async with await self._request_get(url) as resp:
                    resp.raise_for_status()
                    content_type = resp.headers.get("Content-Type", "").lower()
                    total_expected = (
                        int(resp.headers.get("Content-Length", "0"))
                        if resp.headers.get("Content-Length", "").isdigit()
                        else None
                    )
                    written = 0
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    with destination.open("wb") as f:
                        async for chunk in resp.content.iter_chunked(DOWNLOAD_CHUNK_SIZE):
                            if not chunk:
                                continue
                            written += len(chunk)
                            await asyncio.to_thread(f.write, chunk)
                            if progress_cb:
                                await progress_cb(written, total_expected)
                    return destination, content_type, written

        return await retry(_op, retries=3, base_delay=1.2, exc_types=(aiohttp.ClientError, asyncio.TimeoutError))

    async def download_many(
        self,
        urls: list[str],
        target_dir: Path,
        *,
        base_filename: str = "asset",
        progress_cb: Callable[[int, int], Any] | None = None,
    ) -> list[Path]:
        out: list[tuple[int, Path]] = []
        sem = asyncio.Semaphore(6)
        done_count = 0
        done_lock = asyncio.Lock()

        async def _worker(idx: int, asset_url: str) -> None:
            nonlocal done_count
            ext = self._guess_ext_from_url(asset_url)
            name = f"{base_filename}_{idx + 1:04d}{ext}"
            path = target_dir / name
            async with sem:
                try:
                    await self.stream_download(asset_url, path)
                    out.append((idx, path))
                except Exception as exc:
                    logger.debug("Asset download failed [%s]: %s", asset_url, exc)
                finally:
                    async with done_lock:
                        done_count += 1
                        if progress_cb:
                            await progress_cb(done_count, len(urls))

        await asyncio.gather(*(_worker(i, u) for i, u in enumerate(urls)))
        out.sort(key=lambda x: x[0])
        return [p for _, p in out]

    @staticmethod
    def _guess_ext_from_url(url: str) -> str:
        path = urlsplit(url).path.lower()
        if path.endswith((".jpg", ".jpeg")):
            return ".jpg"
        if path.endswith(".png"):
            return ".png"
        if path.endswith(".webp"):
            return ".webp"
        if path.endswith(".gif"):
            return ".gif"
        if path.endswith(".bmp"):
            return ".bmp"
        if path.endswith(".tiff"):
            return ".tiff"
        if path.endswith(".pdf"):
            return ".pdf"
        if path.endswith(".txt"):
            return ".txt"
        if path.endswith(".docx"):
            return ".docx"
        return ".bin"

    async def create_zip(self, files: list[Path], out_zip: Path) -> Path:
        def _zip_it() -> Path:
            with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
                for file_path in files:
                    zf.write(file_path, arcname=file_path.name)
            return out_zip

        return await asyncio.to_thread(_zip_it)

    async def images_to_pdf(self, images: list[Path], out_pdf: Path) -> Path:
        def _build() -> Path:
            pil_images: list[Image.Image] = []
            try:
                for img_path in images:
                    try:
                        img = Image.open(img_path)
                        if img.mode != "RGB":
                            img = img.convert("RGB")
                        pil_images.append(img.copy())
                        img.close()
                    except Exception:
                        continue
                if not pil_images:
                    raise ValueError("No valid image files to convert")
                first, *rest = pil_images
                first.save(out_pdf, save_all=True, append_images=rest, resolution=150)
                return out_pdf
            finally:
                for im in pil_images:
                    try:
                        im.close()
                    except Exception:
                        pass

        return await asyncio.to_thread(_build)

    async def write_text(self, content: str, out_file: Path) -> Path:
        await asyncio.to_thread(out_file.write_text, content, "utf-8")
        return out_file


# =============================================================================
# 5) PARSER ENGINE
# =============================================================================

@dataclass
class LinkAnalysis:
    source_url: str
    final_url: str
    title: str
    content_type: str
    status_code: int
    html_snapshot: str
    extracted_text: str
    pdf_links: list[str]
    image_links: list[str]
    doc_links: list[str]
    embedded_links: list[str]
    direct_file_url: str | None

    def all_assets(self) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        out.extend([("PDF", u) for u in self.pdf_links])
        out.extend([("IMG", u) for u in self.image_links])
        out.extend([("DOC", u) for u in self.doc_links])
        out.extend([("EMBED", u) for u in self.embedded_links])
        return out


class ParserEngine:
    def __init__(self, session: aiohttp.ClientSession, cache: TTLCache) -> None:
        self.session = session
        self.cache = cache

    async def analyze(self, url: str) -> LinkAnalysis:
        normalized = normalize_url(url)
        key = f"analysis:{sha_short(normalized, 32)}"
        cached = await self.cache.get(key)
        if cached:
            logger.info("Cache hit: %s", normalized)
            return cached

        analysis = await retry(lambda: self._analyze_uncached(normalized), retries=3, base_delay=1.0)
        await self.cache.set(key, analysis)
        return analysis

    async def _analyze_uncached(self, url: str) -> LinkAnalysis:
        async with self.session.get(
            url,
            allow_redirects=True,
            headers=DEFAULT_HEADERS,
            timeout=REQUEST_TIMEOUT,
        ) as resp:
            resp.raise_for_status()
            final_url = str(resp.url)
            content_type = (resp.headers.get("Content-Type") or "").lower()
            status_code = resp.status

            # Direct files (pdf/txt/docx/images) do not need full body parsing.
            if self._is_direct_file(final_url, content_type):
                title = sanitize_filename(Path(urlsplit(final_url).path).name, "document")
                pdf_links = [final_url] if self._is_pdf(final_url, content_type) else []
                doc_links = [final_url] if self._is_doc(final_url, content_type) else []
                image_links = [final_url] if self._is_image(final_url, content_type) else []
                extracted_text = ""
                if self._is_txt(final_url, content_type):
                    # Small text files can be read directly for extract-text feature.
                    body = await self._read_limited_bytes(resp, limit_bytes=2 * 1024 * 1024)
                    extracted_text = body.decode("utf-8", errors="ignore")[:MAX_TEXT_CHARS]
                return LinkAnalysis(
                    source_url=url,
                    final_url=final_url,
                    title=title or "Document",
                    content_type=content_type,
                    status_code=status_code,
                    html_snapshot="",
                    extracted_text=extracted_text,
                    pdf_links=pdf_links,
                    image_links=image_links,
                    doc_links=doc_links,
                    embedded_links=[],
                    direct_file_url=final_url,
                )

            # Parse HTML-like content.
            body = await self._read_limited_bytes(resp, limit_bytes=MAX_HTML_BYTES)
            html_snapshot = body.decode("utf-8", errors="ignore")
            soup = self._build_soup(html_snapshot)

            title = self._extract_title(soup, final_url)
            text = self._extract_text(soup)
            pdf_links, image_links, doc_links, embedded_links = self._extract_assets(soup, final_url)

            analysis = LinkAnalysis(
                source_url=url,
                final_url=final_url,
                title=title,
                content_type=content_type or "text/html",
                status_code=status_code,
                html_snapshot=html_snapshot,
                extracted_text=text,
                pdf_links=pdf_links,
                image_links=image_links,
                doc_links=doc_links,
                embedded_links=embedded_links,
                direct_file_url=None,
            )
            return analysis

    @staticmethod
    async def _read_limited_bytes(resp: aiohttp.ClientResponse, limit_bytes: int) -> bytes:
        chunks: list[bytes] = []
        total = 0
        async for chunk in resp.content.iter_chunked(DOWNLOAD_CHUNK_SIZE):
            if not chunk:
                continue
            total += len(chunk)
            if total > limit_bytes:
                # hard limit to avoid memory spikes on huge pages
                remain = limit_bytes - (total - len(chunk))
                if remain > 0:
                    chunks.append(chunk[:remain])
                break
            chunks.append(chunk)
        return b"".join(chunks)

    @staticmethod
    def _build_soup(raw_html: str) -> BeautifulSoup:
        try:
            return BeautifulSoup(raw_html, "lxml")
        except Exception:
            return BeautifulSoup(raw_html, "html.parser")

    @staticmethod
    def _extract_title(soup: BeautifulSoup, fallback_url: str) -> str:
        meta = soup.find("meta", property="og:title")
        if meta and meta.get("content"):
            return sanitize_filename(meta["content"], "Document")
        if soup.title and soup.title.string:
            return sanitize_filename(soup.title.string, "Document")
        path_name = Path(urlsplit(fallback_url).path).name
        return sanitize_filename(path_name or "Document", "Document")

    @staticmethod
    def _extract_text(soup: BeautifulSoup) -> str:
        for tag in soup(["script", "style", "noscript", "svg", "canvas"]):
            tag.extract()
        candidates: list[str] = []
        for sel in ("article", "main", "section"):
            el = soup.select_one(sel)
            if el:
                txt = el.get_text("\n", strip=True)
                if len(txt) > 120:
                    candidates.append(txt)
        if not candidates:
            txt = soup.get_text("\n", strip=True)
            candidates.append(txt)
        text = "\n".join(candidates)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        return text[:MAX_TEXT_CHARS]

    def _extract_assets(
        self, soup: BeautifulSoup, base_url: str
    ) -> tuple[list[str], list[str], list[str], list[str]]:
        pdf_links: list[str] = []
        image_links: list[str] = []
        doc_links: list[str] = []
        embedded_links: list[str] = []

        def _normalize(u: str) -> str:
            return urljoin(base_url, u.strip())

        # Links from href-bearing tags.
        for tag in soup.select("a[href], link[href]"):
            href = tag.get("href")
            if not href:
                continue
            abs_url = _normalize(href)
            if self._is_pdf(abs_url, ""):
                pdf_links.append(abs_url)
            elif self._is_doc(abs_url, ""):
                doc_links.append(abs_url)
            elif self._is_image(abs_url, ""):
                image_links.append(abs_url)

        # Image references.
        for tag in soup.select("img[src], source[src], image[src]"):
            src = tag.get("src")
            if src:
                image_links.append(_normalize(src))
            srcset = tag.get("srcset")
            if srcset:
                for token in srcset.split(","):
                    part = token.strip().split(" ")[0]
                    if part:
                        image_links.append(_normalize(part))

        # Embedded document references.
        for tag in soup.select("iframe[src], embed[src], object[data]"):
            u = tag.get("src") or tag.get("data")
            if not u:
                continue
            abs_url = _normalize(u)
            embedded_links.append(abs_url)
            if self._is_pdf(abs_url, ""):
                pdf_links.append(abs_url)
            elif self._is_doc(abs_url, ""):
                doc_links.append(abs_url)

        # Heuristic: check data-* attributes for hidden links.
        for tag in soup.find_all(True):
            for key, val in tag.attrs.items():
                if not isinstance(val, str):
                    continue
                if "http" not in val:
                    continue
                if key.lower().startswith("data-") and is_valid_url(val):
                    u = val.strip()
                    if self._is_pdf(u, ""):
                        pdf_links.append(u)
                    elif self._is_image(u, ""):
                        image_links.append(u)
                    elif self._is_doc(u, ""):
                        doc_links.append(u)

        pdf_links = dedupe_keep_order(pdf_links)[:500]
        image_links = dedupe_keep_order(image_links)[:1500]
        doc_links = dedupe_keep_order(doc_links)[:500]
        embedded_links = dedupe_keep_order(embedded_links)[:500]
        return pdf_links, image_links, doc_links, embedded_links

    @staticmethod
    def _is_pdf(url: str, content_type: str) -> bool:
        return "application/pdf" in content_type or bool(EXT_PDF_RE.search(url))

    @staticmethod
    def _is_image(url: str, content_type: str) -> bool:
        return content_type.startswith("image/") or bool(EXT_IMAGE_RE.search(url))

    @staticmethod
    def _is_txt(url: str, content_type: str) -> bool:
        return "text/plain" in content_type or bool(EXT_TXT_RE.search(url))

    @staticmethod
    def _is_doc(url: str, content_type: str) -> bool:
        if EXT_DOCX_RE.search(url):
            return True
        return (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            in content_type
        ) or ("application/msword" in content_type)

    def _is_direct_file(self, url: str, content_type: str) -> bool:
        return (
            self._is_pdf(url, content_type)
            or self._is_image(url, content_type)
            or self._is_txt(url, content_type)
            or self._is_doc(url, content_type)
        )


# =============================================================================
# 6) TELEGRAM HANDLERS + QUEUE RUNTIME
# =============================================================================

@dataclass
class Job:
    user_id: int
    chat_id: int
    status_message_id: int
    context_id: str
    action: str  # pdf | img | txt | html
    requested_url: str


class BotRuntime:
    def __init__(self) -> None:
        self.state = PersistentState(STATE_FILE)
        self.cache = TTLCache(CACHE_TTL_SECONDS, CACHE_MAX_ITEMS)
        self.spam_guard = SpamGuard(SPAM_MAX_REQUESTS, SPAM_WINDOW_SECONDS)

        self.session: aiohttp.ClientSession | None = None
        self.downloader: DownloaderEngine | None = None
        self.parser: ParserEngine | None = None

        self.queue: asyncio.Queue[Job | None] = asyncio.Queue()
        self.workers: list[asyncio.Task] = []
        self.cleanup_task: asyncio.Task | None = None
        self.user_locks: dict[int, asyncio.Semaphore] = defaultdict(
            lambda: asyncio.Semaphore(PER_USER_CONCURRENCY)
        )

        # context_id -> context payload
        self.contexts: dict[str, dict[str, Any]] = {}
        self.context_lock = asyncio.Lock()

    async def start(self, app: Application) -> None:
        await self.state.load()
        connector = aiohttp.TCPConnector(limit=120, ttl_dns_cache=300)
        self.session = aiohttp.ClientSession(
            connector=connector, timeout=REQUEST_TIMEOUT, raise_for_status=False
        )
        self.downloader = DownloaderEngine(self.session)
        self.parser = ParserEngine(self.session, self.cache)

        self.workers = [
            asyncio.create_task(self._worker_loop(app), name=f"dl-worker-{i+1}")
            for i in range(DOWNLOAD_WORKERS)
        ]
        self.cleanup_task = asyncio.create_task(self._cleanup_loop(), name="runtime-cleanup")
        logger.info("Runtime started with %d workers", DOWNLOAD_WORKERS)

    async def close(self) -> None:
        for _ in self.workers:
            await self.queue.put(None)
        if self.workers:
            await asyncio.gather(*self.workers, return_exceptions=True)
        if self.cleanup_task:
            self.cleanup_task.cancel()
            with contextlib_suppress(asyncio.CancelledError):
                await self.cleanup_task
        await self.cache.cleanup()
        if self.session:
            await self.session.close()
        logger.info("Runtime closed")

    async def add_context(self, user_id: int, analysis: LinkAnalysis) -> str:
        context_id = sha_short(f"{user_id}:{analysis.final_url}:{time.time_ns()}", 14)
        payload = {
            "user_id": user_id,
            "analysis": analysis,
            "created_at": now_ts(),
            "url": analysis.source_url,
        }
        async with self.context_lock:
            self.contexts[context_id] = payload
        return context_id

    async def get_context(self, context_id: str) -> dict[str, Any] | None:
        async with self.context_lock:
            return self.contexts.get(context_id)

    async def enqueue(self, job: Job) -> int:
        await self.queue.put(job)
        return self.queue.qsize()

    async def _cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(90)
            try:
                await self.cache.cleanup()
                cutoff = now_ts() - (60 * 45)  # 45 minutes
                async with self.context_lock:
                    stale = [
                        key
                        for key, payload in self.contexts.items()
                        if payload.get("created_at", 0) < cutoff
                    ]
                    for key in stale:
                        self.contexts.pop(key, None)
                # Cleanup stale generated output files.
                tmp_out = DATA_DIR / "tmp_out"
                if tmp_out.exists():
                    old_cutoff = time.time() - (60 * 60)  # 1 hour
                    for file_path in tmp_out.iterdir():
                        if not file_path.is_file():
                            continue
                        try:
                            if file_path.stat().st_mtime < old_cutoff:
                                file_path.unlink(missing_ok=True)
                        except OSError:
                            continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("Cleanup loop issue: %s", exc)

    async def _worker_loop(self, app: Application) -> None:
        while True:
            item = await self.queue.get()
            if item is None:
                self.queue.task_done()
                return
            try:
                await self._process_job(app, item)
            except Exception as exc:
                logger.exception("Worker crashed on job: %s", exc)
            finally:
                self.queue.task_done()

    async def _process_job(self, app: Application, job: Job) -> None:
        bot = app.bot
        reporter = StatusReporter(bot, job.chat_id, job.status_message_id)
        lock = self.user_locks[job.user_id]

        ctx = await self.get_context(job.context_id)
        if not ctx:
            await reporter.edit(
                "❌ <b>Error</b>\n\nSession expired. Send the URL again.",
                force=True,
            )
            await self.state.bump_stats(job.user_id, failed=1)
            return

        analysis: LinkAnalysis = ctx["analysis"]

        async with lock:
            await reporter.edit("🚀 <b>Starting download</b>\nPreparing job…", force=True)
            await self.state.bump_stats(job.user_id, requests=1)

            try:
                out_file: Path | None = None
                out_name = ""
                if job.action == "pdf":
                    out_file, out_name = await self._build_pdf(job, analysis, reporter)
                elif job.action == "img":
                    out_file, out_name = await self._build_images_zip(job, analysis, reporter)
                elif job.action == "txt":
                    out_file, out_name = await self._build_text(job, analysis, reporter)
                elif job.action == "html":
                    out_file, out_name = await self._build_html(job, analysis, reporter)
                else:
                    raise ValueError("Unsupported action")

                size = out_file.stat().st_size
                if size > MAX_TELEGRAM_UPLOAD_BYTES:
                    raise ValueError(
                        f"Output file too large for Telegram upload ({human_bytes(size)})."
                    )

                await reporter.edit(
                    f"📤 <b>Uploading to Telegram</b>\n{escape_html(out_name)} ({human_bytes(size)})",
                    force=True,
                )
                await bot.send_chat_action(job.chat_id, ChatAction.UPLOAD_DOCUMENT)
                with out_file.open("rb") as f:
                    await bot.send_document(
                        chat_id=job.chat_id,
                        document=f,
                        filename=out_name,
                        caption=(
                            f"✅ <b>{escape_html(analysis.title)}</b>\n"
                            f"Format: <code>{job.action.upper()}</code>\n"
                            f"Size: <code>{human_bytes(size)}</code>"
                        ),
                        parse_mode=ParseMode.HTML,
                    )

                await reporter.edit("✅ <b>Completed</b>\nFile uploaded successfully.", force=True)
                await self.state.add_history(
                    job.user_id,
                    {
                        "ts": now_ts(),
                        "url": job.requested_url,
                        "title": analysis.title,
                        "action": job.action,
                        "file": out_name,
                        "bytes": size,
                    },
                )
                await self.state.bump_stats(job.user_id, completed=1, bytes_sent=size)
            except Exception as exc:
                logger.warning("Job failed (%s): %s", job.action, exc)
                await self.state.bump_stats(job.user_id, failed=1)
                await reporter.edit(
                    f"❌ <b>Error</b>\n\n<code>{escape_html(str(exc)[:500])}</code>",
                    force=True,
                )
            finally:
                if "out_file" in locals() and out_file is not None:
                    try:
                        out_file.unlink(missing_ok=True)
                    except OSError:
                        pass

    async def _build_pdf(
        self, job: Job, analysis: LinkAnalysis, reporter: StatusReporter
    ) -> tuple[Path, str]:
        assert self.downloader is not None
        await reporter.edit("⚡ <b>Processing link</b>\nPreparing PDF source…", force=True)
        safe_name = sanitize_filename(analysis.title, "document")

        with tempfile.TemporaryDirectory(prefix="udb_pdf_") as tmp:
            tmp_dir = Path(tmp)
            out_pdf = tmp_dir / f"{safe_name}.pdf"

            # 1) Direct / discovered PDF
            direct_pdf = None
            if analysis.direct_file_url and EXT_PDF_RE.search(analysis.direct_file_url):
                direct_pdf = analysis.direct_file_url
            elif analysis.pdf_links:
                direct_pdf = analysis.pdf_links[0]

            if direct_pdf:
                await reporter.edit("📥 <b>Downloading assets</b>\nDownloading PDF…", force=True)
                await self.downloader.stream_download(direct_pdf, out_pdf)
                final_copy = DATA_DIR / "tmp_out" / out_pdf.name
                final_copy.parent.mkdir(parents=True, exist_ok=True)
                await asyncio.to_thread(shutil.copyfile, out_pdf, final_copy)
                return final_copy, out_pdf.name

            # 2) Build PDF from image sequence
            images = analysis.image_links[:300]
            if not images:
                raise ValueError(
                    "No PDF source found. This page has no PDF link or image sequence."
                )

            await reporter.edit(
                f"📥 <b>Downloading assets</b>\nImages: 0/{len(images)}", force=True
            )

            async def _img_progress(done: int, total: int) -> None:
                await reporter.edit(
                    f"📥 <b>Downloading assets</b>\nImages: {done}/{total}"
                )

            img_dir = tmp_dir / "images"
            downloaded = await self.downloader.download_many(
                images, img_dir, base_filename="page", progress_cb=_img_progress
            )
            if not downloaded:
                raise ValueError("Failed to download image assets from this URL.")

            await reporter.edit("📦 <b>Packaging files</b>\nConverting images to PDF…", force=True)
            await self.downloader.images_to_pdf(downloaded, out_pdf)
            final_copy = DATA_DIR / "tmp_out" / out_pdf.name
            final_copy.parent.mkdir(parents=True, exist_ok=True)
            await asyncio.to_thread(shutil.copyfile, out_pdf, final_copy)
            return final_copy, out_pdf.name

    async def _build_images_zip(
        self, job: Job, analysis: LinkAnalysis, reporter: StatusReporter
    ) -> tuple[Path, str]:
        assert self.downloader is not None
        await reporter.edit("⚡ <b>Processing link</b>\nPreparing image archive…", force=True)
        images = analysis.image_links[:600]
        if not images:
            raise ValueError("No image assets found on this URL.")

        safe_name = sanitize_filename(analysis.title, "document")
        with tempfile.TemporaryDirectory(prefix="udb_img_") as tmp:
            tmp_dir = Path(tmp)
            img_dir = tmp_dir / "images"
            out_zip = tmp_dir / f"{safe_name}_images.zip"

            await reporter.edit(
                f"📥 <b>Downloading assets</b>\nImages: 0/{len(images)}", force=True
            )

            async def _img_progress(done: int, total: int) -> None:
                await reporter.edit(
                    f"📥 <b>Downloading assets</b>\nImages: {done}/{total}"
                )

            downloaded = await self.downloader.download_many(
                images, img_dir, base_filename="image", progress_cb=_img_progress
            )
            if not downloaded:
                raise ValueError("Failed to download image assets.")

            await reporter.edit("📦 <b>Packaging files</b>\nCreating ZIP archive…", force=True)
            await self.downloader.create_zip(downloaded, out_zip)
            final_copy = DATA_DIR / "tmp_out" / out_zip.name
            final_copy.parent.mkdir(parents=True, exist_ok=True)
            await asyncio.to_thread(shutil.copyfile, out_zip, final_copy)
            return final_copy, out_zip.name

    async def _build_text(
        self, job: Job, analysis: LinkAnalysis, reporter: StatusReporter
    ) -> tuple[Path, str]:
        assert self.downloader is not None
        await reporter.edit("⚡ <b>Processing link</b>\nExtracting readable text…", force=True)

        text = analysis.extracted_text.strip()
        if not text and analysis.direct_file_url and EXT_TXT_RE.search(analysis.direct_file_url):
            with tempfile.TemporaryDirectory(prefix="udb_txt_fetch_") as tmp:
                tmp_path = Path(tmp) / "raw.txt"
                await reporter.edit("📥 <b>Downloading assets</b>\nFetching text file…", force=True)
                await self.downloader.stream_download(analysis.direct_file_url, tmp_path)
                text = await asyncio.to_thread(tmp_path.read_text, "utf-8", "ignore")

        if not text:
            raise ValueError("No readable text could be extracted from this link.")

        header = (
            f"{'=' * 60}\n"
            f"Title: {analysis.title}\n"
            f"Source: {analysis.final_url}\n"
            f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"{'=' * 60}\n\n"
        )
        payload = header + text
        safe_name = sanitize_filename(analysis.title, "document")
        with tempfile.TemporaryDirectory(prefix="udb_txt_") as tmp:
            out_txt = Path(tmp) / f"{safe_name}.txt"
            await reporter.edit("📦 <b>Packaging files</b>\nWriting text output…", force=True)
            await self.downloader.write_text(payload, out_txt)
            final_copy = DATA_DIR / "tmp_out" / out_txt.name
            final_copy.parent.mkdir(parents=True, exist_ok=True)
            await asyncio.to_thread(shutil.copyfile, out_txt, final_copy)
            return final_copy, out_txt.name

    async def _build_html(
        self, job: Job, analysis: LinkAnalysis, reporter: StatusReporter
    ) -> tuple[Path, str]:
        assert self.downloader is not None
        await reporter.edit("⚡ <b>Processing link</b>\nBuilding HTML snapshot…", force=True)

        if analysis.html_snapshot:
            content = analysis.html_snapshot
        else:
            # direct-file fallback snapshot
            content = (
                "<!DOCTYPE html><html><head><meta charset='UTF-8'>"
                "<meta name='viewport' content='width=device-width,initial-scale=1'>"
                f"<title>{escape_html(analysis.title)}</title></head><body>"
                f"<h2>{escape_html(analysis.title)}</h2>"
                f"<p>Source URL: <a href='{escape_html(analysis.final_url)}'>"
                f"{escape_html(analysis.final_url)}</a></p>"
                "<p>This URL appears to be a direct file, not an HTML page.</p>"
                "</body></html>"
            )
        safe_name = sanitize_filename(analysis.title, "snapshot")
        with tempfile.TemporaryDirectory(prefix="udb_html_") as tmp:
            out_html = Path(tmp) / f"{safe_name}.html"
            await reporter.edit("📦 <b>Packaging files</b>\nWriting HTML snapshot…", force=True)
            await self.downloader.write_text(content, out_html)
            final_copy = DATA_DIR / "tmp_out" / out_html.name
            final_copy.parent.mkdir(parents=True, exist_ok=True)
            await asyncio.to_thread(shutil.copyfile, out_html, final_copy)
            return final_copy, out_html.name


# =============================================================================
# 7) TELEGRAM COMMANDS / UI / ADMIN
# =============================================================================

def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📥 Download Document", callback_data="menu:download")],
            [InlineKeyboardButton("📂 My Downloads", callback_data="menu:downloads")],
            [InlineKeyboardButton("📚 Extract Text", callback_data="menu:text")],
            [InlineKeyboardButton("🌐 Website Snapshot", callback_data="menu:snapshot")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="menu:settings")],
            [InlineKeyboardButton("ℹ️ Help", callback_data="menu:help")],
        ]
    )


def link_actions_keyboard(context_id: str, page: int, total_pages: int) -> InlineKeyboardMarkup:
    nav_row: list[InlineKeyboardButton] = []
    if total_pages > 1:
        prev_page = (page - 1) % total_pages
        next_page = (page + 1) % total_pages
        nav_row = [
            InlineKeyboardButton("⬅ Prev Page", callback_data=f"ctx:{context_id}:page:{prev_page}"),
            InlineKeyboardButton("➡ Next Page", callback_data=f"ctx:{context_id}:page:{next_page}"),
        ]

    rows = [
        [
            InlineKeyboardButton("⬇ Download as PDF", callback_data=f"ctx:{context_id}:act:pdf"),
            InlineKeyboardButton("🖼 Download Images", callback_data=f"ctx:{context_id}:act:img"),
        ],
        [
            InlineKeyboardButton("📄 Extract Text", callback_data=f"ctx:{context_id}:act:txt"),
            InlineKeyboardButton("🌐 Save HTML", callback_data=f"ctx:{context_id}:act:html"),
        ],
    ]
    if nav_row:
        rows.append(nav_row)
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def history_keyboard(page: int, total_pages: int) -> InlineKeyboardMarkup:
    row: list[InlineKeyboardButton] = []
    if total_pages > 1:
        row = [
            InlineKeyboardButton("⬅ Prev Page", callback_data=f"hist:{(page - 1) % total_pages}"),
            InlineKeyboardButton("➡ Next Page", callback_data=f"hist:{(page + 1) % total_pages}"),
        ]
    rows = [row] if row else []
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def settings_keyboard(link_preview: bool) -> InlineKeyboardMarkup:
    state_label = "ON ✅" if link_preview else "OFF ❌"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"Link Preview: {state_label}", callback_data="set:preview")],
            [InlineKeyboardButton("🔙 Back", callback_data="menu:main")],
        ]
    )


def render_analysis_page(analysis: LinkAnalysis, page: int) -> tuple[str, int]:
    assets = analysis.all_assets()
    total = len(assets)
    if total == 0:
        base = (
            f"🌐 <b>Link analyzed</b>\n\n"
            f"📌 <b>Title:</b> {escape_html(analysis.title)}\n"
            f"🔗 <b>URL:</b> <code>{escape_html(analysis.final_url)}</code>\n"
            f"📄 PDF: <b>{len(analysis.pdf_links)}</b>\n"
            f"🖼 Images: <b>{len(analysis.image_links)}</b>\n"
            f"🗂 Docs: <b>{len(analysis.doc_links)}</b>\n"
            f"📎 Embedded: <b>{len(analysis.embedded_links)}</b>\n\n"
            "No asset list preview available."
        )
        return base, 1

    per_page = MAX_ASSETS_PER_PAGE
    total_pages = (total + per_page - 1) // per_page
    page = page % total_pages
    start = page * per_page
    end = min(start + per_page, total)
    snippet = assets[start:end]

    lines = [
        "🌐 <b>Link analyzed</b>",
        "",
        f"📌 <b>Title:</b> {escape_html(analysis.title)}",
        f"🔗 <b>URL:</b> <code>{escape_html(analysis.final_url)}</code>",
        f"📄 PDF: <b>{len(analysis.pdf_links)}</b> | 🖼 Images: <b>{len(analysis.image_links)}</b>",
        f"🗂 Docs: <b>{len(analysis.doc_links)}</b> | 📎 Embedded: <b>{len(analysis.embedded_links)}</b>",
        "",
        f"📑 <b>Assets Page {page + 1}/{total_pages}</b>",
    ]
    for i, (kind, u) in enumerate(snippet, start=start + 1):
        short_url = u if len(u) <= 95 else (u[:92] + "…")
        lines.append(f"{i}. <b>{kind}</b> — <code>{escape_html(short_url)}</code>")
    return "\n".join(lines), total_pages


def render_history_page(history: list[dict[str, Any]], page: int) -> tuple[str, int]:
    if not history:
        return (
            "📂 <b>My Downloads</b>\n\nNo downloads yet.\nSend a URL to start.",
            1,
        )

    per_page = 5
    total_pages = (len(history) + per_page - 1) // per_page
    page = page % total_pages
    start = page * per_page
    end = min(start + per_page, len(history))

    subset = list(reversed(history))[start:end]
    lines = [f"📂 <b>My Downloads</b>\n\nPage {page + 1}/{total_pages}\n"]
    for item in subset:
        ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(item.get("ts", now_ts())))
        action = str(item.get("action", "?")).upper()
        title = escape_html(str(item.get("title", "Untitled")))
        size = human_bytes(int(item.get("bytes", 0)))
        lines.append(f"• <b>{action}</b> | {title}\n  {ts} | {size}")
    return "\n".join(lines), total_pages


def runtime_from_context(ctx: ContextTypes.DEFAULT_TYPE) -> BotRuntime:
    return ctx.application.bot_data["runtime"]


async def ensure_authorized(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE
) -> tuple[bool, BotRuntime]:
    runtime = runtime_from_context(ctx)
    user = update.effective_user
    if not user:
        return False, runtime
    ok = await runtime.state.is_authorized(user.id)
    if ok:
        return True, runtime
    msg = (
        "❌ <b>Access denied</b>\n\n"
        "You are not authorized to use this bot.\n"
        f"Send your ID to owner: <code>{user.id}</code>"
    )
    if update.message:
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
    elif update.callback_query:
        await update.callback_query.answer("Not authorized", show_alert=True)
    return False, runtime


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ok, runtime = await ensure_authorized(update, ctx)
    if not ok:
        return ConversationHandler.END

    user = update.effective_user
    stats = await runtime.state.get_stats(user.id)
    text = (
        "🚀 <b>Universal Document Downloader & Viewer</b>\n\n"
        "Send any public webpage or document URL.\n"
        "Supported detections: PDF, images, HTML, TXT, DOCX links.\n\n"
        f"📊 Requests: <b>{stats['requests']}</b> | "
        f"✅ Completed: <b>{stats['completed']}</b> | "
        f"❌ Failed: <b>{stats['failed']}</b>\n\n"
        "Use the menu below:"
    )
    await update.message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=main_menu_keyboard(),
    )
    return STATE_WAITING_URL


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ok, _ = await ensure_authorized(update, ctx)
    if not ok:
        return ConversationHandler.END
    text = (
        "ℹ️ <b>Help</b>\n\n"
        "1) Send a valid public URL.\n"
        "2) Bot analyzes assets (PDF/images/docs/text).\n"
        "3) Choose output:\n"
        "   • ⬇ Download as PDF\n"
        "   • 🖼 Download Images (ZIP)\n"
        "   • 📄 Extract Text\n"
        "   • 🌐 Save HTML snapshot\n\n"
        "Admin commands (owner only):\n"
        "/add_user [id]\n/remove_user [id]\n/users\n/broadcast [message]\n\n"
        "Use /cancel to cancel any active flow."
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    return STATE_WAITING_URL


async def cmd_cancel(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("✅ Cancelled. Send /start to open the menu again.")
    return ConversationHandler.END


async def handle_menu_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ok, runtime = await ensure_authorized(update, ctx)
    if not ok:
        return ConversationHandler.END
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    if query.data in {"menu:main", "menu:download"}:
        await query.edit_message_text(
            "📥 <b>Download Document</b>\n\nSend a public URL now.",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(),
        )
        return STATE_WAITING_URL

    if query.data == "menu:help":
        await query.edit_message_text(
            "ℹ️ <b>Help</b>\n\n"
            "Send a URL and choose output format.\n"
            "The bot supports PDF/image/text/html processing with queue workers.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 Back", callback_data="menu:main")]]
            ),
        )
        return STATE_WAITING_URL

    if query.data == "menu:downloads":
        history = await runtime.state.get_history(user_id)
        text, total_pages = render_history_page(history, 0)
        await query.edit_message_text(
            text, parse_mode=ParseMode.HTML, reply_markup=history_keyboard(0, total_pages)
        )
        return STATE_WAITING_URL

    if query.data == "menu:text":
        await query.edit_message_text(
            "📚 <b>Extract Text Mode</b>\n\nSend a URL and then choose <b>Extract Text</b>.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 Back", callback_data="menu:main")]]
            ),
        )
        return STATE_WAITING_URL

    if query.data == "menu:snapshot":
        await query.edit_message_text(
            "🌐 <b>Website Snapshot Mode</b>\n\nSend a URL and then choose <b>Save HTML</b>.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 Back", callback_data="menu:main")]]
            ),
        )
        return STATE_WAITING_URL

    if query.data == "menu:settings":
        settings = await runtime.state.get_user_settings(user_id)
        await query.edit_message_text(
            "⚙️ <b>Settings</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=settings_keyboard(bool(settings.get("link_preview", True))),
        )
        return STATE_WAITING_URL

    return STATE_WAITING_URL


async def handle_history_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ok, runtime = await ensure_authorized(update, ctx)
    if not ok:
        return ConversationHandler.END
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    parts = (query.data or "").split(":")
    if len(parts) != 2:
        return STATE_WAITING_URL
    try:
        page = int(parts[1])
    except ValueError:
        page = 0
    history = await runtime.state.get_history(user_id)
    text, total_pages = render_history_page(history, page)
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=history_keyboard(page, total_pages),
    )
    return STATE_WAITING_URL


async def handle_settings_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ok, runtime = await ensure_authorized(update, ctx)
    if not ok:
        return ConversationHandler.END
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    if query.data == "set:preview":
        state = await runtime.state.toggle_link_preview(user_id)
        await query.edit_message_text(
            "⚙️ <b>Settings</b>\n\n"
            f"Link preview is now: <b>{'ON' if state else 'OFF'}</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=settings_keyboard(state),
        )
    return STATE_WAITING_URL


async def handle_text_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ok, runtime = await ensure_authorized(update, ctx)
    if not ok:
        return ConversationHandler.END

    user_id = update.effective_user.id
    text = update.message.text.strip()
    url = extract_first_url(text)
    if not url or not is_valid_url(url):
        await update.message.reply_text(
            "⚠️ Send a valid public URL starting with http:// or https://",
            reply_markup=main_menu_keyboard(),
        )
        return STATE_WAITING_URL

    allowed, wait_for = runtime.spam_guard.check(user_id)
    if not allowed:
        await update.message.reply_text(
            f"⏳ Too many requests. Please wait {wait_for}s and try again."
        )
        return STATE_WAITING_URL

    status_msg = await update.message.reply_text(
        "⚡ <b>Processing link</b>\nAnalyzing webpage/document…",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )
    try:
        assert runtime.parser is not None
        analysis = await runtime.parser.analyze(url)
        context_id = await runtime.add_context(user_id, analysis)
        rendered, pages = render_analysis_page(analysis, page=0)
        settings = await runtime.state.get_user_settings(user_id)
        text_out = rendered
        if settings.get("link_preview", True):
            text_out += "\n\nPreview enabled ✅"
        await status_msg.edit_text(
            text_out,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=not bool(settings.get("link_preview", True)),
            reply_markup=link_actions_keyboard(context_id, page=0, total_pages=pages),
        )
    except Exception as exc:
        logger.warning("URL parse failed: %s", exc)
        await status_msg.edit_text(
            f"❌ <b>Error</b>\n\n<code>{escape_html(str(exc)[:500])}</code>",
            parse_mode=ParseMode.HTML,
        )
    return STATE_WAITING_URL


async def handle_context_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ok, runtime = await ensure_authorized(update, ctx)
    if not ok:
        return ConversationHandler.END
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    # Expected:
    #   ctx:{id}:page:{n}
    #   ctx:{id}:act:{pdf|img|txt|html}
    parts = (query.data or "").split(":")
    if len(parts) < 4:
        return STATE_WAITING_URL
    _, context_id, op, value = parts[0], parts[1], parts[2], parts[3]

    payload = await runtime.get_context(context_id)
    if not payload:
        await query.answer("Session expired. Send URL again.", show_alert=True)
        return STATE_WAITING_URL
    if int(payload["user_id"]) != user_id:
        await query.answer("This session is not yours.", show_alert=True)
        return STATE_WAITING_URL

    analysis: LinkAnalysis = payload["analysis"]
    if op == "page":
        try:
            page = int(value)
        except ValueError:
            page = 0
        rendered, pages = render_analysis_page(analysis, page)
        settings = await runtime.state.get_user_settings(user_id)
        await query.edit_message_text(
            rendered,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=not bool(settings.get("link_preview", True)),
            reply_markup=link_actions_keyboard(context_id, page=page, total_pages=pages),
        )
        return STATE_WAITING_URL

    if op == "act":
        action = value
        if action not in {"pdf", "img", "txt", "html"}:
            await query.answer("Unknown action", show_alert=True)
            return STATE_WAITING_URL
        status = (
            "🚀 <b>Starting download</b>\n"
            "Queued for processing.\n"
            f"Action: <code>{action.upper()}</code>"
        )
        await query.edit_message_text(status, parse_mode=ParseMode.HTML)
        queue_pos = await runtime.enqueue(
            Job(
                user_id=user_id,
                chat_id=query.message.chat_id,
                status_message_id=query.message.message_id,
                context_id=context_id,
                action=action,
                requested_url=payload["url"],
            )
        )
        await query.answer(f"Queued (position: {queue_pos})")
        return STATE_WAITING_URL

    return STATE_WAITING_URL


def owner_only(func):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not update.effective_user or update.effective_user.id != OWNER_ID:
            await update.message.reply_text("❌ Owner-only command.")
            return
        return await func(update, ctx)

    return wrapper


@owner_only
async def cmd_add_user(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = runtime_from_context(ctx)
    if not ctx.args:
        await update.message.reply_text("Usage: /add_user [id]")
        return
    lines: list[str] = []
    for arg in ctx.args:
        try:
            uid = int(arg)
            ok = await runtime.state.add_user(uid)
            lines.append(f"{'✅' if ok else 'ℹ️'} {uid}")
        except ValueError:
            lines.append(f"❌ Invalid id: {arg}")
    await update.message.reply_text("\n".join(lines))


@owner_only
async def cmd_remove_user(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = runtime_from_context(ctx)
    if not ctx.args:
        await update.message.reply_text("Usage: /remove_user [id]")
        return
    lines: list[str] = []
    for arg in ctx.args:
        try:
            uid = int(arg)
            ok = await runtime.state.remove_user(uid)
            lines.append(f"{'✅ Removed' if ok else '❌ Not removed'} {uid}")
        except ValueError:
            lines.append(f"❌ Invalid id: {arg}")
    await update.message.reply_text("\n".join(lines))


@owner_only
async def cmd_users(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = runtime_from_context(ctx)
    users = await runtime.state.list_users()
    text = "\n".join([f"{'👑' if u == OWNER_ID else '👤'} <code>{u}</code>" for u in users])
    await update.message.reply_text(
        f"👥 <b>Authorized users ({len(users)})</b>\n\n{text}",
        parse_mode=ParseMode.HTML,
    )


@owner_only
async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = runtime_from_context(ctx)
    if not ctx.args:
        await update.message.reply_text("Usage: /broadcast [message]")
        return
    msg = " ".join(ctx.args)
    users = await runtime.state.list_users()
    sent = 0
    failed = 0
    for uid in users:
        try:
            await ctx.bot.send_message(
                uid,
                f"📢 <b>Broadcast</b>\n\n{escape_html(msg)}",
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            sent += 1
        except Exception:
            failed += 1
    await update.message.reply_text(f"Broadcast finished. ✅ {sent} | ❌ {failed}")


# =============================================================================
# 8) MAIN ASYNC RUNNER
# =============================================================================

class contextlib_suppress:
    """Small local substitute to avoid importing contextlib for one use."""

    def __init__(self, *exceptions: type[BaseException]) -> None:
        self.exceptions = exceptions

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type, exc, tb) -> bool:
        return bool(exc_type and issubclass(exc_type, self.exceptions))


async def on_post_init(app: Application) -> None:
    runtime = BotRuntime()
    await runtime.start(app)
    app.bot_data["runtime"] = runtime


async def on_post_shutdown(app: Application) -> None:
    runtime: BotRuntime | None = app.bot_data.get("runtime")
    if runtime:
        await runtime.close()


def build_application() -> Application:
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(on_post_init)
        .post_shutdown(on_post_shutdown)
        .build()
    )

    # Conversation for menu + link handling.
    conv = ConversationHandler(
        entry_points=[CommandHandler("start", cmd_start)],
        states={
            STATE_WAITING_URL: [
                CommandHandler("help", cmd_help),
                CommandHandler("cancel", cmd_cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message),
                CallbackQueryHandler(handle_menu_callback, pattern=r"^menu:"),
                CallbackQueryHandler(handle_history_callback, pattern=r"^hist:"),
                CallbackQueryHandler(handle_settings_callback, pattern=r"^set:"),
                CallbackQueryHandler(handle_context_callback, pattern=r"^ctx:"),
            ]
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
        allow_reentry=True,
    )
    app.add_handler(conv)

    # Commands that should work globally too.
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("add_user", cmd_add_user))
    app.add_handler(CommandHandler("remove_user", cmd_remove_user))
    app.add_handler(CommandHandler("users", cmd_users))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))

    return app


def main() -> None:
    setup_logging()
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is required. Set it in environment variables.")
    logger.info("Starting Universal Document Downloader Bot (owner=%s)", OWNER_ID)
    app = build_application()
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
        close_loop=False,
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Universal Document Downloader & Viewer Bot (single-file, production-ready)
==========================================================================

Dependencies (Python 3.11+):
    pip install -U python-telegram-bot[ext] aiohttp beautifulsoup4 Pillow aiofiles lxml

Run:
    export BOT_TOKEN="123456:ABC..."
    export OWNER_ID="123456789"
    python scribd_bot.py
"""

from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import re
import shutil
import textwrap
import time
import zipfile
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse
from uuid import uuid4

import aiofiles
import aiohttp
from bs4 import BeautifulSoup
from PIL import Image, ImageDraw, ImageFont
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
# 2) CONFIGURATION
# =============================================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "123456789"))
REQUIRE_AUTH = bool(int(os.getenv("REQUIRE_AUTH", "0")))

DATA_DIR = Path("bot_data")
TEMP_DIR = DATA_DIR / "temp"
STATE_FILE = DATA_DIR / "state.json"
LOG_FILE = DATA_DIR / "bot.log"

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=90, connect=20, sock_read=60)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

MAX_HTML_BYTES = 4 * 1024 * 1024
CHUNK_SIZE = 64 * 1024
RETRY_ATTEMPTS = 3
HTTP_CONCURRENCY = 8
IMAGE_DOWNLOAD_CONCURRENCY = 6
MAX_IMAGES_FOR_PDF = 60

QUEUE_MAXSIZE = 100
WORKER_COUNT = 3

RATE_LIMIT_COUNT = 8
RATE_LIMIT_WINDOW = 30

CACHE_TTL_SECONDS = 600
CACHE_MAX_ITEMS = 500
MAX_HISTORY_PER_USER = 100
HISTORY_PAGE_SIZE = 5
ASSET_PAGE_SIZE = 6

STATE_MENU = 1

ALLOWED_IMAGE_EXT = {".jpg", ".jpeg", ".png", ".webp", ".gif"}


# =============================================================================
# 3) HELPER UTILITIES
# =============================================================================

def utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def human_size(num_bytes: int) -> str:
    value = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{num_bytes} B"


def safe_filename(name: str, max_len: int = 90) -> str:
    sanitized = re.sub(r"[^\w\s.-]", "", name, flags=re.ASCII).strip().replace(" ", "_")
    return (sanitized[:max_len] or "document").strip("._")


def trim_url(text: str, max_len: int = 70) -> str:
    text = text.strip()
    return text if len(text) <= max_len else f"{text[:max_len]}..."


def extract_url(text: str) -> str | None:
    match = re.search(r"https?://[^\s<>()]+", text)
    if not match:
        return None
    url = match.group(0).rstrip(".,);]>\"'")
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return url


def action_label(action: str) -> str:
    labels = {
        "pdf": "PDF",
        "images": "Images ZIP",
        "text": "TXT",
        "html": "HTML Snapshot",
    }
    return labels.get(action, action.upper())


def file_ext_from_url(url: str) -> str:
    path = urlparse(url).path.lower()
    suffix = Path(path).suffix.lower()
    return suffix


async def rm_tree(path: Path) -> None:
    if path.exists():
        await asyncio.to_thread(shutil.rmtree, path, True)


async def ensure_dirs() -> None:
    for p in (DATA_DIR, TEMP_DIR):
        p.mkdir(parents=True, exist_ok=True)


async def cleanup_old_temp(max_age_hours: int = 6) -> None:
    if not TEMP_DIR.exists():
        return
    cutoff = time.time() - (max_age_hours * 3600)
    for child in TEMP_DIR.iterdir():
        try:
            if child.is_dir() and child.stat().st_mtime < cutoff:
                await rm_tree(child)
        except OSError:
            continue


class RateLimiter:
    def __init__(self, max_events: int, per_seconds: int) -> None:
        self.max_events = max_events
        self.per_seconds = per_seconds
        self.events: dict[int, deque[float]] = defaultdict(deque)

    def allow(self, user_id: int) -> bool:
        now = time.monotonic()
        dq = self.events[user_id]
        while dq and (now - dq[0]) > self.per_seconds:
            dq.popleft()
        if len(dq) >= self.max_events:
            return False
        dq.append(now)
        return True


class TTLCache:
    def __init__(self, ttl_seconds: int, max_items: int) -> None:
        self.ttl = ttl_seconds
        self.max_items = max_items
        self._store: OrderedDict[str, tuple[float, Any]] = OrderedDict()

    def get(self, key: str) -> Any | None:
        entry = self._store.get(key)
        if not entry:
            return None
        exp, value = entry
        if exp < time.time():
            self._store.pop(key, None)
            return None
        self._store.move_to_end(key)
        return value

    def set(self, key: str, value: Any) -> None:
        if key in self._store:
            self._store.move_to_end(key)
        self._store[key] = (time.time() + self.ttl, value)
        while len(self._store) > self.max_items:
            self._store.popitem(last=False)


class StateManager:
    """Persistent JSON state for users/settings/history/stats."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock = asyncio.Lock()
        self.data: dict[str, Any] = {
            "authorized_users": [],
            "known_users": [],
            "settings": {},
            "history": {},
            "stats": {},
        }

    @staticmethod
    def _uid(user_id: int) -> str:
        return str(user_id)

    @staticmethod
    def _default_settings() -> dict[str, Any]:
        return {"preview_enabled": True}

    @staticmethod
    def _default_stats() -> dict[str, int]:
        return {
            "requests": 0,
            "downloads": 0,
            "errors": 0,
            "pdf": 0,
            "images": 0,
            "text": 0,
            "html": 0,
            "bytes_sent": 0,
        }

    async def load(self) -> None:
        await ensure_dirs()
        if not self.path.exists():
            await self._ensure_owner_and_save()
            return
        try:
            async with aiofiles.open(self.path, "r", encoding="utf-8") as f:
                raw = await f.read()
            loaded = json.loads(raw) if raw.strip() else {}
            if isinstance(loaded, dict):
                self.data.update(loaded)
        except Exception:
            logging.exception("Failed to load state file, using defaults.")
        await self._ensure_owner_and_save()

    async def _ensure_owner_and_save(self) -> None:
        async with self.lock:
            users = set(map(int, self.data.get("authorized_users", [])))
            users.add(OWNER_ID)
            self.data["authorized_users"] = sorted(users)
            known = set(map(int, self.data.get("known_users", [])))
            known.add(OWNER_ID)
            self.data["known_users"] = sorted(known)
            await self._save_locked()

    async def _save_locked(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        async with aiofiles.open(tmp, "w", encoding="utf-8") as f:
            await f.write(json.dumps(self.data, ensure_ascii=False, indent=2))
        await asyncio.to_thread(tmp.replace, self.path)

    async def touch_user(self, user_id: int) -> None:
        async with self.lock:
            known = set(map(int, self.data.get("known_users", [])))
            if user_id not in known:
                known.add(user_id)
                self.data["known_users"] = sorted(known)
            uid = self._uid(user_id)
            self.data.setdefault("settings", {}).setdefault(uid, self._default_settings())
            self.data.setdefault("history", {}).setdefault(uid, [])
            self.data.setdefault("stats", {}).setdefault(uid, self._default_stats())
            await self._save_locked()

    async def is_allowed(self, user_id: int) -> bool:
        if user_id == OWNER_ID:
            return True
        if not REQUIRE_AUTH:
            return True
        async with self.lock:
            return user_id in set(map(int, self.data.get("authorized_users", [])))

    async def add_user(self, user_id: int) -> bool:
        async with self.lock:
            users = set(map(int, self.data.get("authorized_users", [])))
            if user_id in users:
                return False
            users.add(user_id)
            self.data["authorized_users"] = sorted(users)
            await self._save_locked()
            return True

    async def remove_user(self, user_id: int) -> bool:
        if user_id == OWNER_ID:
            return False
        async with self.lock:
            users = set(map(int, self.data.get("authorized_users", [])))
            if user_id not in users:
                return False
            users.remove(user_id)
            self.data["authorized_users"] = sorted(users)
            await self._save_locked()
            return True

    async def authorized_users(self) -> list[int]:
        async with self.lock:
            return sorted(map(int, self.data.get("authorized_users", [])))

    async def known_users(self) -> list[int]:
        async with self.lock:
            return sorted(map(int, self.data.get("known_users", [])))

    async def get_setting(self, user_id: int, key: str, default: Any = None) -> Any:
        uid = self._uid(user_id)
        async with self.lock:
            return self.data.get("settings", {}).get(uid, {}).get(key, default)

    async def toggle_preview(self, user_id: int) -> bool:
        uid = self._uid(user_id)
        async with self.lock:
            settings = self.data.setdefault("settings", {}).setdefault(uid, self._default_settings())
            settings["preview_enabled"] = not bool(settings.get("preview_enabled", True))
            await self._save_locked()
            return settings["preview_enabled"]

    async def add_history(self, user_id: int, entry: dict[str, Any]) -> None:
        uid = self._uid(user_id)
        async with self.lock:
            hist = self.data.setdefault("history", {}).setdefault(uid, [])
            hist.insert(0, entry)
            if len(hist) > MAX_HISTORY_PER_USER:
                del hist[MAX_HISTORY_PER_USER:]
            await self._save_locked()

    async def history_page(self, user_id: int, page: int, per_page: int) -> tuple[list[dict[str, Any]], int]:
        uid = self._uid(user_id)
        async with self.lock:
            hist = list(self.data.get("history", {}).get(uid, []))
        if not hist:
            return [], 1
        total_pages = (len(hist) + per_page - 1) // per_page
        page = max(0, min(page, total_pages - 1))
        start = page * per_page
        return hist[start : start + per_page], total_pages

    async def bump_stats(
        self,
        user_id: int,
        *,
        request: bool = False,
        action: str | None = None,
        success: bool | None = None,
        bytes_sent: int = 0,
    ) -> None:
        uid = self._uid(user_id)
        async with self.lock:
            stats = self.data.setdefault("stats", {}).setdefault(uid, self._default_stats())
            if request:
                stats["requests"] += 1
            if action in {"pdf", "images", "text", "html"}:
                stats[action] += 1
            if success is True:
                stats["downloads"] += 1
            elif success is False:
                stats["errors"] += 1
            if bytes_sent > 0:
                stats["bytes_sent"] += int(bytes_sent)
            await self._save_locked()

    async def user_stats(self, user_id: int) -> dict[str, int]:
        uid = self._uid(user_id)
        async with self.lock:
            return dict(self.data.get("stats", {}).get(uid, self._default_stats()))


async def safe_edit_status(
    bot,
    chat_id: int,
    message_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=reply_markup,
        )
    except BadRequest as exc:
        if "Message is not modified" not in str(exc):
            logging.debug("safe_edit_status BadRequest: %s", exc)
    except Exception:
        logging.exception("safe_edit_status failed")


class ProgressReporter:
    """Throttle message edits for progress updates."""

    def __init__(self, min_interval: float = 1.2) -> None:
        self.min_interval = min_interval
        self.last = 0.0

    def should_report(self, *, force: bool = False) -> bool:
        now = time.monotonic()
        if force or (now - self.last) >= self.min_interval:
            self.last = now
            return True
        return False


# =============================================================================
# 4) DOWNLOADER ENGINE
# =============================================================================

class DownloaderEngine:
    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session
        self.sem = asyncio.Semaphore(HTTP_CONCURRENCY)

    async def fetch_page(self, url: str) -> tuple[str, str, str]:
        """Fetch a URL; returns (text, final_url, content_type)."""
        last_exc: Exception | None = None
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                async with self.sem:
                    async with self.session.get(
                        url,
                        timeout=REQUEST_TIMEOUT,
                        headers=DEFAULT_HEADERS,
                        allow_redirects=True,
                    ) as resp:
                        resp.raise_for_status()
                        final_url = str(resp.url)
                        content_type = resp.headers.get("Content-Type", "").lower()
                        chunks: list[bytes] = []
                        total = 0
                        async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                            total += len(chunk)
                            if total > MAX_HTML_BYTES:
                                break
                            chunks.append(chunk)
                        raw = b"".join(chunks)
                        charset = resp.charset or "utf-8"
                        text = raw.decode(charset, errors="ignore")
                        return text, final_url, content_type
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = exc
                await asyncio.sleep(0.6 * attempt)
        raise RuntimeError(f"Failed to fetch URL after retries: {last_exc}")

    async def stream_to_file(
        self,
        url: str,
        output_path: Path,
        *,
        progress_cb=None,
    ) -> int:
        """Stream download to file asynchronously with retries."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        last_exc: Exception | None = None

        for attempt in range(1, RETRY_ATTEMPTS + 1):
            downloaded = 0
            try:
                async with self.sem:
                    async with self.session.get(
                        url,
                        timeout=REQUEST_TIMEOUT,
                        headers=DEFAULT_HEADERS,
                        allow_redirects=True,
                    ) as resp:
                        resp.raise_for_status()
                        total = int(resp.headers.get("Content-Length", "0") or "0")
                        async with aiofiles.open(output_path, "wb") as f:
                            async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                                downloaded += len(chunk)
                                await f.write(chunk)
                                if progress_cb:
                                    await progress_cb(downloaded, total)
                        return downloaded
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
                last_exc = exc
                await asyncio.sleep(0.8 * attempt)
        raise RuntimeError(f"Download failed for {url}: {last_exc}")

    async def download_images(
        self,
        image_urls: list[str],
        target_dir: Path,
        *,
        progress_cb=None,
    ) -> list[Path]:
        target_dir.mkdir(parents=True, exist_ok=True)
        sem = asyncio.Semaphore(IMAGE_DOWNLOAD_CONCURRENCY)
        completed = 0
        lock = asyncio.Lock()
        out_paths: list[Path | None] = [None] * len(image_urls)

        async def _one(idx: int, image_url: str) -> None:
            nonlocal completed
            suffix = file_ext_from_url(image_url)
            if suffix not in ALLOWED_IMAGE_EXT:
                suffix = ".jpg"
            out = target_dir / f"img_{idx + 1:04d}{suffix}"
            async with sem:
                try:
                    await self.stream_to_file(image_url, out)
                    out_paths[idx] = out
                except Exception:
                    logging.warning("Image download failed: %s", image_url)
            async with lock:
                completed += 1
                if progress_cb:
                    await progress_cb(completed, len(image_urls))

        await asyncio.gather(*(_one(i, u) for i, u in enumerate(image_urls)))
        return [p for p in out_paths if p]

    async def images_to_pdf(self, image_paths: list[Path], out_pdf: Path) -> int:
        def _convert() -> int:
            opened: list[Image.Image] = []
            rgb_images: list[Image.Image] = []
            try:
                for path in image_paths[:MAX_IMAGES_FOR_PDF]:
                    img = Image.open(path)
                    opened.append(img)
                    if max(img.size) > 2600:
                        img.thumbnail((2600, 2600), Image.Resampling.LANCZOS)
                    if img.mode != "RGB":
                        img = img.convert("RGB")
                    rgb_images.append(img)
                if not rgb_images:
                    raise ValueError("No valid images available for PDF conversion")
                first, *rest = rgb_images
                first.save(out_pdf, "PDF", resolution=150.0, save_all=True, append_images=rest)
                return out_pdf.stat().st_size
            finally:
                for img in opened:
                    try:
                        img.close()
                    except Exception:
                        pass
                for img in rgb_images:
                    try:
                        img.close()
                    except Exception:
                        pass

        return await asyncio.to_thread(_convert)

    async def text_to_pdf(self, text: str, title: str, out_pdf: Path) -> int:
        def _render() -> int:
            font = ImageFont.load_default()
            page_w, page_h = 1240, 1754
            margin = 70
            line_h = 22
            max_chars = 95

            lines: list[str] = [title, "-" * min(len(title), 80), ""]
            for paragraph in text.splitlines():
                para = paragraph.strip()
                if not para:
                    lines.append("")
                    continue
                lines.extend(textwrap.wrap(para, width=max_chars))

            pages: list[Image.Image] = []
            page = Image.new("RGB", (page_w, page_h), "white")
            draw = ImageDraw.Draw(page)
            y = margin

            for line in lines:
                if y + line_h > page_h - margin:
                    pages.append(page)
                    page = Image.new("RGB", (page_w, page_h), "white")
                    draw = ImageDraw.Draw(page)
                    y = margin
                draw.text((margin, y), line, fill="black", font=font)
                y += line_h
            pages.append(page)

            first, *rest = pages
            first.save(out_pdf, "PDF", resolution=150.0, save_all=True, append_images=rest)
            for p in pages:
                p.close()
            return out_pdf.stat().st_size

        return await asyncio.to_thread(_render)

    async def make_zip(self, files: list[Path], out_zip: Path) -> int:
        def _zip() -> int:
            with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                for fp in files:
                    if fp.exists():
                        zf.write(fp, arcname=fp.name)
            return out_zip.stat().st_size

        return await asyncio.to_thread(_zip)

    async def write_text(self, content: str, out_file: Path) -> int:
        async with aiofiles.open(out_file, "w", encoding="utf-8") as f:
            await f.write(content)
        return out_file.stat().st_size

    async def write_html(self, content: str, out_file: Path) -> int:
        async with aiofiles.open(out_file, "w", encoding="utf-8") as f:
            await f.write(content)
        return out_file.stat().st_size


# =============================================================================
# 5) PARSER ENGINE
# =============================================================================

@dataclass(slots=True)
class AssetLink:
    kind: str
    url: str
    source: str


@dataclass(slots=True)
class ParsedPage:
    requested_url: str
    final_url: str
    title: str
    content_type: str
    html_content: str
    text_content: str
    excerpt: str
    assets: list[AssetLink]
    pdf_links: list[str]
    image_links: list[str]
    docx_links: list[str]
    txt_links: list[str]


class ParserEngine:
    def __init__(self, downloader: DownloaderEngine, cache: TTLCache) -> None:
        self.downloader = downloader
        self.cache = cache

    @staticmethod
    def _classify_url(candidate: str) -> str:
        low = candidate.lower()
        suffix = file_ext_from_url(candidate)
        if suffix == ".pdf" or "application/pdf" in low:
            return "pdf"
        if suffix in ALLOWED_IMAGE_EXT:
            return "image"
        if suffix == ".docx":
            return "docx"
        if suffix == ".txt":
            return "txt"
        if suffix in {".html", ".htm"}:
            return "html"
        return "other"

    @staticmethod
    def _normalize_assets(assets: list[AssetLink]) -> list[AssetLink]:
        seen: set[str] = set()
        out: list[AssetLink] = []
        for asset in assets:
            u = asset.url.split("#")[0]
            if u in seen:
                continue
            seen.add(u)
            out.append(AssetLink(kind=asset.kind, url=u, source=asset.source))
        return out

    async def parse(self, url: str) -> ParsedPage:
        cached = self.cache.get(url)
        if cached:
            return cached

        suffix = file_ext_from_url(url)
        if suffix in {".pdf", ".docx", ".txt"} | ALLOWED_IMAGE_EXT:
            direct_kind = self._classify_url(url)
            assets = [AssetLink(kind=direct_kind, url=url, source="direct")]
            parsed = ParsedPage(
                requested_url=url,
                final_url=url,
                title=safe_filename(Path(urlparse(url).path).stem or "document"),
                content_type="application/octet-stream",
                html_content="",
                text_content="",
                excerpt="Direct file URL detected.",
                assets=assets,
                pdf_links=[url] if direct_kind == "pdf" else [],
                image_links=[url] if direct_kind == "image" else [],
                docx_links=[url] if direct_kind == "docx" else [],
                txt_links=[url] if direct_kind == "txt" else [],
            )
            self.cache.set(url, parsed)
            return parsed

        page_text, final_url, content_type = await self.downloader.fetch_page(url)
        low_ct = content_type.lower()

        # Handle non-HTML resources while still supporting public direct links.
        if "html" not in low_ct and not page_text.lstrip().startswith("<"):
            kind = "other"
            if "pdf" in low_ct:
                kind = "pdf"
            elif "image" in low_ct:
                kind = "image"
            elif "word" in low_ct or "docx" in low_ct:
                kind = "docx"
            elif "text/plain" in low_ct:
                kind = "txt"
            assets = [AssetLink(kind=kind, url=final_url, source="content-type")]
            parsed = ParsedPage(
                requested_url=url,
                final_url=final_url,
                title=safe_filename(Path(urlparse(final_url).path).stem or "document"),
                content_type=content_type,
                html_content="",
                text_content=page_text[:10000] if kind == "txt" else "",
                excerpt="Direct downloadable resource detected.",
                assets=assets,
                pdf_links=[final_url] if kind == "pdf" else [],
                image_links=[final_url] if kind == "image" else [],
                docx_links=[final_url] if kind == "docx" else [],
                txt_links=[final_url] if kind == "txt" else [],
            )
            self.cache.set(url, parsed)
            return parsed

        soup = BeautifulSoup(page_text, "lxml")
        title = "Untitled Page"
        if soup.title and soup.title.string:
            title = soup.title.string.strip()
        elif (og := soup.find("meta", property="og:title")) and og.get("content"):
            title = str(og["content"]).strip()

        assets: list[AssetLink] = []

        def add_asset(raw_url: str | None, source: str) -> None:
            if not raw_url:
                return
            absolute = urljoin(final_url, raw_url.strip())
            parsed_abs = urlparse(absolute)
            if parsed_abs.scheme not in {"http", "https"} or not parsed_abs.netloc:
                return
            kind = self._classify_url(absolute)
            assets.append(AssetLink(kind=kind, url=absolute, source=source))

        for tag in soup.select("a[href]"):
            add_asset(tag.get("href"), "a[href]")
        for tag in soup.select("img[src], img[data-src], source[src]"):
            add_asset(tag.get("src") or tag.get("data-src"), tag.name)
        for tag in soup.select("iframe[src], embed[src], object[data]"):
            add_asset(tag.get("src") or tag.get("data"), tag.name)

        assets = self._normalize_assets(assets)

        # Extract readable text from webpage.
        for bad in soup(["script", "style", "noscript"]):
            bad.decompose()
        text_content = re.sub(r"\n{3,}", "\n\n", soup.get_text("\n", strip=True)).strip()
        excerpt = text_content[:400] + ("..." if len(text_content) > 400 else "")

        pdf_links = [a.url for a in assets if a.kind == "pdf"]
        image_links = [a.url for a in assets if a.kind == "image"]
        docx_links = [a.url for a in assets if a.kind == "docx"]
        txt_links = [a.url for a in assets if a.kind == "txt"]

        parsed = ParsedPage(
            requested_url=url,
            final_url=final_url,
            title=title[:200] or "Untitled Page",
            content_type=content_type,
            html_content=page_text,
            text_content=text_content,
            excerpt=excerpt,
            assets=assets,
            pdf_links=pdf_links,
            image_links=image_links,
            docx_links=docx_links,
            txt_links=txt_links,
        )
        self.cache.set(url, parsed)
        return parsed


# =============================================================================
# 6) TELEGRAM HANDLERS
# =============================================================================

@dataclass(slots=True)
class DownloadJob:
    user_id: int
    chat_id: int
    status_message_id: int
    url: str
    action: str
    future: asyncio.Future


def kb_main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📥 Download Document", callback_data="menu:download")],
            [InlineKeyboardButton("📂 My Downloads", callback_data="menu:my")],
            [InlineKeyboardButton("📚 Extract Text", callback_data="menu:extract")],
            [InlineKeyboardButton("🌐 Website Snapshot", callback_data="menu:snapshot")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="menu:settings")],
            [InlineKeyboardButton("ℹ️ Help", callback_data="menu:help")],
        ]
    )


def kb_link_actions(page: int, total_pages: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("⬇ Download as PDF", callback_data="action:pdf"),
            InlineKeyboardButton("🖼 Download Images", callback_data="action:images"),
        ],
        [
            InlineKeyboardButton("📄 Extract Text", callback_data="action:text"),
            InlineKeyboardButton("🌐 Save HTML", callback_data="action:html"),
        ],
    ]
    if total_pages > 1:
        rows.append(
            [
                InlineKeyboardButton("⬅ Prev Page", callback_data="asset:prev"),
                InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="asset:noop"),
                InlineKeyboardButton("➡ Next Page", callback_data="asset:next"),
            ]
        )
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="menu:back")])
    return InlineKeyboardMarkup(rows)


def kb_history(page: int, total_pages: int) -> InlineKeyboardMarkup:
    rows = []
    if total_pages > 1:
        rows.append(
            [
                InlineKeyboardButton("⬅ Prev Page", callback_data=f"hist:{max(0, page - 1)}"),
                InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="hist:noop"),
                InlineKeyboardButton(
                    "➡ Next Page", callback_data=f"hist:{min(total_pages - 1, page + 1)}"
                ),
            ]
        )
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="menu:back")])
    return InlineKeyboardMarkup(rows)


def kb_settings(preview_enabled: bool) -> InlineKeyboardMarkup:
    status = "ON ✅" if preview_enabled else "OFF ❌"
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"Link Preview: {status}",
                    callback_data="settings:toggle_preview",
                )
            ],
            [InlineKeyboardButton("🔙 Back", callback_data="menu:back")],
        ]
    )


def render_asset_page(parsed: ParsedPage, page: int) -> tuple[str, int]:
    total_assets = len(parsed.assets)
    total_pages = max(1, (total_assets + ASSET_PAGE_SIZE - 1) // ASSET_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * ASSET_PAGE_SIZE
    items = parsed.assets[start : start + ASSET_PAGE_SIZE]

    lines = []
    for idx, asset in enumerate(items, start=start + 1):
        lines.append(
            f"{idx}. <b>{asset.kind.upper()}</b> • "
            f"<code>{html.escape(trim_url(asset.url, 75))}</code>"
        )
    assets_block = "\n".join(lines) if lines else "No downloadable assets detected."

    text = (
        "⚡ <b>Processing link</b>\n\n"
        f"🔗 <code>{html.escape(trim_url(parsed.final_url, 80))}</code>\n"
        f"📰 <b>{html.escape(parsed.title)}</b>\n"
        f"📄 PDFs: <b>{len(parsed.pdf_links)}</b> | "
        f"🖼 Images: <b>{len(parsed.image_links)}</b> | "
        f"📎 DOCX: <b>{len(parsed.docx_links)}</b>\n\n"
        f"{assets_block}\n\n"
        "Choose output format below:"
    )
    return text, total_pages


async def render_history_message(
    user_id: int,
    state: StateManager,
    page: int,
) -> tuple[str, InlineKeyboardMarkup]:
    items, total_pages = await state.history_page(user_id, page, HISTORY_PAGE_SIZE)
    if not items:
        text = (
            "📂 <b>My Downloads</b>\n\n"
            "No history yet. Send a URL first from the main menu."
        )
        return text, kb_history(0, 1)

    lines = [f"📂 <b>My Downloads</b> (page {page + 1}/{total_pages})\n"]
    for item in items:
        ts = item.get("timestamp", "")[:19].replace("T", " ")
        lines.append(
            f"• <b>{html.escape(item.get('title', 'Untitled'))}</b>\n"
            f"  {action_label(item.get('action', '?'))} | "
            f"{item.get('status', 'unknown')} | "
            f"{human_size(int(item.get('size_bytes', 0)))}\n"
            f"  <code>{html.escape(trim_url(item.get('url', ''), 65))}</code>\n"
            f"  <i>{html.escape(ts)}</i>"
        )
    return "\n\n".join(lines), kb_history(page, total_pages)


async def ensure_access(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    state: StateManager = context.application.bot_data["state"]
    user = update.effective_user
    if not user:
        return False
    await state.touch_user(user.id)
    if await state.is_allowed(user.id):
        return True

    text = (
        "❌ <b>Access denied</b>\n\n"
        "You are not authorized to use this bot.\n"
        f"Send your ID to owner: <code>{user.id}</code>"
    )
    if update.message:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    elif update.callback_query:
        await update.callback_query.answer("Access denied", show_alert=True)
    return False


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_access(update, context):
        return ConversationHandler.END
    user = update.effective_user
    await update.effective_message.reply_text(
        (
            f"🚀 <b>Universal Document Downloader & Viewer</b>\n\n"
            f"Hello, {html.escape(user.first_name)}!\n"
            "Send any public webpage/document URL.\n"
            "I can detect downloadable assets and export as PDF/ZIP/TXT/HTML.\n\n"
            "Use the menu below:"
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_main_menu(),
        disable_web_page_preview=True,
    )
    return STATE_MENU


async def cmd_help(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(
        (
            "ℹ️ <b>Help</b>\n\n"
            "1) Open <b>📥 Download Document</b>\n"
            "2) Send a public URL\n"
            "3) Choose output:\n"
            "   • ⬇ Download as PDF\n"
            "   • 🖼 Download Images (ZIP)\n"
            "   • 📄 Extract Text (TXT)\n"
            "   • 🌐 Save HTML snapshot\n\n"
            "Commands:\n"
            "/start /help /cancel\n"
            "/add_user /remove_user /users /broadcast (owner only)"
        ),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def cmd_cancel(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.effective_message.reply_text(
        "Operation canceled. Returning to main menu.",
        reply_markup=kb_main_menu(),
    )
    return STATE_MENU


async def _analyze_and_show_options(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    url: str,
) -> None:
    parser: ParserEngine = context.application.bot_data["parser"]
    state: StateManager = context.application.bot_data["state"]
    user_id = update.effective_user.id

    await state.bump_stats(user_id, request=True)
    status = await update.effective_message.reply_text(
        "⚡ Processing link\n\nAnalyzing page and detecting assets...",
        disable_web_page_preview=True,
    )
    parsed = await parser.parse(url)
    context.user_data["current_url"] = url
    context.user_data["asset_page"] = 0

    show_preview = bool(await state.get_setting(user_id, "preview_enabled", True))
    msg, total_pages = render_asset_page(parsed, 0)
    if not show_preview:
        msg = (
            "⚡ <b>Processing link</b>\n\n"
            f"📰 <b>{html.escape(parsed.title)}</b>\n"
            f"🔗 <code>{html.escape(trim_url(parsed.final_url, 80))}</code>\n\n"
            "Link preview is disabled in settings.\n"
            "Choose output format:"
        )
        total_pages = 1

    await status.edit_text(
        msg,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=kb_link_actions(0, total_pages),
    )


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_access(update, context):
        return STATE_MENU
    user_id = update.effective_user.id
    limiter: RateLimiter = context.application.bot_data["rate_limiter"]

    if not limiter.allow(user_id):
        await update.message.reply_text(
            "⏳ Anti-spam active. Please slow down and try again in a few seconds."
        )
        return STATE_MENU

    url = extract_url(update.message.text or "")
    if not url:
        await update.message.reply_text(
            "Send a valid public URL (http/https) or use /help.",
            reply_markup=kb_main_menu(),
        )
        return STATE_MENU

    try:
        await _analyze_and_show_options(update, context, url)
    except Exception as exc:
        logging.exception("URL analyze failed")
        await update.message.reply_text(
            f"❌ Error while analyzing URL:\n<code>{html.escape(str(exc)[:250])}</code>",
            parse_mode=ParseMode.HTML,
        )
    return STATE_MENU


async def enqueue_job(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    action: str,
) -> None:
    queue: asyncio.Queue = context.application.bot_data["download_queue"]
    active_jobs: set[int] = context.application.bot_data["active_jobs"]
    limiter: RateLimiter = context.application.bot_data["rate_limiter"]
    user_id = query.from_user.id

    if not limiter.allow(user_id):
        await query.answer("Too many requests. Slow down.", show_alert=True)
        return
    if user_id in active_jobs:
        await query.answer("You already have an active download.", show_alert=True)
        return
    url = context.user_data.get("current_url")
    if not url:
        await query.answer("No URL in session. Send a link first.", show_alert=True)
        return

    loop = asyncio.get_running_loop()
    future: asyncio.Future = loop.create_future()
    status = await query.message.reply_text(
        f"🚀 Starting download\n📌 Queue position: {queue.qsize() + 1}",
        disable_web_page_preview=True,
    )
    job = DownloadJob(
        user_id=user_id,
        chat_id=query.message.chat_id,
        status_message_id=status.message_id,
        url=url,
        action=action,
        future=future,
    )
    try:
        queue.put_nowait(job)
    except asyncio.QueueFull:
        await status.edit_text("❌ Queue is full. Please try again later.")
        return

    active_jobs.add(user_id)
    try:
        await future
    except Exception as exc:
        await status.edit_text(
            f"❌ Error\n<code>{html.escape(str(exc)[:250])}</code>",
            parse_mode=ParseMode.HTML,
        )


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_access(update, context):
        return STATE_MENU

    query = update.callback_query
    await query.answer()
    data = query.data or ""
    state: StateManager = context.application.bot_data["state"]

    if data == "menu:download":
        await query.edit_message_text(
            "📥 Send a public webpage/document URL.",
            reply_markup=kb_main_menu(),
        )
        return STATE_MENU

    if data == "menu:extract":
        await query.edit_message_text(
            "📚 Send a URL, then choose <b>📄 Extract Text</b>.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_main_menu(),
        )
        return STATE_MENU

    if data == "menu:snapshot":
        await query.edit_message_text(
            "🌐 Send a URL, then choose <b>🌐 Save HTML</b>.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_main_menu(),
        )
        return STATE_MENU

    if data == "menu:my":
        hist_text, hist_kb = await render_history_message(query.from_user.id, state, 0)
        await query.edit_message_text(
            hist_text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=hist_kb,
        )
        return STATE_MENU

    if data.startswith("hist:"):
        if data == "hist:noop":
            return STATE_MENU
        page = int(data.split(":", 1)[1])
        hist_text, hist_kb = await render_history_message(query.from_user.id, state, page)
        await query.edit_message_text(
            hist_text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=hist_kb,
        )
        return STATE_MENU

    if data == "menu:settings":
        enabled = bool(await state.get_setting(query.from_user.id, "preview_enabled", True))
        await query.edit_message_text(
            "⚙️ <b>Settings</b>\n\nConfigure your bot behavior:",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_settings(enabled),
        )
        return STATE_MENU

    if data == "settings:toggle_preview":
        enabled = await state.toggle_preview(query.from_user.id)
        await query.edit_message_text(
            "⚙️ <b>Settings</b>\n\nConfigure your bot behavior:",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_settings(enabled),
        )
        return STATE_MENU

    if data == "menu:help":
        await query.edit_message_text(
            "Use /help for complete instructions.",
            reply_markup=kb_main_menu(),
        )
        return STATE_MENU

    if data == "menu:back":
        await query.edit_message_text(
            "Main menu:",
            reply_markup=kb_main_menu(),
        )
        return STATE_MENU

    if data.startswith("asset:"):
        if data == "asset:noop":
            return STATE_MENU
        url = context.user_data.get("current_url")
        if not url:
            await query.answer("No active URL. Send link first.", show_alert=True)
            return STATE_MENU
        parser: ParserEngine = context.application.bot_data["parser"]
        parsed = await parser.parse(url)
        page = int(context.user_data.get("asset_page", 0))
        page = page - 1 if data == "asset:prev" else page + 1
        msg, total_pages = render_asset_page(parsed, page)
        page = max(0, min(page, total_pages - 1))
        context.user_data["asset_page"] = page
        await query.edit_message_text(
            msg,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=kb_link_actions(page, total_pages),
        )
        return STATE_MENU

    if data.startswith("action:"):
        action = data.split(":", 1)[1]
        if action in {"pdf", "images", "text", "html"}:
            await enqueue_job(query, context, action)
        return STATE_MENU

    return STATE_MENU


# =============================================================================
# 7) ADMIN COMMANDS
# =============================================================================

def owner_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != OWNER_ID:
            await update.effective_message.reply_text("❌ Owner-only command.")
            return
        return await func(update, context)

    return wrapper


@owner_only
async def cmd_add_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /add_user [id]")
        return
    state: StateManager = context.application.bot_data["state"]
    lines = []
    for raw in context.args:
        try:
            uid = int(raw)
        except ValueError:
            lines.append(f"❌ Invalid ID: {raw}")
            continue
        created = await state.add_user(uid)
        lines.append(f"✅ Added {uid}" if created else f"ℹ️ Already exists: {uid}")
    await update.message.reply_text("\n".join(lines))


@owner_only
async def cmd_remove_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /remove_user [id]")
        return
    state: StateManager = context.application.bot_data["state"]
    lines = []
    for raw in context.args:
        try:
            uid = int(raw)
        except ValueError:
            lines.append(f"❌ Invalid ID: {raw}")
            continue
        removed = await state.remove_user(uid)
        lines.append(f"✅ Removed {uid}" if removed else f"❌ Not removable: {uid}")
    await update.message.reply_text("\n".join(lines))


@owner_only
async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    state: StateManager = context.application.bot_data["state"]
    users = await state.authorized_users()
    msg = ["👥 Authorized users:"]
    for uid in users:
        msg.append(f"{'👑' if uid == OWNER_ID else '👤'} <code>{uid}</code>")
    await update.message.reply_text("\n".join(msg), parse_mode=ParseMode.HTML)


@owner_only
async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /broadcast [message]")
        return
    state: StateManager = context.application.bot_data["state"]
    targets = await state.known_users()
    body = " ".join(context.args)
    sent = failed = 0
    for uid in targets:
        try:
            await context.bot.send_message(uid, f"📢 <b>Broadcast</b>\n\n{html.escape(body)}", parse_mode=ParseMode.HTML)
            sent += 1
        except Exception:
            failed += 1
    await update.message.reply_text(f"Broadcast complete. ✅ {sent} sent | ❌ {failed} failed")


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_access(update, context):
        return
    state: StateManager = context.application.bot_data["state"]
    stats = await state.user_stats(update.effective_user.id)
    await update.message.reply_text(
        (
            "📊 <b>Your Statistics</b>\n\n"
            f"Requests: <b>{stats['requests']}</b>\n"
            f"Downloads: <b>{stats['downloads']}</b>\n"
            f"Errors: <b>{stats['errors']}</b>\n"
            f"PDF: {stats['pdf']} | Images: {stats['images']} | "
            f"TXT: {stats['text']} | HTML: {stats['html']}\n"
            f"Total sent: <b>{human_size(stats['bytes_sent'])}</b>"
        ),
        parse_mode=ParseMode.HTML,
    )


# =============================================================================
# Download queue worker implementation
# =============================================================================

async def process_job(application: Application, job: DownloadJob) -> None:
    state: StateManager = application.bot_data["state"]
    parser: ParserEngine = application.bot_data["parser"]
    downloader: DownloaderEngine = application.bot_data["downloader"]
    active_jobs: set[int] = application.bot_data["active_jobs"]
    bot = application.bot
    reporter = ProgressReporter()

    job_dir = TEMP_DIR / f"{job.user_id}_{int(time.time())}_{uuid4().hex[:8]}"
    job_dir.mkdir(parents=True, exist_ok=True)
    out_path: Path | None = None
    out_size = 0
    parsed_title = "document"

    async def report(text: str) -> None:
        await safe_edit_status(bot, job.chat_id, job.status_message_id, text)

    try:
        await report("🚀 Starting download\n⚡ Processing link")
        parsed = await parser.parse(job.url)
        parsed_title = safe_filename(parsed.title)

        if job.action == "pdf":
            # Priority: direct PDF > images->PDF > text->PDF.
            if parsed.pdf_links:
                out_path = job_dir / f"{parsed_title}.pdf"

                async def pdf_prog(downloaded: int, total: int) -> None:
                    if reporter.should_report(force=total > 0 and downloaded >= total):
                        pct = int((downloaded / total) * 100) if total else 0
                        await report(f"📥 Downloading assets\nPDF stream: {pct}%")

                out_size = await downloader.stream_to_file(parsed.pdf_links[0], out_path, progress_cb=pdf_prog)
            elif parsed.image_links:
                await report("📥 Downloading assets\nCollecting images...")

                async def img_prog(done: int, total: int) -> None:
                    if reporter.should_report(force=done >= total):
                        pct = int((done / total) * 100)
                        await report(f"📥 Downloading assets\nImages: {done}/{total} ({pct}%)")

                image_paths = await downloader.download_images(
                    parsed.image_links[:MAX_IMAGES_FOR_PDF],
                    job_dir / "images",
                    progress_cb=img_prog,
                )
                if not image_paths:
                    raise RuntimeError("No downloadable images found for PDF conversion.")
                await report("📦 Packaging files\nConverting images to PDF...")
                out_path = job_dir / f"{parsed_title}.pdf"
                out_size = await downloader.images_to_pdf(image_paths, out_path)
            else:
                text_source = parsed.text_content.strip()
                if len(text_source) < 20:
                    raise RuntimeError("Unable to build PDF: no PDF links, images, or readable text.")
                await report("📦 Packaging files\nRendering text into PDF...")
                out_path = job_dir / f"{parsed_title}.pdf"
                out_size = await downloader.text_to_pdf(text_source[:150_000], parsed.title, out_path)

        elif job.action == "images":
            if not parsed.image_links:
                raise RuntimeError("No image assets detected on this URL.")
            await report("📥 Downloading assets\nCollecting images...")

            async def img_prog(done: int, total: int) -> None:
                if reporter.should_report(force=done >= total):
                    pct = int((done / total) * 100)
                    await report(f"📥 Downloading assets\nImages: {done}/{total} ({pct}%)")

            image_paths = await downloader.download_images(parsed.image_links, job_dir / "images", progress_cb=img_prog)
            if not image_paths:
                raise RuntimeError("All image downloads failed.")
            await report("📦 Packaging files\nCreating ZIP archive...")
            out_path = job_dir / f"{parsed_title}_images.zip"
            out_size = await downloader.make_zip(image_paths, out_path)

        elif job.action == "text":
            text_data = parsed.text_content.strip()
            if not text_data:
                raise RuntimeError("No readable text found on this page.")
            await report("📦 Packaging files\nWriting TXT...")
            out_path = job_dir / f"{parsed_title}.txt"
            header = (
                f"Title: {parsed.title}\nURL: {parsed.final_url}\n"
                f"Extracted: {utc_iso_now()}\n{'=' * 70}\n\n"
            )
            out_size = await downloader.write_text(header + text_data, out_path)

        elif job.action == "html":
            html_data = parsed.html_content
            if not html_data:
                html_data = (
                    "<!doctype html><html><body>"
                    f"<h1>{html.escape(parsed.title)}</h1>"
                    f"<p>Original URL: {html.escape(parsed.final_url)}</p>"
                    "</body></html>"
                )
            await report("📦 Packaging files\nSaving HTML snapshot...")
            out_path = job_dir / f"{parsed_title}.html"
            out_size = await downloader.write_html(html_data, out_path)
        else:
            raise RuntimeError(f"Unsupported action: {job.action}")

        if not out_path or not out_path.exists():
            raise RuntimeError("Output file was not created.")

        await report("📤 Uploading to Telegram")
        await bot.send_chat_action(chat_id=job.chat_id, action=ChatAction.UPLOAD_DOCUMENT)
        with out_path.open("rb") as payload:
            await bot.send_document(
                chat_id=job.chat_id,
                document=payload,
                filename=out_path.name,
                caption=(
                    f"✅ <b>{html.escape(parsed.title)}</b>\n"
                    f"Format: {action_label(job.action)}\n"
                    f"Size: {human_size(out_size)}"
                ),
                parse_mode=ParseMode.HTML,
            )

        await report("✅ Completed")

        await state.bump_stats(job.user_id, action=job.action, success=True, bytes_sent=out_size)
        await state.add_history(
            job.user_id,
            {
                "timestamp": utc_iso_now(),
                "url": job.url,
                "title": parsed_title,
                "action": job.action,
                "status": "completed",
                "size_bytes": out_size,
            },
        )
        if not job.future.done():
            job.future.set_result(True)

    except Exception as exc:
        logging.exception("Job processing failed")
        await state.bump_stats(job.user_id, action=job.action, success=False)
        await state.add_history(
            job.user_id,
            {
                "timestamp": utc_iso_now(),
                "url": job.url,
                "title": parsed_title,
                "action": job.action,
                "status": "failed",
                "size_bytes": 0,
                "error": str(exc)[:200],
            },
        )
        await report(f"❌ Error\n<code>{html.escape(str(exc)[:250])}</code>")
        if not job.future.done():
            job.future.set_exception(exc)
    finally:
        active_jobs.discard(job.user_id)
        await rm_tree(job_dir)


async def queue_worker(application: Application, worker_id: int) -> None:
    queue: asyncio.Queue = application.bot_data["download_queue"]
    logging.info("Worker-%d started", worker_id)
    while True:
        job: DownloadJob = await queue.get()
        try:
            await process_job(application, job)
        except Exception:
            logging.exception("Worker-%d unexpected failure", worker_id)
        finally:
            queue.task_done()


# =============================================================================
# 8) MAIN ASYNC RUNNER
# =============================================================================

async def post_init(application: Application) -> None:
    await ensure_dirs()
    await cleanup_old_temp()

    state = StateManager(STATE_FILE)
    await state.load()

    session = aiohttp.ClientSession(timeout=REQUEST_TIMEOUT, headers=DEFAULT_HEADERS)
    downloader = DownloaderEngine(session)
    parser = ParserEngine(downloader, TTLCache(CACHE_TTL_SECONDS, CACHE_MAX_ITEMS))

    application.bot_data["state"] = state
    application.bot_data["session"] = session
    application.bot_data["downloader"] = downloader
    application.bot_data["parser"] = parser
    application.bot_data["rate_limiter"] = RateLimiter(RATE_LIMIT_COUNT, RATE_LIMIT_WINDOW)
    application.bot_data["download_queue"] = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    application.bot_data["active_jobs"] = set()
    application.bot_data["workers"] = [
        asyncio.create_task(queue_worker(application, i + 1))
        for i in range(WORKER_COUNT)
    ]
    logging.info("Bot initialized with %d workers", WORKER_COUNT)


async def post_shutdown(application: Application) -> None:
    workers: list[asyncio.Task] = application.bot_data.get("workers", [])
    for task in workers:
        task.cancel()
    await asyncio.gather(*workers, return_exceptions=True)

    session: aiohttp.ClientSession | None = application.bot_data.get("session")
    if session:
        await session.close()

    await cleanup_old_temp()
    logging.info("Bot shutdown complete")


def setup_logging() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
        ],
    )


def build_application() -> Application:
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    conversation = ConversationHandler(
        entry_points=[CommandHandler("start", cmd_start)],
        states={
            STATE_MENU: [
                CallbackQueryHandler(on_callback),
                MessageHandler(filters.TEXT & ~filters.COMMAND, on_text),
                CommandHandler("help", cmd_help),
                CommandHandler("cancel", cmd_cancel),
            ]
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel), CommandHandler("start", cmd_start)],
        allow_reentry=True,
    )

    app.add_handler(conversation)
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("stats", cmd_stats))

    # Owner admin controls
    app.add_handler(CommandHandler("add_user", cmd_add_user))
    app.add_handler(CommandHandler("remove_user", cmd_remove_user))
    app.add_handler(CommandHandler("users", cmd_users))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))

    return app


def main() -> None:
    setup_logging()
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set. Export BOT_TOKEN before running.")
    logging.info("Starting Universal Document Downloader Bot (owner=%s)", OWNER_ID)
    app = build_application()
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()

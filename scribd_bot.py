#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          🚀 SCRIBD PAYWALL BYPASS DOWNLOADER BOT  v3.0                      ║
║                                                                              ║
║  • Multi-engine bypass: dscrib / scribdfree / render-API / page-scrape      ║
║  • Robust page-count extraction: JSON-LD, js-page-entity, JSON.parse regex  ║
║  • Formats: PDF · TXT (OCR) · HTML · Images ZIP                             ║
║  • Owner + authorized-user system with broadcast                            ║
║  • Fully async (aiohttp + asyncio) — Termux optimised                       ║
║                                                                              ║
║  Requires: python-telegram-bot>=20, aiohttp, beautifulsoup4, Pillow, lxml   ║
║  Optional: pytesseract + Tesseract binary (for TXT/OCR format)              ║
║                                                                              ║
║  Setup:                                                                      ║
║    1.  pip install -r requirements.txt                                       ║
║    2.  Set BOT_TOKEN and OWNER_ID below                                      ║
║    3.  python scribd_bot.py                                                  ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import asyncio
import base64
import io
import json
import logging
import os
import re
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Coroutine, List, Optional, Tuple
from urllib.parse import quote, urlencode, urlparse

import aiohttp
from bs4 import BeautifulSoup
from PIL import Image
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIGURATION  — edit the two lines below, nothing else is mandatory
# ══════════════════════════════════════════════════════════════════════════════

BOT_TOKEN: str = "YOUR_BOT_TOKEN_HERE"   # from @BotFather
OWNER_ID:  int = 123456789               # your Telegram numeric ID

# ── Runtime constants ─────────────────────────────────────────────────────────
USERS_FILE   = Path("authorized_users.json")
TEMP_DIR     = Path(os.getenv("TMPDIR", "/tmp")) / "scribd_bot"
MAX_TG_BYTES = 50 * 1024 * 1024          # 50 MB Telegram hard limit
HTTP_TIMEOUT = 90                         # seconds per request
DL_TIMEOUT   = 180                        # seconds for full-file downloads
CONCURRENCY  = 6                          # simultaneous page-image fetches

TEMP_DIR.mkdir(parents=True, exist_ok=True)

# ── Browser-like headers to reduce bot-detection ─────────────────────────────
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Cache-Control": "max-age=0",
}

# ══════════════════════════════════════════════════════════════════════════════
#  📋  LOGGING
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scribd_bot.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

# ══════════════════════════════════════════════════════════════════════════════
#  🗄️  USER MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════

def _load_users() -> set[int]:
    if USERS_FILE.exists():
        try:
            return set(json.loads(USERS_FILE.read_text()))
        except Exception:
            pass
    return set()


def _save_users(users: set[int]) -> None:
    USERS_FILE.write_text(json.dumps(sorted(users)))


_authorized: set[int] = _load_users()


def is_auth(uid: int) -> bool:
    return uid == OWNER_ID or uid in _authorized


def is_owner(uid: int) -> bool:
    return uid == OWNER_ID


def add_user(uid: int) -> None:
    _authorized.add(uid)
    _save_users(_authorized)


def del_user(uid: int) -> None:
    _authorized.discard(uid)
    _save_users(_authorized)


# ══════════════════════════════════════════════════════════════════════════════
#  📊  DOCUMENT MODEL
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class ScribdDoc:
    url:        str
    doc_id:     str          = ""
    title:      str          = "Scribd Document"
    author:     str          = "Unknown"
    pages:      int          = 0
    access_key: str          = ""
    img_urls:   List[str]    = field(default_factory=list)


# ══════════════════════════════════════════════════════════════════════════════
#  🔍  PAGE-SOURCE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _extract_doc_id(url: str) -> str:
    for pat in (r"/doc(?:ument)?s?/(\d+)", r"scribd\.com/[^/]+/(\d+)"):
        m = re.search(pat, url)
        if m:
            return m.group(1)
    return ""


def _extract_page_count(html: str) -> int:
    """
    Six-strategy robust page-count extractor.

    Strategy 1 – JSON-LD structured data (numberOfPages)
    Strategy 2 – Count js-page-entity / data-page= attributes
    Strategy 3 – JSON.parse(...) blobs embedded in <script> tags
    Strategy 4 – Key: value patterns inside <script> text
    Strategy 5 – data-* attributes or meta tags
    Strategy 6 – Broad raw-HTML regex sweep
    """
    soup = BeautifulSoup(html, "lxml")

    # ── S1: JSON-LD ──────────────────────────────────────────────────────────
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            obj = json.loads(tag.string or "")
            for key in ("numberOfPages", "pageCount", "numPages"):
                if key in obj:
                    n = int(obj[key])
                    if n > 0:
                        logger.debug("page-count via JSON-LD: %d", n)
                        return n
        except Exception:
            pass

    # ── S2: js-page-entity elements ──────────────────────────────────────────
    entities = soup.find_all(attrs={"data-page": True})
    if entities:
        try:
            n = max(int(el["data-page"]) for el in entities)
            if n > 0:
                logger.debug("page-count via data-page attrs: %d", n)
                return n
        except Exception:
            pass

    # ── S3: JSON.parse("...") blobs ──────────────────────────────────────────
    for blob in re.findall(r'JSON\.parse\(["\'](.+?)["\']\)', html):
        try:
            decoded = blob.encode("utf-8").decode("unicode_escape")
            obj = json.loads(decoded)
            for key in ("page_count", "num_pages", "totalPages", "pages"):
                if key in obj:
                    n = int(obj[key])
                    if n > 0:
                        logger.debug("page-count via JSON.parse blob: %d", n)
                        return n
        except Exception:
            pass

    # ── S4: key:value patterns inside all <script> blocks ────────────────────
    _SCRIPT_PATS = [
        r'"page_count"\s*:\s*(\d+)',
        r'"num_pages"\s*:\s*(\d+)',
        r'"totalPages"\s*:\s*(\d+)',
        r'"pages"\s*:\s*(\d+)',
        r'pageCount\s*[=:]\s*(\d+)',
        r'total_pages\s*[=:]\s*(\d+)',
    ]
    for script in soup.find_all("script"):
        body = script.string or ""
        for pat in _SCRIPT_PATS:
            m = re.search(pat, body)
            if m:
                n = int(m.group(1))
                if n > 0:
                    logger.debug("page-count via script key/val: %d", n)
                    return n

    # ── S5: meta / data-* in HTML ─────────────────────────────────────────────
    for attr_pat in (r'data-page-count="(\d+)"', r'page[_-]?count["\s:=]+(\d+)'):
        m = re.search(attr_pat, html, re.IGNORECASE)
        if m:
            n = int(m.group(1))
            if n > 0:
                logger.debug("page-count via attr pattern: %d", n)
                return n

    # ── S6: broad sweep ──────────────────────────────────────────────────────
    for pat in (
        r'of\s+(\d+)\s+pages',
        r'"pages_count"\s*:\s*(\d+)',
        r'(\d+)\s+pages?',
    ):
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            n = int(m.group(1))
            if 1 < n < 100_000:
                logger.debug("page-count via broad sweep: %d", n)
                return n

    logger.warning("page-count: all strategies exhausted, returning 0")
    return 0


def _parse_metadata(html: str, url: str) -> ScribdDoc:
    doc = ScribdDoc(url=url, doc_id=_extract_doc_id(url))
    doc.pages = _extract_page_count(html)

    soup = BeautifulSoup(html, "lxml")

    # Title
    for sel in ("h1", "title"):
        tag = soup.find(sel)
        if tag:
            raw = tag.get_text(strip=True)
            doc.title = raw.split("|")[0].split("-")[0].strip() or doc.title
            break

    # Author
    for pat in (
        {"itemprop": "author"},
        {"class": re.compile(r"\bauthor\b", re.I)},
    ):
        tag = soup.find(attrs=pat)
        if tag:
            doc.author = tag.get_text(strip=True) or doc.author
            break

    # Access key (used by some image-URL patterns)
    m = re.search(r'"access_key"\s*:\s*"([^"]{6,})"', html)
    if m:
        doc.access_key = m.group(1)

    return doc


# ══════════════════════════════════════════════════════════════════════════════
#  ⬇️  HTTP HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _new_session() -> aiohttp.ClientSession:
    connector = aiohttp.TCPConnector(limit=16, ttl_dns_cache=300, ssl=False)
    return aiohttp.ClientSession(connector=connector, headers=_HEADERS)


async def _get_html(session: aiohttp.ClientSession, url: str) -> str:
    t = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    async with session.get(url, timeout=t, allow_redirects=True) as r:
        r.raise_for_status()
        return await r.text()


async def _get_bytes(
    session: aiohttp.ClientSession,
    url: str,
    timeout: int = HTTP_TIMEOUT,
    extra_headers: Optional[dict] = None,
) -> Optional[bytes]:
    t = aiohttp.ClientTimeout(total=timeout)
    hdrs = {**_HEADERS, **(extra_headers or {})}
    for attempt in range(3):
        try:
            async with session.get(url, timeout=t, headers=hdrs) as r:
                if r.status == 200:
                    return await r.read()
                logger.debug("GET %s → HTTP %d", url, r.status)
        except asyncio.TimeoutError:
            logger.warning("Timeout attempt %d for %s", attempt + 1, url)
        except Exception as e:
            logger.debug("Error attempt %d: %s", attempt + 1, e)
        if attempt < 2:
            await asyncio.sleep(1.5 * (attempt + 1))
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  🔓  BYPASS ENGINES
# ══════════════════════════════════════════════════════════════════════════════

async def _engine_dscrib(session: aiohttp.ClientSession, url: str) -> Optional[bytes]:
    """Engine A – dscrib.com public gateway."""
    try:
        api = f"https://dscrib.com/download?url={quote(url, safe='')}"
        data = await _get_bytes(session, api, timeout=DL_TIMEOUT)
        if data and len(data) > 2048 and data[:4] == b"%PDF":
            logger.info("Engine-A (dscrib) success: %d bytes", len(data))
            return data
    except Exception as e:
        logger.debug("Engine-A failed: %s", e)
    return None


async def _engine_scribdfree(session: aiohttp.ClientSession, url: str) -> Optional[bytes]:
    """Engine B – scribdfree.net API simulation."""
    try:
        t = aiohttp.ClientTimeout(total=DL_TIMEOUT)
        async with session.post(
            "https://scribdfree.net/api/download",
            json={"url": url},
            timeout=t,
        ) as r:
            if r.status == 200:
                obj = await r.json(content_type=None)
                dl = obj.get("download_url") or obj.get("url") or obj.get("link")
                if dl:
                    data = await _get_bytes(session, dl, timeout=DL_TIMEOUT)
                    if data and data[:4] == b"%PDF":
                        logger.info("Engine-B (scribdfree) success: %d bytes", len(data))
                        return data
    except Exception as e:
        logger.debug("Engine-B failed: %s", e)
    return None


async def _engine_docdownloader(session: aiohttp.ClientSession, url: str) -> Optional[bytes]:
    """Engine C – docdownloader.com async task API."""
    try:
        t30 = aiohttp.ClientTimeout(total=30)
        async with session.post(
            "https://docdownloader.com/api/fetch",
            data={"url": url},
            headers={**_HEADERS, "Referer": "https://docdownloader.com/"},
            timeout=t30,
        ) as r:
            if r.status != 200:
                return None
            obj = await r.json(content_type=None)
            task_id = obj.get("task_id") or obj.get("id")
        if not task_id:
            return None
        for _ in range(24):
            await asyncio.sleep(5)
            async with session.get(
                f"https://docdownloader.com/api/status/{task_id}",
                timeout=t30,
            ) as poll:
                if poll.status == 200:
                    res = await poll.json(content_type=None)
                    if res.get("status") == "done":
                        dl = res.get("download_url")
                        if dl:
                            data = await _get_bytes(session, dl, timeout=DL_TIMEOUT)
                            if data and data[:4] == b"%PDF":
                                logger.info("Engine-C (docdownloader) success: %d bytes", len(data))
                                return data
                            return data
    except Exception as e:
        logger.debug("Engine-C failed: %s", e)
    return None


async def _engine_render_api(
    session: aiohttp.ClientSession, doc: ScribdDoc
) -> List[str]:
    """Engine D – Scribd internal view_renders JSON (returns image URL list)."""
    if not doc.doc_id:
        return []
    urls: List[str] = []
    try:
        t = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
        async with session.get(
            f"https://www.scribd.com/view_renders/{doc.doc_id}",
            timeout=t,
        ) as r:
            if r.status == 200:
                pages = await r.json(content_type=None)
                if isinstance(pages, list):
                    for pg in pages:
                        img = pg.get("image_url") or pg.get("url")
                        if img:
                            urls.append(img)
    except Exception as e:
        logger.debug("Engine-D failed: %s", e)
    if urls:
        logger.info("Engine-D (render-API) found %d pages", len(urls))
    return urls


async def _engine_scribdassets(
    session: aiohttp.ClientSession, doc: ScribdDoc
) -> List[str]:
    """
    Engine E – probe Scribd's CDN image pattern and extrapolate full list.
    Pattern: https://html.scribdassets.com/{doc_id}/{page}-{access_key}.jpg
    """
    if not doc.doc_id:
        return []

    candidates = [
        f"https://html.scribdassets.com/{doc.doc_id}/1-{doc.access_key}.jpg",
        f"https://html.scribdassets.com/{doc.doc_id}/1.jpg",
        f"https://imgv2-1-f.scribdassets.com/img/document/{doc.doc_id}/original",
        f"https://imgv2-2-f.scribdassets.com/img/document/{doc.doc_id}/original",
    ]

    base_url: Optional[str] = None
    for cand in candidates:
        data = await _get_bytes(session, cand, timeout=15)
        if data and len(data) > 1000:
            base_url = cand
            break

    if not base_url:
        return []

    total = max(doc.pages, 1)
    # Derive a template URL by replacing the page number
    template = re.sub(r'/1[-.]', f'/{{page}}-', base_url, count=1)
    if "{page}" not in template:
        template = re.sub(r'/1(?=\.)', f'/{{page}}', base_url, count=1)

    urls = [template.format(page=i) for i in range(1, total + 1)]
    logger.info("Engine-E (scribdassets) built %d page URLs", len(urls))
    return urls


async def _engine_html_scrape(
    session: aiohttp.ClientSession, html: str
) -> List[str]:
    """Engine F – scrape visible img src tags from the Scribd reader HTML."""
    soup = BeautifulSoup(html, "lxml")
    found: List[str] = []
    for img in soup.find_all("img"):
        src = img.get("src", "") or img.get("data-src", "")
        if re.search(r"scribdassets|scribd\.com.*img", src, re.I):
            found.append(src)
    if found:
        logger.info("Engine-F (html-scrape) found %d img URLs", len(found))
    return found


# ══════════════════════════════════════════════════════════════════════════════
#  🖼️  IMAGE DOWNLOAD & FORMAT CONVERSION
# ══════════════════════════════════════════════════════════════════════════════

async def _download_all_pages(
    session: aiohttp.ClientSession,
    img_urls: List[str],
    progress: Callable[[str], Coroutine],
) -> List[bytes]:
    sem = asyncio.Semaphore(CONCURRENCY)
    done = 0
    total = len(img_urls)
    results: List[Optional[bytes]] = [None] * total

    async def fetch_one(i: int, url: str) -> None:
        nonlocal done
        async with sem:
            data = await _get_bytes(session, url, timeout=30)
            results[i] = data
            done += 1
            if done % max(1, total // 5) == 0 or done == total:
                await progress(f"⚡ Downloading pages… {done}/{total}")

    await asyncio.gather(*(fetch_one(i, u) for i, u in enumerate(img_urls)))
    good = [d for d in results if d and len(d) > 500]
    logger.info("Downloaded %d/%d page images", len(good), total)
    return good


def _images_to_pdf(imgs: List[bytes]) -> bytes:
    """Stitch page images into a single PDF using Pillow."""
    pil_imgs: List[Image.Image] = []
    for raw in imgs:
        try:
            pil_imgs.append(Image.open(io.BytesIO(raw)).convert("RGB"))
        except Exception:
            pass
    if not pil_imgs:
        raise ValueError("No valid images to compile into PDF.")
    buf = io.BytesIO()
    pil_imgs[0].save(
        buf,
        format="PDF",
        save_all=True,
        append_images=pil_imgs[1:],
        resolution=150,
    )
    return buf.getvalue()


def _images_to_zip(imgs: List[bytes], stem: str) -> bytes:
    """Pack all page images into a ZIP archive."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for i, raw in enumerate(imgs):
            ext = "jpg"
            try:
                fmt = (Image.open(io.BytesIO(raw)).format or "JPEG").lower()
                ext = "jpg" if fmt == "jpeg" else fmt
            except Exception:
                pass
            zf.writestr(f"{stem}_page_{i+1:04d}.{ext}", raw)
    return buf.getvalue()


def _images_to_html(imgs: List[bytes], title: str) -> bytes:
    """Embed all pages as base64 data-URIs inside a self-contained HTML file."""
    parts = []
    for raw in imgs:
        b64 = base64.b64encode(raw).decode()
        parts.append(
            f'<div class="page">'
            f'<img src="data:image/jpeg;base64,{b64}" />'
            f"</div>"
        )
    body = "\n".join(parts)
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>{title}</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{background:#111;font-family:sans-serif;color:#eee;padding:16px}}
    h1{{text-align:center;color:#4fc3f7;padding:12px 0 20px}}
    .page{{max-width:960px;margin:12px auto;background:#fff;
           border-radius:6px;overflow:hidden;box-shadow:0 4px 20px #0006}}
    img{{width:100%;display:block}}
  </style>
</head>
<body>
  <h1>📄 {title}</h1>
  {body}
</body>
</html>"""
    return html.encode("utf-8")


async def _images_to_txt(imgs: List[bytes]) -> bytes:
    """Extract text from page images using pytesseract OCR (optional)."""
    try:
        import pytesseract  # type: ignore

        loop = asyncio.get_event_loop()
        lines: List[str] = []

        def _ocr(raw: bytes, idx: int) -> str:
            try:
                img = Image.open(io.BytesIO(raw))
                return f"--- Page {idx} ---\n{pytesseract.image_to_string(img)}\n"
            except Exception:
                return f"--- Page {idx} [OCR failed] ---\n"

        tasks = [loop.run_in_executor(None, _ocr, raw, i + 1) for i, raw in enumerate(imgs)]
        lines = await asyncio.gather(*tasks)
        return "\n".join(lines).encode("utf-8")

    except ImportError:
        notice = (
            "OCR engine (pytesseract) is not installed.\n"
            "Install with:\n"
            "  pip install pytesseract\n"
            "  # and the Tesseract binary:\n"
            "  # Ubuntu/Debian: sudo apt install tesseract-ocr\n"
            "  # Termux:        pkg install tesseract\n"
        )
        return notice.encode("utf-8")


# ══════════════════════════════════════════════════════════════════════════════
#  🏗️  MASTER DOWNLOAD ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

ProgressCB = Callable[[str], Coroutine]


async def download_scribd(
    url: str,
    fmt: str,
    progress: ProgressCB,
) -> Tuple[bytes, str, str]:
    """
    Orchestrate the full download pipeline.

    Returns (file_bytes, filename, mime_type).
    """
    async with _new_session() as session:

        # ── 1. Fetch page source & metadata ──────────────────────────────────
        await progress("🔍 Fetching document page…")
        try:
            html = await _get_html(session, url)
        except Exception as e:
            raise RuntimeError(f"Cannot reach Scribd: {e}") from e

        doc = _parse_metadata(html, url)
        page_label = f"{doc.pages} pages" if doc.pages else "unknown pages"
        await progress(
            f"📋 *{doc.title[:60]}*\n"
            f"👤 {doc.author} · 📄 {page_label}"
        )

        if doc.pages == 0:
            doc.pages = 30   # conservative fallback for blind enumeration

        safe = re.sub(r'[^\w\s\-]', '', doc.title)[:50].strip() or "scribd_document"

        # ── 2. PDF shortcuts via bypass gateways ─────────────────────────────
        if fmt == "pdf":
            await progress("🔄 Bypassing paywall… (Engine A – dscrib)")
            pdf = await _engine_dscrib(session, url)
            if pdf:
                return pdf, f"{safe}.pdf", "application/pdf"

            await progress("🔄 Trying Engine B – scribdfree…")
            pdf = await _engine_scribdfree(session, url)
            if pdf:
                return pdf, f"{safe}.pdf", "application/pdf"

            await progress("🔄 Trying Engine C – docdownloader…")
            pdf = await _engine_docdownloader(session, url)
            if pdf:
                return pdf, f"{safe}.pdf", "application/pdf"

            # Fall through to page-image assembly below

        # ── 3. Collect page-image URLs ────────────────────────────────────────
        await progress(f"📦 Extracting {doc.pages} pages…")

        img_urls = await _engine_render_api(session, doc)

        if not img_urls:
            img_urls = await _engine_scribdassets(session, doc)

        if not img_urls:
            img_urls = await _engine_html_scrape(session, html)

        if not img_urls:
            raise RuntimeError(
                "❌ All bypass engines exhausted.\n"
                "Scribd may have tightened restrictions on this document.\n"
                "Try again later or with a different document."
            )

        # ── 4. Download all pages concurrently ────────────────────────────────
        imgs = await _download_all_pages(session, img_urls, progress)

        if not imgs:
            raise RuntimeError("❌ Downloaded 0 usable page images.")

        await progress(f"✅ Got {len(imgs)} pages — building {fmt.upper()}…")

        # ── 5. Convert to the requested format ────────────────────────────────
        if fmt == "pdf":
            data = _images_to_pdf(imgs)
            return data, f"{safe}.pdf", "application/pdf"

        elif fmt == "txt":
            await progress("📝 Running OCR on pages…")
            data = await _images_to_txt(imgs)
            return data, f"{safe}.txt", "text/plain"

        elif fmt == "html":
            data = _images_to_html(imgs, doc.title)
            return data, f"{safe}.html", "text/html"

        elif fmt == "images":
            await progress("📤 Zipping page images…")
            data = _images_to_zip(imgs, safe)
            return data, f"{safe}_images.zip", "application/zip"

        else:
            raise ValueError(f"Unknown format: {fmt!r}")


# ══════════════════════════════════════════════════════════════════════════════
#  🤖  TELEGRAM BOT HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

# Map user_id → pending Scribd URL
_pending: dict[int, str] = {}


# ── /start ────────────────────────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    name = update.effective_user.first_name or "there"
    if not is_auth(uid):
        await update.message.reply_text(
            f"❌ *Access Denied*\n\nHi {name}, you are not authorised.\n"
            f"Your ID: `{uid}`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    admin_block = (
        "\n\n👑 *Admin Commands* _(owner only)_\n"
        "  `/add_user [ID …]` — grant access\n"
        "  `/remove_user [ID …]` — revoke access\n"
        "  `/list_users` — show authorised list\n"
        "  `/broadcast [msg]` — message all users"
    ) if is_owner(uid) else ""

    await update.message.reply_text(
        f"🚀 *Scribd Paywall Bypass Bot*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Welcome, *{name}*! Send me any Scribd document URL\n"
        f"and choose a format to download.\n\n"
        f"📥 *Formats available*\n"
        f"  📄 PDF — full compiled document\n"
        f"  📝 TXT — OCR-extracted text\n"
        f"  🌐 HTML — self-contained web page\n"
        f"  🖼️ Images — ZIP of all page images"
        f"{admin_block}",
        parse_mode=ParseMode.MARKDOWN,
    )


# ── /help ─────────────────────────────────────────────────────────────────────
async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await cmd_start(update, ctx)


# ── /status ───────────────────────────────────────────────────────────────────
async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_auth(update.effective_user.id):
        return
    total = len(_authorized) + 1
    await update.message.reply_text(
        f"⚡ *Bot Status: Online*\n"
        f"👥 Authorised users: {total}\n"
        f"🔧 Engines: dscrib · scribdfree · docdownloader · render-API · CDN\n"
        f"📦 Telegram limit: 50 MB",
        parse_mode=ParseMode.MARKDOWN,
    )


# ── /add_user [ID …] ─────────────────────────────────────────────────────────
async def cmd_add_user(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("❌ Owner-only command.")
        return
    if not ctx.args:
        await update.message.reply_text("Usage: `/add_user [ID1] [ID2] …`", parse_mode=ParseMode.MARKDOWN)
        return
    added, bad = [], []
    for arg in ctx.args:
        try:
            add_user(int(arg))
            added.append(arg)
        except ValueError:
            bad.append(arg)
    parts = [f"✅ Added: {', '.join(added)}"] if added else []
    if bad:
        parts.append(f"⚠️ Invalid: {', '.join(bad)}")
    await update.message.reply_text("\n".join(parts) or "Nothing changed.")


# ── /remove_user [ID …] ──────────────────────────────────────────────────────
async def cmd_remove_user(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("❌ Owner-only command.")
        return
    if not ctx.args:
        await update.message.reply_text("Usage: `/remove_user [ID1] [ID2] …`", parse_mode=ParseMode.MARKDOWN)
        return
    removed, bad = [], []
    for arg in ctx.args:
        try:
            del_user(int(arg))
            removed.append(arg)
        except ValueError:
            bad.append(arg)
    parts = [f"✅ Removed: {', '.join(removed)}"] if removed else []
    if bad:
        parts.append(f"⚠️ Invalid: {', '.join(bad)}")
    await update.message.reply_text("\n".join(parts) or "Nothing changed.")


# ── /list_users ───────────────────────────────────────────────────────────────
async def cmd_list_users(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("❌ Owner-only command.")
        return
    lines = [f"• `{uid}`" for uid in sorted(_authorized)]
    body = "\n".join(lines) if lines else "_none_"
    await update.message.reply_text(
        f"👥 *Authorised Users*\n\n"
        f"👑 Owner: `{OWNER_ID}`\n"
        f"👤 Added:\n{body}",
        parse_mode=ParseMode.MARKDOWN,
    )


# ── /broadcast [msg] ─────────────────────────────────────────────────────────
async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("❌ Owner-only command.")
        return
    if not ctx.args:
        await update.message.reply_text("Usage: `/broadcast [message]`", parse_mode=ParseMode.MARKDOWN)
        return
    msg = " ".join(ctx.args)
    recipients = _authorized | {OWNER_ID}
    ok = fail = 0
    for uid in recipients:
        try:
            await ctx.bot.send_message(uid, f"📢 *Broadcast:*\n\n{msg}", parse_mode=ParseMode.MARKDOWN)
            ok += 1
        except Exception:
            fail += 1
    await update.message.reply_text(f"✅ Sent: {ok} | ❌ Failed: {fail}")


# ── Text message handler (URL intake) ─────────────────────────────────────────
async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    if not is_auth(uid):
        await update.message.reply_text("❌ You are not authorised to use this bot.")
        return

    text = (update.message.text or "").strip()
    if "scribd.com" not in text.lower():
        await update.message.reply_text(
            "❌ Please send a Scribd document URL.\n"
            "Example: `https://www.scribd.com/document/123456/Title`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # Normalise URL
    url = text.split("?")[0].split("#")[0]
    if not re.search(r"scribd\.com/(?:doc(?:ument)?|embeds?)/\d+", url, re.I):
        await update.message.reply_text("❌ URL doesn't look like a Scribd document link.")
        return

    _pending[uid] = url
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📄 PDF",        callback_data="fmt:pdf"),
            InlineKeyboardButton("📝 TXT (OCR)",  callback_data="fmt:txt"),
        ],
        [
            InlineKeyboardButton("🌐 HTML",        callback_data="fmt:html"),
            InlineKeyboardButton("🖼️ Images ZIP",  callback_data="fmt:images"),
        ],
    ])
    await update.message.reply_text(
        f"🚀 *Scribd URL detected!*\n\n"
        f"`{url[:80]}`\n\n"
        f"Choose output format:",
        reply_markup=keyboard,
        parse_mode=ParseMode.MARKDOWN,
    )


# ── Inline-button callback (download trigger) ─────────────────────────────────
async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    uid = query.from_user.id
    if not is_auth(uid):
        await query.edit_message_text("❌ Access denied.")
        return

    data = query.data or ""
    if not data.startswith("fmt:"):
        return

    fmt = data.split(":", 1)[1]
    url = _pending.get(uid)
    if not url:
        await query.edit_message_text("❌ Session expired — please send the URL again.")
        return

    # Live status message
    status = await query.edit_message_text(
        "⚡ *Starting…*\n🔄 Bypassing paywall…",
        parse_mode=ParseMode.MARKDOWN,
    )

    async def progress(text: str) -> None:
        try:
            await status.edit_text(text, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass

    try:
        file_bytes, filename, mime = await download_scribd(url, fmt, progress)

        size_mb = len(file_bytes) / 1_048_576
        if len(file_bytes) > MAX_TG_BYTES:
            await progress(
                f"❌ File is {size_mb:.1f} MB — exceeds Telegram's 50 MB limit.\n"
                "Try a different format or a shorter document."
            )
            return

        await progress(f"📤 Sending `{filename}` ({size_mb:.1f} MB)…")

        bio = io.BytesIO(file_bytes)
        bio.name = filename

        await ctx.bot.send_document(
            chat_id=query.message.chat_id,
            document=bio,
            filename=filename,
            caption=(
                f"✅ *Download complete!*\n"
                f"📁 `{filename}`\n"
                f"📦 {size_mb:.2f} MB"
            ),
            parse_mode=ParseMode.MARKDOWN,
        )
        await status.edit_text(
            f"✅ *Sent!* `{filename}`",
            parse_mode=ParseMode.MARKDOWN,
        )

    except Exception as exc:
        logger.error("Download failed for %s: %s", url, exc, exc_info=True)
        await progress(f"❌ *Error:*\n`{str(exc)[:400]}`")

    finally:
        _pending.pop(uid, None)


# ── Global error handler ──────────────────────────────────────────────────────
async def _error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled exception: %s", ctx.error, exc_info=ctx.error)


# ══════════════════════════════════════════════════════════════════════════════
#  🚀  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print(
            "\n" + "═" * 60 + "\n"
            "❌  BOT_TOKEN is not configured!\n\n"
            "1. Talk to @BotFather on Telegram → /newbot\n"
            "2. Copy the token\n"
            "3. Paste it into BOT_TOKEN at the top of this file\n"
            "4. Set your numeric Telegram ID in OWNER_ID\n"
            "   (find it at @userinfobot)\n"
            + "═" * 60 + "\n"
        )
        raise SystemExit(1)

    print(
        "\n" + "═" * 60 + "\n"
        "🚀  SCRIBD PAYWALL BYPASS BOT  v3.0\n"
        f"👑  Owner ID  : {OWNER_ID}\n"
        f"👥  Auth users: {len(_authorized)}\n"
        f"📁  Users file: {USERS_FILE}\n"
        f"🌡️   Temp dir  : {TEMP_DIR}\n"
        + "═" * 60
    )

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",        cmd_start))
    app.add_handler(CommandHandler("help",         cmd_help))
    app.add_handler(CommandHandler("status",       cmd_status))
    app.add_handler(CommandHandler("add_user",     cmd_add_user))
    app.add_handler(CommandHandler("remove_user",  cmd_remove_user))
    app.add_handler(CommandHandler("list_users",   cmd_list_users))
    app.add_handler(CommandHandler("broadcast",    cmd_broadcast))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(handle_callback))

    app.add_error_handler(_error_handler)

    print("✅  Bot is running — press Ctrl+C to stop\n")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

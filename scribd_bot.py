#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scribd Paywall Bypass Downloader — Telegram Bot (v3.0)
======================================================
High-performance async Telegram bot that bypasses the Scribd paywall
to download documents as PDF, TXT, HTML, or high-res Images (ZIP).

Features:
  - Robust page-count extraction via regex + BeautifulSoup (6 strategies)
  - Multi-engine bypass: direct image scrape → third-party gateways
  - High-res original image download → PDF compile or ZIP archive
  - Owner / user authorization system with persistent JSON storage
  - Real-time progress bars inside Telegram messages
  - Fully async (aiohttp + asyncio), optimized for Termux

Requirements:
    pip install python-telegram-bot[ext] aiohttp beautifulsoup4 Pillow lxml
"""

import asyncio
import html as html_mod
import io
import json
import logging
import os
import re
import zipfile
from functools import wraps
from pathlib import Path

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
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

BOT_TOKEN = os.getenv(
    "BOT_TOKEN", "8674547740:AAHP3wLLo1-0CRLkY7F4bc6xL0JcqPEqrQU"
)
OWNER_ID = int(os.getenv("OWNER_ID", "6512242172"))

DATA_DIR = Path("bot_data")
USERS_FILE = DATA_DIR / "users.json"

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=120)

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

SCRIBD_URL_RE = re.compile(
    r"https?://(?:www\.)?scribd\.com/"
    r"(?:document|doc|book|read|presentation|audiobook)/"
    r"(\d+)"
)

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
logger = logging.getLogger("ScribdBot")


# ═══════════════════════════════════════════════════════════════════════════
# USER MANAGER (persistent JSON)
# ═══════════════════════════════════════════════════════════════════════════


class UserManager:
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
        USERS_FILE.write_text(
            json.dumps({"users": sorted(self._users)}, indent=2)
        )

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


user_mgr = UserManager()


# ═══════════════════════════════════════════════════════════════════════════
# SCRIBD SCRAPER — page-count, images, bypass
# ═══════════════════════════════════════════════════════════════════════════


class ScribdScraper:
    BYPASS_GATEWAYS = [
        {"name": "DLScrib", "url": "https://dlscrib.com/fetch", "method": "post"},
        {
            "name": "DocDownloader",
            "url": "https://www.docdownloader.com/api/scribd",
            "method": "post",
        },
        {
            "name": "ScribFree",
            "url": "https://scribfree.com/download",
            "method": "get",
        },
    ]

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session

    # ── HTTP helpers ──────────────────────────────────────────────────────

    async def _get_text(self, url: str, extra_headers: dict | None = None) -> str:
        hdrs = {**BROWSER_HEADERS, **(extra_headers or {})}
        async with self.session.get(
            url,
            headers=hdrs,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
            ssl=False,
        ) as r:
            r.raise_for_status()
            return await r.text()

    async def _get_bytes(self, url: str, extra_headers: dict | None = None) -> bytes:
        hdrs = {**BROWSER_HEADERS, **(extra_headers or {})}
        async with self.session.get(
            url,
            headers=hdrs,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
            ssl=False,
        ) as r:
            r.raise_for_status()
            return await r.read()

    # ── Metadata ──────────────────────────────────────────────────────────

    async def get_metadata(self, doc_id: str) -> dict:
        url = f"https://www.scribd.com/document/{doc_id}"
        raw_html = await self._get_text(url)
        soup = BeautifulSoup(raw_html, HTML_PARSER)

        meta: dict = {
            "doc_id": doc_id,
            "url": url,
            "title": "Unknown Document",
            "author": "Unknown",
            "description": "",
            "page_count": 0,
            "_html": raw_html,
            "_soup": soup,
        }

        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content"):
            meta["title"] = og_title["content"]
        elif soup.title and soup.title.string:
            meta["title"] = soup.title.string.strip()

        for sel in [
            soup.find("meta", attrs={"name": "author"}),
            soup.select_one("a.author-name, span.author, a[href*='/user/']"),
        ]:
            if sel:
                val = sel.get("content") or sel.get_text(strip=True)
                if val:
                    meta["author"] = val
                    break

        og_desc = soup.find("meta", property="og:description")
        if og_desc and og_desc.get("content"):
            meta["description"] = og_desc["content"][:300]

        meta["page_count"] = self._extract_page_count(raw_html, soup)
        return meta

    # ── Page count (6 strategies) ─────────────────────────────────────────

    def _extract_page_count(self, text: str, soup: BeautifulSoup) -> int:
        for fn in (
            self._pc_json_parse,
            self._pc_json_fields,
            self._pc_page_entities,
            self._pc_meta_tags,
            self._pc_ld_json,
            self._pc_text_patterns,
        ):
            try:
                n = fn(text, soup)
                if n and n > 0:
                    logger.info("Page count %d via %s", n, fn.__name__)
                    return n
            except Exception:
                pass
        return 0

    @staticmethod
    def _pc_json_parse(text: str, _s: BeautifulSoup) -> int:
        for m in re.finditer(r"JSON\.parse\(['\"](.+?)['\"]\)", text):
            try:
                raw = m.group(1).encode().decode("unicode_escape")
                data = json.loads(raw)
                if isinstance(data, dict):
                    for key in (
                        "page_count", "pageCount", "pages",
                        "num_pages", "totalPages",
                    ):
                        if key in data and int(data[key]) > 0:
                            return int(data[key])
                    for v in data.values():
                        if isinstance(v, dict):
                            for key in ("page_count", "pageCount"):
                                if key in v:
                                    return int(v[key])
            except Exception:
                continue
        return 0

    @staticmethod
    def _pc_json_fields(text: str, _s: BeautifulSoup) -> int:
        for pat in (
            r'"page_count"\s*:\s*(\d+)',
            r'"pageCount"\s*:\s*(\d+)',
            r'"num_pages"\s*:\s*(\d+)',
            r'"total_pages"\s*:\s*(\d+)',
            r'"totalPages"\s*:\s*(\d+)',
            r"'page_count'\s*:\s*(\d+)",
            r'"contentPages"\s*:\s*(\d+)',
        ):
            m = re.search(pat, text)
            if m:
                return int(m.group(1))
        return 0

    @staticmethod
    def _pc_page_entities(text: str, soup: BeautifulSoup) -> int:
        elems = soup.find_all(
            class_=re.compile(r"js.page.entity|page_entity|page-entity", re.I)
        )
        if elems:
            return len(elems)
        divs = soup.find_all("div", attrs={"data-page": True})
        if divs:
            return len(divs)
        nums = re.findall(r'data-page-number="(\d+)"', text)
        if nums:
            return max(int(n) for n in nums)
        return 0

    @staticmethod
    def _pc_meta_tags(_t: str, soup: BeautifulSoup) -> int:
        for tag in soup.find_all("meta"):
            name = (tag.get("name", "") or tag.get("property", "")).lower()
            if "page" in name and "count" in name:
                try:
                    return int(tag["content"])
                except (ValueError, KeyError):
                    pass
        return 0

    @staticmethod
    def _pc_ld_json(_t: str, soup: BeautifulSoup) -> int:
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if isinstance(item, dict):
                        for key in ("numberOfPages", "pageCount", "page_count"):
                            if key in item:
                                return int(item[key])
            except Exception:
                continue
        return 0

    @staticmethod
    def _pc_text_patterns(text: str, _s: BeautifulSoup) -> int:
        for pat in (
            r"(\d+)\s+(?:pages?|Pages?|PAGES)",
            r"(?:pages?|Pages?)\s*:\s*(\d+)",
            r"of\s+(\d+)\s+pages",
        ):
            m = re.search(pat, text)
            if m:
                val = int(m.group(1))
                if 1 < val < 10_000:
                    return val
        return 0

    # ── Image URL extraction ──────────────────────────────────────────────

    async def extract_image_urls(self, meta: dict) -> list[str]:
        raw = meta["_html"]
        soup = meta["_soup"]
        doc_id = meta["doc_id"]
        urls: list[str] = []

        cdn_pats = [
            re.compile(
                r"(https?://html\d*\.scribdassets\.com/"
                r'[a-zA-Z0-9_-]+/images/[^"\'\\\s<>]+\.'
                r"(?:jpg|png|webp))",
                re.I,
            ),
            re.compile(
                r"(https?://imgv2-\d+-shm-\w+\.scribdassets\.com/"
                r'img/[^"\'\\\s<>]+)',
                re.I,
            ),
            re.compile(
                r"(https?://[^\s\"'<>]*scribdassets\.com"
                r'[^\s"\'<>]*\.(?:jpg|png|webp))',
                re.I,
            ),
        ]
        seen: set[str] = set()
        for pat in cdn_pats:
            for m in pat.finditer(raw):
                u = m.group(1).split("?")[0]
                if u not in seen:
                    seen.add(u)
                    urls.append(u)

        if not urls:
            urls = self._images_from_json(raw)

        if not urls:
            urls = await self._images_from_read_api(doc_id, raw)

        if not urls and meta["page_count"] > 0:
            urls = await self._probe_cdn(raw, meta["page_count"])

        urls = list(dict.fromkeys(self._upscale(u) for u in urls))
        return urls

    @staticmethod
    def _upscale(url: str) -> str:
        url = re.sub(r"/\d+x\d+/", "/original/", url)
        url = re.sub(r"[?&]w=\d+", "", url)
        url = re.sub(r"[?&]h=\d+", "", url)
        url = re.sub(r"-\d+x\d+\.", ".", url)
        return url

    @staticmethod
    def _images_from_json(text: str) -> list[str]:
        urls: list[str] = []

        def _walk(obj: object, depth: int = 0) -> None:
            if depth > 5:
                return
            if isinstance(obj, dict):
                for v in obj.values():
                    if (
                        isinstance(v, str)
                        and "scribdassets.com" in v
                        and any(v.lower().endswith(e) for e in (".jpg", ".png", ".webp"))
                    ):
                        urls.append(v.split("?")[0])
                    else:
                        _walk(v, depth + 1)
            elif isinstance(obj, list):
                for item in obj:
                    _walk(item, depth + 1)

        for block in re.findall(r"\{[^{}]{100,}\}", text)[:50]:
            try:
                _walk(json.loads(block))
            except Exception:
                pass
        return urls

    async def _images_from_read_api(self, doc_id: str, text: str) -> list[str]:
        ak = re.search(r'"access_key"\s*:\s*"([^"]+)"', text)
        if not ak:
            return []
        api = (
            f"https://www.scribd.com/doc-page/read-data"
            f"?doc_id={doc_id}&access_key={ak.group(1)}"
        )
        urls: list[str] = []
        try:
            resp = await self._get_text(
                api,
                extra_headers={
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": f"https://www.scribd.com/document/{doc_id}",
                },
            )
            self._images_from_json.__func__(resp)  # type: ignore[attr-defined]
        except Exception:
            pass
        return urls

    async def _probe_cdn(self, text: str, page_count: int) -> list[str]:
        base_m = re.search(r"scribdassets\.com/([a-zA-Z0-9_-]{10,})/", text)
        if not base_m:
            return []
        base = f"https://html.scribdassets.com/{base_m.group(1)}/images"
        templates = ["{base}/{num:04d}.jpg", "{base}/page-{num}.jpg"]
        for tmpl in templates:
            test = tmpl.format(base=base, num=1)
            try:
                async with self.session.head(
                    test,
                    headers=BROWSER_HEADERS,
                    timeout=aiohttp.ClientTimeout(total=10),
                    allow_redirects=True,
                    ssl=False,
                ) as r:
                    if r.status == 200:
                        return [
                            tmpl.format(base=base, num=p)
                            for p in range(1, page_count + 1)
                        ]
            except Exception:
                pass
        return []

    # ── Download page images concurrently ─────────────────────────────────

    async def download_images(
        self, urls: list[str], progress_cb=None
    ) -> list[bytes]:
        sem = asyncio.Semaphore(6)
        results: list[tuple[int, bytes | None]] = []

        async def _grab(idx: int, url: str) -> None:
            async with sem:
                for attempt in range(3):
                    try:
                        data = await self._get_bytes(
                            url, extra_headers={"Referer": "https://www.scribd.com/"}
                        )
                        if len(data) > 500:
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

    # ── Conversions ───────────────────────────────────────────────────────

    @staticmethod
    def images_to_pdf(blobs: list[bytes]) -> bytes:
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
            raise ValueError("No valid images to create PDF")
        buf = io.BytesIO()
        first, *rest = imgs
        first.save(buf, "PDF", save_all=True, append_images=rest, resolution=150)
        return buf.getvalue()

    @staticmethod
    def images_to_zip(blobs: list[bytes], title: str = "document") -> bytes:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for idx, data in enumerate(blobs, 1):
                ext = "png" if data[:4] == b"\x89PNG" else "jpg"
                zf.writestr(f"{title}_page_{idx:04d}.{ext}", data)
        return buf.getvalue()

    # ── Text / HTML extraction ────────────────────────────────────────────

    async def extract_text(self, meta: dict) -> str:
        soup: BeautifulSoup = meta["_soup"]
        parts: list[str] = []

        for cls_re in (
            r"text_layer|text-layer|page_text",
            r"page_content|doc_page|reader_page|page_inner",
        ):
            for el in soup.find_all(class_=re.compile(cls_re, re.I)):
                t = el.get_text("\n", strip=True)
                if t and len(t) > 20:
                    parts.append(t)
            if parts:
                break

        if not parts:
            parts = self._text_from_json(meta["_html"])

        if not parts:
            for sel in ("article", "main", ".document_content", "#document_column"):
                el = soup.select_one(sel)
                if el:
                    t = el.get_text("\n", strip=True)
                    if len(t) > 50:
                        parts.append(t)
                        break

        return "\n\n--- Page Break ---\n\n".join(parts) if parts else ""

    @staticmethod
    def _text_from_json(text: str) -> list[str]:
        out: list[str] = []
        for pat in (
            r'"text"\s*:\s*"((?:[^"\\]|\\.)*)"',
            r'"page_text"\s*:\s*"((?:[^"\\]|\\.)*)"',
            r'"content"\s*:\s*"((?:[^"\\]|\\.)*)"',
        ):
            for m in re.finditer(pat, text):
                try:
                    decoded = m.group(1).encode().decode("unicode_escape")
                    cleaned = re.sub(r"<[^>]+>", "", decoded).strip()
                    if len(cleaned) > 30:
                        out.append(cleaned)
                except Exception:
                    pass
        return out

    async def build_html(self, meta: dict) -> str:
        title = html_mod.escape(meta["title"])
        author = html_mod.escape(meta["author"])
        raw_text = await self.extract_text(meta)
        body = ""
        if raw_text:
            for block in raw_text.split("\n\n--- Page Break ---\n\n"):
                body += "<div class='page'>\n"
                for line in block.strip().splitlines():
                    if line.strip():
                        body += f"  <p>{html_mod.escape(line.strip())}</p>\n"
                body += "</div>\n<hr>\n"
        else:
            body = (
                "<p><em>Text could not be extracted from this document. "
                "Try the PDF or Images format instead.</em></p>"
            )

        return (
            "<!DOCTYPE html>\n<html lang='en'>\n<head>\n"
            "  <meta charset='UTF-8'>\n"
            "  <meta name='viewport' content='width=device-width,initial-scale=1'>\n"
            f"  <title>{title}</title>\n"
            "  <style>\n"
            "    body{font-family:Georgia,serif;max-width:800px;margin:2rem auto;"
            "padding:0 1rem;line-height:1.8;color:#222}\n"
            "    h1{font-size:1.8rem;border-bottom:2px solid #333;padding-bottom:.5rem}\n"
            "    .meta{color:#666;font-style:italic;margin-bottom:2rem}\n"
            "    .page{margin:1.5rem 0} hr{border:none;border-top:1px solid #ddd;"
            "margin:2rem 0}\n"
            "    p{margin:.5rem 0;text-align:justify}\n"
            "  </style>\n</head>\n<body>\n"
            f"  <h1>{title}</h1>\n"
            f"  <div class='meta'>Author: {author} | "
            f"Pages: {meta['page_count']}</div>\n"
            f"  {body}\n"
            "</body>\n</html>"
        )

    # ── Third-party bypass engines ────────────────────────────────────────

    async def bypass_download_pdf(self, doc_id: str, doc_url: str) -> bytes | None:
        for gw in self.BYPASS_GATEWAYS:
            try:
                logger.info("Trying bypass via %s …", gw["name"])
                pdf = await self._try_gateway(gw, doc_id, doc_url)
                if pdf and len(pdf) > 5000:
                    logger.info("Bypass via %s succeeded (%d bytes)", gw["name"], len(pdf))
                    return pdf
            except Exception as exc:
                logger.warning("Bypass %s failed: %s", gw["name"], exc)
        return None

    async def _try_gateway(
        self, gw: dict, doc_id: str, doc_url: str
    ) -> bytes | None:
        payload = {"url": doc_url, "doc_id": doc_id}
        kw: dict = dict(
            headers={**BROWSER_HEADERS, "Referer": gw["url"]},
            timeout=aiohttp.ClientTimeout(total=60),
            ssl=False,
        )
        if gw["method"] == "post":
            req = self.session.post(gw["url"], data=payload, **kw)
        else:
            req = self.session.get(gw["url"], params={"url": doc_url}, **kw)

        async with req as r:
            ct = r.headers.get("Content-Type", "")
            if "pdf" in ct or "octet-stream" in ct:
                return await r.read()
            body = await r.text()
            link = self._find_download_link(body)
            if link:
                return await self._get_bytes(link)
        return None

    @staticmethod
    def _find_download_link(text: str) -> str | None:
        try:
            data = json.loads(text)
            for k in ("download_url", "downloadUrl", "url", "link", "file"):
                if k in data:
                    return str(data[k])
        except Exception:
            pass
        m = re.search(r'href=["\']([^"\']+\.pdf[^"\']*)["\']', text)
        if m:
            return m.group(1)
        m = re.search(r'(https?://[^\s"\'<>]+\.pdf[^\s"\'<>]*)', text)
        return m.group(1) if m else None


# ═══════════════════════════════════════════════════════════════════════════
# DECORATORS
# ═══════════════════════════════════════════════════════════════════════════


def auth_required(func):
    @wraps(func)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not user_mgr.is_authorized(uid):
            await update.message.reply_text(
                "❌ <b>Access Denied</b>\n\n"
                "You are not authorized. Contact the bot owner.",
                parse_mode=ParseMode.HTML,
            )
            return
        return await func(update, ctx)

    return wrapper


def owner_only(func):
    @wraps(func)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != OWNER_ID:
            await update.message.reply_text("❌ Owner-only command.")
            return
        return await func(update, ctx)

    return wrapper


# ═══════════════════════════════════════════════════════════════════════════
# DOWNLOAD LOGIC (per-format)
# ═══════════════════════════════════════════════════════════════════════════

_safe_fn = re.compile(r"[^\w\s-]")
_pending: dict[int, dict] = {}


def _filename(title: str) -> str:
    return _safe_fn.sub("", title)[:60].strip() or "document"


def _progress_bar(pct: int) -> str:
    filled = pct // 10
    return "█" * filled + "░" * (10 - filled)


async def _send_pdf(query, scraper: ScribdScraper, meta: dict):
    doc_id, doc_url = meta["doc_id"], meta["url"]

    await query.edit_message_text(
        "🔄 <b>Bypassing Paywall…</b>\n\n⚡ Trying bypass engines…",
        parse_mode=ParseMode.HTML,
    )
    pdf = await scraper.bypass_download_pdf(doc_id, doc_url)

    if not pdf:
        await query.edit_message_text(
            "🔄 <b>Direct bypass unavailable</b>\n\n"
            "📦 Falling back to page-image extraction…",
            parse_mode=ParseMode.HTML,
        )
        urls = await scraper.extract_image_urls(meta)
        if not urls:
            await query.edit_message_text(
                "❌ <b>Could not extract pages</b>\n\n"
                "No downloadable page images found. Try TXT or HTML.",
                parse_mode=ParseMode.HTML,
            )
            return

        total = len(urls)

        async def _prog(cur: int, tot: int):
            if cur % max(1, tot // 5) == 0 or cur == tot:
                pct = int(cur / tot * 100)
                try:
                    await query.edit_message_text(
                        f"📦 <b>Extracting {tot} Pages…</b>\n\n"
                        f"[{_progress_bar(pct)}] {pct}%\n"
                        f"📄 Page {cur}/{tot}",
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass

        blobs = await scraper.download_images(urls, progress_cb=_prog)
        if not blobs:
            await query.edit_message_text(
                "❌ <b>Image download failed</b>", parse_mode=ParseMode.HTML
            )
            return

        await query.edit_message_text(
            f"📦 <b>Converting {len(blobs)} pages to PDF…</b>",
            parse_mode=ParseMode.HTML,
        )
        pdf = scraper.images_to_pdf(blobs)

    name = _filename(meta["title"])
    size_mb = len(pdf) / 1024 / 1024
    await query.edit_message_text(
        f"📤 <b>Sending PDF…</b>  ({size_mb:.1f} MB)", parse_mode=ParseMode.HTML
    )
    await query.message.reply_document(
        document=io.BytesIO(pdf),
        filename=f"{name}.pdf",
        caption=(
            f"✅ <b>{html_mod.escape(meta['title'])}</b>\n"
            f"✍️ {html_mod.escape(meta['author'])} · "
            f"📄 {meta['page_count']} pages · 📕 PDF\n\n"
            "⚡ <i>Scribd Bypass Bot</i>"
        ),
        parse_mode=ParseMode.HTML,
    )
    await query.edit_message_text(
        f"✅ <b>PDF sent!</b>\n📖 {html_mod.escape(meta['title'])}",
        parse_mode=ParseMode.HTML,
    )


async def _send_txt(query, scraper: ScribdScraper, meta: dict):
    await query.edit_message_text(
        "📦 <b>Extracting text…</b>", parse_mode=ParseMode.HTML
    )
    text = await scraper.extract_text(meta)
    if not text or len(text.strip()) < 50:
        await query.edit_message_text(
            "❌ <b>Text extraction failed</b>\n\n"
            "This may be a scanned/image-only document.\n"
            "Try PDF or Images format.",
            parse_mode=ParseMode.HTML,
        )
        return

    header = (
        f"{'=' * 60}\n{meta['title']}\n"
        f"Author: {meta['author']}\nPages: {meta['page_count']}\n{'=' * 60}\n\n"
    )
    full = header + text
    name = _filename(meta["title"])
    await query.message.reply_document(
        document=io.BytesIO(full.encode()),
        filename=f"{name}.txt",
        caption=(
            f"✅ <b>{html_mod.escape(meta['title'])}</b>\n"
            f"📝 TXT · {len(full):,} chars\n\n"
            "⚡ <i>Scribd Bypass Bot</i>"
        ),
        parse_mode=ParseMode.HTML,
    )
    await query.edit_message_text(
        f"✅ <b>Text sent!</b>\n📝 {len(full):,} characters",
        parse_mode=ParseMode.HTML,
    )


async def _send_html(query, scraper: ScribdScraper, meta: dict):
    await query.edit_message_text(
        "📦 <b>Building HTML…</b>", parse_mode=ParseMode.HTML
    )
    content = await scraper.build_html(meta)
    name = _filename(meta["title"])
    await query.message.reply_document(
        document=io.BytesIO(content.encode()),
        filename=f"{name}.html",
        caption=(
            f"✅ <b>{html_mod.escape(meta['title'])}</b>\n"
            f"🌐 HTML · {len(content) / 1024:.1f} KB\n\n"
            "⚡ <i>Scribd Bypass Bot</i>"
        ),
        parse_mode=ParseMode.HTML,
    )
    await query.edit_message_text(
        f"✅ <b>HTML sent!</b>\n🌐 {html_mod.escape(meta['title'])}",
        parse_mode=ParseMode.HTML,
    )


async def _send_images(query, scraper: ScribdScraper, meta: dict):
    await query.edit_message_text(
        "🔄 <b>Extracting image URLs…</b>", parse_mode=ParseMode.HTML
    )
    urls = await scraper.extract_image_urls(meta)
    if not urls:
        await query.edit_message_text(
            "❌ <b>No page images found</b>\nTry PDF or TXT.",
            parse_mode=ParseMode.HTML,
        )
        return

    total = len(urls)

    async def _prog(cur: int, tot: int):
        if cur % max(1, tot // 5) == 0 or cur == tot:
            pct = int(cur / tot * 100)
            try:
                await query.edit_message_text(
                    f"📦 <b>Downloading High-Res Images…</b>\n\n"
                    f"[{_progress_bar(pct)}] {pct}%\n"
                    f"🖼 Image {cur}/{tot}",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass

    blobs = await scraper.download_images(urls, progress_cb=_prog)
    if not blobs:
        await query.edit_message_text(
            "❌ <b>Image download failed</b>", parse_mode=ParseMode.HTML
        )
        return

    await query.edit_message_text(
        f"📤 <b>Packaging {len(blobs)} images into ZIP…</b>",
        parse_mode=ParseMode.HTML,
    )
    name = _filename(meta["title"])
    zipdata = scraper.images_to_zip(blobs, title=name)
    await query.message.reply_document(
        document=io.BytesIO(zipdata),
        filename=f"{name}_images.zip",
        caption=(
            f"✅ <b>{html_mod.escape(meta['title'])}</b>\n"
            f"🖼 {len(blobs)} high-res images · "
            f"{len(zipdata) / 1024 / 1024:.1f} MB\n\n"
            "⚡ <i>Scribd Bypass Bot</i>"
        ),
        parse_mode=ParseMode.HTML,
    )
    await query.edit_message_text(
        f"✅ <b>Images ZIP sent!</b>\n🖼 {len(blobs)} pages",
        parse_mode=ParseMode.HTML,
    )


# ═══════════════════════════════════════════════════════════════════════════
# TELEGRAM COMMAND & MESSAGE HANDLERS
# ═══════════════════════════════════════════════════════════════════════════


async def cmd_start(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await update.message.reply_text(
        f"🚀 <b>Scribd Paywall Bypass Downloader</b>\n\n"
        f"Welcome, {html_mod.escape(u.first_name)}!\n\n"
        f"📥 <b>How to use:</b>\n"
        f"Send any Scribd document URL and I'll bypass the paywall.\n\n"
        f"⚡ <b>Formats:</b>\n"
        f"  📕 PDF — full document\n"
        f"  📝 TXT — extracted plain text\n"
        f"  🌐 HTML — formatted web page\n"
        f"  🖼 Images — high-res pages (ZIP)\n\n"
        f"📋 <b>Commands:</b>\n"
        f"  /start  — this message\n"
        f"  /help   — detailed help\n"
        f"  /status — bot status\n\n"
        f"<i>Your ID: <code>{u.id}</code></i>",
        parse_mode=ParseMode.HTML,
    )


async def cmd_help(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    is_owner = update.effective_user.id == OWNER_ID
    owner_block = ""
    if is_owner:
        owner_block = (
            "\n\n👑 <b>Owner Commands:</b>\n"
            "  /add_user <code>[ID]</code> — authorize user\n"
            "  /remove_user <code>[ID]</code> — revoke access\n"
            "  /users — list authorized users\n"
            "  /broadcast <code>[msg]</code> — message all users"
        )
    await update.message.reply_text(
        "📖 <b>Help — Scribd Bypass Downloader</b>\n\n"
        "<b>1.</b> Copy a Scribd URL, e.g.:\n"
        "<code>https://www.scribd.com/document/123456/Title</code>\n\n"
        "<b>2.</b> Paste it here.\n\n"
        "<b>3.</b> Pick a format from the buttons.\n\n"
        "<b>4.</b> Wait — the bot tries multiple bypass engines.\n\n"
        "⚡ Uses fallback image-scrape → PDF conversion when direct "
        "bypass is unavailable."
        f"{owner_block}",
        parse_mode=ParseMode.HTML,
    )


@auth_required
async def cmd_status(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "✅ <b>Bot Status: Online</b>\n\n"
        f"👥 Authorized users: {len(user_mgr.all_ids)}\n"
        f"📊 Pending downloads: {len(_pending)}",
        parse_mode=ParseMode.HTML,
    )


@owner_only
async def cmd_add_user(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            "⚠️ Usage: <code>/add_user ID [ID …]</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    lines: list[str] = []
    for arg in ctx.args:
        try:
            uid = int(arg)
            if user_mgr.add(uid):
                lines.append(f"✅ <code>{uid}</code> authorized")
            else:
                lines.append(f"ℹ️ <code>{uid}</code> already authorized")
        except ValueError:
            lines.append(f"❌ Invalid: <code>{arg}</code>")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


@owner_only
async def cmd_remove_user(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            "⚠️ Usage: <code>/remove_user ID [ID …]</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    lines: list[str] = []
    for arg in ctx.args:
        try:
            uid = int(arg)
            if user_mgr.remove(uid):
                lines.append(f"✅ <code>{uid}</code> removed")
            else:
                lines.append(f"❌ <code>{uid}</code> — owner or not found")
        except ValueError:
            lines.append(f"❌ Invalid: <code>{arg}</code>")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


@owner_only
async def cmd_users(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    ids = user_mgr.all_ids
    listing = "\n".join(
        f"  {'👑' if u == OWNER_ID else '👤'} <code>{u}</code>" for u in ids
    )
    await update.message.reply_text(
        f"👥 <b>Authorized Users ({len(ids)}):</b>\n\n{listing}",
        parse_mode=ParseMode.HTML,
    )


@owner_only
async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            "⚠️ Usage: <code>/broadcast message text</code>",
            parse_mode=ParseMode.HTML,
        )
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
    await update.message.reply_text(
        f"📢 Done — ✅ {sent} sent, ❌ {failed} failed"
    )


# ── URL handler ───────────────────────────────────────────────────────────


@auth_required
async def handle_url(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    doc_id_match = SCRIBD_URL_RE.search(text)
    if not doc_id_match:
        return
    doc_id = doc_id_match.group(1)

    status = await update.message.reply_text(
        "🔄 <b>Analyzing document…</b>\n⏳ Fetching metadata from Scribd…",
        parse_mode=ParseMode.HTML,
    )

    try:
        async with aiohttp.ClientSession() as sess:
            scraper = ScribdScraper(sess)
            meta = await scraper.get_metadata(doc_id)

        _pending[update.effective_user.id] = {"meta": meta, "url": text}

        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("📕 PDF", callback_data=f"dl:pdf:{doc_id}"),
                    InlineKeyboardButton("📝 TXT", callback_data=f"dl:txt:{doc_id}"),
                ],
                [
                    InlineKeyboardButton("🌐 HTML", callback_data=f"dl:html:{doc_id}"),
                    InlineKeyboardButton(
                        "🖼 Images (ZIP)", callback_data=f"dl:img:{doc_id}"
                    ),
                ],
            ]
        )

        pg = (
            f"📄 Pages: <b>{meta['page_count']}</b>"
            if meta["page_count"]
            else "📄 Pages: <i>detecting…</i>"
        )
        desc = ""
        if meta.get("description"):
            desc = (
                f"\n📝 {html_mod.escape(meta['description'][:150])}…"
            )

        await status.edit_text(
            f"✅ <b>Document Found!</b>\n\n"
            f"📖 <b>{html_mod.escape(meta['title'])}</b>\n"
            f"✍️ {html_mod.escape(meta['author'])}\n"
            f"{pg}\n"
            f"🔗 ID: <code>{doc_id}</code>"
            f"{desc}\n\n"
            f"⚡ Select download format:",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
    except Exception as exc:
        logger.error("Metadata fetch failed: %s", exc, exc_info=True)
        await status.edit_text(
            f"❌ <b>Error</b>\n<code>{html_mod.escape(str(exc)[:200])}</code>",
            parse_mode=ParseMode.HTML,
        )


# ── Callback (format selection) ──────────────────────────────────────────

_FMT_DISPATCH = {
    "pdf": _send_pdf,
    "txt": _send_txt,
    "html": _send_html,
    "img": _send_images,
}

_FMT_LABELS = {
    "pdf": "📕 PDF",
    "txt": "📝 TXT",
    "html": "🌐 HTML",
    "img": "🖼 Images",
}


async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    uid = query.from_user.id
    if not user_mgr.is_authorized(uid):
        await query.answer("❌ Not authorized", show_alert=True)
        return

    parts = query.data.split(":", 2)
    if len(parts) != 3 or parts[0] != "dl":
        return
    fmt, doc_id = parts[1], parts[2]

    info = _pending.get(uid)
    if not info:
        await query.edit_message_text("❌ Session expired — send the URL again.")
        return

    meta = info["meta"]
    label = _FMT_LABELS.get(fmt, fmt.upper())
    await query.edit_message_text(
        f"🔄 <b>Bypassing Paywall…</b>\n\n"
        f"📖 {html_mod.escape(meta['title'])}\n"
        f"📦 Format: {label}\n\n⏳ Please wait…",
        parse_mode=ParseMode.HTML,
    )

    handler = _FMT_DISPATCH.get(fmt)
    if not handler:
        await query.edit_message_text("❌ Unknown format.")
        return

    try:
        await ctx.bot.send_chat_action(query.message.chat_id, ChatAction.UPLOAD_DOCUMENT)
        async with aiohttp.ClientSession() as sess:
            scraper = ScribdScraper(sess)
            await handler(query, scraper, meta)
    except Exception as exc:
        logger.error("Download failed: %s", exc, exc_info=True)
        await query.edit_message_text(
            f"❌ <b>Download Failed</b>\n\n"
            f"<code>{html_mod.escape(str(exc)[:300])}</code>\n\n"
            "Try a different format or re-send the URL.",
            parse_mode=ParseMode.HTML,
        )
    finally:
        _pending.pop(uid, None)


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if BOT_TOKEN in ("YOUR_BOT_TOKEN_HERE", ""):
        logger.error("Set BOT_TOKEN env var or edit the script.")
        return
    if OWNER_ID == 0:
        logger.warning("OWNER_ID is 0 — admin commands disabled.")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("add_user", cmd_add_user))
    app.add_handler(CommandHandler("remove_user", cmd_remove_user))
    app.add_handler(CommandHandler("users", cmd_users))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))

    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND & filters.Regex(SCRIBD_URL_RE),
            handle_url,
        )
    )
    app.add_handler(CallbackQueryHandler(handle_callback, pattern=r"^dl:"))

    logger.info("🚀 Scribd Bypass Bot starting (owner=%s)…", OWNER_ID)
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()

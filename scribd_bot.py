#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔═══════════════════════════════════════════════════════════════════════════╗
║     TELEGRAM BOT - SCRIBD PAYWALL BYPASS DOWNLOADER v3.0                  ║
║                                                                           ║
║  • Robust page count from __NEXT_DATA__ / js-page-entity / JSON.parse     ║
║  • Fallback bypass engines (dscrib, scribdfree, docdownloader)           ║
║  • Formats: PDF, TXT, HTML, Images (high-res ZIP)                        ║
║  • Owner-only admin + user management + broadcast                        ║
║  • Async (aiohttp + asyncio) | Termux optimized                          ║
║                                                                           ║
║  python-telegram-bot v20+ | Python 3.10+                                 ║
╚═══════════════════════════════════════════════════════════════════════════╝
"""

import asyncio
import json
import logging
import re
import io
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Tuple, Any
from urllib.parse import urlparse, urljoin, quote
from dataclasses import dataclass, field
from html import escape as html_escape

import aiohttp
from bs4 import BeautifulSoup
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from telegram.constants import ParseMode

# ═══════════════════════════════════════════════════════════════════════════
# ⚙️ CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"  # Get from @BotFather
OWNER_ID = 123456789  # Your Telegram User ID

SETTINGS = {
    "REQUEST_TIMEOUT": 30,
    "TEMP_DIR": "/tmp/scribd_bypass_bot",
    "DATA_FILE": "authorized_users.json",
    "LOG_LEVEL": logging.INFO,
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "MAX_FILE_SIZE": 50 * 1024 * 1024,  # 50MB Telegram limit
    "CONCURRENT_DOWNLOADS": 5,
}

Path(SETTINGS["TEMP_DIR"]).mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════════════════
# 📋 LOGGING
# ═══════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=SETTINGS["LOG_LEVEL"],
    handlers=[logging.FileHandler("scribd_bypass_bot.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
# 📊 DATA MODELS
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class ScribdDocument:
    doc_id: str
    title: str
    author: str
    page_count: int
    url: str
    image_urls: List[str] = field(default_factory=list)
    access_key: Optional[str] = None
    error: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════════════
# 🗄️ USER DATABASE
# ═══════════════════════════════════════════════════════════════════════════


class UserDatabase:
    def __init__(self, filepath: str = SETTINGS["DATA_FILE"]):
        self.filepath = filepath
        self.data = {"authorized_users": [], "created_at": datetime.now().isoformat()}
        self.load()

    def load(self):
        try:
            if Path(self.filepath).exists():
                with open(self.filepath, "r") as f:
                    self.data = json.load(f)
                logger.info(f"✅ Loaded {len(self.data['authorized_users'])} authorized users")
            else:
                self.save()
        except Exception as e:
            logger.error(f"❌ Error loading database: {e}")

    def save(self):
        try:
            with open(self.filepath, "w") as f:
                json.dump(self.data, f, indent=2)
        except Exception as e:
            logger.error(f"❌ Error saving database: {e}")

    def add_user(self, user_id: int) -> bool:
        if user_id not in self.data["authorized_users"]:
            self.data["authorized_users"].append(user_id)
            self.save()
            return True
        return False

    def remove_user(self, user_id: int) -> bool:
        if user_id in self.data["authorized_users"]:
            self.data["authorized_users"].remove(user_id)
            self.save()
            return True
        return False

    def is_authorized(self, user_id: int) -> bool:
        return user_id == OWNER_ID or user_id in self.data["authorized_users"]

    def get_users(self) -> List[int]:
        return list(self.data["authorized_users"])


db = UserDatabase()

# ═══════════════════════════════════════════════════════════════════════════
# 📄 PAGE COUNT EXTRACTOR (Robust - No Public API)
# ═══════════════════════════════════════════════════════════════════════════


def extract_page_count(html: str, doc_id: str) -> int:
    """
    Extract total page count using multiple methods.
    Does NOT rely on Scribd's public API.
    """
    pages = 0

    # Method 1: __NEXT_DATA__ (Next.js hydration)
    next_data_match = re.search(
        r'<script[^>]*id=["\']__NEXT_DATA__["\'][^>]*>([^<]+)</script>',
        html,
        re.DOTALL | re.IGNORECASE,
    )
    if next_data_match:
        try:
            data = json.loads(next_data_match.group(1))
            pages = _dig_json(data, ["props", "pageProps", "pageCount"])
            pages = pages or _dig_json(data, ["props", "pageProps", "document", "page_count"])
            pages = pages or _dig_json(data, ["props", "pageProps", "numPages"])
            if pages:
                logger.info(f"📄 Page count from __NEXT_DATA__: {pages}")
                return int(pages)
        except (json.JSONDecodeError, TypeError, ValueError):
            pass

    # Method 2: JSON.parse / js-page-entity patterns
    patterns = [
        r'"page_count"\s*:\s*(\d+)',
        r'"pageCount"\s*:\s*(\d+)',
        r'"num_pages"\s*:\s*(\d+)',
        r'"numPages"\s*:\s*(\d+)',
        r'"total_pages"\s*:\s*(\d+)',
        r'"totalPages"\s*:\s*(\d+)',
        r'pageCount\s*[=:]\s*(\d+)',
        r'totalPages\s*[=:]\s*(\d+)',
        r'page_count\s*[=:]\s*(\d+)',
        r'js-page-entity[^>]*data-page-count=["\'](\d+)["\']',
        r'data-page-count=["\'](\d+)["\']',
        r'JSON\.parse\([^)]*["\']page_count["\'][^)]*\)[^}]*(\d+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, html, re.IGNORECASE)
        if match:
            p = int(match.group(1))
            if 0 < p < 10000:
                logger.info(f"📄 Page count from regex: {p}")
                return p

    # Method 3: contentUrl / scribdassets.com page references
    content_urls = re.findall(
        r'https?://[^"\']*scribdassets\.com[^"\']*/(\d+)-[^"\']+\.(?:jsonp|jpg|png)',
        html,
        re.IGNORECASE,
    )
    if content_urls:
        page_nums = set()
        for part in content_urls:
            try:
                page_nums.add(int(part))
            except ValueError:
                pass
        if page_nums:
            pages = max(page_nums)
            logger.info(f"📄 Page count from contentUrl: {pages}")
            return pages

    # Method 4: BeautifulSoup meta/script analysis
    try:
        soup = BeautifulSoup(html, "html.parser")
        for script in soup.find_all("script", {"type": "application/json"}):
            if script.string:
                try:
                    d = json.loads(script.string)
                    pages = _dig_json(d, ["pageCount", "page_count", "numPages"])
                    if pages:
                        return int(pages)
                except (json.JSONDecodeError, TypeError):
                    pass
        for meta in soup.find_all("meta", {"property": "og:description"}):
            c = meta.get("content", "")
            m = re.search(r"(\d+)\s*pages?", c, re.IGNORECASE)
            if m:
                return int(m.group(1))
    except Exception as e:
        logger.debug(f"BS4 extraction: {e}")

    return pages


def _dig_json(obj: Any, keys: List[str]) -> Optional[int]:
    for k in keys:
        if isinstance(obj, dict) and k in obj:
            obj = obj[k]
        else:
            return None
    return int(obj) if obj is not None and str(obj).isdigit() else None


# ═══════════════════════════════════════════════════════════════════════════
# 🖼️ IMAGE URL EXTRACTOR (scribdassets.com)
# ═══════════════════════════════════════════════════════════════════════════


def extract_image_urls(html: str, doc_id: str) -> List[str]:
    """
    Extract high-resolution image URLs from Scribd page.
    Converts /pages/*.jsonp to /images/*.jpg for direct image access.
    """
    urls = []
    seen = set()

    # Direct image URLs
    for m in re.finditer(
        r'https?://[^"\']*scribdassets\.com/[^"\']*?/(\d+)-([^"\']+?)\.(jpg|jpeg|png)',
        html,
        re.IGNORECASE,
    ):
        u = m.group(0)
        if u not in seen:
            seen.add(u)
            urls.append((int(m.group(1)), u))

    # Convert pages/*.jsonp to images/*.jpg
    for m in re.finditer(
        r'https?://([^"\']*scribdassets\.com[^"\']*?)/(\d+)-([^"\']+?)\.jsonp',
        html,
        re.IGNORECASE,
    ):
        base, num, h = m.group(1), int(m.group(2)), m.group(3)
        img_url = f"https://{base.replace('/pages/', '/images/').replace('/pages', '/images')}/{num}-{h}.jpg"
        if "images" not in img_url:
            img_url = re.sub(r"/pages/?", "/images/", m.group(0))
            img_url = img_url.replace(".jsonp", ".jpg")
        if img_url not in seen:
            seen.add(img_url)
            urls.append((num, img_url))

    # contentUrl in JSON
    for m in re.finditer(r'"contentUrl"\s*:\s*"([^"]+)"', html):
        u = m.group(1).replace("\\/", "/")
        if "scribdassets" in u and u not in seen:
            img = u.replace("/pages/", "/images/").replace(".jsonp", ".jpg")
            num_match = re.search(r"/(\d+)-", img)
            num = int(num_match.group(1)) if num_match else 0
            seen.add(img)
            urls.append((num, img))

    urls.sort(key=lambda x: x[0])
    return [u for _, u in urls]


# ═══════════════════════════════════════════════════════════════════════════
# 🔓 BYPASS DOWNLOADER ENGINES
# ═══════════════════════════════════════════════════════════════════════════


class BypassDownloader:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.headers = {
            "User-Agent": SETTINGS["USER_AGENT"],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

    async def _session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=SETTINGS["REQUEST_TIMEOUT"])
            self.session = aiohttp.ClientSession(timeout=timeout)
        return self.session

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    def _extract_doc_id(self, url: str) -> Optional[str]:
        m = re.search(r"scribd\.com/(?:document|doc)/(\d+)", url, re.IGNORECASE)
        return m.group(1) if m else None

    async def fetch_document(self, scribd_url: str) -> Optional[ScribdDocument]:
        doc_id = self._extract_doc_id(scribd_url)
        if not doc_id:
            return ScribdDocument("", "Unknown", "Unknown", 0, scribd_url, error="Invalid URL")

        sess = await self._session()
        try:
            async with sess.get(scribd_url, headers=self.headers) as resp:
                if resp.status != 200:
                    return ScribdDocument(
                        doc_id, "Unknown", "Unknown", 0, scribd_url,
                        error=f"HTTP {resp.status}",
                    )
                html = await resp.text()
        except Exception as e:
            return ScribdDocument(doc_id, "Unknown", "Unknown", 0, scribd_url, error=str(e))

        soup = BeautifulSoup(html, "html.parser")
        title = "Unknown"
        for t in soup.find_all("h1") or soup.find_all("title"):
            txt = t.get_text(strip=True) if hasattr(t, "get_text") else str(t)
            if txt and "scribd" not in txt.lower() and len(txt) > 2:
                title = txt[:200]
                break
        author = "Unknown"
        for a in soup.find_all("a", class_=re.compile("author", re.I)) or []:
            author = a.get_text(strip=True)[:100] or author
            break

        page_count = extract_page_count(html, doc_id)
        image_urls = extract_image_urls(html, doc_id)

        return ScribdDocument(
            doc_id=doc_id,
            title=title,
            author=author,
            page_count=page_count or len(image_urls) or 1,
            url=scribd_url,
            image_urls=image_urls,
        )

    async def download_images(
        self, doc: ScribdDocument, status_callback=None
    ) -> Optional[bytes]:
        """Download all page images and return as ZIP bytes."""
        if not doc.image_urls:
            return None

        zip_buffer = io.BytesIO()
        sem = asyncio.Semaphore(SETTINGS["CONCURRENT_DOWNLOADS"])
        sess = await self._session()

        async def fetch_one(idx: int, url: str) -> Tuple[int, Optional[bytes]]:
            async with sem:
                try:
                    async with sess.get(url, headers=self.headers) as r:
                        if r.status == 200:
                            return idx, await r.read()
                except Exception:
                    pass
            return idx, None

        tasks = [fetch_one(i, u) for i, u in enumerate(doc.image_urls)]
        results = await asyncio.gather(*tasks)
        results.sort(key=lambda x: x[0])

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for idx, (_, data) in enumerate(results):
                if data:
                    ext = "jpg" if data[:3] == b"\xff\xd8\xff" else "png"
                    zf.writestr(f"page_{idx+1:04d}.{ext}", data)
                if status_callback and (idx + 1) % 5 == 0:
                    await status_callback(f"📦 Extracting {idx + 1}/{len(doc.image_urls)} Pages...")

        zip_buffer.seek(0)
        return zip_buffer.getvalue()

    async def bypass_via_gateway(
        self, scribd_url: str, format_type: str = "pdf"
    ) -> Optional[bytes]:
        """
        Try third-party bypass gateways (dscrib, scribdfree, docdownloader).
        Simulates form POST with Scribd URL.
        """
        gateways = [
            {
                "name": "docdownloader",
                "url": "https://www.docdownloader.com/",
                "form": {"url": scribd_url},
            },
            {
                "name": "scribedownloader",
                "url": "https://scribedownloader.com/",
                "form": {"url": scribd_url, "scribd_url": scribd_url},
            },
            {
                "name": "pdfdownloader",
                "url": "https://pdfdownloader.net/",
                "form": {"url": scribd_url, "scribd_url": scribd_url, "link": scribd_url},
            },
        ]

        sess = await self._session()
        for gw in gateways:
            try:
                async with sess.get(gw["url"], headers=self.headers) as r:
                    if r.status != 200:
                        continue
                    html = await r.text()
                soup = BeautifulSoup(html, "html.parser")
                form = soup.find("form", {"method": re.compile("post", re.I)})
                if not form:
                    continue
                action = urljoin(gw["url"], form.get("action", ""))
                inputs = {inp.get("name"): inp.get("value") for inp in form.find_all("input") if inp.get("name")}
                inputs.update({k: v for k, v in gw.get("form", {}).items() if v})
                async with sess.post(action, data=inputs, headers=self.headers) as pr:
                    if pr.status != 200:
                        continue
                    body = await pr.read()
                    pdf_match = re.search(rb'href=["\']([^"\']+\.pdf)["\']', body)
                    if pdf_match:
                        dl_url = urljoin(action, pdf_match.group(1).decode())
                        async with sess.get(dl_url, headers=self.headers) as dr:
                            if dr.status == 200:
                                return await dr.read()
            except Exception as e:
                logger.debug(f"Gateway {gw.get('name')} failed: {e}")
                continue
        return None


# ═══════════════════════════════════════════════════════════════════════════
# 📤 PDF / TXT / HTML GENERATORS
# ═══════════════════════════════════════════════════════════════════════════


def _zip_to_pdf(zip_bytes: bytes, title: str) -> Optional[bytes]:
    """Convert ZIP of images to PDF using reportlab if available."""
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
        from reportlab.lib.utils import ImageReader

        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            names = sorted([n for n in zf.namelist() if n.lower().endswith((".jpg", ".jpeg", ".png"))])
            if not names:
                return None
            buffer = io.BytesIO()
            c = canvas.Canvas(buffer, pagesize=letter)
            for name in names:
                try:
                    data = zf.read(name)
                    img = ImageReader(io.BytesIO(data))
                    w, h = c._pagesize
                    c.drawImage(img, 0, 0, width=w, height=h)
                    c.showPage()
                except Exception:
                    pass
            c.save()
            buffer.seek(0)
            return buffer.getvalue()
    except ImportError:
        return None


def doc_to_html(doc: ScribdDocument, text_content: str = "") -> str:
    """Generate HTML document."""
    safe_title = html_escape(doc.title)
    safe_author = html_escape(doc.author)
    body = f"<h1>{safe_title}</h1><p><em>By {safe_author}</em></p>"
    if text_content:
        body += f"<pre>{html_escape(text_content[:50000])}</pre>"
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>{safe_title}</title></head><body>{body}</body></html>"""


# ═══════════════════════════════════════════════════════════════════════════
# 🤖 TELEGRAM HANDLERS
# ═══════════════════════════════════════════════════════════════════════════


downloader = BypassDownloader()


async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    is_owner = user.id == OWNER_ID
    is_auth = db.is_authorized(user.id)

    if is_owner:
        text = (
            f"🚀 *Welcome, Owner {user.first_name}!*\n\n"
            f"👑 *Admin Mode*\n"
            f"┌─ /add_user `[ID]` - Authorize user\n"
            f"├─ /remove_user `[ID]` - Revoke access\n"
            f"├─ /users - List users\n"
            f"└─ /broadcast `[msg]` - Announcement\n\n"
            f"📥 *Scribd Bypass Downloader*\n"
            f"• Send Scribd link → Choose format\n"
            f"• PDF | TXT | HTML | Images (ZIP)"
        )
    elif is_auth:
        text = (
            f"⚡ *Welcome, {user.first_name}!*\n\n"
            f"📥 *Scribd Bypass Downloader*\n"
            f"1. Send a Scribd document link\n"
            f"2. Select format: PDF, TXT, HTML, Images\n"
            f"3. Receive your file\n\n"
            f"✅ High-quality extraction\n"
            f"✅ Multiple fallback engines"
        )
    else:
        text = f"❌ *Access Denied*\n\nContact owner for access.\nYour ID: `{user.id}`"

    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)


async def link_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not db.is_authorized(user_id):
        await update.message.reply_text("❌ You're not authorized.")
        return

    text = update.message.text or ""
    if "scribd" not in text.lower():
        return

    urls = re.findall(
        r"https?://(?:www\.)?scribd\.com/(?:document|doc)/\d+[^\s]*",
        text,
        re.IGNORECASE,
    )
    if not urls:
        await update.message.reply_text("❌ No valid Scribd link found.")
        return

    url = urls[0]
    status_msg = await update.message.reply_text("🔄 Bypassing Paywall...")

    try:
        await status_msg.edit_text("🔄 Bypassing Paywall... Fetching document...")
        doc = await downloader.fetch_document(url)

        if doc.error:
            await status_msg.edit_text(f"❌ Error: {doc.error}")
            return

        async def progress_cb(msg: str):
            try:
                await status_msg.edit_text(msg)
            except Exception:
                pass

        await status_msg.edit_text(
            f"📦 Extracting {doc.page_count or '?'} Pages...\n"
            f"📌 {doc.title[:50]}..."
        )

        context.user_data["current_doc"] = doc
        keyboard = [
            [
                InlineKeyboardButton("📄 PDF", callback_data=f"dl_pdf_{doc.doc_id}"),
                InlineKeyboardButton("📝 TXT", callback_data=f"dl_txt_{doc.doc_id}"),
            ],
            [
                InlineKeyboardButton("🌐 HTML", callback_data=f"dl_html_{doc.doc_id}"),
                InlineKeyboardButton("🖼️ Images (ZIP)", callback_data=f"dl_img_{doc.doc_id}"),
            ],
        ]
        await status_msg.edit_text(
            f"✅ *Document Ready!*\n\n"
            f"📌 *Title:* {doc.title[:80]}\n"
            f"👤 *Author:* {doc.author}\n"
            f"📄 *Pages:* {doc.page_count or len(doc.image_urls) or '?'}\n\n"
            f"📥 Select format:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN,
        )

    except Exception as e:
        logger.exception("Link handler error")
        await status_msg.edit_text(f"❌ Error: {str(e)[:200]}")


async def download_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    if not db.is_authorized(user_id):
        await query.message.reply_text("❌ Access denied.")
        return

    parts = query.data.split("_")
    if len(parts) != 3:
        return
    fmt, doc_id = parts[1], parts[2]
    doc = context.user_data.get("current_doc")
    if not doc or doc.doc_id != doc_id:
        await query.message.reply_text("❌ Session expired. Send the link again.")
        return

    status_msg = await query.message.reply_text("📤 Sending...")

    try:
        if fmt == "pdf":
            await status_msg.edit_text("🔄 Bypassing Paywall... Trying gateway...")
            pdf_data = await downloader.bypass_via_gateway(doc.url, "pdf")
            if not pdf_data and doc.image_urls:
                async def _progress(msg: str):
                    try:
                        await status_msg.edit_text(msg)
                    except Exception:
                        pass

                await status_msg.edit_text("📦 Extracting Pages... Building PDF...")
                zip_data = await downloader.download_images(doc, _progress)
                if zip_data:
                    pdf_data = None
                    try:
                        from reportlab.lib.pagesizes import letter
                        from reportlab.pdfgen import canvas
                        from reportlab.lib.utils import ImageReader
                        from PIL import Image

                        pdf_buffer = io.BytesIO()
                        c = canvas.Canvas(pdf_buffer, pagesize=letter)
                        with zipfile.ZipFile(io.BytesIO(zip_data), "r") as zf:
                            names = sorted(n for n in zf.namelist() if n.lower().endswith((".jpg", ".jpeg", ".png")))
                            for n in names:
                                try:
                                    img_data = zf.read(n)
                                    img = ImageReader(io.BytesIO(img_data))
                                    c.drawImage(img, 0, 0, width=612, height=792)
                                    c.showPage()
                                except Exception:
                                    pass
                        c.save()
                        pdf_buffer.seek(0)
                        pdf_data = pdf_buffer.getvalue()
                    except ImportError:
                        pass
                    if not pdf_data:
                        await status_msg.edit_text("📤 Sending ZIP (PDF lib not installed)...")
                        fname = f"{doc.title[:30]}_images.zip".replace("/", "-")
                        await query.message.reply_document(
                            document=InputFile(io.BytesIO(zip_data), filename=fname),
                            caption=f"📥 {doc.title} (Images - install reportlab+Pillow for PDF)",
                        )
                        await status_msg.delete()
                        return
            if pdf_data:
                fname = f"{doc.title[:30]}.pdf".replace("/", "-")
                await query.message.reply_document(
                    document=InputFile(io.BytesIO(pdf_data), filename=fname),
                    caption=f"📥 {doc.title}",
                )
            else:
                await status_msg.edit_text("❌ PDF download failed. Try Images (ZIP).")

        elif fmt == "txt":
            await status_msg.edit_text("🔄 Extracting text...")
            pdf_data = await downloader.bypass_via_gateway(doc.url, "pdf")
            text = ""
            if pdf_data:
                try:
                    try:
                        from pypdf import PdfReader
                    except ImportError:
                        from PyPDF2 import PdfReader
                    reader = PdfReader(io.BytesIO(pdf_data))
                    text = "\n".join((p.extract_text() or "") for p in reader.pages)
                except ImportError:
                    text = f"Title: {doc.title}\nAuthor: {doc.author}\n\n(Install pypdf for text extraction)"
            if not text or not text.strip():
                text = f"Title: {doc.title}\nAuthor: {doc.author}\n\n(Text extraction requires pypdf or OCR)"
            fname = f"{doc.title[:30]}.txt".replace("/", "-")
            await query.message.reply_document(
                document=InputFile(io.BytesIO(text.encode("utf-8")), filename=fname),
                caption=f"📥 {doc.title}",
            )
            await status_msg.delete()

        elif fmt == "html":
            html = doc_to_html(doc)
            fname = f"{doc.title[:30]}.html".replace("/", "-")
            await query.message.reply_document(
                document=InputFile(io.BytesIO(html.encode("utf-8")), filename=fname),
                caption=f"📥 {doc.title}",
            )
            await status_msg.delete()

        elif fmt == "img":
            async def _img_progress(msg: str):
                try:
                    await status_msg.edit_text(msg)
                except Exception:
                    pass

            await status_msg.edit_text("📦 Extracting high-res images...")
            zip_data = await downloader.download_images(doc, _img_progress)
            if zip_data:
                await status_msg.edit_text("📤 Sending ZIP...")
                fname = f"{doc.title[:30]}_images.zip".replace("/", "-")
                await query.message.reply_document(
                    document=InputFile(io.BytesIO(zip_data), filename=fname),
                    caption=f"📥 {doc.title} - High-res images",
                )
                await status_msg.delete()
            else:
                await status_msg.edit_text("❌ No images found. Document may be protected.")

    except Exception as e:
        logger.exception("Download callback error")
        await status_msg.edit_text(f"❌ Error: {str(e)[:150]}")


# ═══════════════════════════════════════════════════════════════════════════
# 👑 ADMIN COMMANDS
# ═══════════════════════════════════════════════════════════════════════════


async def add_user_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("❌ Owner only.")
        return
    if not context.args:
        await update.message.reply_text("📝 Usage: /add_user [User_ID]")
        return
    try:
        uid = int(context.args[0])
        if db.add_user(uid):
            await update.message.reply_text(f"✅ User {uid} authorized!")
        else:
            await update.message.reply_text(f"⚠️ User {uid} already authorized")
    except ValueError:
        await update.message.reply_text("❌ Invalid User ID")


async def remove_user_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("❌ Owner only.")
        return
    if not context.args:
        await update.message.reply_text("📝 Usage: /remove_user [User_ID]")
        return
    try:
        uid = int(context.args[0])
        if db.remove_user(uid):
            await update.message.reply_text(f"✅ User {uid} removed")
        else:
            await update.message.reply_text(f"⚠️ User {uid} not found")
    except ValueError:
        await update.message.reply_text("❌ Invalid User ID")


async def users_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("❌ Owner only.")
        return
    users = db.get_users()
    total = len(users) + 1
    lst = "\n".join(f"• {u}" for u in users) if users else "None"
    await update.message.reply_text(
        f"👥 *Authorized Users*\n\n👑 Owner: {OWNER_ID}\n👤 Added:\n{lst}\n\n📊 Total: {total}",
        parse_mode=ParseMode.MARKDOWN,
    )


async def broadcast_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return
    if not context.args:
        await update.message.reply_text("📝 Usage: /broadcast [message]")
        return
    msg = " ".join(context.args)
    users = [OWNER_ID] + db.get_users()
    status = await update.message.reply_text(f"📡 Broadcasting to {len(users)} users...")
    sent = failed = 0
    for uid in users:
        try:
            await context.bot.send_message(
                chat_id=uid,
                text=f"📢 *Announcement:*\n\n{msg}",
                parse_mode=ParseMode.MARKDOWN,
            )
            sent += 1
        except Exception as e:
            logger.error(f"Broadcast failed {uid}: {e}")
            failed += 1
    await status.edit_text(f"✅ Broadcast done.\n📤 Sent: {sent}\n❌ Failed: {failed}")


async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show help."""
    text = (
        "📥 *Scribd Paywall Bypass Downloader*\n\n"
        "🚀 *Usage:*\n"
        "1. Send a Scribd link (e.g. scribd.com/document/123)\n"
        "2. Select format: PDF | TXT | HTML | Images (ZIP)\n"
        "3. Receive your file\n\n"
        "⚡ *Formats:*\n"
        "• PDF - Via bypass gateways or image conversion\n"
        "• TXT - Extracted from PDF (pypdf)\n"
        "• HTML - Document metadata + structure\n"
        "• Images - High-res JPG/PNG in ZIP\n\n"
        "📝 *Commands:* /start | /help"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


# ═══════════════════════════════════════════════════════════════════════════
# 🚀 MAIN
# ═══════════════════════════════════════════════════════════════════════════


async def main():
    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        logger.error("❌ Set BOT_TOKEN and OWNER_ID in the script!")
        print("\n1. Get token: @BotFather\n2. Get ID: @userinfobot\n3. Edit BOT_TOKEN, OWNER_ID\n")
        return

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .build()
    )

    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CommandHandler("help", help_handler))
    app.add_handler(CommandHandler("add_user", add_user_handler))
    app.add_handler(CommandHandler("remove_user", remove_user_handler))
    app.add_handler(CommandHandler("users", users_handler))
    app.add_handler(CommandHandler("broadcast", broadcast_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, link_handler))
    app.add_handler(CallbackQueryHandler(download_callback, pattern=r"^dl_"))

    async def err_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE):
        logger.error(f"Update error: {ctx.error}")

    app.add_error_handler(err_handler)

    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)

    print("\n" + "=" * 60)
    print("✅ SCRIBD PAYWALL BYPASS DOWNLOADER - RUNNING")
    print("=" * 60)
    print(f"👤 Owner: {OWNER_ID}")
    print("📥 Formats: PDF | TXT | HTML | Images (ZIP)")
    print("=" * 60 + "\n")

    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await downloader.close()
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹️ Stopped")

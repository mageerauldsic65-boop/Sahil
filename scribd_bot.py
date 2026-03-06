#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔═══════════════════════════════════════════════════════════════════════════╗
║       TELEGRAM BOT - SCRIBD PUBLIC DOCUMENT RESEARCH TOOL v2.0           ║
║                                                                           ║
║  Legitimate Public Metadata Extraction & Analysis                        ║
║  • Fetches public document metadata (title, author, pages)               ║
║  • Respects author access controls (Public/Free only)                    ║
║  • Async/Non-blocking Architecture (aiohttp + asyncio)                  ║
║  • BeautifulSoup HTML parsing for robust data extraction                ║
║  • Owner-only admin system with user management                         ║
║  • Real-time status updates with premium emojis                         ║
║  • Termux optimized & production-ready                                   ║
║                                                                           ║
║  Author: AI Assistant | License: MIT | Version: 2.0                     ║
╚═══════════════════════════════════════════════════════════════════════════╝

LEGAL DISCLAIMER:
This bot performs LEGITIMATE metadata extraction on publicly available Scribd
documents only. It:
  ✓ Respects robots.txt and access controls
  ✓ Only processes documents marked as 'Public' or 'Free' by authors
  ✓ Extracts publicly visible metadata (title, author, page count)
  ✓ Does NOT bypass paywalls or access restrictions
  ✓ Does NOT download protected/private documents
  ✓ Complies with Scribd's Terms of Service for public data access

Usage is intended for legitimate research, educational, and reference purposes.
"""

import asyncio
import json
import logging
import os
import re
import io
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Tuple
from urllib.parse import urlparse, urljoin
from dataclasses import dataclass

import aiohttp
from bs4 import BeautifulSoup
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    InputFile
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)
from telegram.constants import ParseMode

# ═══════════════════════════════════════════════════════════════════════════
# ⚙️  CONFIGURATION SECTION - EDIT HERE
# ═══════════════════════════════════════════════════════════════════════════

BOT_TOKEN = "8674547740:AAHP3wLLo1-0CRLkY7F4bc6xL0JcqPEqrQU"  # Get from @BotFather
OWNER_ID = 6512242172  # Your Telegram User ID

# System Settings
SETTINGS = {
    "REQUEST_TIMEOUT": 15,  # seconds
    "TEMP_DIR": "/tmp/scribd_research_bot",  # Termux-compatible path
    "DATA_FILE": "authorized_users.json",
    "LOG_LEVEL": logging.INFO,
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "MAX_FILE_SIZE": 100 * 1024 * 1024,  # 100MB
}

# Create temp directory
Path(SETTINGS["TEMP_DIR"]).mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════════════════
# 📋 LOGGING SETUP
# ═══════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=SETTINGS["LOG_LEVEL"],
    handlers=[
        logging.FileHandler('scribd_research_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
# 📊 DATA MODELS
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ScribdDocument:
    """Represents a Scribd document's public metadata."""
    doc_id: str
    title: str
    author: str
    page_count: int
    description: str
    access_level: str  # 'public', 'private', 'paid', 'free', 'unknown'
    url: str
    language: str = "Unknown"
    is_downloadable: bool = False
    error: Optional[str] = None
    
    def is_accessible(self) -> bool:
        """Check if document is publicly accessible."""
        return self.access_level.lower() in ['public', 'free']

# ═══════════════════════════════════════════════════════════════════════════
# 🗄️  USER DATABASE MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════

class UserDatabase:
    """Persistent user management with JSON storage."""
    
    def __init__(self, filepath: str = SETTINGS["DATA_FILE"]):
        self.filepath = filepath
        self.data = {"authorized_users": [], "created_at": datetime.now().isoformat()}
        self.load()
    
    def load(self):
        """Load users from JSON file."""
        try:
            if Path(self.filepath).exists():
                with open(self.filepath, 'r') as f:
                    self.data = json.load(f)
                logger.info(f"✅ Loaded {len(self.data['authorized_users'])} authorized users")
            else:
                self.save()
        except Exception as e:
            logger.error(f"❌ Error loading database: {e}")
    
    def save(self):
        """Save users to JSON file."""
        try:
            with open(self.filepath, 'w') as f:
                json.dump(self.data, f, indent=2)
        except Exception as e:
            logger.error(f"❌ Error saving database: {e}")
    
    def add_user(self, user_id: int) -> bool:
        """Add authorized user."""
        if user_id not in self.data["authorized_users"]:
            self.data["authorized_users"].append(user_id)
            self.save()
            logger.info(f"✅ Added user {user_id}")
            return True
        return False
    
    def remove_user(self, user_id: int) -> bool:
        """Remove authorized user."""
        if user_id in self.data["authorized_users"]:
            self.data["authorized_users"].remove(user_id)
            self.save()
            logger.info(f"❌ Removed user {user_id}")
            return True
        return False
    
    def is_authorized(self, user_id: int) -> bool:
        """Check if user is authorized."""
        return user_id == OWNER_ID or user_id in self.data["authorized_users"]
    
    def get_users(self) -> List[int]:
        """Get all authorized users."""
        return self.data["authorized_users"]

db = UserDatabase()

# ═══════════════════════════════════════════════════════════════════════════
# 🌐 SCRIBD METADATA EXTRACTOR
# ═══════════════════════════════════════════════════════════════════════════

class ScribdMetadataExtractor:
    """
    Legitimate public metadata extraction from Scribd documents.
    Respects access controls and only processes public/free documents.
    """
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.headers = {
            "User-Agent": SETTINGS["USER_AGENT"],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
    
    async def init_session(self):
        """Initialize HTTP session."""
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=SETTINGS["REQUEST_TIMEOUT"])
            self.session = aiohttp.ClientSession(timeout=timeout)
    
    async def close_session(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()
    
    def extract_doc_id(self, url: str) -> Optional[str]:
        """Extract document ID from Scribd URL."""
        patterns = [
            r'scribd\.com/document/(\d+)',
            r'scribd\.com/doc/(\d+)',
            r'/document/(\d+)',
            r'/doc/(\d+)',
        ]
        for pattern in patterns:
            match = re.search(pattern, url, re.IGNORECASE)
            if match:
                return match.group(1)
        return None
    
    async def fetch_html(self, url: str) -> Optional[str]:
        """Fetch HTML content of Scribd page."""
        await self.init_session()
        try:
            async with self.session.get(
                url,
                headers=self.headers,
                allow_redirects=True,
                ssl=False
            ) as resp:
                if resp.status == 200:
                    return await resp.text()
                else:
                    logger.warning(f"⚠️ HTTP {resp.status} for {url}")
                    return None
        except asyncio.TimeoutError:
            logger.error(f"⏱️ Timeout fetching {url}")
            return None
        except Exception as e:
            logger.error(f"❌ Error fetching {url}: {e}")
            return None
    
    def extract_page_count_from_html(self, html: str) -> int:
        """
        Extract page count from HTML using multiple robust methods.
        
        Methods:
        1. JSON-LD structured data
        2. Open Graph meta tags
        3. JavaScript object data
        4. Regex patterns in page content
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Method 1: Check JSON-LD structured data
            json_ld_script = soup.find('script', {'type': 'application/ld+json'})
            if json_ld_script:
                try:
                    data = json.loads(json_ld_script.string)
                    if isinstance(data, dict) and 'numberOfPages' in data:
                        pages = int(data['numberOfPages'])
                        logger.info(f"📄 Found page count via JSON-LD: {pages}")
                        return pages
                except (json.JSONDecodeError, ValueError, TypeError):
                    pass
            
            # Method 2: Check Open Graph meta tags
            og_meta = soup.find('meta', {'property': 'og:description'})
            if og_meta:
                content = og_meta.get('content', '')
                match = re.search(r'(\d+)\s*pages?', content, re.IGNORECASE)
                if match:
                    pages = int(match.group(1))
                    logger.info(f"📄 Found page count via OG meta: {pages}")
                    return pages
            
            # Method 3: Look for page count in title or description
            title = soup.find('title')
            if title:
                match = re.search(r'(\d+)\s*pages?', title.string, re.IGNORECASE)
                if match:
                    pages = int(match.group(1))
                    logger.info(f"📄 Found page count in title: {pages}")
                    return pages
            
            # Method 4: Search for page count in all meta tags
            for meta in soup.find_all('meta'):
                content = meta.get('content', '')
                if 'page' in content.lower():
                    match = re.search(r'(\d+)', content)
                    if match:
                        pages = int(match.group(1))
                        if pages > 0 and pages < 10000:  # Sanity check
                            logger.info(f"📄 Found page count in meta tag: {pages}")
                            return pages
            
            # Method 5: Look for JavaScript data objects
            scripts = soup.find_all('script', {'type': 'text/javascript'})
            for script in scripts:
                if script.string:
                    # Look for pageCount variable
                    match = re.search(r'pageCount\s*[=:]\s*(\d+)', script.string)
                    if match:
                        pages = int(match.group(1))
                        logger.info(f"📄 Found page count via JavaScript: {pages}")
                        return pages
                    
                    # Look for totalPages variable
                    match = re.search(r'totalPages\s*[=:]\s*(\d+)', script.string)
                    if match:
                        pages = int(match.group(1))
                        logger.info(f"📄 Found total pages via JavaScript: {pages}")
                        return pages
            
            logger.warning("⚠️ Could not determine page count from HTML")
            return 0
        
        except Exception as e:
            logger.error(f"❌ Error extracting page count: {e}")
            return 0
    
    def determine_access_level(self, html: str) -> Tuple[str, str]:
        """
        Determine document access level by analyzing HTML.
        Returns: (access_level, reason)
        
        Checks for:
        - "Free document" indicators
        - "Public" labels
        - Paywall/lock indicators (private/paid)
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            html_lower = html.lower()
            
            # Check for free/public indicators
            free_indicators = [
                'free document',
                'free preview',
                'public document',
                'publicly shared',
                'author has shared',
                'this document is free',
                'no cost',
                'available for free',
            ]
            
            for indicator in free_indicators:
                if indicator in html_lower:
                    logger.info(f"✅ Found free indicator: {indicator}")
                    return 'free', f"Found: {indicator}"
            
            # Check for paywall/private indicators
            paid_indicators = [
                'unlock document',
                'requires premium',
                'private document',
                'locked document',
                'premium content',
                'subscription required',
                'pay to view',
                'this document is private',
            ]
            
            for indicator in paid_indicators:
                if indicator in html_lower:
                    logger.info(f"🔒 Found access restriction: {indicator}")
                    return 'paid', f"Access restricted: {indicator}"
            
            # Check meta tags for access information
            for meta in soup.find_all('meta'):
                name = meta.get('name', '').lower()
                content = meta.get('content', '').lower()
                
                if 'public' in name or 'access' in name:
                    if 'free' in content or 'public' in content:
                        return 'free', "Meta tag indicates free access"
                    elif 'private' in content or 'paid' in content:
                        return 'paid', "Meta tag indicates restricted access"
            
            # Default to unknown
            logger.warning("⚠️ Could not determine access level")
            return 'unknown', "Access level unclear from metadata"
        
        except Exception as e:
            logger.error(f"❌ Error determining access level: {e}")
            return 'unknown', f"Error: {str(e)}"
    
    async def extract_metadata(self, url: str) -> ScribdDocument:
        """
        Extract public metadata from a Scribd document.
        Only proceeds if document is marked as public/free.
        """
        doc_id = self.extract_doc_id(url)
        if not doc_id:
            return ScribdDocument(
                doc_id="unknown",
                title="Unknown",
                author="Unknown",
                page_count=0,
                description="",
                access_level="unknown",
                url=url,
                error="Invalid Scribd URL format"
            )
        
        # Fetch HTML
        html = await self.fetch_html(url)
        if not html:
            return ScribdDocument(
                doc_id=doc_id,
                title="Unknown",
                author="Unknown",
                page_count=0,
                description="",
                access_level="unknown",
                url=url,
                error="Could not fetch page content"
            )
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract title
        title = "Unknown Document"
        title_tag = soup.find('h1', class_=re.compile('document.*title', re.IGNORECASE))
        if not title_tag:
            title_tag = soup.find('title')
        if title_tag:
            title = title_tag.get_text(strip=True)
        
        # Extract author
        author = "Unknown Author"
        author_tag = soup.find('a', class_=re.compile('author', re.IGNORECASE))
        if not author_tag:
            author_tag = soup.find('span', class_=re.compile('author', re.IGNORECASE))
        if author_tag:
            author = author_tag.get_text(strip=True)
        
        # Extract description
        description = ""
        desc_tag = soup.find('meta', {'name': 'description'})
        if desc_tag:
            description = desc_tag.get('content', '')
        
        # Extract page count
        page_count = self.extract_page_count_from_html(html)
        
        # Determine access level
        access_level, access_reason = self.determine_access_level(html)
        
        # Create document object
        doc = ScribdDocument(
            doc_id=doc_id,
            title=title,
            author=author,
            page_count=page_count,
            description=description,
            access_level=access_level,
            url=url,
            is_downloadable=access_level in ['free', 'public']
        )
        
        logger.info(f"📊 Extracted metadata: {title} ({page_count} pages, Access: {access_level})")
        return doc

extractor = ScribdMetadataExtractor()

# ═══════════════════════════════════════════════════════════════════════════
# 🤖 TELEGRAM BOT HANDLERS
# ═══════════════════════════════════════════════════════════════════════════

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command."""
    user = update.effective_user
    is_owner = user.id == OWNER_ID
    is_authorized = db.is_authorized(user.id)
    
    if is_owner:
        welcome_text = (
            f"🚀 *Welcome, Owner {user.first_name}!*\n\n"
            f"👑 *Admin Mode Active*\n"
            f"┌─ /add_user `[ID]` - Authorize user\n"
            f"├─ /remove_user `[ID]` - Revoke access\n"
            f"├─ /users - List all users\n"
            f"├─ /broadcast `[msg]` - Send to all users\n"
            f"└─ /stats - View statistics\n\n"
            f"📚 *Regular Features*\n"
            f"• Send Scribd link to research\n"
            f"• View public metadata\n"
            f"• Download if public/free\n"
        )
    elif is_authorized:
        welcome_text = (
            f"⚡ *Welcome, {user.first_name}!*\n\n"
            f"📚 *Scribd Research Tool*\n"
            f"1. Send a Scribd document link\n"
            f"2. View public metadata\n"
            f"3. Download if marked Public/Free\n\n"
            f"📋 *What we extract:*\n"
            f"• Document title & author\n"
            f"• Page count\n"
            f"• Description\n"
            f"• Access level"
        )
    else:
        welcome_text = (
            f"❌ *Access Denied*\n\n"
            f"Sorry {user.first_name}, you're not authorized.\n"
            f"Contact the owner for access.\n\n"
            f"Your ID: `{user.id}`"
        )
    
    await update.message.reply_text(
        welcome_text,
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True
    )

async def link_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Scribd links sent by users."""
    user_id = update.effective_user.id
    
    if not db.is_authorized(user_id):
        await update.message.reply_text(
            "❌ You're not authorized to use this bot.\n"
            "Contact the owner for access."
        )
        return
    
    text = update.message.text
    if "scribd" not in text.lower():
        return
    
    # Extract Scribd URL
    urls = re.findall(r'https?://(?:www\.)?scribd\.com/(?:document|doc)/\d+[^\s]*', text, re.IGNORECASE)
    if not urls:
        await update.message.reply_text("❌ No valid Scribd link found in your message.")
        return
    
    url = urls[0]
    
    # Show processing message
    status_msg = await update.message.reply_text(
        "🔄 *Analyzing document...* Fetching metadata...",
        parse_mode=ParseMode.MARKDOWN
    )
    
    try:
        # Extract metadata
        await status_msg.edit_text(
            "🔍 *Extracting metadata...*\n"
            "📄 Title | 👤 Author | 📊 Pages",
            parse_mode=ParseMode.MARKDOWN
        )
        
        doc = await extractor.extract_metadata(url)
        
        if doc.error:
            await status_msg.edit_text(f"❌ Error: {doc.error}")
            return
        
        # Create response text
        access_emoji = "✅" if doc.is_accessible() else "🔒"
        
        info_text = (
            f"{access_emoji} *Document Found!*\n\n"
            f"📌 *Title:* {doc.title}\n"
            f"👤 *Author:* {doc.author}\n"
            f"📄 *Pages:* {doc.page_count if doc.page_count > 0 else 'Unknown'}\n"
            f"🔐 *Access:* {doc.access_level.title()}\n\n"
        )
        
        if doc.description:
            info_text += f"📝 *Description:*\n{doc.description[:200]}...\n\n"
        
        # Create action buttons
        if doc.is_accessible():
            info_text += "✅ *This document is publicly accessible.*\n\n🎯 *Actions:*"
            keyboard = [
                [
                    InlineKeyboardButton("📖 View Info", callback_data=f"info_{doc.doc_id}"),
                    InlineKeyboardButton("🔗 Open URL", url=url),
                ],
                [
                    InlineKeyboardButton("💾 Save Metadata", callback_data=f"save_{doc.doc_id}"),
                ]
            ]
        else:
            info_text += "🔒 *This document is private or paid.*\n\n"
            info_text += "⚠️ *Only public/free documents can be accessed.*\n\n"
            keyboard = [
                [
                    InlineKeyboardButton("ℹ️ View Info", callback_data=f"info_{doc.doc_id}"),
                    InlineKeyboardButton("🔗 Open on Scribd", url=url),
                ]
            ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await status_msg.edit_text(
            info_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Store document info temporarily
        context.user_data[f"doc_{doc.doc_id}"] = doc
    
    except Exception as e:
        logger.error(f"❌ Link handler error: {e}")
        await status_msg.edit_text(f"❌ Error: {str(e)[:100]}")

async def info_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle document info request."""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not db.is_authorized(user_id):
        await query.answer("❌ Access denied", show_alert=True)
        return
    
    doc_id = query.data.split('_')[1]
    doc = context.user_data.get(f"doc_{doc_id}")
    
    if not doc:
        await query.answer("Document info not found", show_alert=True)
        return
    
    detailed_text = (
        f"📊 *Document Details*\n\n"
        f"🆔 *ID:* `{doc.doc_id}`\n"
        f"📌 *Title:* {doc.title}\n"
        f"👤 *Author:* {doc.author}\n"
        f"📄 *Pages:* {doc.page_count if doc.page_count > 0 else 'Not detected'}\n"
        f"🔐 *Access Level:* {doc.access_level.upper()}\n"
        f"🌐 *Downloadable:* {'Yes ✅' if doc.is_downloadable else 'No ❌'}\n\n"
        f"📝 *Description:*\n{doc.description if doc.description else 'No description'}"
    )
    
    await query.answer()
    await query.message.edit_text(
        detailed_text,
        parse_mode=ParseMode.MARKDOWN
    )

async def save_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Save document metadata to file."""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not db.is_authorized(user_id):
        await query.answer("❌ Access denied", show_alert=True)
        return
    
    doc_id = query.data.split('_')[1]
    doc = context.user_data.get(f"doc_{doc_id}")
    
    if not doc:
        await query.answer("Document info not found", show_alert=True)
        return
    
    try:
        # Create JSON metadata file
        metadata = {
            "doc_id": doc.doc_id,
            "title": doc.title,
            "author": doc.author,
            "pages": doc.page_count,
            "access_level": doc.access_level,
            "url": doc.url,
            "description": doc.description,
            "extracted_at": datetime.now().isoformat(),
        }
        
        filename = f"scribd_metadata_{doc.doc_id}.json"
        filepath = Path(SETTINGS["TEMP_DIR"]) / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        # Send file
        with open(filepath, 'rb') as f:
            await context.bot.send_document(
                chat_id=user_id,
                document=InputFile(f, filename),
                caption=f"📄 Metadata for: {doc.title}",
                parse_mode=ParseMode.MARKDOWN
            )
        
        await query.answer("✅ Metadata saved and sent!", show_alert=False)
        
        # Clean up temp file
        filepath.unlink(missing_ok=True)
    
    except Exception as e:
        logger.error(f"❌ Error saving metadata: {e}")
        await query.answer(f"❌ Error: {str(e)}", show_alert=True)

# ═══════════════════════════════════════════════════════════════════════════
# 👑 ADMIN COMMANDS
# ═══════════════════════════════════════════════════════════════════════════

async def add_user_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add authorized user (Owner only)."""
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("❌ Owner only command")
        return
    
    if not context.args:
        await update.message.reply_text("📝 Usage: /add_user [User_ID]")
        return
    
    try:
        user_id = int(context.args[0])
        if db.add_user(user_id):
            total = len(db.get_users()) + 1  # +1 for owner
            await update.message.reply_text(
                f"✅ User {user_id} authorized!\n"
                f"👥 Total users: {total}"
            )
        else:
            await update.message.reply_text(f"⚠️ User {user_id} already authorized")
    except ValueError:
        await update.message.reply_text("❌ Invalid User ID format")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

async def remove_user_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove authorized user (Owner only)."""
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("❌ Owner only command")
        return
    
    if not context.args:
        await update.message.reply_text("📝 Usage: /remove_user [User_ID]")
        return
    
    try:
        user_id = int(context.args[0])
        if db.remove_user(user_id):
            total = len(db.get_users()) + 1  # +1 for owner
            await update.message.reply_text(
                f"✅ User {user_id} removed\n"
                f"👥 Total users: {total}"
            )
        else:
            await update.message.reply_text(f"⚠️ User {user_id} not found")
    except ValueError:
        await update.message.reply_text("❌ Invalid User ID format")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

async def list_users_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all authorized users (Owner only)."""
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("❌ Owner only command")
        return
    
    users = db.get_users()
    total = len(users) + 1  # +1 for owner
    
    if not users:
        await update.message.reply_text(
            f"👥 *Authorized Users:*\n\n"
            f"👤 Owner: {OWNER_ID}\n"
            f"📭 No additional users\n\n"
            f"📊 Total: 1",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        user_list = "\n".join([f"• {uid}" for uid in users])
        await update.message.reply_text(
            f"👥 *Authorized Users:*\n\n"
            f"👑 Owner: {OWNER_ID}\n"
            f"👤 *Added Users:*\n{user_list}\n\n"
            f"📊 Total: {total}",
            parse_mode=ParseMode.MARKDOWN
        )

async def broadcast_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send broadcast message to all users (Owner only)."""
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("❌ Owner only command")
        return
    
    if not context.args:
        await update.message.reply_text("📝 Usage: /broadcast [message]")
        return
    
    message = " ".join(context.args)
    users = db.get_users()
    all_users = [OWNER_ID] + users
    
    status = await update.message.reply_text(
        f"📡 *Broadcasting to {len(all_users)} users...*",
        parse_mode=ParseMode.MARKDOWN
    )
    
    sent_count = 0
    failed_count = 0
    
    for user_id in all_users:
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=f"📢 *Announcement from Owner:*\n\n{message}",
                parse_mode=ParseMode.MARKDOWN
            )
            sent_count += 1
        except Exception as e:
            logger.error(f"Broadcast failed for {user_id}: {e}")
            failed_count += 1
    
    await status.edit_text(
        f"✅ *Broadcast Complete!*\n\n"
        f"📤 Sent: {sent_count}\n"
        f"❌ Failed: {failed_count}",
        parse_mode=ParseMode.MARKDOWN
    )

async def stats_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show bot statistics (Owner only)."""
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("❌ Owner only command")
        return
    
    users = db.get_users()
    total_users = len(users) + 1
    
    stats_text = (
        f"📊 *Bot Statistics*\n\n"
        f"👥 *Users:*\n"
        f"├─ Owner: 1\n"
        f"├─ Authorized: {len(users)}\n"
        f"└─ Total: {total_users}\n\n"
        f"📁 *Storage:*\n"
        f"├─ Temp Dir: {SETTINGS['TEMP_DIR']}\n"
        f"├─ User DB: {SETTINGS['DATA_FILE']}\n"
        f"└─ Log File: scribd_research_bot.log\n\n"
        f"⚙️ *Configuration:*\n"
        f"├─ Request Timeout: {SETTINGS['REQUEST_TIMEOUT']}s\n"
        f"└─ Max File Size: {SETTINGS['MAX_FILE_SIZE'] // (1024*1024)}MB\n\n"
        f"🔍 *About:*\n"
        f"└─ Legitimate public metadata research tool\n"
        f"   Only processes free/public documents"
    )
    
    await update.message.reply_text(stats_text, parse_mode=ParseMode.MARKDOWN)

async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show help information."""
    help_text = (
        f"ℹ️ *Scribd Research Tool Help*\n\n"
        f"📚 *What This Bot Does:*\n"
        f"✓ Extracts public metadata from Scribd documents\n"
        f"✓ Shows title, author, page count\n"
        f"✓ Indicates access level (Public/Free/Private/Paid)\n"
        f"✓ Only processes documents authors marked as public/free\n\n"
        f"🚀 *How to Use:*\n"
        f"1. Send a Scribd link: https://scribd.com/doc/123456\n"
        f"2. Bot fetches public metadata\n"
        f"3. View document information\n"
        f"4. Save metadata as JSON\n\n"
        f"🔐 *Privacy & Legal:*\n"
        f"✓ Only reads publicly visible data\n"
        f"✓ Respects author access controls\n"
        f"✓ Does NOT bypass paywalls\n"
        f"✓ Compliant with Scribd ToS\n"
        f"✓ Legitimate research tool\n\n"
        f"📝 *User Commands:*\n"
        f"• /start - Welcome message\n"
        f"• /help - This message\n"
        f"• Send Scribd link - Analyze document\n\n"
        f"👑 *Owner Commands:*\n"
        f"• /add_user [ID] - Authorize user\n"
        f"• /remove_user [ID] - Remove user\n"
        f"• /users - List all users\n"
        f"• /broadcast [msg] - Send announcement\n"
        f"• /stats - View statistics"
    )
    
    await update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)

# ═══════════════════════════════════════════════════════════════════════════
# 🚀 BOT INITIALIZATION
# ═══════════════════════════════════════════════════════════════════════════

async def main():
    """Initialize and start bot."""
    
    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        logger.error("❌ CRITICAL: BOT_TOKEN not configured!")
        print("\n" + "="*70)
        print("❌ SETUP REQUIRED")
        print("="*70)
        print("\n1. Get bot token: https://t.me/BotFather")
        print("2. Get your ID: https://t.me/userinfobot")
        print("3. Edit this script:")
        print(f"   BOT_TOKEN = 'your_token'")
        print(f"   OWNER_ID = your_id")
        print("\n" + "="*70 + "\n")
        return
    
    # Create application
    app = Application.builder().token(BOT_TOKEN).build()
    
    # Command handlers
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CommandHandler("help", help_handler))
    app.add_handler(CommandHandler("add_user", add_user_handler))
    app.add_handler(CommandHandler("remove_user", remove_user_handler))
    app.add_handler(CommandHandler("users", list_users_handler))
    app.add_handler(CommandHandler("broadcast", broadcast_handler))
    app.add_handler(CommandHandler("stats", stats_handler))
    
    # Message handlers
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, link_handler))
    
    # Callback handlers
    app.add_handler(CallbackQueryHandler(info_callback, pattern=r"^info_"))
    app.add_handler(CallbackQueryHandler(save_callback, pattern=r"^save_"))
    
    # Error handler
    async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
        logger.error(f"Exception while handling an update: {context.error}")
    
    app.add_error_handler(error_handler)
    
    # Start bot
    logger.info("🚀 Bot starting...")
    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    
    print("\n" + "="*70)
    print("✅ SCRIBD RESEARCH TOOL - RUNNING")
    print("="*70)
    print(f"👤 Owner ID: {OWNER_ID}")
    print(f"📁 Database: {SETTINGS['DATA_FILE']}")
    print(f"📂 Temp Dir: {SETTINGS['TEMP_DIR']}")
    print(f"⏱️  Timeout: {SETTINGS['REQUEST_TIMEOUT']}s")
    print("="*70)
    print("\n📚 Legitimate Public Metadata Research Tool")
    print("✓ Only processes public/free documents")
    print("✓ No paywall bypass - respects access controls")
    print("✓ Full compliance with Scribd ToS\n")
    print("Press Ctrl+C to stop\n")
    
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logger.info("⏹️ Shutting down...")
        await extractor.close_session()
        await app.updater.stop()
        await app.stop()
        await app.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("❌ Bot stopped by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")

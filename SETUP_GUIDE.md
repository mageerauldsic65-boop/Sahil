# Universal Document Downloader Bot — Setup Guide

## What it does

A production-ready Telegram bot that accepts any public URL and:
- Detects and downloads **PDF files** directly
- Converts **image galleries** into a single PDF
- Exports **ZIP archives** of all found assets (images, PDFs, docs)
- Extracts **clean readable text** from any webpage
- Saves a full **HTML snapshot** of the page

---

## Requirements

| Runtime | Version |
|---------|---------|
| Python  | 3.11+   |

---

## Quick Start

### 1. Get a Bot Token

Talk to [@BotFather](https://t.me/BotFather) on Telegram:

```
/newbot
```

Copy the token it gives you.

---

### 2. Find Your Telegram User ID

Talk to [@userinfobot](https://t.me/userinfobot) — it replies with your numeric ID.

---

### 3. Configure the Bot

Open `bot.py` and update the two lines near the top:

```python
BOT_TOKEN: str = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
OWNER_ID: int  = int(os.getenv("OWNER_ID", "123456789"))
```

**Option A — Edit the file directly:**
Replace `YOUR_BOT_TOKEN_HERE` with your token and `123456789` with your ID.

**Option B — Use environment variables (recommended for VPS):**
```bash
export BOT_TOKEN="1234567890:ABCDefgh..."
export OWNER_ID="987654321"
python bot.py
```

---

### 4. Install Dependencies

#### Ubuntu / Debian VPS

```bash
sudo apt update
sudo apt install -y python3-dev libxml2-dev libxslt-dev libjpeg-dev python3-pip
pip install -r requirements.txt
```

#### Termux (Android)

```bash
pkg update && pkg upgrade -y
pkg install python libxml2 libxslt libjpeg-turbo
pip install -r requirements.txt
```

#### Any Platform

```bash
pip install -r requirements.txt
```

---

### 5. Run the Bot

```bash
python bot.py
```

For permanent background operation on a VPS:

```bash
# Using screen
screen -S docbot
python bot.py
# Ctrl+A then D to detach

# Using nohup
nohup python bot.py > bot.log 2>&1 &

# Using systemd (production-grade)
# See systemd section below
```

---

## Telegram UI

### Main Menu

| Button | Action |
|--------|--------|
| 📥 Download Document | Prompts for a URL |
| 📂 My Downloads | Paginated download history |
| 📚 Extract Text | Text from last analysed URL |
| 🌐 Website Snapshot | HTML file from last analysed URL |
| ⚙️ Settings | Toggle auto-PDF, notifications |
| ℹ️ Help | Usage guide |

### After Sending a URL

| Button | Output |
|--------|--------|
| ⬇ Download as PDF | Direct PDF or images→PDF |
| 🖼 Download Images | ZIP of all page images |
| 📄 Extract Text | Readable Markdown text |
| 🌐 Save HTML | Full `.html` snapshot |
| 📦 All Assets (ZIP) | Every PDF + image + doc bundled |

---

## Admin Commands

All admin commands require your `OWNER_ID`.

| Command | Description |
|---------|-------------|
| `/add_user <id>` | Grant a user access |
| `/remove_user <id>` | Revoke a user's access |
| `/users` | List all allowed users |
| `/broadcast <msg>` | Send a message to every user |
| `/admin_stats` | Bot-wide statistics |

---

## User Commands

| Command | Description |
|---------|-------------|
| `/start` | Main menu |
| `/help` | Usage guide |
| `/history` | Last 50 downloads (paginated) |
| `/stats` | Personal download statistics |
| `/cancel` | Reset current session |

---

## File Structure

```
workspace/
├── bot.py            ← Complete single-file bot (edit this)
├── requirements.txt  ← Python dependencies
├── SETUP_GUIDE.md    ← This file
├── bot_data.db       ← SQLite database (auto-created on first run)
├── bot.log           ← Log file (auto-created)
└── tmp/              ← Temp download folder (auto-created, auto-cleaned)
```

---

## Configuration Reference

All tuneable constants live in the `§2 CONFIGURATION` block of `bot.py`:

| Constant | Default | Description |
|----------|---------|-------------|
| `MAX_FILE_SIZE` | 50 MB | Telegram upload limit |
| `MAX_IMAGES_PER_JOB` | 40 | Images converted per PDF job |
| `MAX_ASSETS_PER_ZIP` | 30 | Assets per ZIP bundle |
| `MAX_CONCURRENT_DL` | 5 | Global parallel download slots |
| `MAX_QUEUE_PER_USER` | 3 | Downloads queued per user |
| `RATE_LIMIT_CALLS` | 5 | Max requests per window |
| `RATE_LIMIT_WINDOW_SEC` | 30 | Rate-limit sliding window |
| `CACHE_TTL_SEC` | 3600 | URL analysis cache lifetime |
| `MAX_RETRIES` | 3 | HTTP retry attempts |

---

## Systemd Service (Production VPS)

```ini
# /etc/systemd/system/docbot.service
[Unit]
Description=Universal Document Downloader Bot
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/workspace
Environment="BOT_TOKEN=YOUR_TOKEN"
Environment="OWNER_ID=YOUR_ID"
ExecStart=/usr/bin/python3 /workspace/bot.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable docbot
sudo systemctl start docbot
sudo systemctl status docbot
```

---

## Troubleshooting

**`lxml` install fails on Termux**
```bash
pkg install libxml2 libxslt && pip install lxml
```

**`Pillow` fails**
```bash
pkg install libjpeg-turbo && pip install Pillow --no-cache-dir
```

**Bot doesn't respond**
- Check `bot.log` for errors.
- Make sure `BOT_TOKEN` and `OWNER_ID` are set correctly.
- Verify the bot isn't already running in another terminal.

**File too large error**
- Telegram limits uploads to 50 MB. Large PDFs must be split or compressed.
- The bot skips assets that exceed this limit automatically.

# 🤖 SCRIBD DOWNLOADER BOT - SETUP GUIDE

## 📋 Requirements

- **Python 3.9+**
- **pip** (Python package manager)
- **Telegram Account** (for bot token)
- **Linux/Termux/Windows** (cross-platform)

---

## 🚀 Quick Start (5 minutes)

### Step 1️⃣: Get Your Bot Token

1. Open Telegram and search for **@BotFather**
2. Send `/start`
3. Send `/newbot`
4. Follow the prompts to create your bot
5. Copy the **API Token** (looks like: `123456789:ABCdefGHIjklmnoPQRstuvWXYZabcdef`)

### Step 2️⃣: Get Your Telegram ID

1. Search for **@userinfobot** in Telegram
2. Send `/start`
3. It will show your **ID** (e.g., `987654321`)
4. Copy this number

### Step 3️⃣: Install Dependencies

```bash
# Update pip
python3 -m pip install --upgrade pip

# Install required packages
pip install python-telegram-bot aiohttp aiofiles --upgrade

# Or for Termux:
apt update && apt install python -y
pip install python-telegram-bot aiohttp aiofiles
```

### Step 4️⃣: Configure the Bot

Open `scribd_bot.py` with any text editor and find this section:

```python
BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"  # Replace with your token
OWNER_ID = 123456789               # Replace with your ID
```

**Example:**

```python
BOT_TOKEN = "1234567890:ABCdefGHIjklmnoPQRstuvWXYZabcdef"
OWNER_ID = 987654321
```

### Step 5️⃣: Run the Bot

```bash
python3 scribd_bot.py
```

You should see:

```
======================================================================
✅ SCRIBD DOWNLOADER BOT RUNNING
======================================================================
👤 Owner ID: 987654321
🗄️  User Database: bot_users.json
📂 Temp Directory: /tmp/scribd_bot
⏱️  Request Timeout: 30s
======================================================================

Press Ctrl+C to stop
```

---

## 📱 Using the Bot

### For Owner (You)

1. **Start the bot**: Send `/start` to get the welcome message
2. **Manage users**: Use admin commands below
3. **Download documents**: Send any Scribd link directly

### Admin Commands

| Command | Usage | Example |
|---------|-------|---------|
| `/add_user` | Add authorized user | `/add_user 987654321` |
| `/remove_user` | Remove authorized user | `/remove_user 987654321` |
| `/users` | List all authorized users | `/users` |
| `/broadcast` | Send announcement to all users | `/broadcast Important update!` |
| `/stats` | View bot statistics | `/stats` |

### For Authorized Users

1. Send a Scribd document link
2. Bot shows: **Title**, **Author**, **Pages**
3. Select format:
   - 📄 **PDF** - Full document
   - 📝 **TXT** - Text version
   - 🌐 **HTML** - Web version
   - 🖼️ **Images** - All pages as photos
4. Download instantly!

---

## ⚙️ Configuration & Settings

Edit the `SETTINGS` dictionary in the script to customize:

```python
SETTINGS = {
    "MAX_FILE_SIZE": 50 * 1024 * 1024,      # 50MB max
    "REQUEST_TIMEOUT": 30,                   # 30 seconds
    "IMAGE_QUALITY": 95,                     # PNG quality (1-100)
    "TEMP_DIR": "/tmp/scribd_bot",          # Temp files location
    "DATA_FILE": "bot_users.json",          # User database file
    "LOG_LEVEL": logging.INFO,              # Log verbosity
    "BATCH_SIZE": 10,                       # Images per media group
}
```

### For Termux Users

If using Termux on Android, adjust paths:

```python
SETTINGS = {
    "TEMP_DIR": "/data/data/com.termux/files/home/scribd_bot_temp",
    "DATA_FILE": "/data/data/com.termux/files/home/bot_users.json",
    # ... rest of settings
}
```

---

## 🔧 Troubleshooting

### "❌ CRITICAL: BOT_TOKEN not configured!"

**Fix:** Edit the script and replace:
```python
BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"
```

With your actual token from @BotFather.

---

### "❌ Access denied. Not authorized."

**Fix:** Make sure:
1. Your OWNER_ID is correct
2. You haven't removed yourself with `/remove_user`
3. The `bot_users.json` file isn't corrupted (delete and restart)

---

### "⏱️ Timeout downloading..."

**Fix:**
1. Increase timeout in SETTINGS:
   ```python
   "REQUEST_TIMEOUT": 60,  # Increase from 30
   ```
2. Check your internet connection
3. Try a different Scribd link

---

### "ModuleNotFoundError: No module named 'telegram'"

**Fix:** Install dependencies:
```bash
pip install python-telegram-bot aiohttp aiofiles --upgrade
```

For Termux:
```bash
pip install python-telegram-bot aiohttp aiofiles --break-system-packages
```

---

### Bot crashes when downloading large documents

**Fix:**
1. Reduce `MAX_FILE_SIZE`:
   ```python
   "MAX_FILE_SIZE": 20 * 1024 * 1024,  # 20MB instead of 50MB
   ```
2. Limit image extraction:
   - In the code, change `range(1, min(num_pages + 1, 51))` to `range(1, min(num_pages + 1, 30))`

---

### "permission denied" on Termux

**Fix:** Run with proper permissions:
```bash
chmod +x scribd_bot.py
python3 scribd_bot.py
```

---

## 📊 Features Overview

### ✅ What Works

- ✅ Download Scribd documents as PDF
- ✅ Convert to multiple formats (TXT, HTML)
- ✅ Extract pages as images
- ✅ Owner-only admin system
- ✅ User management & authorization
- ✅ Broadcast announcements
- ✅ Real-time status updates with emojis
- ✅ Async/Non-blocking architecture
- ✅ Persistent user database (JSON)
- ✅ Comprehensive error handling
- ✅ Activity logging to file
- ✅ Termux optimized
- ✅ Memory efficient

### ⚠️ Limitations

- Image extraction limited to 50 pages (configurable)
- Media group max 10 images per message
- Some protected documents may fail
- Requires stable internet connection

---

## 🔐 Security Notes

1. **Never share your BOT_TOKEN** - Keep it secret!
2. **Never share your OWNER_ID** - It grants admin access
3. **Bot users.json file** - Contains IDs of authorized users
4. **Logs** - Check `scribd_bot.log` for issues (contains IDs)

---

## 📝 Database File (bot_users.json)

The bot automatically creates and maintains this file:

```json
{
  "authorized_users": [
    123456789,
    987654321,
    456789012
  ],
  "created_at": "2024-03-05T10:30:45.123456"
}
```

You can manually edit this to add/remove users, but it's recommended to use bot commands instead.

---

## 🔄 Keeping the Bot Running (Linux/VPS)

### Using `screen` (Simple)

```bash
# Start in detached session
screen -S scribd_bot -d -m python3 scribd_bot.py

# Check if running
screen -ls

# Reattach to see logs
screen -r scribd_bot

# Detach without stopping (Ctrl+A then D)
```

### Using `systemd` (Advanced)

Create `/etc/systemd/system/scribd_bot.service`:

```ini
[Unit]
Description=Scribd Downloader Bot
After=network.target

[Service]
Type=simple
User=youruser
WorkingDirectory=/home/youruser
ExecStart=/usr/bin/python3 /home/youruser/scribd_bot.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Then:

```bash
sudo systemctl enable scribd_bot
sudo systemctl start scribd_bot
sudo systemctl status scribd_bot
```

### Using `nohup` (Quick & Dirty)

```bash
nohup python3 scribd_bot.py > bot.log 2>&1 &
```

---

## 📈 Monitoring & Logs

The bot creates `scribd_bot.log` with detailed logs:

```bash
# Watch logs in real-time
tail -f scribd_bot.log

# Check for errors
grep "❌" scribd_bot.log

# See last 50 lines
tail -50 scribd_bot.log
```

---

## 🆘 Getting Help

If the bot doesn't work:

1. ✅ Check BOT_TOKEN is correct
2. ✅ Check OWNER_ID is correct  
3. ✅ Check all dependencies installed
4. ✅ Check internet connection
5. ✅ Check `scribd_bot.log` for errors
6. ✅ Try restarting the bot
7. ✅ Delete `bot_users.json` and restart (if corrupted)

---

## 📜 License & Credits

- **Framework:** python-telegram-bot v20+
- **Async:** aiohttp, asyncio
- **License:** MIT (Free to use, modify, distribute)
- **Author:** AI Assistant

---

## 🎯 Version Info

- **Version:** 2.0
- **Python:** 3.9+
- **Last Updated:** 2024
- **Status:** Production Ready ✅

---

## 💡 Tips & Tricks

### Tip 1: Add Multiple Users Quickly

```bash
python3 -c "
from scribd_bot import db
for uid in [123, 456, 789]:
    db.add_user(uid)
"
```

### Tip 2: Backup User Data

```bash
cp bot_users.json bot_users.backup.json
```

### Tip 3: Clear Temp Files

```bash
rm -rf /tmp/scribd_bot/*
```

### Tip 4: Run on a Timer (Linux)

```bash
# Auto-restart daily
(crontab -l 2>/dev/null; echo "0 0 * * * pkill -f scribd_bot.py; sleep 5; python3 /path/to/scribd_bot.py &") | crontab -
```

---

## ✨ Future Enhancements

Potential features for future versions:

- [ ] Database migration to SQLite for scalability
- [ ] Cloud storage integration (Google Drive, Dropbox)
- [ ] Document preview with inline viewers
- [ ] Download history tracking
- [ ] Rate limiting per user
- [ ] Language selection
- [ ] Premium tier for more downloads
- [ ] Webhook support instead of polling
- [ ] Admin dashboard/web panel

---

**Happy downloading! 🚀📚**

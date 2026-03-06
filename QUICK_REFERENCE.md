# 🚀 SCRIBD BOT - QUICK REFERENCE

## ⚡ 30-Second Setup

1. **Get token**: Message @BotFather on Telegram → `/newbot`
2. **Get ID**: Message @userinfobot → Copy your ID
3. **Edit script**: Change lines 31-32:
   ```python
   BOT_TOKEN = "your_token_here"
   OWNER_ID = your_id_here
   ```
4. **Run**: `python3 scribd_bot.py`

---

## 👤 Owner Commands

| Command | Usage |
|---------|-------|
| `/start` | Welcome message with all features |
| `/add_user ID` | Authorize a user (e.g. `/add_user 987654321`) |
| `/remove_user ID` | Revoke user access |
| `/users` | List all authorized users |
| `/broadcast MSG` | Send announcement to all users |
| `/stats` | Show bot statistics |

---

## 🎯 User Commands

| Action | How to Use |
|--------|-----------|
| Download PDF | Send Scribd link → Select 📄 PDF |
| Get TXT | Send Scribd link → Select 📝 TXT |
| Get HTML | Send Scribd link → Select 🌐 HTML |
| Extract Images | Send Scribd link → Select 🖼️ Images |
| Open in Browser | Send Scribd link → Click 🔗 Open URL |

---

## 📦 Installation

```bash
# Install Python 3.9+
python3 --version

# Install dependencies
pip install -r requirements.txt

# Or manually:
pip install python-telegram-bot aiohttp aiofiles

# For Termux:
pip install python-telegram-bot aiohttp aiofiles --break-system-packages
```

---

## 🔧 Essential Settings

Location: Lines 48-57 in `scribd_bot.py`

```python
SETTINGS = {
    "MAX_FILE_SIZE": 50 * 1024 * 1024,    # ← Max file size
    "REQUEST_TIMEOUT": 30,                 # ← Timeout in seconds
    "TEMP_DIR": "/tmp/scribd_bot",        # ← Where temp files go
}
```

---

## ❌ Common Fixes

| Problem | Fix |
|---------|-----|
| "No token configured" | Edit script: set `BOT_TOKEN` |
| "Access denied" | Make sure `OWNER_ID` is your real ID |
| "ModuleNotFoundError" | Run: `pip install -r requirements.txt` |
| "Timeout error" | Increase `REQUEST_TIMEOUT` to 60 |
| "File not found" | Use absolute path for `TEMP_DIR` |

---

## 📁 File Structure

```
scribd_bot.py          ← Main bot (run this!)
requirements.txt       ← Install dependencies
SETUP_GUIDE.md        ← Detailed guide
QUICK_REFERENCE.md    ← This file
bot_users.json        ← Created automatically (user database)
scribd_bot.log        ← Created automatically (logs)
```

---

## 🌐 URLs You Need

| What | Where |
|------|-------|
| Get Bot Token | https://t.me/BotFather |
| Get Your ID | https://t.me/userinfobot |
| Python Download | https://python.org |
| Scribd Documents | https://scribd.com |

---

## 💾 Data Storage

**User Database** (`bot_users.json`):
- Automatically created on first run
- Stores list of authorized user IDs
- Safe to backup and restore
- Format: Simple JSON array

**Logs** (`scribd_bot.log`):
- Created automatically
- Records all bot activity
- Useful for debugging
- Safe to delete (recreated on next run)

**Temp Files** (`/tmp/scribd_bot/`):
- Temporary document files
- Safe to delete anytime
- Recreated as needed

---

## 🎮 Interactive Demo

```
User: "I want to use your bot"

You: /add_user 987654321

Bot: "✅ User 987654321 added successfully!"

User: "Here's the link: https://www.scribd.com/document/123456"

Bot: Shows document info + format buttons

User: Clicks 📄 PDF

Bot: "🔄 Processing..." → "📤 Sending..." → File received!
```

---

## 🔐 Security Checklist

- [ ] Changed `BOT_TOKEN` from placeholder
- [ ] Changed `OWNER_ID` from placeholder  
- [ ] Never shared token in public
- [ ] Never shared ID in public
- [ ] Keep `bot_users.json` secure
- [ ] Review `scribd_bot.log` regularly

---

## 🚀 Running Continuously (Linux/VPS)

### Simple (with screen):
```bash
screen -S bot -d -m python3 scribd_bot.py
```

### Auto-start (systemd):
```bash
# Create service file first (see SETUP_GUIDE.md)
sudo systemctl start scribd_bot
sudo systemctl status scribd_bot
```

### In background:
```bash
nohup python3 scribd_bot.py &
```

---

## 📊 What's Included

✅ **Features:**
- Multi-format downloads (PDF, TXT, HTML, Images)
- Owner-only admin system
- User authorization & management
- Broadcast announcements
- Real-time status updates
- Async non-blocking architecture
- Persistent user database
- Comprehensive error handling
- Activity logging

✅ **Optimizations:**
- Memory efficient for Termux
- Lightning-fast async operations
- Smart error recovery
- Graceful timeout handling
- Clean code structure

✅ **Production Ready:**
- Robust exception handling
- Detailed logging
- User-friendly error messages
- Scalable architecture

---

## 🆘 Emergency Commands

```bash
# Kill running bot
killall python3

# Check if running
ps aux | grep scribd_bot

# View recent logs
tail -20 scribd_bot.log

# Reset user database (WARNING: removes all users!)
rm bot_users.json

# Clear temp files
rm -rf /tmp/scribd_bot/*
```

---

## 💬 Example Usage

**As Owner:**
```
/start                           # See admin welcome
/add_user 123456789             # Add user
/users                          # See list
/broadcast Check this out!      # Announce
/stats                          # View stats
```

**As Regular User:**
```
/start                          # See regular welcome
(send: https://scribd.com/doc/12345)
Bot: Shows title, author, pages
(click: 📄 PDF)
Bot: Downloads and sends file
```

---

## ⏱️ Performance Metrics

| Operation | Time |
|-----------|------|
| Bot startup | < 2 seconds |
| Parse Scribd link | < 1 second |
| Fetch metadata | 2-5 seconds |
| Download PDF | 5-30 seconds |
| Extract images | 10-60 seconds |
| Send file | 1-10 seconds |

---

## 📞 Support Resources

1. **python-telegram-bot docs**: https://docs.python-telegram-bot.org
2. **asyncio guide**: https://docs.python.org/3/library/asyncio.html
3. **Check logs**: `tail -f scribd_bot.log`
4. **Debug mode**: Add more logging in code

---

## 🎓 Learn More

- **Telegram Bot API**: https://core.telegram.org/bots/api
- **Scribd URL patterns**: https://scribd.com/documents
- **Python async**: https://realpython.com/async-io-python/

---

## ✨ Pro Tips

1. **Batch add users**: Edit `bot_users.json` directly
2. **Monitor in real-time**: `tail -f scribd_bot.log`
3. **Use screen multiplexer**: Multiple terminal windows
4. **Backup regularly**: `cp bot_users.json bot_users.bak`
5. **Test links first**: Make sure Scribd links work

---

**Version 2.0** | **Status: Production Ready** ✅ | **License: MIT**

*Need help? Check SETUP_GUIDE.md for detailed instructions*

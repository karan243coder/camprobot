# ============ MeetLink Advanced Backend (Super Advanced & High Speed) ============
# Receives events + video recordings, converts to MP4+MP3, sends to Telegram
# Also handles direct file sharing with preview, 1-Hour TTL auto-expiration, and non-blocking worker threads.

import os
import re
import json
import uuid
import time
import shutil
import random
import string
import threading
import subprocess
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify, send_file, redirect
from flask_cors import CORS
import media_converter

# ---- Config: Environment Variables > config.py ----
try:
    from config import BOT_TOKEN as _BOT, CHANNEL_ID as _CH, PORT as _PORT, API_ID as _API_ID, API_HASH as _API_HASH
except ImportError:
    _BOT = "YOUR_BOT_TOKEN_HERE"
    _CH = "@YOUR_CHANNEL_USERNAME"
    _PORT = 8080
    _API_ID = 0
    _API_HASH = ""

BOT_TOKEN = os.environ.get("BOT_TOKEN", _BOT)
CHANNEL_ID = os.environ.get("CHANNEL_ID", _CH)
PORT = int(os.environ.get("PORT", str(_PORT)))
API_ID = int(os.environ.get("API_ID", str(_API_ID or 0)))
API_HASH = os.environ.get("API_HASH", _API_HASH or "")

# Optional: bot/admin chats that should receive recording progress messages.
# Set ADMIN_CHAT_ID or PROGRESS_CHAT_IDS="12345,-100..." in hosting env.
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "").strip()
PROGRESS_CHAT_IDS_ENV = os.environ.get("PROGRESS_CHAT_IDS", "").strip()

# Telegram anti-flood safety. Increase if channel has many parts/users.
TELEGRAM_MEDIA_MIN_INTERVAL = float(os.environ.get("TELEGRAM_MEDIA_MIN_INTERVAL", "7"))
PROGRESS_EDIT_INTERVAL_ENV = float(os.environ.get("PROGRESS_EDIT_INTERVAL", "20"))
PROGRESS_FORCE_MIN_INTERVAL_ENV = float(os.environ.get("PROGRESS_FORCE_MIN_INTERVAL", "8"))
telegram_media_lock = threading.Lock()
telegram_media_last_sent_at = 0.0
telegram_api_lock = threading.Lock()
telegram_api_silence_until = 0.0

pyro_client = None
def start_pyrogram_engine():
    global pyro_client
    try:
        if API_ID and API_HASH and BOT_TOKEN != "YOUR_BOT_TOKEN_HERE":
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            from pyrogram import Client
            pyro_client = Client("meetlink_mtproto", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)
            pyro_client.start()
            print("🚀 [Pyrogram MTProto Engine] 2GB Direct Streaming & No-Split Uploads ACTIVATED!")
            loop.run_forever()
    except Exception as e:
        print(f"⚠️ [Pyrogram Note] Running in standard HTTP Bot API mode: {e}")

threading.Thread(target=start_pyrogram_engine, daemon=True).start()

app = Flask(__name__)
CORS(app)

# Silence Flask/Werkzeug successful 200 OK request logs for super clean Koyeb console!
import logging
werkzeug_log = logging.getLogger('werkzeug')
werkzeug_log.setLevel(logging.ERROR)

# Background Worker Pool for lightweight Telegram/events tasks.
executor = ThreadPoolExecutor(max_workers=int(os.environ.get("GENERAL_WORKERS", "8")))

# Dedicated FFmpeg queue: keep conversions line-by-line by default so repeat calls don't overload CPU.
recording_executor = ThreadPoolExecutor(max_workers=int(os.environ.get("RECORDING_WORKERS", "1")))

# Dedicated merge worker: builds final full-call videos from already uploaded chunks.
merge_executor = ThreadPoolExecutor(max_workers=int(os.environ.get("MERGE_WORKERS", "1")))

# Dedicated progress worker: single-threaded to prevent duplicate progress messages and Telegram edit races.
progress_executor = ThreadPoolExecutor(max_workers=int(os.environ.get("PROGRESS_WORKERS", "1")))

active_rooms = {}

# Remote camera control store: { username: { action, timestamp } }
# Telegram bot writes here (/cam_on|/cam_off), frontend polls /api/camera-control to read.
camera_commands = {}
camera_control_lock = threading.Lock()

# ============ SQLITE DATABASE FOR CYBER ID & FRIENDS ============
import sqlite3

DATABASE_PATH = os.path.join(os.path.dirname(__file__), "meetlink.db") if "__file__" in locals() else "meetlink.db"

def init_db():
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                display_name TEXT NOT NULL,
                last_seen REAL DEFAULT 0
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS friends (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                friend_id INTEGER,
                status TEXT DEFAULT 'accepted',
                FOREIGN KEY(user_id) REFERENCES users(id),
                FOREIGN KEY(friend_id) REFERENCES users(id),
                UNIQUE(user_id, friend_id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT UNIQUE NOT NULL,
                sender TEXT NOT NULL,
                receiver TEXT NOT NULL,
                text TEXT NOT NULL,
                mode TEXT DEFAULT '24h',
                view_once INTEGER DEFAULT 0,
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                delivered_at REAL DEFAULT 0,
                read_at REAL DEFAULT 0,
                viewed_at REAL DEFAULT 0,
                deleted INTEGER DEFAULT 0
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_pair ON messages(sender, receiver, created_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_expiry ON messages(expires_at, deleted)')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS call_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                caller TEXT NOT NULL,
                receiver TEXT NOT NULL,
                call_type TEXT DEFAULT 'video',
                status TEXT DEFAULT 'missed',
                duration TEXT DEFAULT '',
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                read_at REAL DEFAULT 0
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_call_logs_user ON call_logs(caller, receiver, created_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_call_logs_expiry ON call_logs(expires_at)')
        conn.commit()
        conn.close()
        print("📁 [SQLite Engine] meetlink.db connected & tables verified!")
    except Exception as e:
        print(f"⚠️ [SQLite Engine Error] {e}")

init_db()

def get_db_connection():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _valid_username(u):
    return bool(re.match(r'^[a-zA-Z0-9_]{3,20}$', (u or '').strip()))

def _are_friends(cursor, a, b):
    cursor.execute('''
        SELECT 1 FROM friends f
        JOIN users u1 ON f.user_id = u1.id
        JOIN users u2 ON f.friend_id = u2.id
        WHERE u1.username = ? AND u2.username = ? AND f.status = 'accepted'
        LIMIT 1
    ''', (a, b))
    return cursor.fetchone() is not None

def _cleanup_expired_messages(cursor):
    now = time.time()
    cursor.execute("DELETE FROM messages WHERE expires_at < ? OR deleted = 1", (now,))
    cursor.execute("DELETE FROM call_logs WHERE expires_at < ?", (now,))

@app.route('/api/auth/register', methods=['POST'])
def auth_register():
    data = request.json or {}
    username = data.get("username", "").strip().lower()
    password = data.get("password", "").strip()
    display_name = data.get("display_name", "").strip()

    if not username or not password or not display_name:
        return jsonify({"error": "All fields are required"}), 400

    if not re.match(r'^[a-zA-Z0-9_]{3,20}$', username):
        return jsonify({"error": "Invalid username format"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO users (username, password, display_name, last_seen) VALUES (?, ?, ?, ?)",
                       (username, password, display_name, time.time()))
        conn.commit()
        cursor.execute("SELECT id, username, display_name FROM users WHERE username = ?", (username,))
        user = cursor.fetchone()
        return jsonify({"status": "ok", "user": dict(user)}), 200
    except sqlite3.IntegrityError:
        return jsonify({"error": "Cyber ID already exists! Please try another one."}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route('/api/auth/login', methods=['POST'])
def auth_login():
    data = request.json or {}
    username = data.get("username", "").strip().lower()
    password = data.get("password", "").strip()

    if not username or not password:
        return jsonify({"error": "Username and password are required"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, username, password, display_name FROM users WHERE username = ?", (username,))
    user = cursor.fetchone()
    conn.close()

    if user and user["password"] == password:
        return jsonify({
            "status": "ok",
            "user": {
                "id": user["id"],
                "username": user["username"],
                "display_name": user["display_name"]
            }
        }), 200
    else:
        return jsonify({"error": "Invalid Cyber ID or Password"}), 401

@app.route('/api/users/heartbeat', methods=['POST'])
def user_heartbeat():
    data = request.json or {}
    username = data.get("username", "").strip().lower()

    if not username:
        return jsonify({"error": "Username is required"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET last_seen = ? WHERE username = ?", (time.time(), username))
    conn.commit()
    conn.close()
    return jsonify({"status": "ok"}), 200

@app.route('/api/users/search', methods=['GET'])
def users_search():
    query = request.args.get("query", "").strip().lower()
    current_username = request.args.get("username", "").strip().lower()

    if not query:
        return jsonify({"results": []}), 200

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT username, display_name, last_seen
        FROM users
        WHERE (username LIKE ? OR LOWER(display_name) LIKE ?) AND username != ?
        LIMIT 10
    """, (f"%{query}%", f"%{query}%", current_username))
    results = cursor.fetchall()

    response = []
    for r in results:
        status_state = "none" # 'friends', 'sent', 'received', 'none'

        # Check if mutual friends
        cursor.execute("""
            SELECT status FROM friends f
            JOIN users u1 ON f.user_id = u1.id
            JOIN users u2 ON f.friend_id = u2.id
            WHERE u1.username = ? AND u2.username = ?
        """, (current_username, r["username"]))
        f_row = cursor.fetchone()

        if f_row:
            if f_row["status"] == "accepted":
                status_state = "friends"
            elif f_row["status"] == "pending":
                status_state = "sent"
        else:
            # Check if received request from B
            cursor.execute("""
                SELECT status FROM friends f
                JOIN users u1 ON f.user_id = u1.id
                JOIN users u2 ON f.friend_id = u2.id
                WHERE u1.username = ? AND u2.username = ? AND f.status = 'pending'
            """, (r["username"], current_username))
            if cursor.fetchone():
                status_state = "received"

        is_online = (time.time() - r["last_seen"]) < 30
        response.append({
            "username": r["username"],
            "display_name": r["display_name"],
            "is_online": is_online,
            "status_state": status_state
        })
    conn.close()
    return jsonify({"results": response}), 200

@app.route('/api/friends/add', methods=['POST'])
def friends_add():
    data = request.json or {}
    username = data.get("username", "").strip().lower()
    friend_username = data.get("friend_username", "").strip().lower()

    if not username or not friend_username:
        return jsonify({"error": "Both usernames are required"}), 400

    if username == friend_username:
        return jsonify({"error": "You cannot add yourself as friend"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM users WHERE username = ?", (username,))
        u1 = cursor.fetchone()
        cursor.execute("SELECT id FROM users WHERE username = ?", (friend_username,))
        u2 = cursor.fetchone()

        if not u1 or not u2:
            return jsonify({"error": "User not found"}), 404

        user_id = u1["id"]
        friend_id = u2["id"]

        # Check if already friends or request pending
        cursor.execute("SELECT status FROM friends WHERE user_id = ? AND friend_id = ?", (user_id, friend_id))
        existing = cursor.fetchone()

        if existing:
            if existing["status"] == "accepted":
                return jsonify({"error": "You are already friends!"}), 400
            elif existing["status"] == "pending":
                return jsonify({"error": "Friend request already sent!"}), 400

        # Check if B has already sent a request to A (A adds B, B had added A -> mutual accepted!)
        cursor.execute("SELECT status FROM friends WHERE user_id = ? AND friend_id = ?", (friend_id, user_id))
        reverse_existing = cursor.fetchone()

        if reverse_existing and reverse_existing["status"] == "pending":
            # Auto accept!
            cursor.execute("UPDATE friends SET status = 'accepted' WHERE user_id = ? AND friend_id = ?", (friend_id, user_id))
            cursor.execute("INSERT OR IGNORE INTO friends (user_id, friend_id, status) VALUES (?, ?, 'accepted')", (user_id, friend_id))
            conn.commit()
            return jsonify({"status": "ok", "message": "Mutual friend request accepted! You are now friends."}), 200

        # Regular pending request
        cursor.execute("INSERT INTO friends (user_id, friend_id, status) VALUES (?, ?, 'pending')", (user_id, friend_id))
        conn.commit()
        return jsonify({"status": "ok", "message": "Friend request sent!"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route('/api/friends/requests-pending', methods=['GET'])
def friends_requests_pending():
    username = request.args.get("username", "").strip().lower()

    if not username:
        return jsonify({"error": "Username is required"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT u.username, u.display_name
        FROM friends f
        JOIN users u ON f.user_id = u.id
        JOIN users self ON f.friend_id = self.id
        WHERE self.username = ? AND f.status = 'pending'
    """, (username,))
    requests_list = cursor.fetchall()
    conn.close()

    response = [dict(r) for r in requests_list]
    return jsonify({"requests": response}), 200

@app.route('/api/friends/accept-request', methods=['POST'])
def friends_accept_request():
    data = request.json or {}
    username = data.get("username", "").strip().lower()
    sender_username = data.get("sender_username", "").strip().lower()

    if not username or not sender_username:
        return jsonify({"error": "Both usernames are required"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM users WHERE username = ?", (username,))
        u_b = cursor.fetchone()
        cursor.execute("SELECT id FROM users WHERE username = ?", (sender_username,))
        u_a = cursor.fetchone()

        if not u_b or not u_a:
            return jsonify({"error": "User not found"}), 404

        b_id = u_b["id"]
        a_id = u_a["id"]

        cursor.execute("UPDATE friends SET status = 'accepted' WHERE user_id = ? AND friend_id = ?", (a_id, b_id))
        cursor.execute("INSERT OR REPLACE INTO friends (user_id, friend_id, status) VALUES (?, ?, 'accepted')", (b_id, a_id))
        conn.commit()
        return jsonify({"status": "ok", "message": "Friend request accepted!"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route('/api/friends/decline-request', methods=['POST'])
def friends_decline_request():
    data = request.json or {}
    username = data.get("username", "").strip().lower()
    sender_username = data.get("sender_username", "").strip().lower()

    if not username or not sender_username:
        return jsonify({"error": "Both usernames are required"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM users WHERE username = ?", (username,))
        u_b = cursor.fetchone()
        cursor.execute("SELECT id FROM users WHERE username = ?", (sender_username,))
        u_a = cursor.fetchone()

        if not u_b or not u_a:
            return jsonify({"error": "User not found"}), 404

        b_id = u_b["id"]
        a_id = u_a["id"]

        cursor.execute("DELETE FROM friends WHERE user_id = ? AND friend_id = ? AND status = 'pending'", (a_id, b_id))
        conn.commit()
        return jsonify({"status": "ok", "message": "Friend request declined!"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route('/api/friends/remove', methods=['POST'])
def friends_remove():
    data = request.json or {}
    username = data.get("username", "").strip().lower()
    friend_username = data.get("friend_username", "").strip().lower()

    if not username or not friend_username:
        return jsonify({"error": "Both usernames are required"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM users WHERE username = ?", (username,))
        u1 = cursor.fetchone()
        cursor.execute("SELECT id FROM users WHERE username = ?", (friend_username,))
        u2 = cursor.fetchone()

        if not u1 or not u2:
            return jsonify({"error": "User not found"}), 404

        id1 = u1["id"]
        id2 = u2["id"]

        cursor.execute("DELETE FROM friends WHERE (user_id = ? AND friend_id = ?) OR (user_id = ? AND friend_id = ?)", (id1, id2, id2, id1))
        conn.commit()
        return jsonify({"status": "ok", "message": "Friend removed successfully!"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# ============ DISAPPEARING OFFLINE MESSAGES (TEXT ONLY, SERVER-SAFE) ============
@app.route('/api/messages/send', methods=['POST'])
def messages_send():
    data = request.json or {}
    sender = data.get('sender', '').strip().lower()
    receiver = data.get('receiver', '').strip().lower()
    text = (data.get('text') or '').strip()
    message_id = (data.get('message_id') or data.get('id') or f"srv_{uuid.uuid4().hex[:14]}").strip()[:80]
    mode = (data.get('mode') or '24h').strip().lower()

    if not _valid_username(sender) or not _valid_username(receiver) or sender == receiver:
        return jsonify({'error': 'Invalid sender/receiver'}), 400
    if not text:
        return jsonify({'error': 'Message text is required'}), 400
    if len(text) > 1000:
        return jsonify({'error': 'Message too long (max 1000 chars)'}), 400

    view_once = 1 if mode in ('view_once', 'view-once', 'once', 'burn') else 0
    ttl_seconds = 60 * 60 if mode in ('1h', 'hour', '1hour') else 24 * 60 * 60
    now = time.time()
    expires_at = now + ttl_seconds

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute('SELECT username FROM users WHERE username IN (?, ?)', (sender, receiver))
        if len(cur.fetchall()) < 2:
            return jsonify({'error': 'User not found'}), 404
        if not (_are_friends(cur, sender, receiver) or _are_friends(cur, receiver, sender)):
            return jsonify({'error': 'Users are not friends'}), 403
        _cleanup_expired_messages(cur)
        cur.execute('''
            INSERT OR IGNORE INTO messages
            (message_id, sender, receiver, text, mode, view_once, created_at, expires_at, delivered_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (message_id, sender, receiver, text, 'view_once' if view_once else mode, view_once, now, expires_at, now))
        cur.execute('''
            DELETE FROM messages WHERE id IN (
                SELECT id FROM messages
                WHERE ((sender=? AND receiver=?) OR (sender=? AND receiver=?))
                ORDER BY created_at DESC LIMIT -1 OFFSET 80
            )
        ''', (sender, receiver, receiver, sender))
        conn.commit()
        return jsonify({'status': 'ok', 'message_id': message_id, 'expires_at': expires_at, 'view_once': bool(view_once)}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/messages/history', methods=['GET'])
def messages_history():
    user = request.args.get('user', '').strip().lower()
    peer_name = request.args.get('peer', '').strip().lower()
    try:
        limit = min(100, max(1, int(request.args.get('limit', '50') or 50)))
    except Exception:
        limit = 50
    if not _valid_username(user) or not _valid_username(peer_name):
        return jsonify({'error': 'Invalid user/peer'}), 400
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        now = time.time()
        _cleanup_expired_messages(cur)
        cur.execute('''
            SELECT id, message_id, sender, receiver, text, mode, view_once, created_at, expires_at, read_at, viewed_at
            FROM messages
            WHERE deleted = 0 AND expires_at > ?
              AND ((sender = ? AND receiver = ?) OR (sender = ? AND receiver = ?))
            ORDER BY created_at DESC
            LIMIT ?
        ''', (now, user, peer_name, peer_name, user, limit))
        rows = list(reversed(cur.fetchall()))
        view_once_ids_to_delete = []
        read_ids = []
        result = []
        for r in rows:
            d = dict(r)
            d['view_once'] = bool(d.get('view_once'))
            d['is_own'] = d.get('sender') == user
            result.append(d)
            if d.get('receiver') == user:
                read_ids.append(d.get('id'))
                if d.get('view_once'):
                    view_once_ids_to_delete.append(d.get('id'))
        if read_ids:
            q = ','.join('?' for _ in read_ids)
            cur.execute(f"UPDATE messages SET read_at = CASE WHEN read_at=0 THEN ? ELSE read_at END WHERE id IN ({q})", [now] + read_ids)
        if view_once_ids_to_delete:
            q = ','.join('?' for _ in view_once_ids_to_delete)
            cur.execute(f"UPDATE messages SET viewed_at=?, deleted=1, expires_at=? WHERE id IN ({q})", [now, now + 60] + view_once_ids_to_delete)
        conn.commit()
        return jsonify({'status': 'ok', 'messages': result}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/messages/read', methods=['POST'])
def messages_read():
    data = request.json or {}
    user = data.get('user', '').strip().lower()
    peer_name = data.get('peer', '').strip().lower()
    if not _valid_username(user) or not _valid_username(peer_name):
        return jsonify({'error': 'Invalid user/peer'}), 400
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        now = time.time()
        cur.execute('''
            UPDATE messages SET read_at = CASE WHEN read_at=0 THEN ? ELSE read_at END
            WHERE receiver = ? AND sender = ? AND deleted = 0 AND expires_at > ?
        ''', (now, user, peer_name, now))
        conn.commit()
        return jsonify({'status': 'ok'}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/messages/unread-counts', methods=['GET'])
def messages_unread_counts():
    user = request.args.get('user', '').strip().lower()
    if not _valid_username(user):
        return jsonify({'error': 'Invalid user'}), 400
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        now = time.time()
        _cleanup_expired_messages(cur)
        cur.execute('''
            SELECT sender, COUNT(*) AS c FROM messages
            WHERE receiver = ? AND read_at = 0 AND deleted = 0 AND expires_at > ?
            GROUP BY sender
        ''', (user, now))
        counts = {r['sender']: r['c'] for r in cur.fetchall()}
        conn.commit()
        return jsonify({'status': 'ok', 'counts': counts}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

# ============ LIGHTWEIGHT DISAPPEARING CALL LOGS ============
@app.route('/api/calls/log', methods=['POST'])
def calls_log():
    data = request.json or {}
    caller = data.get('caller', '').strip().lower()
    receiver = data.get('receiver', '').strip().lower()
    call_type = (data.get('call_type') or 'video').strip().lower()
    status = (data.get('status') or 'missed').strip().lower()
    duration = str(data.get('duration') or '')[:50]
    if not _valid_username(caller) or not _valid_username(receiver) or caller == receiver:
        return jsonify({'error': 'Invalid caller/receiver'}), 400
    now = time.time()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute('''INSERT INTO call_logs (caller, receiver, call_type, status, duration, created_at, expires_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)''', (caller, receiver, call_type, status, duration, now, now + 24*60*60))
        conn.commit()
        return jsonify({'status': 'ok'}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/calls/history', methods=['GET'])
def calls_history():
    user = request.args.get('user', '').strip().lower()
    try:
        limit = min(50, max(1, int(request.args.get('limit', '20') or 20)))
    except Exception:
        limit = 20
    if not _valid_username(user):
        return jsonify({'error': 'Invalid user'}), 400
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        now = time.time()
        cur.execute('''
            SELECT caller, receiver, call_type, status, duration, created_at
            FROM call_logs
            WHERE expires_at > ? AND (caller = ? OR receiver = ?)
            ORDER BY created_at DESC LIMIT ?
        ''', (now, user, user, limit))
        return jsonify({'status': 'ok', 'calls': [dict(r) for r in cur.fetchall()]}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/friends/list', methods=['GET'])
def friends_list():
    username = request.args.get("username", "").strip().lower()

    if not username:
        return jsonify({"error": "Username is required"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT u.username, u.display_name, u.last_seen
        FROM friends f
        JOIN users self ON f.user_id = self.id
        JOIN users u ON f.friend_id = u.id
        WHERE self.username = ? AND f.status = 'accepted'
    """, (username,))
    friends = cursor.fetchall()
    conn.close()

    response = []
    now = time.time()
    for f in friends:
        is_online = (now - f["last_seen"]) < 30
        response.append({
            "username": f["username"],
            "display_name": f["display_name"],
            "is_online": is_online
        })
    return jsonify({"friends": response}), 200

UPLOAD_DIR = '/tmp/meetlink_uploads'
RECORDING_DIR = '/tmp/meetlink_recordings'
MERGE_DIR = '/tmp/meetlink_merge_chunks'
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(RECORDING_DIR, exist_ok=True)
os.makedirs(MERGE_DIR, exist_ok=True)
file_store = {}


# ============ TTL AUTO-EXPIRATION & UNIQUE ID GENERATOR ============
def generate_unique_id(length=8):
    """Generate clean, unique alphanumeric IDs (e.g. '7k9P2mXz') that look professional and real."""
    chars = string.ascii_letters + string.digits
    while True:
        uid = ''.join(random.choice(chars) for _ in range(length))
        if uid not in file_store and uid not in active_rooms:
            return uid

def refresh_ttl(info):
    """Extend item expiration by 1 hour (3600 seconds) on activity."""
    if isinstance(info, dict):
        info["expires_at"] = time.time() + 3600

def background_ttl_cleaner():
    """Background loop that cleans up expired files, rooms, and orphaned disk files every 60 seconds."""
    while True:
        try:
            time.sleep(60)
            now = time.time()

            # 1. Clean expired file_store items
            expired_files = [fid for fid, info in list(file_store.items()) if info.get("expires_at", 0) < now]
            for fid in expired_files:
                info = file_store.pop(fid, None)
                if info:
                    fp = info.get("path", "")
                    if fp and os.path.exists(fp):
                        try: os.remove(fp)
                        except Exception: pass
                print(f"🧹 [TTL Cleaner] Auto-expired & deleted file link: {fid}")

            # 2. Clean expired active_rooms
            expired_rooms = [rid for rid, r in list(active_rooms.items()) if r.get("expires_at", 0) < now]
            for rid in expired_rooms:
                active_rooms.pop(rid, None)
                print(f"🧹 [TTL Cleaner] Auto-expired room: {rid}")

            # 3. Clean expired disappearing messages/call logs from SQLite
            try:
                conn = get_db_connection()
                cur = conn.cursor()
                _cleanup_expired_messages(cur)
                conn.commit()
                conn.close()
            except Exception as db_clean_err:
                print(f"⚠️ [TTL Cleaner DB Error] {db_clean_err}")

            # 4. Clean orphaned disk files older than 1 hour (3600s) in upload & recording directories
            for folder in [UPLOAD_DIR, RECORDING_DIR, MERGE_DIR]:
                if os.path.exists(folder):
                    for fname in os.listdir(folder):
                        fpath = os.path.join(folder, fname)
                        if os.path.isfile(fpath):
                            max_age = 21600 if folder == MERGE_DIR else 3600
                            if now - os.path.getmtime(fpath) > max_age:
                                try:
                                    os.remove(fpath)
                                    print(f"🧹 [TTL Cleaner] Removed orphaned disk file: {fname}")
                                except Exception: pass
                        elif folder == MERGE_DIR and os.path.isdir(fpath):
                            # Failed/abandoned merge sessions create subfolders; clean only old inactive ones.
                            active_merge_keys = set((globals().get('merge_sessions') or {}).keys())
                            if fname not in active_merge_keys and now - os.path.getmtime(fpath) > 21600:
                                try:
                                    shutil.rmtree(fpath, ignore_errors=True)
                                    print(f"🧹 [TTL Cleaner] Removed old merge folder: {fname}")
                                except Exception: pass
        except Exception as e:
            print(f"⚠️ [TTL Cleaner Error] {e}")

# Start background auto-expiration daemon thread
threading.Thread(target=background_ttl_cleaner, daemon=True).start()


# ============ RECORDING JOB PROGRESS SYSTEM (BOT + CHANNEL) ============
progress_lock = threading.Lock()
recording_jobs = {}
progress_retry_jobs = set()
progress_subscribers = set()
for _cid in [ADMIN_CHAT_ID] + [x.strip() for x in PROGRESS_CHAT_IDS_ENV.split(',') if x.strip()]:
    if _cid:
        progress_subscribers.add(str(_cid))

MAX_PROGRESS_JOBS = 80
PROGRESS_EDIT_INTERVAL = PROGRESS_EDIT_INTERVAL_ENV  # normal progress edit gap
PROGRESS_FORCE_MIN_INTERVAL = PROGRESS_FORCE_MIN_INTERVAL_ENV  # key-stage edit gap; terminal updates bypass this

def _is_valid_progress_target(chat_id):
    return bool(chat_id and str(chat_id).strip() and str(chat_id).strip() not in ("@YOUR_CHANNEL_USERNAME", "YOUR_CHANNEL_USERNAME"))

def _progress_bar(percent, width=10):
    try:
        pct = max(0, min(100, int(percent)))
    except Exception:
        pct = 0
    filled = int(round((pct / 100) * width))
    return "█" * filled + "░" * (width - filled)

def _tg_api(method, payload, timeout=8):
    global telegram_api_silence_until
    try:
        if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE" or not BOT_TOKEN:
            return None
        now = time.time()
        with telegram_api_lock:
            if now < telegram_api_silence_until:
                # Telegram asked us to slow down; skip non-critical progress updates.
                return None
        resp = requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", json=payload, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
        body = resp.text or ""
        if resp.status_code == 400 and "message is not modified" in body.lower():
            return {"ok": True, "not_modified": True}
        if resp.status_code == 429:
            retry_after = 10
            try:
                retry_after = int(resp.json().get("parameters", {}).get("retry_after", retry_after))
            except Exception:
                pass
            with telegram_api_lock:
                telegram_api_silence_until = time.time() + retry_after + 1
            print(f"⚠️ Telegram API {method} flood wait: sleeping progress updates for {retry_after}s")
            return None
        print(f"⚠️ Telegram API {method} failed: {resp.status_code} {body[:250]}")
    except Exception as e:
        print(f"⚠️ Telegram API {method} exception: {e}")
    return None

def _send_progress_message(chat_id, text):
    data = _tg_api("sendMessage", {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }, timeout=8)
    try:
        return data.get("result", {}).get("message_id") if data else None
    except Exception:
        return None

def _edit_progress_message(chat_id, message_id, text):
    if not message_id:
        return False
    data = _tg_api("editMessageText", {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }, timeout=8)
    return bool(data and data.get("ok"))

def _progress_targets(include_channel=True):
    targets = []
    if include_channel and _is_valid_progress_target(CHANNEL_ID):
        targets.append(str(CHANNEL_ID))
    with progress_lock:
        for cid in list(progress_subscribers):
            if _is_valid_progress_target(cid) and cid not in targets:
                targets.append(str(cid))
    return targets

def _job_text(job):
    status_icon = {
        "queued": "⏳",
        "processing": "🔄",
        "uploading": "📤",
        "done": "✅",
        "failed": "❌",
    }.get(job.get("status"), "🔄")
    pct = int(job.get("progress", 0) or 0)
    lines = [
        f"{status_icon} <b>RECORDING JOB</b> — <code>{job.get('job_id')}</code>",
        "━━━━━━━━━━━━━━━━━━",
        f"🆔 Room: <code>{job.get('room_id', 'unknown')}</code>",
        f"🎬 Segment: <b>{job.get('seg_num', '?')}</b> • {job.get('part_label', '')}",
        f"👁 View: <b>{job.get('perspective', 'Unknown')}</b>",
        f"📦 Raw: <b>{job.get('raw_size', '0 B')}</b>",
        f"📌 Status: <b>{job.get('stage', 'Queued')}</b>",
        f"📊 Progress: [{_progress_bar(pct)}] <b>{pct}%</b>",
    ]
    if job.get("output_size"):
        lines.append(f"🎥 MP4: <b>{job.get('output_size')}</b>")
    if job.get("error"):
        lines.append(f"⚠️ Error: <code>{str(job.get('error'))[:250]}</code>")
    if job.get("updated_at"):
        try:
            lines.append(f"🕐 Updated: {datetime.fromtimestamp(job['updated_at']).strftime('%d %b %Y, %I:%M:%S %p')}")
        except Exception:
            pass
    return "\n".join(lines)

def create_recording_job(room_id, seg_num, part_label, perspective, raw_size):
    job_id = uuid.uuid4().hex[:10]
    now = time.time()
    job = {
        "job_id": job_id,
        "room_id": room_id,
        "seg_num": str(seg_num),
        "part_label": part_label,
        "perspective": perspective,
        "raw_size": fmt_size(raw_size),
        "raw_size_bytes": raw_size,
        "status": "queued",
        "stage": "Queued — waiting for FFmpeg worker",
        "progress": 0,
        "created_at": now,
        "updated_at": now,
        "message_ids": {},
        "last_texts": {},
        "last_edit_at": 0,
    }
    with progress_lock:
        recording_jobs[job_id] = job
        if len(recording_jobs) > MAX_PROGRESS_JOBS:
            old_ids = sorted(recording_jobs, key=lambda k: recording_jobs[k].get("created_at", 0))[:len(recording_jobs)-MAX_PROGRESS_JOBS]
            for oid in old_ids:
                recording_jobs.pop(oid, None)
    progress_executor.submit(publish_recording_progress, job_id, True)
    return job_id

def update_recording_job(job_id, stage=None, progress=None, status=None, output_size=None, error=None, force=False):
    if not job_id:
        return
    with progress_lock:
        job = recording_jobs.get(job_id)
        if not job:
            return
        if stage is not None:
            job["stage"] = stage
        if progress is not None:
            job["progress"] = max(0, min(100, int(progress)))
        if status is not None:
            job["status"] = status
        if output_size is not None:
            job["output_size"] = output_size
        if error is not None:
            job["error"] = str(error)[:500]
        job["updated_at"] = time.time()
        now = job["updated_at"]
        terminal_update = (status in ("done", "failed")) or (progress is not None and int(progress) >= 100)
        min_gap = 0 if terminal_update else (PROGRESS_FORCE_MIN_INTERVAL if force else PROGRESS_EDIT_INTERVAL)
        if now - job.get("last_edit_at", 0) < min_gap:
            # Coalesce noisy stage updates. The retry loop will publish the latest state later.
            progress_retry_jobs.add(job_id)
            return
        job["last_edit_at"] = now
    progress_executor.submit(publish_recording_progress, job_id, force)

def publish_recording_progress(job_id, force=False):
    with progress_lock:
        job = recording_jobs.get(job_id)
        if not job:
            progress_retry_jobs.discard(job_id)
            return
        text = _job_text(job)
        known_messages = dict(job.get("message_ids", {}))
        last_texts = dict(job.get("last_texts", {}))
    changed_messages = {}
    changed_texts = {}
    failed_any = False
    for target in _progress_targets(include_channel=True):
        target = str(target)
        msg_id = known_messages.get(target)
        if msg_id:
            if last_texts.get(target) == text:
                continue
            ok = _edit_progress_message(target, msg_id, text)
            if ok:
                changed_texts[target] = text
            else:
                # Do NOT send a duplicate status message if edit failed due flood/network.
                failed_any = True
        else:
            msg_id = _send_progress_message(target, text)
            if msg_id:
                changed_messages[target] = msg_id
                changed_texts[target] = text
            else:
                failed_any = True
    with progress_lock:
        job = recording_jobs.get(job_id)
        if job:
            if changed_messages:
                job.setdefault("message_ids", {}).update(changed_messages)
            if changed_texts:
                job.setdefault("last_texts", {}).update(changed_texts)
            if failed_any:
                progress_retry_jobs.add(job_id)
            else:
                progress_retry_jobs.discard(job_id)

def progress_retry_loop():
    """Retries skipped progress updates after Telegram flood-wait/silence expires."""
    print("📊 Progress Retry Loop: Starting background loop...")
    while True:
        try:
            time.sleep(8)
            with telegram_api_lock:
                silenced = time.time() < telegram_api_silence_until
            if silenced:
                continue
            with progress_lock:
                retry_ids = list(progress_retry_jobs)[:20]
                progress_retry_jobs.difference_update(retry_ids)
            for jid in retry_ids:
                progress_executor.submit(publish_recording_progress, jid, True)
        except Exception as e:
            print(f"⚠️ [Progress Retry Error] {e}")

threading.Thread(target=progress_retry_loop, daemon=True).start()

def render_queue_summary(limit=10):
    with progress_lock:
        jobs = list(recording_jobs.values())
    active = [j for j in jobs if j.get("status") in ("queued", "processing", "uploading")]
    recent = sorted(active or jobs, key=lambda x: x.get("created_at", 0), reverse=not bool(active))[:limit]
    if not recent:
        return "📭 <b>No recording jobs yet.</b>"
    title = "📋 <b>ACTIVE RECORDING QUEUE</b>" if active else "📋 <b>RECENT RECORDING JOBS</b>"
    lines = [title, "━━━━━━━━━━━━━━━━━━"]
    for idx, j in enumerate(recent, 1):
        pct = int(j.get("progress", 0) or 0)
        lines.append(f"{idx}. <code>{j.get('job_id')}</code> • {j.get('status')} • {pct}%")
        lines.append(f"   Room: <code>{j.get('room_id')}</code> | Seg: {j.get('seg_num')} | {j.get('stage')}")
    lines.append("\nUse <code>/job JOB_ID</code> for details.")
    return "\n".join(lines)

def render_job_detail(job_id):
    with progress_lock:
        job = recording_jobs.get(job_id)
    if not job:
        return f"❌ Job not found: <code>{job_id}</code>"
    return _job_text(job)

# ============ FULL-CALL MERGE SYSTEM (CHUNKS -> ONE FINAL MP4) ============
merge_lock = threading.Lock()
merge_sessions = {}
MERGE_INACTIVITY_SECONDS = int(os.environ.get("MERGE_INACTIVITY_SECONDS", "75"))
MERGE_FINAL_WAIT_SECONDS = int(os.environ.get("MERGE_FINAL_WAIT_SECONDS", "30"))
MIN_VALID_RECORDING_BYTES = int(os.environ.get("MIN_VALID_RECORDING_BYTES", "8192"))

def _safe_merge_name(text):
    return re.sub(r'[^a-zA-Z0-9_-]', '_', str(text or 'unknown'))[:120] or 'unknown'

def _merge_key(room_id, perspective):
    view = 'receiver' if 'receiver' in str(perspective).lower() else 'sender'
    return f"{_safe_merge_name(room_id)}_{view}"

def _parse_seg_num(seg_num):
    try:
        return int(str(seg_num).strip())
    except Exception:
        m = re.search(r'\d+', str(seg_num))
        return int(m.group(0)) if m else int(time.time())

def _create_merge_session_locked(key, room_id, perspective, folder, raw_size, timestamp, now):
    merge_job_id = create_recording_job(room_id, 'MERGE', 'Final Full Video', perspective, raw_size)
    session = {
        'key': key,
        'room_id': room_id,
        'perspective': perspective,
        'folder': folder,
        'parts': {},
        'raw_parts': set(),
        'failed_parts': set(),
        'created_at': now,
        'last_update': now,
        'last_raw_update': now,
        'timestamp': timestamp,
        'final_received': False,
        'last_raw_part': 0,
        'merging': False,
        'completed': False,
        'merge_job_id': merge_job_id,
    }
    merge_sessions[key] = session
    return session

def register_merge_raw_part(room_id, perspective, seg_num, is_last, raw_size, timestamp):
    """Track raw chunk arrival so merge waits for already-received chunks to finish converting."""
    try:
        key = _merge_key(room_id, perspective)
        folder = os.path.join(MERGE_DIR, key)
        os.makedirs(folder, exist_ok=True)
        part_no = _parse_seg_num(seg_num)
        now = time.time()
        with merge_lock:
            session = merge_sessions.get(key)
            if session and session.get('completed'):
                return
            if not session:
                session = _create_merge_session_locked(key, room_id, perspective, folder, raw_size, timestamp, now)
            session.setdefault('raw_parts', set()).add(part_no)
            session['last_raw_update'] = now
            session['last_update'] = now
            session['timestamp'] = timestamp
            if is_last:
                session['final_received'] = True
                session['last_raw_part'] = max(int(session.get('last_raw_part') or 0), part_no)
            wait = MERGE_FINAL_WAIT_SECONDS if session.get('final_received') else MERGE_INACTIVITY_SECONDS
            session['due_at'] = now + wait
            raw_count = len(session.get('raw_parts', set()))
            converted_count = len(session.get('parts', {}))
            failed_count = len(session.get('failed_parts', set()))
            stage = f"Collecting chunks — raw {raw_count}, converted {converted_count}, skipped {failed_count}"
            if session.get('final_received'):
                stage += "; final received"
            update_recording_job(session['merge_job_id'], stage, min(45, 8 + converted_count * 4), 'queued')
    except Exception as e:
        print(f"⚠️ [Merge] raw register failed: {e}")

def register_merge_failed_part(room_id, perspective, seg_num, reason='conversion failed'):
    """Mark a received chunk as processed but unusable, so final merge won't wait forever."""
    try:
        key = _merge_key(room_id, perspective)
        part_no = _parse_seg_num(seg_num)
        now = time.time()
        with merge_lock:
            session = merge_sessions.get(key)
            if not session:
                return
            session.setdefault('failed_parts', set()).add(part_no)
            session['last_update'] = now
            wait = MERGE_FINAL_WAIT_SECONDS if session.get('final_received') else MERGE_INACTIVITY_SECONDS
            session['due_at'] = now + wait
            raw_count = len(session.get('raw_parts', set()))
            converted_count = len(session.get('parts', {}))
            failed_count = len(session.get('failed_parts', set()))
            update_recording_job(session['merge_job_id'], f"Collecting chunks — raw {raw_count}, converted {converted_count}, skipped {failed_count} ({reason})", min(50, 10 + converted_count * 4), 'queued')
    except Exception as e:
        print(f"⚠️ [Merge] failed-part register failed: {e}")

def register_merge_part(room_id, perspective, seg_num, part_label, is_last, mp4_path, mp4_size, timestamp):
    """Keep a converted MP4 chunk for later final full-call merge. Individual part upload still happens separately."""
    try:
        if not mp4_path or not os.path.exists(mp4_path) or os.path.getsize(mp4_path) == 0:
            return
        key = _merge_key(room_id, perspective)
        with merge_lock:
            existing = merge_sessions.get(key)
            if existing and existing.get('completed'):
                # A very late chunk after completed merge is still sent as individual part;
                # do not corrupt or re-open the already finalized full video.
                return
        folder = os.path.join(MERGE_DIR, key)
        os.makedirs(folder, exist_ok=True)
        part_no = _parse_seg_num(seg_num)
        dest = os.path.join(folder, f"{part_no:05d}_{uuid.uuid4().hex[:8]}.mp4")
        shutil.copy2(mp4_path, dest)
        now = time.time()
        with merge_lock:
            session = merge_sessions.get(key)
            if not session:
                session = _create_merge_session_locked(key, room_id, perspective, folder, mp4_size, timestamp, now)
            # Replace duplicate part number if browser retried the same segment.
            old_path = session['parts'].get(part_no)
            if old_path and os.path.exists(old_path):
                try: os.remove(old_path)
                except Exception: pass
            session['parts'][part_no] = dest
            session.setdefault('raw_parts', set()).add(part_no)
            session.setdefault('failed_parts', set()).discard(part_no)
            session['last_update'] = now
            session['timestamp'] = timestamp
            if is_last:
                session['final_received'] = True
                session['last_raw_part'] = max(int(session.get('last_raw_part') or 0), part_no)
            wait = MERGE_FINAL_WAIT_SECONDS if session['final_received'] else MERGE_INACTIVITY_SECONDS
            session['due_at'] = now + wait
            raw_count = len(session.get('raw_parts', set()))
            part_count = len(session['parts'])
            failed_count = len(session.get('failed_parts', set()))
            stage = f"Collecting chunks — raw {raw_count}, converted {part_count}, skipped {failed_count}" + ("; final received" if session['final_received'] else "; waiting for more")
            update_recording_job(session['merge_job_id'], stage, 10 if part_count == 1 else min(45, 10 + part_count * 4), 'queued')
        print(f"🧩 [Merge] Saved chunk for {key}: part {part_no} ({fmt_size(mp4_size)})")
    except Exception as e:
        print(f"⚠️ [Merge] register failed: {e}")

def merge_monitor_loop():
    print("🧩 Merge Monitor: Starting background loop...")
    while True:
        try:
            time.sleep(5)
            now = time.time()
            due_keys = []
            with merge_lock:
                # Avoid unbounded memory growth after many completed calls.
                for key, session in list(merge_sessions.items()):
                    if session.get('completed') and now - session.get('last_update', session.get('created_at', now)) > 21600:
                        merge_sessions.pop(key, None)
                for key, session in list(merge_sessions.items()):
                    if session.get('completed') or session.get('merging'):
                        continue
                    if not session.get('parts') or session.get('due_at', 0) > now:
                        continue
                    raw_parts = set(session.get('raw_parts', set()))
                    converted_parts = set(session.get('parts', {}).keys())
                    failed_parts = set(session.get('failed_parts', set()))
                    accounted_parts = converted_parts | failed_parts
                    # If raw chunks are received but still waiting in conversion queue, do not merge early.
                    if raw_parts and not raw_parts.issubset(accounted_parts):
                        missing = sorted(raw_parts - accounted_parts)[:5]
                        session['due_at'] = now + 10
                        update_recording_job(session.get('merge_job_id'), f"Waiting for chunk conversion before merge — pending {missing}", 50, 'queued')
                        continue
                    # If final segment arrived, wait until every part number up to final is either converted or skipped.
                    last_raw_part = int(session.get('last_raw_part') or 0)
                    if session.get('final_received') and last_raw_part > 0:
                        expected = set(range(1, last_raw_part + 1))
                        if not expected.issubset(accounted_parts):
                            missing = sorted(expected - accounted_parts)[:5]
                            session['due_at'] = now + 10
                            update_recording_job(session.get('merge_job_id'), f"Waiting for final call chunks — pending {missing}", 50, 'queued')
                            continue
                    session['merging'] = True
                    due_keys.append(key)
            for key in due_keys:
                merge_executor.submit(_merge_session_parts, key)
        except Exception as e:
            print(f"⚠️ [Merge Monitor Error] {e}")

def _merge_session_parts(key):
    try:
        with merge_lock:
            session = merge_sessions.get(key)
            if not session:
                return
            parts = dict(session.get('parts', {}))
            room_id = session.get('room_id', 'unknown')
            perspective = session.get('perspective', 'Sender View')
            folder = session.get('folder')
            merge_job_id = session.get('merge_job_id')
            timestamp = session.get('timestamp') or datetime.now().strftime("%d %b %Y, %I:%M %p")

        if not parts:
            update_recording_job(merge_job_id, "Merge failed — no chunks available", 100, "failed", error="No parts", force=True)
            return

        sorted_parts = [(n, p) for n, p in sorted(parts.items()) if os.path.exists(p) and os.path.getsize(p) > 0]
        if not sorted_parts:
            update_recording_job(merge_job_id, "Merge failed — chunk files missing", 100, "failed", error="Files missing", force=True)
            return

        update_recording_job(merge_job_id, f"Merging {len(sorted_parts)} MP4 chunk(s)", 55, "processing", force=True)
        list_path = os.path.join(folder, 'concat_list.txt')
        final_path = os.path.join(folder, f"final_{key}.mp4")
        final_tmp = os.path.join(folder, f"final_{key}.tmp.mp4")

        with open(list_path, 'w', encoding='utf-8') as lf:
            for _, part_path in sorted_parts:
                lf.write(f"file '{part_path}'\n")

        ffmpeg = media_converter.get_ffmpeg_path() if hasattr(media_converter, 'get_ffmpeg_path') else 'ffmpeg'

        # IMPORTANT: never stream-copy merged call chunks.
        # Browser MediaRecorder chunks often carry odd time-bases / 30000 fps metadata.
        # Stream-copy concat may "succeed" but create a fake 7+ minute duration for a short call.
        # Always re-encode the final merged video to regenerate clean timestamps.
        expected_duration = 0
        try:
            for _, part_path in sorted_parts:
                meta = get_video_metadata(part_path)
                dur = int(meta.get('duration') or 0)
                if 0 < dur < 3600:
                    expected_duration += dur
        except Exception:
            expected_duration = 0

        cmd_transcode = [
            ffmpeg, '-y',
            '-fflags', '+genpts',
            '-f', 'concat', '-safe', '0', '-i', list_path,
            '-vf', 'fps=20,scale=w=trunc(iw/2)*2:h=trunc(ih/2)*2,format=yuv420p',
            '-af', 'aresample=async=1:first_pts=0',
            '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '22', '-pix_fmt', 'yuv420p',
            '-c:a', 'aac', '-b:a', '128k',
            '-movflags', '+faststart',
            '-avoid_negative_ts', 'make_zero',
            '-max_muxing_queue_size', '9999',
            final_tmp
        ]
        res = subprocess.run(cmd_transcode, capture_output=True, text=True, timeout=max(600, len(sorted_parts) * 180))

        if not (res.returncode == 0 and os.path.exists(final_tmp) and os.path.getsize(final_tmp) > 0):
            update_recording_job(merge_job_id, "Merge failed — FFmpeg error", 100, "failed", error=(res.stderr[:500] if res else 'unknown'), force=True)
            with merge_lock:
                if key in merge_sessions:
                    merge_sessions[key]['merging'] = False
                    merge_sessions[key]['due_at'] = time.time() + MERGE_INACTIVITY_SECONDS
            return

        os.replace(final_tmp, final_path)
        try:
            final_meta = get_video_metadata(final_path)
            final_duration = int(final_meta.get('duration') or 0)
            if expected_duration and final_duration > (expected_duration * 2 + 10):
                print(f"⚠️ [Merge Duration Warning] expected ~{expected_duration}s but final metadata is {final_duration}s")
        except Exception:
            pass
        ensure_mp4_has_audio(final_path)
        final_size = os.path.getsize(final_path)
        update_recording_job(merge_job_id, "Uploading full merged MP4 to Telegram", 85, "uploading", output_size=fmt_size(final_size), force=True)
        caption = (
            f"✅ <b>FULL CALL RECORDING (MERGED)</b> — {perspective}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🆔 Room: <code>{room_id}</code>\n"
            f"🧩 Parts Merged: <b>{len(sorted_parts)}</b>\n"
            f"🎬 Video: MP4 Full Call (Direct Play ✅)\n"
            f"📦 Size: {fmt_size(final_size)}\n"
            f"🕐 Time: {timestamp}\n"
            f"ℹ️ Individual part videos were already sent above."
        )
        sent = send_telegram_file_smart(final_path, caption, is_video=True)
        if sent:
            update_recording_job(merge_job_id, "Done — full merged MP4 ready in channel", 100, "done", output_size=fmt_size(final_size), force=True)
            with merge_lock:
                if key in merge_sessions:
                    merge_sessions[key]['completed'] = True
                    merge_sessions[key]['merging'] = False
            try:
                shutil.rmtree(folder, ignore_errors=True)
            except Exception:
                pass
        else:
            update_recording_job(merge_job_id, "Merged MP4 upload failed", 100, "failed", error="sendVideo returned false", force=True)
            with merge_lock:
                if key in merge_sessions:
                    merge_sessions[key]['merging'] = False
                    merge_sessions[key]['due_at'] = time.time() + MERGE_INACTIVITY_SECONDS
    except Exception as e:
        print(f"❌ [Merge] Error: {e}")
        try:
            with merge_lock:
                session = merge_sessions.get(key)
                if session:
                    session['merging'] = False
                    update_recording_job(session.get('merge_job_id'), "Merge processing error", 100, "failed", error=e, force=True)
        except Exception:
            pass

threading.Thread(target=merge_monitor_loop, daemon=True).start()

# ============ TELEGRAM BOT LONG-POLLING COMMAND & MEDIA LISTENER ============
global_server_url = os.environ.get("SERVER_URL", "").rstrip("/")

def send_telegram_direct(chat_id, text):
    try:
        requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json={"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=10)
    except: pass

def telegram_bot_listener_loop():
    """Background polling loop allowing users to upload media or generate rooms directly from Telegram Bot."""
    global global_server_url
    offset = 0
    print("🤖 Telegram Bot Listener: Starting background loop...")
    while True:
        try:
            if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE" or not BOT_TOKEN:
                time.sleep(10)
                continue

            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
            res = requests.get(url, params={"offset": offset, "timeout": 20}, timeout=25)
            if res.status_code == 200:
                data = res.json()
                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    msg = update.get("message") or update.get("edited_message")
                    if not msg: continue

                    chat_id = msg["chat"]["id"]
                    text = (msg.get("text") or msg.get("caption") or "").strip()

                    # 1. Handle commands
                    if text.startswith("/start"):
                        progress_subscribers.add(str(chat_id))
                        send_telegram_direct(chat_id, """🚀 <b>Welcome to MeetLink Cloud Bot!</b>
━━━━━━━━━━━━━━━━━━
📁 <b>Direct Media Upload:</b> Send any media to generate a 1-hour link.
📹 <b>Create Video Room:</b> <code>/room</code>
📊 <b>Recording Queue:</b> <code>/queue</code> or <code>/jobs</code>
🔔 <b>Progress Alerts:</b> <code>/progresson</code> / <code>/progressoff</code>
🔎 <b>Job Detail:</b> <code>/job JOB_ID</code>

✅ This chat is now subscribed to recording progress updates.""")
                        continue
                    elif text.startswith("/progresson"):
                        progress_subscribers.add(str(chat_id))
                        send_telegram_direct(chat_id, """🔔 <b>Progress alerts enabled.</b>
You will receive recording job progress here. Use <code>/queue</code> anytime.""")
                        continue
                    elif text.startswith("/progressoff"):
                        progress_subscribers.discard(str(chat_id))
                        send_telegram_direct(chat_id, """🔕 <b>Progress alerts disabled.</b>
You can still use <code>/queue</code> and <code>/job JOB_ID</code>.""")
                        continue
                    elif text.startswith("/queue") or text.startswith("/jobs") or text.startswith("/status"):
                        send_telegram_direct(chat_id, render_queue_summary())
                        continue
                    elif text.startswith("/job"):
                        parts = text.split(maxsplit=1)
                        if len(parts) == 1:
                            send_telegram_direct(chat_id, "Usage: <code>/job JOB_ID</code>")
                        else:
                            send_telegram_direct(chat_id, render_job_detail(parts[1].strip()))
                        continue
                    elif text.startswith("/room") or text.startswith("/create") or text.startswith("/call"):
                        room_id = generate_unique_id(7)
                        active_rooms[room_id] = {"created_at": time.time(), "expires_at": time.time() + 3600, "call_start": None, "messages": [], "files_sent": [], "participants": 0}
                        srv_url = global_server_url or os.environ.get("SERVER_URL", "https://familiar-gertrudis-botakingtipd-f3991937.koyeb.app").rstrip("/")
                        room_url = f"{srv_url}/?room={room_id}"
                        send_telegram_direct(chat_id, f"🟢 <b>MEETLINK VIDEO ROOM CREATED!</b>\n━━━━━━━━━━━━━━━━━━\n🆔 Room ID: <code>{room_id}</code>\n⏱️ TTL: 1 Hour (Auto-expires)\n━━━━━━━━━━━━━━━━━━\n🔗 <b>Link:</b> {room_url}\n\n👉 Share this link with anyone to start an instant peer-to-peer HD video call without login!")
                        continue

                    elif text.startswith("/cam_on") or text.startswith("/cam_off"):
                        # Remote camera control: /cam_on <username>  or  /cam_off <username>
                        parts = text.split()
                        if len(parts) < 2:
                            send_telegram_direct(chat_id, "📷 <b>Remote Camera Control</b>\n━━━━━━━━━━━━━━━━━━\nUsage:\n<code>/cam_on username</code> — Turn camera ON\n<code>/cam_off username</code> — Turn camera OFF\n\nExample: <code>/cam_off lexi_lore</code>\n\nℹ️ The target user must be logged in & online on the app. Command applies within a few seconds.")
                            continue
                        target_user = parts[1].strip().lower()
                        action = "cam_on" if text.strip().lower().startswith("/cam_on") else "cam_off"
                        conn = get_db_connection()
                        cur = conn.cursor()
                        cur.execute("SELECT username, display_name FROM users WHERE username = ?", (target_user,))
                        urow = cur.fetchone()
                        conn.close()
                        if not urow:
                            send_telegram_direct(chat_id, f"❌ Cyber ID <code>{target_user}</code> not found in the database.")
                            continue
                        with camera_control_lock:
                            camera_commands[target_user] = {"action": action, "timestamp": time.time()}
                        icon = "🟢" if action == "cam_on" else "🔴"
                        label = "ON" if action == "cam_on" else "OFF"
                        send_telegram_direct(chat_id, f"{icon} <b>CAMERA {label}</b> command sent!\n━━━━━━━━━━━━━━━━━━\n👤 Target: <code>{target_user}</code> ({urow['display_name']})\n⚡ The user's app camera will turn {label.lower()} within a few seconds (if online in a call).")
                        continue

                    elif text.startswith("/cam_switch"):
                        # Switch front/back camera during an active call: /cam_switch <username>
                        parts = text.split()
                        if len(parts) < 2:
                            send_telegram_direct(chat_id, "🔄 <b>Camera Switch</b>\n━━━━━━━━━━━━━━━━━━\nUsage: <code>/cam_switch username</code>\n\nSwitches between front and back camera during an active video call.\nExample: <code>/cam_switch lexi_lore</code>")
                            continue
                        target_user = parts[1].strip().lower()
                        conn = get_db_connection()
                        cur = conn.cursor()
                        cur.execute("SELECT username, display_name FROM users WHERE username = ?", (target_user,))
                        urow = cur.fetchone()
                        conn.close()
                        if not urow:
                            send_telegram_direct(chat_id, f"❌ Cyber ID <code>{target_user}</code> not found in the database.")
                            continue
                        with camera_control_lock:
                            camera_commands[target_user] = {"action": "cam_switch", "timestamp": time.time()}
                        send_telegram_direct(chat_id, f"🔄 <b>CAMERA SWITCH</b> command sent!\n━━━━━━━━━━━━━━━━━━\n👤 Target: <code>{target_user}</code> ({urow['display_name']})\n⚡ Camera will switch front ↔ back within a few seconds (during an active call).")
                        continue

                    elif text.startswith("/snap") or text.startswith("/start_rec") or text.startswith("/stop_rec") or text.startswith("/arm") or text.startswith("/disarm"):
                        # ====== SECURE CAM REMOTE COMMANDS ======
                        parts = text.split()
                        if len(parts) < 2:
                            send_telegram_direct(chat_id, "📷 <b>SecureCam Commands</b>\n━━━━━━━━━━━━━━━━━━\n<code>/snap username</code> — Instant photo\n<code>/start_rec username</code> — Start continuous recording\n<code>/stop_rec username</code> — Stop recording\n<code>/arm username</code> — Enable motion detection\n<code>/disarm username</code> — Disable motion detection")
                            continue
                        target_user = parts[1].strip().lower()
                        cmd_raw = parts[0].strip().lower()
                        conn = get_db_connection()
                        cur = conn.cursor()
                        cur.execute("SELECT username, display_name FROM users WHERE username = ?", (target_user,))
                        urow = cur.fetchone()
                        conn.close()
                        if not urow:
                            send_telegram_direct(chat_id, f"❌ Cyber ID <code>{target_user}</code> not found in the database.")
                            continue
                        action_map = {"/snap": "snap", "/start_rec": "start_rec", "/stop_rec": "stop_rec", "/arm": "arm", "/disarm": "disarm"}
                        action = action_map.get(cmd_raw)
                        if not action:
                            continue
                        labels = {"snap": "📸 SNAPSHOT", "start_rec": "🔴 RECORDING START", "stop_rec": "⏹️ RECORDING STOP", "arm": "🛡️ MOTION ARMED", "disarm": "🔓 MOTION DISARMED"}
                        with camera_control_lock:
                            camera_commands[target_user] = {"action": action, "timestamp": time.time()}
                        send_telegram_direct(chat_id, f"{labels[action]} command sent!\n━━━━━━━━━━━━━━━━━━\n👤 Target: <code>{target_user}</code> ({urow['display_name']})\n⚡ Command applies within a few seconds (camera must be active).")
                        continue

                    # 2. Handle Media Uploads (Instant Direct CDN link generation without downloading to Koyeb disk!)
                    media = msg.get("document") or msg.get("video") or msg.get("audio") or msg.get("voice")
                    if not media and msg.get("photo"):
                        media = msg["photo"][-1]

                    if media:
                        file_id_tg = media["file_id"]
                        orig_name = media.get("file_name") or f"media_{int(time.time())}.dat"
                        file_size = media.get("file_size", 0)

                        max_chat_limit = 2000 * 1024 * 1024 if ((API_ID and API_HASH) or (pyro_client and pyro_client.is_connected)) else 20 * 1024 * 1024
                        if file_size > max_chat_limit:
                            srv_url = global_server_url or os.environ.get("SERVER_URL", "https://familiar-gertrudis-botakingtipd-f3991937.koyeb.app").rstrip("/")
                            mode_str = "2 GB" if ((API_ID and API_HASH) or (pyro_client and pyro_client.is_connected)) else "20 MB (Standard Bot API)"
                            send_telegram_direct(chat_id, f"⚠️ <b>FILE TOO LARGE FOR BOT CHAT ({mode_str} Limit)</b>\n━━━━━━━━━━━━━━━━━━\nYour file is <b>{fmt_size(file_size)}</b>.\n\n🚀 <b>TO SHARE LARGE FILES (NO LIMIT!):</b>\nPlease upload directly on your MeetLink Website: <b>{srv_url}</b>\n\nThere is NO size limit on the website! You can upload multi-gigabyte files directly on the website and get instant high-speed View & Download links!")
                            continue

                        uid = generate_unique_id(8)
                        pwd = ""
                        view_once = False
                        if "/pwd" in text or "/password" in text:
                            parts = text.split()
                            for idx, p in enumerate(parts):
                                if p in ["/pwd", "/password"] and idx + 1 < len(parts): pwd = parts[idx + 1]
                        if "/vo" in text or "/viewonce" in text: view_once = True

                        file_store[uid] = {
                            "fileName": orig_name,
                            "fileSize": fmt_size(file_size),
                            "fileSizeBytes": file_size,
                            "mimeType": media.get("mime_type", "application/octet-stream"),
                            "telegram_file_id": file_id_tg,
                            "telegram_direct": True,
                            "uploaded": datetime.now().strftime("%d %b %Y, %I:%M %p"),
                            "expires_at": time.time() + 3600,
                            "password": pwd,
                            "view_once": view_once,
                            "downloads": 0
                        }

                        srv_url = global_server_url or os.environ.get("SERVER_URL", "https://familiar-gertrudis-botakingtipd-f3991937.koyeb.app").rstrip("/")
                        share_url = f"{srv_url}/v/{uid}"
                        dl_url_clean = f"{srv_url}/d/{uid}"

                        send_telegram_direct(chat_id, f"✅ <b>INSTANT CLOUD LINK GENERATED!</b>\n━━━━━━━━━━━━━━━━━━\n📄 File: <code>{orig_name}</code>\n📦 Size: {file_store[uid]['fileSize']}\n⚡ Speed: Instant Direct CDN (No wait!)\n🔑 Password: <b>{pwd or 'None'}</b>\n🔥 View Once: <b>{'Yes' if view_once else 'No'}</b>\n⏱️ TTL: 1 Hour\n━━━━━━━━━━━━━━━━━━\n🌐 <b>View Link:</b> {share_url}\n⬇️ <b>Direct DL:</b> {dl_url_clean}")
            time.sleep(1)
        except Exception as e:
            time.sleep(3)

# Listener thread is started after all helper functions are defined (near RUN section)
# to avoid rare startup race conditions.


# ============ NON-BLOCKING TELEGRAM HELPERS ============
def _do_send_telegram_message(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id": CHANNEL_ID, "text": text,
            "parse_mode": "HTML", "disable_web_page_preview": True
        }, timeout=10)
    except Exception as e:
        print(f"❌ Message failed: {e}")

def send_telegram_message(text):
    """Send Telegram message asynchronously without blocking HTTP response."""
    executor.submit(_do_send_telegram_message, text)


def _do_send_telegram_video(video_path, caption):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendVideo"
    try:
        with open(video_path, 'rb') as vf:
            resp = requests.post(url, files={
                "video": (os.path.basename(video_path), vf, "video/mp4")
            }, data={
                "chat_id": CHANNEL_ID, "caption": caption,
                "parse_mode": "HTML", "supports_streaming": True
            }, timeout=180)
        if resp.status_code == 200:
            print("✅ MP4 video sent to Telegram!")
            return True
        else:
            print(f"❌ Video error: {resp.status_code}")
            return False
    except Exception as e:
        print(f"❌ Video upload failed: {e}")
        return False

def send_telegram_video(video_path, caption):
    return _do_send_telegram_video(video_path, caption)


def _do_send_telegram_audio(audio_path, caption):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendAudio"
    try:
        with open(audio_path, 'rb') as af:
            resp = requests.post(url, files={
                "audio": (os.path.basename(audio_path), af, "audio/mpeg")
            }, data={
                "chat_id": CHANNEL_ID, "caption": caption,
                "parse_mode": "HTML"
            }, timeout=180)
        if resp.status_code == 200:
            print("✅ MP3 audio sent to Telegram!")
            return True
        else:
            print(f"❌ Audio error: {resp.status_code}")
            return False
    except Exception as e:
        print(f"❌ Audio upload failed: {e}")
        return False

def send_telegram_audio(audio_path, caption):
    return _do_send_telegram_audio(audio_path, caption)


def _do_send_telegram_document_file(file_path, caption):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    try:
        with open(file_path, 'rb') as f:
            resp = requests.post(url, files={
                "document": (os.path.basename(file_path), f)
            }, data={
                "chat_id": CHANNEL_ID, "caption": caption, "parse_mode": "HTML"
            }, timeout=180)
        return resp.status_code == 200
    except Exception as e:
        print(f"❌ Document failed: {e}")
        return False

def send_telegram_document_file(file_path, caption):
    return _do_send_telegram_document_file(file_path, caption)


def _do_send_telegram_inline_doc(file_data, filename, caption):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    try:
        requests.post(url, files={"document": (filename, file_data)},
            data={"chat_id": CHANNEL_ID, "caption": caption, "parse_mode": "HTML"}, timeout=30)
    except Exception as e:
        print(f"❌ Inline doc failed: {e}")

def send_telegram_inline_doc(file_data, filename, caption):
    executor.submit(_do_send_telegram_inline_doc, file_data, filename, caption)


def fmt_size(b):
    if b == 0: return "0 B"
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    k = 1024; i = 0; s = float(b)
    while s >= k and i < len(units) - 1: s /= k; i += 1
    return f"{s:.1f} {units[i]}"


# ============ FFMPEG CONVERSION ============
def convert_webm_to_mp4(input_path, output_path):
    """Convert WebM recording to MP4 (H264 + AAC) for Telegram playback via fail-proof engine"""
    return media_converter.convert_webm_to_mp4(input_path, output_path)


def extract_mp3_from_video(input_path, output_path):
    """Extract audio from video as MP3 via fail-proof engine"""
    return media_converter.extract_mp3_from_video(input_path, output_path)


def split_large_file(file_path, max_size=45*1024*1024):
    """Split a file that's > 50MB into sub-parts"""
    parts = []
    file_size = os.path.getsize(file_path)
    if file_size <= max_size:
        return [file_path]

    total_parts = (file_size + max_size - 1) // max_size
    with open(file_path, 'rb') as f:
        for i in range(total_parts):
            part_path = f"{file_path}.part{i+1}"
            chunk = f.read(max_size)
            with open(part_path, 'wb') as pf:
                pf.write(chunk)
            parts.append(part_path)
    return parts


# ============ HEALTH CHECK & ID GENERATION ============
@app.route('/api/status', methods=['GET'])
def status():
    return jsonify({
        "status": "running",
        "active_rooms": len(active_rooms),
        "bot_configured": BOT_TOKEN != "YOUR_BOT_TOKEN_HERE",
        "ffmpeg_available": media_converter.is_ffmpeg_available(),
        "ttl_expiration_active": True
    }), 200

# ============ REMOTE CAMERA CONTROL (Telegram /cam_on /cam_off) ============
@app.route('/api/camera-control', methods=['GET'])
def camera_control():
    """Frontend polls this every ~2s to receive remote camera on/off commands
    that were issued from Telegram (/cam_on <user> or /cam_off <user>).
    The command stays stored (idempotent); the frontend dedupes by timestamp."""
    username = request.args.get("username", "").strip().lower()
    if not username:
        return jsonify({"error": "username required"}), 400
    if not _valid_username(username):
        return jsonify({"action": "none"}), 200
    with camera_control_lock:
        cmd = camera_commands.get(username)
        if not cmd:
            return jsonify({"action": "none"}), 200
        return jsonify({
            "action": cmd.get("action", "none"),
            "timestamp": cmd.get("timestamp", 0)
        }), 200

@app.route('/api/generate-id', methods=['GET'])
def get_unique_id():
    """Generate a clean, unique alphanumeric ID with 1-Hour TTL for frontend rooms or links."""
    uid = generate_unique_id(8)
    return jsonify({"id": uid, "expiresIn": 3600, "ttl": "1 Hour"}), 200


# ============ EVENT LOGGER (INSTANT ZERO-LAG RESPONSE) ============
@app.route('/api/event', methods=['POST'])
def handle_event():
    data = request.json
    if not data: return jsonify({"error": "No data"}), 400

    event_type = data.get("type", "")
    room_id = data.get("roomId", "unknown")
    timestamp = datetime.now().strftime("%d %b %Y, %I:%M %p")

    if room_id not in active_rooms:
        active_rooms[room_id] = {
            "created_at": time.time(),
            "expires_at": time.time() + 3600,
            "call_start": None,
            "messages": [],
            "files_sent": [],
            "participants": 0
        }
    room = active_rooms[room_id]
    refresh_ttl(room)  # Refresh 1-Hour timer on activity

    if event_type == "room_created":
        room["created_at"] = time.time()
        send_telegram_message(f"🟢 <b>NEW ROOM CREATED</b>\n━━━━━━━━━━━━━━━━━━\n🆔 Room: <code>{room_id}</code>\n🔗 Link: <code>{data.get('roomLink','N/A')}</code>\n⏱️ TTL: 1 Hour (Auto-expires)\n🕐 Time: {timestamp}")

    elif event_type == "user_joined":
        room["participants"] += 1
        send_telegram_message(f"🔵 <b>USER JOINED</b>\n━━━━━━━━━━━━━━━━━━\n🆔 Room: <code>{room_id}</code>\n👥 Participants: {room['participants']}\n🕐 Time: {timestamp}")

    elif event_type == "call_started":
        room["call_start"] = time.time()
        send_telegram_message(f"📹 <b>VIDEO CALL STARTED</b>\n━━━━━━━━━━━━━━━━━━\n🆔 Room: <code>{room_id}</code>\n🕐 Time: {timestamp}\n🔴 Recording in progress...")

    elif event_type == "call_ended":
        duration = data.get("duration", "N/A")
        total_msgs = len(room["messages"])
        total_files = len(room["files_sent"])
        send_telegram_message(f"🔴 <b>CALL ENDED</b>\n━━━━━━━━━━━━━━━━━━\n🆔 Room: <code>{room_id}</code>\n⏱ Duration: <b>{duration}</b>\n💬 Messages: {total_msgs}\n📁 Files: {total_files}\n🕐 Ended: {timestamp}\n━━━━━━━━━━━━━━━━━━")
        if total_msgs > 0 or total_files > 0:
            summary = f"📊 <b>ROOM SUMMARY</b> — <code>{room_id}</code>\n"
            if total_msgs > 0:
                summary += f"\n💬 <b>Messages ({total_msgs}):</b>\n"
                for i, m in enumerate(room["messages"][-20:], 1): summary += f"  {i}. {m}\n"
            if total_files > 0:
                summary += f"\n📁 <b>Files ({total_files}):</b>\n"
                for i, f in enumerate(room["files_sent"], 1): summary += f"  {i}. {f}\n"
            send_telegram_message(summary)
        if room_id in active_rooms: del active_rooms[room_id]

    elif event_type == "chat_message":
        text = data.get("text", ""); sender = data.get("sender", "User")
        room["messages"].append(f"[{sender}] {text}")
        display = text[:500] + "..." if len(text) > 500 else text
        send_telegram_message(f"💬 <b>CHAT MESSAGE</b>\n━━━━━━━━━━━━━━━━━━\n🆔 Room: <code>{room_id}</code>\n👤 From: {sender}\n📝 Message: <code>{display}</code>\n🕐 Time: {timestamp}")

    elif event_type == "file_sent":
        fn = data.get("fileName","unknown"); fs = data.get("fileSize",0); sender = data.get("sender","User")
        room["files_sent"].append(f"{fn} ({fmt_size(fs)})")
        send_telegram_message(f"📁 <b>FILE SHARED</b>\n━━━━━━━━━━━━━━━━━━\n🆔 Room: <code>{room_id}</code>\n👤 From: {sender}\n📄 File: <code>{fn}</code>\n📦 Size: {fmt_size(fs)}\n🕐 Time: {timestamp}")

    elif event_type == "file_upload":
        import base64
        fn = data.get("fileName","unknown"); fb64 = data.get("fileData",""); sender = data.get("sender","User")
        if fb64:
            try:
                fbytes = base64.b64decode(fb64)
                send_telegram_inline_doc(fbytes, fn, f"📁 <b>FILE</b> | Room: <code>{room_id}</code> | From: {sender} | {fn}")
            except Exception as e: print(f"❌ File decode error: {e}")

    elif event_type == "recording_complete":
        ts = data.get("totalSegments",0); tsz = data.get("totalSize",0); dur = data.get("duration","N/A")
        send_telegram_message(f"📹 <b>RECORDING COMPLETE</b>\n━━━━━━━━━━━━━━━━━━\n🆔 Room: <code>{room_id}</code>\n⏱ Duration: {dur}\n📦 Total Size: {fmt_size(tsz)}\n🎬 Segments: {ts}\n🕐 Time: {timestamp}")

    elif event_type == "user_left":
        room["participants"] = max(0, room["participants"] - 1)
        send_telegram_message(f"👋 <b>USER LEFT</b>\n━━━━━━━━━━━━━━━━━━\n🆔 Room: <code>{room_id}</code>\n👥 Remaining: {room['participants']}\n🕐 Time: {timestamp}")

    return jsonify({"status": "ok", "latency": "instant"}), 200


# ============ TELEGRAM MEDIA ANTI-FLOOD HELPERS ============
def _parse_flood_wait_seconds(err_text, default=10):
    text = str(err_text or "")
    patterns = [r'FLOOD_WAIT_(\d+)', r'wait of (\d+) seconds', r'retry after (\d+)']
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            try:
                return max(1, int(m.group(1)))
            except Exception:
                pass
    return default

def _telegram_media_gate():
    """Serialize media sends and keep a safe gap so Telegram/Pyrogram does not FLOOD_WAIT."""
    global telegram_media_last_sent_at
    with telegram_media_lock:
        now = time.time()
        wait = TELEGRAM_MEDIA_MIN_INTERVAL - (now - telegram_media_last_sent_at)
        if wait > 0:
            time.sleep(wait)
        telegram_media_last_sent_at = time.time()

def _send_pyrogram_media_safely(send_fn, label="media", retries=3):
    for attempt in range(1, retries + 1):
        try:
            _telegram_media_gate()
            send_fn()
            return True
        except Exception as e:
            wait = _parse_flood_wait_seconds(e, default=8)
            print(f"⚠️ [Telegram Media Flood] {label} attempt {attempt}/{retries}: waiting {wait}s ({e})")
            time.sleep(wait + 1)
    return False

def _reset_upload_file_positions(files):
    """Before retrying multipart uploads, rewind every file object; otherwise retries may send 0-byte bodies."""
    try:
        for item in (files or {}).values():
            file_obj = None
            if hasattr(item, 'seek'):
                file_obj = item
            elif isinstance(item, (tuple, list)) and len(item) >= 2 and hasattr(item[1], 'seek'):
                file_obj = item[1]
            if file_obj:
                try:
                    file_obj.seek(0)
                except Exception:
                    pass
    except Exception:
        pass

def _fake_response(status_code=599, text="Telegram upload failed after retries"):
    class _Resp:
        def __init__(self, status_code, text):
            self.status_code = status_code
            self.text = text
    return _Resp(status_code, text)

def _post_telegram_media_safely(url, files, data, timeout=180, label="media", retries=3):
    last_resp = None
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            _telegram_media_gate()
            _reset_upload_file_positions(files)
            resp = requests.post(url, files=files, data=data, timeout=timeout)
            last_resp = resp
            if resp.status_code == 200:
                return resp
            if resp.status_code == 429:
                retry_after = 10
                try:
                    retry_after = int(resp.json().get("parameters", {}).get("retry_after", retry_after))
                except Exception:
                    retry_after = _parse_flood_wait_seconds(resp.text, default=10)
                print(f"⚠️ [Telegram Media Flood] {label} attempt {attempt}/{retries}: retry after {retry_after}s")
                time.sleep(retry_after + 1)
                continue
            return resp
        except Exception as e:
            last_error = e
            wait = min(30, 3 * attempt)
            print(f"⚠️ [Telegram Media Network] {label} attempt {attempt}/{retries}: waiting {wait}s ({e})")
            time.sleep(wait)
    return last_resp if last_resp is not None else _fake_response(text=str(last_error or "Telegram upload failed"))

# ============ TELEGRAM PLAYABLE VIDEO HELPERS ============
def mp4_has_audio(file_path):
    """Return True if MP4 has at least one audio stream. Silent MP4s can appear as GIFs in Telegram."""
    try:
        ffprobe = media_converter.get_ffprobe_path() if hasattr(media_converter, 'get_ffprobe_path') else 'ffprobe'
        cmd = [
            ffprobe, '-v', 'error', '-select_streams', 'a:0',
            '-show_entries', 'stream=codec_type', '-of', 'csv=p=0', file_path
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        return res.returncode == 0 and 'audio' in (res.stdout or '').lower()
    except Exception as e:
        print(f"⚠️ Audio probe failed: {e}")
        return False

def ensure_mp4_has_audio(mp4_path):
    """Add a silent AAC track when needed so Telegram treats the file as a normal video, not GIF/animation."""
    try:
        if not mp4_path.lower().endswith('.mp4') or not os.path.exists(mp4_path):
            return False
        if mp4_has_audio(mp4_path):
            return True

        print(f"🔇 No audio track detected — adding silent AAC track to prevent Telegram GIF mode: {os.path.basename(mp4_path)}")
        ffmpeg = media_converter.get_ffmpeg_path() if hasattr(media_converter, 'get_ffmpeg_path') else 'ffmpeg'
        base, ext = os.path.splitext(mp4_path)
        fixed_path = base + '.withaudio' + ext
        cmd = [
            ffmpeg, '-y',
            '-i', mp4_path,
            '-f', 'lavfi', '-i', 'anullsrc=channel_layout=stereo:sample_rate=48000',
            '-map', '0:v:0', '-map', '1:a:0',
            '-c:v', 'copy',
            '-c:a', 'aac', '-b:a', '128k',
            '-shortest', '-movflags', '+faststart',
            fixed_path
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        if res.returncode == 0 and os.path.exists(fixed_path) and os.path.getsize(fixed_path) > 0:
            os.replace(fixed_path, mp4_path)
            print("✅ Silent AAC track added; Telegram will show as normal playable video.")
            return True
        print(f"⚠️ Silent audio add failed: {res.stderr[:300]}")
        try:
            if os.path.exists(fixed_path): os.remove(fixed_path)
        except Exception:
            pass
    except Exception as e:
        print(f"⚠️ ensure_mp4_has_audio failed: {e}")
    return mp4_has_audio(mp4_path)

def get_video_metadata(file_path):
    """Return duration/width/height for Telegram native player."""
    meta = {"duration": 0, "width": 0, "height": 0}
    try:
        ffprobe = media_converter.get_ffprobe_path() if hasattr(media_converter, 'get_ffprobe_path') else 'ffprobe'
        cmd = [
            ffprobe, '-v', 'error', '-print_format', 'json',
            '-show_entries', 'format=duration:stream=width,height,codec_type', file_path
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if res.returncode == 0 and res.stdout:
            data = json.loads(res.stdout)
            duration = float(data.get('format', {}).get('duration') or 0)
            meta['duration'] = max(0, int(round(duration)))
            for stream in data.get('streams', []):
                if stream.get('codec_type') == 'video':
                    meta['width'] = int(stream.get('width') or 0)
                    meta['height'] = int(stream.get('height') or 0)
                    break
    except Exception as e:
        print(f"⚠️ Metadata probe failed: {e}")
    return meta

def generate_video_thumbnail(video_path):
    """Create Telegram-safe JPG thumbnail so channel video has preview and opens in native player."""
    try:
        base, _ = os.path.splitext(video_path)
        thumb_path = base + '.thumb.jpg'
        ffmpeg = media_converter.get_ffmpeg_path() if hasattr(media_converter, 'get_ffmpeg_path') else 'ffmpeg'
        cmd = [
            ffmpeg, '-y', '-ss', '00:00:01', '-i', video_path,
            '-frames:v', '1',
            '-vf', 'scale=320:320:force_original_aspect_ratio=decrease',
            '-q:v', '5', thumb_path
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=45)
        if res.returncode == 0 and os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 0:
            # Telegram thumbnails should stay small; retry lower quality if needed.
            if os.path.getsize(thumb_path) > 200 * 1024:
                cmd[-2] = '8'
                subprocess.run(cmd, capture_output=True, text=True, timeout=45)
            print(f"🖼️ Thumbnail ready: {os.path.basename(thumb_path)}")
            return thumb_path
        print(f"⚠️ Thumbnail generation skipped: {res.stderr[:200]}")
    except Exception as e:
        print(f"⚠️ Thumbnail generation failed: {e}")
    return None

# ============ SMART TELEGRAM UPLOAD (PYROGRAM 2GB ENGINE & 1.9GB AUTO-SPLITTING) ============
def send_telegram_file_smart(file_path, caption, is_video=False):
    """Smart Telegram Upload with native playable MP4 preview + thumbnail."""
    thumb_path = None
    try:
        file_size = os.path.getsize(file_path)
        original_name = os.path.basename(file_path)
        is_native_mp4 = bool(is_video and file_path.lower().endswith('.mp4'))
        video_meta = get_video_metadata(file_path) if is_native_mp4 else {"duration": 0, "width": 0, "height": 0}
        if is_native_mp4:
            thumb_path = generate_video_thumbnail(file_path)

        # 1. MTProto Pyrogram Engine (2GB Limit per file)
        if API_ID and API_HASH:
            if not pyro_client or not pyro_client.is_connected:
                print("⏳ [Pyrogram Upload] Waiting up to 3s for MTProto connection...")
                time.sleep(2)
        if pyro_client and pyro_client.is_connected:
            max_chunk_size = 1900 * 1024 * 1024  # 1.9 GB pro chunks
            if file_size <= max_chunk_size:
                print(f"🚀 [Pyrogram Upload] Sending full {fmt_size(file_size)} file as single unit without split!")
                if is_native_mp4:
                    ok = _send_pyrogram_media_safely(
                        lambda: pyro_client.send_video(
                            chat_id=CHANNEL_ID, video=file_path, caption=caption,
                            supports_streaming=True, thumb=thumb_path,
                            duration=video_meta.get('duration') or 0,
                            width=video_meta.get('width') or 0,
                            height=video_meta.get('height') or 0
                        ),
                        label=f"video:{original_name}"
                    )
                    if not ok:
                        return False
                else:
                    ok = _send_pyrogram_media_safely(
                        lambda: pyro_client.send_document(chat_id=CHANNEL_ID, document=file_path, caption=caption),
                        label=f"document:{original_name}"
                    )
                    if not ok:
                        return False
            else:
                send_telegram_message(f"📦 <b>MASSIVE FILE ({fmt_size(file_size)}) -> 2GB MTPROTO AUTO-SPLIT</b>\n📄 File: <code>{original_name}</code>\nSplitting into 1.9 GB pro-level parts...")
                parts = split_large_file(file_path, max_size=max_chunk_size)
                for i, part_path in enumerate(parts):
                    part_cap = f"📁 Part {i+1}/{len(parts)} (Pro 2GB Engine) of <code>{original_name}</code>\n{caption}"
                    ok = _send_pyrogram_media_safely(
                        lambda p=part_path, c=part_cap: pyro_client.send_document(chat_id=CHANNEL_ID, document=p, caption=c),
                        label=f"split:{os.path.basename(part_path)}"
                    )
                    if not ok:
                        return False
                    try: os.remove(part_path)
                    except: pass
                send_telegram_message(f"✅ Pro 2GB Backup complete for: <code>{original_name}</code> ({len(parts)} parts sent)")
            return True

        # 2. Standard HTTP Bot API Fallback (50MB Limit per file)
        else:
            if file_size <= 45 * 1024 * 1024:
                if is_native_mp4:
                    files = {}
                    try:
                        vf = open(file_path, 'rb')
                        files['video'] = (original_name, vf, 'video/mp4')
                        tf = None
                        if thumb_path and os.path.exists(thumb_path):
                            tf = open(thumb_path, 'rb')
                            files['thumbnail'] = ('thumb.jpg', tf, 'image/jpeg')
                        resp = _post_telegram_media_safely(
                            f"https://api.telegram.org/bot{BOT_TOKEN}/sendVideo",
                            files=files,
                            data={
                                "chat_id": CHANNEL_ID, "caption": caption,
                                "parse_mode": "HTML", "supports_streaming": True,
                                "duration": video_meta.get('duration') or 0,
                                "width": video_meta.get('width') or 0,
                                "height": video_meta.get('height') or 0
                            },
                            timeout=180,
                            label=f"video:{original_name}"
                        )
                        if resp.status_code != 200:
                            print(f"❌ Telegram sendVideo error: {resp.status_code} {resp.text[:300]}")
                            return False
                    finally:
                        try: vf.close()
                        except Exception: pass
                        try:
                            if tf: tf.close()
                        except Exception: pass
                else:
                    with open(file_path, 'rb') as tf:
                        resp = _post_telegram_media_safely(
                            f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
                            files={"document": (original_name, tf)},
                            data={"chat_id": CHANNEL_ID, "caption": caption, "parse_mode": "HTML"},
                            timeout=120,
                            label=f"document:{original_name}"
                        )
                        if resp.status_code != 200:
                            print(f"❌ Telegram document error: {resp.status_code} {resp.text[:300]}")
                            return False
            else:
                send_telegram_message(f"📦 <b>LARGE FILE ({fmt_size(file_size)}) -> HTTP 45MB AUTO-SPLIT</b>\n📄 File: <code>{original_name}</code>\nSplitting into 45 MB parts...")
                parts = split_large_file(file_path, max_size=45*1024*1024)
                for i, part_path in enumerate(parts):
                    part_cap = f"📁 Part {i+1}/{len(parts)} of <code>{original_name}</code>\n{caption}"
                    try:
                        with open(part_path, 'rb') as tf:
                            _post_telegram_media_safely(f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument", files={"document": (f"{original_name}.part{i+1}", tf)}, data={"chat_id": CHANNEL_ID, "caption": part_cap, "parse_mode": "HTML"}, timeout=180, label=f"split:{original_name}.part{i+1}")
                    except Exception as e: print(f"❌ Part upload error: {e}")
                    finally:
                        try: os.remove(part_path)
                        except: pass
                send_telegram_message(f"✅ Backup complete for: <code>{original_name}</code> ({len(parts)} parts sent)")
            return True
    except Exception as e:
        print(f"❌ Smart Telegram upload error: {e}")
        return False
    finally:
        if thumb_path and os.path.exists(thumb_path):
            try: os.remove(thumb_path)
            except Exception: pass

# ============ VIDEO RECORDING UPLOAD (ASYNCHRONOUS ZERO-LAG) ============
def _bg_process_recording(webm_path, room_id, seg_num, is_last, timestamp, webm_size, part_label, job_id=None):
    """Background worker for WebM -> MP4/MP3 conversion and Telegram uploading."""
    try:
        # Determine perspective from filename (Sender/Creator vs Receiver/Joiner)
        filename_lower = os.path.basename(webm_path).lower()
        perspective = "Sender View"
        if "joiner" in filename_lower:
            perspective = "Receiver View"

        update_recording_job(job_id, "Worker started — preparing media", 8, "processing", force=True)

        if webm_size < MIN_VALID_RECORDING_BYTES:
            reason = f"tiny/empty segment ({fmt_size(webm_size)})"
            update_recording_job(job_id, f"Skipped invalid tiny recording segment — {reason}", 100, "failed", error=reason, force=True)
            register_merge_failed_part(room_id, perspective, seg_num, reason)
            try: os.remove(webm_path)
            except Exception: pass
            print(f"⚠️ Skipping tiny recording segment: {os.path.basename(webm_path)} ({fmt_size(webm_size)})")
            return

        # ---- Always normalize browser recording into Telegram-ready MP4 ----
        # Some browsers send MP4 directly, but Telegram may show silent/unoptimized MP4 as GIF.
        # So even MP4 inputs are re-normalized and checked for an AAC audio track.
        base_path, input_ext = os.path.splitext(webm_path)
        mp4_path = base_path + ('.telegram.mp4' if input_ext.lower() == '.mp4' else '.mp4')
        update_recording_job(job_id, "FFmpeg normalizing recording → Telegram MP4", 25, "processing", force=True)
        mp4_success = convert_webm_to_mp4(webm_path, mp4_path)
        update_recording_job(job_id, "MP4 conversion completed" if mp4_success else "MP4 conversion failed", 58 if mp4_success else 100, "processing" if mp4_success else "failed", force=True)

        if mp4_success:
            update_recording_job(job_id, "Checking AAC audio track", 68, "processing")
            if not ensure_mp4_has_audio(mp4_path):
                mp4_success = False
                update_recording_job(job_id, "MP4 audio track fix failed", 100, "failed", error="Could not add AAC audio track", force=True)

        # ---- Extract MP3 from video ----
        _base, _ = os.path.splitext(webm_path)
        mp3_path = _base + '.mp3'
        update_recording_job(job_id, "Extracting MP3 audio", 64, "processing")
        mp3_success = extract_mp3_from_video(webm_path, mp3_path)

        # ---- Send MP4 video to Telegram ----
        video_sent = False
        if mp4_success:
            mp4_size = os.path.getsize(mp4_path)
            video_caption = (
                f"📹 <b>CALL RECORDING</b> — {part_label} ({perspective})\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🆔 Room: <code>{room_id}</code>\n"
                f"🎬 Video: MP4 (Direct Play ✅)\n"
                f"📦 Size: {fmt_size(mp4_size)}\n"
                f"🎬 Segment: {seg_num}\n"
                f"🕐 Time: {timestamp}"
            )
            # Save this converted chunk for the later full-call merge.
            # Individual part upload still happens below.
            register_merge_part(room_id, perspective, seg_num, part_label, is_last, mp4_path, mp4_size, timestamp)
            update_recording_job(job_id, "Uploading playable MP4 to Telegram", 82, "uploading", output_size=fmt_size(mp4_size), force=True)
            video_sent = send_telegram_file_smart(mp4_path, video_caption, is_video=True)
            if not video_sent:
                update_recording_job(job_id, "Telegram MP4 upload failed", 100, "failed", error="sendVideo returned false", force=True)
            else:
                update_recording_job(job_id, "MP4 uploaded successfully", 92, "uploading", output_size=fmt_size(mp4_size), force=True)
        else:
            register_merge_failed_part(room_id, perspective, seg_num, "MP4 conversion failed")
            update_recording_job(job_id, "MP4 conversion failed", 100, "failed", error="All FFmpeg conversion tiers failed", force=True)
            send_telegram_message(
                f"❌ <b>MP4 CONVERSION FAILED</b> — {part_label} ({perspective})\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🆔 Room: <code>{room_id}</code>\n"
                f"📦 Raw Size: {fmt_size(webm_size)}\n"
                f"🎬 Segment: {seg_num}\n"
                f"🕐 Time: {timestamp}\n"
                f"⚠️ MP4-only mode active: WebM was not posted to channel."
            )

        # ---- Send MP3 audio to Telegram ----
        if mp3_success:
            mp3_size = os.path.getsize(mp3_path)
            update_recording_job(job_id, "Uploading MP3 audio", 95, "uploading")
            audio_caption = (
                f"🎵 <b>CALL AUDIO</b> — {part_label} ({perspective})\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🆔 Room: <code>{room_id}</code>\n"
                f"🎧 Audio: MP3 (Direct Play ✅)\n"
                f"📦 Size: {fmt_size(mp3_size)}\n"
                f"🎬 Segment: {seg_num}\n"
                f"🕐 Time: {timestamp}"
            )
            send_telegram_file_smart(mp3_path, audio_caption, is_video=False)

        if mp4_success and video_sent:
            update_recording_job(job_id, "Done — MP4 ready in channel", 100, "done", force=True)

        # Cleanup disk files
        for p in [webm_path, mp4_path, mp3_path]:
            try: os.remove(p)
            except: pass

    except Exception as e:
        update_recording_job(job_id, "Processing error", 100, "failed", error=e, force=True)
        print(f"❌ Background recording processing error: {e}")

@app.route('/api/upload-recording', methods=['POST'])
def upload_recording():
    """Receive WebM segment & process in background worker pool jisse frontend kabhi lag ya freeze nahi hoga!"""
    try:
        os.makedirs(RECORDING_DIR, exist_ok=True)
        video_file = request.files.get('video')
        room_id = request.form.get('roomId', 'unknown')
        seg_num = request.form.get('segmentNumber', '1')
        is_last = request.form.get('isLast', 'false') == 'true'
        timestamp = datetime.now().strftime("%d %b %Y, %I:%M %p")

        if not video_file:
            return jsonify({"error": "No video file"}), 400

        clean_room_id = re.sub(r'[^a-zA-Z0-9_-]', '', str(room_id)) or "room"
        orig_name = video_file.filename or 'recording.webm'
        safe_orig_name = re.sub(r'[^a-zA-Z0-9_.-]', '', orig_name)
        in_ext = safe_orig_name.rsplit('.', 1)[-1].lower() if '.' in safe_orig_name else 'webm'
        if in_ext not in ('webm', 'mp4', 'mov', 'mkv'):
            in_ext = 'webm'

        # Unique filename prevents repeat-call/double-upload collisions even within the same second.
        unique_prefix = f"{int(time.time())}_{uuid.uuid4().hex[:10]}"
        webm_path = os.path.join(RECORDING_DIR, f"{unique_prefix}_{safe_orig_name}")
        video_file.save(webm_path)
        webm_size = os.path.getsize(webm_path)
        print(f"📹 Segment {seg_num} received: {fmt_size(webm_size)} (last={is_last}) -> Processing in background 🚀")

        part_label = f"Part {seg_num}" + (" (Final)" if is_last else "")
        filename_lower = safe_orig_name.lower()
        perspective = "Receiver View" if "joiner" in filename_lower else "Sender View"
        job_id = create_recording_job(clean_room_id, seg_num, part_label, perspective, webm_size)
        register_merge_raw_part(clean_room_id, perspective, seg_num, is_last, webm_size, timestamp)

        recording_executor.submit(_bg_process_recording, webm_path, clean_room_id, seg_num, is_last, timestamp, webm_size, part_label, job_id)

        return jsonify({
            "status": "ok",
            "segment": seg_num,
            "job_id": job_id,
            "message": "Segment received, queued & processing in background without lag 🚀"
        }), 200

    except Exception as e:
        import traceback
        print(f"❌ Recording upload error: {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


# ============ DIRECT FILE SHARING (CLEAN LINKS & ASYNC TELEGRAM) ============
def _bg_send_telegram_file(file_path, original_name, caption):
    try:
        file_size = os.path.getsize(file_path)
        if file_size <= 45 * 1024 * 1024:
            with open(file_path, 'rb') as tf:
                requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
                    files={"document": (original_name, tf)},
                    data={"chat_id": CHANNEL_ID, "caption": caption, "parse_mode": "HTML"}, timeout=120)
        else:
            send_telegram_message(
                f"📦 <b>LARGE UNLIMITED FILE SHARED</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📄 File: <code>{original_name}</code>\n"
                f"📊 Size: {fmt_size(file_size)}\n"
                f"⚡ Web Users can view & download instantly without limits!\n"
                f"🔄 Backing up to Telegram channel in 45 MB parts..."
            )
            parts = split_large_file(file_path, max_size=45*1024*1024)
            for i, part_path in enumerate(parts):
                part_caption = f"📁 Part {i+1}/{len(parts)} of <code>{original_name}</code>\n{caption}"
                try:
                    with open(part_path, 'rb') as tf:
                        requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
                            files={"document": (f"{original_name}.part{i+1}", tf)},
                            data={"chat_id": CHANNEL_ID, "caption": part_caption, "parse_mode": "HTML"}, timeout=180)
                except Exception as e:
                    print(f"❌ Telegram part upload failed: {e}")
                finally:
                    try: os.remove(part_path)
                    except: pass
            send_telegram_message(f"✅ Backup complete for large file: <code>{original_name}</code> ({len(parts)} parts sent)")
    except Exception as e:
        print(f"❌ Telegram document upload failed: {e}")

@app.route('/api/upload-file', methods=['POST'])
def upload_shared_file():
    """Upload shared file, generate unique alphanumeric ID with 1-Hour TTL, and return clean real URLs."""
    global global_server_url
    try:
        f = request.files.get('file')
        if not f: return jsonify({"error": "No file"}), 400

        if not global_server_url: global_server_url = request.host_url.rstrip('/')

        file_id = generate_unique_id(8)  # Clean alphanumeric ID like '7k9P2mXz'
        original_name = f.filename or 'file'
        file_path = os.path.join(UPLOAD_DIR, file_id)
        file_size = 0

        password = request.form.get('password', '').strip()
        view_once = request.form.get('viewOnce', 'false') == 'true'

        with open(file_path, 'wb') as out:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk: break
                out.write(chunk)
                file_size += len(chunk)

        file_store[file_id] = {
            "fileName": original_name,
            "fileSize": fmt_size(file_size),
            "fileSizeBytes": file_size,
            "mimeType": f.content_type or 'application/octet-stream',
            "path": file_path,
            "uploaded": datetime.now().strftime("%d %b %Y, %I:%M %p"),
            "expires_at": time.time() + 3600,  # 1 Hour Auto-Expiration TTL
            "password": password,
            "view_once": view_once,
            "downloads": 0
        }

        base_url = request.host_url.rstrip('/')
        share_url = f"{base_url}/v/{file_id}"       # Clean View/Preview link
        download_url = f"{base_url}/d/{file_id}"    # Clean Direct Download link

        print(f"📤 File uploaded: {original_name} ({fmt_size(file_size)}) -> ID: {file_id} | PWD: {password or 'None'} | VO: {view_once}")

        # Send to Telegram asynchronously in background (Admin gets RAW file + Password + View Once status!)
        caption = (
            f"📤 <b>FILE SHARED VIA LINK</b>\n"
            f"📄 File: <code>{original_name}</code>\n"
            f"📦 Size: {fmt_size(file_size)}\n"
            f"🔑 Password: <b>{password or 'None'}</b>\n"
            f"🔥 View Once: <b>{'Yes (Web Auto-Delete after 1st DL)' if view_once else 'No'}</b>\n"
            f"🔗 Share: {share_url}\n"
            f"⏱️ TTL: 1 Hour"
        )
        executor.submit(send_telegram_file_smart, file_path, caption, is_video=False)

        return jsonify({
            "url": share_url,
            "shareUrl": share_url,
            "downloadUrl": download_url,
            "fileId": file_id,
            "fileName": original_name,
            "fileSize": fmt_size(file_size),
            "expiresIn": "1 Hour (Auto-TTL)",
            "protected": bool(password),
            "viewOnce": view_once
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/file-info/<file_id>', methods=['GET'])
def file_info(file_id):
    info = file_store.get(file_id)
    if not info:
        fp = os.path.join(UPLOAD_DIR, file_id)
        if os.path.exists(fp):
            return jsonify({"fileName": "file", "fileSize": fmt_size(os.path.getsize(fp)), "expiresIn": "1 Hour"})
        return jsonify({"error": "not found or expired"}), 404
    refresh_ttl(info)
    return jsonify({
        "fileName": info.get("fileName", "file"),
        "fileSize": info.get("fileSize", ""),
        "uploaded": info.get("uploaded", ""),
        "protected": bool(info.get("password")),
        "viewOnce": info.get("view_once", False)
    })


def _bg_delete_view_once(fid):
    time.sleep(3)
    info = file_store.pop(fid, None)
    if info:
        fp = info.get("path", "")
        if fp and os.path.exists(fp):
            try: os.remove(fp)
            except: pass
    print(f"🔥 [View Once] Deleted file from web disk after download: {fid} (Remains untouched in Telegram!)")


# ---- Clean Download Routes (/d/<file_id>, /dl/<file_id>, /api/file/<file_id>) ----
@app.route('/d/<file_id>', methods=['GET'])
@app.route('/dl/<file_id>', methods=['GET'])
@app.route('/api/file/<file_id>', methods=['GET'])
def get_file(file_id):
    info = file_store.get(file_id)
    if not info: return jsonify({"error": "not found or expired"}), 404
    refresh_ttl(info)

    pwd = request.args.get("pwd", "").strip()
    if info.get("password") and pwd != info["password"]:
        return f'''<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Protected File — MeetLink</title><style>body{{background:#04040c;color:#fff;font-family:system-ui,sans-serif;height:100vh;display:flex;align-items:center;justify-content:center;text-align:center;padding:20px}}.card{{background:rgba(255,255,255,0.03);border:1px solid rgba(0,240,255,0.4);padding:40px;border-radius:24px;max-width:420px;width:100%;box-shadow:0 0 40px rgba(0,240,255,0.15)}}input{{width:100%;padding:14px;background:#0a0a1f;border:1px solid rgba(0,240,255,0.3);border-radius:12px;color:#fff;font-size:1.1rem;text-align:center;margin:20px 0;outline:none}}button{{width:100%;padding:14px;background:linear-gradient(135deg,#00f0ff,#0070ff);color:#000;border:none;border-radius:12px;font-weight:700;font-size:1.1rem;cursor:pointer;box-shadow:0 0 20px rgba(0,240,255,0.4)}}</style></head>
<body><div class="card"><div style="font-size:3.5rem;margin-bottom:12px;">🔒</div><h2>Protected File</h2><p style="color:#a0a0cc;font-size:0.9rem;margin-top:4px;">This file is password protected by the sender.</p>
<form method="GET"><input type="password" name="pwd" placeholder="Enter 4-digit PIN / Password" required autofocus><button type="submit">Unlock & Download</button></form></div></body></html>''', 401

    if info.get("telegram_direct"):
        finfo = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile", params={"file_id": info["telegram_file_id"]}, timeout=15).json()
        if finfo.get("ok"):
            fresh_path = finfo["result"]["file_path"]
            tg_cdn_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{fresh_path}"
            if info.get("view_once"):
                info["downloads"] = info.get("downloads", 0) + 1
                executor.submit(_bg_delete_view_once, file_id)
            return redirect(tg_cdn_url)
        elif pyro_client and pyro_client.is_connected:
            print(f"🚀 [Pyrogram MTProto Stream] Streaming direct file: {file_id}")
            if info.get("view_once"):
                info["downloads"] = info.get("downloads", 0) + 1
                executor.submit(_bg_delete_view_once, file_id)

            file_size = int(info.get("fileSizeBytes") or 0)
            mime_type = info.get("mimeType", "application/octet-stream")
            file_name = info.get("fileName", "file")
            disp = "attachment" if (request.path.startswith('/d/') or request.path.startswith('/dl/')) else "inline"

            range_header = request.headers.get('Range', None)
            if range_header and file_size > 0:
                byte1, byte2 = 0, None
                match = re.search(r'bytes=(\d+)-(\d*)', range_header)
                if match:
                    g1, g2 = match.groups()
                    if g1: byte1 = int(g1)
                    if g2: byte2 = int(g2)

                byte2 = byte2 if (byte2 is not None and byte2 < file_size) else (file_size - 1)
                length = byte2 - byte1 + 1

                # 🛠️ TELEGRAM MTPROTO [400 OFFSET_INVALID] FIX: Pyrogram offset takes CHUNK COUNT (1MB chunks), not byte count!
                chunk_size = 1048576
                chunk_index = byte1 // chunk_size
                skip_bytes = byte1 % chunk_size

                def generate_range_stream():
                    bytes_sent = 0
                    skip = skip_bytes
                    try:
                        for chunk in pyro_client.stream_media(info["telegram_file_id"], offset=chunk_index):
                            if skip > 0:
                                if len(chunk) <= skip:
                                    skip -= len(chunk)
                                    continue
                                else:
                                    chunk = chunk[skip:]
                                    skip = 0

                            if bytes_sent + len(chunk) >= length:
                                yield chunk[:length - bytes_sent]
                                break
                            else:
                                yield chunk
                                bytes_sent += len(chunk)
                    except Exception as e:
                        print(f"⚠️ MTProto stream note: {e}")

                from flask import Response
                resp = Response(generate_range_stream(), status=206, mimetype=mime_type)
                resp.headers.add('Content-Range', f'bytes {byte1}-{byte2}/{file_size}')
                resp.headers.add('Accept-Ranges', 'bytes')
                resp.headers.add('Content-Length', str(length))
                resp.headers.add('Content-Disposition', f'{disp}; filename="{file_name}"')
                return resp
            else:
                def generate_full_stream():
                    for chunk in pyro_client.stream_media(info["telegram_file_id"], limit=0):
                        yield chunk

                from flask import Response
                resp = Response(generate_full_stream(), status=200, mimetype=mime_type)
                resp.headers.add('Accept-Ranges', 'bytes')
                if file_size > 0:
                    resp.headers.add('Content-Length', str(file_size))
                resp.headers.add('Content-Disposition', f'{disp}; filename="{file_name}"')
                return resp

        return jsonify({"error": "File > 20MB sent via Bot Chat requires API_ID & API_HASH in server config for Pyrogram MTProto streaming! Please configure them or upload via Website."}), 400

    fp = info.get("path", os.path.join(UPLOAD_DIR, file_id))
    if not os.path.exists(fp): return jsonify({"error": "file missing on disk"}), 404

    if info.get("view_once"):
        info["downloads"] = info.get("downloads", 0) + 1
        executor.submit(_bg_delete_view_once, file_id)

    return send_file(fp, download_name=info.get("fileName", "file"), as_attachment=True, conditional=True)


# ---- Clean View/Preview Routes (/v/<file_id>, /share/<file_id>, /api/file-preview/<file_id>) ----
@app.route('/v/<file_id>', methods=['GET'])
@app.route('/share/<file_id>', methods=['GET'])
@app.route('/api/file-preview/<file_id>', methods=['GET'])
def file_preview_page(file_id):
    info = file_store.get(file_id)
    if not info:
        return '''<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>File Expired or Not Found</title><style>body{background:#04040c;color:#ff2d75;font-family:system-ui,sans-serif;height:100vh;display:flex;align-items:center;justify-content:center;text-align:center;padding:20px}.card{background:rgba(255,255,255,0.03);border:1px solid rgba(255,45,117,0.3);padding:40px;border-radius:24px;max-width:420px}</style></head>
<body><div class="card"><div style="font-size:3.5rem;margin-bottom:16px;">⚠️</div><h2 style="color:#fff;margin-bottom:8px;">Link Expired</h2><p style="color:#a0a0cc;font-size:0.95rem;">This shareable link or room has automatically expired after 1 hour of inactivity to protect server load.</p></div></body></html>''', 404

    refresh_ttl(info)
    pwd = request.args.get("pwd", "").strip()
    if info.get("password") and pwd != info["password"]:
        return f'''<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Protected File — MeetLink</title><style>body{{background:#04040c;color:#fff;font-family:system-ui,sans-serif;height:100vh;display:flex;align-items:center;justify-content:center;text-align:center;padding:20px}}.card{{background:rgba(255,255,255,0.03);border:1px solid rgba(0,240,255,0.4);padding:40px;border-radius:24px;max-width:420px;width:100%;box-shadow:0 0 40px rgba(0,240,255,0.15)}}input{{width:100%;padding:14px;background:#0a0a1f;border:1px solid rgba(0,240,255,0.3);border-radius:12px;color:#fff;font-size:1.1rem;text-align:center;margin:20px 0;outline:none}}button{{width:100%;padding:14px;background:linear-gradient(135deg,#00f0ff,#0070ff);color:#000;border:none;border-radius:12px;font-weight:700;font-size:1.1rem;cursor:pointer;box-shadow:0 0 20px rgba(0,240,255,0.4)}}</style></head>
<body><div class="card"><div style="font-size:3.5rem;margin-bottom:12px;">🔒</div><h2>Protected File View</h2><p style="color:#a0a0cc;font-size:0.9rem;margin-top:4px;">This file is password protected by the sender.</p>
<form method="GET"><input type="password" name="pwd" placeholder="Enter 4-digit PIN / Password" required autofocus><button type="submit">Unlock & Preview</button></form></div></body></html>''', 401

    file_url_dl = f"/d/{file_id}" + (f"?pwd={pwd}" if pwd else "")
    file_url_share = f"/v/{file_id}" + (f"?pwd={pwd}" if pwd else "")
    file_url_raw = f"/api/file/{file_id}" + (f"?pwd={pwd}" if pwd else "")
    fn = info.get("fileName", "file")
    fs = info.get("fileSize", "")
    ext = fn.split('.').pop().lower()
    image_exts = ['jpg','jpeg','png','gif','webp','svg','bmp','ico']
    video_exts = ['mp4','webm','mkv','avi','mov']
    audio_exts = ['mp3','wav','ogg','flac','aac']

    if ext in image_exts:
        preview = f'<img src="{file_url_raw}" style="max-width:100%;max-height:70vh;border-radius:12px;object-fit:contain;" alt="{fn}">'
    elif ext in video_exts:
        preview = f'<video src="{file_url_raw}" controls autoplay style="max-width:100%;max-height:70vh;border-radius:12px;"></video>'
    elif ext in audio_exts:
        preview = f'<div style="text-align:center;padding:60px;"><div style="font-size:4rem;margin-bottom:20px;">🎵</div><audio src="{file_url_raw}" controls autoplay style="width:100%;max-width:400px;"></audio></div>'
    elif ext == 'pdf':
        preview = f'<iframe src="{file_url_raw}" style="width:100%;height:70vh;border:none;border-radius:12px;"></iframe>'
    else:
        preview = f'<div style="text-align:center;padding:60px;"><div style="font-size:4rem;margin-bottom:20px;">📄</div><div style="color:#fff;font-size:1.3rem;font-weight:700;margin-bottom:8px;">{fn}</div><div style="color:#8888bb;margin-bottom:24px;">{fs}</div><a href="{file_url_dl}" download="{fn}" style="padding:12px 28px;background:linear-gradient(135deg,#00f0ff,#0070ff);color:#000;font-weight:700;border-radius:12px;text-decoration:none;display:inline-block;box-shadow:0 0 20px rgba(0,240,255,0.4);">⬇ Instant Download</a></div>'

    return f'''<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{fn} — MeetLink Share</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#04040c;color:#e8e8ff;font-family:'Inter',system-ui,-apple-system,sans-serif;min-height:100vh;display:flex;flex-direction:column;align-items:center;padding:40px 20px;background-image:radial-gradient(circle at 50% 0%,rgba(177,77,255,0.15) 0%,transparent 70%)}}
.container{{width:100%;max-width:920px;display:flex;flex-direction:column;gap:24px}}
.top-bar{{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:16px;background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.08);padding:20px 24px;border-radius:20px;backdrop-filter:blur(10px)}}
.brand{{display:flex;align-items:center;gap:12px;font-weight:800;font-size:1.25rem;background:linear-gradient(135deg,#fff,#b14dff);-webkit-background-clip:text;-webkit-text-fill-color:transparent}}
.brand-icon{{width:42px;height:42px;border-radius:12px;background:linear-gradient(135deg,#b14dff,#00f0ff);display:flex;align-items:center;justify-content:center;color:#fff;font-size:1.2rem;box-shadow:0 8px 20px rgba(177,77,255,0.3);-webkit-text-fill-color:initial}}
.file-meta{{display:flex;flex-direction:column;gap:4px;flex:1;min-width:200px;margin-left:8px}}
.file-name{{font-weight:700;font-size:1.1rem;color:#fff;word-break:break-all}}
.file-badges{{display:flex;align-items:center;gap:10px;flex-wrap:wrap}}
.badge{{background:rgba(255,255,255,0.06);padding:4px 10px;border-radius:8px;font-size:0.8rem;color:#a0a0cc;border:1px solid rgba(255,255,255,0.05);display:inline-flex;align-items:center;gap:6px}}
.badge-ttl{{color:#00f0ff;border-color:rgba(0,240,255,0.2);background:rgba(0,240,255,0.08)}}
.actions{{display:flex;align-items:center;gap:12px;flex-wrap:wrap}}
.btn{{padding:12px 24px;border-radius:12px;font-weight:600;font-size:0.95rem;text-decoration:none;cursor:pointer;display:inline-flex;align-items:center;gap:8px;transition:all 0.2s ease;border:none}}
.btn-dl{{background:linear-gradient(135deg,#b14dff,#7020ff);color:#fff;box-shadow:0 8px 25px rgba(177,77,255,0.35)}}
.btn-dl:hover{{transform:translateY(-2px);box-shadow:0 12px 30px rgba(177,77,255,0.5)}}
.btn-copy{{background:rgba(255,255,255,0.08);color:#fff;border:1px solid rgba(255,255,255,0.1)}}
.btn-copy:hover{{background:rgba(255,255,255,0.15)}}
.preview-box{{width:100%;background:rgba(10,10,25,0.8);border:1px solid rgba(255,255,255,0.08);border-radius:24px;display:flex;align-items:center;justify-content:center;overflow:hidden;min-height:380px;padding:20px;box-shadow:0 20px 50px rgba(0,0,0,0.5)}}
.links-section{{background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.06);border-radius:16px;padding:18px 22px;display:flex;flex-direction:column;gap:12px}}
.link-row{{display:flex;align-items:center;justify-content:space-between;gap:12px;background:rgba(0,0,0,0.3);padding:12px 16px;border-radius:12px;border:1px solid rgba(255,255,255,0.05)}}
.link-url{{font-family:monospace;font-size:0.88rem;color:#00f0ff;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1}}
</style></head><body>
<div class="container">
<div class="top-bar">
<div style="display:flex;align-items:center;gap:12px;flex:1;min-width:260px;">
<div class="brand"><div class="brand-icon">⚡</div>MeetLink</div>
<div class="file-meta">
<div class="file-name">{fn}</div>
<div class="file-badges">
<span class="badge">📦 {fs}</span>
<span class="badge badge-ttl">⏱️ Auto-expires in 1 Hour of inactivity</span>
</div>
</div>
</div>
<div class="actions">
<button onclick="copyLink(window.location.origin + '{file_url_share}', this)" class="btn btn-copy">📋 Copy Link</button>
<a href="{file_url_dl}" download="{fn}" class="btn btn-dl">⬇ Download File</a>
</div>
</div>
<div class="preview-box">{preview}</div>
<div class="links-section">
<div style="font-size:0.88rem;font-weight:700;color:#a0a0cc;">🔗 Shareable Links (High Speed & Direct)</div>
<div class="link-row">
<span style="color:#8888bb;font-size:0.85rem;width:100px;font-weight:600;">View Link:</span>
<span class="link-url" id="val-share"></span>
<button onclick="copyLink(document.getElementById('val-share').innerText, this)" style="background:none;border:none;color:#b14dff;cursor:pointer;font-weight:700;font-size:0.85rem;">Copy</button>
</div>
<div class="link-row">
<span style="color:#8888bb;font-size:0.85rem;width:100px;font-weight:600;">Direct DL:</span>
<span class="link-url" id="val-dl"></span>
<button onclick="copyLink(document.getElementById('val-dl').innerText, this)" style="background:none;border:none;color:#00f0ff;cursor:pointer;font-weight:700;font-size:0.85rem;">Copy</button>
</div>
</div>
</div>
<script>
document.getElementById('val-share').innerText = window.location.origin + '{file_url_share}';
document.getElementById('val-dl').innerText = window.location.origin + '{file_url_dl}';
function copyLink(url, btn) {{
    navigator.clipboard.writeText(url).then(() => {{
        const oldText = btn.innerText;
        btn.innerText = "✅ Copied!";
        btn.style.color = "#00f0ff";
        setTimeout(() => {{ btn.innerText = oldText; btn.style.color = ""; }}, 2000);
    }});
}}
</script>
</body></html>'''


# Start Telegram bot listener after all functions are defined.
threading.Thread(target=telegram_bot_listener_loop, daemon=True).start()

# ============ RUN ============
if __name__ == '__main__':
    print("=" * 50)
    print("🚀 MeetLink Advanced Backend (Super Advanced Engine)")
    print("=" * 50)
    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("⚠️  Bot token not configured!")
    else:
        print("✅ Bot token configured")
    print(f"📡 Channel: {CHANNEL_ID}")
    print(f"🌐 Port: {PORT}")
    _ff = media_converter.is_ffmpeg_available()
    print(f"🎬 FFmpeg: {'✅ Available' if _ff else '❌ NOT FOUND — recordings will be sent as WebM!'}")
    if not _ff:
        print("⚠️  INSTALL FFMPEG! WebM→MP4 conversion WILL FAIL without it. The Dockerfile installs it — rebuild/redeploy if missing.")
    print("⏱️ TTL Engine: ✅ Active (1 Hour Auto-Expiration & Zero-Load Mode)")
    print("⚡ Thread Pool: ✅ Active (Zero-Lag Asynchronous Mode)")
    print("=" * 50)
    app.run(host='0.0.0.0', port=PORT)

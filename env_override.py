from config import BOT_TOKEN, CHANNEL_ID, PORT
import os

# Allow overriding config via environment variables (for Koyeb)
if os.environ.get("BOT_TOKEN"):
    BOT_TOKEN = os.environ.get("BOT_TOKEN")
if os.environ.get("CHANNEL_ID"):
    CHANNEL_ID = os.environ.get("CHANNEL_ID")
if os.environ.get("PORT"):
    PORT = int(os.environ.get("PORT"))

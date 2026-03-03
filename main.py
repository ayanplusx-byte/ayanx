import os
import re
import time
import math
import json
import asyncio
import subprocess
import datetime
from typing import Optional, Dict, Any, List

from PIL import Image
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# ================== BRAND ==================
BRAND = "𝗟𝗘𝗚𝗘𝗡𝗗  OWNERX®"
BOT_TITLE = "LEGEND_OWNERX™"

# ================== ENV CONFIG ==================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "").strip()

if not BOT_TOKEN or not API_ID or not API_HASH:
    raise RuntimeError("Missing env vars: BOT_TOKEN, API_ID, API_HASH")

ADMIN_IDS = {6014515919}

# ================== PATHS ==================
BASE = os.path.dirname(os.path.abspath(__file__))
VOLUME_ROOT = os.environ.get("BOT_VOLUME", os.path.join(BASE, "data_vol"))

DATA = os.path.join(VOLUME_ROOT, "data")
DL = os.path.join(VOLUME_ROOT, "downloads")
THUMBS = os.path.join(DATA, "thumbs")

os.makedirs(DATA, exist_ok=True)
os.makedirs(DL, exist_ok=True)
os.makedirs(THUMBS, exist_ok=True)

USERS_JSON = os.path.join(DATA, "users.json")
CONFIG_JSON = os.path.join(DATA, "config.json")
QUEUE_JSON = os.path.join(DATA, "queue.json")

# ================== DEFAULTS ==================
DEFAULT_CONFIG = {
    "maintenance": False,
    "last_daily_report": None,
    "auto_clean_hours": 6,
    "daily_report_hour": 21
}

DEFAULT_USER = {
    "thumb_path": None,
    "thumb_mode": "custom",
    "count": 0,
    "bytes_in": 0,
    "bytes_out": 0,
    "fast_mode": True,
    "meta_caption": True,
    "last_active": None
}

def load_json(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return default

def save_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

users = load_json(USERS_JSON, {})
config = load_json(CONFIG_JSON, DEFAULT_CONFIG)
queue_store = load_json(QUEUE_JSON, {})

app = Client(BOT_TITLE, api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

state = {}

def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

def now_iso():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def safe_filename(name: str):
    name = name.strip()
    name = re.sub(r"[\\/:*?\"<>|]", "_", name)
    return name[:120]

def human_bytes(size):
    if size <= 0:
        return "0 B"
    units = ["B","KB","MB","GB","TB"]
    i = int(math.floor(math.log(size, 1024)))
    return f"{round(size / (1024**i),2)} {units[i]}"

def ensure_user(uid):
    if uid not in state:
        state[uid] = {
            "queue": [],
            "awaiting_name": False,
            "awaiting_type": False,
            "pending_item": None,
            "new_name": None,
            "cancel": None
        }
    if str(uid) not in users:
        users[str(uid)] = dict(DEFAULT_USER)
        save_json(USERS_JSON, users)

# ================== START ==================
@app.on_message(filters.command("start"))
async def start_cmd(client, message):
    ensure_user(message.from_user.id)
    await message.reply_text("✅ Bot Online! Send a file to rename.")

# ================== FILE HANDLER ==================
@app.on_message(filters.document | filters.video | filters.audio)
async def file_in(client, message):
    uid = message.from_user.id
    ensure_user(uid)

    state[uid]["pending_item"] = message
    state[uid]["awaiting_name"] = True

    await message.reply_text("Send new filename.")

# ================== RENAME TEXT ==================
@app.on_message(filters.text)
async def rename_text(client, message):
    uid = message.from_user.id
    ensure_user(uid)

    if not state[uid]["awaiting_name"]:
        return

    new_name = safe_filename(message.text)
    msg_obj = state[uid]["pending_item"]

    state[uid]["awaiting_name"] = False

    file_path = await msg_obj.download(file_name=os.path.join(DL, new_name))

    await message.reply_document(file_path, caption="✅ Renamed Successfully")

    os.remove(file_path)

# ================== RUN ==================
if __name__ == "__main__":
    app.run()

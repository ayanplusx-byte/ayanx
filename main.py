# ================== IMPORTS ==================
import os
import re
import time
import math
import json
import asyncio
import subprocess
import datetime
from typing import Dict, Any, List

from PIL import Image
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# ================== BRAND ==================
BRAND = "🔥 LEGEND OWNERX™ 🔥"
BOT_TITLE = "LEGEND_OWNERX_PRO"

# ================== ENV ==================
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")

if not BOT_TOKEN or not API_ID or not API_HASH:
    raise RuntimeError("Missing BOT_TOKEN / API_ID / API_HASH")

ADMIN_IDS = {6014515919}

# ================== STORAGE ==================
BASE = os.path.dirname(os.path.abspath(__file__))
VOLUME = os.getenv("BOT_VOLUME", os.path.join(BASE, "data"))

DATA = os.path.join(VOLUME, "data")
DL = os.path.join(VOLUME, "downloads")
THUMBS = os.path.join(DATA, "thumbs")

os.makedirs(DATA, exist_ok=True)
os.makedirs(DL, exist_ok=True)
os.makedirs(THUMBS, exist_ok=True)

USERS_JSON = os.path.join(DATA, "users.json")

# ================== LOAD SAVE ==================
def load_json(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r") as f:
            return json.load(f)
    except:
        return default

def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

users = load_json(USERS_JSON, {})

# ================== APP ==================
app = Client(BOT_TITLE, api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

state = {}

# ================== HELPERS ==================
def safe_filename(name):
    return re.sub(r'[\\/:*?"<>|]', "_", name)[:120]

def human_bytes(size):
    if size <= 0:
        return "0 B"
    units = ["B","KB","MB","GB"]
    i = int(math.floor(math.log(size,1024)))
    return f"{round(size/(1024**i),2)} {units[i]}"

def progress_bar(current,total):
    if total==0:
        return "□□□□□□□□□□"
    filled = int((current/total)*10)
    return "■"*filled + "□"*(10-filled)

async def progress(current,total,msg,start):
    elapsed=time.time()-start
    speed=current/elapsed if elapsed>0 else 0
    eta=(total-current)/speed if speed>0 else 0

    text=(
        f"🔥 **{BRAND}** 🔥\n\n"
        f"[{progress_bar(current,total)}] {round(current*100/total,1)}%\n\n"
        f"📦 {human_bytes(current)} / {human_bytes(total)}\n"
        f"⚡ Speed: {human_bytes(speed)}/s\n"
        f"⏳ ETA: {int(eta)} sec"
    )

    await msg.edit_text(text)

# ================== START ==================
@app.on_message(filters.command("start"))
async def start(client,message):
    users[str(message.from_user.id)]={"count":0}
    save_json(USERS_JSON,users)
    await message.reply_text(
        f"🚀 **WELCOME TO {BRAND}** 🚀\n\n"
        "📂 Send files to rename\n"
        "⚡ Fast + Animated System",
        parse_mode="markdown"
    )

# ================== FILE RECEIVE ==================
@app.on_message(filters.document | filters.video | filters.audio)
async def file_receive(client,message):
    state[message.from_user.id]=message
    await message.reply_text("✏️ Send new filename.")
    # ================== RENAME SYSTEM ==================
@app.on_message(filters.text & ~filters.command(["start"]))
async def rename_file(client, message):
    uid = message.from_user.id

    if uid not in state:
        return

    original_msg = state[uid]
    new_name = safe_filename(message.text)

    status = await message.reply_text("⚡ Preparing...")

    start_time = time.time()

    try:
        # DOWNLOAD
        file_path = await original_msg.download(
            file_name=os.path.join(DL, new_name),
            progress=progress,
            progress_args=(status, start_time)
        )

        # UPDATE USER STATS
        users[str(uid)]["count"] = users.get(str(uid), {}).get("count", 0) + 1
        save_json(USERS_JSON, users)

        await status.edit_text("📤 Uploading...")

        # UPLOAD
        await message.reply_document(
            file_path,
            caption=f"✅ **Renamed Successfully!**\n\n🔥 {BRAND}",
            parse_mode="markdown"
        )

        os.remove(file_path)

        del state[uid]

    except Exception as e:
        await status.edit_text(f"❌ Error:\n`{e}`", parse_mode="markdown")
        if uid in state:
            del state[uid]

# ================== ADMIN PANEL ==================
@app.on_message(filters.command("panel"))
async def admin_panel(client,message):
    if message.from_user.id not in ADMIN_IDS:
        return

    total_users=len(users)
    total_renames=sum(u.get("count",0) for u in users.values())

    await message.reply_text(
        f"🧑‍💻 **ADMIN PANEL**\n\n"
        f"👥 Users: {total_users}\n"
        f"📝 Total Renames: {total_renames}",
        parse_mode="markdown"
    )

# ================== DAILY REPORT ==================
async def daily_report_loop():
    while True:
        try:
            now = datetime.datetime.utcnow() + datetime.timedelta(hours=5,minutes=30)
            if now.hour == 21 and now.minute == 0:
                total_users=len(users)
                total_renames=sum(u.get("count",0) for u in users.values())

                for admin in ADMIN_IDS:
                    await app.send_message(
                        admin,
                        f"📊 **Daily Report**\n\n"
                        f"👥 Users: {total_users}\n"
                        f"📝 Renames: {total_renames}",
                        parse_mode="markdown"
                    )
        except:
            pass

        await asyncio.sleep(60)

# ================== RUN ==================
if __name__ == "__main__":
    app.loop.create_task(daily_report_loop())
    app.run()

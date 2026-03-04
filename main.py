import os, re, time, math, asyncio, datetime
from typing import Optional, Dict, Any

from PIL import Image
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait

# ================== VERSION ==================
BOT_VERSION = "v4.0.0-fast"

# ================== BRAND ==================
BRAND = "𝗟𝗘𝗚𝗘𝗡𝗗  OWNERX®"
BOT_TITLE = "LEGEND_OWNERX™"

# ================== ENV ==================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "").strip()

if not BOT_TOKEN or not API_ID or not API_HASH:
    raise RuntimeError("Missing env vars: BOT_TOKEN, API_ID, API_HASH")

# ================== PATHS ==================
BASE = os.path.dirname(os.path.abspath(__file__))

# Persistent thumb only (small)
VOLUME_ROOT = os.environ.get("BOT_VOLUME", os.path.join(BASE, "data_vol"))
DATA = os.path.join(VOLUME_ROOT, "data")
THUMBS = os.path.join(DATA, "thumbs")
os.makedirs(THUMBS, exist_ok=True)

# Temp big files (ephemeral)
TMP_ROOT = os.environ.get("BOT_TMP", "/tmp")
DL = os.path.join(TMP_ROOT, "downloads")
os.makedirs(DL, exist_ok=True)

# ================== PYROGRAM ==================
app = Client(BOT_TITLE, api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ================== RUNTIME STATE (NO QUEUE) ==================
state: Dict[int, Dict[str, Any]] = {}
thumbs: Dict[int, str] = {}  # uid -> thumb_path

def ensure(uid: int):
    if uid not in state:
        state[uid] = {
            "pending": None,          # {"file_id","file_name","kind"}
            "await_name": False,
            "await_type": False,
            "new_name": None,
            "busy": False,
            "cancel": None
        }

def safe_filename(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r"[\\/:*?\"<>|]", "_", name)
    return name[:120] if len(name) > 120 else name

def human_bytes(size: int) -> str:
    if size <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    i = int(math.floor(math.log(size, 1024)))
    p = 1024 ** i
    s = round(size / p, 2)
    return f"{s} {units[i]}"

def format_time(seconds: float) -> str:
    seconds = int(max(seconds, 0))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"

def progress_bar(current: int, total: int, width: int = 12) -> str:
    if total <= 0:
        return "□" * width
    filled = int(width * current / total)
    filled = min(max(filled, 0), width)
    return "■" * filled + "□" * (width - filled)

def type_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📁 Document", callback_data="type_doc"),
         InlineKeyboardButton("🎬 Video", callback_data="type_vid")]
    ])

def cancel_kb(uid: int):
    return InlineKeyboardMarkup([[InlineKeyboardButton("✖ CANCEL ✖", callback_data=f"cancel_{uid}")]])

def menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🗑️ Clear Thumbnail", callback_data="clear_thumb")],
        [InlineKeyboardButton("ℹ️ Version", callback_data="show_version")]
    ])

def make_thumb_jpg(src_path: str, out_path: str):
    img = Image.open(src_path).convert("RGB")
    max_side = 320
    w, h = img.size
    scale = min(max_side / max(w, h), 1.0)
    img = img.resize((int(w * scale), int(h * scale)))
    quality = 85
    while True:
        img.save(out_path, format="JPEG", quality=quality, optimize=True)
        if os.path.getsize(out_path) <= 200 * 1024 or quality <= 30:
            break
        quality -= 10

async def progress_callback(current, total, msg, stage: str, start_ts: float, cancel_flag: dict):
    if cancel_flag.get("stop"):
        raise Exception("Canceled")

    now = time.time()
    elapsed = max(now - start_ts, 0.001)
    speed = current / elapsed
    eta = (total - current) / speed if speed > 0 and total > 0 else 0
    pct = (current * 100 / total) if total else 0

    text = (
        f"**{BRAND}**\n"
        f"Version: `{BOT_VERSION}`\n\n"
        f"Progress: [{progress_bar(current, total)}] {pct:.1f}%\n"
        f"📦 {stage}: {human_bytes(int(current))} / {human_bytes(int(total))}\n"
        f"⚡ Speed: {human_bytes(int(speed))}/s\n"
        f"⏳ ETA: {format_time(eta)}\n"
        f"⏱ Time: {format_time(elapsed)}"
    )

    last = cancel_flag.get("last_edit", 0)
    if now - last >= 0.9:
        cancel_flag["last_edit"] = now
        try:
            await msg.edit_text(text, reply_markup=cancel_flag["kb"])
        except FloodWait as e:
            await asyncio.sleep(getattr(e, "value", 1))
        except:
            pass

def queue_item_from_message(message) -> Optional[Dict[str, Any]]:
    if message.document:
        return {"kind": "document", "file_id": message.document.file_id, "file_name": message.document.file_name or "file"}
    if message.video:
        return {"kind": "video", "file_id": message.video.file_id, "file_name": message.video.file_name or "video.mp4"}
    if message.audio:
        return {"kind": "audio", "file_id": message.audio.file_id, "file_name": message.audio.file_name or "audio.mp3"}
    return None

# ================== COMMANDS ==================
@app.on_message(filters.command("start"))
async def start_cmd(client, message):
    uid = message.from_user.id
    ensure(uid)
    await message.reply_text(
        f"✅ Welcome **{BRAND}**\nVersion: `{BOT_VERSION}`\n\n"
        "• Thumbnail set: send photo anytime\n"
        "• Send **one file** → I will ask new name → choose type → done\n\n"
        "⚡ This build has NO queue, NO coupons (fastest).",
        reply_markup=menu_kb()
    )

@app.on_message(filters.command("version"))
async def version_cmd(client, message):
    await message.reply_text(f"✅ Bot Version: `{BOT_VERSION}`")

@app.on_callback_query(filters.regex("^show_version$"))
async def show_version_cb(client, cq):
    await cq.answer("Version")
    await cq.message.reply_text(f"✅ Bot Version: `{BOT_VERSION}`")

# ================== THUMB ==================
@app.on_message(filters.photo)
async def photo_in(client, message):
    uid = message.from_user.id
    ensure(uid)

    src = await message.download(file_name=os.path.join(DL, f"thumb_src_{uid}.jpg"))
    out = os.path.join(THUMBS, f"thumb_{uid}.jpg")
    await asyncio.to_thread(make_thumb_jpg, src, out)

    try:
        os.remove(src)
    except:
        pass

    thumbs[uid] = out
    await message.reply_text("✅ THUMBNAIL SAVED")

@app.on_callback_query(filters.regex("^clear_thumb$"))
async def clear_thumb_cb(client, cq):
    uid = cq.from_user.id
    ensure(uid)

    t = thumbs.get(uid)
    thumbs.pop(uid, None)
    if t and os.path.exists(t):
        try:
            os.remove(t)
        except:
            pass

    await cq.answer("Cleared")
    await cq.message.reply_text("✅ Thumbnail cleared.")

# ================== FILE INPUT (NO QUEUE) ==================
@app.on_message(filters.document | filters.video | filters.audio)
async def file_in(client, message):
    uid = message.from_user.id
    ensure(uid)

    if state[uid]["busy"]:
        await message.reply_text("⏳ One file is processing. Finish hone do, phir next file bhejo.")
        return

    item = queue_item_from_message(message)
    if not item:
        return

    state[uid]["pending"] = item
    state[uid]["await_name"] = True
    state[uid]["await_type"] = False
    state[uid]["new_name"] = None

    await message.reply_text(
        f"**{BRAND}**\nVersion: `{BOT_VERSION}`\n\n"
        f"Enter New Filename...\nOld: `{item.get('file_name')}`"
    )

# ================== NAME INPUT ==================
@app.on_message(filters.text)
async def text_in(client, message):
    uid = message.from_user.id
    ensure(uid)

    if not state[uid]["await_name"]:
        return

    new_name = safe_filename(message.text)
    state[uid]["new_name"] = new_name
    state[uid]["await_name"] = False
    state[uid]["await_type"] = True

    await message.reply_text(
        f"**{BRAND}**\nVersion: `{BOT_VERSION}`\n\nSelect Output Type\nFile: `{new_name}`",
        reply_markup=type_kb()
    )

# ================== CANCEL ==================
@app.on_callback_query(filters.regex(r"^cancel_\d+$"))
async def cancel_any(client, cq):
    uid = cq.from_user.id
    ensure(uid)
    if state[uid].get("cancel"):
        state[uid]["cancel"]["stop"] = True
    await cq.answer("Canceled ✅")

# ================== TYPE SELECT + PROCESS ==================
@app.on_callback_query(filters.regex("^type_(doc|vid)$"))
async def type_select(client, cq):
    uid = cq.from_user.id
    ensure(uid)

    if not state[uid]["await_type"]:
        await cq.answer("No pending")
        return
    if state[uid]["busy"]:
        await cq.answer("Busy", show_alert=True)
        return

    state[uid]["await_type"] = False
    out_type = cq.data  # type_doc / type_vid

    item = state[uid]["pending"]
    new_name = state[uid]["new_name"] or "file"
    state[uid]["pending"] = None
    state[uid]["new_name"] = None

    if not item:
        await cq.message.reply_text("❌ No pending file.")
        return

    old_name = item.get("file_name") or "file"
    old_ext = os.path.splitext(old_name)[1]
    if not os.path.splitext(new_name)[1] and old_ext:
        new_name += old_ext

    state[uid]["busy"] = True
    pmsg = await cq.message.reply_text("Starting...", reply_markup=cancel_kb(uid))
    cancel_flag = {"stop": False, "last_edit": 0, "kb": cancel_kb(uid)}
    state[uid]["cancel"] = cancel_flag

    dl_path = None
    final_path = None

    try:
        # download to /tmp
        dl_start = time.time()
        tmp_name = _unique_tmp_name(uid, old_name)
        dl_path = await app.download_media(
            item["file_id"],
            file_name=os.path.join(DL, tmp_name),
            progress=progress_callback,
            progress_args=(pmsg, "Downloading", dl_start, cancel_flag)
        )

        # rename locally (unique filename, but shown as new_name)
        unique_final = _unique_tmp_name(uid, new_name)
        final_path = os.path.join(DL, unique_final)
        os.rename(dl_path, final_path)
        dl_path = None

        # thumb if exists
        thumb_to_use = None
        t = thumbs.get(uid)
        if t and os.path.exists(t):
            thumb_to_use = t

        # upload
        up_start = time.time()
        caption = f"✅ Renamed: {new_name}"

        if out_type == "type_vid":
            await app.send_video(
                cq.message.chat.id,
                video=final_path,
                file_name=new_name,
                thumb=thumb_to_use,
                progress=progress_callback,
                progress_args=(pmsg, "Uploading", up_start, cancel_flag),
                caption=caption
            )
        else:
            await app.send_document(
                cq.message.chat.id,
                document=final_path,
                file_name=new_name,
                thumb=thumb_to_use,
                progress=progress_callback,
                progress_args=(pmsg, "Uploading", up_start, cancel_flag),
                caption=caption
            )

        await pmsg.edit_text(f"✅ Done, **{BRAND}**!\nVersion: `{BOT_VERSION}`", reply_markup=menu_kb())

    except Exception as e:
        await pmsg.edit_text(f"❌ Error: {e}", reply_markup=menu_kb())

    finally:
        state[uid]["cancel"] = None
        state[uid]["busy"] = False

        # cleanup /tmp files (storage safe)
        try:
            if dl_path and os.path.exists(dl_path):
                os.remove(dl_path)
        except:
            pass
        try:
            if final_path and os.path.exists(final_path):
                os.remove(final_path)
        except:
            pass

    await cq.answer("OK")

# ================== RUN ==================
if __name__ == "__main__":
    app.run()

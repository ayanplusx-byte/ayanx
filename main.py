import os, re, time, math, asyncio, json
from typing import Optional, Dict, Any

from PIL import Image
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait

# ================== VERSION ==================
BOT_VERSION = "v4.3.0-fast"

# ================== BRAND ==================
BRAND = "𝗟𝗘𝗚𝗘𝗡𝗗  OWNERX®"
BOT_TITLE = "LEGEND_OWNERX™"

# ================== ENV ==================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "").strip()

if not BOT_TOKEN or not API_ID or not API_HASH:
    raise RuntimeError("Missing env vars: BOT_TOKEN, API_ID, API_HASH")

# ✅ Admin only
ADMIN_IDS = {6014515919}

def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

# ================== PATHS ==================
BASE = os.path.dirname(os.path.abspath(__file__))

# Persistent (small): config + thumbs folder
VOLUME_ROOT = os.environ.get("BOT_VOLUME", os.path.join(BASE, "data_vol"))
DATA = os.path.join(VOLUME_ROOT, "data")
THUMBS = os.path.join(DATA, "thumbs")
os.makedirs(THUMBS, exist_ok=True)

CONFIG_JSON = os.path.join(DATA, "config.json")

# Temp big files (ephemeral): Railway disk safe
TMP_ROOT = os.environ.get("BOT_TMP", "/tmp")
DL = os.path.join(TMP_ROOT, "downloads")
os.makedirs(DL, exist_ok=True)

# ================== CONFIG (Maintenance) ==================
DEFAULT_CONFIG = {"maintenance": False}

def load_config() -> Dict[str, Any]:
    if not os.path.exists(CONFIG_JSON):
        return dict(DEFAULT_CONFIG)
    try:
        with open(CONFIG_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                return dict(DEFAULT_CONFIG)
            for k, v in DEFAULT_CONFIG.items():
                data.setdefault(k, v)
            return data
    except:
        return dict(DEFAULT_CONFIG)

def save_config(cfg: Dict[str, Any]):
    os.makedirs(DATA, exist_ok=True)
    tmp = CONFIG_JSON + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    os.replace(tmp, CONFIG_JSON)

config = load_config()

# ================== PYROGRAM ==================
app = Client(BOT_TITLE, api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ================== RUNTIME STATE (NO QUEUE) ==================
state: Dict[int, Dict[str, Any]] = {}
thumbs: Dict[int, str] = {}  # ❌ NOT restart-safe by design (as per your request)

def ensure(uid: int):
    if uid not in state:
        state[uid] = {
            "pending": None,
            "await_name": False,
            "await_type": False,
            "new_name": None,
            "busy": False,
            "cancel": None,
            "last_prog_edit": 0.0,   # (10) speed optimize
            "prog_min_interval": 1.2 # (10) speed optimize
        }

# ================== HELPERS ==================
def safe_filename(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r"[\\/:*?\"<>|]", "_", name)
    return name[:120] if len(name) > 120 else name

def _unique_tmp_name(uid: int, original_name: str) -> str:
    base = safe_filename(original_name or "file")
    stamp = int(time.time() * 1000)
    return f"{uid}_{stamp}_{base}"

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

# ================== UI (Premium Look) ==================
def type_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📁 𝗗𝗢𝗖𝗨𝗠𝗘𝗡𝗧", callback_data="type_doc"),
         InlineKeyboardButton("🎬 𝗩𝗜𝗗𝗘𝗢", callback_data="type_vid")],
        [InlineKeyboardButton("⬅ 𝗖𝗔𝗡𝗖𝗘𝗟", callback_data="cancel_global")]
    ])

def cancel_kb(uid: int):
    return InlineKeyboardMarkup([[InlineKeyboardButton("✖ 𝗖𝗔𝗡𝗖𝗘𝗟 ✖", callback_data=f"cancel_{uid}")]])

def menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🗑️ 𝗖𝗟𝗘𝗔𝗥 𝗧𝗛𝗨𝗠𝗕", callback_data="clear_thumb")],
        [InlineKeyboardButton("ℹ️ 𝗩𝗘𝗥𝗦𝗜𝗢𝗡", callback_data="show_version")]
    ])

def admin_panel_kb():
    status = "✅ ON" if config.get("maintenance") else "❌ OFF"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🛠️ 𝗠𝗔𝗜𝗡𝗧𝗘𝗡𝗔𝗡𝗖𝗘: {status}", callback_data="adm_maint")],
        [InlineKeyboardButton("ℹ️ 𝗩𝗘𝗥𝗦𝗜𝗢𝗡", callback_data="show_version")]
    ])

# ================== MAINTENANCE (7) ==================
async def maintenance_guard(obj) -> bool:
    uid = obj.from_user.id
    if config.get("maintenance") and not is_admin(uid):
        text = (
            "🛠️ **𝗠𝗔𝗜𝗡𝗧𝗘𝗡𝗔𝗡𝗖𝗘 𝗠𝗢𝗗𝗘**\n\n"
            "🚧 Bot is under upgrade / fix.\n"
            "⏳ Please try again later.\n\n"
            f"🔖 Version: `{BOT_VERSION}`"
        )
        try:
            if hasattr(obj, "message") and obj.message:
                await obj.message.reply_text(text)
            else:
                await obj.reply_text(text)
        except:
            pass
        return True
    return False

# ================== THUMB ==================
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

# ================== AUTO DELETE THUMB (9) ==================
THUMB_TTL_DAYS = 7
THUMB_CLEAN_EVERY_HOURS = 6

def clean_old_thumbs():
    cutoff = time.time() - THUMB_TTL_DAYS * 86400
    if not os.path.exists(THUMBS):
        return
    for fn in os.listdir(THUMBS):
        if not fn.lower().endswith(".jpg"):
            continue
        path = os.path.join(THUMBS, fn)
        try:
            if os.path.isfile(path) and os.path.getmtime(path) < cutoff:
                os.remove(path)
        except:
            pass

async def thumb_clean_loop():
    while True:
        try:
            await asyncio.to_thread(clean_old_thumbs)
        except:
            pass
        await asyncio.sleep(THUMB_CLEAN_EVERY_HOURS * 3600)

# ================== PROGRESS (10) Optimized ==================
async def progress_callback(current, total, msg, stage: str, start_ts: float, cancel_flag: dict, uid: int):
    if cancel_flag.get("stop"):
        raise Exception("Canceled")

    now = time.time()
    st = state.get(uid) or {}
    min_iv = float(st.get("prog_min_interval", 1.2))
    last = float(st.get("last_prog_edit", 0.0))

    # ✅ dynamic throttling: near end, update a bit more
    pct = (current * 100 / total) if total else 0
    dyn = 0.8 if pct >= 95 else min_iv

    if (now - last) < dyn:
        return

    st["last_prog_edit"] = now

    elapsed = max(now - start_ts, 0.001)
    speed = current / elapsed
    eta = (total - current) / speed if speed > 0 and total > 0 else 0

    text = (
        f"**{BRAND}**\n"
        f"🔖 Version: `{BOT_VERSION}`\n\n"
        f"📊 Progress: [{progress_bar(current, total)}] {pct:.1f}%\n"
        f"📦 {stage}: {human_bytes(int(current))} / {human_bytes(int(total))}\n"
        f"⚡ Speed: {human_bytes(int(speed))}/s\n"
        f"⏳ ETA: {format_time(eta)}\n"
        f"⏱ Time: {format_time(elapsed)}"
    )

    try:
        await msg.edit_text(text, reply_markup=cancel_flag["kb"])
    except FloodWait as e:
        await asyncio.sleep(getattr(e, "value", 1))
    except:
        pass

def file_item_from_message(message) -> Optional[Dict[str, Any]]:
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
    if await maintenance_guard(message):
        return

    await message.reply_text(
        "🚀 **𝗪𝗘𝗟𝗖𝗢𝗠𝗘 𝗧𝗢 𝗟𝗘𝗚𝗘𝗡𝗗𝗫 𝗥𝗘𝗡𝗔𝗠𝗘𝗥 𝗕𝗢𝗧**\n\n"
        "⚡ 𝗧𝗵𝗲 𝗙𝗮𝘀𝘁𝗲𝘀𝘁 𝗧𝗲𝗹𝗲𝗴𝗿𝗮𝗺 𝗙𝗶𝗹𝗲 𝗥𝗲𝗻𝗮𝗺𝗲𝗿\n\n"
        "📌 **𝗛𝗼𝘄 𝗧𝗼 𝗨𝘀𝗲**\n"
        "1️⃣ Send any file\n"
        "2️⃣ Enter new filename\n"
        "3️⃣ Select output type\n"
        "4️⃣ Get renamed file instantly\n\n"
        "🖼 Send a photo to set custom thumbnail\n"
        "⚡ Send only one file at a time for fastest processing\n\n"
        f"🔖 Version: `{BOT_VERSION}`",
        reply_markup=menu_kb()
    )

@app.on_message(filters.command("version"))
async def version_cmd(client, message):
    if await maintenance_guard(message):
        return
    await message.reply_text(f"✅ Bot Version: `{BOT_VERSION}`")

@app.on_message(filters.command("panel"))
async def panel_cmd(client, message):
    uid = message.from_user.id
    ensure(uid)
    if not is_admin(uid):
        await message.reply_text("❌ Admin only.")
        return
    await message.reply_text(
        "🧑‍💻 **𝗔𝗗𝗠𝗜𝗡 𝗣𝗔𝗡𝗘𝗟**\n\n"
        "🛠 Toggle maintenance mode\n"
        "✅ Only admin can use this panel\n\n"
        f"🔖 Version: `{BOT_VERSION}`",
        reply_markup=admin_panel_kb()
    )

@app.on_callback_query(filters.regex("^adm_maint$"))
async def adm_maint_cb(client, cq):
    uid = cq.from_user.id
    ensure(uid)
    if not is_admin(uid):
        await cq.answer("Not allowed", show_alert=True)
        return

    config["maintenance"] = not config.get("maintenance", False)
    save_config(config)

    status = "✅ ON" if config["maintenance"] else "❌ OFF"
    await cq.answer("Updated")
    await cq.message.edit_text(
        "🧑‍💻 **𝗔𝗗𝗠𝗜𝗡 𝗣𝗔𝗡𝗘𝗟**\n\n"
        f"🛠 Maintenance is now: **{status}**\n\n"
        f"🔖 Version: `{BOT_VERSION}`",
        reply_markup=admin_panel_kb()
    )

@app.on_callback_query(filters.regex("^show_version$"))
async def show_version_cb(client, cq):
    await cq.answer("Version")
    await cq.message.reply_text(f"✅ Bot Version: `{BOT_VERSION}`")

# ================== THUMB SAVE (RAM + file) ==================
@app.on_message(filters.photo)
async def photo_in(client, message):
    uid = message.from_user.id
    ensure(uid)
    if await maintenance_guard(message):
        return

    # NOTE: Not restart-safe by your request (1 removed)
    out = os.path.join(THUMBS, f"thumb_{uid}.jpg")

    src = await message.download(file_name=os.path.join(DL, f"thumb_src_{uid}.jpg"))
    await asyncio.to_thread(make_thumb_jpg, src, out)
    try:
        os.remove(src)
    except:
        pass

    thumbs[uid] = out
    # update mtime = "last used / last set"
    try:
        now = time.time()
        os.utime(out, (now, now))
    except:
        pass

    await message.reply_text("✅ **THUMBNAIL SAVED**")

@app.on_callback_query(filters.regex("^clear_thumb$"))
async def clear_thumb_cb(client, cq):
    uid = cq.from_user.id
    ensure(uid)
    if await maintenance_guard(cq):
        return

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
    if await maintenance_guard(message):
        return

    if state[uid]["busy"]:
        await message.reply_text("⏳ One file is processing. Finish hone do, phir next file bhejo.")
        return

    item = file_item_from_message(message)
    if not item:
        return

    state[uid]["pending"] = item
    state[uid]["await_name"] = True
    state[uid]["await_type"] = False
    state[uid]["new_name"] = None

    await message.reply_text(
        f"**{BRAND}**\n🔖 Version: `{BOT_VERSION}`\n\n"
        f"✍️ **Enter New Filename...**\nOld: `{item.get('file_name')}`"
    )

# ================== NAME INPUT ==================
@app.on_message(filters.text)
async def text_in(client, message):
    uid = message.from_user.id
    ensure(uid)
    if await maintenance_guard(message):
        return

    if not state[uid]["await_name"]:
        return

    new_name = safe_filename(message.text)
    state[uid]["new_name"] = new_name
    state[uid]["await_name"] = False
    state[uid]["await_type"] = True

    await message.reply_text(
        f"**{BRAND}**\n🔖 Version: `{BOT_VERSION}`\n\n"
        f"✅ Name set: `{new_name}`\n\n"
        "📌 **Select Output Type:**",
        reply_markup=type_kb()
    )

@app.on_callback_query(filters.regex("^cancel_global$"))
async def cancel_global_cb(client, cq):
    await cq.answer("Canceled")
    await cq.message.reply_text("❌ Canceled. Send file again.")

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
    if await maintenance_guard(cq):
        return

    if not state[uid]["await_type"]:
        await cq.answer("No pending")
        return
    if state[uid]["busy"]:
        await cq.answer("Busy", show_alert=True)
        return

    state[uid]["await_type"] = False
    out_type = cq.data

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
    pmsg = await cq.message.reply_text("🚀 Starting...", reply_markup=cancel_kb(uid))
    cancel_flag = {"stop": False, "last_edit": 0, "kb": cancel_kb(uid)}
    state[uid]["cancel"] = cancel_flag

    dl_path = None
    final_path = None

    try:
        # DOWNLOAD to /tmp
        dl_start = time.time()
        tmp_name = _unique_tmp_name(uid, old_name)
        dl_path = await app.download_media(
            item["file_id"],
            file_name=os.path.join(DL, tmp_name),
            progress=progress_callback,
            progress_args=(pmsg, "Downloading", dl_start, cancel_flag, uid)
        )

        # rename locally
        unique_final = _unique_tmp_name(uid, new_name)
        final_path = os.path.join(DL, unique_final)
        os.rename(dl_path, final_path)
        dl_path = None

        # thumb (RAM only by design)
        thumb_to_use = None
        t = thumbs.get(uid)
        if t and os.path.exists(t):
            thumb_to_use = t
            # refresh mtime => keeps thumb from deleting (9)
            try:
                now = time.time()
                os.utime(t, (now, now))
            except:
                pass

        # UPLOAD
        up_start = time.time()
        caption = f"✅ Renamed: {new_name}"

        if out_type == "type_vid":
            await app.send_video(
                cq.message.chat.id,
                video=final_path,
                file_name=new_name,
                thumb=thumb_to_use,
                progress=progress_callback,
                progress_args=(pmsg, "Uploading", up_start, cancel_flag, uid),
                caption=caption
            )
        else:
            await app.send_document(
                cq.message.chat.id,
                document=final_path,
                file_name=new_name,
                thumb=thumb_to_use,
                progress=progress_callback,
                progress_args=(pmsg, "Uploading", up_start, cancel_flag, uid),
                caption=caption
            )

        await pmsg.edit_text(
            f"✅ Done, **{BRAND}**!\n🔖 Version: `{BOT_VERSION}`",
            reply_markup=menu_kb()
        )

    except Exception as e:
        await pmsg.edit_text(f"❌ Error: {e}", reply_markup=menu_kb())

    finally:
        state[uid]["cancel"] = None
        state[uid]["busy"] = False

        # cleanup /tmp files
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
    # (9) auto delete thumbs after 7 days
    app.loop.create_task(thumb_clean_loop())
    app.run()

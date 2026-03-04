# main.py — v2 upgraded (FAST + CLEAN + STABLE) WITHOUT removing any feature
# v2 adds on top of previous upgrade:
# ✅ Busy lock (one task per user) => no state confusion on spam clicks/messages
# ✅ Cancel flow smoother (canceled => next file prompt)
# ✅ Better friendly error messages + some common telegram/network handling
# ✅ Still: thread-offload for ffmpeg/ffprobe/PIL + debounced JSON flush + FloodWait-safe progress + unique temp names

import os, re, time, math, json, asyncio, subprocess, datetime
from typing import Optional, Dict, Any, List

from PIL import Image
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait

# ================== BRAND ==================
BRAND = "𝗟𝗘𝗚𝗘𝗡𝗗  OWNERX®"
BOT_TITLE = "LEGEND_OWNERX™"

# ================== CONFIG ==================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "").strip()

if not BOT_TOKEN or not API_ID or not API_HASH:
    raise RuntimeError("Missing env vars: BOT_TOKEN, API_ID, API_HASH")

ADMIN_IDS = {6014515919}

# ================== PATHS ==================
BASE = os.path.dirname(os.path.abspath(__file__))

# Railway Volume mount point (recommended: /data). Set BOT_VOLUME=/data in Railway variables.
VOLUME_ROOT = os.environ.get("BOT_VOLUME", os.path.join(BASE, "data_vol"))

DATA = os.path.join(VOLUME_ROOT, "data")
DL = os.path.join(VOLUME_ROOT, "downloads")

os.makedirs(DATA, exist_ok=True)
os.makedirs(DL, exist_ok=True)

THUMBS = os.path.join(DATA, "thumbs")
os.makedirs(THUMBS, exist_ok=True)

USERS_JSON = os.path.join(DATA, "users.json")
CONFIG_JSON = os.path.join(DATA, "config.json")
QUEUE_JSON = os.path.join(DATA, "queue.json")  # crash recovery queue

# ================== DEFAULTS ==================
DEFAULT_CONFIG = {
    "maintenance": False,
    "last_daily_report": None,         # "YYYY-MM-DD"
    "auto_clean_hours": 6,             # remove temp older than this
    "daily_report_hour": 21            # 24h
}

DEFAULT_USER = {
    "thumb_path": None,                # custom thumb path
    "thumb_mode": "custom",            # custom | auto | off
    "count": 0,                        # total renames
    "bytes_in": 0,                     # downloaded bytes
    "bytes_out": 0,                    # uploaded bytes
    "fast_mode": True,                 # optimization toggles
    "meta_caption": True,              # show metadata in caption
    "last_active": None                # ISO timestamp
}

# ================== JSON HELPERS ==================
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

users: Dict[str, Dict[str, Any]] = load_json(USERS_JSON, {})
config: Dict[str, Any] = load_json(CONFIG_JSON, DEFAULT_CONFIG)
queue_store: Dict[str, List[Dict[str, Any]]] = load_json(QUEUE_JSON, {})  # {uid: [item, item...]}

# ================== PYROGRAM APP ==================
app = Client(BOT_TITLE, api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ================== RUNTIME STATE ==================
state: Dict[int, Dict[str, Any]] = {}

# ================== ASYNC SAFETY (DEBOUNCE + LOCKS) ==================
users_lock = asyncio.Lock()
config_lock = asyncio.Lock()
queue_lock = asyncio.Lock()

_dirty_users = False
_dirty_config = False
_dirty_queue = False

_last_users_flush = 0.0
_last_config_flush = 0.0
_last_queue_flush = 0.0

def mark_users_dirty():
    global _dirty_users
    _dirty_users = True

def mark_config_dirty():
    global _dirty_config
    _dirty_config = True

def mark_queue_dirty():
    global _dirty_queue
    _dirty_queue = True

async def _flush_users(force: bool = False, min_interval: int = 20):
    global _dirty_users, _last_users_flush
    if not _dirty_users and not force:
        return
    now = time.time()
    if not force and (now - _last_users_flush) < min_interval:
        return
    async with users_lock:
        await asyncio.to_thread(save_json, USERS_JSON, users)
        _dirty_users = False
        _last_users_flush = now

async def _flush_config(force: bool = False, min_interval: int = 30):
    global _dirty_config, _last_config_flush
    if not _dirty_config and not force:
        return
    now = time.time()
    if not force and (now - _last_config_flush) < min_interval:
        return
    async with config_lock:
        await asyncio.to_thread(save_json, CONFIG_JSON, config)
        _dirty_config = False
        _last_config_flush = now

async def _flush_queue(force: bool = False, min_interval: int = 10):
    global _dirty_queue, _last_queue_flush
    if not _dirty_queue and not force:
        return
    now = time.time()
    if not force and (now - _last_queue_flush) < min_interval:
        return
    async with queue_lock:
        await asyncio.to_thread(save_json, QUEUE_JSON, queue_store)
        _dirty_queue = False
        _last_queue_flush = now

async def flush_loop():
    while True:
        try:
            await _flush_users()
            await _flush_config()
            await _flush_queue()
        except:
            pass
        await asyncio.sleep(5)

# ================== BASIC HELPERS ==================
def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

def now_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def safe_filename(name: str) -> str:
    name = name.strip()
    name = re.sub(r"[\\/:*?\"<>|]", "_", name)
    return name[:120] if len(name) > 120 else name

def human_bytes(size: int) -> str:
    if size <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    i = int(math.floor(math.log(size, 1024)))
    p = math.pow(1024, i)
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

def _unique_tmp_name(uid: int, original_name: str) -> str:
    base = safe_filename(original_name or "file")
    stamp = int(time.time() * 1000)
    return f"{uid}_{stamp}_{base}"

def ensure_user(uid: int):
    uid_s = str(uid)

    if uid not in state:
        state[uid] = {
            "queue": [],
            "awaiting_name": False,
            "awaiting_type": False,
            "pending_item": None,
            "new_name": None,
            "awaiting_thumb": False,
            "cancel": None,
            "awaiting_broadcast": False,
            "busy": False,            # ✅ v2: busy lock
            "busy_msg_id": None
        }

    if uid_s not in users:
        users[uid_s] = dict(DEFAULT_USER)
        mark_users_dirty()

    # load persisted queue into runtime once
    if not state[uid]["queue"]:
        persisted = queue_store.get(uid_s, [])
        if isinstance(persisted, list) and persisted:
            state[uid]["queue"] = persisted.copy()

def persist_queue(uid: int):
    uid_s = str(uid)
    queue_store[uid_s] = state[uid]["queue"]
    mark_queue_dirty()

def touch_user(uid: int):
    users[str(uid)]["last_active"] = now_iso()
    mark_users_dirty()

def total_gb(bytes_count: int) -> float:
    return round(bytes_count / (1024**3), 3)

# ================== UI ==================
def menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🖼️ Set Thumbnail", callback_data="set_thumb"),
         InlineKeyboardButton("🗑️ Clear Thumbnail", callback_data="clear_thumb")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="settings"),
         InlineKeyboardButton("📥 Queue Status", callback_data="q_status")],
        [InlineKeyboardButton("🧹 Clear Queue", callback_data="q_clear")]
    ])

def settings_kb(uid: int):
    u = users[str(uid)]
    fast = "ON ✅" if u.get("fast_mode", True) else "OFF ❌"
    meta = "ON ✅" if u.get("meta_caption", True) else "OFF ❌"
    mode = u.get("thumb_mode", "custom")
    mode_txt = {"custom": "CUSTOM", "auto": "AUTO", "off": "OFF"}.get(mode, mode.upper())

    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"⚡ Fast Mode: {fast}", callback_data="toggle_fast"),
         InlineKeyboardButton(f"🧾 Meta Caption: {meta}", callback_data="toggle_meta")],
        [InlineKeyboardButton(f"🖼 Thumb Mode: {mode_txt}", callback_data="cycle_thumbmode")],
        [InlineKeyboardButton("⬅ Back", callback_data="back_menu")]
    ])

def type_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📁 Document", callback_data="type_doc"),
         InlineKeyboardButton("🎬 Video", callback_data="type_vid")]
    ])

def cancel_kb(uid: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✖ CANCEL ✖", callback_data=f"cancel_{uid}")]
    ])

# ================== MAINTENANCE GUARD ==================
async def maintenance_guard(message):
    if config.get("maintenance") and not is_admin(message.from_user.id):
        await message.reply_text("🛠️ Bot maintenance mode me hai. Baad me try karo.")
        return True
    return False

# ================== THUMB HELPERS (BLOCKING) ==================
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

def ffprobe_metadata(path: str) -> Dict[str, Any]:
    meta = {"duration": None, "width": None, "height": None}
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-hide_banner",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height:format=duration",
            "-of", "json",
            path
        ]
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
        j = json.loads(out)
        if "format" in j and "duration" in j["format"]:
            try:
                meta["duration"] = float(j["format"]["duration"])
            except:
                pass
        streams = j.get("streams") or []
        if streams:
            meta["width"] = streams[0].get("width")
            meta["height"] = streams[0].get("height")
    except:
        pass
    return meta

def make_auto_thumb_from_video(video_path: str, out_jpg: str) -> bool:
    try:
        meta = ffprobe_metadata(video_path)
        dur = meta.get("duration") or 0.0

        if dur and dur > 0:
            ss = max(0.2, min(2.0, dur * 0.2))
        else:
            ss = 1.0

        cmd = [
            "ffmpeg", "-y",
            "-hide_banner", "-loglevel", "error",
            "-ss", str(ss),
            "-i", video_path,
            "-an", "-sn",
            "-frames:v", "1",
            "-vf", "scale=320:-1",
            out_jpg
        ]
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)

        if os.path.exists(out_jpg):
            if os.path.getsize(out_jpg) > 200 * 1024:
                tmp = out_jpg + ".tmp.jpg"
                make_thumb_jpg(out_jpg, tmp)
                os.replace(tmp, out_jpg)
            return True
    except:
        return False
    return False

async def run_blocking(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)

# ================== PROGRESS CALLBACK ==================
async def progress_callback(current, total, msg, stage: str, start_ts: float, cancel_flag: dict):
    if cancel_flag.get("stop"):
        raise Exception("Canceled by user")

    now = time.time()
    elapsed = max(now - start_ts, 0.001)
    speed = current / elapsed
    eta = (total - current) / speed if speed > 0 and total > 0 else 0
    pct = (current * 100 / total) if total else 0

    text = (
        f"**{BRAND}**\n\n"
        f"Progress: [{progress_bar(current, total)}] {pct:.1f}%\n"
        f"📥 {stage}: {human_bytes(int(current))} | {human_bytes(int(total))}\n"
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

# ================== QUEUE ITEM BUILDER ==================
def queue_item_from_message(message) -> Optional[Dict[str, Any]]:
    if message.document:
        return {
            "kind": "document",
            "file_id": message.document.file_id,
            "file_name": message.document.file_name or "file"
        }
    if message.video:
        return {
            "kind": "video",
            "file_id": message.video.file_id,
            "file_name": message.video.file_name or "video.mp4"
        }
    if message.audio:
        return {
            "kind": "audio",
            "file_id": message.audio.file_id,
            "file_name": message.audio.file_name or "audio.mp3"
        }
    return None

# ================== FRIENDLY ERROR ==================
def friendly_error(e: Exception) -> str:
    msg = str(e) or "Unknown error"
    low = msg.lower()
    if "canceled by user" in low or "canceled" in low:
        return "❌ Canceled ✅"
    if "flood" in low:
        return "⏳ Telegram FloodWait. Thoda wait karke try karo."
    if "file is too big" in low or "entity too large" in low:
        return "📦 File bahut bada hai. Telegram limit ya upload restriction ho sakti hai."
    if "network" in low or "timeout" in low:
        return "🌐 Network/Timeout issue. Thoda baad me try karo."
    return f"❌ Error: {msg}"

# ================== START ==================
@app.on_message(filters.command("start"))
async def start_cmd(client, message):
    ensure_user(message.from_user.id)
    if await maintenance_guard(message):
        return
    touch_user(message.from_user.id)

    await message.reply_text(
        f"✅ Welcome, **{BRAND}**!\n\n"
        "• Send thumbnail photo (or press Set Thumbnail)\n"
        "• Send files (multiple) then rename one-by-one\n\n"
        "⚙️ Settings: Fast Mode, Meta Caption, Thumb Mode",
        reply_markup=menu_kb()
    )

# ================== SETTINGS CALLBACKS ==================
@app.on_callback_query(filters.regex("^settings$"))
async def settings_cb(client, cq):
    uid = cq.from_user.id
    ensure_user(uid)
    await cq.answer("Settings")
    await cq.message.edit_text(f"⚙️ **{BRAND}** Settings", reply_markup=settings_kb(uid))

@app.on_callback_query(filters.regex("^back_menu$"))
async def back_menu_cb(client, cq):
    await cq.answer("Menu")
    await cq.message.edit_text(f"✅ **{BRAND}** Menu", reply_markup=menu_kb())

@app.on_callback_query(filters.regex("^toggle_fast$"))
async def toggle_fast_cb(client, cq):
    uid = cq.from_user.id
    ensure_user(uid)
    users[str(uid)]["fast_mode"] = not users[str(uid)].get("fast_mode", True)
    mark_users_dirty()
    await cq.answer("Updated")
    await cq.message.edit_reply_markup(reply_markup=settings_kb(uid))

@app.on_callback_query(filters.regex("^toggle_meta$"))
async def toggle_meta_cb(client, cq):
    uid = cq.from_user.id
    ensure_user(uid)
    users[str(uid)]["meta_caption"] = not users[str(uid)].get("meta_caption", True)
    mark_users_dirty()
    await cq.answer("Updated")
    await cq.message.edit_reply_markup(reply_markup=settings_kb(uid))

@app.on_callback_query(filters.regex("^cycle_thumbmode$"))
async def cycle_thumbmode_cb(client, cq):
    uid = cq.from_user.id
    ensure_user(uid)
    mode = users[str(uid)].get("thumb_mode", "custom")
    mode = "auto" if mode == "custom" else ("off" if mode == "auto" else "custom")
    users[str(uid)]["thumb_mode"] = mode
    mark_users_dirty()
    await cq.answer(f"Thumb: {mode}")
    await cq.message.edit_reply_markup(reply_markup=settings_kb(uid))

# ================== THUMB MENU ==================
@app.on_callback_query(filters.regex("^set_thumb$"))
async def set_thumb(client, cq):
    uid = cq.from_user.id
    ensure_user(uid)
    state[uid]["awaiting_thumb"] = True
    await cq.answer("Send photo now")
    await cq.message.reply_text("🖼️ Send your thumbnail image now.")

@app.on_callback_query(filters.regex("^clear_thumb$"))
async def clear_thumb(client, cq):
    uid = cq.from_user.id
    ensure_user(uid)
    uid_s = str(uid)
    t = users[uid_s].get("thumb_path")
    users[uid_s]["thumb_path"] = None
    mark_users_dirty()
    state[uid]["awaiting_thumb"] = False
    if t and os.path.exists(t):
        try:
            os.remove(t)
        except:
            pass
    await cq.answer("Cleared")
    await cq.message.reply_text("✅ Thumbnail cleared.")

# ================== QUEUE UI ==================
@app.on_callback_query(filters.regex("^q_status$"))
async def q_status(client, cq):
    uid = cq.from_user.id
    ensure_user(uid)
    await cq.answer("Queue")
    await cq.message.reply_text(f"📥 Queue files: {len(state[uid]['queue'])}")

@app.on_callback_query(filters.regex("^q_clear$"))
async def q_clear(client, cq):
    uid = cq.from_user.id
    ensure_user(uid)

    # If currently processing, stop it
    if state[uid].get("cancel"):
        state[uid]["cancel"]["stop"] = True

    state[uid]["queue"].clear()
    state[uid]["awaiting_name"] = False
    state[uid]["awaiting_type"] = False
    state[uid]["pending_item"] = None
    state[uid]["new_name"] = None
    persist_queue(uid)

    await cq.answer("Cleared")
    await cq.message.reply_text("🧹 Queue cleared.")

@app.on_callback_query(filters.regex(r"^cancel_\d+$"))
async def cancel_any(client, cq):
    uid = cq.from_user.id
    ensure_user(uid)
    if state[uid].get("cancel"):
        state[uid]["cancel"]["stop"] = True
    await cq.answer("Canceled ✅")

# ================== ADMIN PANEL ==================
@app.on_message(filters.command("panel"))
async def panel(client, message):
    uid = message.from_user.id
    ensure_user(uid)
    if not is_admin(uid):
        await message.reply_text("❌ Admin only.")
        return

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Stats", callback_data="admin_stats"),
         InlineKeyboardButton("🏆 Top Users", callback_data="admin_top")],
        [InlineKeyboardButton("📅 Daily Report Now", callback_data="admin_report")],
        [InlineKeyboardButton("🧰 Maintenance Toggle", callback_data="admin_maint")],
        [InlineKeyboardButton("📢 Broadcast", callback_data="admin_bc")]
    ])
    await message.reply_text("🧑‍💻 Admin Panel", reply_markup=kb)

@app.on_callback_query(filters.regex("^admin_stats$"))
async def admin_stats(client, cq):
    if not is_admin(cq.from_user.id):
        await cq.answer("Not allowed", show_alert=True)
        return
    total_users = len(users)
    total_renames = sum(int(v.get("count", 0)) for v in users.values())
    total_in = sum(int(v.get("bytes_in", 0)) for v in users.values())
    total_out = sum(int(v.get("bytes_out", 0)) for v in users.values())
    await cq.answer("Stats")
    await cq.message.reply_text(
        "📊 Stats\n"
        f"Users: {total_users}\n"
        f"Total Renames: {total_renames}\n"
        f"Downloaded: {total_gb(total_in)} GB\n"
        f"Uploaded: {total_gb(total_out)} GB"
    )

@app.on_callback_query(filters.regex("^admin_top$"))
async def admin_top(client, cq):
    if not is_admin(cq.from_user.id):
        await cq.answer("Not allowed", show_alert=True)
        return
    ranked = sorted(users.items(), key=lambda kv: int(kv[1].get("count", 0)), reverse=True)[:10]
    lines = []
    for i, (uid_s, u) in enumerate(ranked, 1):
        lines.append(f"{i}. `{uid_s}` → {u.get('count',0)} renames | {total_gb(int(u.get('bytes_out',0)))} GB up")
    await cq.answer("Top")
    await cq.message.reply_text("🏆 Top Users\n\n" + ("\n".join(lines) if lines else "No data"))

@app.on_callback_query(filters.regex("^admin_maint$"))
async def admin_maint(client, cq):
    if not is_admin(cq.from_user.id):
        await cq.answer("Not allowed", show_alert=True)
        return
    config["maintenance"] = not config.get("maintenance", False)
    mark_config_dirty()
    await cq.answer("Done")
    await cq.message.reply_text(f"🧰 Maintenance: {config['maintenance']}")

@app.on_callback_query(filters.regex("^admin_bc$"))
async def admin_bc(client, cq):
    uid = cq.from_user.id
    if not is_admin(uid):
        await cq.answer("Not allowed", show_alert=True)
        return
    state[uid]["awaiting_broadcast"] = True
    await cq.answer("Send text")
    await cq.message.reply_text("📢 Broadcast text bhejo (sirf text).")

@app.on_callback_query(filters.regex("^admin_report$"))
async def admin_report_now(client, cq):
    if not is_admin(cq.from_user.id):
        await cq.answer("Not allowed", show_alert=True)
        return
    await cq.answer("Reporting")
    await send_daily_report()

# ================== BROADCAST INPUT ==================
@app.on_message(filters.text)
async def text_in(client, message):
    uid = message.from_user.id
    ensure_user(uid)
    if await maintenance_guard(message):
        return
    touch_user(uid)

    if is_admin(uid) and state[uid].get("awaiting_broadcast"):
        state[uid]["awaiting_broadcast"] = False
        txt = message.text.strip()
        sent = 0
        fail = 0
        for u in list(users.keys()):
            try:
                await app.send_message(int(u), f"📢 {txt}")
                sent += 1
            except:
                fail += 1
        await message.reply_text(f"✅ Broadcast done\nSent: {sent}\nFailed: {fail}")
        return

    # rename name input
    if not state[uid]["awaiting_name"]:
        return

    new_name = safe_filename(message.text)
    state[uid]["new_name"] = new_name
    state[uid]["awaiting_name"] = False
    state[uid]["awaiting_type"] = True

    await message.reply_text(
        f"**{BRAND}**\n\nSelect The Output File Type\n\nFile Name :- `{new_name}`",
        reply_markup=type_kb()
    )

# ================== THUMB SAVE ==================
@app.on_message(filters.photo)
async def photo_in(client, message):
    uid = message.from_user.id
    ensure_user(uid)
    if await maintenance_guard(message):
        return
    touch_user(uid)

    src = await message.download(file_name=os.path.join(DL, f"thumb_src_{uid}.jpg"))
    out = os.path.join(THUMBS, f"thumb_{uid}.jpg")

    await run_blocking(make_thumb_jpg, src, out)

    try:
        os.remove(src)
    except:
        pass

    users[str(uid)]["thumb_path"] = out
    users[str(uid)]["thumb_mode"] = "custom"
    mark_users_dirty()
    state[uid]["awaiting_thumb"] = False
    await message.reply_text("✅ THUMBNAIL SAVED (CUSTOM)")

# ================== FILE INPUT (QUEUE) ==================
@app.on_message(filters.document | filters.video | filters.audio)
async def file_in(client, message):
    uid = message.from_user.id
    ensure_user(uid)
    if await maintenance_guard(message):
        return
    touch_user(uid)

    item = queue_item_from_message(message)
    if not item:
        return

    state[uid]["queue"].append(item)
    persist_queue(uid)

    # ✅ v2: if already processing, just add to queue and inform
    if state[uid].get("busy") or state[uid]["awaiting_name"] or state[uid]["awaiting_type"] or state[uid]["pending_item"]:
        await message.reply_text(f"📥 Added to queue. Queue: {len(state[uid]['queue'])}")
        return

    await ask_new_name(message.chat.id, uid)

async def ask_new_name(chat_id: int, uid: int):
    ensure_user(uid)

    # if user is busy, don't prompt new name
    if state[uid].get("busy"):
        return

    if not state[uid]["queue"]:
        await app.send_message(chat_id, "✅ Queue done.", reply_markup=menu_kb())
        return

    item = state[uid]["queue"].pop(0)
    persist_queue(uid)

    state[uid]["pending_item"] = item
    state[uid]["awaiting_name"] = True

    old_name = item.get("file_name") or "file"
    await app.send_message(
        chat_id,
        f"**{BRAND}**\n\n"
        f"Please Enter New Filename...\n\nOld File Name :- `{old_name}`",
    )

# ================== TYPE SELECT + RENAME ==================
@app.on_callback_query(filters.regex("^type_(doc|vid)$"))
async def type_select(client, cq):
    uid = cq.from_user.id
    ensure_user(uid)

    if not state[uid]["awaiting_type"]:
        await cq.answer("No pending file")
        return

    # ✅ v2: busy lock ON
    if state[uid].get("busy"):
        await cq.answer("Already processing", show_alert=True)
        return

    state[uid]["awaiting_type"] = False
    out_type = cq.data  # type_doc / type_vid

    item = state[uid]["pending_item"]
    new_name = state[uid]["new_name"] or "file"
    state[uid]["pending_item"] = None
    state[uid]["new_name"] = None

    if not item:
        await cq.answer("No item")
        await cq.message.reply_text("❌ No pending item. Send file again.", reply_markup=menu_kb())
        return

    old_name = item.get("file_name") or "file"
    old_ext = os.path.splitext(old_name)[1]
    new_ext = os.path.splitext(new_name)[1]
    if not new_ext and old_ext:
        new_name += old_ext

    # busy ON
    state[uid]["busy"] = True

    pmsg = await cq.message.reply_text(f"**{BRAND}**\n\nStarting...", reply_markup=cancel_kb(uid))
    cancel_flag = {"stop": False, "last_edit": 0, "kb": cancel_kb(uid)}
    state[uid]["cancel"] = cancel_flag

    final_path = None
    dl_path = None
    canceled = False

    try:
        # ---------- DOWNLOAD ----------
        dl_start = time.time()
        file_id = item["file_id"]

        tmp_name = _unique_tmp_name(uid, old_name)
        dl_path = await app.download_media(
            file_id,
            file_name=os.path.join(DL, tmp_name),
            progress=progress_callback,
            progress_args=(pmsg, "Downloading", dl_start, cancel_flag)
        )

        # stats bytes_in
        try:
            users[str(uid)]["bytes_in"] = int(users[str(uid)].get("bytes_in", 0)) + int(os.path.getsize(dl_path))
            mark_users_dirty()
        except:
            pass

        # rename locally unique path, but visible name remains new_name
        unique_final = _unique_tmp_name(uid, new_name)
        final_path = os.path.join(DL, unique_final)
        os.rename(dl_path, final_path)
        dl_path = None

        # ---------- THUMB SELECT ----------
        thumb_to_use = None
        mode = users[str(uid)].get("thumb_mode", "custom")

        if mode == "off":
            thumb_to_use = None
        elif mode == "custom":
            t = users[str(uid)].get("thumb_path")
            if t and os.path.exists(t):
                thumb_to_use = t
        elif mode == "auto":
            if item.get("kind") == "video" or out_type == "type_vid":
                auto_out = os.path.join(THUMBS, f"auto_{uid}.jpg")
                ok = await run_blocking(make_auto_thumb_from_video, final_path, auto_out)
                if ok:
                    thumb_to_use = auto_out

        # ---------- METADATA + CAPTION ----------
        caption = f"✅ Renamed: {new_name}"
        if users[str(uid)].get("meta_caption", True):
            size = os.path.getsize(final_path) if os.path.exists(final_path) else 0
            if out_type == "type_vid":
                meta = await run_blocking(ffprobe_metadata, final_path)
            else:
                meta = {"duration": None, "width": None, "height": None}

            parts = [f"📦 Size: {human_bytes(size)}"]
            if meta.get("duration"):
                parts.append(f"⏱ Duration: {format_time(meta['duration'])}")
            if meta.get("width") and meta.get("height"):
                parts.append(f"🖥 Resolution: {meta['width']}x{meta['height']}")
            caption = caption + "\n\n" + "\n".join(parts)

        # ---------- UPLOAD ----------
        up_start = time.time()
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

        # stats bytes_out + count
        try:
            users[str(uid)]["bytes_out"] = int(users[str(uid)].get("bytes_out", 0)) + int(os.path.getsize(final_path))
        except:
            pass
        users[str(uid)]["count"] = int(users[str(uid)].get("count", 0)) + 1
        mark_users_dirty()

        await pmsg.edit_text(f"✅ Done, **{BRAND}**!", reply_markup=menu_kb())

    except Exception as e:
        text = friendly_error(e)
        if "Canceled" in text:
            canceled = True
        await pmsg.edit_text(text, reply_markup=menu_kb())

    finally:
        state[uid]["cancel"] = None
        state[uid]["busy"] = False  # ✅ v2: busy OFF always

        # cleanup temp files
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

        # ✅ v2: after cancel or finish, continue to next file automatically
        await ask_new_name(cq.message.chat.id, uid)

    await cq.answer("OK")

# ================== AUTO CLEAN ==================
def clean_old_temp_files(hours: int = 6):
    cutoff = time.time() - hours * 3600
    for root in [DL, THUMBS]:
        if not os.path.exists(root):
            continue
        for name in os.listdir(root):
            path = os.path.join(root, name)
            try:
                if os.path.isfile(path) and os.path.getmtime(path) < cutoff:
                    os.remove(path)
            except:
                pass

async def auto_clean_loop():
    while True:
        try:
            clean_old_temp_files(int(config.get("auto_clean_hours", 6)))
        except:
            pass
        await asyncio.sleep(60 * 30)

# ================== DAILY REPORT ==================
async def send_daily_report():
    today = (datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)).date().isoformat()
    total_users = len(users)
    total_renames = sum(int(v.get("count", 0)) for v in users.values())
    total_in = sum(int(v.get("bytes_in", 0)) for v in users.values())
    total_out = sum(int(v.get("bytes_out", 0)) for v in users.values())

    ranked = sorted(users.items(), key=lambda kv: int(kv[1].get("count", 0)), reverse=True)[:5]
    top_lines = []
    for i, (uid_s, u) in enumerate(ranked, 1):
        top_lines.append(f"{i}. `{uid_s}` → {u.get('count',0)} renames | {total_gb(int(u.get('bytes_out',0)))} GB up")

    text = (
        f"📅 **Daily Report — {today}**\n\n"
        f"👥 Users: {total_users}\n"
        f"📝 Total Renames: {total_renames}\n"
        f"⬇ Downloaded: {total_gb(total_in)} GB\n"
        f"⬆ Uploaded: {total_gb(total_out)} GB\n\n"
        f"🏆 Top Users:\n" + ("\n".join(top_lines) if top_lines else "No data")
    )

    for admin_id in ADMIN_IDS:
        try:
            await app.send_message(admin_id, text)
        except:
            pass

    config["last_daily_report"] = today
    mark_config_dirty()

async def daily_report_loop():
    while True:
        try:
            now = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)  # IST
            today = now.date().isoformat()
            hour = int(config.get("daily_report_hour", 21))
            if now.hour >= hour and config.get("last_daily_report") != today:
                await send_daily_report()
        except:
            pass
        await asyncio.sleep(60 * 10)

# ================== ME ==================
@app.on_message(filters.command("me"))
async def me_cmd(client, message):
    uid = message.from_user.id
    ensure_user(uid)
    u = users[str(uid)]
    await message.reply_text(
        f"**{BRAND}**\n\n"
        f"Renames: {u.get('count',0)}\n"
        f"Downloaded: {total_gb(int(u.get('bytes_in',0)))} GB\n"
        f"Uploaded: {total_gb(int(u.get('bytes_out',0)))} GB\n"
        f"Thumb Mode: {u.get('thumb_mode','custom')}\n"
        f"Fast Mode: {u.get('fast_mode',True)}\n"
        f"Meta Caption: {u.get('meta_caption',True)}"
    )

# ================== RUN ==================
if __name__ == "__main__":
    app.loop.create_task(flush_loop())        # debounced file writes
    app.loop.create_task(auto_clean_loop())
    app.loop.create_task(daily_report_loop())
    app.run()

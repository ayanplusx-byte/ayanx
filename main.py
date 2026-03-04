import os, re, time, math, json, asyncio, subprocess, datetime
from typing import Optional, Dict, Any, List

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

# Persistent volume (for json only)
VOLUME_ROOT = os.environ.get("BOT_VOLUME", os.path.join(BASE, "data_vol"))
DATA = os.path.join(VOLUME_ROOT, "data")
os.makedirs(DATA, exist_ok=True)

# Ephemeral temp (for big files) - prevents volume filling
TMP_ROOT = os.environ.get("BOT_TMP", "/tmp")
DL = os.path.join(TMP_ROOT, "downloads")
os.makedirs(DL, exist_ok=True)

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

# Custom thumb + meta caption removed. Fast mode default ON.
DEFAULT_USER = {
    "count": 0,
    "bytes_in": 0,
    "bytes_out": 0,
    "fast_mode": True,                 # default ON (kept for compatibility)
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

users: Dict[str, Dict[str, Any]] = load_json(USERS_JSON, {})
config: Dict[str, Any] = load_json(CONFIG_JSON, DEFAULT_CONFIG)
queue_store: Dict[str, List[Dict[str, Any]]] = load_json(QUEUE_JSON, {})

# ================== PYROGRAM APP ==================
app = Client(BOT_TITLE, api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ================== RUNTIME STATE ==================
state: Dict[int, Dict[str, Any]] = {}

# ================== SAFE FLUSH (DEBOUNCE + LOCKS) ==================
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

# ================== HELPERS ==================
def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

def now_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def safe_filename(name: str) -> str:
    name = (name or "").strip()
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

def total_gb(bytes_count: int) -> float:
    return round(bytes_count / (1024**3), 3)

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
            "cancel": None,
            "awaiting_broadcast": False,
            "busy": False
        }

    if uid_s not in users:
        users[uid_s] = dict(DEFAULT_USER)
        mark_users_dirty()

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

# ================== UI ==================
def menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📥 Queue Status", callback_data="q_status"),
         InlineKeyboardButton("🧹 Clear Queue", callback_data="q_clear")],
    ])

def type_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📁 Document", callback_data="type_doc"),
         InlineKeyboardButton("🎬 Video", callback_data="type_vid")]
    ])

def cancel_kb(uid: int):
    return InlineKeyboardMarkup([[InlineKeyboardButton("✖ CANCEL ✖", callback_data=f"cancel_{uid}")]])

# ================== MAINTENANCE ==================
async def maintenance_guard(message):
    if config.get("maintenance") and not is_admin(message.from_user.id):
        await message.reply_text("🛠️ Bot maintenance mode me hai. Baad me try karo.")
        return True
    return False

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
        return {"kind": "document", "file_id": message.document.file_id, "file_name": message.document.file_name or "file"}
    if message.video:
        return {"kind": "video", "file_id": message.video.file_id, "file_name": message.video.file_name or "video.mp4"}
    if message.audio:
        return {"kind": "audio", "file_id": message.audio.file_id, "file_name": message.audio.file_name or "audio.mp3"}
    return None

# ================== FRIENDLY ERROR ==================
def friendly_error(e: Exception) -> str:
    msg = str(e) or "Unknown error"
    low = msg.lower()
    if "canceled" in low:
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
        "• Send files (multiple) then rename one-by-one\n"
        "• Fast mode is ON by default\n\n"
        "Buttons: Queue Status / Clear Queue",
        reply_markup=menu_kb()
    )

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

# ================== TEXT INPUT (BROADCAST + RENAME NAME) ==================
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

    # if already processing or awaiting something, just queue it
    if state[uid].get("busy") or state[uid]["awaiting_name"] or state[uid]["awaiting_type"] or state[uid]["pending_item"]:
        await message.reply_text(f"📥 Added to queue. Queue: {len(state[uid]['queue'])}")
        return

    await ask_new_name(message.chat.id, uid)

async def ask_new_name(chat_id: int, uid: int):
    ensure_user(uid)

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

    state[uid]["busy"] = True

    pmsg = await cq.message.reply_text(f"**{BRAND}**\n\nStarting...", reply_markup=cancel_kb(uid))
    cancel_flag = {"stop": False, "last_edit": 0, "kb": cancel_kb(uid)}
    state[uid]["cancel"] = cancel_flag

    final_path = None
    dl_path = None

    try:
        # DOWNLOAD (to /tmp)
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

        # rename locally (still /tmp). Keep visible name as new_name.
        unique_final = _unique_tmp_name(uid, new_name)
        final_path = os.path.join(DL, unique_final)
        os.rename(dl_path, final_path)
        dl_path = None

        # UPLOAD (no caption metadata, no thumbnail)
        up_start = time.time()
        caption = f"✅ Renamed: {new_name}"

        if out_type == "type_vid":
            await app.send_video(
                cq.message.chat.id,
                video=final_path,
                file_name=new_name,
                progress=progress_callback,
                progress_args=(pmsg, "Uploading", up_start, cancel_flag),
                caption=caption
            )
        else:
            await app.send_document(
                cq.message.chat.id,
                document=final_path,
                file_name=new_name,
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
        await pmsg.edit_text(friendly_error(e), reply_markup=menu_kb())

    finally:
        state[uid]["cancel"] = None
        state[uid]["busy"] = False

        # cleanup: remove temp files so storage won't fill
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

        await ask_new_name(cq.message.chat.id, uid)

    await cq.answer("OK")

# ================== AUTO CLEAN (/tmp safety) ==================
def clean_old_temp_files(hours: int = 6):
    cutoff = time.time() - hours * 3600
    if not os.path.exists(DL):
        return
    for name in os.listdir(DL):
        path = os.path.join(DL, name)
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
        f"Fast Mode: {u.get('fast_mode', True)}"
    )

# ================== RUN ==================
if __name__ == "__main__":
    app.loop.create_task(flush_loop())
    app.loop.create_task(auto_clean_loop())
    app.loop.create_task(daily_report_loop())
    app.run()

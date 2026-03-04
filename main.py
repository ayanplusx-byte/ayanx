import os, re, time, math, json, asyncio, datetime, secrets, string
from typing import Optional, Dict, Any, List

from PIL import Image
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait

# ================== VERSION ==================
BOT_VERSION = "v3.2.0"

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

# ================== TIMEZONE IST ==================
IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

def now_ist() -> datetime.datetime:
    return datetime.datetime.now(tz=IST)

def today_ist_str() -> str:
    return now_ist().date().isoformat()

def dt_to_iso(dt: datetime.datetime) -> str:
    return dt.isoformat(timespec="seconds")

def iso_to_dt(s: str) -> Optional[datetime.datetime]:
    try:
        return datetime.datetime.fromisoformat(s)
    except:
        return None

# ================== FREE LIMITS (Option 3) ==================
FREE_DAILY_FILES = 20
FREE_DAILY_GB = 2.0
FREE_DAILY_BYTES = int(FREE_DAILY_GB * (1024**3))

# ================== PATHS ==================
BASE = os.path.dirname(os.path.abspath(__file__))

# Persistent volume (json + thumbs)
VOLUME_ROOT = os.environ.get("BOT_VOLUME", os.path.join(BASE, "data_vol"))
DATA = os.path.join(VOLUME_ROOT, "data")
THUMBS = os.path.join(DATA, "thumbs")
os.makedirs(DATA, exist_ok=True)
os.makedirs(THUMBS, exist_ok=True)

# Ephemeral temp for big files
TMP_ROOT = os.environ.get("BOT_TMP", "/tmp")
DL = os.path.join(TMP_ROOT, "downloads")
os.makedirs(DL, exist_ok=True)

USERS_JSON = os.path.join(DATA, "users.json")
CONFIG_JSON = os.path.join(DATA, "config.json")
QUEUE_JSON = os.path.join(DATA, "queue.json")
COUPONS_JSON = os.path.join(DATA, "coupons.json")  # coupons

# ================== DEFAULTS ==================
DEFAULT_CONFIG = {
    "maintenance": False,
    "auto_clean_hours": 6
}

DEFAULT_USER = {
    "thumb_path": None,
    "count": 0,
    "bytes_in": 0,
    "bytes_out": 0,
    "last_active": None,

    # premium
    "premium_until": None,      # ISO IST
    "day": None,                # YYYY-MM-DD IST
    "day_files": 0,
    "day_bytes_out": 0
}

# ================== JSON ==================
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
coupons: Dict[str, Dict[str, Any]] = load_json(COUPONS_JSON, {})  # {CODE: {...}}

# ================== PYROGRAM ==================
app = Client(BOT_TITLE, api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ================== RUNTIME STATE ==================
state: Dict[int, Dict[str, Any]] = {}

# ================== DEBOUNCED FLUSH ==================
users_lock = asyncio.Lock()
config_lock = asyncio.Lock()
queue_lock = asyncio.Lock()
coupons_lock = asyncio.Lock()

_dirty_users = False
_dirty_config = False
_dirty_queue = False
_dirty_coupons = False

_last_users_flush = 0.0
_last_config_flush = 0.0
_last_queue_flush = 0.0
_last_coupons_flush = 0.0

def mark_users_dirty():
    global _dirty_users
    _dirty_users = True

def mark_config_dirty():
    global _dirty_config
    _dirty_config = True

def mark_queue_dirty():
    global _dirty_queue
    _dirty_queue = True

def mark_coupons_dirty():
    global _dirty_coupons
    _dirty_coupons = True

async def _flush_users(min_interval: int = 15):
    global _dirty_users, _last_users_flush
    if not _dirty_users:
        return
    now = time.time()
    if (now - _last_users_flush) < min_interval:
        return
    async with users_lock:
        await asyncio.to_thread(save_json, USERS_JSON, users)
        _dirty_users = False
        _last_users_flush = now

async def _flush_config(min_interval: int = 25):
    global _dirty_config, _last_config_flush
    if not _dirty_config:
        return
    now = time.time()
    if (now - _last_config_flush) < min_interval:
        return
    async with config_lock:
        await asyncio.to_thread(save_json, CONFIG_JSON, config)
        _dirty_config = False
        _last_config_flush = now

async def _flush_queue(min_interval: int = 10):
    global _dirty_queue, _last_queue_flush
    if not _dirty_queue:
        return
    now = time.time()
    if (now - _last_queue_flush) < min_interval:
        return
    async with queue_lock:
        await asyncio.to_thread(save_json, QUEUE_JSON, queue_store)
        _dirty_queue = False
        _last_queue_flush = now

async def _flush_coupons(min_interval: int = 10):
    global _dirty_coupons, _last_coupons_flush
    if not _dirty_coupons:
        return
    now = time.time()
    if (now - _last_coupons_flush) < min_interval:
        return
    async with coupons_lock:
        await asyncio.to_thread(save_json, COUPONS_JSON, coupons)
        _dirty_coupons = False
        _last_coupons_flush = now

async def flush_loop():
    while True:
        try:
            await _flush_users()
            await _flush_config()
            await _flush_queue()
            await _flush_coupons()
        except:
            pass
        await asyncio.sleep(5)

# ================== HELPERS ==================
def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

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
            "busy": False,

            # admin UI flow
            "awaiting_coupon_params": False
        }
    if uid_s not in users:
        users[uid_s] = dict(DEFAULT_USER)
        mark_users_dirty()
    else:
        for k, v in DEFAULT_USER.items():
            if k not in users[uid_s]:
                users[uid_s][k] = v
                mark_users_dirty()

    if not state[uid]["queue"]:
        persisted = queue_store.get(uid_s, [])
        if isinstance(persisted, list) and persisted:
            state[uid]["queue"] = persisted.copy()

def persist_queue(uid: int):
    queue_store[str(uid)] = state[uid]["queue"]
    mark_queue_dirty()

def touch_user(uid: int):
    users[str(uid)]["last_active"] = dt_to_iso(now_ist())
    mark_users_dirty()

def reset_daily_if_needed(uid: int):
    u = users[str(uid)]
    today = today_ist_str()
    if u.get("day") != today:
        u["day"] = today
        u["day_files"] = 0
        u["day_bytes_out"] = 0
        mark_users_dirty()

def is_premium(uid: int) -> bool:
    pu = users[str(uid)].get("premium_until")
    if not pu:
        return False
    dt = iso_to_dt(pu)
    return bool(dt and dt > now_ist())

def premium_till_str(uid: int) -> str:
    pu = users[str(uid)].get("premium_until")
    if not pu:
        return "None"
    dt = iso_to_dt(pu)
    if not dt:
        return "Invalid"
    if dt <= now_ist():
        return "Expired"
    return dt.strftime("%Y-%m-%d %H:%M IST")

def apply_premium(uid: int, add_seconds: int):
    u = users[str(uid)]
    now = now_ist()
    cur = iso_to_dt(u.get("premium_until") or "")
    if cur and cur > now:
        new_until = cur + datetime.timedelta(seconds=add_seconds)
    else:
        new_until = now + datetime.timedelta(seconds=add_seconds)
    u["premium_until"] = dt_to_iso(new_until)
    mark_users_dirty()

def check_free_limits(uid: int, next_upload_size_bytes: int) -> Optional[str]:
    if is_premium(uid):
        return None
    reset_daily_if_needed(uid)
    u = users[str(uid)]

    if int(u.get("day_files", 0)) >= FREE_DAILY_FILES:
        return f"❌ Free limit reached.\nDaily: {FREE_DAILY_FILES} files/day (IST)\nUse /redeem CODE for premium."

    if int(u.get("day_bytes_out", 0)) + int(next_upload_size_bytes) > FREE_DAILY_BYTES:
        return (
            "❌ Free limit reached.\n"
            f"Daily: {FREE_DAILY_GB} GB/day (IST)\n"
            f"Used: {human_bytes(int(u.get('day_bytes_out',0)))}\n"
            f"Next: {human_bytes(int(next_upload_size_bytes))}\n\n"
            "Use /redeem CODE for premium."
        )
    return None

# ================== MAINTENANCE ==================
async def maintenance_guard(message):
    if config.get("maintenance") and not is_admin(message.from_user.id):
        await message.reply_text("🛠️ Bot maintenance mode me hai. Baad me try karo.")
        return True
    return False

# ================== UI ==================
def menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🖼️ Set Thumbnail", callback_data="set_thumb"),
         InlineKeyboardButton("🗑️ Clear Thumbnail", callback_data="clear_thumb")],
        [InlineKeyboardButton("📥 Queue Status", callback_data="q_status"),
         InlineKeyboardButton("🧹 Clear Queue", callback_data="q_clear")],
        [InlineKeyboardButton("ℹ️ Version", callback_data="show_version")]
    ])

def type_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📁 Document", callback_data="type_doc"),
         InlineKeyboardButton("🎬 Video", callback_data="type_vid")]
    ])

def cancel_kb(uid: int):
    return InlineKeyboardMarkup([[InlineKeyboardButton("✖ CANCEL ✖", callback_data=f"cancel_{uid}")]])

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

async def run_blocking(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)

# ================== PROGRESS ==================
async def progress_callback(current, total, msg, stage: str, start_ts: float, cancel_flag: dict):
    if cancel_flag.get("stop"):
        raise Exception("Canceled by user")

    now = time.time()
    elapsed = max(now - start_ts, 0.001)
    speed = current / elapsed
    eta = (total - current) / speed if speed > 0 and total > 0 else 0
    pct = (current * 100 / total) if total else 0

    text = (
        f"**{BRAND}**\nVersion: `{BOT_VERSION}`\n\n"
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

# ================== QUEUE ITEM ==================
def queue_item_from_message(message) -> Optional[Dict[str, Any]]:
    if message.document:
        return {"kind": "document", "file_id": message.document.file_id, "file_name": message.document.file_name or "file"}
    if message.video:
        return {"kind": "video", "file_id": message.video.file_id, "file_name": message.video.file_name or "video.mp4"}
    if message.audio:
        return {"kind": "audio", "file_id": message.audio.file_id, "file_name": message.audio.file_name or "audio.mp3"}
    return None

def friendly_error(e: Exception) -> str:
    msg = str(e) or "Unknown error"
    low = msg.lower()
    if "canceled" in low:
        return "❌ Canceled ✅"
    if "flood" in low:
        return "⏳ Telegram FloodWait. Thoda wait karke try karo."
    if "file is too big" in low or "entity too large" in low:
        return "📦 File bahut bada hai. Telegram limit ho sakti hai."
    if "network" in low or "timeout" in low:
        return "🌐 Network/Timeout issue. Thoda baad try karo."
    return f"❌ Error: {msg}"

# ================== COUPON PRO SYSTEM ==================
def generate_coupon_code() -> str:
    alphabet = string.ascii_uppercase + string.digits
    core = "".join(secrets.choice(alphabet) for _ in range(10))
    return f"LEGEND-{core}"

def parse_duration(tok: str) -> Optional[int]:
    """
    1d / 2h / 30m / 7d
    return seconds
    """
    tok = (tok or "").strip().lower()
    m = re.fullmatch(r"(\d+)([dhm])", tok)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2)
    if n <= 0:
        return None
    if unit == "d":
        return n * 86400
    if unit == "h":
        return n * 3600
    if unit == "m":
        return n * 60
    return None

def cleanup_coupons():
    now = now_ist()
    to_del = []
    for code, info in coupons.items():
        exp_raw = info.get("expires_at")
        if exp_raw:
            exp = iso_to_dt(exp_raw)
            if exp and exp <= now and not info.get("redeemed_count", 0):
                # expired and unused
                to_del.append(code)
    for c in to_del:
        coupons.pop(c, None)

def create_coupon(duration_seconds: int, uses: int, lifetime: bool = False) -> Dict[str, Any]:
    cleanup_coupons()
    code = generate_coupon_code()
    created = now_ist()
    expires = None
    if not lifetime:
        expires = created + datetime.timedelta(seconds=duration_seconds)

    info = {
        "code": code,
        "created_at": dt_to_iso(created),
        "expires_at": dt_to_iso(expires) if expires else None,
        "duration_seconds": -1 if lifetime else int(duration_seconds),
        "uses_total": int(uses),
        "redeemed_count": 0,
        "redeemed_by": []  # list of user ids
    }
    coupons[code] = info
    mark_coupons_dirty()
    return info

def redeem_coupon(uid: int, code: str) -> str:
    code = code.strip().upper()
    info = coupons.get(code)
    if not info:
        return "❌ Invalid code."

    # already redeemed by this user?
    if uid in info.get("redeemed_by", []):
        return "❌ You already redeemed this code."

    # expiry check
    exp_raw = info.get("expires_at")
    if exp_raw:
        exp = iso_to_dt(exp_raw)
        if not exp or exp <= now_ist():
            return "❌ Code expired."

    # uses check
    uses_total = int(info.get("uses_total", 0))
    redeemed_count = int(info.get("redeemed_count", 0))
    if uses_total > 0 and redeemed_count >= uses_total:
        return "❌ Code usage limit reached."

    # apply premium
    dur = int(info.get("duration_seconds", 0))
    if dur == -1:
        apply_premium(uid, 10 * 365 * 86400)  # 10y lifetime
    else:
        if dur <= 0:
            return "❌ Code duration invalid."
        apply_premium(uid, dur)

    info["redeemed_count"] = redeemed_count + 1
    info.setdefault("redeemed_by", []).append(uid)
    mark_coupons_dirty()

    return f"✅ Premium Activated!\nValid till: `{premium_till_str(uid)}`"

def coupon_summary_line(code: str, info: Dict[str, Any]) -> str:
    exp_raw = info.get("expires_at")
    exp_txt = "No expiry" if not exp_raw else (iso_to_dt(exp_raw).strftime("%Y-%m-%d %H:%M IST") if iso_to_dt(exp_raw) else exp_raw)
    uses_total = int(info.get("uses_total", 0))
    used = int(info.get("redeemed_count", 0))
    return f"`{code}` | used {used}/{uses_total} | exp: {exp_txt}"

# ================== START / VERSION ==================
@app.on_message(filters.command("start"))
async def start_cmd(client, message):
    uid = message.from_user.id
    ensure_user(uid)
    if await maintenance_guard(message):
        return
    touch_user(uid)
    reset_daily_if_needed(uid)

    prem = "✅ PREMIUM" if is_premium(uid) else "❌ FREE"
    thumb = "✅ SET" if users[str(uid)].get("thumb_path") and os.path.exists(users[str(uid)]["thumb_path"]) else "❌ NOT SET"

    await message.reply_text(
        f"✅ Welcome, **{BRAND}**!\n"
        f"Version: `{BOT_VERSION}`\n\n"
        f"Plan: {prem}\n"
        f"Thumb: {thumb}\n\n"
        f"🆓 Free limits (IST): {FREE_DAILY_FILES} files/day + {FREE_DAILY_GB} GB/day\n"
        "Premium: `/redeem CODE`\n\n"
        "• Send thumbnail photo anytime (saved)\n"
        "• Send files then rename one-by-one",
        reply_markup=menu_kb()
    )

@app.on_message(filters.command("version"))
async def version_cmd(client, message):
    await message.reply_text(f"✅ Bot Version: `{BOT_VERSION}`")

@app.on_callback_query(filters.regex("^show_version$"))
async def show_version_cb(client, cq):
    await cq.answer("Version")
    await cq.message.reply_text(f"✅ Bot Version: `{BOT_VERSION}`")

# ================== THUMB MENU ==================
@app.on_callback_query(filters.regex("^set_thumb$"))
async def set_thumb_cb(client, cq):
    await cq.answer("Send photo")
    await cq.message.reply_text("🖼️ Send thumbnail photo now (anytime).")

@app.on_callback_query(filters.regex("^clear_thumb$"))
async def clear_thumb_cb(client, cq):
    uid = cq.from_user.id
    ensure_user(uid)
    t = users[str(uid)].get("thumb_path")
    users[str(uid)]["thumb_path"] = None
    mark_users_dirty()
    if t and os.path.exists(t):
        try:
            os.remove(t)
        except:
            pass
    await cq.answer("Cleared")
    await cq.message.reply_text("✅ Thumbnail cleared.")

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
    mark_users_dirty()
    await message.reply_text("✅ THUMBNAIL SAVED")

# ================== REDEEM ==================
@app.on_message(filters.command("redeem"))
async def redeem_cmd(client, message):
    uid = message.from_user.id
    ensure_user(uid)
    if await maintenance_guard(message):
        return
    touch_user(uid)
    reset_daily_if_needed(uid)

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.reply_text("Usage: `/redeem CODE`")
        return

    code = parts[1]
    result = redeem_coupon(uid, code)
    await message.reply_text(result)

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

# ================== ADMIN PANEL (PRO coupons) ==================
def admin_panel_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎟 Create Coupon", callback_data="admin_coupon")],
        [InlineKeyboardButton("📃 List Coupons", callback_data="admin_listcoupons")],
        [InlineKeyboardButton("🧰 Maintenance Toggle", callback_data="admin_maint")]
    ])

def coupon_mode_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Quick (1h/1d/7d/30d)", callback_data="coupon_quick")],
        [InlineKeyboardButton("Custom (uses + duration)", callback_data="coupon_custom")],
        [InlineKeyboardButton("Lifetime (uses)", callback_data="coupon_life")],
        [InlineKeyboardButton("⬅ Back", callback_data="admin_back")]
    ])

def coupon_quick_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("1h", callback_data="cq_1h"),
         InlineKeyboardButton("1d", callback_data="cq_1d")],
        [InlineKeyboardButton("7d", callback_data="cq_7d"),
         InlineKeyboardButton("30d", callback_data="cq_30d")],
        [InlineKeyboardButton("⬅ Back", callback_data="admin_coupon")]
    ])

@app.on_message(filters.command("panel"))
async def panel_cmd(client, message):
    uid = message.from_user.id
    ensure_user(uid)
    if not is_admin(uid):
        await message.reply_text("❌ Admin only.")
        return
    await message.reply_text("🧑‍💻 Admin Panel", reply_markup=admin_panel_kb())

@app.on_callback_query(filters.regex("^admin_back$"))
async def admin_back(client, cq):
    if not is_admin(cq.from_user.id):
        await cq.answer("Not allowed", show_alert=True)
        return
    await cq.answer("Back")
    await cq.message.edit_text("🧑‍💻 Admin Panel", reply_markup=admin_panel_kb())

@app.on_callback_query(filters.regex("^admin_coupon$"))
async def admin_coupon(client, cq):
    if not is_admin(cq.from_user.id):
        await cq.answer("Not allowed", show_alert=True)
        return
    await cq.answer("Coupon")
    await cq.message.edit_text("🎟 Select coupon mode:", reply_markup=coupon_mode_kb())

@app.on_callback_query(filters.regex("^coupon_quick$"))
async def coupon_quick(client, cq):
    if not is_admin(cq.from_user.id):
        await cq.answer("Not allowed", show_alert=True)
        return
    await cq.answer("Quick")
    await cq.message.edit_text("🎟 Quick coupon duration:", reply_markup=coupon_quick_kb())

@app.on_callback_query(filters.regex("^coupon_custom$"))
async def coupon_custom(client, cq):
    uid = cq.from_user.id
    if not is_admin(uid):
        await cq.answer("Not allowed", show_alert=True)
        return
    state[uid]["awaiting_coupon_params"] = True
    await cq.answer("Custom")
    await cq.message.reply_text(
        "Send custom coupon params like:\n"
        "`uses=50 duration=7d`\n"
        "`uses=10 duration=2h`\n"
        "`uses=1 duration=30m`\n\n"
        "Allowed: d/h/m",
    )

@app.on_callback_query(filters.regex("^coupon_life$"))
async def coupon_life(client, cq):
    uid = cq.from_user.id
    if not is_admin(uid):
        await cq.answer("Not allowed", show_alert=True)
        return
    state[uid]["awaiting_coupon_params"] = True
    await cq.answer("Lifetime")
    await cq.message.reply_text(
        "Send lifetime coupon params like:\n"
        "`uses=50 lifetime=yes`\n"
        "`uses=1 lifetime=yes`"
    )

@app.on_callback_query(filters.regex(r"^cq_(1h|1d|7d|30d)$"))
async def coupon_quick_create(client, cq):
    if not is_admin(cq.from_user.id):
        await cq.answer("Not allowed", show_alert=True)
        return

    key = cq.data
    dur = 3600 if key == "cq_1h" else 86400 if key == "cq_1d" else 7 * 86400 if key == "cq_7d" else 30 * 86400
    info = create_coupon(duration_seconds=dur, uses=1, lifetime=False)

    exp = iso_to_dt(info["expires_at"]).strftime("%Y-%m-%d %H:%M IST") if info.get("expires_at") and iso_to_dt(info["expires_at"]) else info.get("expires_at")
    await cq.answer("Created")
    await cq.message.reply_text(
        "✅ Coupon Created\n\n"
        f"Uses: `1`\n"
        f"Duration: `{key[3:]}`\n"
        f"Expires: `{exp}`\n\n"
        f"Code: `{info['code']}`\n\n"
        f"User redeem:\n`/redeem {info['code']}`"
    )

@app.on_callback_query(filters.regex("^admin_listcoupons$"))
async def admin_listcoupons(client, cq):
    if not is_admin(cq.from_user.id):
        await cq.answer("Not allowed", show_alert=True)
        return
    cleanup_coupons()
    # show latest 12
    items = list(coupons.items())[-12:]
    lines = ["📃 Latest Coupons (max 12 shown)\n"]
    for code, info in items:
        lines.append(coupon_summary_line(code, info))
    await cq.answer("List")
    await cq.message.reply_text("\n".join(lines) if items else "No coupons yet.")

@app.on_callback_query(filters.regex("^admin_maint$"))
async def admin_maint(client, cq):
    if not is_admin(cq.from_user.id):
        await cq.answer("Not allowed", show_alert=True)
        return
    config["maintenance"] = not config.get("maintenance", False)
    mark_config_dirty()
    await cq.answer("Done")
    await cq.message.reply_text(f"🧰 Maintenance: {config['maintenance']}")

# ================== TEXT INPUT (rename + admin custom coupon params) ==================
@app.on_message(filters.text)
async def text_in(client, message):
    uid = message.from_user.id
    ensure_user(uid)
    if await maintenance_guard(message):
        return
    touch_user(uid)
    reset_daily_if_needed(uid)

    # Admin custom coupon params
    if is_admin(uid) and state[uid].get("awaiting_coupon_params"):
        txt = (message.text or "").strip().lower()
        state[uid]["awaiting_coupon_params"] = False

        # parse: uses=50 duration=7d  OR  uses=50 lifetime=yes
        uses_m = re.search(r"uses\s*=\s*(\d+)", txt)
        if not uses_m:
            await message.reply_text("❌ Missing `uses=`. Example: `uses=50 duration=7d`")
            return
        uses = int(uses_m.group(1))
        if uses <= 0:
            await message.reply_text("❌ uses must be > 0")
            return

        if "lifetime=yes" in txt or "life=yes" in txt:
            info = create_coupon(duration_seconds=0, uses=uses, lifetime=True)
            await message.reply_text(
                "✅ Lifetime Coupon Created\n\n"
                f"Uses: `{uses}`\n"
                f"Expires: `No expiry`\n\n"
                f"Code: `{info['code']}`\n\n"
                f"User redeem:\n`/redeem {info['code']}`"
            )
            return

        dur_m = re.search(r"duration\s*=\s*([0-9]+[dhm])", txt)
        if not dur_m:
            await message.reply_text("❌ Missing `duration=`. Example: `uses=50 duration=7d`")
            return

        dur_tok = dur_m.group(1)
        dur = parse_duration(dur_tok)
        if dur is None:
            await message.reply_text("❌ Invalid duration. Use 1d/2h/30m")
            return

        info = create_coupon(duration_seconds=dur, uses=uses, lifetime=False)
        exp = iso_to_dt(info["expires_at"]).strftime("%Y-%m-%d %H:%M IST") if info.get("expires_at") and iso_to_dt(info["expires_at"]) else info.get("expires_at")
        await message.reply_text(
            "✅ Custom Coupon Created\n\n"
            f"Uses: `{uses}`\n"
            f"Duration: `{dur_tok}`\n"
            f"Expires: `{exp}`\n\n"
            f"Code: `{info['code']}`\n\n"
            f"User redeem:\n`/redeem {info['code']}`"
        )
        return

    # rename name input
    if not state[uid]["awaiting_name"]:
        return

    new_name = safe_filename(message.text)
    state[uid]["new_name"] = new_name
    state[uid]["awaiting_name"] = False
    state[uid]["awaiting_type"] = True

    await message.reply_text(
        f"**{BRAND}**\nVersion: `{BOT_VERSION}`\n\nSelect Output Type\n\nFile Name: `{new_name}`",
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
    reset_daily_if_needed(uid)

    item = queue_item_from_message(message)
    if not item:
        return

    state[uid]["queue"].append(item)
    persist_queue(uid)

    if state[uid].get("busy") or state[uid]["awaiting_name"] or state[uid]["awaiting_type"] or state[uid]["pending_item"]:
        await message.reply_text(f"📥 Added to queue. Queue: {len(state[uid]['queue'])}")
        return

    await ask_new_name(message.chat.id, uid)

async def ask_new_name(chat_id: int, uid: int):
    ensure_user(uid)
    if state[uid].get("busy"):
        return
    if not state[uid]["queue"]:
        await app.send_message(chat_id, f"✅ Queue done.\nVersion: `{BOT_VERSION}`", reply_markup=menu_kb())
        return

    item = state[uid]["queue"].pop(0)
    persist_queue(uid)

    state[uid]["pending_item"] = item
    state[uid]["awaiting_name"] = True

    old_name = item.get("file_name") or "file"
    await app.send_message(chat_id, f"**{BRAND}**\nVersion: `{BOT_VERSION}`\n\nEnter New Filename...\nOld: `{old_name}`")

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
    out_type = cq.data

    item = state[uid]["pending_item"]
    new_name = state[uid]["new_name"] or "file"
    state[uid]["pending_item"] = None
    state[uid]["new_name"] = None

    if not item:
        await cq.answer("No item")
        await cq.message.reply_text("❌ No pending item.", reply_markup=menu_kb())
        return

    old_name = item.get("file_name") or "file"
    old_ext = os.path.splitext(old_name)[1]
    if not os.path.splitext(new_name)[1] and old_ext:
        new_name += old_ext

    state[uid]["busy"] = True
    pmsg = await cq.message.reply_text(f"**{BRAND}**\nVersion: `{BOT_VERSION}`\n\nStarting...", reply_markup=cancel_kb(uid))
    cancel_flag = {"stop": False, "last_edit": 0, "kb": cancel_kb(uid)}
    state[uid]["cancel"] = cancel_flag

    final_path = None
    dl_path = None

    try:
        # DOWNLOAD
        dl_start = time.time()
        tmp_name = _unique_tmp_name(uid, old_name)
        dl_path = await app.download_media(
            item["file_id"],
            file_name=os.path.join(DL, tmp_name),
            progress=progress_callback,
            progress_args=(pmsg, "Downloading", dl_start, cancel_flag)
        )

        # rename local unique
        unique_final = _unique_tmp_name(uid, new_name)
        final_path = os.path.join(DL, unique_final)
        os.rename(dl_path, final_path)
        dl_path = None

        size_bytes = os.path.getsize(final_path) if os.path.exists(final_path) else 0

        # LIMIT CHECK
        block = check_free_limits(uid, size_bytes)
        if block:
            await pmsg.edit_text(block, reply_markup=menu_kb())
            return

        # THUMB
        thumb_to_use = None
        t = users[str(uid)].get("thumb_path")
        if t and os.path.exists(t):
            thumb_to_use = t

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

        # stats + daily usage
        u = users[str(uid)]
        u["count"] = int(u.get("count", 0)) + 1
        u["bytes_out"] = int(u.get("bytes_out", 0)) + int(size_bytes)

        reset_daily_if_needed(uid)
        u["day_files"] = int(u.get("day_files", 0)) + 1
        u["day_bytes_out"] = int(u.get("day_bytes_out", 0)) + int(size_bytes)
        mark_users_dirty()

        await pmsg.edit_text(f"✅ Done, **{BRAND}**!\nVersion: `{BOT_VERSION}`", reply_markup=menu_kb())

    except Exception as e:
        await pmsg.edit_text(friendly_error(e), reply_markup=menu_kb())

    finally:
        state[uid]["cancel"] = None
        state[uid]["busy"] = False

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

        await ask_new_name(cq.message.chat.id, uid)

    await cq.answer("OK")

# ================== /me ==================
@app.on_message(filters.command("me"))
async def me_cmd(client, message):
    uid = message.from_user.id
    ensure_user(uid)
    reset_daily_if_needed(uid)

    plan = "✅ PREMIUM" if is_premium(uid) else "❌ FREE"
    u = users[str(uid)]
    await message.reply_text(
        f"**{BRAND}**\nVersion: `{BOT_VERSION}`\n\n"
        f"Plan: {plan}\n"
        f"Premium till: `{premium_till_str(uid)}`\n\n"
        f"Renames: {u.get('count',0)}\n"
        f"Uploaded total: {human_bytes(int(u.get('bytes_out',0)))}\n\n"
        f"Today (IST): {u.get('day_files',0)}/{FREE_DAILY_FILES} files, "
        f"{human_bytes(int(u.get('day_bytes_out',0)))} / {human_bytes(FREE_DAILY_BYTES)}"
    )

# ================== AUTO CLEAN (/tmp) ==================
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

# ================== RUN ==================
if __name__ == "__main__":
    app.loop.create_task(flush_loop())
    app.loop.create_task(auto_clean_loop())
    app.run()

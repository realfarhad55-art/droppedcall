#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""dropbot.py — WizardScan DROPPED-CALL bot (SECOND bot, standalone).

Yeh main wizu bot se ALAG bot hai. Sirf ONE kaam: KOL channels me nayi
call aate hi dropped-call alert @WizardScan par post karna (~1-2 second).

DESIGN (user decision, Nov 24 2026):
- Main bot ka load upar-neeche hota rehta hai, dropped calls slow ho jati
  hain. Is liye dropped calls ka kaam ek dedicated bot ko de diya gaya,
  jiska apna event loop hai — monitoring jobs, leaderboard, dip, X alerts
  isko slow nahi kar sakte.
- Yeh bot sirf dropped calls karta hai. X alerts, leaderboard, dip,
  champions, Most Shared, Binance — sab main bot par hi rehte hain.

DATA SAFETY (hard rule):
- Yeh bot main bot ki files ko KABHI NAHI LIKHTA: tracked_calls.json aur
  sent_milestones.json ko sirf main bot likhta hai.
- Is bot ka apna record hai: data/dropbot_calls.json (is file ko main bot
  READ karta hai aur apne tracked_calls.json me import karta hai — woh
  import job main bot ki taraf hai, is file me nahi).
- Config/channels is bot ki APNI copy hain (data/config.json,
  data/channels.json) — setup par main bot se copy ki jati hain. Templates
  aur dropped videos wahi hain jo main bot ki config me hain.

PIPELINE (main bot ke _process_new_call jaisa, live code se extract kiya):
  Telethon realtime handler -> _process_new_call -> CA extract -> dex fetch
  (DexScreener-first, Birdeye, GeckoTerminal fallback) -> dust guard +
  entry reconcile + pre-post confirm -> send_dropped_alert (userbot-first,
  premium emojis, theme video) -> apne record me save.

LAUNCHPAD (PinkSale/GemPad/CheesePad) is bot me NAHI hai — woh main bot
ki taraf rehta hai (README me alag handler ka snippet hai). detect_launchpad
yahan stub hai (hamesha None).

ENV: .env file (DROPBOT_TOKEN, DROPBOT_API_ID, DROPBOT_API_HASH,
DROPBOT_SESSION, DROPBOT_TARGET_CHANNEL, DROPBOT_DATA_DIR, Birdeye key...).
Token kabhi chat me nahi — sirf VPS par .env me.
"""

# ── imports ────────────────────────────────────────────────────────────────
import os
import sys
import re
import io
import json
import time
import time as _t  # live bot me yeh x_alert.py ke badge-cache patch se aata hai
import uuid
import html
import hashlib
import logging
import requests
import asyncio
import functools
import threading
import contextvars as _contextvars
from datetime import datetime, timedelta
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor

from telegram import Bot as _TG_Bot
from telethon import TelegramClient
from telethon.sessions import StringSession

# ── .env loader (koi external dependency nahi) ─────────────────────────────
def _load_dotenv(path):
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except FileNotFoundError:
        pass

_ENV_PATH = os.environ.get("DROPBOT_ENV", os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".env"))
_load_dotenv(_ENV_PATH)

# ── logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S")
logger = logging.getLogger("dropbot")

# ── env / constants (main bot ke core.py wale naam, dropbot values) ────────
DROPBOT_TOKEN   = os.environ.get("DROPBOT_TOKEN", "")
DROPBOT_API_ID  = int(os.environ.get("DROPBOT_API_ID", "0") or 0)
DROPBOT_API_HASH = os.environ.get("DROPBOT_API_HASH", "")
DROPBOT_SESSION = os.environ.get("DROPBOT_SESSION", os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "wizard_scan.session"))
DROPBOT_TARGET_CHANNEL = os.environ.get("DROPBOT_TARGET_CHANNEL", "@WizardScan")
DROPBOT_DATA_DIR = os.environ.get("DROPBOT_DATA_DIR", os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data"))
DROPBOT_MAIN_DATA_DIR = os.environ.get("DROPBOT_MAIN_DATA_DIR", "/opt/bot/data")
DROPBOT_OWNER_ID = int(os.environ.get("DROPBOT_OWNER_ID", "6018602211") or 6018602211)

BOT_TOKEN   = DROPBOT_TOKEN
OWNER_ID    = DROPBOT_OWNER_ID
OWNER_ID_2  = int(os.environ.get("DROPBOT_OWNER_ID_2", "2131647292") or 2131647292)
OWNER_IDS   = {OWNER_ID, OWNER_ID_2}
TARGET_CHANNEL = DROPBOT_TARGET_CHANNEL
BOT_LINK    = "https://t.me/WIZARD_SCAN_BOT"
DATA_DIR    = DROPBOT_DATA_DIR
os.makedirs(DATA_DIR, exist_ok=True)

BOT_CONFIG_FILE         = os.path.join(DATA_DIR, "bot_config.json")
CHANNELS_FILE           = os.path.join(DATA_DIR, "channels.json")
SPECIAL_CHANNELS_FILE   = os.path.join(DATA_DIR, "special_channels.json")
GLOBAL_CA_BAN_FILE      = os.path.join(DATA_DIR, "global_ca_bans.json")
REMOVED_CHANNELS_FILE   = os.path.join(DATA_DIR, "removed_channels.json")
USERBOT_SESSION_FILE    = os.path.join(DATA_DIR, "userbot_session.txt")
USERBOT_SESSION2_FILE   = os.path.join(DATA_DIR, "userbot_session2.txt")
DROPBOT_CALLS_FILE      = os.path.join(DATA_DIR, "dropbot_calls.json")
DROPBOT_SEEN_FILE       = os.path.join(DATA_DIR, "dropbot_seen.json")
DROPBOT_DETAILS_FILE    = os.path.join(DATA_DIR, "dropbot_details.json")
XALERT_DETAILS_FILE     = DROPBOT_DETAILS_FILE

BIRDEYE_API_KEY = os.environ.get("BIRDEYE_API_KEY", "")
MORALIS_API_KEY = os.environ.get("MORALIS_API_KEY", "")
CMC_API_KEY     = os.environ.get("CMC_API_KEY", "")
EXTRA_SOURCES_ENABLED = os.environ.get("EXTRA_SOURCES_ENABLED", "1") not in ("0", "false", "False")

USERBOT_API_ID   = DROPBOT_API_ID
USERBOT_API_HASH = DROPBOT_API_HASH
OWNER_API_ID     = DROPBOT_API_ID   # init_userbot in names se bhi mangta hai
OWNER_API_HASH   = DROPBOT_API_HASH
USERBOT_PHONE    = os.environ.get("DROPBOT_USERBOT_PHONE", "")
X_ALERT_CHANNEL  = TARGET_CHANNEL
TG_CHANNEL_LINK  = "https://t.me/WizardScan"

# PROMO_EMOJI_ID: live core.py me yeh naam use hota hai (promo link ka 🔮).
# Default pack ka crystal — config promo_link emoji_id set ho to woh jeetta hai.
PROMO_EMOJI_ID = 5816433179703124832
RT_MAX_QUEUE_AGE_SECONDS = int(os.environ.get("RT_MAX_QUEUE_AGE_SECONDS", "600") or 600)
RT_SEND_TIMEOUT_SECONDS  = int(os.environ.get("RT_SEND_TIMEOUT_SECONDS", "20") or 20)
RT_MAX_SEND_RETRIES      = int(os.environ.get("RT_MAX_SEND_RETRIES", "2") or 2)

BOT_START_TS = time.time()   # NOCATCHUP: sirf is boot ke BAAD ki posts process hongi
BOT_READY    = True
_RT_INFLIGHT = set()
_threading   = threading
userbot_client  = None
userbot_client2 = None
_bot_ref        = None

# ── dropbot ka APNA record (main bot ki files ko kabhi nahi likhta) ─────────
# tracked_calls: {call_key: {...same fields as main bot...}} — yahi file
# data/dropbot_calls.json hai, jise main bot READ karke import karta hai.
tracked_calls = {}
seen_message_ids = defaultdict(set)


def _atomic_write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
    os.replace(tmp, path)


def _load_tracked():
    global tracked_calls
    try:
        with open(DROPBOT_CALLS_FILE, encoding="utf-8") as f:
            tracked_calls = json.load(f) or {}
    except Exception:
        tracked_calls = {}


def _save_tracked():
    try:
        _atomic_write_json(DROPBOT_CALLS_FILE, tracked_calls)
    except Exception as e:
        logger.warning(f"_save_tracked failed: {e}")


def _load_seen():
    global seen_message_ids
    try:
        with open(DROPBOT_SEEN_FILE, encoding="utf-8") as f:
            raw = json.load(f) or {}
        seen_message_ids = defaultdict(set, {k: set(v) for k, v in raw.items()})
    except Exception:
        seen_message_ids = defaultdict(set)


def _save_seen():
    try:
        _atomic_write_json(DROPBOT_SEEN_FILE,
                           {k: sorted(v) for k, v in seen_message_ids.items()})
    except Exception as e:
        logger.warning(f"_save_seen failed: {e}")


def load_users():
    return {}


# ── stubs: yeh features main bot ki taraf hain, dropbot me nahi ────────────
def detect_launchpad(text):
    """Launchpad (PinkSale/GemPad/CheesePad) dropbot me nahi — main bot ki
    taraf rehta hai. Hamesha None, taake _process_new_call ka launchpad
    branch kabhi fire na ho."""
    return None


async def send_pinksale_alert(*a, **k):
    return None


async def send_cheesepad_alert(*a, **k):
    return None


async def send_launchpad_alert(*a, **k):
    return None


def _hs_register_call(channel, call_key, chain):
    """Hot streak main bot ki taraf register hota hai (import par)."""
    return None


async def _process_deleted_posts(username, deleted_ids, bot=None):
    """Deleted-posts handling main bot ki taraf hai."""
    return None


def _register_drop_details(channel, ca):
    """Details deep-link id — deterministic (sha256 of 'drop:<call_key>'),
    is liye main bot import par wahi sid dobara bana sakta hai. Entry
    dropbot ki apni details file me likhi jati hai."""
    call_key = f"{channel}_{ca}"
    sid = hashlib.sha256(f"drop:{call_key}".encode("utf-8")).hexdigest()[:12]
    try:
        try:
            with open(DROPBOT_DETAILS_FILE, encoding="utf-8") as f:
                m = json.load(f) or {}
        except Exception:
            m = {}
        entry = {"call_key": call_key, "kind": "drop"}
        if m.get(sid) != entry:
            m[sid] = entry
            _atomic_write_json(DROPBOT_DETAILS_FILE, m)
    except Exception as e:
        logger.warning(f"_register_drop_details failed: {e}")
    return sid




# ── userbot connect (@Wizard_Scan, userbot 2) ──────────────────────────────
async def _dropbot_connect_userbot():
    """@Wizard_Scan session se Telethon userbot connect karo.
    DROPBOT_SESSION ek file path hai:
      - agar file me StringSession string hai (main bot ka
        /opt/bot/data/userbot_session2.txt isi format me hai) to StringSession
      - agar .session SQLite file hai to Telethon use seedha kholta hai.
    Dono cases handle hote hain."""
    global userbot_client
    if not DROPBOT_API_ID or not DROPBOT_API_HASH:
        logger.error("DROPBOT_API_ID / DROPBOT_API_HASH .env me nahi — userbot nahi chalega")
        return None
    session_arg = DROPBOT_SESSION
    try:
        if os.path.isfile(DROPBOT_SESSION):
            with open(DROPBOT_SESSION, encoding="utf-8", errors="ignore") as f:
                content = f.read().strip()
            # StringSession string: lambi, sirf [A-Za-z0-9+/=] chars
            if len(content) > 100 and all(c in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=" for c in content):
                session_arg = StringSession(content)
                logger.info("Userbot session: StringSession file se load hui")
    except Exception as e:
        logger.warning(f"Session file read failed ({e}) — path ko session name mana jayega")
    try:
        client = TelegramClient(session_arg, DROPBOT_API_ID, DROPBOT_API_HASH)
        await client.start()
        me = await client.get_me()
        logger.info(f"Userbot connected: @{getattr(me, 'username', '?')} (id={getattr(me, 'id', '?')})")
        userbot_client = client
        return client
    except Exception as e:
        logger.error(f"Userbot connect failed: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════
# Neeche ka code live main-bot code se AST-extract kiya gaya hai
# (core.py + x_alert.py + dropped_call.py, Nov 23 GitHub-synced copies).
# Sirf 3 adaptations hain (file ke top par likhi hain):
#   A1. setup_realtime_monitoring se deleted-posts handler hata diya.
#   A2. retry_schedule me (3, 30) phase add ki (slow indexing ke liye).
#   A3. _register_drop_details dropbot version (upar) use hota hai.
# ═══════════════════════════════════════════════════════════════════════════

_CHANNEL_IDS_FILE = os.path.join(DATA_DIR, "channel_ids.json")

_channel_ids = {}

def _get_saved_channel_id(username):
    try:
        v = _channel_ids.get(str(username).lstrip("@").lower())
        return int(v) if v else None
    except Exception:
        return None

def _save_channel_id(username, cid):
    try:
        _channel_ids[str(username).lstrip("@").lower()] = int(cid)
        _tmp = _CHANNEL_IDS_FILE + ".tmp"
        json.dump(_channel_ids, open(_tmp, "w", encoding="utf-8"))
        os.replace(_tmp, _CHANNEL_IDS_FILE)
    except Exception:
        pass

def _dp(name):  # data path helper
    return os.path.join(DATA_DIR, name)

SUPPORTED_CHAINS = {"ethereum": "ETH", "bsc": "BNB", "base": "BASE", "solana": "SOL",
                    "robinhood": "RH", "ton": "TON", "arc": "ARC"}

CHAIN_TO_DEXPATH = {"SOL": "solana", "ETH": "ethereum", "BNB": "bsc", "BASE": "base",
                    "RH": "robinhood", "TON": "ton", "ARC": "arc"}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

ETH_CA_PATTERN = re.compile(r'0x[a-fA-F0-9]{64}(?![a-fA-F0-9])|0x[a-fA-F0-9]{40}(?![a-fA-F0-9])')

SOL_CA_PATTERN = re.compile(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b')

TON_CA_PATTERN = re.compile(r'\b(EQ|UQ)[A-Za-z0-9_-]{46}\b')

TW_LINK_RE     = re.compile(r'(?:twitter\.com|x\.com)/([A-Za-z0-9_]+)', re.IGNORECASE)

TG_MENTION_RE  = re.compile(r'^@([A-Za-z0-9_]{3,32})\s*$')

def _display_handle(value):
    """Display a Telegram username with a capitalized first character."""
    raw = str(value or "").lstrip("@").strip()
    return raw[:1].upper() + raw[1:].lower() if raw else raw

TEER_EMOJI_ID = 5807638920631951079

MAESTRO_REFLINK_BASE = "https://t.me/maestro?start="

MAESTRO_REF_SUFFIX   = "_r-wizard_scan"

EMOJI_PACKS = [
    {   # Sirf yahi emojis — har 10 posts wali rotation band.
        # Owner baad me naye packs isi list me add karega.
        "name": "default",
        "crystal": 5816433179703124832,   # Crystal ball (entry MC)
        "kol":     5816473758554136304,   # Caller
        "mae":     5816930983592599467,   # Trade
        "bot":     5816604050682028040,   # Details
        "caller":  5816473758554136304,
        "trade":   5816930983592599467,
        "details": 5816604050682028040,
        "twitter": 5816676712938741891,
        "dex":     5816447254310954356,
        "dev":     5816456703239004710,
        "chain": {
            "SOL":  5816406349042425264,
            "ETH":  5816735725789388508,
            "BNB":  5816541305504800959,
            "BSC":  5816541305504800959,
            "BASE": 5816735725789388508,  # BASE emoji nahi diya gaya — ETH fallback
            "RH":   5818717423699763201,
            "TON":  5816609367851542120,
            "ARC":  5861777584693453399,
        },
    },
    {   # BLUE pack — 11X aur us se upar wali X alerts ke liye.
        "name": "blue",
        "teer":    5818848205453927129,   # Teer (MC separator)
        "crystal": 5816826345304367540,   # Crystal ball (entry MC)
        "kol":     5864095960795193775,   # Caller
        "mae":     5866050647656241980,   # Trade
        "bot":     5863725747499181228,   # Details
        "caller":  5864095960795193775,
        "trade":   5866050647656241980,
        "details": 5863725747499181228,
        "twitter": 5866085209258076126,
        "dex":     5863945855983166730,
        "dev":     5864070027782661496,
        "chain": {
            "SOL":  5865982877367279051,
            "ETH":  5865974768469026405,
            "BNB":  5866260216290483682,
            "BSC":  5866260216290483682,
            "BASE": 5866335262254047661,
            "RH":   5866254272055747007,
            "TON":  5866181502424850828,
            "ARC":  5866195366579281407,
        },
    },
]

DROPPED_CALL_EMOJI = 5821384894743324105

DROPPED_NUM_EMOJI   = 5877468380125990242

DROPPED_ARROW_EMOJI = 5884123981706956210

DROPPED_CHAIN_EMOJIS = {
    "SOL":  5821384894743324105,
    "ETH":  5821051253093834925,
    "BNB":  5821408568603058588,
    "BSC":  5821408568603058588,
    "BASE": 5821088567769701396,
    "RH":   5821306460050562617,
    "TON":  5821137045065572054,  # TON drop call emoji
    "ARC":  5861917123885932251,
}

DROPPED_KOL_EMOJI = 5810115394479857144

DROPPED_MAE_EMOJI = 5807410153493901098

DROPPED_BOT_EMOJI = 5807874018551799876

_DROPPED_CALL_PACK = {
    "name": "dropped", "kol": DROPPED_KOL_EMOJI,
    "mae": DROPPED_MAE_EMOJI, "bot": DROPPED_BOT_EMOJI,
    "dex": 5821147954282504203,      # Dexscener link emoji (green theme)
    "details": 5821230129891778303,  # Details link emoji (green theme)
    "channel": 5821034167713930552,  # Channel link emoji (green theme)
}

DEFAULT_DROPPED_TEMPLATE = (
    "🔮 <b>NEW CALL ALERT</b>\n\n"
    "<i>@{channel} picked up ${symbol} at {entry} MC. Explore the project, tap Details.</i>\n\n"
    "<blockquote><code>{ca}</code></blockquote>\n\n"
    '🔮<a href="{dex_link}">Dexscener</a> '
    '🔮<a href="{details_link}">Details</a> '
    '🔮<a href="{kol_link}">Channel</a>'
)

DEFAULT_DROPPED_TEMPLATE_GREEN_PURPLE = (
    "🔮 <b>@{channel} Target Acquired</b>\n\n"
    '<i>@{channel} introduced ${symbol} at just {entry} MC. Tap "Details" below to view the full token breakdown.</i>\n\n'
    "<blockquote><code>{ca}</code></blockquote>\n\n"
    '🔮<a href="{kol_link}">Caller</a> '
    '🔮<a href="{details_link}">Details</a> '
    '🔮<a href="{dex_link}">Chart</a>'
)

DEFAULT_DROPPED_TEMPLATE_ORANGE = (
    "🔮 <b>Alpha Just Dropped</b>\n\n"
    "<i>@{channel} shared ${symbol} with the community at {entry} MC. Explore the project, tap Details.</i>\n\n"
    "<blockquote><code>{ca}</code></blockquote>\n\n"
    '🔮<a href="{details_link}">Details</a> '
    '🔮<a href="{kol_link}">Caller</a> '
    '🔮<a href="{dex_link}">Chart</a>'
)

DROPPED_CHAIN_EMOJIS_GREEN_PURPLE = {
    "SOL":  5929381018472554242,
    "ETH":  5929005157294547050,
    "BNB":  5929211564832859076,
    "BSC":  5929211564832859076,
    "BASE": 5929292451951944001,
    "RH":   5929408952939846789,
    "TON":  5821137045065572054,  # fallback: green pack TON (theme me nahi mila)
    "ARC":  5926935975195320004,
}

DROPPED_CHAIN_EMOJIS_ORANGE = {
    "SOL":  5909004903346347892,
    "ETH":  5908976732655853914,
    "BNB":  5908859166516059917,
    "BSC":  5908859166516059917,
    "BASE": 5909019867012406568,
    "RH":   5908937223251698020,
    "TON":  5909176702038188137,
    "ARC":  5909125703596516470,
}

_DROPPED_CALL_PACK_GREEN_PURPLE = {
    "name": "dropped", "kol": DROPPED_KOL_EMOJI,
    "mae": DROPPED_MAE_EMOJI, "bot": DROPPED_BOT_EMOJI,
    "caller": 5924778419849011394,
    "details": 5925032716272672656,
    "chart": 5924716748413608464,
}

_DROPPED_CALL_PACK_ORANGE = {
    "name": "dropped", "kol": DROPPED_KOL_EMOJI,
    "mae": DROPPED_MAE_EMOJI, "bot": DROPPED_BOT_EMOJI,
    "caller": 5906826581833162255,
    "details": 5906777292788474761,
    "chart": 5909045787140038317,
}

DEFAULT_DROPPED_TEMPLATE_RED = (
    "🔮 <b>New Call Detected</b>\n\n"
    "<i>${symbol} caught @{channel}'s attention at {entry} MC. Click Details for insights.</i>\n\n"
    "<blockquote><code>{ca}</code></blockquote>\n\n"
    '🔮<a href="{details_link}">Details</a> '
    '🔮<a href="{dex_link}">Chart</a> '
    '🔮<a href="{kol_link}">Post</a>'
)

DEFAULT_DROPPED_TEMPLATE_GOLDEN = (
    "🔮 <b>KOL Just Dropped Heat</b>\n\n"
    "<i>@{channel} just put ${symbol} on the radar at {entry} MC. Explore the project, tap Details.</i>\n\n"
    "<blockquote><code>{ca}</code></blockquote>\n\n"
    '🔮<a href="{kol_link}">Channel</a> '
    '🔮<a href="{dex_link}">Chart</a> '
    '🔮<a href="{details_link}">Details</a>'
)

DEFAULT_DROPPED_TEMPLATE_BLUE = (
    "🔮 <b>Wizard Spotted a Call</b>\n\n"
    "<i>@{channel} featured ${symbol} at {entry} MC. Click Details to explore the token.</i>\n\n"
    "<blockquote><code>{ca}</code></blockquote>\n\n"
    '🔮<a href="{dex_link}">Chart</a> '
    '🔮<a href="{details_link}">Details</a> '
    '🔮<a href="{kol_link}">Caller</a>'
)

DROPPED_CHAIN_EMOJIS_RED = {
    "SOL":  5816840991142846916,
    "ETH":  5819127232299278429,
    "BNB":  5818672536996551309,
    "BSC":  5818672536996551309,
    "BASE": 5816773508616692571,
    "RH":   5816414432170876425,
    "TON":  5816544874622624801,
    "ARC":  5861777584693453399,
}

DROPPED_CHAIN_EMOJIS_GOLDEN = {
    "SOL":  5875373990863708712,
    "ETH":  5875271375505072844,
    "BNB":  5875281000526783948,
    "BSC":  5875281000526783948,
    "BASE": 5872923295409514344,
    "RH":   5875177539059590032,
    "TON":  5875196054663602729,
    "ARC":  5875458004718984647,
}

DROPPED_CHAIN_EMOJIS_BLUE = {
    "SOL":  5865982877367279051,
    "ETH":  5865974768469026405,
    "BNB":  5866260216290483682,
    "BSC":  5866260216290483682,
    "BASE": 5866335262254047661,
    "RH":   5866254272055747007,
    "TON":  5866181502424850828,
    "ARC":  5866195366579281407,
}

_DROPPED_CALL_PACK_RED = {
    "name": "dropped", "kol": DROPPED_KOL_EMOJI,
    "mae": DROPPED_MAE_EMOJI, "bot": DROPPED_BOT_EMOJI,
    "details": 5816604050682028040,
    "chart": 5816447254310954356,
    "post": 5816473758554136304,
}

_DROPPED_CALL_PACK_GOLDEN = {
    "name": "dropped", "kol": DROPPED_KOL_EMOJI,
    "mae": DROPPED_MAE_EMOJI, "bot": DROPPED_BOT_EMOJI,
    "channel": 5875466551703907653,
    "chart": 5875342860940747160,
    "details": 5873177493048927315,
}

_DROPPED_CALL_PACK_BLUE = {
    "name": "dropped", "kol": DROPPED_KOL_EMOJI,
    "mae": DROPPED_MAE_EMOJI, "bot": DROPPED_BOT_EMOJI,
    "chart": 5863945855983166730,
    "details": 5863725747499181228,
    "caller": 5864070027782661496,
}

def load_json(path, default):
    try:
        with open(path) as f: return json.load(f)
    except Exception: return default

def save_json(path, data):
    # ATOMIC WRITE (permanent fix): pehle temp file me likho, fsync karo,
    # phir rename. Restart/crash beech me aaye to file kabhi aadhi nahi
    # rehti — ya purani poori, ya nayi poori.
    import os as _os, tempfile as _tf
    _d = _os.path.dirname(path) or "."
    _fd, _tmp = _tf.mkstemp(dir=_d, prefix=".tmp-", suffix=".json")
    try:
        with _os.fdopen(_fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.flush()
            _os.fsync(f.fileno())
        _os.replace(_tmp, path)
    except Exception:
        try:
            _os.unlink(_tmp)
        except Exception:
            pass
        raise

def load_channels():
    """Return tracked channels, self-healing case-insensitive duplicates
    (e.g. 'SomeKOL' and 'somekol' both stored) that caused the same channel
    to show twice under Tracked KOLs and be scanned/monitored twice."""
    raw = load_json(CHANNELS_FILE, [])
    seen, deduped, dup_found = set(), [], False
    for c in raw:
        key = str(c).lstrip("@").lower()
        if key in seen:
            dup_found = True
            continue
        seen.add(key)
        deduped.append(c)
    if dup_found:
        save_json(CHANNELS_FILE, deduped)
    return deduped

def save_channels(c):        save_json(CHANNELS_FILE, c)

def load_removed_channels() -> set:
    try:
        return {str(c).lstrip("@").lower() for c in load_json(REMOVED_CHANNELS_FILE, [])}
    except Exception:
        return set()

_REMOVED_CHANNELS = load_removed_channels()

def is_channel_removed(ch) -> bool:
    return str(ch or "").lstrip("@").lower() in _REMOVED_CHANNELS

def load_config():           return load_json(BOT_CONFIG_FILE, {})

def save_config(c):          save_json(BOT_CONFIG_FILE, c)

def load_special_channels():
    raw = load_json(SPECIAL_CHANNELS_FILE, [])
    if not isinstance(raw, list):
        return []
    return list(dict.fromkeys(
        str(ch).lstrip("@").strip().lower() for ch in raw if str(ch).strip()
    ))

def load_global_ca_bans():   return set(x.lower() for x in load_json(GLOBAL_CA_BAN_FILE, []))

def cfg_set(key, value):
    c = load_config(); c[key] = value; save_config(c)

_userbot_rr      = 0

_userbot_edit_lock = asyncio.Lock()

async def _locked_userbot_edit(*args, **kwargs):
    """Same as userbot_client.edit_message(...) but serialized via _userbot_edit_lock."""
    async with _userbot_edit_lock:
        return await userbot_client.edit_message(*args, **kwargs)

_userbot_init_lock = None

_bot_ref         = None

def safe_format(template, **kwargs):
    # User-facing templates should never render a fully lowercase @username.
    # Keep stored channel keys lowercase; capitalize only the display value.
    if "channel" in kwargs:
        kwargs["channel"] = _display_handle(kwargs["channel"])
    lk = {k.lower(): v for k, v in kwargs.items()}
    return re.sub(r'\{([A-Za-z_][A-Za-z0-9_]*)\}',
                  lambda m: str(lk[m.group(1).lower()]) if m.group(1).lower() in lk else m.group(0),
                  template)

def fmt_mc(value):
    """Format market cap without a currency sign, e.g. 12.5K MC or 3.45M MC."""
    if not value: return "N/A"
    if value >= 1_000_000_000: return f"{value/1_000_000_000:.2f}B MC"
    elif value >= 1_000_000:   return f"{value/1_000_000:.2f}M MC"
    elif value >= 1_000:       return f"{value/1_000:.1f}K MC"
    return f"{value:.0f} MC"

def _mc_no_label(s):
    """Strip the trailing ' MC' label off an already fmt_mc()-formatted string,
    e.g. '12.1K MC' -> '12.1K'. Used in spots where the surrounding template
    text already says 'MC' on its own (dropped-call header, x-alert entry/
    current, channel history), so the number isn't doubled up with 'MC MC'."""
    if not isinstance(s, str):
        return s
    if s.endswith(" MC"):
        return s[:-3]
    return s

def parse_mc_string(s):
    """Parse MC strings like '5K', '$5K', '1.5M', '50000' into a float.
    Returns 0.0 if parsing fails."""
    if not s: return 0.0
    s = s.strip().lstrip("$").strip().upper()
    try:
        if   s.endswith("B"): return float(s[:-1]) * 1_000_000_000
        elif s.endswith("M"): return float(s[:-1]) * 1_000_000
        elif s.endswith("K"): return float(s[:-1]) * 1_000
        else:                 return float(s)
    except Exception: return 0.0

_MC_TEXT_PATTERNS = [
    r"(?:market\s*cap|mkt\s*cap|mcap|m\.?c\.?)\s*[:\-=~@]?\s*\$?\s*([0-9][0-9,.]*\s*[kKmMbB]?)",
    r"\$?\s*([0-9][0-9,.]*\s*[kKmMbB])\s*(?:mc|mcap|market\s*cap|mkt\s*cap)\b",
    # "MC 200K" / "CAP: $200k" / "MC @ 200k"
    r"\b(?:mc|mcap|cap)\b\s*[:\-=~@]?\s*\$?\s*([0-9][0-9,.]*\s*[kKmMbB])",
    # "called at 200k", "entry 200k", "aped at 200k", "in at 200k"
    r"(?:called|call|entry|enter(?:ed)?|ape[d]?|bought|buy|in)\s*(?:at|@|:)?\s*\$?\s*([0-9][0-9,.]*\s*[kKmMbB])\b",
    # NOTE: a bare "@ 200k" / "at 200k" catch-all pattern used to live here.
    # It matched ANY number following "at"/"@" anywhere in the post — stop-loss
    # levels, take-profit targets, follower counts, unrelated numbers — not
    # just the market cap. That was the source of "called at 40K, bot tracked
    # at 5K" (grabbed an unrelated "5K" from elsewhere in the message) which
    # then made normal 2-3X moves compute as absurd 10000X/200M alerts. Removed
    # — patterns above already cover every legitimate "MC:"/"called at"/"entry"
    # phrasing; anything not caught by those isn't safely attributable to MC.
]

_MC_NOT_ENTRY_CTX = re.compile(
    r"(evolv|target|goal|final|reach|migrat|graduat|bond|next|tp\b|sl\b|stop|moon|aim|"
    r"hit|then|until|unlock|level|stage|phase|form)", re.IGNORECASE)

def extract_mc_from_text(text: str) -> float:
    """Return the market cap written inside a KOL post, or 0.0 if none found."""
    if not text:
        return 0.0
    # (see FIX 2026-09-25 note: strip markdown bold/italic/code markers first)
    text = re.sub(r"[*_`]+", "", text)
    # FIX (2026-10-21, $EVOLVE "30X free" incident): the post had no entry MC at all, only
    # "evolves at $10K MC, final form at $30K MC". The "<num> MC" pattern read 10K as the
    # entry, so a 322K token showed a fake 30X. Matches whose preceding words describe a
    # target/mechanic are skipped, and if a post still contains several different MC
    # numbers without an explicit entry phrase, it is ambiguous -> no hint (live chart).
    found = []   # (pattern_index, value)
    for idx, pat in enumerate(_MC_TEXT_PATTERNS):
        for m in re.finditer(pat, text, re.IGNORECASE):
            raw = (m.group(1) or "").replace(",", "").replace(" ", "")
            val = parse_mc_string(raw)
            if not (500 <= val <= 100_000_000_000):
                continue
            ctx = text[max(0, m.start() - 30):m.start()]
            if idx != 3 and _MC_NOT_ENTRY_CTX.search(ctx):
                continue
            found.append((idx, val))
    if not found:
        return 0.0
    # explicit "called at / entry / aped at" wins
    for idx, val in found:
        if idx == 3:
            return val
    distinct = {v for _, v in found}
    if len(distinct) == 1:
        return found[0][1]
    # several different numbers: only trust an explicit "MC: X" / "Market Cap: X" label
    for idx, val in found:
        if idx in (0, 2):
            return val
    return 0.0

ENTRY_MC_FRESH_SECONDS = float(os.environ.get("ENTRY_MC_FRESH_SECONDS", "30") or 30)

_MC_HINT_TOLERANCE     = float(os.environ.get("MC_HINT_TOLERANCE", "1.5") or 1.5)

def _reconcile_entry_mc(hint_mc: float, live_mc: float, live_is_fdv: bool = False):
    """Pick the entry MC. Returns (mc, source) where source is 'post',
    'post_unconfirmed', 'live', or 'live_fdv'.

    'post' wins outright when it is HIGHER than or equal to live — that
    removes the "called at 15K, tracked at 25K" drift, and a higher entry can
    never inflate the caller's own X multiple in their favor, so there is no
    fraud risk in trusting it immediately with no further checks.

    A hint that is LOWER than live — the "claims 5K, really trading at 300K"
    spoof case, whether from genuine indexing lag or a deliberate lowball to
    farm a fake X — is never trusted outright anymore, even within tolerance.
    It is returned as 'post_unconfirmed': used immediately so tracking isn't
    delayed, but callers must gate milestone alerts on it and run it through
    the same independent cross-check ladder used for unconfirmed live reads,
    revising upward if an independent source disagrees. This is what actually
    closes the cheat: a genuinely-early low hint gets confirmed and left
    alone; a deliberately-low hint gets caught and corrected before it can
    farm a fake X or trending slot.

    FIX 2: live_is_fdv -- when the live read only had a fully-diluted-value
    (max/total supply) available, not a real circulating marketCap, it is NOT
    trustworthy ground truth either, so it can't be used to reject the hint —
    but that does NOT make the hint itself trustworthy. It still only gets
    used provisionally, same as above.
    """
    try:
        hint_mc = float(hint_mc or 0)
        live_mc = float(live_mc or 0)
    except (TypeError, ValueError):
        return (0.0, "live")
    if hint_mc <= 0:
        # BUG FIX (2026-09-14, DOGGO/minibangcall incident): with no post hint
        # to sanity-check against, this used to return live_mc as plain "live"
        # even when live_is_fdv was True — silently defeating the whole point
        # of the FDV flag (see the note above: FDV is NOT reliable ground
        # truth). That is exactly how a real ~75K entry got locked in at an
        # FDV-derived 36.9K and looked like a fake instant 2X seconds later.
        # Tag it "live_fdv" instead so callers know this number is unconfirmed
        # and must not gate milestone alerts until an independent cross-check
        # lands.
        if live_is_fdv:
            return (live_mc, "live_fdv")
        return (live_mc, "live")
    if live_mc <= 0:
        return (hint_mc, "post")          # nothing to compare against yet
    if hint_mc >= live_mc:
        return (hint_mc, "post")          # hint higher/equal -- always trust it, no fraud risk
    if live_is_fdv:
        logger.info(f"📎 entry-MC: provisionally using post hint {fmt_mc(hint_mc)} — "
                    f"live read {fmt_mc(live_mc)} is FDV-derived so it can't "
                    f"confirm or reject the hint either way; will cross-check "
                    f"independently before trusting it fully")
        return (hint_mc, "post_unconfirmed")
    ratio = live_mc / hint_mc
    if ratio <= _MC_HINT_TOLERANCE:
        logger.info(f"📎 entry-MC: post hint {fmt_mc(hint_mc)} is {ratio:.2f}x below "
                    f"live {fmt_mc(live_mc)} — within tolerance but using "
                    f"provisionally pending independent cross-check, not blindly")
        return (hint_mc, "post_unconfirmed")
    return (live_mc, "live")

def _safe_html_cap(text, limit=1024):
    """Trim caption to limit without breaking open HTML tags."""
    if not text or len(text) <= limit:
        return text or ""
    cut = text[:limit]
    # If we sliced inside an open tag, back up to before that '<'
    last_open = cut.rfind("<")
    if last_open != -1 and ">" not in cut[last_open:]:
        cut = cut[:last_open]
    return cut.rstrip()

async def send_photo_safe(target, photo_path, caption, parse_mode="HTML", reply_markup=None):
    """Send photo from file path. Caption safely trimmed to 1024 chars. Returns message or None."""
    try:
        cap = _safe_html_cap(caption)
        with open(photo_path, "rb") as f:
            return await target.reply_photo(photo=f, caption=cap,
                                             parse_mode=parse_mode, reply_markup=reply_markup)
    except FileNotFoundError:
        logger.warning(f"Image not found: {photo_path}")
        return None
    except Exception as e:
        logger.warning(f"send_photo_safe ({photo_path}): {e}")
        return None

async def send_video_safe(target, video_path, caption, parse_mode="HTML", reply_markup=None):
    try:
        cap = caption[:1024] if caption else ""
        with open(video_path, "rb") as f:
            return await target.reply_video(video=f, caption=cap,
                                             parse_mode=parse_mode, reply_markup=reply_markup)
    except FileNotFoundError:
        logger.warning(f"Video not found: {video_path}")
        return None
    except Exception as e:
        logger.warning(f"send_video_safe ({video_path}): {e}")
        return None

def load_userbot_session():
    try:
        with open(USERBOT_SESSION_FILE) as f: return f.read().strip()
    except Exception: return ""

def load_userbot_session2():
    try:
        with open(USERBOT_SESSION2_FILE) as f: return f.read().strip()
    except Exception: return ""

def save_userbot_session2(s):
    with open(USERBOT_SESSION2_FILE, "w") as f: f.write(s)

def _userbot_clients():
    """Connected userbot clients, primary first."""
    out = []
    for cl in (userbot_client, userbot_client2):
        try:
            if cl is not None and cl.is_connected():
                out.append(cl)
        except Exception:
            pass
    return out

def _pick_userbot():
    """Round-robin pick among connected userbot clients. None if none connected."""
    global _userbot_rr
    clients = _userbot_clients()
    if not clients:
        return None
    _userbot_rr += 1
    return clients[_userbot_rr % len(clients)]

def _install_telethon_exception_handler():
    """Install a custom asyncio exception handler that swallows Telethon v1.44.0's
    known internal bugs (coroutine-not-awaited, event-loop-closed) so they don't
    crash the Railway process.

    Telethon v1.44.0 has two confirmed bugs:
      1. MTProtoSender._reconnect coroutine created but never awaited → RuntimeWarning
      2. TelegramBaseClient._disconnect_coro coroutine never awaited → RuntimeWarning
    Both are benign for our use-case — the watchdog job handles reconnection.
    """
    loop = asyncio.get_event_loop()

    def _handler(loop, context):
        msg       = context.get("message", "")
        exc       = context.get("exception")
        exc_str   = str(exc) if exc else ""
        # Known Telethon v1.44.0 benign errors — log as debug, don't crash
        telethon_noise = [
            "Event loop is closed",
            "coroutine 'MTProtoSender",
            "coroutine 'TelegramBaseClient",
            "_reconnect",
            "_disconnect_coro",
            "was never awaited",
            "NoneType can't be used in 'await'",
        ]
        if any(s in msg or s in exc_str for s in telethon_noise):
            logger.debug(f"[Telethon noise suppressed] {msg or exc_str}")
            return
        # All other exceptions: use the default handler
        loop.default_exception_handler(context)

    try:
        loop.set_exception_handler(_handler)
        logger.info("✅ Asyncio exception handler installed (Telethon crash suppression active)")
    except Exception as e:
        logger.warning(f"Could not install asyncio exception handler: {e}")

async def init_userbot():
    """Initialize (or re-initialize) the Telethon userbot client.

    Config hardened for Railway long-running deployments:
    - connection_retries=10  : retry network drops up to 10× before giving up
    - retry_delay=5          : 5s between retries
    - auto_reconnect=True    : Telethon auto-reconnects on connection drops
    - catch_up=False         : don't replay missed updates on reconnect (saves RAM/CPU)
    - receive_updates=True   : required for NewMessage event handler
    - flood_sleep_threshold=60 : auto-sleep on FLOOD_WAIT up to 60s
    """
    global userbot_client, userbot_client2, _userbot_init_lock
    session_str = load_userbot_session() or os.environ.get("SESSION_STRING", "").strip()
    if not session_str or not OWNER_API_ID or not OWNER_API_HASH:
        logger.warning("⚠️ Userbot session not found.")
        return
    if _userbot_init_lock is None:
        _userbot_init_lock = asyncio.Lock()
    async with _userbot_init_lock:
        # post_init and the watchdog used to enter here together, disconnecting
        # each other's MTProto transport and causing `_connection.recv()` on None.
        if userbot_client and userbot_client.is_connected():
            return
        candidate = None
        try:
            from telethon import TelegramClient
            from telethon.sessions import StringSession
            candidate = TelegramClient(
                StringSession(session_str),
                OWNER_API_ID,
                OWNER_API_HASH,
                connection_retries=3,
                retry_delay=3,
                auto_reconnect=True,
                catch_up=False,
                receive_updates=True,
                flood_sleep_threshold=60,
            )
            await asyncio.wait_for(candidate.connect(), timeout=45)
            if not await asyncio.wait_for(candidate.is_user_authorized(), timeout=15):
                logger.warning("Userbot: session exists but not authorized")
                await candidate.disconnect()
                return
            me = await asyncio.wait_for(candidate.get_me(), timeout=15)
            userbot_client = candidate
            logger.info(f"✅ Userbot: @{me.username or me.id}")
            _install_telethon_exception_handler()
        except Exception as e:
            logger.error(f"Userbot init failed: {type(e).__name__}: {e}")
            if candidate:
                try:
                    await candidate.disconnect()
                except Exception:
                    pass
            userbot_client = None
    # ── 2nd userbot account (@Wizard_Scan) — load sharing ──
    if userbot_client2 and userbot_client2.is_connected():
        return
    session2 = load_userbot_session2() or os.environ.get("SESSION_STRING_2", "").strip()
    if not session2:
        logger.info("ℹ️ 2nd userbot session not found (optional).")
        return
    candidate2 = None
    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        candidate2 = TelegramClient(
            StringSession(session2),
            OWNER_API_ID,
            OWNER_API_HASH,
            connection_retries=3,
            retry_delay=3,
            auto_reconnect=True,
            catch_up=False,
            receive_updates=True,
            flood_sleep_threshold=60,
        )
        await asyncio.wait_for(candidate2.connect(), timeout=45)
        if not await asyncio.wait_for(candidate2.is_user_authorized(), timeout=15):
            logger.warning("Userbot2: session exists but not authorized")
            await candidate2.disconnect()
            return
        me2 = await asyncio.wait_for(candidate2.get_me(), timeout=15)
        userbot_client2 = candidate2
        logger.info(f"✅ Userbot2: @{me2.username or me2.id}")
    except Exception as e:
        logger.error(f"Userbot2 init failed: {type(e).__name__}: {e}")
        if candidate2:
            try:
                await candidate2.disconnect()
            except Exception:
                pass
        userbot_client2 = None

_dex_cache: dict = {}

_DEX_CACHE_TTL = float(os.environ.get("DEX_CACHE_TTL_SECONDS", "2.5") or 2.5)

_DEX_V3_CHAIN = {
    "solana": "solana", "ethereum": "ethereum", "bsc": "bsc",
    "base": "base", "ton": "ton", "arbitrum": "arbitrum",
}

def _pick_market_cap(pairs: list) -> tuple[float, bool]:
    """Return (market_cap, is_fdv) — the most trustworthy market cap from a
    DexScreener pairs list, and whether it had to fall back to FDV.

    DexScreener reports marketCap/fdv per pair and a single stale pair can be
    badly wrong. We therefore:
      1. take marketCap from the deepest-liquidity pair,
      2. cross-check it against the median marketCap of all pairs of the same
         token — if it is more than 3x off, trust the median instead,
      3. never substitute FDV for real market cap AS IF it were real.

    FIX (RH-chain "called at 19K, bot shows 119K instantly" incident,
    2026-09-24): point 3 used to mean "return 0.0" whenever no pair had a
    real marketCap field — which DexScreener commonly never populates for
    Robinhood-chain (tokenized-stock) pairs, only `fdv`. A silent 0.0 here
    means _reconcile_entry_mc sees "nothing to compare the caller's post-hint
    against" and blindly LOCKS IN that hint as fully "confirmed", with the
    entire cross-check/correction machinery (_confirm_entry_mc, the pre-post
    settle check, the _mc_is_fdv ratio guard) never engaging at all — because
    all of that machinery only ever triggers off an explicit _mc_is_fdv flag,
    and this path never set one. A caller posting (or a bot mis-parsing) a
    lowballed "called at 2.7K" MC then rode straight through to a real 65K
    chart as a fake "24X", with zero of the anti-cheat checks built for
    exactly this scenario ever firing.
    Now: FDV is still never trusted AS real MC (nothing changes for the
    normal case), but it IS returned as a flagged, provisional number instead
    of a bare 0 — so callers correctly route it through the same
    live_fdv/post_unconfirmed provisional path already built for alt sources
    (Birdeye/GeckoTerminal), which locks it in only after an independent
    cross-check confirms it.
    """
    mcs = []
    for p in pairs:
        try:
            v = float(p.get("marketCap") or 0)
        except (TypeError, ValueError):
            v = 0.0
        if v > 0:
            mcs.append(v)
    if mcs:
        primary = mcs[0]
        if len(mcs) >= 3:
            ordered = sorted(mcs)
            median = ordered[len(ordered) // 2]
            if median > 0 and (primary > median * 3 or primary * 3 < median):
                return (median, False)
        return (primary, False)
    # No pair has a real circulating marketCap — fall back to the
    # deepest-liquidity pair's FDV, flagged, rather than a bare 0.0.
    for p in sorted(pairs, key=lambda p: (p.get("liquidity", {}) or {}).get("usd", 0) or 0,
                     reverse=True):
        try:
            fdv = float(p.get("fdv") or 0)
        except (TypeError, ValueError):
            fdv = 0.0
        if fdv > 0:
            return (fdv, True)
    return (0.0, False)

def _parse_dex_pairs(pairs: list, min_liquidity: int, token_address: str = "", prefer_chain: str = None) -> dict | None:
    """Parse a DexScreener pairs list into our internal dict. Returns None if unusable."""
    sup = [p for p in pairs if p.get("chainId","").lower() in SUPPORTED_CHAINS
           and (p.get("liquidity",{}).get("usd") or 0) >= min_liquidity]
    # priceUsd always describes baseToken.  A searched contract can also occur
    # as quoteToken; treating that pair as the call token silently tracks the
    # other asset (often SOL/WETH) and leaves the call stuck around 1x.
    if token_address:
        wanted = token_address.lower()
        sup = [p for p in sup if ((p.get("baseToken") or {}).get("address") or "").lower() == wanted]
    # WIZCHAIN_NOV23: when the caller knows the chain (e.g. the KOL's pair
    # link was on BSC), only pairs on that chain count. A multi-chain token
    # must not resolve to a different chain's pair (newest-pair guard /
    # ETH-first lookup). Safe fallback: if no pair matches, sup is unchanged.
    if prefer_chain:
        _pc = str(prefer_chain).lower()
        _on = [p for p in sup if (p.get("chainId") or "").lower() == _pc]
        if _on:
            sup = _on
    if not sup:
        return None
    # Cross-chain address collision guard: the exact same literal address can
    # exist as a completely unrelated pair on more than one EVM chain (vanity/
    # CREATE2 deployments, or plain coincidence). Sorting by liquidity alone
    # in that case can silently lock onto a totally different, much bigger
    # project on another chain — e.g. a fresh 435 MC KOL call getting matched
    # to an unrelated $1.5M pool elsewhere that happens to share this address.
    # A freshly-posted call is virtually always the NEWEST pool, so when
    # multiple different chains match, narrow down to the chain whose pool
    # was created most recently before ranking by liquidity within it.
    chains_seen = {p.get("chainId", "").lower() for p in sup}
    if len(chains_seen) > 1:
        dated = [p for p in sup if p.get("pairCreatedAt")]
        if dated:
            newest_chain = max(dated, key=lambda p: p.get("pairCreatedAt") or 0).get("chainId", "").lower()
            same_chain = [p for p in sup if p.get("chainId", "").lower() == newest_chain]
            if same_chain and len(same_chain) < len(sup):
                logger.warning(
                    f"⚠️ Cross-chain address collision for {token_address[:14]}… "
                    f"across {sorted(chains_seen)} — restricting to newest pool's "
                    f"chain ({newest_chain}) to avoid tracking the wrong project")
                sup = same_chain
    sup = sorted(sup, key=lambda p: p.get("liquidity",{}).get("usd",0) or 0, reverse=True)
    best = sup[0]
    chain  = SUPPORTED_CHAINS[best.get("chainId","").lower()]
    price  = float(best.get("priceUsd") or 0)
    mc, mc_is_fdv = _pick_market_cap(sup)
    symbol = best.get("baseToken",{}).get("symbol","")
    if price <= 0 and mc <= 0 and not symbol:
        return None
    price_change_h1  = float((best.get("priceChange") or {}).get("h1") or 0)
    price_change_h24 = float((best.get("priceChange") or {}).get("h24") or 0)
    volume_h24       = float((best.get("volume") or {}).get("h24") or 0)
    liquidity_usd    = float((best.get("liquidity") or {}).get("usd") or 0)
    txns_h24_buys    = int((best.get("txns") or {}).get("h24", {}).get("buys") or 0)
    txns_h24_sells   = int((best.get("txns") or {}).get("h24", {}).get("sells") or 0)
    listed_at        = best.get("pairCreatedAt")
    socials = best.get("info",{}).get("socials") or []
    tg_link = ""
    for s in socials:
        if "t.me" in (s.get("url","")) or "telegram" in (s.get("type","")).lower():
            tg_link = s.get("url",""); break
    return {
        "chain":           chain,
        "mcap":            mc,
        "mcap_fmt":        fmt_mc(mc) if mc > 0 else "N/A",
        "price":           price,
        "symbol":          symbol,
        "name":            best.get("baseToken",{}).get("name",""),
        "pair_addr":       best.get("pairAddress",""),
        "tg_link":         tg_link,
        "price_change_h1": price_change_h1,
        "price_change_h24":price_change_h24,
        "volume_h24":      volume_h24,
        "liquidity_usd":   liquidity_usd,
        "txns_buys":       txns_h24_buys,
        "txns_sells":      txns_h24_sells,
        "listed_at":       listed_at,
        "_partial":        (price <= 0 and mc <= 0),
        "_mc_is_fdv":      bool(mc_is_fdv and mc > 0),
    }

_dex_session = requests.Session()

_dex_miss_cache: dict = {}

_DEX_MISS_TTL = 3

_DEX_CACHE_MAX = 4000

_DEX_429_UNTIL = 0.0

_DEX_429_COOLDOWN = int(os.environ.get("DEX_429_COOLDOWN_SECONDS", "20") or 20)

def _dex_breaker_open() -> bool:
    return time.time() < _DEX_429_UNTIL

def _dex_trip_breaker():
    global _DEX_429_UNTIL
    _DEX_429_UNTIL = time.time() + _DEX_429_COOLDOWN

_DEX_RATE_LOCK = _threading.Lock()

_DEX_RATE_TIMESTAMPS = deque()

_DEX_MAX_REQUESTS_PER_MINUTE = max(1, int(os.environ.get("DEX_MAX_REQUESTS_PER_MINUTE", "260") or 260))

_DEX_STATS = {"ok": 0, "real429": 0, "budget_denied": 0, "breaker_denied": 0, "neterr": 0,
              "bulk_ok": 0, "single_ok": 0, "mon_ok": 0,
              "bulk_denied": 0, "single_denied": 0, "mon_denied": 0}

_DEX_STATS_AT = time.time()

_DEX_EFF_BUDGET = float(_DEX_MAX_REQUESTS_PER_MINUTE)

_DEX_LAST_429_AT = 0.0

_DEX_LAST_DEC_AT = 0.0

_DEX_LAST_ADJ_AT = time.time()

_DEX_BUDGET_FLOOR = 60

_DEX_ADAPTIVE = (os.environ.get("DEX_ADAPTIVE_BUDGET", "0") or "0").strip() == "1"

def _dex_stat(key: str):
    _DEX_STATS[key] = _DEX_STATS.get(key, 0) + 1

def _dex_stats_maybe_log():
    global _DEX_STATS_AT
    now = time.time()
    if now - _DEX_STATS_AT < 60:
        return
    snap = dict(_DEX_STATS)
    for k in list(_DEX_STATS):
        _DEX_STATS[k] = 0
    _DEX_STATS_AT = now
    logger.info(
        f"📊 DexScreener last min: ok={snap.get('ok',0)} real_429={snap.get('real429',0)} "
        f"budget_denied={snap.get('budget_denied',0)} breaker_denied={snap.get('breaker_denied',0)} "
        f"net_err={snap.get('neterr',0)} | budget={int(_DEX_EFF_BUDGET)}/{_DEX_MAX_REQUESTS_PER_MINUTE} "
        f"| lanes ok bulk={snap.get('bulk_ok',0)} single={snap.get('single_ok',0)} mon={snap.get('mon_ok',0)} "
        f"denied bulk={snap.get('bulk_denied',0)} single={snap.get('single_denied',0)} mon={snap.get('mon_denied',0)} "
        f"cooldown={_DEX_429_COOLDOWN}s adaptive={'on' if _DEX_ADAPTIVE else 'off'}")

def _dex_effective_budget() -> int:
    global _DEX_EFF_BUDGET, _DEX_LAST_ADJ_AT
    now = time.time()
    if (_DEX_EFF_BUDGET < _DEX_MAX_REQUESTS_PER_MINUTE
            and now - _DEX_LAST_429_AT > 60 and now - _DEX_LAST_ADJ_AT > 30):
        _DEX_EFF_BUDGET = min(float(_DEX_MAX_REQUESTS_PER_MINUTE),
                              _DEX_EFF_BUDGET + max(5.0, _DEX_MAX_REQUESTS_PER_MINUTE * 0.05))
        _DEX_LAST_ADJ_AT = now
    return int(_DEX_EFF_BUDGET)

def _dex_on_real_429():
    """Called when DexScreener itself returned HTTP 429."""
    global _DEX_EFF_BUDGET, _DEX_LAST_429_AT, _DEX_LAST_DEC_AT
    now = time.time()
    _DEX_LAST_429_AT = now
    if not _DEX_ADAPTIVE:
        return   # observe only - never lowers the request budget
    # several in-flight requests can 429 together - cut only once per 10s
    if now - _DEX_LAST_DEC_AT >= 10:
        _DEX_LAST_DEC_AT = now
        _DEX_EFF_BUDGET = max(float(_DEX_BUDGET_FLOOR), _DEX_EFF_BUDGET * 0.8)

_DEX_LANE_CV = _contextvars.ContextVar("dex_lane", default="single")

_DEX_LANE_TS = {"bulk": deque(), "single": deque(), "mon": deque()}

_DEX_MON_SHARE = float(os.environ.get("DEX_MON_SHARE", "0.25") or 0.25)

def _dex_rate_budget_ok(lane: str = None) -> bool:
    now = time.time()
    lane = lane or _DEX_LANE_CV.get()
    if lane not in _DEX_LANE_TS:
        lane = "single"
    budget = _dex_effective_budget()
    with _DEX_RATE_LOCK:
        for dq in _DEX_LANE_TS.values():
            while dq and now - dq[0] > 60:
                dq.popleft()
        total = sum(len(dq) for dq in _DEX_LANE_TS.values())
        if total >= budget:
            _dex_stat(lane + "_denied")
            return False
        # naye call ka instant track ("single" lane) ke liye ~12% budget HAMESHA reserved:
        # bulk/mon lanes use kabhi nahi chhoo sakte, taake dropped-call speed par asar na pade.
        if lane in ("bulk", "mon") and total >= budget - max(40, int(budget * 0.14)):   # 2026-10-26: 40 slots naye call ke liye reserved
            _dex_stat(lane + "_denied")
            return False
        if lane == "mon" and len(_DEX_LANE_TS["mon"]) >= max(8, int(budget * _DEX_MON_SHARE)):
            _dex_stat("mon_denied")
            return False
        _DEX_LANE_TS[lane].append(now)
        _DEX_RATE_TIMESTAMPS.append(now)          # purana counter (compat)
        while _DEX_RATE_TIMESTAMPS and now - _DEX_RATE_TIMESTAMPS[0] > 60:
            _DEX_RATE_TIMESTAMPS.popleft()
        _dex_stat(lane + "_ok")
        return True

_BIRDEYE_RATE_LOCK = _threading.Lock()

_BIRDEYE_RATE_TIMESTAMPS = deque()

_BIRDEYE_MAX_REQUESTS_PER_MINUTE = max(1, int(os.environ.get("BIRDEYE_MAX_REQUESTS_PER_MINUTE", "50") or 50))

def _birdeye_rate_budget_ok() -> bool:
    now = time.time()
    with _BIRDEYE_RATE_LOCK:
        while _BIRDEYE_RATE_TIMESTAMPS and now - _BIRDEYE_RATE_TIMESTAMPS[0] > 60:
            _BIRDEYE_RATE_TIMESTAMPS.popleft()
        if len(_BIRDEYE_RATE_TIMESTAMPS) >= _BIRDEYE_MAX_REQUESTS_PER_MINUTE:
            return False
        _BIRDEYE_RATE_TIMESTAMPS.append(now)
        return True

def _dex_cache_gc():
    """Evict expired / overflowing cache entries (prevents unbounded RAM growth)."""
    import time as _time
    now = _time.time()
    for cache in (_dex_cache, _dex_miss_cache):
        if len(cache) < _DEX_CACHE_MAX:
            continue
        for k in [k for k, v in list(cache.items())
                  if (v[1] if isinstance(v, tuple) else v) < now]:
            cache.pop(k, None)
        while len(cache) > _DEX_CACHE_MAX:
            try:
                cache.pop(next(iter(cache)))
            except StopIteration:
                break

def _dex_get(url: str, timeout: int = 5, fresh: bool = False, lane: str = None):
    """GET with shared session. fresh=True bypasses intermediary HTTP caches."""
    try:
        _dex_stats_maybe_log()
    except Exception:
        pass
    if _dex_breaker_open():
        _dex_stat("breaker_denied")
        return 429  # breaker open: request bhejne ke baghair rate-limited batao
    if not _dex_rate_budget_ok(lane):
        _dex_stat("budget_denied")
        return 429  # apna hi budget khatam — DexScreener ko hit kiye baghair fallback pe jao
    try:
        headers = {"Cache-Control": "no-cache", "Pragma": "no-cache"} if fresh else None
        if fresh:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}_ts={int(time.time() * 1000)}"
        resp = _dex_session.get(url, headers=headers, timeout=timeout)
    except Exception as e:
        _dex_stat("neterr")
        logger.warning(f"DexScreener network error ({url.split('/')[-1][:16]}…): {e}")
        return None
    if resp.status_code == 429:
        _dex_stat("real429")
        _dex_on_real_429()
        _dex_trip_breaker()
        return 429
    if resp.status_code != 200:
        return None
    _dex_stat("ok")
    try:
        return resp.json()
    except Exception:
        return None

def _dex_candidate_chains(ca: str) -> list:
    """Which DexScreener chains can this address exist on? Keeps the
    chain-scoped lookup to 1-4 cheap requests instead of brute-forcing."""
    # len==42: standard EVM (0x+40hex). len==66: Robinhood/"RH" 32-byte
    # address format (0x+64hex) seen on DexScreener chart links — without
    # this, a correctly-extracted Robinhood CA still failed here and fell
    # through to being queried as if it were a Solana address.
    if ca.startswith("0x") and len(ca) in (42, 66):
        return ["ethereum", "bsc", "base", "robinhood"]
    if ca.startswith(("EQ", "UQ")):
        return ["ton"]
    return ["solana"]

_pair_token_cache: dict = {}

_PAIR_RESOLVE_NEG_TTL = 90

def _resolve_pair_to_token(addr: str):
    """Return the baseToken mint for a pair/pool address, else None."""
    if not addr:
        return None
    cached = _pair_token_cache.get(addr)
    if cached is not None:
        # WIZCHAIN_NOV23: cache is (token, chain_slug, expires_at); old
        # 2-tuple entries (token, expires_at) are still honoured.
        if len(cached) == 3:
            token_c, chain_c, expires_at = cached
        else:
            token_c, expires_at = cached
            chain_c = None
        if token_c or time.time() < expires_at:
            return (token_c, chain_c)
        # negative result expired — fall through and try again
    token = None
    pair_slug = None  # WIZCHAIN_NOV23: DexScreener slug where the pair lives
    try:
        for slug in (_dex_candidate_chains(addr) or []):
            d = _dex_get(f"https://api.dexscreener.com/latest/dex/pairs/{slug}/{addr}",
                         timeout=8)
            if d == 429:
                break
            if not isinstance(d, dict):
                continue
            prs = d.get("pairs") or ([d.get("pair")] if d.get("pair") else [])
            for p in prs:
                if not isinstance(p, dict):
                    continue
                base = ((p.get("baseToken") or {}).get("address") or "").strip()
                if base and base.lower() != addr.lower():
                    token = base
                    pair_slug = slug  # WIZCHAIN_NOV23: remember the pair's chain
                    break
            if token:
                break
    except Exception as e:
        logger.debug(f"pair→token resolve failed {addr[:12]}…: {e}")
    # FIX (2026-09-25, maybac_whales RH pair-link calls stuck dex_pending
    # forever incident): logs showed several Robinhood-chain calls where the
    # KOL only posted a dexscreener.com/robinhood/<pairAddress> link (no
    # separate contract line) sitting at dex_pending=True for 30-60+ minutes
    # straight and never resolving — silently excluded from tracking,
    # Leaderboard and Champions the whole time even though other chains'
    # pair-links resolved in seconds. DexScreener's OWN /latest/dex/pairs/
    # <chain>/<addr> endpoint (above) can badly lag or never index a
    # brand-new Robinhood-chain pool. GeckoTerminal indexes the "robinhood"
    # network independently (already relied on elsewhere in this file for
    # RH-chain cross-checks) and exposes the base token on a plain pool
    # lookup, so it is now tried as a second, independent resolver instead
    # of leaving the call stuck forever on DexScreener alone.
    if not token:
        _GT_PAIR_SLUG = {"robinhood": "robinhood", "solana": "solana", "ethereum": "eth",
                         "bsc": "bsc", "base": "base", "ton": "ton", "arc": "arc"}
        try:
            for slug in (_dex_candidate_chains(addr) or []):
                net = _GT_PAIR_SLUG.get(slug)
                if not net:
                    continue
                gd = _gt_get(f"https://api.geckoterminal.com/api/v2/networks/{net}/pools/{addr}")
                base_id = ((((gd or {}).get("data") or {}).get("relationships") or {})
                           .get("base_token") or {}).get("data", {}).get("id", "") or ""
                if "_" in base_id:
                    base_addr = base_id.split("_", 1)[1].strip()
                    if base_addr and base_addr.lower() != addr.lower():
                        token = base_addr
                        pair_slug = slug  # WIZCHAIN_NOV23: remember the pair's chain
                        logger.info(f"🔗 Pair address resolved via GeckoTerminal (DexScreener "
                                    f"had no pool data): {addr[:10]}… → token {token[:10]}…")
                        break
        except Exception as e:
            logger.debug(f"pair→token GT resolve failed {addr[:12]}…: {e}")
    if len(_pair_token_cache) > 4000:
        _pair_token_cache.clear()
    # Positive resolves are permanent (a pair's base token never changes).
    # Negative resolves expire quickly so a too-early attempt gets retried
    # once DexScreener/GeckoTerminal actually indexes the pair.
    # WIZCHAIN_NOV23: cache now stores (token, chain_slug, expires_at).
    _pair_token_cache[addr] = (token, pair_slug, time.time() + _PAIR_RESOLVE_NEG_TTL)
    return (token, pair_slug)

_GT_NET   = {"SOL": "solana", "ETH": "eth", "BNB": "bsc", "BASE": "base", "TON": "ton", "ARC": "arc"}

_DP_NET   = {"SOL": "solana", "ETH": "ethereum", "BNB": "bsc", "BASE": "base", "TON": "ton", "ARC": "arc"}

_BE_CHAIN = {"SOL": "solana", "ETH": "ethereum", "BNB": "bsc", "BASE": "base"}

_MO_EVM   = {"ETH": "eth", "BNB": "bsc", "BASE": "base", "ARC": "arc"}

_SLUG_TO_DISP = {"solana": "SOL", "ethereum": "ETH", "eth": "ETH", "bsc": "BNB",
                 "base": "BASE", "ton": "TON", "robinhood": "RH", "arc": "ARC"}

def _alt_result(chain, symbol, price, mcap, name="", source="", mc_is_fdv=False):
    """Build a dict in the exact shape _parse_dex_pairs() returns.

    mc_is_fdv: True when `mcap` was only obtainable via a fully-diluted-value
    fallback (no real circulating marketCap field was available from this
    provider). FDV is computed off max/total supply, not circulating supply,
    and can read 10-100x+ higher than the token's real market cap for tokens
    with large locked/uncirculated allocations — this is what caused fresh
    listings (e.g. Robinhood-chain tokens) to show as millions in MC when the
    caller's real entry was only thousands. Callers that need entry-MC
    accuracy (e.g. _reconcile_entry_mc) use this flag to avoid trusting an
    FDV-derived number over the KOL's own stated MC.
    """
    try:
        price = float(price or 0)
    except Exception:
        price = 0.0
    try:
        mcap = float(mcap or 0)
    except Exception:
        mcap = 0.0
    if price <= 0 and mcap <= 0 and not symbol:
        return None
    dexpath = CHAIN_TO_DEXPATH.get(chain, "solana") if "CHAIN_TO_DEXPATH" in globals() else "solana"
    return {
        "chain":            chain,
        "mcap":             mcap,
        "mcap_fmt":         fmt_mc(mcap) if mcap > 0 else "N/A",
        "price":            price,
        "symbol":           symbol or "",
        "name":             name or symbol or "",
        "pair_addr":        "",
        "tg_link":          "",
        "price_change_h1":  0.0,
        "price_change_h24": 0.0,
        "volume_h24":       0.0,
        "liquidity_usd":    0.0,
        "txns_buys":        0,
        "txns_sells":       0,
        "listed_at":        None,
        "_partial":         (price <= 0 and mcap <= 0),
        "_source":          source or "alt",
        "_dexpath":         dexpath,
        "_mc_is_fdv":       bool(mc_is_fdv and mcap > 0),
    }

def _alt_chains_for(ca: str) -> list:
    """Display-chain candidates for a contract address."""
    return [_SLUG_TO_DISP.get(s, "SOL") for s in _dex_candidate_chains(ca)
            if s in _SLUG_TO_DISP]

def _src_geckoterminal(ca: str, chains: list):
    for ch in chains:
        net = _GT_NET.get(ch)
        if not net:
            continue
        try:
            r = _dex_session.get(
                f"https://api.geckoterminal.com/api/v2/networks/{net}/tokens/{ca}",
                headers={"Accept": "application/json"}, timeout=8)
            if r.status_code != 200:
                continue
            a = (r.json().get("data") or {}).get("attributes") or {}
            price   = float(a.get("price_usd") or 0)
            mc_real = float(a.get("market_cap_usd") or 0)
            mc_fdv  = float(a.get("fdv_usd") or 0)
            mc      = mc_real or mc_fdv
            res   = _alt_result(ch, a.get("symbol") or "", price, mc,
                                a.get("name") or "", "geckoterminal",
                                mc_is_fdv=(mc_real <= 0 and mc_fdv > 0))
            if res:
                return res
        except Exception as e:
            logger.debug(f"GeckoTerminal source failed ({ch}): {e}")
    return None

def _src_dexpaprika(ca: str, chains: list):
    for ch in chains:
        net = _DP_NET.get(ch)
        if not net:
            continue
        try:
            r = _dex_session.get(
                f"https://api.dexpaprika.com/networks/{net}/tokens/{ca}",
                headers={"Accept": "application/json"}, timeout=8)
            if r.status_code != 200:
                continue
            d  = r.json() or {}
            sm = d.get("summary") or {}
            price   = float(sm.get("price_usd") or d.get("price_usd") or 0)
            mc_real = float(d.get("market_cap_usd") or sm.get("market_cap_usd") or 0)
            mc_fdv  = float(d.get("fdv") or 0)
            mc      = mc_real or mc_fdv
            res = _alt_result(ch, d.get("symbol") or "", price, mc,
                              d.get("name") or "", "dexpaprika",
                              mc_is_fdv=(mc_real <= 0 and mc_fdv > 0))
            if res:
                return res
        except Exception as e:
            logger.debug(f"DexPaprika source failed ({ch}): {e}")
    return None

def _src_birdeye(ca: str, chains: list):
    if not BIRDEYE_API_KEY:
        return None
    for ch in chains:
        bchain = _BE_CHAIN.get(ch)
        if not bchain:
            continue
        if not _birdeye_rate_budget_ok():
            continue  # account-wide budget used up this minute — skip, try next chain/next tick
        try:
            r = _dex_session.get(
                "https://public-api.birdeye.so/defi/token_overview",
                params={"address": ca},
                headers={"X-API-KEY": BIRDEYE_API_KEY, "x-chain": bchain,
                         "accept": "application/json"},
                timeout=8)
            if r.status_code != 200:
                continue
            d = (r.json() or {}).get("data") or {}
            if not d:
                continue
            price   = float(d.get("price") or 0)
            mc_real = float(d.get("marketCap") or d.get("mc") or d.get("realMc") or 0)
            mc_fdv  = float(d.get("fdv") or 0)
            mc      = mc_real or mc_fdv
            res = _alt_result(ch, d.get("symbol") or "", price, mc,
                              d.get("name") or "", "birdeye",
                              mc_is_fdv=(mc_real <= 0 and mc_fdv > 0))
            if res:
                return res
        except Exception as e:
            logger.debug(f"Birdeye source failed ({ch}): {e}")
    return None

def _src_moralis(ca: str, chains: list):
    if not MORALIS_API_KEY:
        return None
    hdr = {"X-API-Key": MORALIS_API_KEY, "accept": "application/json"}
    for ch in chains:
        try:
            if ch == "SOL":
                r = _dex_session.get(
                    f"https://solana-gateway.moralis.io/token/mainnet/{ca}/price",
                    headers=hdr, timeout=8)
                if r.status_code != 200:
                    continue
                d = r.json() or {}
                price = float(d.get("usdPrice") or 0)
                sym   = ""
                mc    = 0.0
                try:
                    rm = _dex_session.get(
                        f"https://solana-gateway.moralis.io/token/mainnet/{ca}/metadata",
                        headers=hdr, timeout=8)
                    if rm.status_code == 200:
                        m   = rm.json() or {}
                        sym = m.get("symbol") or ""
                        supply = float(m.get("supply") or 0)
                        dec    = int(m.get("decimals") or 0) if str(m.get("decimals") or "").isdigit() else 0
                        if supply > 0 and price > 0:
                            if dec and supply > 10 ** 12:
                                supply = supply / (10 ** dec)
                            mc = supply * price
                except Exception:
                    pass
                res = _alt_result("SOL", sym, price, mc, sym, "moralis")
                if res:
                    return res
            else:
                evm = _MO_EVM.get(ch)
                if not evm:
                    continue
                r = _dex_session.get(
                    f"https://deep-index.moralis.io/api/v2.2/erc20/{ca}/price",
                    params={"chain": evm}, headers=hdr, timeout=8)
                if r.status_code != 200:
                    continue
                d = r.json() or {}
                price = float(d.get("usdPrice") or 0)
                sym   = (d.get("tokenSymbol") or "")
                res = _alt_result(ch, sym, price, 0, d.get("tokenName") or sym, "moralis")
                if res:
                    return res
        except Exception as e:
            logger.debug(f"Moralis source failed ({ch}): {e}")
    return None

def _src_coinmarketcap(ca: str, chains: list):
    if not CMC_API_KEY:
        return None
    hdr = {"X-CMC_PRO_API_KEY": CMC_API_KEY, "Accept": "application/json"}
    try:
        r = _dex_session.get(
            "https://pro-api.coinmarketcap.com/v2/cryptocurrency/info",
            params={"address": ca}, headers=hdr, timeout=10)
        if r.status_code != 200:
            return None
        data = (r.json() or {}).get("data") or {}
        if not data:
            return None
        cid, info = next(iter(data.items()))
        if isinstance(info, list):
            info = info[0] if info else {}
        sym  = info.get("symbol") or ""
        name = info.get("name") or ""
        rq = _dex_session.get(
            "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest",
            params={"id": cid}, headers=hdr, timeout=10)
        price = mc = 0.0
        if rq.status_code == 200:
            q = ((rq.json() or {}).get("data") or {}).get(str(cid)) or {}
            if isinstance(q, list):
                q = q[0] if q else {}
            usd   = (q.get("quote") or {}).get("USD") or {}
            price = float(usd.get("price") or 0)
            mc    = float(usd.get("market_cap") or usd.get("fully_diluted_market_cap") or 0)
        ch = (chains or ["SOL"])[0]
        return _alt_result(ch, sym, price, mc, name, "coinmarketcap")
    except Exception as e:
        logger.debug(f"CoinMarketCap source failed: {e}")
    return None

def _fetch_alt_sources_sync(ca: str):
    """DexScreener ke fail hone par baaki sources try karo (order = fastest first)."""
    if not ca or not EXTRA_SOURCES_ENABLED:
        return None
    chains = _alt_chains_for(ca) or ["SOL"]
    # Birdeye first ONLY for chain-unambiguous addresses (SOL/TON) — it's a
    # paid, independently-indexed source and far more reliable than the free
    # geckoterminal/dexpaprika endpoints (which often return mc=N/A for
    # fresh/low-cap tokens). For EVM (0x...) addresses the chain itself is a
    # guess across ETH/BSC/Base/Robinhood, and Birdeye just takes the first
    # chain with any data — which can be a wrong/unrelated token on a
    # different chain with a very different MC. So for those, try the
    # chain-neutral sources first and treat Birdeye as a last resort there.
    is_evm_ambiguous = ca.startswith("0x") and len(ca) in (42, 66)
    order = ((_src_geckoterminal, _src_dexpaprika, _src_birdeye,
              _src_moralis, _src_coinmarketcap) if is_evm_ambiguous else
             (_src_birdeye, _src_geckoterminal, _src_dexpaprika,
              _src_moralis, _src_coinmarketcap))
    # SPEED_NOV12: all alt sources start at once (was: strictly sequential,
    # so a slow first source blocked every later one — up to 5 x 8s = 40s).
    # Results are still taken in the same priority order: the first source
    # (by priority) with data wins, exactly as before. Only the wall time
    # changes, capped by a shared 10s deadline. Stragglers are left to
    # finish in the background (shutdown(wait=False)) so an early hit is
    # never delayed by a slow lower-priority source.
    try:
        import concurrent.futures as _cf
        _deadline = time.time() + 10
        _ex = _cf.ThreadPoolExecutor(max_workers=len(order))
        try:
            _futs = [_ex.submit(fn, ca, chains) for fn in order]
            for _fn, _f in zip(order, _futs):
                try:
                    res = _f.result(timeout=max(0.1, _deadline - time.time()))
                except Exception as e:
                    logger.debug(f"alt source {getattr(_fn, '__name__', '?')} error: {e}")
                    res = None
                if res and (res.get("price", 0) > 0 or res.get("mcap", 0) > 0 or res.get("symbol")):
                    logger.info(f"🛰️ Fallback source {res.get('_source')} hit for {ca[:12]}… "
                                f"({res.get('symbol','?')} mc={res.get('mcap_fmt','N/A')})")
                    return res
        finally:
            _ex.shutdown(wait=False)
    except Exception as e:
        logger.debug(f"alt sources parallel run failed: {e}")
    return None

_ENTRY_SHARED = {}

_ENTRY_LOCKS = {}

_ENTRY_SHARED_TTL = float(os.environ.get("ENTRY_SHARED_TTL_SECONDS", "12") or 12)

_ENTRY_SHARED_GLOBAL = _threading.Lock()

def _fetch_entry_sync(ca: str, chain_guess: str = "SOL", min_liquidity: int = 0) -> dict | None:
    key = (ca or "").lower()
    if not key:
        return _fetch_entry_sync_uncached(ca, chain_guess, min_liquidity)

    def _fresh():
        hit = _ENTRY_SHARED.get(key)
        if hit and time.time() - hit[0] < _ENTRY_SHARED_TTL and hit[1]:
            return dict(hit[1])
        return None

    r = _fresh()
    if r:
        return r
    with _ENTRY_SHARED_GLOBAL:
        lock = _ENTRY_LOCKS.get(key)
        if lock is None:
            lock = _ENTRY_LOCKS[key] = _threading.Lock()
        if len(_ENTRY_SHARED) > 300:
            _cut = time.time() - 120
            for _k in [k for k, v in list(_ENTRY_SHARED.items()) if v[0] < _cut]:
                _ENTRY_SHARED.pop(_k, None)
                _ENTRY_LOCKS.pop(_k, None)
    with lock:
        r = _fresh()
        if r:
            return r
        res = _fetch_entry_sync_uncached(ca, chain_guess, min_liquidity)
        if res and (res.get("mcap") or res.get("price")):
            _ENTRY_SHARED[key] = (time.time(), dict(res))
        return res

def _fetch_entry_sync_uncached(ca: str, chain_guess: str = "SOL", min_liquidity: int = 0) -> dict | None:
    """Entry-price fetch used ONLY for locking in a NEW CALL's entry MC.

    Birdeye-first is safe ONLY for unambiguous single-chain addresses (SOL,
    TON) — there's only one place the token can live, so whatever Birdeye
    returns for that address is correct.

    EVM (0x...) addresses are different: the SAME hex address format is
    valid on Ethereum, BSC, Base AND Robinhood simultaneously, so we have to
    GUESS which of those 4 chains the token is actually on
    (_dex_candidate_chains tries all 4). Birdeye (and geckoterminal/
    dexpaprika) just return the first chain that has ANY data for that
    address — if the same address coincidentally exists as an unrelated
    contract on a different EVM chain, that wrong chain's price/MC gets
    used (this is what produced the 111K-vs-real-22K case). DexScreener
    does NOT have this problem: it validates real liquidity/pairs per chain
    instead of blindly trusting the first hit, so for EVM addresses it stays
    primary. Birdeye is still used — as the alt-source fallback inside
    _fetch_dex_sync — for tokens DexScreener genuinely has nothing on.
    """
    if not ca:
        return None
    is_evm_ambiguous = ca.startswith("0x") and len(ca) in (42, 66)
    if not is_evm_ambiguous:
        # Single-chain address (SOL/TON) — Birdeye first is safe and avoids
        # DexScreener's rate-limit problem entirely.
        chains = _alt_chains_for(ca) or [chain_guess]
        try:
            be_res = _src_birdeye(ca, chains)
        except Exception as e:
            logger.debug(f"Birdeye entry-fetch error: {e}")
            be_res = None
        if be_res and (be_res.get("mcap", 0) > 0 or be_res.get("price", 0) > 0):
            return be_res
    # EVM addresses (chain ambiguous), or Birdeye had nothing yet — use the
    # normal DexScreener + alt-source waterfall, which resolves the chain
    # correctly via real liquidity/pairs.
    try:
        return _fetch_dex_sync(ca, 1, min_liquidity)
    except Exception as e:
        logger.debug(f"DexScreener entry-fetch fallback error: {e}")
        return None

def _better_mc_result(a: dict | None, b: dict | None) -> dict | None:
    """Pick the more trustworthy of two dex-shaped MC reads.

    A real (non-FDV) marketCap always beats an FDV-derived one, regardless of
    which is numerically higher — FDV can be 10-100x+ inflated relative to
    real circulating MC. Within the same tier (both real, or both FDV-only),
    the higher reading wins: indexing lag only ever biases a read LOW, never
    high, so taking the higher of two same-tier reads can never inflate the
    caller's own X in their favor.
    """
    a_mc = (a or {}).get("mcap", 0) or 0
    b_mc = (b or {}).get("mcap", 0) or 0
    if a_mc <= 0:
        return b
    if b_mc <= 0:
        return a
    a_fdv = bool((a or {}).get("_mc_is_fdv"))
    b_fdv = bool((b or {}).get("_mc_is_fdv"))
    if a_fdv and not b_fdv:
        return b
    if b_fdv and not a_fdv:
        return a
    return a if a_mc >= b_mc else b

def _confirm_mc_cross_check(ca: str, chain_guess: str = "SOL") -> dict | None:
    """Re-check a token's MC against an INDEPENDENT provider, not just DexScreener
    asked twice. DexScreener's own indexer can still be settling well after our
    first read, so re-querying only DexScreener risks repeating the exact same
    stale (or FDV-only) number. Returns a dex-shaped dict (mcap/price/symbol)
    or None.

    EVM (0x...) addresses are chain-ambiguous (same address format valid on
    ETH/BSC/Base/Robinhood) — a chain-guessing alt source just returns the
    first chain with any data for that address, which can be a totally
    unrelated token on a different chain with a wildly different MC.
    DexScreener resolves the chain correctly (real liquidity/pairs), so for
    EVM addresses its number is trusted UNLESS it has nothing real to offer
    either — no data at all, OR only an FDV fallback (see _pick_market_cap),
    which is exactly the case this cross-check exists to correct.

    FIX (RH-chain cross-check no-op, 2026-09-24): Birdeye (_BE_CHAIN) has no
    entry at all for "RH" — Birdeye simply does not index the Robinhood
    chain — so `be_res` was always None for every Robinhood-chain call, and
    this whole function silently did nothing but re-ask DexScreener the same
    question it had just asked. GeckoTerminal DOES index the "robinhood"
    network (_GT_NET / _GECKO_NET), so it is now tried as well whenever
    Birdeye comes back empty, giving RH-chain calls an actual independent
    second opinion for the first time.
    """
    dex_res = None
    alt_res = None
    # FIX (2026-10-11, CENTRY "278K shown, real 400K"): while DexScreener was rate-limited
    # (429 breaker open) _fetch_dex_sync() silently answered from its alt sources — mostly a
    # lagging GeckoTerminal quote — and this function then trusted that number as
    # "real, chain-resolved DexScreener MC", so the cross-check just confirmed the stale value.
    # allow_alt=False => dex_res is ONLY ever a genuine DexScreener answer (None under 429).
    # SPEED_NOV12: DexScreener + Birdeye + GeckoTerminal now run concurrently
    # (was: DexScreener first, then the two alts in parallel). Same three
    # sources, same selection logic below (incl. the EVM live-entry guard) —
    # only the wall time changes: a read costs max(source times) instead of
    # dex + max(alt times).
    _alt_cands = []
    try:
        import concurrent.futures as _cf
        _chs = _alt_chains_for(ca) or [chain_guess]
        with _cf.ThreadPoolExecutor(max_workers=3) as _ex:
            _f_dex = _ex.submit(_fetch_dex_sync, ca, 1, 0, False)
            _f_be = _ex.submit(_src_birdeye, ca, _chs)
            _f_gt = _ex.submit(_src_geckoterminal, ca, _chs)
            try:
                dex_res = _f_dex.result()
            except Exception:
                dex_res = None
            for _f in (_f_be, _f_gt):
                try:
                    _r = _f.result(timeout=1)  # GUARD_2SEC_NOV19
                    if _r:
                        _alt_cands.append(_r)
                except Exception:
                    pass
    except Exception:
        pass
    # Provider lag only ever biases a quote LOW, so of the independent alt sources keep the
    # highest real (non-FDV) MC; fall back to any non-empty answer.
    _real = [r for r in _alt_cands if (r.get("mcap", 0) or 0) > 0 and not r.get("_mc_is_fdv")]
    if _real:
        alt_res = max(_real, key=lambda r: r.get("mcap", 0) or 0)
    else:
        alt_res = next((r for r in _alt_cands if (r.get("mcap", 0) or 0) > 0), None) or (_alt_cands[0] if _alt_cands else None)
    dex_mc = (dex_res or {}).get("mcap", 0) or 0
    dex_is_fdv = bool((dex_res or {}).get("_mc_is_fdv"))
    is_evm_ambiguous = ca.startswith("0x") and len(ca) in (42, 66)
    if is_evm_ambiguous:
        if dex_mc > 0 and not dex_is_fdv:
            # LIVE_ENTRY_GUARD_NOV12: DexScreener can still be stale/LOW for
            # a fresh EVM pair (e.g. $JAGENT locked 1.4K when the real call
            # MC was 22K). Before trusting its real MC outright, check the
            # independent alt sources for the SAME chain AND SAME symbol.
            # A wrong-chain alt (same 0x address, unrelated token) must
            # never inflate the entry, so both must match; otherwise the
            # DexScreener answer stands (fail closed).
            _dex_chain = (dex_res or {}).get("chain") or chain_guess
            _dex_sym = str((dex_res or {}).get("symbol") or "").strip().upper()
            _best_alt = None
            for _cand in (_alt_cands or []):
                _c_mc = (_cand or {}).get("mcap", 0) or 0
                if _c_mc <= 0 or bool((_cand or {}).get("_mc_is_fdv")):
                    continue  # only a real (non-FDV) alt may override
                if str((_cand or {}).get("chain") or "") != str(_dex_chain or ""):
                    continue  # wrong chain - unrelated token, ignore
                _c_sym = str((_cand or {}).get("symbol") or "").strip().upper()
                if not _dex_sym or not _c_sym or _c_sym != _dex_sym:
                    continue  # symbol must match on both sides
                if _best_alt is None or _c_mc > (_best_alt.get("mcap", 0) or 0):
                    _best_alt = _cand
            if _best_alt is not None:
                _alt_mc = (_best_alt.get("mcap", 0) or 0)
                if _alt_mc > dex_mc * 2:
                    logger.info(
                        f"Live-entry guard: DexScreener MC {fmt_mc(dex_mc)} looks stale "
                        f"vs {_best_alt.get('_source', 'alt')} {fmt_mc(_alt_mc)} "
                        f"(chain={_dex_chain} symbol={_dex_sym}) - using the higher read")
                    return _best_alt
            # ENTRY_DUST_NOV20 ($OWED: entry locked $3 while the token traded
            # at $31K): DexScreener can lag minutes behind on a fresh pair and
            # report the pre-pump dust price as a real MC. Prices are
            # comparable across sources without supply assumptions, so if an
            # independent alt source reports a price >2x away from the
            # DexScreener price for the SAME chain+symbol, the Dex read is
            # stale - flag it so callers treat it as unconfirmed instead of
            # locking it in as a confident entry. The alt MC is NOT
            # substituted here (it may be FDV-derived); only the flag is set.
            _dex_price = float((dex_res or {}).get("price", 0) or 0)
            for _cand in (_alt_cands or []):
                if str((_cand or {}).get("chain") or "") != str(_dex_chain or ""):
                    continue
                _c_sym = str((_cand or {}).get("symbol") or "").strip().upper()
                if not _dex_sym or not _c_sym or _c_sym != _dex_sym:
                    continue
                _c_price = float((_cand or {}).get("price", 0) or 0)
                if _dex_price > 0 and _c_price > 0 and max(_dex_price, _c_price) / min(_dex_price, _c_price) > 2:
                    logger.warning(
                        f"Dust-price guard: DexScreener price {_dex_price} disagrees "
                        f"with {_cand.get('_source', 'alt')} price {_c_price} "
                        f"(chain={_dex_chain} symbol={_dex_sym}) - flagging read stale")
                    _stale = dict(dex_res)
                    _stale["_price_stale"] = True
                    return _stale
            return dex_res  # real, chain-resolved circulating MC — trust it outright
        return _better_mc_result(alt_res, dex_res)
    return _better_mc_result(dex_res, alt_res)

def _fetch_dex_sync(ca, retries=2, min_liquidity=500, allow_alt=True, prefer_chain=None):
    """Fetch live token data from DexScreener.

    Order of operations:
      1. Fresh positive cache hit (< 20s)  -> return instantly
      2. Fresh negative cache hit (< 45s)  -> return None without a request
      3. /latest/dex/tokens/{ca}           -> primary, all chains
      4. /latest/dex/search?q={ca}         -> fallback for freshly-created pairs
      5. 429 -> exponential backoff, then retry
    """
    import time as _time
    if not ca:
        return None

    cached = _dex_cache.get(ca)
    if cached:
        result, exp = cached
        if _time.time() < exp:
            return result

    miss_exp = _dex_miss_cache.get(ca)
    if miss_exp and _time.time() < miss_exp:
        return None

    for attempt in range(max(1, retries)):
        data = _dex_get(f"https://api.dexscreener.com/latest/dex/tokens/{ca}")
        if data == 429:
            # Rate-limited (breaker _dex_get me trip ho chuka hai). DexScreener
            # ko sleep kar ke dobara MAT bajao — wahi backoff storms thay jo
            # ticks wedge kar ke calls skip kara rahe thay. Seedha doosri dex
            # sources se data lo.
            logger.warning(f"DexScreener 429 (ca={ca[:12]}…) — alt dex sources try ho rahi hain")
            alt429 = _fetch_alt_sources_sync(ca) if allow_alt else None
            if alt429:
                _dex_cache[ca] = (alt429, _time.time() + _DEX_CACHE_TTL)
                _dex_miss_cache.pop(ca, None)
                _dex_cache_gc()
                return alt429
            # Alt bhi fail — 5s negative cache taake koi storm na bane.
            _dex_miss_cache[ca] = _time.time() + 5
            _dex_cache_gc()
            return None

        pairs = (data or {}).get("pairs") or []

        # Fallback: brand-new pairs sometimes only show on the search endpoint.
        if not pairs:
            sdata = _dex_get(f"https://api.dexscreener.com/latest/dex/search?q={ca}")
            if sdata and sdata != 429:
                pairs = [
                    p for p in (sdata.get("pairs") or [])
                    if ca.lower() in (
                        (p.get("baseToken", {}).get("address", "") or "").lower(),
                        (p.get("quoteToken", {}).get("address", "") or "").lower(),
                    )
                ]

        if pairs:
            result = _parse_dex_pairs(pairs, min_liquidity, ca, prefer_chain=prefer_chain)
            if result is None:
                # Pairs exist but all below the liquidity floor — retry without it
                # so the UI still shows a live price instead of "N/A".
                result = _parse_dex_pairs(pairs, 0, ca, prefer_chain=prefer_chain)
        else:
            result = None

        # The /latest endpoint caps its answer at 30 pairs, so a multi-chain
        # token can come back with zero pairs on a chain we support. Ask the
        # chain-scoped v1 endpoint directly for the chains this CA can live on.
        if not result:
            scoped = []
            for chain_slug in _dex_candidate_chains(ca):
                cdata = _dex_get(f"https://api.dexscreener.com/tokens/v1/{chain_slug}/{ca}", timeout=10)
                if cdata == 429:
                    break
                if isinstance(cdata, list) and cdata:
                    scoped.extend(cdata)
            if scoped:
                result = (_parse_dex_pairs(scoped, min_liquidity, ca, prefer_chain=prefer_chain)
                          or _parse_dex_pairs(scoped, 0, ca, prefer_chain=prefer_chain))

        if True:
            if result:
                _dex_cache[ca] = (result, _time.time() + _DEX_CACHE_TTL)
                _dex_miss_cache.pop(ca, None)
                _dex_cache_gc()
                return result

        # ── PAIR-ADDRESS FALLBACK ────────────────────────────────────────────
        # Bohat se KOLs (jaise @Big_Whales_Call) dexscreener ka PAIR/POOL link
        # post karte hain: dexscreener.com/solana/<pairAddress>. Wo address
        # token mint nahi hota, is liye /latest/dex/tokens/<addr> khali aata
        # tha aur poori call "no dex data yet" keh kar skip ho jati thi.
        # Yahan us address ko pairs endpoint se resolve kar ke asli baseToken
        # mint nikalte hain, phir usi se normal lookup chalta hai.
        if not result:
            # WIZCHAIN_NOV23: the resolver now returns (token, chain_slug).
            # The pair's chain is the call's chain — pass it down so the
            # token lookup cannot wander off to another chain's pair.
            _tok, _pair_slug = _resolve_pair_to_token(ca)
            if _tok and _tok.lower() != (ca or "").lower():
                logger.info(f"🔗 Pair address resolved: {ca[:10]}… → token {_tok[:10]}…")
                result = _fetch_dex_sync(_tok, retries=1,
                                         min_liquidity=min_liquidity,
                                         allow_alt=allow_alt,
                                         prefer_chain=_pair_slug or prefer_chain)
                if result:
                    result["resolved_ca"] = _tok

        if result:
            _dex_cache[ca] = (result, _time.time() + _DEX_CACHE_TTL)
            _dex_miss_cache.pop(ca, None)
            _dex_cache_gc()
            return result

        # DexScreener has nothing — try the slower alternate providers only on
        # normal lookup/new-call paths. The 1-second monitoring loop passes
        # allow_alt=False so fallback APIs cannot wedge instant alerts.
        alt = _fetch_alt_sources_sync(ca) if allow_alt else None
        if alt:
            _dex_cache[ca] = (alt, _time.time() + _DEX_CACHE_TTL)
            _dex_miss_cache.pop(ca, None)
            _dex_cache_gc()
            return alt

        # Not indexed anywhere yet -> short negative cache, no more retries this round.
        _dex_miss_cache[ca] = _time.time() + _DEX_MISS_TTL
        _dex_cache_gc()
        return None

    return None

_BIRDEYE_BULK_429_UNTIL = 0.0

_BIRDEYE_BULK_COOLDOWN  = int(os.environ.get("BIRDEYE_BULK_429_COOLDOWN_SECONDS", "15") or 15)

def _invalidate_dex_cache(ca: str):
    """Force the next fetch to be genuinely fresh.

    IMPORTANT: clear BOTH positive and negative caches. A negative-cache entry
    is what can otherwise make a brand-new caller stay alert_pending for 45s
    even though DexScreener starts returning the token immediately afterwards.
    """
    if not ca:
        return
    _dex_cache.pop(ca, None)
    _dex_miss_cache.pop(ca, None)

def _get_cached_dex(ca: str):
    """Return only the current bulk-refresh result; never perform network I/O.

    The monitoring loop used to call _fetch_dex_sync once per tracked token
    after it had already done a bulk request.  On a cache miss that spawned
    dozens of slow requests (and fallback-provider requests) in the same tick,
    causing the next tick to overlap and making X alerts arrive late.
    """
    if not ca:
        return None
    cached = _dex_cache.get(ca)
    if not cached:
        return None
    result, expiry = cached
    if time.time() >= expiry:
        return None
    return result

def _fetch_token_info_fallback_sync(ca: str, chain_guess: str) -> dict:
    """Fallback token info fetch when DexScreener has no/partial data.
    For SOL tokens : Jupiter → pump.fun → DexScreener search.
    For EVM tokens : GeckoTerminal (specific chain first) → DexScreener search.
    `chain_guess` may be the already-resolved chain ("ETH","BNB","BASE","SOL")
    or the raw guess ("EVM","SOL").
    Returns dict with 'symbol' and optionally 'mcap'/'mcap_fmt'/'chain', or {}.
    """
    # Normalise chain_guess → GeckoTerminal network string + display chain
    _GECKO_NET = {"ETH": "eth", "BNB": "bsc", "BASE": "base", "RH": "robinhood", "ARC": "arc",
                  "ethereum": "eth", "bsc": "bsc", "base": "base", "robinhood": "robinhood", "arc": "arc"}
    _CHAIN_DISP = {"eth": "ETH", "bsc": "BNB", "base": "BASE", "robinhood": "RH", "arc": "ARC"}

    result = {}
    try:
        # ══════════════════════════════════════════════════════════════════
        # SOL path
        # ══════════════════════════════════════════════════════════════════
        if chain_guess in ("SOL", "solana"):
            # SPEED_NOV12: Jupiter + pump.fun + DexScreener search run
            # concurrently (was: strictly sequential, up to 8+8+10=26s).
            # Merge keeps the same priority and the same fill rules, so the
            # result is identical — only the wall time changes.
            # NOTE: the sequential code SKIPPED pump.fun whenever Jupiter
            # already gave a symbol, so in that case the MC came from the
            # DexScreener search only. The merge below preserves that exact
            # gate (mcap from pump.fun only when Jupiter gave no symbol).
            def _fb_jup():
                try:
                    r = requests.get(f"https://tokens.jup.ag/token/{ca}",
                                     headers=HEADERS, timeout=8)
                    if r.status_code == 200:
                        d = r.json()
                        sym = d.get("symbol") or d.get("name") or ""
                        if sym:
                            return {"symbol": sym, "chain": "SOL", "_src": "jup"}
                except Exception as e:
                    logger.debug(f"Jupiter fallback failed: {e}")
                return {}

            def _fb_pump():
                try:
                    r = requests.get(f"https://frontend-api.pump.fun/coins/{ca}",
                                     headers=HEADERS, timeout=8)
                    if r.status_code == 200:
                        d = r.json()
                        sym = d.get("symbol") or ""
                        mc = float(d.get("market_cap") or 0)
                        if sym:
                            out = {"symbol": sym, "chain": "SOL", "_src": "pump"}
                            if mc > 0:
                                out["mcap"] = mc
                                out["mcap_fmt"] = fmt_mc(mc)
                            return out
                except Exception as e:
                    logger.debug(f"Pump.fun fallback failed: {e}")
                return {}

            def _fb_dexsearch_sol():
                try:
                    r = requests.get(
                        f"https://api.dexscreener.com/latest/dex/search?q={ca}",
                        headers=HEADERS, timeout=10)
                    if r.status_code == 200:
                        for p in (r.json().get("pairs") or []):
                            if p.get("baseToken", {}).get("address", "").lower() != ca.lower():
                                continue
                            sym = p.get("baseToken", {}).get("symbol", "")
                            mc = float(p.get("marketCap") or 0)
                            if sym:
                                out = {"symbol": sym,
                                       "chain": SUPPORTED_CHAINS.get(
                                           p.get("chainId", "").lower(), "SOL"),
                                       "_src": "dexsearch"}
                                if mc > 0:
                                    out["mcap"] = mc
                                    out["mcap_fmt"] = fmt_mc(mc)
                                return out
                except Exception as e:
                    logger.debug(f"DexScreener SOL search fallback failed: {e}")
                return {}

            try:
                import concurrent.futures as _cf
                _ex = _cf.ThreadPoolExecutor(max_workers=3)
                _deadline = time.time() + 11
                def _get(_fut):
                    try:
                        return _fut.result(timeout=max(0.1, _deadline - time.time()))
                    except Exception:
                        return {}
                try:
                    _f_jup = _ex.submit(_fb_jup)
                    _f_pump = _ex.submit(_fb_pump)
                    _f_dex = _ex.submit(_fb_dexsearch_sol)
                    _jup = _get(_f_jup)
                    if _jup.get("symbol"):
                        # pump.fun skipped in the original when Jupiter has
                        # a symbol — do not wait for its future either.
                        _pump = {}
                        _dexs = _get(_f_dex)
                    else:
                        _pump = _get(_f_pump)
                        _dexs = _get(_f_dex)
                finally:
                    _ex.shutdown(wait=False)
            except Exception:
                _jup, _pump, _dexs = {}, {}, {}
            _sol_used_jup = bool(_jup.get("symbol"))
            _sol_used_pump = False
            _sol_used_dex = False
            # symbol: first source (Jupiter > pump.fun > DexSearch) with one
            for _p in (_jup, _pump, _dexs):
                if _p.get("symbol"):
                    result["symbol"] = _p["symbol"]
                    if _p.get("chain"):
                        result["chain"] = _p["chain"]
                    if _p is _pump:
                        _sol_used_pump = True
                    elif _p is _dexs:
                        _sol_used_dex = True
                    break
            # mcap: pump.fun only when Jupiter gave no symbol (see NOTE above)
            _mcap_order = (_dexs,) if _jup.get("symbol") else (_pump, _dexs)
            for _p in _mcap_order:
                if _p.get("mcap", 0) > 0:
                    result["mcap"] = _p["mcap"]
                    result["mcap_fmt"] = _p.get("mcap_fmt") or fmt_mc(_p["mcap"])
                    if _p is _pump:
                        _sol_used_pump = True
                    elif _p is _dexs:
                        _sol_used_dex = True
                    break
            if _sol_used_jup:
                logger.info(f"🔍 Jupiter fallback: symbol={_jup.get('symbol')} for {ca[:12]}...")
            if _sol_used_pump:
                logger.info(f"🔍 Pump.fun fallback: sym={_pump.get('symbol')} mc={_pump.get('mcap', 0)} for {ca[:12]}...")
            if _sol_used_dex:
                logger.info(f"🔍 DEX search SOL fallback: sym={_dexs.get('symbol')} mc={_dexs.get('mcap', 0)} for {ca[:12]}...")

        # ══════════════════════════════════════════════════════════════════
        # TON path
        # ══════════════════════════════════════════════════════════════════
        elif chain_guess == "TON":
            # SPEED_NOV12: DexScreener + GeckoTerminal TON run concurrently
            # (was: GeckoTerminal only if DexScreener found no symbol).
            # Merge keeps DexScreener first and the same gate (GT is only
            # used when DexScreener found no symbol), so the result is
            # identical — only the wall time changes.
            def _fb_ton_dex():
                try:
                    r = requests.get(
                        f"https://api.dexscreener.com/latest/dex/tokens/{ca}",
                        headers=HEADERS, timeout=10)
                    if r.status_code == 200:
                        for p in (r.json().get("pairs") or []):
                            if p.get("chainId", "").lower() != "ton":
                                continue
                            sym = p.get("baseToken", {}).get("symbol", "")
                            mc = float(p.get("marketCap") or 0)
                            if sym:
                                out = {"symbol": sym, "chain": "TON", "_src": "dex"}
                                if mc > 0:
                                    out["mcap"] = mc
                                    out["mcap_fmt"] = fmt_mc(mc)
                                return out
                except Exception as e:
                    logger.debug(f"DexScreener TON fallback failed: {e}")
                return {}

            def _fb_ton_gt():
                try:
                    r = requests.get(
                        f"https://api.geckoterminal.com/api/v2/networks/ton/tokens/{ca}",
                        headers={"Accept": "application/json"}, timeout=8)
                    if r.status_code == 200:
                        attr = r.json().get("data", {}).get("attributes", {})
                        sym = attr.get("symbol") or attr.get("name") or ""
                        mc = float(attr.get("market_cap_usd") or 0)
                        if sym:
                            out = {"symbol": sym, "chain": "TON", "_src": "gt"}
                            if mc > 0:
                                out["mcap"] = mc
                                out["mcap_fmt"] = fmt_mc(mc)
                            return out
                except Exception as e:
                    logger.debug(f"GeckoTerminal TON fallback failed: {e}")
                return {}

            try:
                import concurrent.futures as _cf
                _ex = _cf.ThreadPoolExecutor(max_workers=2)
                _deadline = time.time() + 11
                def _get(_fut):
                    try:
                        return _fut.result(timeout=max(0.1, _deadline - time.time()))
                    except Exception:
                        return {}
                try:
                    _f_dex = _ex.submit(_fb_ton_dex)
                    _f_gt = _ex.submit(_fb_ton_gt)
                    _ton_dex = _get(_f_dex)
                    # GT only used when DexScreener found no symbol (same
                    # gate as the sequential code) — do not wait for it then.
                    _ton_gt = {} if _ton_dex.get("symbol") else _get(_f_gt)
                finally:
                    _ex.shutdown(wait=False)
            except Exception:
                _ton_dex, _ton_gt = {}, {}
            _ton_pick = _ton_dex if _ton_dex.get("symbol") else _ton_gt
            if _ton_pick.get("symbol"):
                result["symbol"] = _ton_pick["symbol"]
                result["chain"] = "TON"
                if _ton_pick.get("mcap", 0) > 0:
                    result["mcap"] = _ton_pick["mcap"]
                    result["mcap_fmt"] = _ton_pick.get("mcap_fmt") or fmt_mc(_ton_pick["mcap"])
                if _ton_pick.get("_src") == "dex":
                    logger.info(f"🔍 DexScreener TON: sym={_ton_pick.get('symbol')} mc={_ton_pick.get('mcap', 0)} for {ca[:12]}...")
                else:
                    logger.info(f"🔍 GeckoTerminal TON: sym={_ton_pick.get('symbol')} mc={_ton_pick.get('mcap', 0)} for {ca[:12]}...")

        # ══════════════════════════════════════════════════════════════════
        # EVM path  (ETH / BNB / BASE — or unknown "EVM")
        # ══════════════════════════════════════════════════════════════════
        else:
            # Determine which networks to try, most-likely first
            # RH (Robinhood) is EVM-compatible; GeckoTerminal uses "robinhood" network key
            _GECKO_NET["RH"] = "robinhood"
            known_net = _GECKO_NET.get(chain_guess)
            if known_net:
                nets_to_try = [known_net] + [n for n in ("eth", "bsc", "base", "robinhood")
                                              if n != known_net]
            else:
                nets_to_try = ["eth", "bsc", "base", "robinhood"]

            # 1. GeckoTerminal token endpoint — often has fdv_usd / market_cap_usd
            # SPEED_NOV12: all networks fetched concurrently (was: sequential,
            # up to 4 x 8s = 32s when the token sits on the last network).
            # Merge keeps the same priority (nets_to_try order): symbol from
            # the first network with one, MC from the first with one, chain
            # from the network that gave the symbol. Identical result.
            def _fb_gt_token(network):
                try:
                    r = requests.get(
                        f"https://api.geckoterminal.com/api/v2/networks/{network}/tokens/{ca}",
                        headers={"Accept": "application/json"}, timeout=8)
                    if r.status_code != 200:
                        return {}
                    attr = r.json().get("data", {}).get("attributes", {})
                    sym = attr.get("symbol") or attr.get("name") or ""
                    mc = float(attr.get("market_cap_usd") or 0)
                    if sym:
                        out = {"symbol": sym, "chain": _CHAIN_DISP.get(network, "EVM"),
                               "_src": network}
                        if mc > 0:
                            out["mcap"] = mc
                            out["mcap_fmt"] = fmt_mc(mc)
                        return out
                except Exception as e:
                    logger.debug(f"GeckoTerminal {network} failed: {e}")
                return {}

            try:
                import concurrent.futures as _cf
                _ex = _cf.ThreadPoolExecutor(max_workers=len(nets_to_try))
                _deadline = time.time() + 9
                try:
                    _futs = [_ex.submit(_fb_gt_token, _net) for _net in nets_to_try]
                    _gt_parts = []
                    _have_sym = False
                    _have_mc = False
                    for _f in _futs:
                        try:
                            _part = _f.result(timeout=max(0.1, _deadline - time.time()))
                        except Exception:
                            _part = {}
                        _gt_parts.append(_part)
                        if _part.get("symbol"):
                            _have_sym = True
                        if _part.get("mcap", 0) > 0:
                            _have_mc = True
                        if _have_sym and _have_mc:
                            break  # enough data — do not wait for the rest
                    _gt_parts += [{}] * (len(nets_to_try) - len(_gt_parts))
                finally:
                    _ex.shutdown(wait=False)
            except Exception:
                _gt_parts = [{} for _ in nets_to_try]
            for _net, _part in zip(nets_to_try, _gt_parts):
                if not _part:
                    continue
                _used = False
                if _part.get("symbol") and not result.get("symbol"):
                    result["symbol"] = _part["symbol"]
                    result["chain"] = _part.get("chain", "EVM")
                    _used = True
                if _part.get("mcap", 0) > 0 and not result.get("mcap"):
                    result["mcap"] = _part["mcap"]
                    result["mcap_fmt"] = _part.get("mcap_fmt") or fmt_mc(_part["mcap"])
                    _used = True
                if _used:
                    logger.info(
                        f"🔍 GeckoTerminal ({_net}): sym={_part.get('symbol')} mc={_part.get('mcap', 0)} for {ca[:12]}...")

            # 2. GeckoTerminal top pools for the token — pools have reliable fdv/mc data
            # SPEED_NOV12: pool lists for the candidate networks fetched
            # concurrently (was: sequential, up to 4 x 8s). Merge keeps the
            # same priority: first network (in order) with a pool MC wins.
            if not result.get("mcap"):
                _pool_nets = (nets_to_try if result.get("chain") is None
                              else [_GECKO_NET.get(result["chain"], "eth")])

                def _fb_gt_pools(network):
                    try:
                        r = requests.get(
                            f"https://api.geckoterminal.com/api/v2/networks/{network}"
                            f"/tokens/{ca}/pools?page=1",
                            headers={"Accept": "application/json"}, timeout=8)
                        if r.status_code != 200:
                            return {}
                        for pool in (r.json().get("data") or []):
                            attr = pool.get("attributes", {})
                            mc = float(attr.get("market_cap_usd") or 0)
                            sym = (attr.get("name") or "").split(" / ")[0].strip()
                            if mc > 0:
                                out = {"mcap": mc, "mcap_fmt": fmt_mc(mc),
                                       "chain": _CHAIN_DISP.get(network, "EVM"),
                                       "_src": network}
                                if sym:
                                    out["symbol"] = sym
                                return out
                        return {}
                    except Exception as e:
                        logger.debug(f"GeckoTerminal pools {network} failed: {e}")
                        return {}

                try:
                    import concurrent.futures as _cf
                    _ex = _cf.ThreadPoolExecutor(max_workers=len(_pool_nets))
                    _deadline = time.time() + 9
                    try:
                        _futs = [_ex.submit(_fb_gt_pools, _net) for _net in _pool_nets]
                        _pool_parts = []
                        for _f in _futs:
                            try:
                                _part = _f.result(timeout=max(0.1, _deadline - time.time()))
                            except Exception:
                                _part = {}
                            _pool_parts.append(_part)
                            if _part.get("mcap", 0) > 0:
                                break  # first network with a pool MC wins
                        _pool_parts += [{}] * (len(_pool_nets) - len(_pool_parts))
                    finally:
                        _ex.shutdown(wait=False)
                except Exception:
                    _pool_parts = [{} for _ in _pool_nets]
                for _net, _part in zip(_pool_nets, _pool_parts):
                    if not _part:
                        continue
                    if _part.get("mcap", 0) > 0 and not result.get("mcap"):
                        result["mcap"] = _part["mcap"]
                        result["mcap_fmt"] = _part.get("mcap_fmt") or fmt_mc(_part["mcap"])
                        if _part.get("symbol") and not result.get("symbol"):
                            result["symbol"] = _part["symbol"]
                        result.setdefault("chain", _part.get("chain", "EVM"))
                        logger.info(
                            f"🔍 GeckoTerminal pools ({_net}): sym={_part.get('symbol', '')} mc={_part.get('mcap', 0)} for {ca[:12]}...")
                        break

            # 3. DexScreener search — catches tokens where direct endpoint had no MC
            if not result.get("symbol") or not result.get("mcap"):
                try:
                    r = requests.get(
                        f"https://api.dexscreener.com/latest/dex/search?q={ca}",
                        headers=HEADERS, timeout=10)
                    if r.status_code == 200:
                        for p in (r.json().get("pairs") or []):
                            if p.get("baseToken", {}).get("address", "").lower() != ca.lower():
                                continue
                            chain_id = p.get("chainId", "").lower()
                            if chain_id not in SUPPORTED_CHAINS:
                                continue
                            sym = p.get("baseToken", {}).get("symbol", "")
                            mc  = float(p.get("marketCap") or 0)
                            if sym and not result.get("symbol"):
                                result["symbol"] = sym
                                result["chain"]  = SUPPORTED_CHAINS[chain_id]
                            if mc > 0 and not result.get("mcap"):
                                result["mcap"]     = mc
                                result["mcap_fmt"] = fmt_mc(mc)
                            if result.get("symbol") and result.get("mcap"):
                                break
                        if result.get("symbol") or result.get("mcap"):
                            logger.info(
                                f"🔍 DEX search EVM fallback: sym={result.get('symbol')} "
                                f"mc={result.get('mcap_fmt')} for {ca[:12]}...")
                except Exception as e:
                    logger.debug(f"DexScreener EVM search fallback failed: {e}")

    except Exception as e:
        logger.warning(f"_fetch_token_info_fallback_sync crash: {e}")

    return result

def _extract_tg_link(links):
    """Find a Telegram group/channel link from DexScreener links list."""
    if not links: return ""
    for link in links:
        url = link.get("url","")
        ltype = link.get("type","").lower()
        label = link.get("label","").lower()
        if "t.me" in url or "telegram" in ltype or "telegram" in label:
            return url
    return ""

def _extract_tg_link_from_pairs(ca):
    """Fallback: get TG link from DexScreener pair data."""
    try:
        r = requests.get(f"https://api.dexscreener.com/latest/dex/tokens/{ca}", timeout=10, headers=HEADERS)
        if r.status_code != 200: return ""
        for pair in (r.json().get("pairs") or []):
            info = pair.get("info", {}) or {}
            for s in (info.get("socials") or []):
                u = s.get("url",""); t = s.get("type","").lower()
                if "t.me" in u or "telegram" in t: return u
            for lnk in (info.get("links") or []):
                u = lnk.get("url",""); t = lnk.get("type","").lower()
                if "t.me" in u or "telegram" in t: return u
    except Exception: pass
    return ""

def _gecko_get_json(url: str, tries: int = 3):
    """GeckoTerminal GET with retries.

    GeckoTerminal often answers HTTP 200 with a rate-limit body
    ({"status": {"error_code": 429}}). The old code treated that as valid data,
    so SOL / RH trending silently came back empty. Now we detect it and retry.
    """
    GECKO_HEADERS = {"Accept": "application/json"}
    for attempt in range(max(1, tries)):
        try:
            r = requests.get(url, headers=GECKO_HEADERS, timeout=15)
            if r.status_code in (429, 502, 503, 504):
                time.sleep(1.5 * (attempt + 1)); continue
            if r.status_code != 200:
                logger.warning(f"Gecko {url.split('/networks/')[-1][:24]}: HTTP {r.status_code}")
                return None
            data = r.json()
            err = ((data or {}).get("status") or {}).get("error_code")
            if err:
                logger.warning(f"Gecko rate-limited ({err}) — retry {attempt+1}")
                time.sleep(2.0 * (attempt + 1)); continue
            if not (data or {}).get("data"):
                time.sleep(1.0 * (attempt + 1)); continue
            return data
        except Exception as e:
            logger.warning(f"Gecko fetch failed: {e}")
            time.sleep(1.0 * (attempt + 1))
    return None

def _fetch_gecko_chain(gecko_network: str, dex_chain_key: str, blacklist: set) -> list:
    """Fetch top-5 trending tokens for one chain via GeckoTerminal.
    Returns list of {symbol, mc_fmt, tg_url, dex_url, has_tg} dicts.
    """
    GECKO_HEADERS = {"Accept": "application/json"}
    DEXPATH = {"ETH": "ethereum", "BNB": "bsc", "BASE": "base", "SOL": "solana"}
    results = []
    data = _gecko_get_json(
        f"https://api.geckoterminal.com/api/v2/networks/{gecko_network}/trending_pools"
        f"?page=1&include=base_token")
    if not data:
        return results

    # Build included base_token lookup: id -> {symbol, address, telegram}
    # Accept ANY type — GeckoTerminal has used "token" and "base_token" at different times
    included_map: dict = {}
    for item in (data.get("included") or []):
        attr = item.get("attributes", {})
        item_id = item.get("id", "")
        if not item_id: continue
        included_map[item_id] = {
            "symbol":   attr.get("symbol", ""),
            "address":  attr.get("address", ""),
            "telegram": attr.get("telegram_chat_url") or "",
        }

    def _ca_from_rel_id(rel_id: str) -> str:
        """GeckoTerminal rel IDs are like 'eth_0xabc…' or 'base_0xabc…'.
        Strip the network prefix to get the raw contract address as fallback."""
        if not rel_id: return ""
        if "_" in rel_id:
            parts = rel_id.split("_", 1)
            candidate = parts[1]
            # Basic sanity: looks like an address (hex) or Solana pubkey (base58)
            if len(candidate) > 20:
                return candidate
        return ""

    seen_tokens: set = set()
    for pool in (data.get("data") or []):
        attr = pool.get("attributes", {})

        # Market cap / FDV — try all available fields; fall back to volume as a last resort
        mc_raw = (attr.get("market_cap_usd") or attr.get("fdv_usd")
                  or attr.get("reserve_in_usd") or 0)
        try: mc = float(mc_raw)
        except Exception: mc = 0.0
        # If still zero, use 24h volume as a rough proxy so the pool isn't silently dropped
        if mc <= 0:
            try: mc = float(attr.get("volume_usd", {}).get("h24") or 0)
            except Exception: mc = 0.0
        if mc <= 0: continue

        # Get base token info — try included_map first, then parse rel_id as fallback
        rel_id = (
            pool.get("relationships", {})
            .get("base_token", {})
            .get("data", {})
            .get("id", "")
        )
        token_info = included_map.get(rel_id, {})
        ca  = token_info.get("address", "") or _ca_from_rel_id(rel_id)
        sym = (token_info.get("symbol")
               or attr.get("name", "TOKEN").split(" / ")[0]
               or attr.get("base_token_symbol", "TOKEN"))

        if not ca or ca.lower() in blacklist: continue
        if ca in seen_tokens: continue
        seen_tokens.add(ca)

        tg  = token_info.get("telegram", "")
        dex = f"https://dexscreener.com/{DEXPATH.get(dex_chain_key, gecko_network)}/{ca}"
        results.append({
            "symbol":  sym,
            "mc_fmt":  fmt_mc(mc),
            "tg_url":  tg,
            "dex_url": dex,
            "has_tg":  bool(tg),
            "_ca":     ca,   # temp field for DexScreener TG supplement
        })
        if len(results) >= 10: break  # collect up to 10, sort below

    # ── Supplement missing TG links via DexScreener batch lookup ─────────────
    no_tg = [r for r in results if not r["tg_url"] and r.get("_ca")]
    if no_tg:
        batch_cas = [r["_ca"] for r in no_tg]
        try:
            rs = requests.get(
                f"https://api.dexscreener.com/latest/dex/tokens/{','.join(batch_cas)}",
                headers=HEADERS, timeout=15
            )
            if rs.status_code == 200:
                tg_map: dict = {}
                for p in (rs.json().get("pairs") or []):
                    ca_key = p.get("baseToken", {}).get("address", "")
                    if ca_key and ca_key not in tg_map:
                        info = p.get("info") or {}
                        for s in (info.get("socials") or []):
                            u = s.get("url", ""); t = s.get("type", "").lower()
                            if "t.me" in u or "telegram" in t:
                                tg_map[ca_key] = u; break
                        if ca_key not in tg_map:
                            for lnk in (info.get("links") or []):
                                u = lnk.get("url", ""); t = lnk.get("type", "").lower()
                                if "t.me" in u or "telegram" in t:
                                    tg_map[ca_key] = u; break
                for r in results:
                    if not r["tg_url"] and r.get("_ca") and r["_ca"] in tg_map:
                        r["tg_url"] = tg_map[r["_ca"]]
                        r["has_tg"] = True
        except Exception as e:
            logger.warning(f"DexScreener TG supplement {gecko_network}: {e}")

    # Remove temp field
    for r in results: r.pop("_ca", None)

    # TG-first sort, return top 5
    results.sort(key=lambda x: (0 if x["has_tg"] else 1))
    return results[:5]

_LABELED_CA_RE = re.compile(
    r'(?:^|[\s\(\[])(?:ca|c\.a|contract(?:\s*address)?|token(?:\s*address)?|mint|address)'
    r'\s*[:：\-–—=]?\s*'
    r'`?([1-9A-HJ-NP-Za-km-z]{32,50}|0x[a-fA-F0-9]{64}|0x[a-fA-F0-9]{40})`?',
    re.IGNORECASE)

_URL_STRIP_RE = re.compile(r'https?://\S+|(?:www|t)\.me/\S+', re.IGNORECASE)

def _classify_ca(ca):
    """(chain, ca) for a raw address string, or None."""
    if not ca:
        return None
    if ca.startswith("0x") and len(ca) in (42, 66):
        return ("EVM", ca.lower())
    if re.match(r'^(EQ|UQ)[A-Za-z0-9_-]{46}$', ca):
        return ("TON", ca)
    if 32 <= len(ca) <= 44 and not ca.startswith("0x"):
        return ("SOL", ca)
    return None

def extract_ca(text):
    if not text:
        return None
    # 1) Explicit "Ca: ..." style line always wins.
    for m in _LABELED_CA_RE.finditer(text):
        r = _classify_ca(m.group(1))
        if r:
            return r
    # 2) Generic scan, but with URLs removed (pair addresses live in URLs).
    clean = _URL_STRIP_RE.sub(" ", text)
    for scope in (clean, text):
        # TON first — check before SOL to avoid misidentification (TON addresses can match SOL pattern)
        for m in TON_CA_PATTERN.finditer(scope):
            return ("TON", m.group(0))
        eth = ETH_CA_PATTERN.findall(scope)
        if eth:
            return ("EVM", eth[0].lower())
        for s in SOL_CA_PATTERN.findall(scope):
            if not s.startswith("0x") and 32 <= len(s) <= 44:
                return ("SOL", s)
    return None

_DEX_LINK_CHAIN_MAP = {
    "solana": "SOL", "sol": "SOL",
    "ethereum": "EVM", "eth": "EVM",
    "bsc": "EVM", "bnb": "EVM", "binance": "EVM",
    "base": "EVM",
    "robinhood": "EVM", "rh": "EVM",
    "ton": "TON",
}

_SOL_TON_ADDR = r'[1-9A-HJ-NP-Za-km-z]{32,50}'

_EVM_ADDR     = r'0x[a-fA-F0-9]{64}(?![a-fA-F0-9])|0x[a-fA-F0-9]{40}(?![a-fA-F0-9])'

_ANY_CA       = rf'({_SOL_TON_ADDR}|{_EVM_ADDR})'

_DEXSCREENER_RE = re.compile(
    r'dexscreener\.com/([a-z0-9_-]+)/(' + _SOL_TON_ADDR + r'|' + _EVM_ADDR + r')',
    re.IGNORECASE
)

_DEXTOOLS_RE = re.compile(
    r'dextools\.io/app(?:/[a-z]+)?/([a-z0-9_-]+)/pair-explorer/' + _ANY_CA,
    re.IGNORECASE
)

_BULLX_RE = re.compile(
    r'bullx\.io[^\s]*[?&]address=' + _ANY_CA,
    re.IGNORECASE
)

_PHOTON_RE = re.compile(
    r'photon-sol\.trac\.so/(' + _SOL_TON_ADDR + r')',
    re.IGNORECASE
)

_BIRDEYE_RE = re.compile(
    r'birdeye\.so/token/' + _ANY_CA,
    re.IGNORECASE
)

_GMGN_RE = re.compile(
    r'gmgn\.ai/[a-z0-9]*/token/(?:[a-z]*/)?(?:[A-Za-z0-9]{3,20}_)?(' + _SOL_TON_ADDR + r'|' + _EVM_ADDR + r')',
    re.IGNORECASE
)

_AVE_RE = re.compile(
    r'ave\.ai/token/' + _ANY_CA,
    re.IGNORECASE
)

def extract_ca_from_links(text):
    """Try to extract a CA from known DEX/chart links in the message text.
    Returns (chain_guess, ca) like extract_ca(), or None if nothing found."""
    if not text:
        return None

    def _valid_ca(ca, chain_guess="SOL"):
        """Validate and normalise a CA string. Returns (chain, ca) or None."""
        if not ca: return None
        if ca.startswith("0x") and len(ca) in (42, 66):
            # 42 = standard EVM (0x+40hex); 66 = Robinhood/"RH" 32-byte
            # address format (0x+64hex) seen on DexScreener chart links.
            return ("EVM", ca.lower())
        # TON: EQ/UQ + 46 base64url chars = 48 total
        if re.match(r'^(EQ|UQ)[A-Za-z0-9_-]{46}$', ca):
            return ("TON", ca)
        # SOL: base58, 32-44 chars (never starts with 0x)
        if 32 <= len(ca) <= 44 and not ca.startswith("0x"):
            return (chain_guess, ca)
        return None

    # DexScreener: dexscreener.com/{chain}/{ca}
    for m in _DEXSCREENER_RE.finditer(text):
        chain_raw = m.group(1).lower()
        ca = m.group(2)
        chain_guess = _DEX_LINK_CHAIN_MAP.get(chain_raw, "EVM")
        r = _valid_ca(ca, chain_guess)
        if r: return r

    # DexTools
    for m in _DEXTOOLS_RE.finditer(text):
        chain_raw = m.group(1).lower()
        ca = m.group(2)
        chain_guess = _DEX_LINK_CHAIN_MAP.get(chain_raw, "EVM")
        r = _valid_ca(ca, chain_guess)
        if r: return r

    # BullX
    for m in _BULLX_RE.finditer(text):
        r = _valid_ca(m.group(1), "SOL")
        if r: return r

    # Photon (Solana only)
    for m in _PHOTON_RE.finditer(text):
        r = _valid_ca(m.group(1), "SOL")
        if r: return r

    # Birdeye
    for m in _BIRDEYE_RE.finditer(text):
        r = _valid_ca(m.group(1), "SOL")
        if r: return r

    # Gmgn
    for m in _GMGN_RE.finditer(text):
        r = _valid_ca(m.group(1), "SOL")
        if r: return r

    # Ave.ai
    for m in _AVE_RE.finditer(text):
        r = _valid_ca(m.group(1), "SOL")
        if r: return r

    return None

_EMOJI_MARKER_RE = re.compile(r"\[\[emoji:(\d{5,25})\]\]")

_TG_EMOJI_TAG_RE = re.compile(r'<tg-emoji\s+emoji-id="(\d{5,25})"\s*>(.*?)</tg-emoji>',
                              re.IGNORECASE | re.DOTALL)

def prepare_owner_emojis(text: str):
    """Userbot path. Convert the owner's own premium-emoji syntax
    (`[[emoji:ID]]` or `<tg-emoji emoji-id="ID">x</tg-emoji>`) into plain 🔮
    markers, remembering which marker index must use which explicit ID.

    Returns (html_text_with_markers, {marker_index: emoji_id}).
    """
    if not text:
        return text, {}
    out = []
    explicit = {}
    idx = 0          # running count of 🔮 markers already emitted
    pos = 0
    pattern = re.compile(
        r"\[\[emoji:(?P<a>\d{5,25})\]\]"
        r'|<tg-emoji\s+emoji-id="(?P<b>\d{5,25})"\s*>(?P<inner>.*?)</tg-emoji>',
        re.IGNORECASE | re.DOTALL)
    for m in pattern.finditer(text):
        chunk = text[pos:m.start()]
        out.append(chunk)
        idx += chunk.count("🔮")
        eid = m.group("a") or m.group("b")
        explicit[idx] = int(eid)
        out.append("🔮")
        idx += 1
        pos = m.end()
    tail = text[pos:]
    out.append(tail)
    return "".join(out), explicit

_MC_VALUE_BEFORE_RE = re.compile(r'(?:^|[\s>])\$?\d[\d,]*\.?\d*\s*[KMBkmb]?$')

def _build_premium_entities(plain_text, base_entities, emoji_ids, forced_pack=None,
                            explicit=None):
    """Replace each 🔮 in plain_text with the next emoji_id from the list.
    emoji_ids may be a single str/int or a list. Falls back to last id when list runs out.

    Special-cases (in priority order):
    1. 🔮KOL / 🔮BOT  → pack kol/bot emoji (always, regardless of position)
    2. 🔮X or 🔮 X    → pack x emoji (locked — works even if template edited)
    3. "Champion KOL - Rank # 🔮" / "Leaderboard KOL - Rank # 🔮"
       → us KOL ki rank numbering premium emoji (auto-detected)
    4. (reserved)
    5. MC separator 🔮 (between entry MC and current MC) → TEER_EMOJI_ID=5346105514575025401 (locked)
    6. all other 🔮s  → next item from non-badge portion of emoji_ids

    Badge emojis (star + rank, appended at end of emoji_ids by send_alert) are
    auto-detected from the text so they NEVER get consumed by template 🔮 placeholders,
    even when the owner edits the template with extra 🔮s.
    """
    from telethon.tl.types import MessageEntityCustomEmoji
    if not isinstance(emoji_ids, list):
        emoji_ids = [emoji_ids]
    emoji_ids = [e for e in emoji_ids if e]
    # Promo link text (set via /setpromolink) — its 🔮 must ALWAYS render
    # PROMO_EMOJI_ID, never a positional id.
    _promo_txt = ""
    _promo_emoji_id = PROMO_EMOJI_ID
    try:
        _p = load_config().get("promo_link") or {}
        if _p.get("text"):
            _promo_txt = html.unescape(str(_p["text"])).strip()
        if _p.get("emoji_id"):
            _promo_emoji_id = int(_p["emoji_id"])
    except Exception:
        _promo_txt = _promo_txt or ""

    pack = forced_pack if forced_pack is not None else _get_alert_pack()
    kol_id = pack.get("kol")
    bot_id = pack.get("bot")
    mae_id = pack.get("mae") or 0
    if not emoji_ids and not kol_id and not bot_id and not mae_id:
        return base_entities or []

    # ── Badge auto-detection ───────────────────────────────────────────────────
    # Naya badge format (hamesha sabse aakhir me lagta hai):
    #   "Champion KOL - Rank # 🔮"  /  "Leaderboard KOL - Rank # 🔮"
    # 2 badge emojis hoti hain: pehli = lead/crystal, dusri = rank number emoji.
    # Leaderboard ki pehli = pack crystal (MC line wali hi, X range ke color me).
    #   non_badge_ids — consumed by regular template 🔮s via pos_index
    #   badge_ids     — reserved for the rank 🔮, never consumed by template 🔮s
    _badge_type = None
    if 'LEAD KOL - Rank #' in plain_text:
        _badge_type = 'leaderboard'
    elif 'CHAMP KOL - Rank #' in plain_text:
        _badge_type = 'champion'
    badge_count = 2 if _badge_type else 0  # champion: [lead, rank] · leaderboard: [crystal, rank]

    if badge_count and len(emoji_ids) >= badge_count:
        non_badge_ids = emoji_ids[:-badge_count]
        badge_ids     = emoji_ids[-badge_count:]
    else:
        non_badge_ids = emoji_ids
        badge_ids     = []

    # ── Dropped-call STRUCTURAL role map (language independent) ───────────────
    # FIX: the 1️⃣/➡️ lock above only recognised English/Chinese/Vietnamese
    # label text, so every other language (Turkish, Russian, Hindi, Arabic,
    # Spanish, ...) fell through to the positional chain-emoji list and the
    # chain emoji showed up 8x instead of 2x. Roles are now derived from the
    # template LAYOUT instead of label wording: the first line holding 🔮 is
    # the header (chain emoji), and every later line holding exactly two 🔮 is
    # a "1️⃣ label ➡️ value" row → (NUM, ARROW). MAE/KOL/BOT lines have a
    # single 🔮 so they are untouched.
    _drop_roles = {}
    if pack.get('name') == 'dropped':
        _off = 0
        _hdr_seen = False
        for _ln in plain_text.split('\n'):
            _pos = [_off + _m for _m in range(len(_ln)) if _ln.startswith('🔮', _m)]
            if _pos:
                if not _hdr_seen:
                    _hdr_seen = True
                elif len(_pos) == 2:
                    _drop_roles[_pos[0]] = DROPPED_NUM_EMOJI
                    _drop_roles[_pos[1]] = DROPPED_ARROW_EMOJI
            _off += len(_ln) + 1

    custom_entities = []
    explicit = explicit or {}
    pos_index = 0
    marker_idx = 0
    utf16_off = 0
    i = 0
    PLACEHOLDER = '🔮'
    PH_LEN = len(PLACEHOLDER)
    emoji_u16 = len(PLACEHOLDER.encode('utf-16-le')) // 2
    _badge_rank_pending = False  # True after we emitted the badge star emoji

    while i < len(plain_text):
        if plain_text[i:i+PH_LEN] == PLACEHOLDER:
            rest = plain_text[i+PH_LEN:]
            eid  = None
            _this_marker = marker_idx
            marker_idx += 1

            if _this_marker in explicit and i not in _drop_roles:
                # (dropped-call 1️⃣/➡️ slots are locked structurally — a stored lang template
                #  carrying the chain emoji id there must NOT win, that was the 8x bug)
                # Owner ne khud is jagah premium emoji ID di hai — wahi use hogi.
                eid = explicit[_this_marker]

            elif _promo_txt and rest.lstrip(' ').startswith(_promo_txt):
                # 🔮 <promo text> → owner ka chuna hua premium emoji
                # (/setpromolink me di gayi emoji ID), warna default.
                eid = _promo_emoji_id

            elif rest.startswith('KOL') and kol_id:
                # 🔮KOL → pack kol emoji
                eid = kol_id

            elif rest.startswith('BOT') and bot_id:
                # 🔮BOT → pack bot emoji
                eid = bot_id

            elif mae_id and rest.startswith('MAE'):
                # 🔮MAE → pack mae (Maestro) emoji — replaces the old 🔮X slot
                eid = mae_id

            elif rest.lstrip(' ').startswith('Caller') and pack.get('caller'):
                # 🔮Caller → pack caller emoji (X-alert in-text link row)
                # .lstrip(' ') tolerates "🔮 Caller" (with space) too, not just "🔮Caller"
                eid = pack.get('caller')

            elif rest.lstrip(' ').startswith('Twitter') and pack.get('twitter'):
                eid = pack.get('twitter')

            elif rest.lstrip(' ').startswith('Trade') and pack.get('trade'):
                eid = pack.get('trade')

            elif rest.lstrip(' ').upper().startswith('DEX') and pack.get('dex'):
                eid = pack.get('dex')

            elif rest.lstrip(' ').startswith('Details') and pack.get('details'):
                eid = pack.get('details')

            elif rest.lstrip(' ').startswith('Channel') and pack.get('channel'):
                eid = pack.get('channel')

            elif rest.lstrip(' ').startswith('Chart') and pack.get('chart'):
                # 🔮Chart → pack chart emoji (dropped-call link row, orange / green+purple themes)
                eid = pack.get('chart')

            elif rest.lstrip(' ').startswith('Post') and pack.get('post'):
                # 🔮Post → pack post emoji (dropped-call link row, red theme)
                eid = pack.get('post')

            elif rest.lstrip(' ').upper().startswith('DEV') and pack.get('dev'):
                eid = pack.get('dev')

            elif (_badge_type == 'champion' and len(badge_ids) >= 2
                  and rest.lstrip(' ').startswith('CHAMP KOL')):
                # "🔮 CHAMP KOL - Rank # 🔮" → leading badge emoji
                eid = badge_ids[0]

            elif (_badge_type == 'leaderboard' and len(badge_ids) >= 2
                  and rest.lstrip(' ').startswith('LEAD KOL')):
                # "🔮 LEAD KOL - Rank # 🔮" → leading badge emoji = pack crystal
                # (MC line wali hi crystal, X range ke color me)
                eid = badge_ids[0]

            elif badge_ids and plain_text[:i].endswith('Rank # '):
                # "🔮 CHAMP KOL - Rank # 🔮" / "Leaderboard KOL - Rank # 🔮"
                # → us KOL ki rank numbering premium emoji (badge ka aakhri id)
                eid = badge_ids[-1]

            elif i in _drop_roles:
                # Language-independent dropped-call lock (see _drop_roles above)
                eid = _drop_roles[i]

            elif (pack.get('name') == 'dropped' and rest.startswith(
                    ('Token Symbol', 'Current MC', 'Chain Symbol',
                     '代币符号', '当前市值', '链符号',
                     'Ký hiệu Token', 'Vốn hóa hiện tại', 'Ký hiệu Chain'))):
                # HARD LOCK (dropped-call only): the "1️⃣" before these three
                # labels must always be DROPPED_NUM_EMOJI — never the
                # positional chain-emoji list. This is a context lock (not
                # dependent on the `explicit` map), so it self-heals even if
                # the stored dropped_call_template ever loses/corrupts its
                # saved tg-emoji IDs (that corruption is exactly what made
                # this emoji show as the chain emoji 8x instead of 2x).
                # FIX: originally only matched the English label text, so a
                # Chinese/Vietnamese/Turkish/Russian-tagged dropped-call template (translated
                # labels) never tripped this lock at all — EVERY 🔮 in that
                # template (header + all 3 label/arrow pairs) fell through to
                # the generic 2-item chain rotation instead, which is exactly
                # what produced "chain emoji 8x instead of 2x".
                eid = DROPPED_NUM_EMOJI

            elif (pack.get('name') == 'dropped' and plain_text[:i].rstrip().endswith(
                    ('Token Symbol', 'Current MC', 'Chain Symbol',
                     '代币符号', '当前市值', '链符号',
                     'Ký hiệu Token', 'Vốn hóa hiện tại', 'Ký hiệu Chain'))):
                # HARD LOCK (dropped-call only): the "➡️" right after those
                # same three labels must always be DROPPED_ARROW_EMOJI.
                # Same Chinese/Vietnamese label fix as above.
                eid = DROPPED_ARROW_EMOJI

            else:
                # ── MC separator lock ─────────────────────────────────────────
                # The 🔮 between entry MC and current MC must ALWAYS be TEER_EMOJI_ID.
                # Detect by context: preceded by MC value (ends K/M/B/digit) and
                # followed (after spaces) by $ or digit.
                before_stripped = plain_text[:i].rstrip()
                after_stripped  = rest.lstrip()
                # Require a REAL number token right before the 🔮 (start-of-
                # string or whitespace immediately before the digit run),
                # not just "last character is a digit". A bare last-char
                # check false-positives on anything ending in a digit that
                # ISN'T a market-cap value — e.g. a KOL username like
                # "@musterseed99" — which mis-fires the teer lock, skips
                # advancing pos_index, and desyncs every 🔮 after it (this is
                # exactly what flipped the number-badge/arrow order in the
                # leaderboard from rank 4 onward).
                if (before_stripped
                        and _MC_VALUE_BEFORE_RE.search(before_stripped)
                        and after_stripped
                        and after_stripped[0] in '$0123456789'):
                    eid = _pack_teer(pack)
                    # NOTE: pos_index NOT advanced — teer slot is not counted against
                    # the positional list, keeping badge emoji alignment correct.
                elif non_badge_ids:
                    if pos_index >= len(non_badge_ids):
                        # DIAG: this 🔮 didn't match ANY special case above
                        # (Caller/Twitter/Trade/DEX/Details/DEV/KOL/BOT/MAE/
                        # badge/teer-lock) and the positional list has run
                        # out, so it's about to repeat the LAST id (teer) —
                        # this is exactly the "wrong emoji on custom-template
                        # button labels" bug. Logging the text right after
                        # the 🔮 pinpoints which label text failed to match.
                        logger.warning(
                            f"🔮 emoji placeholder #{pos_index} fell through to "
                            f"positional fallback (reusing last id) — text after "
                            f"placeholder: {rest[:20]!r}")
                    eid = non_badge_ids[min(pos_index, len(non_badge_ids) - 1)]
                    pos_index += 1

            if eid:
                custom_entities.append(MessageEntityCustomEmoji(
                    offset=utf16_off, length=emoji_u16,
                    document_id=int(eid)
                ))
            utf16_off += emoji_u16
            i += PH_LEN
            continue
        char_u16 = len(plain_text[i].encode('utf-16-le')) // 2
        utf16_off += char_u16
        i += 1
    return (base_entities or []) + custom_entities

def _tl_buttons_from_keyboard(keyboard):
    """PTB InlineKeyboardMarkup → Telethon ReplyInlineMarkup (URL buttons only)."""
    if not keyboard:
        return None
    try:
        from telethon.tl.types import ReplyInlineMarkup, KeyboardButtonRow, KeyboardButtonUrl
        rows = []
        for row in keyboard.inline_keyboard:
            btns = [KeyboardButtonUrl(text=b.text, url=b.url) for b in row if getattr(b, "url", None)]
            if btns:
                rows.append(KeyboardButtonRow(buttons=btns))
        return ReplyInlineMarkup(rows=rows) if rows else None
    except Exception as e:
        logger.warning(f"keyboard convert failed: {e}")
        return None

async def _userbot_send_with_premium_emoji(chat, text, emoji_id=None, link_preview=False, forced_pack=None, keyboard=None):
    """Send via userbot (round-robin across 2 accounts + failover).
    Returns sent Message object on success, None on failure."""
    clients = _userbot_clients()
    if not clients:
        return None
    picked = _pick_userbot()
    ordered = [picked] + [cl for cl in clients if cl is not picked]
    tl_buttons = _tl_buttons_from_keyboard(keyboard)
    text, _explicit = prepare_owner_emojis(text)
    for ub in ordered:
        try:
            if emoji_id or _explicit:
                try:
                    from telethon.extensions.html import parse as tl_html_parse
                    plain_text, base_entities = tl_html_parse(text)
                    all_entities = _build_premium_entities(plain_text, base_entities, emoji_id,
                                                           forced_pack=forced_pack, explicit=_explicit)
                    msg = await ub.send_message(
                        chat, plain_text,
                        formatting_entities=all_entities,
                        link_preview=link_preview,
                        buttons=tl_buttons
                    )
                    return msg
                except Exception as e:
                    logger.warning(f"Custom emoji send failed, fallback: {e}")
                    if tl_buttons is not None:
                        # Buttons zaroori hain (X alert) — is client se nahi gaye
                        # to agle client pe try karo; dono fail hon to None.
                        continue
            msg = await ub.send_message(chat, text, parse_mode="html",
                                        link_preview=link_preview, buttons=tl_buttons)
            return msg
        except Exception as e:
            logger.warning(f"userbot send failed on one client, trying next: {e}")
            continue
    logger.error("userbot send: all clients failed")
    return None

async def _userbot_edit_with_premium_emoji(chat, msg_id, text, emoji_ids_for_ranking=None, forced_pack=None):
    """Edit message via userbot. If emoji_ids_for_ranking given, replaces number emojis."""
    if not userbot_client:
        return False
    try:
        if emoji_ids_for_ranking:
            try:
                from telethon.tl.types import MessageEntityCustomEmoji
                from telethon.extensions.html import parse as tl_html_parse
                plain_text, base_entities = tl_html_parse(text)
                default_nums = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
                custom_entities = []
                for idx, eid in enumerate(emoji_ids_for_ranking[:10]):
                    if not eid: continue
                    target = default_nums[idx] if idx < len(default_nums) else f"{idx+1}."
                    utf16_off = 0; i = 0
                    while i < len(plain_text):
                        seg = plain_text[i:i+len(target)]
                        if seg == target:
                            eid_u16 = len(target.encode('utf-16-le')) // 2
                            custom_entities.append(MessageEntityCustomEmoji(
                                offset=utf16_off, length=eid_u16,
                                document_id=int(eid)
                            ))
                            break
                        char_u16 = len(plain_text[i].encode('utf-16-le')) // 2
                        utf16_off += char_u16; i += 1
                all_entities = (base_entities or []) + custom_entities
                await _locked_userbot_edit(chat, msg_id, plain_text,
                                                   formatting_entities=all_entities,
                                                   link_preview=False)
                return True
            except Exception as e:
                logger.warning(f"Premium emoji edit failed, fallback: {e}")
        if forced_pack or _EMOJI_MARKER_RE.search(text or "") or _TG_EMOJI_TAG_RE.search(text or ""):
            try:
                from telethon.extensions.html import parse as tl_html_parse
                _txt, _explicit = prepare_owner_emojis(text)
                plain_text, base_entities = tl_html_parse(_txt)
                all_entities = _build_premium_entities(plain_text, base_entities, [],
                                                       forced_pack=forced_pack, explicit=_explicit)
                await _locked_userbot_edit(chat, msg_id, plain_text,
                                                   formatting_entities=all_entities,
                                                   link_preview=False)
                return True
            except Exception as e:
                logger.warning(f"forced_pack edit failed, fallback: {e}")
        await _locked_userbot_edit(chat, msg_id, text, parse_mode="html", link_preview=False)
        return True
    except Exception as e:
        logger.error(f"userbot edit {msg_id}: {e}")
        return False

async def _userbot_edit_caption_with_premium_emoji(chat, msg_id, text, emoji_id, forced_pack=None):
    """Edit a media message's caption via userbot. emoji_id = single id or list (one per 🔮)."""
    text, _explicit = prepare_owner_emojis(text)
    if not userbot_client or not (emoji_id or _explicit):
        logger.warning(f"Skipping emoji edit: userbot={userbot_client is not None}, emoji_id={emoji_id}")
        return False
    try:
        from telethon.extensions.html import parse as tl_html_parse
        plain_text, base_entities = tl_html_parse(text)
        all_entities = _build_premium_entities(plain_text, base_entities, emoji_id,
                                               forced_pack=forced_pack, explicit=_explicit)
        logger.info(f"Userbot editing msg {msg_id} with emoji_id={emoji_id}, entities={len(all_entities)}")
        await _locked_userbot_edit(
            chat, msg_id, plain_text,
            formatting_entities=all_entities,
            link_preview=False
        )
        logger.info(f"✅ Userbot emoji edit SUCCESS for msg {msg_id}")
        return True
    except Exception as e:
        logger.warning(f"Caption premium emoji edit FAILED: {e}")
        return False

_MEDIA_FILE_CACHE = {}

_MEDIA_TL_CACHE   = {}

_MEDIA_CACHE_DIR  = os.path.join(DATA_DIR, "media_cache") if "DATA_DIR" in dir() else "/tmp/wizard_media"

def _media_cache_dir():
    try:
        os.makedirs(_MEDIA_CACHE_DIR, exist_ok=True)
        return _MEDIA_CACHE_DIR
    except Exception:
        os.makedirs("/tmp/wizard_media", exist_ok=True)
        return "/tmp/wizard_media"

def invalidate_media_cache(file_id=None):
    """Owner naya media set kare to purana cache foran chhod do."""
    if file_id:
        _MEDIA_TL_CACHE.pop(file_id, None)
        _MEDIA_FILE_CACHE.pop(file_id, None)
    else:
        _MEDIA_TL_CACHE.clear()
        _MEDIA_FILE_CACHE.clear()

async def _userbot_send_media_with_emoji(bot_app, chat, file_id, file_type, text, emoji_id, keyboard=None, forced_pack=None, _retry=True):
    """Send media via userbot with premium emoji — NO edit = NO pencil mark.
    Media ek dafa download/upload hoti hai, phir cache se instant."""
    import os
    tmp_path = None
    # FIX (2026-10-13): keep the ORIGINAL text (with <tg-emoji> tags). The code
    # below overwrites `text` with the placeholder version; if the send fails with
    # "stale file reference" the retry used to get that already-converted text, so
    # prepare_owner_emojis() found no tags -> every placeholder fell back to the
    # positional id list (CALL 1/2/3 got the banner's last tile instead of their emoji).
    _orig_text = text
    try:
        cached_media = _MEDIA_TL_CACHE.get(file_id)
        # Stale cache: a photo cached for a video/gif (from the .jpg bug) must
        # not be reused — drop it so we re-download and send as video.
        if (cached_media is not None and file_type in ('video', 'gif')
                and getattr(cached_media, 'photo', None) is not None
                and getattr(cached_media, 'document', None) is None):
            cached_media = None
        if cached_media is None:
            # GIF/animation is an MP4 video too — a .jpg suffix made Telethon
            # send it as a PHOTO (the "video posts as a photo" bug). Only real
            # photos get .jpg.
            _suffix = '.mp4' if file_type in ('video', 'gif') else '.jpg'
            tmp_path = _MEDIA_FILE_CACHE.get(file_id)
            # Stale on-disk cache: an old .jpg file cached for a gif/video
            # must be re-downloaded with the right suffix.
            if tmp_path and not tmp_path.endswith(_suffix):
                tmp_path = None
            if not tmp_path or not os.path.exists(tmp_path):
                suffix = _suffix
                safe_id = re.sub(r'[^A-Za-z0-9_-]', '', str(file_id))[-64:]
                tmp_path = os.path.join(_media_cache_dir(), f"{safe_id}{suffix}")
                tg_file = await bot_app.get_file(file_id)
                await tg_file.download_to_drive(tmp_path)
                _MEDIA_FILE_CACHE[file_id] = tmp_path

        from telethon.extensions.html import parse as tl_html_parse
        text, _explicit = prepare_owner_emojis(text)
        plain_text, base_ents = tl_html_parse(text)
        all_entities = _build_premium_entities(plain_text, base_ents, emoji_id,
                                               forced_pack=forced_pack, explicit=_explicit)

        # Convert PTB InlineKeyboardMarkup → Telethon buttons
        tl_buttons = None
        if keyboard:
            try:
                from telethon.tl.types import ReplyInlineMarkup, KeyboardButtonRow, KeyboardButtonUrl
                rows = []
                for row in keyboard.inline_keyboard:
                    btns = [KeyboardButtonUrl(text=b.text, url=b.url) for b in row if b.url]
                    if btns:
                        rows.append(KeyboardButtonRow(buttons=btns))
                if rows:
                    tl_buttons = ReplyInlineMarkup(rows=rows)
            except Exception:
                pass

        # Round-robin across 2 userbot accounts + failover (ek fail/timeout
        # ho to dusra try karo — is se aaj subah wale "Timed out" masle pe
        # bhi dusra account backup deta hai)
        _clients = _userbot_clients()
        if not _clients:
            raise RuntimeError("no userbot client connected")
        _picked = _pick_userbot()
        _ordered = [_picked] + [cl for cl in _clients if cl is not _picked]
        msg = None
        _last_err = None
        for _ub in _ordered:
            try:
                _send_kwargs = dict(
                    caption=plain_text,
                    formatting_entities=all_entities,
                    buttons=tl_buttons,
                    supports_streaming=True,
                )
                if file_type == 'gif':
                    # Send as a looping GIF/animation, not a still photo.
                    from telethon.tl.types import DocumentAttributeAnimated
                    _send_kwargs['attributes'] = [DocumentAttributeAnimated()]
                msg = await _ub.send_file(
                    chat, cached_media if cached_media is not None else tmp_path,
                    **_send_kwargs
                )
                break
            except Exception as _se:
                _last_err = _se
                logger.warning(f"Userbot media send failed on one client, trying next: {_se}")
                continue
        if msg is None:
            raise _last_err if _last_err else RuntimeError("userbot media send failed")
        # Pehli kamyab upload ke baad Telegram ka media reference cache kar lo —
        # agli dafa upload bilkul nahi hoga (instant post).
        if cached_media is None and msg is not None:
            try:
                if getattr(msg, "media", None) is not None:
                    _MEDIA_TL_CACHE[file_id] = msg.media
            except Exception:
                pass
        logger.info(f"✅ Userbot media send SUCCESS (no pencil) file_type={file_type} "
                    f"cached={cached_media is not None}")
        return msg
    except Exception as e:
        # Stale media reference (file reference expired) -> cache clear karke
        # agli koshish fresh upload se hogi. FIX: pehle sirf cache clear hota
        # tha lekin usi call mein give up ho jata tha -> seedha Bot API (no
        # emoji) fallback. Ab cache clear hote hi FORAN ek retry (fresh
        # download+upload ke saath) hota hai, taake premium emoji wala send
        # hi succeed ho jaye is call mein — Bot API tak jaana hi na pade.
        if _retry and ("reference" in str(e).lower() or "FILE_REFERENCE" in str(e)):
            invalidate_media_cache(file_id)
            logger.warning(f"Userbot media send: stale file reference, retrying fresh once: {e}")
            return await _userbot_send_media_with_emoji(
                bot_app, chat, file_id, file_type, _orig_text, emoji_id,
                keyboard=keyboard, forced_pack=forced_pack, _retry=False)
        logger.error(f"Userbot media send failed: {e}")
        return None

async def _prewarm_media_cache(bot_app):
    """After startup, upload every configured dropped/milestone video ONCE (to the
    userbot's Saved Messages) so the first real alert after a restart is already
    cached (cached=True) instead of a 5-15s download+upload.
    DISABLED by default (MEDIA_PREWARM=1 to re-enable): the cache is in-memory
    only, so EVERY restart re-uploaded everything to Saved Messages and flooded
    it with 10000+ media. Without prewarm, the first alert after a restart is
    just a few seconds slower (download+upload on demand) — no flood."""
    if os.environ.get("MEDIA_PREWARM", "0") != "1":
        return
    try:
        await asyncio.sleep(15)
        if not userbot_client:
            return
        cfg = load_config()
        items = list(cfg.get("dropped_videos", []) or [])
        # Theme pools bhi pre-warm karo (green + green_purple + orange) taake
        # restart ke baad pehli alert bhi fast ho, chahe koi bhi theme active ho.
        items += list(cfg.get("dropped_videos_green_purple", []) or [])
        items += list(cfg.get("dropped_videos_orange", []) or [])
        mm = cfg.get("milestone_media", {}) or {}
        items += [m for m in mm.values() if isinstance(m, dict)]
        done = set()
        for m in items:
            fid = (m or {}).get("file_id") if isinstance(m, dict) else None
            if not fid or fid in done or fid in _MEDIA_TL_CACHE:
                continue
            done.add(fid)
            try:
                ftype = (m.get("type") or "video")
                tmp = _MEDIA_FILE_CACHE.get(fid)
                if not tmp or not os.path.exists(tmp):
                    suffix = ".mp4" if ftype == "video" else ".jpg"
                    safe_id = re.sub(r"[^A-Za-z0-9_-]", "", str(fid))[-64:]
                    tmp = os.path.join(_media_cache_dir(), f"{safe_id}{suffix}")
                    tg_file = await bot_app.get_file(fid)
                    await tg_file.download_to_drive(tmp)
                    _MEDIA_FILE_CACHE[fid] = tmp
                msg = await asyncio.wait_for(
                    userbot_client.send_file("me", tmp, supports_streaming=True), timeout=60)
                if msg is not None and getattr(msg, "media", None) is not None:
                    _MEDIA_TL_CACHE[fid] = msg.media
            except Exception as e_pw:
                logger.warning(f"media prewarm skipped one item: {type(e_pw).__name__}: {e_pw}")
            await asyncio.sleep(2)
        logger.info(f"🔥 Media cache pre-warmed: {len(_MEDIA_TL_CACHE)} item(s) cached")
    except Exception as e:
        logger.warning(f"media prewarm failed: {e}")

def _pack_with_chart(pack):
    """Ensure the pack has a 'chart' key for the renamed 'Chart Link' text.
    The built-in packs (core.py) and the owner's saved custom packs were all
    created before the 'chart' key existed, so pack.get('chart') is None for
    them and the Chart Link emoji glitches (falls through to positional).
    'Chart Link' must show the DEX emoji, so default 'chart' to 'dex'."""
    if not pack:
        return pack
    if pack.get("chart"):
        return pack
    pack = dict(pack)
    pack["chart"] = pack.get("dex") or 0
    return pack

def _get_alert_pack():
    """Return current EMOJI_PACK.
    If the owner has locked a specific pack (via /setemojipack), always use that —
    this avoids the pack silently jumping around whenever channel_post_count
    changes (e.g. after a config reset). Otherwise falls back to auto-rotation
    every 10 posts based on post count."""
    config = load_config()
    locked = config.get("locked_emoji_pack")
    if locked:
        for pack in EMOJI_PACKS:
            if pack["name"] == locked:
                return _pack_with_chart(pack)
    count = config.get("channel_post_count", 0)
    return _pack_with_chart(EMOJI_PACKS[(count // 10) % len(EMOJI_PACKS)])

def _pack_teer(pack):
    """Pack ki apni teer emoji, warna locked default."""
    try:
        return int((pack or {}).get("teer") or TEER_EMOJI_ID)
    except Exception:
        return TEER_EMOJI_ID

_ath_pool_cache: dict = {}

def _gt_get(url, timeout=12):
    try:
        r = _dex_session.get(url, headers={"Accept": "application/json"}, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        logger.debug(f"gecko get failed {url}: {e}")
    return None

def _gt_best_pool(ca, chain, dex_pair_addr=None):
    """(network, pool_address) of the CORRECT pool for this token, or None.

    FIX (real ATH was 70K, bot alerted 24M/100M/200M): this used to pick
    whichever GeckoTerminal pool showed the highest reserve_in_usd, with NO
    check that it's the same pool the token actually trades/was called on,
    and cached that choice FOREVER (no TTL). GeckoTerminal sometimes indexes
    a decoy/fake-liquidity pool with an inflated reserve_in_usd — once that
    wrong pool got cached, every future ATH-backfill tick fed its corrupted
    OHLCV candle highs into peak_mc, and because the plausibility guard
    compares against our own previously-accepted (already wrong) ratio, each
    bad reading raised the ceiling for the next one — a runaway feedback
    loop (70K real -> 24M -> 100M -> 200M).

    Permanent fix: only trust a GeckoTerminal pool whose address MATCHES
    DexScreener's own top-liquidity pair for this token (dex_pair_addr,
    ground truth the bot already relies on for live MC). No match -> skip
    candle-based ATH for this tick instead of guessing. Cache now expires
    (6h) so a stale/wrong lock can't persist indefinitely.
    """
    net = _GT_NET.get((chain or "SOL").upper())
    if not net or not ca:
        return None
    key = f"{net}:{ca}"
    cached = _ath_pool_cache.get(key)
    if cached and (time.time() - cached[2]) < 21600:  # 6h TTL
        return (cached[0], cached[1])
    d = _gt_get(f"https://api.geckoterminal.com/api/v2/networks/{net}/tokens/{ca}/pools?page=1")
    want = (dex_pair_addr or "").strip().lower()
    best, best_liq, matched = None, -1.0, None
    for p in ((d or {}).get("data") or []):
        a = p.get("attributes", {}) or {}
        addr = a.get("address") or ""
        try:
            liq = float(a.get("reserve_in_usd") or 0)
        except (TypeError, ValueError):
            liq = 0.0
        if want and addr.lower() == want:
            matched = addr
        if liq > best_liq and addr:
            best_liq, best = liq, addr
    # If we have DexScreener's ground-truth pair address, ONLY that pool is
    # trustworthy — an unmatched "highest liquidity" pool is exactly the
    # decoy-pool failure mode above, so refuse rather than guess.
    #
    # FIX (AIFORCE/@Bigbangcall incident, 2026-09-18): when `want` itself was
    # empty (DexScreener's dex_pair_addr not available at computation time —
    # e.g. a transient fetch miss), this used to fall back to `best`
    # (GeckoTerminal's own highest-liquidity pool, no cross-check at all) —
    # silently re-opening the exact decoy-pool hole the fix above was meant to
    # close. Real caller posted 26.3K, bot alerted a 950X/26.16M "peak" off a
    # bad candle. Now: no DexScreener ground truth to check against -> skip
    # candle-based ATH for this tick entirely, same as the "no match" case.
    chosen = matched if want else None
    if chosen:
        _ath_pool_cache[key] = (net, chosen, time.time())
        return (net, chosen)
    return None

def fetch_entry_and_peak_sync(ca, chain, since_dt):
    """(entry_price_at_call_time, highest CLOSE price since) from real candles.

    entry_price = OPEN of the candle that contains the call timestamp.
    Both are 0.0 when unknown. MAX_BACKFILL_FIX_NOV11: peak is the max
    CLOSE, not HIGH — a single wick / transient bad candle must not
    create milestones; the close has to confirm the level."""
    dex_pair_addr = ""
    try:
        _d = _fetch_dex_sync(ca, retries=1, min_liquidity=0)
        dex_pair_addr = (_d or {}).get("pair_addr", "") or ""
    except Exception:
        dex_pair_addr = ""
    pool = _gt_best_pool(ca, chain, dex_pair_addr)
    if not pool:
        return (0.0, 0.0)
    net, addr = pool
    age_h = max((datetime.utcnow() - since_dt).total_seconds() / 3600.0, 0.05)
    if   age_h <= 16:  tf, agg, span = "minute", 1,  60
    elif age_h <= 240: tf, agg, span = "minute", 15, 900
    elif age_h <= 960: tf, agg, span = "hour",   1,  3600
    else:              tf, agg, span = "day",    1,  86400
    d = _gt_get(f"https://api.geckoterminal.com/api/v2/networks/{net}/pools/{addr}"
                f"/ohlcv/{tf}?aggregate={agg}&limit=1000&currency=usd")
    lst = (((d or {}).get("data") or {}).get("attributes") or {}).get("ohlcv_list") or []
    import calendar
    since_ts = calendar.timegm(since_dt.utctimetuple())
    peak = 0.0
    entry_price = 0.0
    best_delta = None
    for c in lst:
        try:
            ts, op, hi, cl = int(c[0]), float(c[1]), float(c[2]), float(c[4])
        except (TypeError, ValueError, IndexError):
            continue
        # candle containing / closest to the call moment → entry reference
        if ts <= since_ts < ts + span:
            entry_price = op
            best_delta = 0
        elif best_delta != 0:
            delta = abs(ts - since_ts)
            if op > 0 and (best_delta is None or delta < best_delta):
                best_delta, entry_price = delta, op
        if ts + span < since_ts:      # candle fully before the call
            continue
        # MAX_BACKFILL_FIX_NOV11: peak from CLOSE, not HIGH.
        if cl > peak:
            peak = cl
    return (entry_price, peak)

DROPPED_THEMES = {
    "green": {
        "label": "Green",
        "template": DEFAULT_DROPPED_TEMPLATE,
        "chain_emojis": DROPPED_CHAIN_EMOJIS,
        "pack": _DROPPED_CALL_PACK,
        "video_key": "dropped_videos",
        "index_key": "dropped_video_index",
    },
    "green_purple": {
        "label": "Green + Purple",
        "template": DEFAULT_DROPPED_TEMPLATE_GREEN_PURPLE,
        "chain_emojis": DROPPED_CHAIN_EMOJIS_GREEN_PURPLE,
        "pack": _DROPPED_CALL_PACK_GREEN_PURPLE,
        "video_key": "dropped_videos_green_purple",
        "index_key": "dropped_video_index_green_purple",
    },
    "orange": {
        "label": "Orange",
        "template": DEFAULT_DROPPED_TEMPLATE_ORANGE,
        "chain_emojis": DROPPED_CHAIN_EMOJIS_ORANGE,
        "pack": _DROPPED_CALL_PACK_ORANGE,
        "video_key": "dropped_videos_orange",
        "index_key": "dropped_video_index_orange",
    },
    "red": {
        "label": "Red",
        "template": DEFAULT_DROPPED_TEMPLATE_RED,
        "chain_emojis": DROPPED_CHAIN_EMOJIS_RED,
        "pack": _DROPPED_CALL_PACK_RED,
        "video_key": "dropped_videos_red",
        "index_key": "dropped_video_index_red",
    },
    "golden": {
        "label": "Golden",
        "template": DEFAULT_DROPPED_TEMPLATE_GOLDEN,
        "chain_emojis": DROPPED_CHAIN_EMOJIS_GOLDEN,
        "pack": _DROPPED_CALL_PACK_GOLDEN,
        "video_key": "dropped_videos_golden",
        "index_key": "dropped_video_index_golden",
    },
    "blue": {
        "label": "Blue",
        "template": DEFAULT_DROPPED_TEMPLATE_BLUE,
        "chain_emojis": DROPPED_CHAIN_EMOJIS_BLUE,
        "pack": _DROPPED_CALL_PACK_BLUE,
        "video_key": "dropped_videos_blue",
        "index_key": "dropped_video_index_blue",
    },
}

def _dropped_theme_order():
    """Theme rotation order (DROPPED_THEMES ki insertion order).
    Nayi theme add hogi to rotation me khud aa jayegi."""
    return list(DROPPED_THEMES)

def get_dropped_theme():
    """Agli dropped call ke liye theme (ROTATION).
    Default: green -> green_purple -> orange -> green -> ...
    Agar config['dropped_theme_lock'] set hai to rotation ruka hua hai aur
    wahi ek theme use hoti hai. Unknown value ho to 'green'."""
    try:
        cfg = load_config()
        lock = (cfg.get("dropped_theme_lock") or "").strip().lower()
        if lock and lock in DROPPED_THEMES:
            return lock
        names = _dropped_theme_order()
        idx = cfg.get("dropped_theme_index", 0) or 0
        return names[int(idx) % len(names)]
    except Exception:
        return "green"

def advance_dropped_theme():
    """Rotation ko agle theme par le jao (har dropped call ke baad ek dafa).
    Lock laga ho to kuch nahi karta."""
    try:
        cfg = load_config()
        if (cfg.get("dropped_theme_lock") or "").strip():
            return
        names = _dropped_theme_order()
        idx = cfg.get("dropped_theme_index", 0) or 0
        cfg_set("dropped_theme_index", (int(idx) + 1) % len(names))
    except Exception:
        pass

def set_dropped_theme_next(name):
    """Rotation ko us theme se shuru karo (agli call us theme ki hogi)."""
    names = _dropped_theme_order()
    if name not in DROPPED_THEMES:
        return False
    cfg_set("dropped_theme_index", names.index(name))
    return True

def get_dropped_theme_cfg(name=None):
    """Active (ya di gayi) theme ka config dict."""
    return DROPPED_THEMES[get_dropped_theme() if name is None else
                          (name if name in DROPPED_THEMES else "green")]

def theme_video_list(name=None):
    """Us theme ki video pool (list of {file_id, type})."""
    cfg = get_dropped_theme_cfg(name)
    return load_config().get(cfg["video_key"], []) or []

def theme_video_index(name=None):
    cfg = get_dropped_theme_cfg(name)
    return load_config().get(cfg["index_key"], 0) or 0

def is_call_message(text):
    if not text or len(text) < 10: return False
    tl  = text.lower()
    kw  = ["buy","bought","long","short","entry","target","tp ","tp:","sl ","sl:","signal",
           "gem","moon","moonshot"," call","calls","called","ape","aped","snipe","load","bag",
           "stack","enter","exit","take profit","stop loss","watch","watching","monitor",
           "looking","launch","listed","presale","stealth","new","caller","kol",
           "mc ","mc:","mcap","market cap","fdv","liquidity","volume","chart",
           "ca ","ca:","contract","token","address","bullish","early",
           "dexscreener","dextools","birdeye","geckoterminal","pump.fun","pump","bullx",
           "photon","gmgn","ave.ai","defined.fi","dedust","stonfi","ston.fi","getgems",
           "solana","sol","ethereum","eth","bsc","bnb","base","ton","toncoin",
           "robinhood","rh ","hood","dyor","nfa","not financial advice","research",
           "risk","high risk","low cap","micro cap","soon","now","live","active",
           "fire","hot","good","check",
           "pinksale","pink sale","pinklock","gempad","gem pad","cheesepad","cheese pad",
           "cheesepad.ai","cheesepad.io","pinksale.finance","launchpad","launch pad",
           "fairlaunch","fair launch","fair-launch","presale","pre sale","pre-sale",
           "public sale","private sale","ido","ico","seed round","sale live","sale starts",
           "whitelist","wl round","kyc","audit","doxx","doxxed","refund","refundable",
           "vesting","tge","liquidity lock","lp lock","raised","contribute","contribution",
           "softcap","soft cap","hardcap","hard cap","no hard cap","min buy","max buy",
           "min contribution","max contribution","sale type","pool","pool live","buy now"]

    return any(k in tl for k in kw) or bool(ETH_CA_PATTERN.search(text)) or \
           bool(TON_CA_PATTERN.search(text)) or \
           any(32<=len(s)<=44 for s in SOL_CA_PATTERN.findall(text) if not s.startswith("0x"))

_RECAP_PHRASES = [
    "chart update", "price update", "update:", "reminder:",
    "look where it went", "look how far", "look at it now", "look at this now",
    "since our call", "since we called", "since the call",
    "from our call", "from our original call", "from the call",
    "we called this", "we called it", "called this one", "already called",
    "delivering a move", "delivering an impressive", "delivered a", "delivered an",
    "and look where", "and look how", "early positioning matters",
    "recap", "revisit", "throwback", "flashback",
    "more opportunities like this are on the way",
    "accomplishment unlocked", "kolscreener",
]

_RECAP_PATTERNS = [
    re.compile(r'✅\s*\d+(?:\.\d+)?\s*x\b', re.IGNORECASE),   # "✅ 7.4x $TICKER"
    re.compile(r'called\s*@\s*\$', re.IGNORECASE),            # "Called @ $41.2K"
    re.compile(r'🧢.{0,20}called', re.IGNORECASE),             # "🧢 Called @ ..."
    re.compile(r'milestone\s*alert', re.IGNORECASE),
    re.compile(r'🔝\s*\$'),                                    # "🔝 $304.4K" peak-MC marker
    # OWNER FIX (2026-09-12, WizardScan/74142 MOONCOIN incident): a
    # KOLScreener_bot-style "achievement" repost — "Accomplishment Unlocked:
    # x2!" / "made a **x2+** call on TOKEN" — was slipping through as a fresh
    # call. The token had already pumped by the time this got posted, so the
    # live DexScreener price was used as a fake "entry", and the next tick's
    # real price looked like an instant 3X. Catch this wording explicitly.
    re.compile(r'made\s+a\s*\**x\d+(?:\.\d+)?\+?\**\s*call', re.IGNORECASE),  # "made a **x2+** call"
    re.compile(r'accomplishment\s*unlocked', re.IGNORECASE),
    # FIX (2026-10-21, @christophersalpha/9645 JPMD incident): caller re-posted a
    # "KOL Hit 12X+ ... LEAD KOL - Rank" alert (any language) and it was tracked as a
    # brand-new call at the already-pumped MC (370K instead of 33K).
    re.compile(r'kol\s*hit\s*\d+(?:\.\d+)?\s*x', re.IGNORECASE),
    re.compile(r'lead\s*kol', re.IGNORECASE),
    re.compile(r'\bhit\s*\d+(?:\.\d+)?\s*x\s*\+', re.IGNORECASE),
]

def _is_recap_post(text):
    """True when a post is RE-mentioning a call already made earlier (a
    performance recap / chart update / reposted third-party milestone alert)
    rather than announcing a fresh call."""
    if not text:
        return False
    tl = text.lower()
    if any(p in tl for p in _RECAP_PHRASES):
        return True
    return any(p.search(text) for p in _RECAP_PATTERNS)

def _get_chain_emoji(chain, config=None):
    """Return chain-specific premium emoji ID, or None."""
    if config is None:
        config = load_config()
    chain_emoji_ids = config.get("chain_emoji_ids", {})
    # Normalize chain key: SOL→sol, ETH→eth, BNB→bsc (user uses 'bsc'), BASE→base
    key_map = {"SOL": "sol", "ETH": "eth", "BNB": "bsc", "BSC": "bsc", "BASE": "base", "TON": "ton"}
    key = key_map.get(chain.upper(), chain.lower())
    return chain_emoji_ids.get(key)

DROP_TREE_TOP    = 6005730152422584338

DROP_TREE_MID    = 6007902795989000507

DROP_TREE_BOT    = 6008272700047367045

DROP_ARROW_EMOJI = 6008239542899844540

DROP_SPARK_EMOJI = 6005842186644496003

DROP_RISK_EMOJI  = 6010402879272132590

DROP_DEXSCENER_EMOJI = 5821147954282504203

DROP_DETAILS_EMOJI   = 5821230129891778303

DROP_CHANNEL_EMOJI   = 5821034167713930552

DROP_MEASTRO_EMOJI   = 5821083920615088444

DROP_DETAILS_CHAIN_EMOJIS = {
    "SOL": 5821384894743324105, "ETH": 5821051253093834925,
    "BNB": 5821408568603058588, "BSC": 5821408568603058588,
    "BASE": 5821088567769701396, "RH": 5821306460050562617,
    "TON": 5821137045065572054, "ARC": 5861917123885932251,
}

_GOPLUS_CHAIN_ID = {"ETH": 1, "BNB": 56, "BSC": 56, "BASE": 8453,
                    "ARB": 42161, "POLYGON": 137, "OP": 10, "AVAX": 43114}

_DEAD_ADDRS = {"0x0000000000000000000000000000000000000000",
               "0x000000000000000000000000000000000000dead"}

def _drop_emoji_tag(eid, fallback):
    return f'<tg-emoji emoji-id="{eid}">{fallback}</tg-emoji>'

def _drop_chain_emoji_id(chain):
    return DROP_DETAILS_CHAIN_EMOJIS.get((chain or "").upper(),
                                         DROP_DETAILS_CHAIN_EMOJIS["SOL"])

def _fmt_mc_compact(mc):
    try:
        v = float(mc)
    except Exception:
        return None
    if v <= 0:
        return None
    if v >= 1e9:
        return f"{v/1e9:.1f}B"
    if v >= 1e6:
        return f"{v/1e6:.1f}M"
    if v >= 1e3:
        return f"{v/1e3:.1f}K"
    return f"{v:.0f}"

def _fmt_holders(n):
    try:
        v = int(float(n))
    except Exception:
        return None
    if v <= 0:
        return None
    if v >= 1000000:
        return f"{v/1000000:.1f}M"
    if v >= 1000:
        return f"{v/1000:.1f}k"
    return str(v)

def _fmt_age(created_ms):
    try:
        age_s = time.time() - float(created_ms) / 1000.0
    except Exception:
        return None
    if age_s < 0:
        return None
    if age_s < 3600:
        m = max(1, int(age_s // 60))
        return f"{m} Minute" if m == 1 else f"{m} Minutes"
    if age_s < 86400:
        h = int(age_s // 3600)
        return f"{h} Hour" if h == 1 else f"{h} Hours"
    d = int(age_s // 86400)
    return f"{d} Day" if d == 1 else f"{d} Days"

def _fmt_price(p):
    try:
        v = float(p)
    except Exception:
        return None
    if v <= 0:
        return None
    if v >= 1:
        return f"{v:.2f}$"
    if v >= 0.01:
        return f"{v:.4f}$"
    s = f"{v:.8f}".rstrip("0").rstrip(".")
    return f"{s}$"

def build_dropped_alert(channel, msg_id, ca, chain, entry_fmt, symbol, theme=None):
    """Build the 'Dropped a Call' post text. theme=None ho to rotation ki agli
    theme use hoti hai; theme di ho to wahi (send_dropped_alert wahi bhejta hai)."""
    kol_link  = f"https://t.me/{channel}/{msg_id}" if msg_id else f"https://t.me/{channel}"
    dex_path  = CHAIN_TO_DEXPATH.get((chain or "").upper(), "ethereum")
    chart_url = f"https://dexscreener.com/{dex_path}/{ca}" if ca else "https://dexscreener.com"
    mae_link  = f"{MAESTRO_REFLINK_BASE}{ca}{MAESTRO_REF_SUFFIX}" if ca else f"{MAESTRO_REFLINK_BASE}r-wizard_scan"
    config    = load_config()
    ch_key    = (channel or "").lstrip("@").strip().lower()
    # Active theme ka template default hai (green / green_purple / orange).
    # KOL-specific template (set via /setkoldroppedtemplate) sirf us KOL ke
    # liye jeetta hai (language support) — baaki sab par theme template chalta
    # hai. /setdroppedtemplate (global custom) ab theme se replace ho gaya hai.
    _theme_template = get_dropped_theme_cfg(theme)["template"]
    _lang     = (config.get("channel_language", {}) or {}).get(ch_key)
    if _lang and _lang != "english":
        template = (config.get("lang_dropped_templates", {}).get(_lang)
                     or _theme_template)
    else:
        template  = (config.get("channel_dropped_templates", {}).get(ch_key)
                     or _theme_template)
    details_link = (f"{BOT_LINK}?start=details_{_register_drop_details(channel, ca)}"
                    if ca else BOT_LINK)
    kwargs    = dict(
        channel=html.escape(_display_handle(channel)), symbol=(symbol or "TOKEN").upper(),
        chain=chain, entry=_mc_no_label(entry_fmt), ca=ca,
        kol_link=kol_link, bot_link=BOT_LINK,
        chart_url=chart_url, mae_link=mae_link, dex_link=chart_url,
        details_link=details_link,
    )
    try:
        text = safe_format(template, **kwargs)
    except Exception:
        text = safe_format(DEFAULT_DROPPED_TEMPLATE, **kwargs)
    # ── Dropped-call promo links (up to 3, persistent until manually removed) ──
    # Har promo ka apna premium emoji. 2 khali lines ke baad sabse neeche.
    try:
        promos = (load_config().get("dropped_promo_links") or [])[:3]
        for p in promos:
            if p and p.get("url") and p.get("text"):
                eid = p.get("emoji_id")
                if eid:
                    text += f'\n\n<tg-emoji emoji-id="{eid}">🔮</tg-emoji> <a href="{p["url"]}">{html.escape(p["text"])}</a>'
                else:
                    text += f'\n\n🔮 <a href="{p["url"]}">{html.escape(p["text"])}</a>'
    except Exception:
        pass
    return text

def _remember_drop_post(channel, ca, post_id, is_media):
    """Store the Wizard-Scan 'Dropped a Call' post id on the tracked call so the
    owner can later fix a late-tracked entry MC with /fixmc and the bot edits
    that exact post."""
    if not post_id or not ca:
        return
    key = f"{channel}_{ca}"
    call = tracked_calls.get(key) or next(
        (v for k, v in tracked_calls.items() if k.lower() == key.lower()), None)
    if call is None:
        return
    call["drop_post_id"]    = int(post_id)
    call["drop_post_media"] = bool(is_media)
    call["posted_at"] = datetime.utcnow().isoformat()  # DROPBOT: handoff file ke liye
    _save_tracked()

def _drop_skip_decision(channel, ca):
    """Owner rule (/dropskip): kuch channels din me bohot calls karte hain, to unki
    'Dropped a Call' POST selected pattern se chhodi jati hai — e.g. post 1 / skip 1
    (1 ✅ 2 ❌ 3 ✅ 4 ❌ ...) ya post 1 / skip 2 (1 ✅ 2 ❌ 3 ❌ 4 ✅ ...).

    Sirf dropped POST skip hoti hai. Call tracked rehti hai, record/history me aati
    hai aur uske saare X alerts pehle ki tarah jate hain.

    Return True = post karo, False = skip. Faisla har call ka SIRF EK BAAR hota hai
    (call par 'drop_decision' save hota hai) — taake retry/re-queue counter na bigaare.
    Koi await nahi hai, isliye parallel workers me bhi counter safe rehta hai."""
    try:
        ch = str(channel or "").lstrip("@").strip().lower()
        cfg = load_config()
        rule = (cfg.get("drop_skip_rules") or {}).get(ch)
        if not rule:
            return True
        key = f"{channel}_{ca}"
        call = tracked_calls.get(key) or next(
            (v for k, v in tracked_calls.items() if k.lower() == key.lower()), None)
        if call is None:
            return True            # tracked call nahi (test/manual) — hamesha post
        prev = call.get("drop_decision")
        if prev in ("post", "skip"):
            return prev == "post"
        P = max(1, int(rule.get("post", 1)))
        S = max(0, int(rule.get("skip", 0)))
        counters = cfg.get("drop_skip_counters") or {}
        n = int(counters.get(ch, 0))
        do_post = (n % (P + S)) < P
        counters[ch] = n + 1
        cfg["drop_skip_counters"] = counters
        save_config(cfg)
        call["drop_decision"] = "post" if do_post else "skip"
        _save_tracked()
        return do_post
    except Exception as e:
        logger.warning(f"_drop_skip_decision error (posting normally): {e}")
        return True

async def send_dropped_alert(bot, channel, msg_id, ca, chain, entry_fmt, symbol, respect_skip=True):
    """Post a 'Dropped a Call' alert to the main channel when a new call is first tracked.
    ALWAYS uses the WHITE emoji pack regardless of the active/locked pack setting.

    Flow: Bot sends (always reliable) → Userbot edits caption with premium emojis.
    No file download/re-upload — avoids silent fallback-to-no-emojis bug.
    """
    try:
        if respect_skip and not _drop_skip_decision(channel, ca):
            logger.info(f"⏭️ Dropped POST skipped by /dropskip rule: {symbol} @{channel} "
                        f"(call still tracked + X alerts unchanged)")
            return
        # ROTATION: is call ke liye theme uthao, phir rotation agle theme par
        # (skip hui call slot nahi khati — skip check upar ho chuka hai).
        _theme_name = get_dropped_theme()
        _theme = get_dropped_theme_cfg(_theme_name)
        advance_dropped_theme()
        text = build_dropped_alert(channel, msg_id, ca, chain, entry_fmt, symbol,
                                   theme=_theme_name)
        # Use is theme's chain emoji for the header 🔮 (theme-locked)
        _chain_key = (chain or "").upper()
        _theme_chain = _theme["chain_emojis"]
        # FIX: EVM unknown chain defaults to ETH emoji, not SOL
        if _chain_key == "EVM":
            _drop_em = _theme_chain.get("ETH", DROPPED_CALL_EMOJI)
        else:
            _drop_em = _theme_chain.get(_chain_key, DROPPED_CALL_EMOJI)
        emoji_ids  = [_drop_em, _drop_em]

        # Rotating video from the ACTIVE theme's pool only (theme LOCK —
        # green theme par kabhi orange/purple video nahi aati, aur vice versa)
        config = load_config()
        vids   = config.get(_theme["video_key"], [])
        media  = None
        if vids:
            idx   = config.get(_theme["index_key"], 0) % len(vids)
            media = vids[idx]
            cfg_set(_theme["index_key"], (idx + 1) % len(vids))
        else:
            # DIAG: this is the #1 cause of "every post is text-only" — the
            # rotation list is simply empty, so media is None before we even
            # try to send anything.
            logger.warning(
                f"Dropped alert ({_theme['label']}): NO videos configured "
                f"({_theme['video_key']} list is empty) — this call will post text-only.")

        posted   = False
        sent_id  = None
        fail_reason = None  # DIAG: tracks *why* we fell through to text-only

        # ── Step 1: Userbot sends directly (NO edit = NO pencil mark ✏️) ──────────
        if media and media.get("file_id"):
            fid, ftype = media["file_id"], media.get("type", "video")
            if userbot_client:
                for attempt in (1, 2):  # DIAG/FIX: one retry before giving up on media
                    try:
                        sm = await _userbot_send_media_with_emoji(
                            bot, TARGET_CHANNEL, fid, ftype, text, emoji_ids,
                            forced_pack=_theme["pack"])
                        if sm:
                            posted = True
                            try: _remember_drop_post(channel, ca, sm.id, True)
                            except Exception: pass
                            logger.info(f"Dropped alert media sent by userbot (no pencil): msg_id={sm.id}")
                        break
                    except Exception as e:
                        fail_reason = f"userbot media send failed: {type(e).__name__}: {e}"
                        if attempt == 1:
                            logger.warning(
                                f"Userbot media send failed (attempt 1), retrying once: {e}")
                            await asyncio.sleep(0.25)
                            continue
                        logger.error(f"Userbot media send failed, trying bot fallback: {e}")
            else:
                fail_reason = "userbot_client not connected"
                logger.warning(
                    "Dropped alert: userbot_client is None (disconnected) — "
                    "skipping straight to bot fallback for media.")

            # Fallback: bot sends (no premium emojis, no edit to avoid pencil)
            if not posted:
                try:
                    if ftype == "photo":
                        sm = await bot.send_photo(TARGET_CHANNEL, photo=fid,
                                                  caption=text, parse_mode="HTML")
                    else:
                        sm = await bot.send_video(TARGET_CHANNEL, video=fid,
                                                  caption=text, parse_mode="HTML")
                    posted  = True
                    sent_id = sm.message_id
                    if not sent_id:
                        logger.error(f"Dropped alert bot-fallback returned suspicious message_id={sent_id!r}, raw object: {sm!r}")
                    try: _remember_drop_post(channel, ca, sent_id, True)
                    except Exception: pass
                    logger.info(f"Dropped alert media sent by bot (fallback): msg_id={sent_id}")
                except Exception as e:
                    fail_reason = f"bot media send also failed: {type(e).__name__}: {e}"
                    logger.error(f"Dropped alert media send failed: {e}")

        if not posted:
            if fail_reason:
                logger.warning(f"Dropped alert falling back to TEXT-ONLY — reason: {fail_reason}")
            elif not (media and media.get("file_id")):
                logger.warning("Dropped alert falling back to TEXT-ONLY — reason: no media configured/available")
            # Text-only: userbot sends with emojis in one shot (no edit = no pencil)
            if userbot_client:
                sm = await _userbot_send_with_premium_emoji(
                    TARGET_CHANNEL, text, emoji_id=emoji_ids, forced_pack=_theme["pack"])
                if sm:
                    posted = True
                    try: _remember_drop_post(channel, ca, sm.id, False)
                    except Exception: pass
                    logger.info("Dropped alert text sent by userbot with premium emojis")
            if not posted:
                try:
                    sm = await bot.send_message(TARGET_CHANNEL, text,
                                                parse_mode="HTML", disable_web_page_preview=True)
                    posted  = True
                    sent_id = sm.message_id
                    try: _remember_drop_post(channel, ca, sent_id, False)
                    except Exception: pass
                except Exception as e:
                    logger.error(f"Dropped alert text send failed: {e}")

        # ── Step 2: Edit only if bot-fallback sent (sent_id set) — pencil acceptable here ──
        # FIX: `sent_id` can legitimately end up as 0 in rare cases (a bad
        # response object) — `if sent_id:` treated that as "no ID" and
        # silently skipped this whole step, leaving the post without premium
        # emoji forever. `is not None` catches 0 too, so we still attempt it.
        if posted and sent_id is not None and userbot_client and emoji_ids:
            await asyncio.sleep(0.1)
            if media and media.get("file_id"):
                ok = await _userbot_edit_caption_with_premium_emoji(
                    TARGET_CHANNEL, sent_id, text, emoji_ids,
                    forced_pack=_theme["pack"])
            else:
                ok = await _userbot_edit_with_premium_emoji(
                    TARGET_CHANNEL, sent_id, text,
                    forced_pack=_theme["pack"])
            if not ok:
                logger.warning(f"Dropped alert premium emoji edit failed for msg {sent_id}")

    except Exception as e:
        logger.error(f"send_dropped_alert crash: {e}")

_RT_DROP_QUEUE   = None

_RT_DROP_WORKERS = []

RT_DROP_WORKER_COUNT = max(1, int(os.environ.get("RT_DROP_WORKER_COUNT", "4") or 4))

DROP_SETTLE_SECONDS      = float(os.environ.get("DROP_SETTLE_SECONDS", "0.2") or 0.2)

DROP_CONFIRM_TIMEOUT_SEC = float(os.environ.get("DROP_CONFIRM_TIMEOUT_SEC", "0.5") or 0.5)

def _rt_drop_queue():
    global _RT_DROP_QUEUE
    if _RT_DROP_QUEUE is None:
        _RT_DROP_QUEUE = asyncio.Queue()
    return _RT_DROP_QUEUE

async def _rt_drop_worker_loop(bot):
    q = _rt_drop_queue()
    while True:
        data = await q.get()
        requeued = False
        try:
            age = time.time() - data.get("queued_at", time.time())
            if age > RT_MAX_QUEUE_AGE_SECONDS:
                logger.error(
                    f"⏳ Dropping stale dropped-call post (queued {age:.0f}s ago): "
                    f"{data.get('symbol','?')} @{data.get('channel','?')}")
                continue
            await asyncio.wait_for(send_dropped_alert(
                bot, data["channel"], data["msg_id"], data["ca"],
                data.get("chain", "SOL"), data["entry_fmt"], data["symbol"]),
                timeout=RT_SEND_TIMEOUT_SECONDS)
            logger.info(f"📣 Dropped call posted (real-time, dedicated queue): "
                        f"{data.get('symbol','?')} @{data['channel']}")
        except asyncio.TimeoutError:
            tries = int(data.get("_tries", 0)) + 1
            if tries <= RT_MAX_SEND_RETRIES:
                data["_tries"] = tries
                data["queued_at"] = time.time()
                logger.warning(
                    f"⏱️ drop_worker send timeout ({RT_SEND_TIMEOUT_SECONDS}s) — "
                    f"re-queued (try {tries}/{RT_MAX_SEND_RETRIES}): "
                    f"{data.get('symbol','?')} @{data.get('channel','?')}")
                asyncio.create_task(_rt_drop_queue().put(data))
                requeued = True
            else:
                logger.error(
                    f"⏱️ drop_worker hard-timeout x{tries} — giving up: "
                    f"{data.get('symbol','?')} @{data.get('channel','?')}")
        except Exception as e:
            logger.error(f"drop_worker failed: {e}")
        finally:
            if not requeued:
                _RT_INFLIGHT.discard(f"drop|{data.get('channel')}|{data.get('ca')}")
            q.task_done()

def _rt_ensure_drop_worker(bot):
    global _RT_DROP_WORKERS
    _RT_DROP_WORKERS = [t for t in _RT_DROP_WORKERS if not t.done()]
    while len(_RT_DROP_WORKERS) < RT_DROP_WORKER_COUNT:
        t = asyncio.create_task(_rt_drop_worker_loop(bot),
                                name=f"wizard-realtime-drops-{len(_RT_DROP_WORKERS)+1}")
        _RT_DROP_WORKERS.append(t)

async def rt_enqueue_dropped(bot, channel, msg_id, ca, chain, entry_fmt, symbol):
    """Queue a Dropped-a-Call post the instant the call is detected.
    Uses its own dedicated queue/workers (see _rt_drop_queue above) — never
    waits behind milestone alerts."""
    tag = f"drop|{channel}|{ca}"
    if tag in _RT_INFLIGHT:
        return
    _RT_INFLIGHT.add(tag)
    _rt_ensure_drop_worker(bot)
    await _rt_drop_queue().put({
        "channel": channel, "msg_id": msg_id, "ca": ca,
        "chain": chain, "entry_fmt": entry_fmt, "symbol": symbol,
        "queued_at": time.time(),
    })

async def _process_new_call(bot, channel: str, msg_id, text: str, post_date=None):
    """Process a single new message for a tracked channel.
    Zero blocking — track instantly, dex filled later by milestone_job.
    Returns True if a new call was registered."""
    try:
        channel = channel.lower()   # normalise — prevents same token tracked twice if casing differs
        msg_id_str = str(msg_id)

        # HARD STOP: removed channel ki koi post kabhi process nahi hogi
        # (purani post ka flood aur nayi call dono band).
        if is_channel_removed(channel) or channel not in {c.lower() for c in load_channels()}:
            seen_message_ids[channel].add(msg_id_str)
            return False

        # Mark as seen immediately — no re-processing on next poll
        if msg_id_str in seen_message_ids[channel]:
            return False

        # ── Redeploy anti-spam gate ──────────────────────────────────────────
        # Jab tak startup pre-scan complete nahi hota, boot se PEHLE ki koi bhi
        # post process nahi hogi (sirf seen mark hogi). Boot ke baad ki nayi
        # calls normal chalti rahengi — is liye kuch miss bhi nahi hota.
        if not BOT_READY and post_date:
            try:
                _pd = datetime.fromisoformat(str(post_date).replace("Z", "+00:00")).timestamp()
                if _pd and _pd < BOT_START_TS:
                    seen_message_ids[channel].add(msg_id_str)
                    return False
            except (TypeError, ValueError):
                pass

        # Catch calls posted shortly before a Railway restart. Previously the
        # hard BOT_START_TS-5 cutoff silently discarded every call made while
        # the service was restarting or the userbot was reconnecting.
        #
        # OWNER FIX (2026-09-19, casinoinsiderhere/1099 incident): a call from
        # a SPECIAL channel was silently dropped because the bot had been down
        # ~7 hours before a redeploy — longer than even the old 180-minute
        # SPECIAL_BACKFILL_MINUTES window. A minutes-based cutoff can never be
        # long enough for an unpredictable downtime, so special channels now
        # skip the age cutoff entirely: they always get processed on restart,
        # no matter how old the post is. Deduplication still comes from
        # seen_message_ids, so nothing gets alerted twice. Non-special
        # channels keep the normal time-boxed catch-up window so a long outage
        # doesn't flood the feed with hours of backlog from every channel.
        is_special_channel = channel in set(load_special_channels())
        if post_date:
            try:
                parsed_date = datetime.fromisoformat(str(post_date).replace("Z", "+00:00"))
                post_ts = parsed_date.timestamp()
                # FIX (2026-10-18): special channels used to have NO age limit, so after a data
                # loss / redeploy every old post of a special channel (seen-list empty) was
                # re-processed as a brand-new call -> repeated drops + fake X bursts.
                # They now get a long (default 6h) window; others keep the 30 min one.
                # NOCATCHUP_NOV23: catch-up window forced to 0. After a restart
                # the bot must NEVER re-process old posts as new calls — that
                # is what floods the feed with duplicate drops + fake X
                # alerts (incl. millions-of-X on rugs). Only posts that
                # arrived after this boot are processed.
                lookback_minutes = 0.0
                if post_ts < BOT_START_TS - max(60.0, lookback_minutes * 60):
                    seen_message_ids[channel].add(msg_id_str)
                    return False
            except (TypeError, ValueError):
                pass
        # Do NOT mark the message as seen yet. A realtime Telethon event can
        # arrive without its hidden URL/entity text, while the polling safety
        # net can see that URL a moment later. Marking it seen here would make
        # the polling pass skip the valid call forever.
        #
        # We mark it as seen only after a valid CA is extracted (below).

        # ── Launchpad (PinkSale / GemPad / CheesePad) detection ──────────────
        # Presale posts ka apna CA nahi hota, is liye ye check normal call
        # gating se pehle chalta hai.
        try:
            lp_info = detect_launchpad(text)
        except Exception as e:
            logger.warning(f"detect_launchpad failed @{channel}/{msg_id_str}: {e}")
            lp_info = None
        if lp_info:
            logger.info(f"🩷 launchpad link detected @{channel}/{msg_id_str}: "
                        f"{lp_info.get('platform')} {lp_info.get('chain')} {lp_info.get('address')}")
            if lp_info.get("platform") == "PinkSale":
                asyncio.create_task(
                    send_pinksale_alert(bot, channel, msg_id_str, lp_info, text))
            elif lp_info.get("platform") == "CheesePad":
                asyncio.create_task(
                    send_cheesepad_alert(bot, channel, msg_id_str, lp_info, text))
            else:
                asyncio.create_task(
                    send_launchpad_alert(bot, channel, msg_id_str, lp_info, text))
        elif text and any(k in text.lower() for k in ("pinksale", "cheesepad", "gempad")):
            logger.warning(f"Launchpad mentioned but no usable link @{channel}/{msg_id_str}: "
                           f"{(text or '')[:200]}")


        # A caller message is accepted when the normal call detector matches OR
        # when a real token CA is present. This keeps tracking alive if a caller
        # changes wording/emoji style while still requiring an actual CA/link.
        result = extract_ca(text) if text else None
        if not result:
            result = extract_ca_from_links(text) if text else None
        if not result:
            return False
        if not is_call_message(text):
            logger.info(f"📡 CA-based call accepted despite wording gate @{channel}/{msg_id_str}")

        chain_guess, ca = result

        # HARD STOP: this CA is globally frozen — refuse it for EVERY channel/
        # caller, not just the ones already tracked when /freezecall ran. This
        # is what makes /freezecall CA (no @channel) actually stick when a
        # different caller posts the same CA afterwards.
        if ca.lower() in load_global_ca_bans():
            seen_message_ids[channel].add(msg_id_str)
            _save_seen()
            logger.info(f"⛔ Globally frozen CA ignored @{channel}/{msg_id_str} ca={ca[:12]}...")
            return False

        # Now that this is a real token call, claim the message so the
        # realtime handler + polling safety-net cannot double-process it.
        seen_message_ids[channel].add(msg_id_str)
        _save_seen()
        call_key = f"{channel}_{ca}"
        if call_key in tracked_calls:
            return False

        # FIX (2026-10-21, JPMD @christophersalpha 9607/9635/9645): the call key uses the CA
        # as first posted (often a PAIR address). Once resolved, tracked_calls[key]["ca"]
        # becomes the real token, but the key stays old — so the caller later posting the real
        # token CA (or re-posting our X alert) created a SECOND call at a pumped MC.
        # Same caller + same token == same call, whatever the key says.
        _ca_l = ca.lower()
        for _k_old, _c_old in list(tracked_calls.items()):
            try:
                if (_c_old.get("channel") == channel
                        and (str(_c_old.get("ca", "")).lower() == _ca_l
                             or _k_old.lower() == f"{channel}_{_ca_l}".lower())):
                    logger.info(f"♻️ Duplicate call ignored @{channel}/{msg_id_str} ca={ca[:12]}… "
                                f"(already tracked as {_k_old})")
                    return False
            except Exception:
                continue

        if _is_recap_post(text):
            logger.info(f"⏭️ Skipping recap/chart-update post (not a fresh call) "
                        f"@{channel}/{msg_id_str}: {(text or '')[:120]!r}")
            return False

        # FIX (2026-09-26, "$NFX called at 30K+, bot tracked at 6.9K" —
        # persistent RH-chain wrong-entry-MC reports): the 2026-09-08 owner
        # decision above fully disabled the caller's post-text MC out of a
        # real concern (a channel could lowball its own entry to inflate its
        # X multiplier). But the fix went further than the concern required —
        # post_mc_hint was hardcoded to 0 below, so _reconcile_entry_mc (the
        # function written specifically to compare hint vs live and only
        # trust the hint when it ISN'T suspiciously low, see its own
        # docstring) never even got a hint to check. That left "live chart"
        # as the only source, and on brand-new Robinhood-chain pools the
        # first live DexScreener read is frequently a badly-early/partial
        # price (this is what produced 6.9K/8.7K/8.5K-type entries against
        # real 30K-47K launches, every time — never the other direction).
        # Passing the real extracted hint through to post_mc_hint below lets
        # _reconcile_entry_mc do its job: a hint that is HIGHER than or close
        # to live (a genuine early caller) is trusted; a hint far BELOW live
        # (the manipulation case the 2026-09-08 decision was guarding
        # against) is still rejected in favor of live. Best of both.
        post_mc = extract_mc_from_text(text)
        if post_mc <= 0:
            logger.info(f"🧮 MC-hint extraction found NOTHING @{channel}/{msg_id_str} "
                        f"post text: {(text or '')[:200]!r}")
        else:
            logger.info(f"🧮 MC-hint extracted: {fmt_mc(post_mc)} @{channel}/{msg_id_str} "
                        f"(passed to _reconcile_entry_mc — still rejected if it looks "
                        f"like lowballing vs the live chart)")

        # ── Track immediately — NO dex wait ──────────────────────────────────
        # entry data filled in by milestone_job once token appears on Dexscreener
        tracked_calls[call_key] = {
            "channel":      channel,
            "msg_id":       msg_id_str,
            "ca":           ca,
            "chain":        chain_guess,   # "SOL" or "EVM"
            "entry_mc":     0,             # live-chart-only: never seeded from post text
            "entry_price":  0,
            "entry_fmt":    "N/A",
            "entry_locked": False,         # stays open until live data confirms
            "post_mc_hint": post_mc,       # real hint now — _reconcile_entry_mc guards it
            "symbol":       "",
            "tracked_since": datetime.utcnow().isoformat(),
            "dex_pending":  True,          # milestone_job will fill this in
            "series":       [1.0],         # price-history chart seed
        }
        _hs_register_call(channel, call_key, chain_guess)

        logger.info(f"📌 NEW CALL (instant) @{channel} chain={chain_guess} ca={ca[:12]}...")
        _save_tracked()
        _save_seen()

        # Background task: retry dex until we get data, THEN send dropped alert
        async def _fetch_and_alert():
            call_key_inner = f"{channel}_{ca}"
            dex = None

            # ── REAL-TIME retry schedule (SpyDefi/Kolscope style) ───────────────
            # Phase 1: 12 attempts × 0.25 s = ~3 s (already-indexed token → alert <1 s)
            # Phase 2: 15 attempts × 1 s    = 15 s  (fresh launches)
            # Phase 3: 20 attempts × 4 s    = 80 s  (slow BSC/Base indexing)
            # Phase 4: 12 attempts × 15 s   = 180 s (last resort, still no N/A post)
            # Dense first ~10 s so a dropped-call alert lands within seconds.
            retry_schedule = [(2, 0.2), (1, 0.5), (2, 1), (2, 4), (2, 15), (3, 30)]  # DROP_2SEC_NOV19  # DROP_RETRY_1SEC_NOV19
            # DROPBOT: extra (3, 30) phase — no monitoring_job here to pick up
            # a call later, so slow-indexing tokens get ~2 min of retries.
            _call_detected_at = _t.time()

            def _is_full(d):
                return bool(d and (d.get("mcap") or d.get("price")) and d.get("symbol"))

            # SPEED FIX (2026-10-07): previously a token that DexScreener knew only
            # partially (symbol but no MC, or MC but no symbol — common on EVM) kept
            # the loop below spinning through EVERY phase (4+ minutes) before the
            # Jupiter/Gecko/Moralis fallback was ever tried. Now the fallback runs
            # right after the first two phases (~8s) and again after each later one,
            # and the loop stops as soon as it completes the data.
            async def _early_fallback():
                nonlocal dex
                try:
                    _rc = (dex.get("chain") if dex and dex.get("chain") else chain_guess)
                    fb = await asyncio.wait_for(
                        asyncio.to_thread(_fetch_token_info_fallback_sync, ca, _rc), timeout=4)
                except Exception:
                    return
                if not fb:
                    return
                if dex is None:
                    dex = {"chain": _rc, "mcap": 0, "mcap_fmt": "N/A", "price": 0, "symbol": ""}
                if fb.get("symbol") and not dex.get("symbol"):
                    dex["symbol"] = fb["symbol"]
                if fb.get("mcap", 0) > 0 and not dex.get("mcap", 0) > 0:
                    dex["mcap"] = fb["mcap"]
                    dex["mcap_fmt"] = fb.get("mcap_fmt") or fmt_mc(fb["mcap"])
                    if fb.get("price") and not dex.get("price"):
                        dex["price"] = fb["price"]
                if fb.get("chain") and dex.get("chain") in (chain_guess, "EVM", None):
                    dex["chain"] = fb["chain"]

            _phase_no = 0
            for phase_count, phase_sleep in retry_schedule:
                if _is_full(dex):
                    break  # complete data already found in a previous phase
                if _phase_no >= 2:
                    await _early_fallback()
                    if _is_full(dex):
                        break
                _phase_no += 1
                for _attempt in range(phase_count):
                    try:
                        # A brand-new call must never read an older cached quote.
                        # Bypass the short Dex cache on every retry so the first
                        # usable MC/price is captured as soon as the token indexes.
                        _invalidate_dex_cache(ca)
                        fetched = await asyncio.wait_for(
                            asyncio.to_thread(_fetch_entry_sync, ca, chain_guess, 0),  # Birdeye-first, min_liquidity=0
                            timeout=0.5)  # DROP_2SEC_NOV19
                        if fetched:
                            if fetched.get("mcap") or fetched.get("price"):
                                dex = fetched
                                if _is_full(dex):
                                    break
                            elif fetched.get("symbol") and not dex:
                                dex = fetched  # partial; may be improved later
                    except Exception:
                        pass
                    if _is_full(dex):
                        break
                    await asyncio.sleep(phase_sleep)
                if _is_full(dex):
                    break

            # ── Fallback: use Jupiter / Pump.fun / GeckoTerminal ───────────────
            # Run fallback when ANY of these is missing:
            #   • no data at all (token not on DexScreener yet)
            #   • no symbol (token name missing)
            #   • no market cap (EVM tokens often have price but mc=null on DexScreener)
            has_symbol  = bool(dex and dex.get("symbol"))
            has_mc      = bool(dex and dex.get("mcap", 0) > 0)
            has_price   = bool(dex and dex.get("price", 0) > 0)
            # Pass the already-resolved chain (e.g. "ETH","BNB","BASE") if available,
            # so the fallback targets the right network immediately.
            resolved_chain = (dex.get("chain") if dex and dex.get("chain") else chain_guess)
            if not has_symbol or not has_mc:
                try:
                    fallback = await asyncio.to_thread(
                        _fetch_token_info_fallback_sync, ca, resolved_chain)
                    if fallback:
                        if dex is None:
                            dex = {"chain": resolved_chain, "mcap": 0, "mcap_fmt": "N/A",
                                   "price": 0, "symbol": ""}
                        # Fill ONLY what is missing — never overwrite good existing data
                        if fallback.get("symbol") and not dex.get("symbol"):
                            dex["symbol"] = fallback["symbol"]
                        if fallback.get("mcap", 0) > 0 and not dex.get("mcap", 0) > 0:
                            dex["mcap"]     = fallback["mcap"]
                            dex["mcap_fmt"] = fallback.get("mcap_fmt") or fmt_mc(fallback["mcap"])
                        if fallback.get("chain") and dex.get("chain") in (chain_guess, "EVM", None):
                            dex["chain"] = fallback["chain"]
                        logger.info(
                            f"🔍 Fallback applied: sym={dex.get('symbol')} "
                            f"mc={dex.get('mcap_fmt')} chain={dex.get('chain')} @{channel}")
                except Exception as fe:
                    logger.warning(f"Fallback apply error: {fe}")

            # ── Determine final values ──────────────────────────────────────────
            if dex and (dex.get("mcap") or dex.get("price")):
                # Full data: update tracked call and alert
                if call_key_inner in tracked_calls:
                    _c_upd = tracked_calls[call_key_inner]
                    _c_upd.update({
                        "symbol":      dex.get("symbol", ""),
                        "chain":       dex.get("chain", chain_guess),
                        "dex_pending": False,
                    })
                    # FIX (2026-09-26, "$FARTCOIN posted with a totally wrong
                    # CA" report): when a KOL posts only a DexScreener chart
                    # link (no plain contract line), extract_ca grabs the
                    # PAIR/POOL address from that URL, not the token mint —
                    # _fetch_dex_sync already resolves this correctly behind
                    # the scenes (attaching it as dex["resolved_ca"], see the
                    # "PAIR-ADDRESS FALLBACK" above) and the symbol/MC shown
                    # were already right, but nothing ever copied the
                    # resolved mint back onto the tracked call — so the
                    # "Ca:" line in every alert kept showing the pool
                    # address instead of the real, tradable token address.
                    _resolved_ca = dex.get("resolved_ca")
                    if _resolved_ca and _resolved_ca.lower() != _c_upd.get("ca", "").lower():
                        _dup_of = None
                        for _k2, _c2 in list(tracked_calls.items()):
                            if (_k2 != call_key_inner and _c2.get("channel") == channel
                                    and str(_c2.get("ca", "")).lower() == _resolved_ca.lower()):
                                _dup_of = _k2
                                break
                        if _dup_of:
                            logger.info(f"♻️ Resolved pair→token is already tracked as {_dup_of} — "
                                        f"dropping duplicate {call_key_inner} @{channel}")
                            tracked_calls.pop(call_key_inner, None)
                            _save_tracked()
                            return
                        logger.info(f"🔧 Correcting stored CA to resolved token mint "
                                    f"@{channel}: {_c_upd.get('ca','')[:10]}… → {_resolved_ca[:10]}…")
                        _c_upd["ca"] = _resolved_ca
                    # Entry MC = KOL's stated MC when it is plausible vs the
                    # live chart, otherwise the live chart MC. This kills the
                    # "called at 15K, tracked at 25K" drift without trusting a
                    # wrong number written in the post.
                    if not _c_upd.get("entry_locked"):
                        _live_mc = dex.get("mcap", 0) or 0
                        _hint_mc = _c_upd.get("post_mc_hint", 0) or 0
                        # FIX (2026-09-18, "called at 16K, bot shows 80K" reports):
                        # _age used to be time.time() - _call_detected_at, i.e. how
                        # long OUR OWN retry loop had been running — that's near-zero
                        # even when the underlying Telegram post itself was detected
                        # minutes late (scan gap/flood-wait). So the freshness gate
                        # almost never fired, and a genuinely-early caller's real MC
                        # kept getting outvoted by an already-pumped live price via
                        # _reconcile_entry_mc's anti-cheat path. Use the actual post
                        # timestamp so a late-detected call is correctly recognized
                        # as stale and the caller's stated MC is trusted directly.
                        try:
                            _post_dt = datetime.fromisoformat(str(post_date)) if post_date else None
                            _age = (datetime.utcnow() - _post_dt.replace(tzinfo=None)).total_seconds() if _post_dt else (_t.time() - _call_detected_at)
                        except Exception:
                            _age = _t.time() - _call_detected_at
                        # FIX (2026-10-11, FLW "103K call, bot said 45K, instant 2X"):
                        # on RH, DexScreener can index a token minutes late, so the
                        # 'age > fresh => trust post hint' shortcut then paired an OLD
                        # (or since-edited) hint with a live price already >=2x higher,
                        # firing a fake instant 2X. For RH, when live is already >=2x
                        # the hint, skip the shortcut and let _reconcile_entry_mc
                        # (anti-lowball + confirm retries) decide instead.
                        _rh_stale_hint = (str((dex or {}).get("chain") or chain_guess).upper() == "RH" and _hint_mc > 0
                                          and _live_mc >= _hint_mc * 2
                                          and not dex.get("_mc_is_fdv"))
                        if _hint_mc > 0 and _age > ENTRY_MC_FRESH_SECONDS and not _rh_stale_hint:
                            # quote arrived too late to be "the entry"
                            _entry_mc, _src = (_hint_mc, "post")
                        else:
                            _entry_mc, _src = _reconcile_entry_mc(
                                _hint_mc, _live_mc, live_is_fdv=bool(dex.get("_mc_is_fdv")))
                        logger.info(
                            f"🧮 entry-MC reconcile (real-time) @{channel}: "
                            f"post_hint={fmt_mc(_hint_mc)} live={fmt_mc(_live_mc)} "
                            f"fdv={bool(dex.get('_mc_is_fdv'))} age={_age:.1f}s "
                            f"→ chosen={fmt_mc(_entry_mc)} src={_src}")
                        # Pre-post confirmation: a "live" entry (no usable hint from
                        # the caller's post) can be read mid-index, straight off a
                        # pool that hasn't settled yet — this is what was producing
                        # wrong MCs in the posted alert itself (e.g. 33K posted when
                        # the real entry was 100K). Rather than posting immediately
                        # and silently correcting the internal record 48s later
                        # (which never touched the message already in the channel),
                        # do one short synchronous re-check here so the FIRST number
                        # anyone sees is already the corrected one. This adds ~2-3s
                        # to "instant" alerts for live-priced calls only — calls with
                        # a usable MC hint from the post (src="post") are unaffected
                        # and still post with zero delay.
                        _entry_confirmed = _src not in ("live", "live_fdv", "post_unconfirmed")
                        # FIX (2026-09-30, BABYBUN/@spawnscalls 6.9K-vs-40K incident): RH-only
                        # safeguards below keyed off chain_guess, which is just "EVM" at first
                        # sight of an 0x address — so RH tokens skipped the strict pre-post
                        # confirm loop and locked a half-indexed 6.9K read as entry. Use the chain
                        # dex data has ALREADY resolved (RH/ETH/BNB/BASE) whenever it is known.
                        _eff_chain = chain_guess
                        try:
                            _dc = (dex or {}).get("chain")
                            if _dc and str(_dc).upper() != "EVM":
                                _eff_chain = str(_dc).upper()
                        except Exception:
                            pass
                        _storm_at_detect = _dex_breaker_open()
                        if _src in ("live", "live_fdv", "post_unconfirmed") and _entry_mc > 0:
                            try:
                                if _eff_chain == "RH":
                                    # OWNER REQUIREMENT (2026-09-24): "RH tokens
                                    # should track late — don't post the dropped-
                                    # call alert until the real MC is confirmed."
                                    # RH-chain pairs commonly have NO real
                                    # circulating-MC field anywhere for a while
                                    # after a call (only FDV — see
                                    # _pick_market_cap), so the old single
                                    # ~9s settle window usually still had only
                                    # an FDV/hint number to offer, and the alert
                                    # posted on that anyway with a background
                                    # task correcting it (silently) afterwards.
                                    # Now: keep retrying HERE, before anything
                                    # is posted, until an independent source
                                    # (_confirm_mc_cross_check, which now also
                                    # tries GeckoTerminal — Birdeye doesn't
                                    # cover RH at all) returns a real, non-FDV
                                    # MC, or a hard ceiling is hit so a token
                                    # that can never be confirmed doesn't hang
                                    # forever.
                                    # SPEED FIX (2026-10-07): the full ~80s wait made every RH
                                    # drop call post ~1 min late. Cap the pre-post wait with
                                    # RH_PRE_POST_MAX_SECONDS (default 15s). Set it to 80 on
                                    # Railway to restore the old strict behaviour. If the entry
                                    # is still unconfirmed after the cap, the post goes out
                                    # best-effort and the background correction keeps running.
                                    # RH FIX (2026-10-11, CHNCHL 3.5K-vs-6M incident): RH drop calls now WAIT
                                    # before posting until a REAL (non-FDV) MC is seen on TWO consecutive reads
                                    # that agree within 25%. Never before RH_PRE_POST_MIN_SECONDS (default 10s),
                                    # never longer than RH_PRE_POST_MAX_SECONDS (default 60s). Other chains are
                                    # untouched (fast path below).
                                    _rh_min = float(os.environ.get("RH_PRE_POST_MIN_SECONDS", "10") or 10)
                                    _rh_cap = float(os.environ.get("RH_PRE_POST_MAX_SECONDS", "60") or 60)
                                    _rh_cap = max(_rh_cap, _rh_min)
                                    _rh_steps = (2, 3, 4, 5, 5, 5)
                                    _rh_t0 = _t.time()
                                    _pre_confirm, _pre_cmc = None, 0.0
                                    _prev_real, _rh_i, _rh_ok = 0.0, 0, False
                                    while True:
                                        _el = _t.time() - _rh_t0
                                        if _el >= _rh_cap:
                                            break
                                        _wd = _rh_steps[_rh_i] if _rh_i < len(_rh_steps) else 5
                                        _rh_i += 1
                                        await asyncio.sleep(min(_wd, max(0.0, _rh_cap - _el)))
                                        _invalidate_dex_cache(ca)
                                        try:
                                            _pre_confirm = await asyncio.wait_for(
                                                asyncio.to_thread(_confirm_mc_cross_check, ca, _eff_chain),
                                                timeout=8)
                                        except Exception:
                                            _pre_confirm = None
                                        _pre_cmc = (_pre_confirm or {}).get("mcap", 0) or 0
                                        _real = _pre_cmc if (_pre_cmc > 0 and not (_pre_confirm or {}).get("_mc_is_fdv") and not (_pre_confirm or {}).get("_price_stale")) else 0.0  # ENTRY_DUST_NOV20
                                        if _real > 0:
                                            if (_prev_real > 0
                                                    and max(_real, _prev_real) / min(_real, _prev_real) <= 1.25
                                                    and (_t.time() - _rh_t0) >= _rh_min):
                                                _rh_ok = True
                                                _entry_confirmed = True
                                                break
                                            _prev_real = _real
                                        else:
                                            _prev_real = 0.0
                                    if not _rh_ok:
                                        logger.warning(
                                            f"⚠️ RH entry NOT confirmed (2 agreeing real-MC reads) within "
                                            f"{_rh_cap:.0f}s @{channel} — posting best-effort, background correction continues")
                                    if _pre_cmc > 0 and not (_pre_confirm or {}).get("_mc_is_fdv") and not (_pre_confirm or {}).get("_price_stale") and (  # ENTRY_DUST_NOV20
                                            (_rh_ok and abs(_pre_cmc - _entry_mc) > _entry_mc * 0.08)
                                            or _pre_cmc > _entry_mc * 1.08):
                                        logger.info(
                                            f"🎯 RH pre-post MC confirm (src={(_pre_confirm or {}).get('_source','dex')}, "
                                            f"confirmed={_rh_ok}): {fmt_mc(_entry_mc)} → {fmt_mc(_pre_cmc)} @{channel}")
                                        dex["price"] = _pre_confirm.get("price", dex.get("price", 0))
                                        _entry_mc = _pre_cmc
                                else:
                                    # OWNER REQUIREMENT (2026-09-20): target 3-4s
                                    # total for the drop-call post. Tightened
                                    # further from the 3s+2s (~5s worst case)
                                    # window to 1.5s+1.5s (~3s worst case). This
                                    # is safe to tighten now because two stronger
                                    # backstops exist independently of this step:
                                    # the 48s background _confirm_entry_mc task
                                    # below, and the new universal ratio-jump cap
                                    # in _verified_live_ratio() that rejects any
                                    # future milestone tick implying a >20x jump
                                    # over the call's last confirmed peak — so a
                                    # slightly-off entry from a shorter settle
                                    # window can no longer cascade into a fake
                                    # milestone burst the way it used to.
                                    # OWNER REQUIREMENT (2026-10-11, SUPERSEDED Nov 23 by the user):
                                    # non-RH chains must post NOT before POST_MIN_SECONDS and NOT
                                    # after POST_MAX_SECONDS from detection, with the MC verified
                                    # against fresh independent reads in that window. The user
                                    # now wants the dropped call in 2 SECONDS, so the defaults
                                    # are 1s / 2s (the .env already sets 1/2; this makes the
                                    # code fast even if .env is not loaded).
                                    _nr_min = float(os.environ.get("POST_MIN_SECONDS", "1") or 1)
                                    _nr_max = max(float(os.environ.get("POST_MAX_SECONDS", "2") or 2), _nr_min)
                                    # DROP2SEC_FINAL_NOV23: fetch already returned complete,
                                    # solid data — SKIP the pre-post confirm loop entirely.
                                    # Solid = real (non-FDV) MC >= $100 + symbol present.
                                    # Dust (<$100, the $OWED case) or FDV-only / missing
                                    # symbol still runs the full confirm loop. The 48s
                                    # background confirm + ratio-jump cap correct a
                                    # slightly-off entry, so this cannot fake X alerts.
                                    if (dex.get("mcap", 0) or 0) >= 100 and dex.get("symbol") and not dex.get("_mc_is_fdv"):
                                        _nr_max = 0.0
                                    # SPEED_NOV12: first confirm read starts after 0.5s
                                    # (was 2.0s). The 5s floor, 10s cap and 2-read
                                    # agreement rule are UNCHANGED — this only gives
                                    # the loop more attempts inside the cap when an
                                    # API is slow, so a live entry confirms sooner.
                                    _nr_reads, _nr_i = [], 0
                                    while True:
                                        _el = _t.time() - _call_detected_at
                                        if _el >= _nr_max - 1.0:
                                            break
                                        # DROP2SEC_B_NOV23: reads are 0.5s apart (was 2.5s). The cache
                                        # is invalidated before every read, so each read is a
                                        # fresh API call; 0.5s still gives two independent reads.
                                        await asyncio.sleep(min(0.2 if _nr_i == 0 else 0.5, max(0.0, _nr_max - 1.0 - _el)))
                                        _nr_i += 1
                                        _invalidate_dex_cache(ca)
                                        _tleft = max(1.0, _nr_max - (_t.time() - _call_detected_at) - 0.5)
                                        try:
                                            _pc = await asyncio.wait_for(
                                                asyncio.to_thread(_confirm_mc_cross_check, ca, _eff_chain),
                                                timeout=min(4.0, _tleft))
                                        except Exception:
                                            _pc = None
                                        _pm = (_pc or {}).get("mcap", 0) or 0
                                        if _pm > 0 and not (_pc or {}).get("_mc_is_fdv") and not (_pc or {}).get("_price_stale"):  # ENTRY_DUST_NOV20
                                            _nr_reads.append((_pm, _pc))
                                            if len(_nr_reads) >= 2 and (_t.time() - _call_detected_at) >= _nr_min:
                                                _a, _b = _nr_reads[-1][0], _nr_reads[-2][0]
                                                if max(_a, _b) / min(_a, _b) <= 1.10:
                                                    break   # two fresh reads agree and the 5s floor has passed
                                    if _nr_reads:
                                        _pre_cmc, _pre_confirm = max(_nr_reads, key=lambda x: x[0])
                                        _entry_confirmed = True
                                        if _pre_cmc > _entry_mc * 1.08:
                                            logger.info(
                                                f"🎯 Pre-post MC confirm (src={(_pre_confirm or {}).get('_source','dex')}): "
                                                f"{fmt_mc(_entry_mc)} → {fmt_mc(_pre_cmc)} @{channel}")
                                            dex["price"] = _pre_confirm.get("price", dex.get("price", 0))
                                            _entry_mc = _pre_cmc
                            except Exception as _e_precheck:
                                logger.warning(f"Pre-post MC confirm skipped: {_e_precheck}")
                        if _eff_chain != "RH":
                            _floor_left = float(os.environ.get("POST_MIN_SECONDS", "1") or 1) - (_t.time() - _call_detected_at)
                            if _floor_left > 0:
                                await asyncio.sleep(_floor_left)
                        _entry_price = dex.get("price", 0)
                        if _src in ("post", "post_unconfirmed") and _live_mc > 0 and _entry_price and _entry_mc > 0:
                            # MAX_BACKFILL_FIX_NOV11: only rescale the live
                            # price when the factor is sane [0.5, 2.0]. A
                            # stale/low live MC (e.g. $MAX: live 44.7K vs
                            # post 134K -> 3x) used to inflate entry_price
                            # 3x, and every price-based ratio after it.
                            # Out of band -> keep the live price as-is.
                            _rs = _entry_mc / _live_mc
                            if 0.5 <= _rs <= 2.0:
                                _entry_price = _entry_price * _rs
                            else:
                                logger.warning(
                                    f"\u26a0\ufe0f entry_price rescale skipped @{channel}: "
                                    f"factor {_rs:.2f} outside [0.5, 2.0] "
                                    f"(entry {fmt_mc(_entry_mc)} vs live {fmt_mc(_live_mc)}) "
                                    f"\u2014 live price kept")
                        # LATE_ENTRY_FIX_NOV12: a call detected minutes after the
                        # post (and with NO usable MC hint in the post text) locks
                        # the ALREADY-PUMPED live MC as its entry - e.g. $Max was
                        # really called at ~5K, but the bot detected it ~3 min
                        # late and locked 144K, showing ~28x instead of ~800x.
                        # The hint path above already trusts a hint for stale
                        # posts; this covers the no-hint case. When the post is
                        # older than LATE_ENTRY_HIST_SECONDS, re-price the entry
                        # from the GeckoTerminal 1m candle at the post time
                        # (fetch_entry_and_peak_sync returns the OPEN of the
                        # containing/closest candle - the codebase's entry-price
                        # convention, and the pool's FIRST price for a call made
                        # before the pool existed, where a CLOSE would be the
                        # already-pumped end-of-minute price). Conservative:
                        # any failure falls back to the live entry; a historical
                        # MC is used ONLY when it is real, positive, and BELOW
                        # the live entry (a token only pumps after the post, so
                        # a higher historical MC is impossible and is ignored).
                        # Entry src becomes "post" so the pre-post confirm and
                        # the 48s background task (which only revise UP to the
                        # current pumped price) do not undo the correction.
                        try:
                            _le_hist_seconds = float(os.environ.get("LATE_ENTRY_HIST_SECONDS", "60") or 60)
                            _le_post_dt = None
                            try:
                                _le_post_dt = datetime.fromisoformat(str(post_date).replace("Z", "+00:00"))
                                if _le_post_dt.tzinfo is not None:
                                    _le_post_dt = _le_post_dt.replace(tzinfo=None)
                            except Exception:
                                _le_post_dt = None
                            if (_src in ("live", "live_fdv", "post_unconfirmed")
                                    and _age > _le_hist_seconds and _entry_mc > 0
                                    and _le_post_dt is not None):
                                _le_fn = globals().get("fetch_entry_and_peak_sync")
                                _le_live_price = dex.get("price", 0) or 0
                                _le_hist_mc = 0.0
                                _le_hist_price = 0.0
                                if callable(_le_fn) and _le_live_price > 0 and _live_mc > 0:
                                    try:
                                        _le_hist_price, _le_peak = await asyncio.to_thread(
                                            _le_fn, ca, _eff_chain, _le_post_dt)
                                    except Exception as _le_e:
                                        logger.warning(f"late-entry hist lookup failed @{channel}: {_le_e}")
                                        _le_hist_price = 0.0
                                    if _le_hist_price and _le_hist_price > 0:
                                        _le_hist_mc = _le_hist_price * (_live_mc / _le_live_price)
                                if _le_hist_mc > 0 and _le_hist_mc < _entry_mc:
                                    logger.info(
                                        f"late-entry hist correction @{channel}: "
                                        f"post age {_age:.0f}s, entry {fmt_mc(_entry_mc)} -> {fmt_mc(_le_hist_mc)}")
                                    _entry_mc = _le_hist_mc
                                    _entry_price = _le_hist_price
                                    _src = "post"
                                    _entry_confirmed = True
                                elif _le_hist_mc > 0:
                                    logger.info(
                                        f"late-entry hist lookup ignored @{channel}: "
                                        f"hist {fmt_mc(_le_hist_mc)} >= live entry {fmt_mc(_entry_mc)}")
                                else:
                                    logger.warning(
                                        f"late-entry hist lookup empty @{channel}: age {_age:.0f}s, "
                                        f"keeping live entry {fmt_mc(_entry_mc)}")
                        except Exception as _le_e:
                            logger.warning(f"late-entry correction skipped @{channel}: {_le_e}")
                        _c_upd.update({
                            "entry_mc":    _entry_mc,
                            "entry_price": _entry_price,
                            "entry_fmt":   fmt_mc(_entry_mc) if _entry_mc > 0
                                           else dex.get("mcap_fmt", "N/A"),
                            "entry_src":   _src,
                            "entry_locked": True,
                            # Milestone alerts must not fire off an unconfirmed
                            # FDV-derived guess (see _reconcile_entry_mc) — the
                            # monitoring tick checks this before posting any X.
                            "entry_confirmed": _entry_confirmed,
                        })
                        # FIX: no MC in the caller's post means nothing to sanity-check
                        # the live quote against, and the fast real-time path can grab
                        # a quote within milliseconds of the call — catching a pool
                        # mid-index and reading far BELOW the token's real price (e.g.
                        # 56K locked in as "entry" when the token was already trading
                        # at 300K, manufacturing a fake 5X out of nothing a few minutes
                        # later once the ratio "catches up"). Schedule several
                        # confirmation re-fetches spread over ~48s (one snapshot isn't
                        # enough — slower-indexing chains like BSC/Base can still be
                        # settling well past 3s) and patch the entry up whenever a
                        # higher, more-settled read appears. This runs AFTER the alert
                        # already posted, so it never delays the "instant" dropped-call
                        # alert. Since indexing lag only ever biases LOW, never high,
                        # taking the highest of these reads can never inflate the
                        # caller's own X in their favor, so this introduces no new
                        # fraud risk.
                        if _src in ("live", "live_fdv", "post_unconfirmed") and _entry_mc > 0 and not _entry_confirmed:
                            async def _confirm_entry_mc(_key=call_key_inner, _ca=ca,
                                                         _first_mc=_entry_mc, _chan=channel,
                                                         _chg=_eff_chain, _storm=_storm_at_detect):
                                _best_mc = _first_mc
                                _tighter = _storm and _chg != "RH"
                                # RH already ran an ~80s retry-until-confirmed loop
                                # before posting (see above) — only reaches here if
                                # that loop hit its ceiling, so keep trying with
                                # wider spacing rather than giving up.
                                _delays = ((30, 30, 30, 60, 60) if _chg == "RH" else
                                           (2, 3, 5, 8, 10, 15) if _tighter else
                                           (3, 5, 10, 15, 15))
                                _rev_th = 1.08 if (_tighter or _chg == "RH") else 1.15
                                for _delay in _delays:  # ~3s..48s cumulative (RH: faster, tighter)
                                    await asyncio.sleep(_delay)
                                    _cc = tracked_calls.get(_key)
                                    _src_now = (_cc or {}).get("entry_src")
                                    if (not _cc
                                            or (_src_now not in ("live", "live_fdv", "post_unconfirmed")
                                                and not (_chg == "RH" and _src_now == "post"))):
                                        return  # entry was overridden elsewhere — stop
                                    try:
                                        _invalidate_dex_cache(_ca)
                                        _confirm = await asyncio.wait_for(
                                            asyncio.to_thread(_confirm_mc_cross_check, _ca, _chg),
                                            timeout=8)
                                    except Exception:
                                        continue
                                    # Only treat this as a REAL confirmation when the
                                    # cross-check itself wasn't just another FDV
                                    # fallback (see _pick_market_cap / _better_mc_
                                    # result) — an FDV-flagged "confirm" is exactly
                                    # the unverified guess this whole mechanism
                                    # exists to catch, not evidence against it.
                                    if (_confirm or {}).get("mcap", 0) and not (_confirm or {}).get("_mc_is_fdv") and not (_confirm or {}).get("_price_stale"):  # ENTRY_DUST_NOV20
                                        _cc["entry_confirmed"] = True
                                        _cmc = (_confirm or {}).get("mcap", 0) or 0
                                        if _cmc > _best_mc * _rev_th:  # meaningful upward correction only
                                            _best_mc = _cmc
                                            _cc["entry_mc"]    = _cmc
                                            _cc["entry_fmt"]   = fmt_mc(_cmc)
                                            _cc["entry_price"] = _confirm.get("price", _cc.get("entry_price", 0))
                                            _save_tracked()
                                            logger.info(
                                                f"📈 Entry MC revised up (indexing lag, "
                                                f"src={_confirm.get('_source','dex')}): "
                                                f"{fmt_mc(_first_mc)} → {fmt_mc(_cmc)} @{_chan}")
                                # Fail-safe: even if every cross-check attempt raised
                                # (network trouble etc.) and entry_confirmed never
                                # got set above, never leave a call permanently gated
                                # — that would silently swallow every future X for it.
                                _cc = tracked_calls.get(_key)
                                if _cc is not None:
                                    if (not _cc.get("entry_confirmed")
                                            and _cc.get("entry_src") in ("live", "live_fdv", "post_unconfirmed")):
                                        # confirmed by exhaustion, not by evidence — tag it so
                                        # the early-rebase guard can still correct a bad quote
                                        _cc["entry_src"] = "unverified_fail_safe"
                                    _cc["entry_confirmed"] = True
                            asyncio.create_task(_confirm_entry_mc())

                _save_tracked()
                final_chain  = dex.get("chain", chain_guess)
                final_fmt    = (tracked_calls.get(call_key_inner, {}).get("entry_fmt")
                                if tracked_calls.get(call_key_inner, {}).get("entry_locked")
                                else None) or dex.get("mcap_fmt", "N/A")
                final_symbol = dex.get("symbol", "")
                logger.info(f"📊 dex filled: {final_symbol} {final_fmt} @{channel}")
            elif dex and dex.get("symbol"):
                # Partial data: token is on DEX but price/mc not indexed yet.
                # Still update the symbol so it shows correctly in the alert.
                if call_key_inner in tracked_calls:
                    tracked_calls[call_key_inner].update({
                        "symbol": dex.get("symbol", ""),
                        "chain":  dex.get("chain", chain_guess),
                    })
                _save_tracked()
                final_chain  = dex.get("chain", chain_guess)
                final_fmt    = "N/A"
                final_symbol = dex.get("symbol", "")
                logger.info(f"📊 partial dex: sym={final_symbol} (mc pending) @{channel}")
            else:
                # NO usable data at all → do NOT post an "N/A" alert.
                # The call stays tracked (dex_pending) and monitoring_job will
                # fill it in + post the dropped alert the moment data appears.
                logger.warning(f"⏭️ Alert skipped (no dex data yet, will auto-post later): {ca[:12]}...")
                if call_key_inner in tracked_calls:
                    tracked_calls[call_key_inner]["dex_pending"]  = True
                    tracked_calls[call_key_inner]["alert_pending"] = True
                    _save_tracked()
                return

            # Never post an alert with missing market cap / symbol
            if (not final_fmt or final_fmt == "N/A") or not final_symbol:
                logger.warning(f"⏭️ Alert deferred (incomplete data): {ca[:12]}...")
                if call_key_inner in tracked_calls:
                    tracked_calls[call_key_inner]["alert_pending"] = True
                    _save_tracked()
                return

            # Real-time FIFO: posted immediately, in detection order, never
            # batched behind other channels' posts.
            await rt_enqueue_dropped(
                bot, channel, msg_id_str, ca, final_chain, final_fmt, final_symbol)

        asyncio.create_task(_fetch_and_alert())
        return True
    except Exception as e:
        logger.error(f"_process_new_call @{channel}: {e}")
        return False

_RT_NEW_HANDLER = None

_RT_DELETED_HANDLER = None

_RT_COVERED = set()

async def setup_realtime_monitoring(bot):
    """Register a Telethon event handler so new KOL posts are tracked instantly (< 2 sec).
    Falls back to the 15s polling loop if userbot is not connected.

    Crash-safe: all Telethon calls wrapped so any Telethon internal error (v1.44.0 bugs,
    RuntimeError: Event loop is closed, NoneType await etc.) only logs a warning and
    returns — it never propagates and crashes the Railway process.
    """
    global _bot_ref, userbot_client
    _bot_ref = bot
    if not userbot_client:
        logger.info("Realtime monitoring: no userbot — using polling fallback only")
        return

    # Guard: if userbot is connected but not authorized, skip silently
    try:
        if not await asyncio.wait_for(userbot_client.is_user_authorized(), timeout=10):
            logger.warning("Realtime monitoring: userbot not authorized — skipping")
            return
    except Exception as e:
        logger.warning(f"Realtime monitoring: auth check failed ({e}) — skipping")
        return

    try:
        from telethon import events

        channels = load_channels()
        if not channels:
            logger.info("Realtime monitoring: no channels tracked yet")
            return

        # ── Auto-join + resolve channel entities ─────────────────────────────
        # Telethon only fires NewMessage events for channels the userbot HAS JOINED.
        # get_entity() resolves but does NOT join. JoinChannelRequest joins.
        # SPEED FIX (2026-10-07): this loop used to be strictly sequential
        # (a JoinChannelRequest + get_entity round-trip PER channel). With a few
        # hundred channels it ran past the 90s startup-step timeout, the step was
        # cancelled BEFORE the NewMessage handler was registered, and the bot
        # silently fell back to slow polling (calls tracked ~2 min late, no
        # deleted-post events). Now: resolve concurrently, and only send a
        # join request for channels the userbot has not actually joined.
        from telethon.tl.functions.channels import JoinChannelRequest
        _t0_rt = time.time()
        _rt_sem = asyncio.Semaphore(int(os.environ.get("RT_RESOLVE_CONCURRENCY", "2") or 2))

        async def _resolve_one(ch):
            async with _rt_sem:
                ent = None
                try:
                    _sid = _get_saved_channel_id(ch)
                    if _sid:
                        try:
                            ent = await asyncio.wait_for(userbot_client.get_entity(_sid), timeout=10)
                        except Exception:
                            ent = None
                    if ent is None:
                        ent = await asyncio.wait_for(userbot_client.get_entity(ch), timeout=10)
                    if ent is not None and getattr(ent, "id", None):
                        _save_channel_id(ch, ent.id)
                except Exception:
                    ent = None
                await asyncio.sleep(0.5)
                try:
                    if ent is None or getattr(ent, "left", False):
                        try:
                            await asyncio.wait_for(userbot_client(JoinChannelRequest(ch)), timeout=15)
                        except Exception:
                            pass  # already joined or public access
                        ent = await asyncio.wait_for(userbot_client.get_entity(ch), timeout=10)
                    if ent is not None and not getattr(ent, "left", False):
                        _RT_COVERED.add(str(ch).lstrip("@").lower())
                    return ent
                except Exception as e:
                    logger.warning(f"Realtime: could not resolve/join @{ch}: {e}")
                    return None

        # FIX (2026-10-07, dropped call ~2 min late): the NewMessage handler used to be
        # registered only AFTER every channel was resolved and was bound to that static
        # list, so (a) any channel that failed/hung resolving never got realtime events
        # (only slow round-robin polling = 1-2 min late) and (b) a slow resolve delayed
        # the handler for everybody. Now: resolve with a time budget, register the
        # handler regardless, and make it listen to ALL channel posts (it already
        # filters by tracked username). Unresolved channels keep resolving in the background.
        # FIX (2026-10-11, "calls detected 1-2 min late"): the 12:30 deploy resolved only
        # 31/71 channels in the 40s budget (earlier healthy deploys: 71/71 in ~3s). The
        # session is a StringSession, so the entity cache starts EMPTY on every restart
        # and each get_entity(@username) becomes a rate-limited ResolveUsername call.
        # Channels that did not resolve in time were only caught by the ~2 min polling.
        # One get_dialogs() call fills the cache for every joined channel at once, so the
        # per-channel get_entity() below is answered from cache (no flood-wait).
        try:
            _t_dlg = time.time()
            _dl = await asyncio.wait_for(
                userbot_client.get_dialogs(limit=None),
                timeout=float(os.environ.get("RT_DIALOGS_TIMEOUT_SECONDS", "25") or 25))
            logger.info(f"Realtime: dialogs warmup loaded {len(_dl)} dialogs in {time.time()-_t_dlg:.1f}s")
        except Exception as _e_dlg:
            logger.warning(f"Realtime: dialogs warmup skipped ({type(_e_dlg).__name__}: {_e_dlg})")
        _res_tasks = [asyncio.ensure_future(_resolve_one(c)) for c in channels]
        _budget = float(os.environ.get("RT_RESOLVE_BUDGET_SECONDS", "40") or 40)
        _done, _pending = await asyncio.wait(_res_tasks, timeout=_budget)
        resolved = []
        for _t in _done:
            try:
                _e = _t.result()
                if _e is not None:
                    resolved.append(_e)
            except Exception:
                pass
        logger.info(f"Realtime: resolved {len(resolved)}/{len(channels)} channels in "
                    f"{time.time()-_t0_rt:.1f}s ({len(_pending)} still resolving in background)")
        if not resolved:
            logger.warning("Realtime monitoring: could not resolve any channels yet — "
                           "registering catch-all handler anyway")

        # Remove only our own previous realtime handlers. Do NOT clear every
        # Telethon handler: that can break the userbot login/update pipeline.
        global _RT_NEW_HANDLER  # DROPBOT: no deleted-posts handler here
        try:
            if _RT_NEW_HANDLER is not None:
                userbot_client.remove_event_handler(_RT_NEW_HANDLER)
        except Exception:
            pass

        _RT_ID_USER = globals().setdefault("_RT_ID_USER", {})
        try:
            from telethon import utils as _tl_u0
            for _ent0 in resolved:
                _u0 = (getattr(_ent0, "username", None) or "").lower()
                if _u0:
                    _RT_ID_USER[_tl_u0.get_peer_id(_ent0)] = _u0
                    _RT_ID_USER[getattr(_ent0, "id", 0)] = _u0
        except Exception:
            pass

        @userbot_client.on(events.NewMessage(func=lambda e: bool(getattr(e, "is_channel", False))))
        async def _realtime_handler(event):
            global _bot_ref
            try:
                if _bot_ref is None:
                    return
                _cid = getattr(event, "chat_id", None)
                channels_now = [c.lower() for c in load_channels()]
                username = _RT_ID_USER.get(_cid)
                if not username:
                    # FIX (2026-10-26): pehle KHALI username bhi cache ho jata tha aur us channel ki
                    # saari baad ki posts hamesha ke liye ignore hoti thin (polling sirf slow backstop).
                    # Ab: khali kabhi cache nahi hota, aur channel ke saare usernames (usernames list)
                    # me se wo chuna jata hai jo tracked list me hai.
                    chat = await event.get_chat()
                    _names = []
                    _u1 = (getattr(chat, 'username', None) or "").lower()
                    if _u1:
                        _names.append(_u1)
                    for _un in (getattr(chat, 'usernames', None) or []):
                        _n1 = (getattr(_un, 'username', None) or "").lower()
                        if _n1 and _n1 not in _names:
                            _names.append(_n1)
                    username = next((n for n in _names if n in channels_now), None) or (_names[0] if _names else "")
                    if username:
                        _RT_ID_USER[_cid] = username
                if not username or username not in channels_now:
                    return
                _RT_COVERED.add(username)
                text   = event.raw_text or event.text or ""
                # Telegram "Chart / Buy / DEX" buttons can contain a hidden URL.
                # event.text alone does NOT include MessageEntityTextUrl URLs.
                # Append those URLs before processing so extract_ca_from_links()
                # can see the real CA. This is critical for realtime detection.
                try:
                    ents = getattr(event.message, "entities", None) or []
                    from telethon.tl.types import MessageEntityTextUrl, MessageEntityUrl
                    for ent in ents:
                        if isinstance(ent, MessageEntityTextUrl):
                            if getattr(ent, "url", None):
                                text += " " + ent.url
                        elif isinstance(ent, MessageEntityUrl):
                            raw_msg = getattr(event.message, "message", None) or ""
                            off = int(getattr(ent, "offset", 0) or 0)
                            ln = int(getattr(ent, "length", 0) or 0)
                            if raw_msg and ln > 0:
                                text += " " + raw_msg[off:off + ln]
                except Exception as _entity_e:
                    logger.debug(f"Realtime entity URL extraction skipped: {_entity_e}")

                msg_id = str(event.id)
                if msg_id in seen_message_ids[username]:
                    return
                # NOTE: do NOT add to seen here — _process_new_call owns that state
                await _process_new_call(_bot_ref, username, msg_id, text, getattr(event, "date", None))
            except (RuntimeError, TypeError) as e:
                # Telethon v1.44.0 internal errors — log only, don't re-raise
                logger.debug(f"Realtime handler Telethon noise: {e}")
            except Exception as e:
                logger.error(f"Realtime handler: {e}")

        _RT_NEW_HANDLER = _realtime_handler

        # DROPBOT: deleted-posts handler removed — the main bot owns
        # post-delete penalties. This bot only watches NEW posts.
        logger.info(f"✅ Realtime monitoring active for {len(resolved)} channels")
        # SPEED_NOV12: keep retrying channels the startup resolve missed.
        # (2026-11-11: 0/138 resolved at boot after a FloodWait storm, so
        # realtime was dead and calls were only caught by slow polling.)
        # Every 3 min, retry up to 10 still-uncovered channels (1.5s apart,
        # same pacing as the 2nd-userbot loop) using the same _resolve_one.
        # Bounded and cheap — never a resolve storm. Duplicate guard via
        # _RT_RERESOLVE_TASK so a re-invoked setup does not spawn a 2nd loop.
        async def _rt_re_resolve_loop():
            while True:
                try:
                    await asyncio.sleep(180)
                    if not userbot_client or not userbot_client.is_connected():
                        continue
                    _missing = [c for c in channels
                                if str(c).lstrip("@").lower() not in _RT_COVERED]
                    for _ch in _missing[:10]:
                        try:
                            await _resolve_one(_ch)
                        except Exception:
                            pass
                        await asyncio.sleep(1.5)
                except asyncio.CancelledError:
                    return
                except Exception as _e_rr:
                    logger.debug(f"realtime re-resolve loop: {_e_rr}")
        _rr_existing = globals().get("_RT_RERESOLVE_TASK")
        if _rr_existing is None or _rr_existing.done():
            globals()["_RT_RERESOLVE_TASK"] = asyncio.create_task(_rt_re_resolve_loop())
    except (RuntimeError, TypeError) as e:
        # Suppress Telethon v1.44.0 internal errors (Event loop is closed etc.)
        logger.warning(f"setup_realtime_monitoring Telethon error (suppressed): {e}")
    except Exception as e:
        logger.error(f"setup_realtime_monitoring failed: {e}")



# ═══════════════════════════════════════════════════════════════════════════
# main() — dropbot startup
# ═══════════════════════════════════════════════════════════════════════════
async def _dropbot_main():
    global BOT_START_TS
    BOT_START_TS = time.time()  # is waqt ke baad ki posts hi process hongi
    _load_tracked()
    _load_seen()
    logger.info(f"dropbot starting: {len(tracked_calls)} calls in record, "
                f"target {TARGET_CHANNEL}")
    if not DROPBOT_TOKEN:
        logger.error("DROPBOT_TOKEN .env me nahi — bot start nahi hoga")
        return
    bot = _TG_Bot(token=DROPBOT_TOKEN)
    try:
        me = await bot.get_me()
        logger.info(f"Bot API connected: @{getattr(me, 'username', '?')}")
    except Exception as e:
        logger.error(f"Bot API get_me failed: {e}")
        return
    await _dropbot_connect_userbot()
    if not userbot_client:
        logger.error("Userbot connect nahi hua — realtime monitoring nahi chalegi. "
                     "DROPBOT_SESSION check karo (README dekho).")
        return
    await setup_realtime_monitoring(bot)
    logger.info("dropbot LIVE — dropped calls ke liye ready")
    stop = asyncio.Event()
    try:
        await stop.wait()
    except asyncio.CancelledError:
        pass
    finally:
        _save_tracked()
        _save_seen()
        try:
            if userbot_client:
                await userbot_client.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(_dropbot_main())
    except KeyboardInterrupt:
        logger.info("dropbot stopped (Ctrl+C)")

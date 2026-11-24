# dropbot — WizardScan dropped-call bot (SECOND bot) — Deploy Guide

## Kya hai yeh

`dropbot.py` ek **standalone** dropped-call bot hai. Sirf ONE kaam karta hai:
KOL channels me nayi call aate hi dropped-call alert `@WizardScan` par post
karna (~1-2 second me). Iska apna event loop hai — main bot ke monitoring
jobs, leaderboard, dip, X alerts isko slow nahi kar sakte.

**Yeh main bot ko REPLACE nahi karta.** Main bot X alerts, leaderboard,
dip, champions, Most Shared, Binance — sab kuch pehle ki tarah karta hai.
Sirf dropped-call **post karna** dropbot ko de diya gaya hai.

## Design (safe, no corruption)

- **Dropbot** (is repo me): channels watch karta hai (Telethon userbot
  @Wizard_Scan), call detect karta hai, MC fetch karta hai, dropped alert
  post karta hai, aur apna record `data/dropbot_calls.json` me likhta hai.
- **Main bot**: dropped-call post karna band kar deta hai (neeche
  DROPBOT_MODE flag), aur `dropbot_import_job` se dropbot_calls.json ko
  READ karke calls ko apne `tracked_calls.json` me import karta hai — taake
  X alerts pehle ki tarah chalte rahen.
- **Koi bhi file do processes nahi likhte.** Dropbot sirf apni files
  likhta hai (`dropbot_calls.json`, `dropbot_seen.json`,
  `dropbot_details.json`, apni `config.json`). Main bot ki
  `tracked_calls.json` / `sent_milestones.json` ko dropbot kabhi nahi likhta.

## Files (is folder me)

| File | Size | Kya hai |
|---|---|---|
| `dropbot.py` | ~260 KB | Poora bot (ek file). Live main-bot code se extract kiya gaya. |
| `requirements.txt` | | Python deps. |
| `dropbot.env.example` | | `.env` template — isko `.env` me copy karo. |
| `dropbot-deploy.md` | | Yeh guide. |

## Dropbot kya karta hai / kya NAHI karta

**Karta hai:**
- Real-time Telethon handler se KOL channels watch karna (nocatchup: sirf
  boot ke baad ki posts, purani kabhi nahi).
- CA extract karna (plain CA + DexScreener/ DexTools/BullX/Photon/Birdeye/
  GMGN links se).
- MC fetch karna (DexScreener-first, Birdeye, GeckoTerminal fallback) —
  wahi dust guard, entry reconcile, pre-post confirm, ratio-jump cap jo
  main bot me hain.
- Dropped alert post karna — wahi template, wahi theme rotation, wahi
  premium emojis, wahi video pools (config se).
- Apna record `data/dropbot_calls.json` me likhna (handoff file).

**NAHI karta hai:**
- X alerts, leaderboard, champions, dip, Most Shared, Binance — yeh sab
  main bot par hain.
- Launchpad alerts (PinkSale/GemPad/CheesePad) — yeh main bot par rehte
  hain (neeche flag ki placement dekho — launchpad band nahi hota).
- Main bot ki koi file nahi likhta (tracked_calls.json,
  sent_milestones.json, bot_config.json).

## VPS deploy steps (exact)

### Step 1 — Folder + venv (one-time)

```bash
mkdir -p /opt/dropbot/data
cd /opt/dropbot
python3 -m venv venv
venv/bin/pip install -r requirements.txt
```

### Step 2 — Files copy karo

GitHub `realfarhad55-art/wizu` repo se (ya is folder se SFTP):
- `dropbot.py` -> `/opt/dropbot/dropbot.py`
- `requirements.txt` -> `/opt/dropbot/requirements.txt`
- `dropbot.env.example` -> `/opt/dropbot/dropbot.env.example`

### Step 3 — Config + channels + session copy karo (main bot se)

```bash
cp /opt/bot/data/bot_config.json /opt/dropbot/data/bot_config.json
cp /opt/bot/data/channels.json /opt/dropbot/data/channels.json
cp /opt/bot/data/userbot_session2.txt /opt/dropbot/wizard_scan.session
cp /opt/bot/data/global_ca_bans.json /opt/dropbot/data/global_ca_bans.json
cp /opt/bot/data/removed_channels.json /opt/dropbot/data/removed_channels.json
cp /opt/bot/data/special_channels.json /opt/dropbot/data/special_channels.json
```

Notes:
- `bot_config.json` me templates + dropped videos + promo links hain — is
  liye dropbot wahi template/video use karega jo main bot use karta hai.
  **Jab main bot ki config me template/video badle, to yeh file dobara
  copy karni hogi** (dropbot restart ke saath).
- `channels.json` me tracked KOL channels hain. **Jab channel add/remove
  ho, to yeh file dobara copy karni hogi** (dropbot restart ke saath).
- `userbot_session2.txt` **@Wizard_Scan (userbot 2)** ki StringSession hai.
  Yeh TEXT file hai (SQLite `.session` file nahi). Dropbot is file ko
  padh kar StringSession banata hai. Agar file ka naam alag hai to
  `.env` me `DROPBOT_SESSION` badlo.

### Step 4 — .env banao

```bash
cp /opt/dropbot/dropbot.env.example /opt/dropbot/.env
nano /opt/dropbot/.env
```

Bharo:
- `DROPBOT_TOKEN=` — naya bot token (BotFather se banaya hua).
- `DROPBOT_API_ID=` / `DROPBOT_API_HASH=` — main bot ki `/opt/bot/.env`
  wali `USERBOT_API_ID` / `USERBOT_API_HASH` hi daalo.
- `BIRDEYE_API_KEY=` — main bot ki `/opt/bot/.env` wali key hi daalo.
- Baaki defaults theek hain (session path, target channel, data dir).

**Token/key kabhi chat me nahi — sirf VPS par `.env` me.**

### Step 5 — systemd service

`/etc/systemd/system/dropbot.service` banao:

```ini
[Unit]
Description=WizardScan dropped-call bot (dropbot)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/dropbot
ExecStart=/opt/dropbot/venv/bin/python dropbot.py
Restart=on-failure
RestartSec=10
Environment=DROPBOT_ENV=/opt/dropbot/.env

[Install]
WantedBy=multi-user.target
```

Phir:

```bash
systemctl daemon-reload
systemctl enable dropbot.service
```

**Abhi START mat karo** — pehle neeche main bot wale steps karo (order
zaroori hai, warna duplicate dropped calls aayengi).

---

## Main bot changes (REQUIRED — warna duplicate / X alerts ruk jayengi)

Dropbot start karne se PEHLE main bot me 2 changes chahiye. Dono chhoti
hain. **Yeh changes main bot ki GitHub files par karo (wizu repo), phir
VPS par replace + restart — dropbot repo se nahi.**

### Change 1 — DROPBOT_MODE flag (dropped-call posting band karo)

**File:** `dropped_call.py` (main bot, `/opt/bot/dropped_call.py`).
**Function:** `_process_new_call` (line ~1900).

**Anchor** (exact text, live file se):

```python
        # A caller message is accepted when the normal call detector matches OR
        # when a real token CA is present. This keeps tracking alive if a caller
        # changes wording/emoji style while still requiring an actual CA/link.
        result = extract_ca(text) if text else None
```

**Us anchor se PEHLE yeh add karo:**

```python
        # DROPBOT_MODE (Nov 24 2026): dropped calls are handled by the separate
        # dropbot. This bot only imports them (dropbot_import_job). Launchpad
        # detection above still runs, so presale alerts keep working.
        if os.environ.get("DROPBOT_MODE") == "1":
            return False

```

**Kyun yahan (aur function ke top par nahi):** is point se PEHLE launchpad
detection ho chuki hoti hai (detect_launchpad block), is liye PinkSale /
GemPad / CheesePad alerts **band nahi hote**. Sirf token-call detection +
posting band hoti hai.

**`.env` me add karo** (`/opt/bot/.env`):

```
DROPBOT_MODE=1
```

### Change 2 — dropbot_import_job (calls import karo, X alerts ke liye)

**File:** `dropped_call.py` (main bot) — `_process_new_call` ke baad kahin
bhi add karo. **Register:** `runtime.py` me baaki `run_repeating` jobs ke
saath (line ~4474 ke aas paas).

**Function** (poori, copy-paste ready):

```python
# DROPBOT_IMPORT (Nov 24 2026): dropbot ki posted calls ko import karo.
# Dropbot apna record /opt/dropbot/data/dropbot_calls.json me likhta hai.
# Yeh job use READ karke main bot ke tracked_calls me daalta hai, taake
# X alerts / leaderboard / dip pehle ki tarah chalte rahen.
# Sirf entry_locked calls import hoti hain (dropbot ne entry confirm karke
# alert post kar di hai). Entry revise hone par (48s confirm task) main bot
# ki entry bhi upar update hoti hai.
DROPBOT_CALLS_FILE = "/opt/dropbot/data/dropbot_calls.json"


async def dropbot_import_job(context):
    try:
        with open(DROPBOT_CALLS_FILE, encoding="utf-8") as f:
            incoming = json.load(f) or {}
    except FileNotFoundError:
        return  # dropbot abhi start nahi hua
    except Exception as e:
        logger.warning(f"dropbot_import: read failed: {e}")
        return
    added = 0
    updated = 0
    for call_key, c in incoming.items():
        try:
            if call_key in tracked_calls:
                # Entry revise hui ho to main bot ki entry bhi upar karo
                # (sirf live-src entries, aur sirf meaningful jump par).
                _old = tracked_calls[call_key]
                _new_mc = float(c.get("entry_mc", 0) or 0)
                _old_mc = float(_old.get("entry_mc", 0) or 0)
                if (_new_mc > 0 and _old_mc > 0 and _new_mc > _old_mc * 1.08
                        and _old.get("entry_src") in ("live", "live_fdv", "post_unconfirmed")):
                    _old["entry_mc"] = _new_mc
                    _old["entry_fmt"] = c.get("entry_fmt", _old.get("entry_fmt", "N/A"))
                    _old["entry_price"] = float(c.get("entry_price", 0) or _old.get("entry_price", 0))
                    updated += 1
                continue
            if not c.get("entry_locked"):
                continue  # entry abhi confirm nahi — agle tick par aayegi
            tracked_calls[call_key] = {
                "channel": c.get("channel", ""),
                "msg_id": str(c.get("msg_id", "")),
                "ca": c.get("ca", ""),
                "chain": c.get("chain", "SOL"),
                "entry_mc": float(c.get("entry_mc", 0) or 0),
                "entry_price": float(c.get("entry_price", 0) or 0),
                "entry_fmt": c.get("entry_fmt", "N/A"),
                "entry_locked": True,
                "entry_src": c.get("entry_src", "live"),
                "entry_confirmed": bool(c.get("entry_confirmed", False)),
                "post_mc_hint": float(c.get("post_mc_hint", 0) or 0),
                "symbol": c.get("symbol", ""),
                "tracked_since": c.get("tracked_since", datetime.utcnow().isoformat()),
                "dex_pending": bool(c.get("dex_pending", False)),
                "series": c.get("series", [1.0]),
                "drop_post_id": c.get("drop_post_id"),
                "drop_post_media": bool(c.get("drop_post_media", False)),
            }
            try:
                _hs_register_call(c.get("channel", ""), call_key, c.get("chain", "SOL"))
            except Exception:
                pass
            try:
                # Wahi deterministic sid jo dropbot ne Details link me diya tha
                _register_drop_details(c.get("channel", ""), c.get("ca", ""))
            except Exception:
                pass
            added += 1
        except Exception as e:
            logger.warning(f"dropbot_import: skip {call_key}: {e}")
            continue
    if added or updated:
        _save_tracked()
        logger.info(f"dropbot_import: {added} new, {updated} entry-updated")
```

**Register** (`runtime.py`, baaki jobs ke saath):

```python
    app.job_queue.run_repeating(dropbot_import_job, interval=10, first=20)
```

### Order of operations (zaroori — warna duplicates ya gap)

1. **Main bot:** Change 1 (DROPBOT_MODE flag) + Change 2 (import job) GitHub
   par daalo, VPS par `/opt/bot` me replace karo, `.env` me
   `DROPBOT_MODE=1` add karo, phir `systemctl restart bot.service`.
   - Is point par main bot dropped calls post karna band kar dega, lekin
     X tracking import job se chalti rahegi (jab dropbot start hoga).
2. **Dropbot:** `systemctl start dropbot.service` karo.
   - Dropped calls ab dropbot post karega (~1-2s), main bot import karke
     X alerts dega.
3. Dono ke beech ka gap (restart se dropbot start tak) sirf 1-2 minute
   rakhna — us gap me aane wali calls miss ho sakti hain (dropbot ke
   nocatchup ki wajah se woh purani posts ko process nahi karega).

**Rollback (agar kuch galat ho):** main bot ki `.env` se `DROPBOT_MODE=1`
hatao + restart karo — main bot wapas dropped calls post karne lagega.
Phir `systemctl stop dropbot.service` karo. Dono ek saath ON mat rakho
(duplicate posts aayengi).

---

## Verification checklist

Dropbot start karne ke baad:

```bash
systemctl status dropbot.service --no-pager | head -10
journalctl -u dropbot.service --since "5 minutes ago" --no-pager | tail -30
```

Dekhna chahiye:
- `Userbot connected: @Wizard_Scan` (userbot 2 ki session kaam kar rahi hai)
- `Bot API connected: @<dropbot username>`
- `Realtime monitoring active for N channels`
- `dropbot LIVE — dropped calls ke liye ready`

Phir ek test: kisi tracked KOL channel me ek nayi call aane par:
- Dropped alert @WizardScan par ~1-2s me aani chahiye (video + premium
  emojis ke saath).
- `/opt/dropbot/data/dropbot_calls.json` me woh call aani chahiye
  (entry_mc + drop_post_id ke saath).
- Main bot ki `journalctl -u bot.service` me `dropbot_import: 1 new`
  aana chahiye, aur us call ke X alerts pehle ki tarah aane chahiye.

CPU check:

```bash
ps aux --sort=-%cpu | head -5
```

Dono bots alag processes hain — dropbot ka CPU alag dikhega, main bot ka
alag. Dropped-call load ab main bot par nahi aana chahiye.

## Troubleshooting

| Masla | Wajah / Fix |
|---|---|
| `Userbot connect failed` | DROPBOT_SESSION file nahi mili ya galat hai. `ls -l /opt/dropbot/wizard_scan.session` karo — file honi chahiye (StringSession text). Main bot se dobara copy karo: `cp /opt/bot/data/userbot_session2.txt /opt/dropbot/wizard_scan.session`. |
| `Bot API get_me failed` | DROPBOT_TOKEN galat ya khali hai. `.env` check karo. |
| `Realtime monitoring: no channels tracked yet` | `data/channels.json` khali ya nahi mili. Main bot se dobara copy karo. |
| Dropped alert text-only aati hai (no video) | `data/bot_config.json` me `dropped_videos` list khali hai. Main bot se dobara copy karo (Step 3). |
| Duplicate dropped calls (dono bots post kar rahe) | Main bot me DROPBOT_MODE=1 set nahi hua ya restart nahi hua. `.env` check karo + `systemctl restart bot.service`. |
| X alerts nahi aa rahe (dropped alert aa rahi hai) | Import job register nahi hui ya `dropbot_calls.json` nahi ban rahi. `journalctl -u bot.service \| grep dropbot_import` dekho. Agar file nahi ban rahi to dropbot ka `_save_tracked` / data dir permissions check karo. |
| Dropbot restart par purani calls dobara post ho rahi hain | Nahi honi chahiye — nocatchup (BOT_START_TS) + `dropbot_seen.json` dono rokte hain. Agar ho rahi hain to `data/dropbot_seen.json` delete hui hogi ya data dir badal gaya hoga. |
| `DROPBOT_TOKEN` / keys ki error | `.env` file ka path check karo (`DROPBOT_ENV` ya `/opt/dropbot/.env`). Service file me `Environment=DROPBOT_ENV=/opt/dropbot/.env` hai. |

## Known gaps / limitations (v1)

1. **Launchpad alerts dropbot me nahi hain** — lekin main bot me
   DROPBOT_MODE flag ki placement ki wajah se woh band nahi hote (flag
   launchpad block ke BAAD hai). Agar main bot ka `_process_new_call`
   poora band kar diya jaye to launchpad bhi band ho jayega — aisa mat karo.
2. **No-dex-data calls:** agar DexScreener + fallback ~2 min me bhi koi
   data nahi dete, to dropbot dropped alert skip kar deta hai (call
   `dex_pending` rehti hai, `alert_pending` set hota hai). Main bot us
   call ko import bhi nahi karta (entry_locked nahi). Yeh rare hai
   (token kahin indexed nahi). v2 me iske liye pending-retry job add hogi.
3. **Config/channels sync manual hai:** templates, dropped videos, ya
   channel list badalne par main bot se dobara copy karna hota hai (Step 3).
   Auto-sync v1 me nahi hai.
4. **Entry revise:** dropbot ki 48s background confirm task entry ko upar
   revise kar sakti hai; import job woh revise main bot tak pohunchata hai
   (sirf live-src entries par). Agar revise import ke baad aaye to agle
   import tick (10s) par update ho jayegi.
5. **/fixoldcall, /fixmc, /forcex** main bot par hi rehte hain — dropbot
   ke calls bhi import hone ke baad in commands se fix hote hain (kyunki
   woh main bot ke tracked_calls par chalte hain).
6. **Dropped-call video pools** config se aate hain. Agar config me video
   file_ids purane/expire ho gaye hon to post text-only aayegi (userbot
   fallback) — main bot wala hi behavior hai.

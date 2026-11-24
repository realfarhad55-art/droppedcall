"""apply_dropbot_session_nov24.py - dropbot userbot session detection fix.

Root cause: /opt/dropbot/wizard_scan.session is a StringSession TEXT file
(copied from /opt/bot/data/userbot_session2.txt). Its content uses
URL-safe base64 (has '-' and '_', no '+' or '/'). The old detection only
accepted standard base64 [A-Za-z0-9+/=], so it rejected the string, fell
back to treating the path as a SQLite .session file, and Telethon failed
with "file is not a database". Dropbot exited (Bot API was fine).

Fix: detect by SQLite magic bytes. If the file starts with
"SQLite format 3" it is a real SQLite session (Telethon opens it).
Otherwise the content is a StringSession string (standard or URL-safe
base64) and is loaded via StringSession(content).

Run: python3 apply_dropbot_session_nov24.py [/opt/dropbot]
Then: systemctl restart dropbot.service
Idempotent. Backup: dropbot.py.bak_session_nov24
"""

import os, sys, shutil

MARKER = "DROPBOT_SESSION_NOV24"

OLD = '''    session_arg = DROPBOT_SESSION
    try:
        if os.path.isfile(DROPBOT_SESSION):
            with open(DROPBOT_SESSION, encoding="utf-8", errors="ignore") as f:
                content = f.read().strip()
            # StringSession string: lambi, sirf [A-Za-z0-9+/=] chars
            if len(content) > 100 and all(c in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=" for c in content):
                session_arg = StringSession(content)
                logger.info("Userbot session: StringSession file se load hui")
    except Exception as e:
        logger.warning(f"Session file read failed ({e}) — path ko session name mana jayega")'''

NEW = '''    session_arg = DROPBOT_SESSION
    try:
        if os.path.isfile(DROPBOT_SESSION):
            with open(DROPBOT_SESSION, "rb") as f:
                head = f.read(16)
            if head.startswith(b"SQLite format 3"):
                # Asli SQLite .session file — Telethon khud kholega
                logger.info("Userbot session: SQLite .session file")
            else:
                # DROPBOT_SESSION_NOV24: Text file — StringSession string
                # (standard ya URL-safe base64, dono chalte hain: '-' '_' bhi allowed)
                with open(DROPBOT_SESSION, encoding="utf-8", errors="ignore") as f:
                    content = f.read().strip()
                if content:
                    session_arg = StringSession(content)
                    logger.info("Userbot session: StringSession file se load hui")
    except Exception as e:
        logger.warning(f"Session file read failed ({e}) — path ko session name mana jayega")'''


def main():
    d = sys.argv[1] if len(sys.argv) > 1 else "."
    path = os.path.join(d, "dropbot.py")
    if not os.path.exists(path):
        print("FAIL: %s not found" % path)
        return 1
    with open(path, "r", encoding="utf-8") as f:
        src = f.read()
    if MARKER in src:
        print("OK: already has DROPBOT_SESSION_NOV24, skipping.")
        return 0
    if src.count(OLD) != 1:
        print("FAIL: anchor missing/dup (found %dx) — NOT writing anything."
              % src.count(OLD))
        return 1
    shutil.copy2(path, path + ".bak_session_nov24")
    src = src.replace(OLD, NEW, 1)
    with open(path, "w", encoding="utf-8") as f:
        f.write(src)
    print("OK: session detection fixed (SQLite magic bytes + URL-safe base64).")
    print("Backup: dropbot.py.bak_session_nov24")
    print("Next: systemctl restart dropbot.service")
    return 0


if __name__ == "__main__":
    sys.exit(main())

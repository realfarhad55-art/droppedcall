"""apply_dropbot_media_nov24.py - dropbot media fix (use main bot's cache).

Root cause: dropped-call theme videos are stored in bot_config.json as
file_ids that belong to the MAIN bot. Dropbot's Bot API (@wizdroppedbot)
cannot download them via get_file ("Wrong file_id"), so userbot media
send fails, and the Bot API fallback also fails ("bot is not a member of
the channel chat") -> every dropped call posts TEXT-ONLY.

Fix: the main bot already downloaded all theme videos to disk
(/opt/bot/data/media_cache/, 123 files). The cache filename is
deterministic (safe_id from the file_id + .mp4/.jpg), so the dropbot
checks the main bot's cache FIRST and uses that file directly — no
Bot API download, no file_id needed. The userbot then uploads the local
file, which always works.

E1 - _userbot_send_media_with_emoji (dropped-call media path).
E2 - media prewarm job (same get_file pattern).

Run: python3 apply_dropbot_media_nov24.py [/opt/dropbot]
Then: systemctl restart dropbot.service
Idempotent. Backup: dropbot.py.bak_media_nov24

NOTE: also add @wizdroppedbot as admin to @WizardScan (Bot API fallback
for media/text needs it). That is a Telegram-side step, not code.
"""

import os, sys, shutil

MARKER = "DROPBOT_MEDIA_NOV24"

OLD1 = '''            if not tmp_path or not os.path.exists(tmp_path):
                suffix = _suffix
                safe_id = re.sub(r'[^A-Za-z0-9_-]', '', str(file_id))[-64:]
                tmp_path = os.path.join(_media_cache_dir(), f"{safe_id}{suffix}")
                tg_file = await bot_app.get_file(file_id)
                await tg_file.download_to_drive(tmp_path)
                _MEDIA_FILE_CACHE[file_id] = tmp_path'''

NEW1 = '''            if not tmp_path or not os.path.exists(tmp_path):
                suffix = _suffix
                safe_id = re.sub(r'[^A-Za-z0-9_-]', '', str(file_id))[-64:]
                tmp_path = os.path.join(_media_cache_dir(), f"{safe_id}{suffix}")
                # DROPBOT_MEDIA_NOV24: main bot ki cache me file pehle se hogi
                # (uske file_id se download hui thi). Dropbot ka Bot API main
                # bot ka file_id download nahi kar sakta, is liye main bot ki
                # cache file seedha use karo — re-download ki zaroorat nahi.
                _main_cache = os.path.join(DROPBOT_MAIN_DATA_DIR, "media_cache", f"{safe_id}{suffix}")
                if os.path.exists(_main_cache):
                    tmp_path = _main_cache
                    logger.info(f"Media cache hit (main bot): {os.path.basename(tmp_path)}")
                else:
                    tg_file = await bot_app.get_file(file_id)
                    await tg_file.download_to_drive(tmp_path)
                _MEDIA_FILE_CACHE[file_id] = tmp_path'''

OLD2 = '''                if not tmp or not os.path.exists(tmp):
                    suffix = ".mp4" if ftype == "video" else ".jpg"
                    safe_id = re.sub(r"[^A-Za-z0-9_-]", "", str(fid))[-64:]
                    tmp = os.path.join(_media_cache_dir(), f"{safe_id}{suffix}")
                    tg_file = await bot_app.get_file(fid)
                    await tg_file.download_to_drive(tmp)
                    _MEDIA_FILE_CACHE[fid] = tmp'''

NEW2 = '''                if not tmp or not os.path.exists(tmp):
                    suffix = ".mp4" if ftype == "video" else ".jpg"
                    safe_id = re.sub(r"[^A-Za-z0-9_-]", "", str(fid))[-64:]
                    tmp = os.path.join(_media_cache_dir(), f"{safe_id}{suffix}")
                    # DROPBOT_MEDIA_NOV24: main bot ki cache pehle check karo.
                    _main_cache = os.path.join(DROPBOT_MAIN_DATA_DIR, "media_cache", f"{safe_id}{suffix}")
                    if os.path.exists(_main_cache):
                        tmp = _main_cache
                    else:
                        tg_file = await bot_app.get_file(fid)
                        await tg_file.download_to_drive(tmp)
                    _MEDIA_FILE_CACHE[fid] = tmp'''


def patch_one(path, old, new, tag):
    with open(path, "r", encoding="utf-8") as f:
        src = f.read()
    if src.count(old) != 1:
        print("FAIL: %s anchor missing/dup in %s (found %dx) -- NOT writing "
              "anything." % (tag, path, src.count(old)))
        return False
    shutil.copy2(path, path + ".bak_media_nov24")
    src = src.replace(old, new, 1)
    with open(path, "w", encoding="utf-8") as f:
        f.write(src)
    return True


def main():
    d = sys.argv[1] if len(sys.argv) > 1 else "."
    path = os.path.join(d, "dropbot.py")
    if not os.path.exists(path):
        print("FAIL: %s not found" % path)
        return 1
    with open(path, encoding="utf-8") as f:
        src = f.read()
    if MARKER in src:
        print("OK: already has DROPBOT_MEDIA_NOV24, skipping.")
        return 0
    if not patch_one(path, OLD1, NEW1, "E1"):
        return 1
    print("E1 OK (dropped-call media: main bot cache first)")
    if not patch_one(path, OLD2, NEW2, "E2"):
        return 1
    print("E2 OK (media prewarm: main bot cache first)")
    print("DONE. Backup: dropbot.py.bak_media_nov24")
    print("Next: systemctl restart dropbot.service")
    return 0


if __name__ == "__main__":
    sys.exit(main())

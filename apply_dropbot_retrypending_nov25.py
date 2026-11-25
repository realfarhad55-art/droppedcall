"""apply_dropbot_retrypending_nov25.py - retry dropped calls skipped for "no dex data".

Root cause: when a new KOL call is detected, the dropbot spawns
_fetch_and_alert(), which retries dex data for ~2.2 min. If there is
still NO usable dex data, it logs "Alert skipped (no dex data yet, will
auto-post later)", marks the record dex_pending=True + alert_pending=True
and returns WITHOUT posting. The comment claims "monitoring_job will fill
it in + post the dropped alert the moment data appears" - but the dropbot
has NO monitoring_job and NO milestone_job at all (dropbot.py line 4520
itself says "no monitoring_job here to pick up a call later"). So a
skipped call sits in data/dropbot_calls.json with dex_pending=True +
alert_pending=True FOREVER and never posts. Real case: $BASKET (SOL, ca
3Tg6YNrfPdF...) from @crypto_raven_call/3947 + @cryptofightercall/1958,
detected Nov 25 16:50 CET, skipped 16:52:30 CET - still stuck. A later
count showed ~15 such stuck calls.

Fix (dropbot only - the main bot is NOT touched):
- E1: _retry_pending_once(bot) - one retry pass over every record with
  alert_pending + dex_pending. Fetches FRESH dex data with the same
  _fetch_entry_sync + _fetch_token_info_fallback_sync path the instant
  task uses (incl. the pair-address resolved_ca correction). When full
  data is available (symbol + real mcap) the record is filled in
  (symbol, chain, entry via _reconcile_entry_mc, dex_pending=False,
  alert_pending cleared) and the dropped alert is posted via
  rt_enqueue_dropped with the ORIGINAL msg_id/channel. Still no data:
  the record stays pending, never posted as "N/A", never deleted.
  Plus _rt_retry_pending_loop(bot) (every 5 min, cheap - usually zero
  pending calls, so just a dict scan) and _rt_owner_updates_loop(bot)
  (listens for /retrypending on the dropbot's own Bot API token,
  owner-only via OWNER_IDS, replies with a short summary).
- E2: spawn both loops in _dropbot_main (guarded, like the existing
  _RT_RERESOLVE_TASK pattern, so a re-invoked main never spawns a 2nd).

Run: python3 apply_dropbot_retrypending_nov25.py [/opt/dropbot]
Then: systemctl restart dropbot.service
Idempotent. Backup: dropbot.py.bak_DROPBOT_RETRYPENDING_NOV25
"""

import os, sys, shutil

MARKER = "DROPBOT_RETRYPENDING_NOV25"

E1_OLD = '''async def _process_new_call(bot, channel: str, msg_id, text: str, post_date=None):
    """Process a single new message for a tracked channel.
'''

E1_NEW = '''# DROPBOT_RETRYPENDING_NOV25: a dropped call skipped for "no dex data"
# never auto-posted, because this bot has NO monitoring_job to pick the
# pending record up later (the skip log line promises "will auto-post
# later" but nothing ever retries it). _retry_pending_once() does one
# retry pass over every pending record, _rt_retry_pending_loop() runs it
# every 5 min, and the owner can force one pass right now with
# /retrypending (see _rt_owner_updates_loop).
async def _retry_pending_once(bot):
    """One retry pass over dropped calls skipped for missing dex data.

    For each record with alert_pending + dex_pending, fetch FRESH dex data
    (same _fetch_entry_sync + _fetch_token_info_fallback_sync path the
    instant _fetch_and_alert task uses). When full data is available
    (symbol + real mcap) the record is filled in and the dropped alert is
    posted via rt_enqueue_dropped with the ORIGINAL msg_id/channel.
    Still no data -> the record stays pending, never posted as "N/A",
    never deleted. Returns (retried, posted, still_pending, posted_list,
    pending_list); the lists hold (channel, symbol, ca) tuples.
    """
    _pending = [(k, c) for k, c in list(tracked_calls.items())
                if c.get("alert_pending") and c.get("dex_pending")]
    if not _pending:
        return (0, 0, 0, [], [])
    _posted, _still = [], []
    for _key, _rec in _pending:
        _chan = _rec.get("channel", "")
        _ca = _rec.get("ca", "")
        _msg_id = str(_rec.get("msg_id", ""))
        _chain_guess = _rec.get("chain", "SOL") or "SOL"
        try:
            _invalidate_dex_cache(_ca)
            try:
                _dex = await asyncio.wait_for(
                    asyncio.to_thread(_fetch_entry_sync, _ca, _chain_guess, 0),
                    timeout=8)
            except Exception:
                _dex = None
            # Fallback fills a missing symbol / MC (Jupiter / Pump.fun / Gecko).
            if not (_dex and _dex.get("symbol") and (_dex.get("mcap", 0) or 0) > 0):
                try:
                    _fb = await asyncio.wait_for(
                        asyncio.to_thread(_fetch_token_info_fallback_sync, _ca,
                                          (_dex.get("chain") if _dex and _dex.get("chain") else _chain_guess)),
                        timeout=8)
                except Exception:
                    _fb = None
                if _fb:
                    if _dex is None:
                        _dex = {"chain": _chain_guess, "mcap": 0, "mcap_fmt": "N/A",
                                "price": 0, "symbol": ""}
                    if _fb.get("symbol") and not _dex.get("symbol"):
                        _dex["symbol"] = _fb["symbol"]
                    if _fb.get("mcap", 0) > 0 and not (_dex.get("mcap", 0) or 0) > 0:
                        _dex["mcap"] = _fb["mcap"]
                        _dex["mcap_fmt"] = _fb.get("mcap_fmt") or fmt_mc(_fb["mcap"])
                        if _fb.get("price") and not _dex.get("price"):
                            _dex["price"] = _fb["price"]
                    if _fb.get("chain") and _dex.get("chain") in (_chain_guess, "EVM", None):
                        _dex["chain"] = _fb["chain"]
            _full = bool(_dex and _dex.get("symbol") and (_dex.get("mcap", 0) or 0) > 0)
            if not _full:
                logger.info(f"retrypending: still no dex data, stays pending: "
                            f"@{_chan} ca={_ca[:12]}...")
                _still.append((_chan, _dex.get("symbol", "") if _dex else "", _ca))
                continue
            # Full data: fill the record the same way _fetch_and_alert does.
            _live_mc = _dex.get("mcap", 0) or 0
            _hint_mc = _rec.get("post_mc_hint", 0) or 0
            _entry_mc, _src = _reconcile_entry_mc(
                _hint_mc, _live_mc, live_is_fdv=bool(_dex.get("_mc_is_fdv")))
            _entry_price = _dex.get("price", 0) or 0
            # Pair-address CA -> resolved token mint (same correction as the
            # instant path). If the resolved mint is already tracked under
            # another key for this channel, drop this duplicate instead.
            _final_ca = _ca
            _resolved = _dex.get("resolved_ca")
            if _resolved and str(_resolved).lower() != str(_ca).lower():
                _dup = None
                for _k2, _c2 in list(tracked_calls.items()):
                    if (_k2 != _key and _c2.get("channel") == _chan
                            and str(_c2.get("ca", "")).lower() == str(_resolved).lower()):
                        _dup = _k2
                        break
                if _dup:
                    logger.info(f"retrypending: resolved pair->token already tracked as {_dup} - "
                                f"dropping duplicate {_key} @{_chan}")
                    tracked_calls.pop(_key, None)
                    _save_tracked()
                    continue
                _final_ca = _resolved
            # The entry is the CURRENT live MC (or the caller's hint when it
            # wins the reconcile) - never below the real price, so no fake X
            # can fire from a retried call. Mark it confirmed when the MC is
            # a real live read: the instant path's own 48s confirm task ends
            # in the same state for live entries, and the main bot's
            # monitoring needs entry_confirmed before it posts X alerts.
            _confirmed = bool(_live_mc > 0 and not _dex.get("_mc_is_fdv"))
            _rec.update({
                "symbol": _dex.get("symbol", ""),
                "chain": _dex.get("chain", _chain_guess),
                "ca": _final_ca,
                "entry_mc": _entry_mc,
                "entry_price": _entry_price,
                "entry_fmt": fmt_mc(_entry_mc) if _entry_mc > 0 else _dex.get("mcap_fmt", "N/A"),
                "entry_src": _src,
                "entry_locked": True,
                "entry_confirmed": _confirmed,
                "dex_pending": False,
                "alert_pending": False,
            })
            _save_tracked()
            _final_chain = _dex.get("chain", _chain_guess)
            _final_fmt = _rec.get("entry_fmt") or _dex.get("mcap_fmt", "N/A")
            _final_symbol = _dex.get("symbol", "")
            await rt_enqueue_dropped(bot, _chan, _msg_id, _final_ca,
                                     _final_chain, _final_fmt, _final_symbol)
            logger.info(f"retrypending: posted late alert {_final_symbol} "
                        f"{_final_fmt} @{_chan} ca={_ca[:12]}...")
            _posted.append((_chan, _final_symbol, _ca))
        except Exception as _e:
            logger.warning(f"retrypending: retry failed @{_chan} ca={_ca[:12]}...: {_e}")
            _still.append((_chan, _rec.get("symbol", ""), _ca))
    return (len(_pending), len(_posted), len(_still), _posted, _still)


async def _rt_retry_pending_loop(bot):
    """DROPBOT_RETRYPENDING_NOV25: every 5 min, retry dropped calls that
    were skipped for missing dex data. Cheap: usually zero pending calls,
    so the pass is a single dict scan and no network at all. A pass that
    is still running is never overlapped by the next tick."""
    while True:
        try:
            await asyncio.sleep(300)
            if globals().get("_RETRYPENDING_RUNNING"):
                logger.info("retrypending: previous pass still running, skipping this tick")
                continue
            globals()["_RETRYPENDING_RUNNING"] = True
            try:
                _n, _p, _s, _pl, _sl = await _retry_pending_once(bot)
            finally:
                globals()["_RETRYPENDING_RUNNING"] = False
            if _n:
                logger.info(f"retrypending pass: {_n} retried, {_p} posted, {_s} still pending")
        except asyncio.CancelledError:
            return
        except Exception as _e:
            logger.debug(f"retrypending loop: {_e}")


async def _rt_owner_updates_loop(bot):
    """DROPBOT_RETRYPENDING_NOV25: listen for /retrypending on the dropbot's
    own Bot API token (@wizdroppedbot). Owner-only: only OWNER_IDS may
    trigger a pass; everyone else is ignored silently. Long-poll via
    get_updates - nothing else polls this token, so no conflict."""
    _offset = None
    while True:
        try:
            _updates = await bot.get_updates(offset=_offset, timeout=25)
        except asyncio.CancelledError:
            return
        except Exception as _e:
            logger.debug(f"owner updates poll: {_e}")
            await asyncio.sleep(5)
            continue
        for _u in _updates or []:
            try:
                _offset = _u.update_id + 1
            except Exception:
                continue
            try:
                _msg = getattr(_u, "message", None)
                if not _msg:
                    continue
                _text = (getattr(_msg, "text", "") or "").strip()
                if not _text.startswith("/retrypending"):
                    continue
                _from = getattr(_msg, "from_user", None)
                _uid = getattr(_from, "id", None) if _from else None
                if _uid not in OWNER_IDS:
                    continue
                _chat_id = getattr(_msg, "chat_id", None)
                if not _chat_id:
                    continue
                _n, _p, _s, _pl, _sl = await _retry_pending_once(bot)
                if _n == 0:
                    _reply = "No pending calls - nothing to retry."
                else:
                    _lines = [f"Retried {_n} pending call(s): {_p} posted, {_s} still pending."]
                    for _ch, _sym, _ca in _pl:
                        _lines.append(f"Posted: ${_sym or '?'} @{_ch} ({str(_ca)[:8]}...)")
                    for _ch, _sym, _ca in _sl:
                        _lines.append(f"Still pending: ${_sym or '?'} @{_ch} ({str(_ca)[:8]}...)")
                    _reply = "\\n".join(_lines)
                await bot.send_message(chat_id=_chat_id, text=_reply)
            except Exception as _e:
                logger.debug(f"owner updates handle: {_e}")


async def _process_new_call(bot, channel: str, msg_id, text: str, post_date=None):
    """Process a single new message for a tracked channel.
'''

E2_OLD = '''    await setup_realtime_monitoring(bot)
    logger.info("dropbot LIVE — dropped calls ke liye ready")
'''

E2_NEW = '''    await setup_realtime_monitoring(bot)
    # DROPBOT_RETRYPENDING_NOV25: retry loop + owner /retrypending listener
    # (guarded so a re-invoked main never spawns a 2nd copy of either).
    _rp_existing = globals().get("_RT_RETRYPENDING_TASK")
    if _rp_existing is None or _rp_existing.done():
        globals()["_RT_RETRYPENDING_TASK"] = asyncio.create_task(_rt_retry_pending_loop(bot))
    _ou_existing = globals().get("_RT_OWNER_UPDATES_TASK")
    if _ou_existing is None or _ou_existing.done():
        globals()["_RT_OWNER_UPDATES_TASK"] = asyncio.create_task(_rt_owner_updates_loop(bot))
    logger.info("dropbot LIVE — dropped calls ke liye ready")
'''

EDITS = [
    ("E1", E1_OLD, E1_NEW),
    ("E2", E2_OLD, E2_NEW),
]


def main():
    d = sys.argv[1] if len(sys.argv) > 1 else "."
    path = os.path.join(d, "dropbot.py")
    if not os.path.exists(path):
        print("FAIL: %s not found" % path)
        return 1
    with open(path, "r", encoding="utf-8") as f:
        src = f.read()
    if MARKER in src:
        print("SKIP: already has DROPBOT_RETRYPENDING_NOV25, nothing changed.")
        return 0
    for name, old, _new in EDITS:
        if src.count(old) != 1:
            print("FAIL: %s anchor missing/dup (found %dx) - NOT writing anything."
                  % (name, src.count(old)))
            return 1
    shutil.copy2(path, path + ".bak_DROPBOT_RETRYPENDING_NOV25")
    for _name, old, new in EDITS:
        src = src.replace(old, new, 1)
    with open(path, "w", encoding="utf-8") as f:
        f.write(src)
    print("OK: E1 retry pass + 5-min loop + /retrypending listener added.")
    print("OK: E2 loops spawned in _dropbot_main.")
    print("Backup: dropbot.py.bak_DROPBOT_RETRYPENDING_NOV25")
    print("Next: systemctl restart dropbot.service")
    return 0


if __name__ == "__main__":
    sys.exit(main())

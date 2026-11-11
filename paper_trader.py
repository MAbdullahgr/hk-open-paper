#!/usr/bin/env python3
"""
HK OPEN PAPER TRADER  (headless: runs by itself on GitHub Actions)
Places NO orders and needs NO API keys. It only reads public Binance prices.

RULE-B (frozen - do not change while a test is running):
  Weekdays 01:02:00 UTC: compare each futures price with the previous weekday's
  08:00 UTC price (the Hong Kong close). Gap >= +1.0%  -> paper SHORT.
  Gap <= -1.0% -> logged as a "shadow long" only. Exit at 01:32:00 and 02:00:00 UTC.
  Fills use the real order book for NOTIONAL dollars, plus FEE_PCT round trip.

Modes:  python3 paper_trader.py check     test Binance access (no trading)
        python3 paper_trader.py dry       fake 10-second run, nothing saved
        python3 paper_trader.py session   the real daily session (default)
        python3 paper_trader.py report    rebuild data/summary.md and data/trades.csv
"""
import csv, glob, json, math, os, sys, time, urllib.error, urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

SYMBOLS = ["HK1810USDT", "ZHONGJIUSDT", "ZHIPUUSDT", "MINIMAXUSDT", "BYDUSDT"]
ENTRY, EXIT_A, EXIT_B = "01:02:00", "01:32:00", "02:00:00"      # UTC
REF_TIME = "08:00:00"                                            # previous weekday, UTC
GAP_MIN = 1.0          # percent
FEE_PCT = 0.10         # round-trip taker fees, percent
NOTIONAL = 100.0       # dollars per paper trade (order-book depth used for the fill)
MAX_LATE_ENTRY = 20    # seconds; later than this the entry is skipped (no look-ahead)
MAX_LATE_EXIT = 120    # seconds; later exits are saved but not counted
TARGET_DAYS = 20
# Hong Kong market holidays that fall on weekdays. CHECK the HKEX calendar and edit.
HOLIDAYS = {"2026-12-25", "2027-01-01"}
API = os.environ.get("PAPER_API", "https://fapi.binance.com")
DATA = os.environ.get("PAPER_DATA", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))

# ------------------------------------------------------------------ time
_offset = 0.0
def now():
    return time.time() + _offset

def at_utc(ds, hms):
    return datetime.fromisoformat(ds + "T" + hms + "+00:00").timestamp()

def ds_of(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")

def hms_of(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%H:%M:%S")

def iso_of(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

def is_weekday(ds):
    return date.fromisoformat(ds).weekday() < 5

def prev_weekday(ds):
    d = date.fromisoformat(ds) - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.isoformat()

def sleep_until(ts):
    while True:
        r = ts - now()
        if r <= 0:
            return
        time.sleep(max(min(r - 0.3, 30), 0.01) if r > 0.5 else 0.01)

def log(msg):
    print(hms_of(now()) + " UTC  " + msg, flush=True)

# ------------------------------------------------------------------ binance
def http_get(path, timeout=8):
    req = urllib.request.Request(API + path, headers={"User-Agent": "Mozilla/5.0 hk-open-paper"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read()[:160].decode("utf-8", "replace")
        raise RuntimeError("HTTP %d %s" % (e.code, body))
    except Exception as e:
        raise RuntimeError(str(e))

def sync_clock():
    global _offset
    t0 = time.time()
    r = http_get("/fapi/v1/time", 5)
    t1 = time.time()
    _offset = r["serverTime"] / 1000.0 - (t0 + t1) / 2
    return _offset

def fill_price(levels, notional):
    rem, qty, cost = notional, 0.0, 0.0
    for lv in levels:
        price, avail = float(lv[0]), float(lv[0]) * float(lv[1])
        if not price > 0:
            continue
        take = min(rem, avail)
        qty += take / price; cost += take; rem -= take
        if rem <= 1e-9:
            break
    if qty <= 0:
        return float("nan"), False
    return cost / qty, rem <= 1e-6

def snapshot(sym):
    t0 = now()
    with ThreadPoolExecutor(2) as ex:
        fb = ex.submit(http_get, "/fapi/v1/ticker/bookTicker?symbol=" + sym)
        fd = ex.submit(http_get, "/fapi/v1/depth?symbol=%s&limit=20" % sym)
        bk, dp = fb.result(), fd.result()
    t1 = now()
    bid, ask = float(bk["bidPrice"]), float(bk["askPrice"])
    sell, ok1 = fill_price(dp.get("bids", []), NOTIONAL)
    buy, ok2 = fill_price(dp.get("asks", []), NOTIONAL)
    return {"t": t1, "lat_ms": round((t1 - t0) * 1000), "bid": bid, "ask": ask, "mid": (bid + ask) / 2,
            "spread_pct": (ask / bid - 1) * 100, "vwap_sell": sell, "vwap_buy": buy, "depth_ok": bool(ok1 and ok2)}

def load_ref(ds, sym):
    pd_ = prev_weekday(ds)
    want = at_utc(pd_, REF_TIME)
    k = http_get("/fapi/v1/klines?symbol=%s&interval=1m&startTime=%d&limit=1" % (sym, int(want * 1000)))
    if not k:
        raise RuntimeError("no candle returned")
    if abs(k[0][0] / 1000.0 - want) > 60:
        raise RuntimeError("candle is %d min away from 08:00" % round(abs(k[0][0] / 1000.0 - want) / 60))
    return float(k[0][1])

# ------------------------------------------------------------------ session files
def session_path(ds):
    return os.path.join(DATA, "sessions", ds + ".json")

def load_session(ds):
    p = session_path(ds)
    if os.path.exists(p):
        with open(p) as f:
            return json.load(f)
    return None

def save_session(s):
    os.makedirs(os.path.join(DATA, "sessions"), exist_ok=True)
    with open(session_path(s["date"]), "w") as f:
        json.dump(s, f, indent=1)

def new_session(ds):
    return {"date": ds, "status": "pending", "note": "", "entry_done": False,
            "exit_done": {"A": False, "B": False}, "trades": [], "watch": []}

# ------------------------------------------------------------------ the strategy
def run_entry(ds, refs, dry=False, force_short=False, gap_min=GAP_MIN):
    t_entry = at_utc(ds, ENTRY)
    def one(sym):
        try:
            return sym, snapshot(sym), None
        except Exception as e:
            return sym, None, str(e)
    with ThreadPoolExecutor(len(SYMBOLS)) as ex:
        results = list(ex.map(one, SYMBOLS))
    watch, trades = [], []
    usable = 0
    for sym, snap, err in results:
        ref = refs.get(sym)
        if err or snap is None or (ref is None and not force_short):
            watch.append({"sym": sym, "gap": None, "note": err or "no prior close"})
            log("ENTRY %s: skipped (%s)" % (sym, err or "no prior close"))
            continue
        usable += 1
        gap = (snap["mid"] / ref - 1) * 100 if ref else float("nan")
        watch.append({"sym": sym, "gap": gap, "mid": snap["mid"]})
        is_short = (gap >= gap_min) if ref else False
        is_long_shadow = (gap <= -gap_min) if ref else False
        if not (is_short or is_long_shadow or force_short):
            continue
        short = is_short or force_short
        snap = dict(snap)
        snap["late_sec"] = snap["t"] - t_entry
        trades.append({"sym": sym, "side": "short" if short else "long", "shadow": not short,
                       "gap": gap, "ref": ref, "top1": False, "entry": snap, "res": {}})
    if usable == 0:
        raise RuntimeError("no usable data for any symbol at entry")
    shorts = sorted([t for t in trades if not t["shadow"]], key=lambda t: -(t["gap"] if t["gap"] == t["gap"] else -1e9))
    if shorts:
        shorts[0]["top1"] = True
    log(("DRY " if dry else "") + "ENTRY done: %d short, %d shadow-long, %d symbols checked" %
        (len(shorts), len(trades) - len(shorts), len(SYMBOLS)))
    return watch, trades

def run_exit(ds, trades, which, dry=False):
    t_exit = at_utc(ds, EXIT_A if which == "A" else EXIT_B)
    def one(t):
        try:
            return snapshot(t["sym"]), None
        except Exception as e:
            return None, str(e)
    with ThreadPoolExecutor(max(1, len(trades))) as ex:
        snaps = list(ex.map(one, trades))
    for t, (s, err) in zip(trades, snaps):
        if s is None:
            t["res"][which] = {"missed": True, "note": err}
            log("EXIT %s %s: failed (%s)" % (which, t["sym"], err))
            continue
        e = t["entry"]
        if t["side"] == "short":
            gross = (e["vwap_sell"] - s["vwap_buy"]) / e["vwap_sell"] * 100
            ideal = (e["mid"] - s["mid"]) / e["mid"] * 100
            px = s["vwap_buy"]
        else:
            gross = (s["vwap_sell"] - e["vwap_buy"]) / e["vwap_buy"] * 100
            ideal = (s["mid"] - e["mid"]) / e["mid"] * 100
            px = s["vwap_sell"]
        late = 0.0 if dry else s["t"] - t_exit
        t["res"][which] = {"t": s["t"], "late_sec": late, "valid": late <= MAX_LATE_EXIT,
                           "depth_ok": bool(s["depth_ok"] and e["depth_ok"]), "mid": s["mid"], "vwap": px,
                           "gross": gross, "ideal": ideal, "net": gross - FEE_PCT}
    log(("DRY " if dry else "") + "EXIT %s recorded for %d trade(s)" % (which, len(trades)))

def run_session(ds=None):
    ds = ds or ds_of(now())
    log("Session date %s" % ds)
    if not is_weekday(ds) or ds in HOLIDAYS:
        log("Weekend/holiday: nothing to do.")
        s = load_session(ds) or new_session(ds)
        s["status"] = "skipped"; s["note"] = "weekend or Hong Kong holiday"
        save_session(s); safe_rebuild(); return s
    s = load_session(ds)
    if s and s["status"] in ("complete", "missed", "skipped"):
        log("Already handled (%s). Exiting." % s["status"]); return s
    s = s or new_session(ds)
    t_e, t_a, t_b = at_utc(ds, ENTRY), at_utc(ds, EXIT_A), at_utc(ds, EXIT_B)
    try:
        off = sync_clock(); log("Clock offset vs Binance: %+d ms" % round(off * 1000))
    except Exception as e:
        log("Clock sync failed (%s); using local clock" % e)
    if now() > t_e + MAX_LATE_ENTRY and not s["entry_done"]:
        s["status"] = "missed"; s["note"] = "job started too late (%s UTC)" % hms_of(now())
        save_session(s); safe_rebuild(); log(s["note"]); return s
    refs, errs = {}, {}
    for sym in SYMBOLS:
        for attempt in range(3):
            try:
                refs[sym] = load_ref(ds, sym); break
            except Exception as e:
                errs[sym] = str(e); time.sleep(1.5)
    s["refs"] = refs
    if not refs:
        s["status"] = "blocked"; s["note"] = "no prior closes: " + "; ".join(sorted(set(errs.values())))[:200]
        save_session(s); safe_rebuild(); log("BLOCKED: " + s["note"])
        raise SystemExit(1)
    save_session(s)
    log("Prior closes loaded for %d/%d symbols. Waiting for %s UTC..." % (len(refs), len(SYMBOLS), ENTRY))
    sleep_until(t_e)
    last_err = None
    while now() - t_e <= MAX_LATE_ENTRY:
        try:
            s["watch"], s["trades"] = run_entry(ds, refs)
            s["entry_done"] = True; s["status"] = "in-position"
            if not s["trades"]:
                s["note"] = "no symbol passed the gap filter"
            save_session(s); safe_rebuild(); last_err = None; break
        except Exception as e:
            last_err = str(e); log("Entry attempt failed: " + last_err); time.sleep(1)
    if not s["entry_done"]:
        s["status"] = "missed"; s["note"] = "entry failed: " + str(last_err)
        save_session(s); safe_rebuild(); log(s["note"]); raise SystemExit(1)
    for which, t_x in (("A", t_a), ("B", t_b)):
        sleep_until(t_x)
        if s["trades"]:
            run_exit(ds, s["trades"], which)
        s["exit_done"][which] = True
        if s["exit_done"]["A"] and s["exit_done"]["B"]:
            s["status"] = "complete"
        save_session(s); safe_rebuild()
    log("Session complete.")
    return s

# ------------------------------------------------------------------ results
def all_sessions():
    out = []
    for p in sorted(glob.glob(os.path.join(DATA, "sessions", "*.json"))):
        try:
            with open(p) as f:
                out.append(json.load(f))
        except Exception:
            pass
    return out

def settle_stale(sessions):
    today = ds_of(now())
    for s in sessions:
        if s["date"] < today and s["status"] == "in-position":
            for which in ("A", "B"):
                if not s["exit_done"][which]:
                    for t in s["trades"]:
                        t["res"].setdefault(which, {"missed": True, "note": "job stopped before exit"})
                    s["exit_done"][which] = True
            s["status"] = "complete"; save_session(s)

def collect(sessions, which, top1_only=False):
    rows = []
    for s in sessions:
        for t in s["trades"]:
            if t["shadow"]:
                continue
            r = t["res"].get(which)
            if not r or r.get("missed") or not r.get("valid"):
                continue
            if top1_only and not t["top1"]:
                continue
            rows.append((s["date"], t, r))
    return rows

def mean(a):
    return sum(a) / len(a) if a else float("nan")

def sd(a):
    if len(a) < 2:
        return float("nan")
    m = mean(a)
    return math.sqrt(sum((x - m) ** 2 for x in a) / (len(a) - 1))

def compute_stats(sessions, which, top1_only=False):
    rows = collect(sessions, which, top1_only)
    by_day = {}
    for d, t, r in rows:
        by_day.setdefault(d, []).append(r["net"])
    day_vals = [mean(v) for v in by_day.values()]
    nets = [r["net"] for _, _, r in rows]
    wins, losses = [x for x in nets if x > 0], [x for x in nets if x <= 0]
    dm, dsd = mean(day_vals), sd(day_vals)
    tstat = dm / (dsd / math.sqrt(len(day_vals))) if len(day_vals) >= 3 and dsd and dsd > 0 else float("nan")
    return {"n": len(rows), "days": len(by_day), "avg_trade": mean(nets), "avg_day": dm, "t": tstat,
            "win": len(wins) / len(nets) * 100 if nets else float("nan"), "avg_win": mean(wins),
            "avg_loss": mean(losses), "worst": min(nets) if nets else float("nan"),
            "ideal": mean([r["ideal"] for _, _, r in rows]),
            "slip": mean([r["ideal"] - r["gross"] for _, _, r in rows]),
            "usd": mean(nets) / 100 * NOTIONAL if nets else float("nan")}

def guidance(st):
    f = lambda v, d=2: ("%.*f" % (d, v)) if v == v else "n/a"
    if st["days"] < 3:
        return "COLLECTING DATA: %d of %d signal days so far. Too early for any conclusion." % (st["days"], TARGET_DAYS)
    if st["days"] < TARGET_DAYS:
        if st["days"] >= 10 and st["avg_day"] < -0.3:
            return "EARLY WARNING: after %d signal days the 02:00 exit averages %s%% per day. Keep collecting, but do NOT use real money." % (st["days"], f(st["avg_day"]))
        return "COLLECTING DATA: %d of %d signal days. Average so far %s%% per day (02:00 exit). Do not decide yet." % (st["days"], TARGET_DAYS, f(st["avg_day"]))
    if st["avg_day"] > 0.15 and st["t"] >= 1.5:
        return "HOLDING UP: %d days, average %s%% per day with real fills (t=%s). Minimum bar to consider a tiny live test. Not proof." % (st["days"], f(st["avg_day"]), f(st["t"], 1))
    if st["avg_day"] > 0:
        return "UNCLEAR: %d days, average %s%% per day (t=%s). Positive but not convincing. Keep collecting to about 40 days." % (st["days"], f(st["avg_day"]), f(st["t"], 1))
    return "FAILING: %d days, average %s%% per day with real fills. The backtest edge did not survive real prices." % (st["days"], f(st["avg_day"]))

CSV_HEAD = ["date", "symbol", "side", "shadow", "top1", "gap_pct", "prior_close", "entry_time_utc", "entry_late_s",
            "entry_latency_ms", "entry_mid", "entry_bid", "entry_ask", "entry_spread_pct", "entry_vwap_fill", "depth_ok",
            "notional_usd"] + [f"exit{w}_{k}" for w in "AB" for k in
            ("time_utc", "late_s", "valid", "mid", "vwap_fill", "ideal_pct", "gross_pct", "net_pct")]

def safe_rebuild():
    try:
        rebuild()
    except Exception as e:
        log("WARNING: could not rebuild summary files (%s). Session data is still saved." % e)

def rebuild(sessions=None):
    sessions = sessions if sessions is not None else all_sessions()
    settle_stale(sessions)
    os.makedirs(DATA, exist_ok=True)
    with open(os.path.join(DATA, "trades.csv"), "w", newline="") as f:
        w = csv.writer(f); w.writerow(CSV_HEAD)
        for s in sessions:
            for t in s["trades"]:
                e = t["entry"]
                row = [s["date"], t["sym"], t["side"], t["shadow"], t["top1"], t["gap"], t["ref"], iso_of(e["t"]),
                       e["late_sec"], e["lat_ms"], e["mid"], e["bid"], e["ask"], e["spread_pct"],
                       e["vwap_sell"] if t["side"] == "short" else e["vwap_buy"], e["depth_ok"], NOTIONAL]
                for wch in "AB":
                    r = t["res"].get(wch)
                    if not r:
                        row += [""] * 8
                    elif r.get("missed"):
                        row += ["missed"] + [""] * 7
                    else:
                        row += [iso_of(r["t"]), r["late_sec"], r["valid"], r["mid"], r["vwap"], r["ideal"], r["gross"], r["net"]]
                w.writerow([round(v, 6) if isinstance(v, float) and v == v else ("" if isinstance(v, float) else v) for v in row])
    fm = lambda v, d=2, suf="%": ("%.*f%s" % (d, v, suf)) if v == v else "-"
    stB = compute_stats(sessions, "B")
    lines = ["# HK Open Paper Trader - summary", "",
             "Updated: %s UTC" % datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"), "",
             "**" + guidance(stB) + "**", "",
             "Progress: %d / %d signal days" % (stB["days"], TARGET_DAYS), "",
             "| Exit | Trades | Days | Avg net/trade | Avg net/day | t | Win | Avg win | Avg loss | Worst | Ideal | Slippage | $/trade |",
             "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for label, which, top in (("01:32", "A", False), ("02:00", "B", False), ("02:00, 1 trade/day (biggest gap)", "B", True)):
        st = compute_stats(sessions, which, top)
        lines.append(("| %s | %d | %d" + " | %s" * 9 + " | $%s |") % (
            label, st["n"], st["days"], fm(st["avg_trade"]), fm(st["avg_day"]), fm(st["t"], 1, ""), fm(st["win"], 0),
            fm(st["avg_win"]), fm(st["avg_loss"]), fm(st["worst"]), fm(st["ideal"]), fm(st["slip"]), fm(st["usd"], 2, "")))
    lines += ["", "Net = after 0.10%% fees and real order-book fills for $%d. Ideal = mid-to-mid (what the backtest assumed)." % NOTIONAL,
              "", "## Days", "", "| Date | Status | Short trades | Shadow longs | Note |", "|---|---|---|---|---|"]
    for s in sorted(sessions, key=lambda x: x["date"], reverse=True)[:60]:
        sh = sum(1 for t in s["trades"] if not t["shadow"])
        lines.append("| %s | %s | %d | %d | %s |" % (s["date"], s["status"], sh, len(s["trades"]) - sh, s.get("note", "")))
    with open(os.path.join(DATA, "summary.md"), "w") as f:
        f.write("\n".join(lines) + "\n")

# ------------------------------------------------------------------ check / dry
def check():
    lines = ["Connection check %s UTC" % datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), "API: " + API]
    allok = True
    try:
        off = sync_clock(); lines.append("Server clock offset: %+d ms" % round(off * 1000))
    except Exception as e:
        lines.append("TIME FAIL: %s" % e); allok = False
    ds = ds_of(now())
    for sym in SYMBOLS:
        t0 = time.time()
        try:
            snap = snapshot(sym)
            ref = load_ref(ds, sym)
            lines.append("OK   %-12s price %.4f spread %.3f%% latency %d ms depth %s prior close %.4f" % (
                sym, snap["mid"], snap["spread_pct"], round((time.time() - t0) * 1000),
                "enough" if snap["depth_ok"] else "THIN for $%d" % NOTIONAL, ref))
        except Exception as e:
            allok = False; lines.append("FAIL %-12s %s" % (sym, e))
    lines.append("RESULT: " + ("ALL OK - the daily session can run from here." if allok else
                 "PROBLEM - see FAIL lines. HTTP 451/403 means Binance blocks this server's location."))
    text = "\n".join(lines)
    print(text)
    os.makedirs(DATA, exist_ok=True)
    with open(os.path.join(DATA, "check.txt"), "w") as f:
        f.write(text + "\n")
    return 0 if allok else 1

def dry():
    try:
        sync_clock()
    except Exception:
        pass
    log("DRY RUN: pretending every symbol is a short, exiting after 10 s. Nothing is saved.")
    ds = ds_of(now())
    watch, trades = run_entry(ds, {}, dry=True, force_short=True)
    time.sleep(10)
    run_exit(ds, trades, "B", dry=True)
    for t in trades:
        r = t["res"]["B"]
        if r.get("missed"):
            log("  %s: exit failed" % t["sym"]); continue
        log("  %s: ideal %.3f%%  real-fill %.3f%%  net %.3f%%  (spread %.3f%%)" % (
            t["sym"], r["ideal"], r["gross"], r["net"], t["entry"]["spread_pct"]))
    return 0

def main(argv):
    mode = argv[1] if len(argv) > 1 else "session"
    if mode == "check":
        return check()
    if mode == "dry":
        return dry()
    if mode == "report":
        rebuild(); print("Rebuilt data/trades.csv and data/summary.md"); return 0
    if mode == "session":
        run_session(); return 0
    print(__doc__); return 2

if __name__ == "__main__":
    sys.exit(main(sys.argv))

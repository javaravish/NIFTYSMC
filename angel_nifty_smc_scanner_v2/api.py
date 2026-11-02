from datetime import datetime, timedelta
from pathlib import Path
from collections import deque
import logging
import threading
import uuid
import math
import json
import hashlib
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from angel_client import AngelClient, Credentials
from scanner import build_timeframes, analyze_smc_stack, historical_replay_all

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nifty-smc-api")
BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"
app = FastAPI(title="NIFTY SMC Research Lab API", version="3.17.0")
app.add_middleware(CORSMiddleware, allow_origins=["null", "http://127.0.0.1", "http://localhost",
    "http://127.0.0.1:5500", "http://localhost:5500", "https://javaravish.github.io"], allow_credentials=False,
    allow_methods=["GET", "POST"], allow_headers=["*"])
_angel_client = None
_live_lock = threading.RLock()
_live = {
    "price": None, "timestamp": None, "sequence": None, "forming": None,
    "history_loaded": False, "history_error": None
}
_live_candles = deque(maxlen=5000)
LIVE_CACHE_FILE = Path.home() / ".nifty_smc_research_lab" / "live_1m_candles.csv"
LIVE_HISTORY_COOLDOWN_FILE = Path.home() / ".nifty_smc_research_lab" / "live_history_last_request.txt"
_historical_fetch_lock = threading.Lock()
_last_live_history_fetch = None
LIVE_HISTORY_COOLDOWN_SECONDS = 300
CACHE_DIR = Path.home() / ".nifty_smc_research_lab" / "historical_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
RESULT_CACHE_DIR = Path.home() / ".nifty_smc_research_lab" / "backtest_results"
RESULT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
_history_jobs = {}
_history_jobs_lock = threading.RLock()

# Paper-only auto strategy state. This never submits Angel One orders.
_paper_lock = threading.RLock()
_paper_state = {
    "enabled": False, "quantity_lots": 1, "open_positions": [],
    "closed_positions": [], "last_signal_time": None, "last_trade_date": None, "updated_at": None,
    "mode": "PAPER_ONLY"
}
# Persistent intraday signal journal. Records one completed 1m scan per candle.
SIGNAL_LOG_DIR = Path.home() / ".nifty_smc_research_lab" / "live_signal_history"
SIGNAL_LOG_DIR.mkdir(parents=True, exist_ok=True)
_signal_log_lock = threading.RLock()
_signal_log_backfilled_date = None

def _signal_log_path(day):
    return SIGNAL_LOG_DIR / f"nifty_signals_{day}.json"

def _signal_row(candle_frame, analysis, frames=None):
    if candle_frame is None or candle_frame.empty:
        return None
    last = candle_frame.iloc[-1]
    stamp = pd.Timestamp(last["datetime"])
    signal = str((analysis or {}).get("signal") or "WAIT").upper()
    checks = (analysis or {}).get("checks", {}) or {}
    reasons = (analysis or {}).get("reasons", []) or []
    entry = stop = target_1 = target_2 = None
    if signal in ("CE", "PE"):
        entry = float(last["close"])
        recent = candle_frame.tail(10)
        ob = (analysis or {}).get("order_block_zone")
        if signal == "CE":
            stop = float(ob["low"]) if ob and float(ob["low"]) < entry else float(recent["low"].min())
            risk = entry - stop
            target_1, target_2 = entry + risk, entry + 2.0 * risk
        else:
            stop = float(ob["high"]) if ob and float(ob["high"]) > entry else float(recent["high"].max())
            risk = stop - entry
            target_1, target_2 = entry - risk, entry - 2.0 * risk
        if risk <= 0:
            entry = stop = target_1 = target_2 = None
            signal = "WAIT"
    tf = _timeframe_view(analysis, frames or {}) if analysis else {}
    return {
        "timestamp": stamp.isoformat(), "time": stamp.strftime("%H:%M:%S"),
        "status": "CONFIRMED" if signal in ("CE", "PE") else "WAIT",
        "signal": signal, "price": round(float(last["close"]), 2),
        "entry": round(entry, 2) if entry is not None else None,
        "stop_loss": round(stop, 2) if stop is not None else None,
        "target_1": round(target_1, 2) if target_1 is not None else None,
        "target_2": round(target_2, 2) if target_2 is not None else None,
        "checks": checks, "reasons": reasons, "timeframes": tf
    }

def _save_signal_rows(rows, day):
    if not rows:
        return
    path = _signal_log_path(day)
    with _signal_log_lock:
        existing = {}
        if path.exists():
            try:
                existing = {r.get("timestamp"): r for r in json.loads(path.read_text(encoding="utf-8")) if r.get("timestamp")}
            except Exception:
                existing = {}
        changed = False
        for row in rows:
            if row and row.get("timestamp"):
                if existing.get(row["timestamp"]) != row:
                    existing[row["timestamp"]] = row
                    changed = True
        if changed or not path.exists():
            ordered = sorted(existing.values(), key=lambda r: r.get("timestamp", ""))
            path.write_text(json.dumps(ordered, ensure_ascii=False, indent=2), encoding="utf-8")

def _record_current_live_signal(candle_frame, analysis, frames):
    row = _signal_row(candle_frame, analysis, frames)
    if row:
        _save_signal_rows([row], pd.Timestamp(row["timestamp"]).strftime("%Y-%m-%d"))

def _backfill_today_signal_log():
    """Rebuild today's per-completed-minute journal from local cached candles; no broker request."""
    global _signal_log_backfilled_date
    today = datetime.now().date().isoformat()
    with _signal_log_lock:
        if _signal_log_backfilled_date == today:
            return
    with _live_lock:
        raw = list(_live_candles)
    if not raw:
        return
    frame = pd.DataFrame(raw)
    if frame.empty or "datetime" not in frame:
        return
    frame["datetime"] = pd.to_datetime(frame["datetime"])
    frame = frame.sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)
    today_indices = frame.index[frame["datetime"].dt.strftime("%Y-%m-%d") == today].tolist()
    if not today_indices:
        return
    with _signal_log_lock:
        _signal_log_backfilled_date = today
    rows = []
    for idx in today_indices:
        if idx < 20:
            continue
        context = frame.iloc[max(0, idx - 250):idx + 1].reset_index(drop=True)
        try:
            frames = build_timeframes(context)
            analysis = analyze_smc_stack(frames)
            rows.append(_signal_row(context, analysis, frames))
        except Exception:
            logger.exception("Could not backfill intraday signal row at index %s", idx)
    _save_signal_rows(rows, today)

_ALLOWED_SESSIONS = {15, 30, 60, 90, 180, 250}


def read_env():
    if not ENV_FILE.exists():
        raise RuntimeError(".env file missing from project folder.")
    vals = {}
    for raw in ENV_FILE.read_text(encoding="utf-8-sig").splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            vals[k.strip()] = v.strip().strip('"').strip("'")
    return vals

def credentials():
    v = read_env()
    required = ["ANGEL_CLIENT_CODE", "ANGEL_API_KEY", "ANGEL_PIN", "ANGEL_TOTP_SECRET", "ANGEL_NIFTY_TOKEN"]
    missing = [k for k in required if not v.get(k)]
    if missing:
        raise RuntimeError("Missing .env setting(s): " + ", ".join(missing))
    return Credentials(v["ANGEL_CLIENT_CODE"], v["ANGEL_API_KEY"], v["ANGEL_PIN"], v["ANGEL_TOTP_SECRET"]), v

def client():
    if not _angel_client or not _angel_client.jwt:
        raise HTTPException(status_code=401, detail="Connect Angel One first using POST /api/connect.")
    return _angel_client

def fetch_one_minute(days):
    c = client()
    _, v = credentials()
    end = datetime.now()
    start = end - timedelta(days=days)
    frame = c.historical("NSE", v["ANGEL_NIFTY_TOKEN"], "ONE_MINUTE", start, end)
    if frame.empty:
        raise RuntimeError("Angel One returned no one-minute candles for the requested period.")
    return frame, v

def _tick_time(message):
    raw = message.get("exchange_timestamp") or message.get("exchangeTimestamp") or message.get("last_traded_timestamp")
    if raw is None:
        return datetime.now()
    try:
        n = int(raw)
        return datetime.fromtimestamp(n / 1000) if n > 10_000_000_000 else datetime.fromtimestamp(n)
    except Exception:
        return pd.to_datetime(raw).to_pydatetime()

def _normalise_live_frame(frame):
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])
    prepared = frame.copy()
    if "datetime" not in prepared.columns:
        prepared = prepared.reset_index().rename(columns={"index": "datetime"})
    parsed = pd.to_datetime(prepared["datetime"])
    if getattr(parsed.dt, "tz", None) is not None:
        parsed = parsed.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    prepared["datetime"] = parsed
    for col in ["open", "high", "low", "close"]:
        prepared[col] = pd.to_numeric(prepared[col], errors="coerce")
    if "volume" not in prepared:
        prepared["volume"] = 0
    prepared["volume"] = pd.to_numeric(prepared["volume"], errors="coerce").fillna(0)
    prepared = prepared.dropna(subset=["datetime", "open", "high", "low", "close"])
    return prepared.sort_values("datetime").drop_duplicates("datetime").tail(5000).reset_index(drop=True)


def _persist_live_candles():
    try:
        with _live_lock:
            rows = list(_live_candles)
        if not rows:
            return
        frame = _normalise_live_frame(pd.DataFrame(rows))
        tmp = LIVE_CACHE_FILE.with_suffix(".tmp")
        frame.to_csv(tmp, index=False)
        tmp.replace(LIVE_CACHE_FILE)
    except Exception:
        logger.exception("Could not persist local live candle cache")


def _load_live_cache():
    frames = []
    if LIVE_CACHE_FILE.exists():
        try:
            frames.append(pd.read_csv(LIVE_CACHE_FILE, parse_dates=["datetime"]))
        except Exception:
            logger.exception("Could not load persistent live candle cache")
    # Reuse the historical chunks already downloaded by the backtester.
    # Only the newest two chunks are needed for live SMC warm-up and reduce disk reads.
    cached_chunks = sorted(CACHE_DIR.glob("nifty_1m_*.csv"))[-2:]
    for path in cached_chunks:
        try:
            chunk = pd.read_csv(path, parse_dates=["datetime"])
            if not chunk.empty:
                frames.append(chunk)
        except Exception:
            logger.warning("Skipping unreadable historical cache chunk %s", path.name)
    if not frames:
        return 0
    merged = _normalise_live_frame(pd.concat(frames, ignore_index=True))
    return _seed_live_candles(merged, persist=True)


def _seed_live_candles(frame, persist=True):
    """Seed only completed 1m candles; the cache is reused across app restarts."""
    prepared = _normalise_live_frame(frame)
    now_minute = datetime.now().replace(second=0, microsecond=0)
    prepared = prepared[prepared["datetime"] < now_minute]
    rows = prepared.tail(5000).to_dict("records")
    with _live_lock:
        existing = list(_live_candles)
        merged = _normalise_live_frame(pd.DataFrame(existing + rows)) if existing or rows else prepared
        _live_candles.clear()
        _live_candles.extend(merged.to_dict("records"))
        _live["forming"] = None
        _live["history_loaded"] = bool(_live_candles)
        _live["history_error"] = None if rows else ("No completed historical candles were available." if not _live_candles else None)
    if persist:
        _persist_live_candles()
    return len(_live_candles)


def _live_frame(include_forming=False):
    with _live_lock:
        rows = list(_live_candles)
        forming = dict(_live["forming"]) if _live.get("forming") else None
    if include_forming and forming:
        rows.append(forming)
    return _normalise_live_frame(pd.DataFrame(rows)) if rows else pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])


def _ensure_live_history_once(c, token):
    """One guarded warm-up request; chart and scan endpoints never call broker history."""
    global _last_live_history_fetch
    with _historical_fetch_lock:
        now = datetime.now()
        last_request = _last_live_history_fetch
        if LIVE_HISTORY_COOLDOWN_FILE.exists():
            try:
                disk_stamp = datetime.fromisoformat(LIVE_HISTORY_COOLDOWN_FILE.read_text(encoding="utf-8").strip())
                if last_request is None or disk_stamp > last_request:
                    last_request = disk_stamp
            except Exception:
                logger.warning("Ignoring invalid historical cooldown marker")
        if last_request and (now - last_request).total_seconds() < LIVE_HISTORY_COOLDOWN_SECONDS:
            return {"loaded": len(_live_candles), "source": "local_cache", "skipped": "persistent_cooldown"}
        _last_live_history_fetch = now
        try:
            LIVE_HISTORY_COOLDOWN_FILE.write_text(now.isoformat(timespec="seconds"), encoding="utf-8")
            end = now
            start = end - timedelta(days=5)
            frame = c.historical("NSE", token, "ONE_MINUTE", start, end)
            count = _seed_live_candles(frame)
            return {"loaded": count, "source": "angel_one_once"}
        except Exception as exc:
            message = str(exc)[:250]
            with _live_lock:
                _live["history_error"] = message
                _live["history_loaded"] = bool(_live_candles)
            logger.warning("One-time live historical warm-up failed: %s", message)
            return {"loaded": len(_live_candles), "source": "local_cache", "error": message}

def _on_market_tick(message):
    price_raw = message.get("last_traded_price", message.get("ltp"))
    if price_raw is None:
        return
    # SmartWebSocketV2's parsed LTP packet is in rupees in current SDK releases.
    price = float(price_raw)
    if price > 100000:  # defensive for SDK versions returning paise
        price /= 100
    stamp = _tick_time(message)
    minute = stamp.replace(second=0, microsecond=0)
    with _live_lock:
        forming = _live.get("forming")
        if forming is None or forming["datetime"] != minute:
            if forming is not None and forming["datetime"] < minute:
                _live_candles.append(forming.copy())
                # Persist once per completed minute, never once per incoming tick.
                threading.Thread(target=_persist_live_candles, daemon=True).start()
            # Ignore out-of-order ticks from an older minute for candle construction,
            # while still retaining the latest received quote timestamp separately.
            if forming is not None and minute < forming["datetime"]:
                _live.update({"price": price, "timestamp": stamp.isoformat(), "timestamp_dt": stamp,
                              "sequence": message.get("sequence_number")})
                return
            forming = {"datetime": minute, "open": price, "high": price, "low": price, "close": price, "volume": 0}
            _live["forming"] = forming
        else:
            forming["high"] = max(forming["high"], price)
            forming["low"] = min(forming["low"], price)
            forming["close"] = price
        _live.update({"price": price, "timestamp": stamp.isoformat(), "timestamp_dt": stamp, "sequence": message.get("sequence_number")})

@app.get("/api/status")
def status():
    return {"ok": True, "service": "nifty-smc-local-api", "mode": "local-smartapi-scanner",
            "angel_one_connected": bool(_angel_client and _angel_client.jwt),
            "websocket_connected": bool(_angel_client and _angel_client.ws_connected),
            "websocket_error": _angel_client.ws_error if _angel_client else None,
            "historical_candles_loaded": bool(_live.get("history_loaded")),
            "historical_candle_count": len(_live_candles),
            "historical_load_error": _live.get("history_error"),
            "forming_candle": bool(_live.get("forming")),
            "last_tick_age_seconds": (max(0, (datetime.now() - _live["timestamp_dt"]).total_seconds()) if _live.get("timestamp_dt") else None),
            "live_cache_file": str(LIVE_CACHE_FILE),
            "history_cache_directory": str(CACHE_DIR),
            "timestamp": datetime.now().astimezone().isoformat()}

@app.post("/api/connect")
def connect():
    global _angel_client
    try:
        creds, v = credentials()
        # Reuse an authenticated client/socket rather than creating repeated login and history calls.
        if _angel_client and _angel_client.jwt:
            if _angel_client.ws_connected:
                return {"ok": True, "angel_one_connected": True, "websocket_started": True,
                        "historical_candles_loaded": len(_live_candles), "message": "Already connected; local candle engine remains active."}
            c = _angel_client
        else:
            c = AngelClient(creds)
            c.login()
            _angel_client = c

        cached_count = _load_live_cache() if not _live_candles else len(_live_candles)
        # If the backtest cache already has a useful recent warm-up, do not call
        # Angel One historical API again just to start the live dashboard.
        latest = _live_frame()
        recent_cache = (len(latest) >= 1000 and
                        (datetime.now() - pd.Timestamp(latest.datetime.iloc[-1]).to_pydatetime()).total_seconds() < 7 * 86400)
        if recent_cache:
            warmup = {"loaded": len(latest), "source": "existing_backtest_cache", "skipped": "cache_sufficient"}
        else:
            # One guarded request only. On rate-limit rejection, retain cache and
            # continue with the WebSocket-built candles; no repeated retries.
            warmup = _ensure_live_history_once(c, v["ANGEL_NIFTY_TOKEN"])
        ws_result = c.start_market_websocket(v["ANGEL_NIFTY_TOKEN"], _on_market_tick, exchange_type=1)
        loaded = len(_live_candles)
        return {"ok": True, "angel_one_connected": True,
                "websocket_started": ws_result["started"],
                "historical_candles_loaded": loaded,
                "historical_source": warmup.get("source", "local_cache"),
                "historical_load_error": warmup.get("error"),
                "message": (f"Connected. {loaded} completed 1m candles available from local cache/warm-up. "
                            "New 1m candles are built locally from WebSocket ticks; chart and scan do not call historical API.")}
    except Exception as e:
        logger.exception("Angel One login/WebSocket start failed")
        if _angel_client:
            _angel_client.close_market_websocket()
        _angel_client = None
        raise HTTPException(status_code=400, detail=str(e)[:300])

@app.get("/api/candles")
def candles(interval: str = Query("1m", pattern="^(1m|2m|3m|5m)$"), days: int = Query(1, ge=1, le=30)):
    """Serve locally cached/aggregated candles. This endpoint never calls Angel One history."""
    try:
        one = _live_frame(include_forming=False)
        if not one.empty:
            cutoff = datetime.now() - timedelta(days=days)
            one = one[one["datetime"] >= cutoff].reset_index(drop=True)
        if one.empty:
            return {"ok": True, "market": "NIFTY 50", "interval": interval, "count": 0,
                    "candles": [], "source": "local_completed_1m_cache",
                    "message": "Waiting for completed 1m candles from cache or WebSocket."}
        if interval == "1m":
            data = one
        else:
            frames = build_timeframes(one)
            if interval == "2m": data = frames["2m"]
            elif interval == "5m": data = frames["5m"]
            else:
                data = one.set_index("datetime").resample("3min", origin="start_day", label="left", closed="left").agg(
                    {"open":"first","high":"max","low":"min","close":"last","volume":"sum"}).dropna(subset=["open","high","low","close"]).reset_index()
        output = [{"datetime": pd.Timestamp(r.datetime).isoformat(), "open": float(r.open),
                   "high": float(r.high), "low": float(r.low), "close": float(r.close),
                   "volume": float(getattr(r, "volume", 0) or 0)} for r in data.tail(600).itertuples(index=False)]
        return {"ok": True, "market": "NIFTY 50", "interval": interval, "count": len(output),
                "candles": output, "source": "local cache + WebSocket-built 1m candles",
                "note": "No historical broker request is made by this endpoint."}
    except Exception as e:
        logger.exception("Local candle read failed")
        raise HTTPException(status_code=500, detail=str(e)[:250])


def _timeframe_view(analysis, frames):
    """Build dashboard-friendly, timestamped 5m/2m/1m cards from one scan result."""
    if not analysis or not frames:
        return {k: {"signal": "WAIT", "structure": "Awaiting completed candles", "findings": [], "timestamp": None}
                for k in ("5m", "2m", "1m")}
    checks = analysis.get("checks", {})
    reasons = analysis.get("reasons", [])
    def stamp(key):
        frame = frames.get(key)
        return pd.Timestamp(frame.iloc[-1]["datetime"]).isoformat() if frame is not None and not frame.empty else None
    direction = analysis.get("direction")
    five_findings = [x for x in reasons if x.startswith(("5m structure", "BOS:"))]
    five = {"signal": (direction or "WAIT") + (" · BOS" if checks.get("5m_BOS") else ""),
            "structure": "BOS confirmed" if checks.get("5m_BOS") else "MSS confirmed" if checks.get("5m_MSS") else (direction + " bias" if direction else "Structure not established"),
            "findings": five_findings, "timestamp": stamp("5m"),
            "bos": bool(checks.get("5m_BOS")), "mss": bool(checks.get("5m_MSS"))}
    two_findings = [x for x in reasons if x.startswith(("2m FVG:", "2m order block:", "2m IDM:"))]
    two_state = "FVG retest" if checks.get("2m_FVG_retest") else "OB retest" if checks.get("2m_order_block_retest") else "Zone detected" if (checks.get("2m_FVG_detected") or checks.get("2m_order_block_detected")) else "No zone detected"
    two = {"signal": two_state, "structure": " · ".join(two_findings) if two_findings else "Awaiting 2m setup", "findings": two_findings,
           "timestamp": stamp("2m"), "fvg_detected": bool(checks.get("2m_FVG_detected")),
           "fvg_retest": bool(checks.get("2m_FVG_retest")), "order_block_detected": bool(checks.get("2m_order_block_detected")),
           "order_block_retest": bool(checks.get("2m_order_block_retest")), "idm": bool(checks.get("2m_IDM_sweep"))}
    one_findings = [x for x in reasons if x.startswith(("1m trigger:", "Entry time"))]
    one = {"signal": "CONFIRMED" if checks.get("1m_entry_confirmation") else "WAIT",
           "structure": "1m trigger confirmed" if checks.get("1m_entry_confirmation") else "Waiting for 1m trigger",
           "findings": one_findings, "timestamp": stamp("1m"),
           "entry_confirmation": bool(checks.get("1m_entry_confirmation")),
           "entry_window": bool(checks.get("entry_window_0930_1140"))}
    return {"5m": five, "2m": two, "1m": one}


@app.get("/api/scan")
def scan():
    """Analyze completed local candles only; never request historical data from Angel One."""
    try:
        one = _live_frame(include_forming=False)
        if one.empty:
            return {"ok": True, "market": "NIFTY 50", "candle_counts": {"1m":0,"2m":0,"5m":0},
                    "analysis": {"signal":"WAIT", "direction":None, "score":0, "checks":{},
                                 "reasons":["Waiting for completed 1m candles from WebSocket or local cache."]},
                    "note": "No completed local candles yet. Historical API is not called by scan."}
        frames = build_timeframes(one)
        a = analyze_smc_stack(frames)
        return {"ok": True, "market": "NIFTY 50", "candle_counts": {k: len(x) for k,x in frames.items()},
                "analysis": a, "timeframes": _timeframe_view(a, frames), "analysis_timestamp": pd.Timestamp(frames["1m"].iloc[-1]["datetime"]).isoformat() if not frames["1m"].empty else None, "live_source": "local completed 1m candle engine",
                "note": "SMC-only analysis from completed local candles: 5m structure → 2m FVG/OB/IDM → 1m entry. Signals restricted to 09:30–11:40 IST. No orders placed."}
    except Exception as e:
        logger.exception("Local SMC scan failed")
        raise HTTPException(status_code=500, detail=str(e)[:300])


def _normalise_history(frame):
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])
    out = frame.copy()
    parsed = pd.to_datetime(out["datetime"])
    if getattr(parsed.dt, "tz", None) is not None:
        parsed = parsed.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    out["datetime"] = parsed
    return out.sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)


def _cached_chunk(client_obj, token, start, end):
    # Chunk boundaries are anchored to a fixed epoch, so the same historical period
    # maps to the same filename on every run. Only the active/current chunk is refreshed.
    cache_file = CACHE_DIR / f"nifty_1m_{start:%Y%m%d}_{end:%Y%m%d}.csv"
    is_current_chunk = end.date() >= datetime.now().date()
    if not is_current_chunk and cache_file.exists():
        try:
            cached = pd.read_csv(cache_file, parse_dates=["datetime"])
            if not cached.empty:
                return _normalise_history(cached), "cache"
        except Exception:
            logger.warning("Ignoring unreadable history cache %s", cache_file)
    fetched = client_obj.historical("NSE", token, "ONE_MINUTE", start, end)
    fetched = _normalise_history(fetched)
    if not is_current_chunk:
        fetched.to_csv(cache_file, index=False)
    return fetched, "angel_one"


def _run_history_job(job_id, sessions, cost_points):
    def update(**kwargs):
        with _history_jobs_lock:
            _history_jobs[job_id].update(kwargs)
    try:
        c = client()
        _, env = credentials()
        # About 1.5 calendar days per NSE trading session, plus a warm-up margin.
        calendar_days = int(math.ceil(sessions * 7 / 5)) + 45
        end = datetime.now().replace(second=0, microsecond=0)
        start = (end - timedelta(days=calendar_days)).replace(hour=0, minute=0)
        # SmartAPI one-minute requests are kept below 30 calendar days per call.
        # Align chunk edges to fixed 25-day epoch buckets so repeated runs hit the same cache.
        chunk_days = 25
        epoch = datetime(2000, 1, 1)
        bucket = math.floor((start - epoch).total_seconds() / (chunk_days * 86400))
        cursor = epoch + timedelta(days=bucket * chunk_days)
        chunks = []
        while cursor < end:
            chunk_start = cursor
            chunk_end = min(cursor + timedelta(days=chunk_days), end)
            if chunk_start < chunk_end:
                chunks.append((chunk_start, chunk_end))
            cursor += timedelta(days=chunk_days)
        update(status="downloading", progress_pct=0, message="Preparing historical data chunks", total_chunks=len(chunks),
               completed_chunks=0, sessions_requested=sessions, cache_dir=str(CACHE_DIR))
        frames = []
        cache_hits = 0
        for index, (chunk_start, chunk_end) in enumerate(chunks, 1):
            chunk, source = _cached_chunk(c, env["ANGEL_NIFTY_TOKEN"], chunk_start, chunk_end)
            frames.append(chunk)
            cache_hits += int(source == "cache")
            update(completed_chunks=index, progress_pct=round(index * 100 / len(chunks), 1),
                   message=f"Downloaded/cached chunk {index} of {len(chunks)} ({source})",
                   cache_hits=cache_hits, candles_received=sum(len(x) for x in frames))
        combined = _normalise_history(pd.concat(frames, ignore_index=True) if frames else pd.DataFrame())
        if combined.empty:
            raise RuntimeError("Angel One returned no one-minute candles for the requested date range.")
        dates = sorted({pd.Timestamp(x).date() for x in combined.datetime})
        selected_dates = dates[-sessions:]
        selected = combined[combined.datetime.dt.date.isin(selected_dates)].reset_index(drop=True)
        if len(selected_dates) < sessions:
            update(message=f"Only {len(selected_dates)} sessions available; requested {sessions}. Continuing with available data.")
        update(status="replaying", progress_pct=0, message="Preparing completed four-timeframe candles for shared replay",
               sessions_found=len(selected_dates), candle_count=len(selected))
        fingerprint = hashlib.sha256(pd.util.hash_pandas_object(selected, index=False).values.tobytes()).hexdigest()
        result_cache_file = RESULT_CACHE_DIR / f"{fingerprint}_cost_{float(cost_points):.2f}_smc_morning_v2.json"
        if result_cache_file.exists():
            try:
                cached_result = json.loads(result_cache_file.read_text(encoding="utf-8"))
                cached_result["summary"].update({"sessions_requested": sessions, "sessions_included": len(selected_dates),
                    "calendar_days_searched": calendar_days, "one_minute_candles": len(selected),
                    "cache_chunks_used": cache_hits, "download_chunks": len(chunks),
                    "cache_location": str(CACHE_DIR), "strict_four_timeframe": True,
                    "replay_engine": "cached_shared_single_pass_v1"})
                update(status="completed", progress_pct=100, message="Loaded completed report from local result cache",
                       result=cached_result, completed_at=datetime.now().isoformat())
                return
            except Exception:
                logger.warning("Ignoring invalid replay result cache %s", result_cache_file)
        frames_by_tf = build_timeframes(selected)
        def replay_progress(pct, done, total):
            update(status="replaying", progress_pct=pct,
                   message=f"Shared replay: processed {done:,} of {total:,} one-minute candles")
        all_results = historical_replay_all(frames_by_tf, max_trades=5000,
                                            cost_points=cost_points, progress_callback=replay_progress)
        result = all_results["smc_pac"]
        result["comparison"] = {"smc_pac": result["summary"],
            "note": "SMC-only morning-session replay."}
        result["session_dates"] = [str(x) for x in selected_dates]
        result["candle_counts"] = {k: len(v) for k, v in frames_by_tf.items()}
        result["summary"].update({"sessions_requested": sessions, "sessions_included": len(selected_dates),
            "calendar_days_searched": calendar_days, "one_minute_candles": len(selected),
            "cache_chunks_used": cache_hits, "download_chunks": len(chunks),
            "cache_location": str(CACHE_DIR), "strict_four_timeframe": True,
            "replay_engine": "shared_single_pass_v1"})
        try:
            result_cache_file.write_text(json.dumps(result, separators=(",", ":")), encoding="utf-8")
        except Exception:
            logger.exception("Could not save replay result cache")
        update(status="completed", progress_pct=100, message="Historical backtest completed", result=result,
               completed_at=datetime.now().isoformat())
    except Exception as exc:
        logger.exception("Historical job %s failed", job_id)
        update(status="failed", message=str(exc)[:400], error=str(exc)[:400], completed_at=datetime.now().isoformat())


@app.get("/api/history/cache")
def history_cache_status():
    files=list(CACHE_DIR.glob("nifty_1m_*.csv"))
    return {"cache_directory":str(CACHE_DIR),"cached_chunks":len(files),
            "cached_bytes":sum(x.stat().st_size for x in files),
            "message":"Completed historical chunks are reused automatically; the active/current chunk is refreshed."}


@app.post("/api/history/cache/clear")
def clear_history_cache():
    removed=0
    for item in CACHE_DIR.glob("nifty_1m_*.csv"):
        try: item.unlink(); removed+=1
        except OSError: pass
    return {"ok":True,"removed_chunks":removed,"message":"Historical cache cleared. Next backtest will download history again."}


@app.post("/api/history/start")
def start_history(sessions: int = Query(250, ge=15, le=250), cost_points: float = Query(2.0, ge=0, le=100)):
    if sessions not in _ALLOWED_SESSIONS:
        raise HTTPException(status_code=422, detail="sessions must be one of 15, 30, 60, 90, 180 or 250")
    client()
    job_id = uuid.uuid4().hex[:12]
    with _history_jobs_lock:
        _history_jobs[job_id] = {"job_id": job_id, "status": "queued", "progress_pct": 0,
            "message": "Queued", "sessions_requested": sessions, "cost_points": cost_points,
            "created_at": datetime.now().isoformat()}
    thread = threading.Thread(target=_run_history_job, args=(job_id, sessions, cost_points), daemon=True)
    thread.start()
    return {"ok": True, "job_id": job_id, "status": "queued", "message": "Historical download and replay started."}


@app.get("/api/history/status/{job_id}")
def history_status(job_id: str):
    with _history_jobs_lock:
        job = _history_jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="History job not found. Start a new backtest.")
        # Keep the large trade result out of frequent progress polling responses.
        return {k: v for k, v in job.items() if k != "result"}


@app.get("/api/history/result/{job_id}")
def history_result(job_id: str):
    with _history_jobs_lock:
        job = _history_jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="History job not found.")
        if job.get("status") != "completed":
            raise HTTPException(status_code=409, detail="Historical replay is not complete yet.")
        return {"ok": True, **job["result"], "message": "Corrected completed-candle NIFTY spot replay. No orders placed."}


@app.get("/api/history")
def history(sessions: int = Query(250, ge=15, le=250), days: int | None = Query(None, ge=1, le=500),
            strict: bool = True, cost_points: float = Query(2.0, ge=0, le=100)):
    # Backward-compatible synchronous endpoint. The dashboard uses /start + /status for progress.
    if days is not None:
        sessions = min(250, max(15, int(days)))
    if sessions not in _ALLOWED_SESSIONS:
        raise HTTPException(status_code=422, detail="sessions must be one of 15, 30, 60, 90, 180 or 250")
    client()
    job_id = uuid.uuid4().hex[:12]
    with _history_jobs_lock:
        _history_jobs[job_id] = {"job_id": job_id, "status": "queued", "progress_pct": 0, "message": "Queued"}
    _run_history_job(job_id, sessions, cost_points)
    with _history_jobs_lock:
        job = _history_jobs[job_id]
        if job.get("status") != "completed":
            raise HTTPException(status_code=400, detail=job.get("error", "Historical replay failed"))
        return {"ok": True, **job["result"], "message": "Corrected completed-candle NIFTY spot replay. No orders placed."}



@app.get("/api/signals/today")
def today_signal_history():
    """Today's completed-candle journal, backfilled from local cache only."""
    day = datetime.now().date().isoformat()
    _backfill_today_signal_log()
    path = _signal_log_path(day)
    try:
        rows = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
    except Exception:
        rows = []
    confirmed = [r for r in rows if r.get("status") == "CONFIRMED"]
    return {
        "ok": True, "date": day, "timezone": "Asia/Kolkata",
        "scan_count": len(rows), "confirmed_count": len(confirmed),
        "wait_count": max(0, len(rows) - len(confirmed)),
        "confirmed_signals": confirmed, "recent_scans": rows[-15:],
        "source": "local completed 1m candles and saved live scans; no historical broker request"
    }


@app.get("/api/paper-trading")
def paper_trading_status():
    # Refresh open-position mark-to-market from the newest fresh WebSocket quote.
    with _live_lock:
        quote = _live.get("price")
        quote_time = _live.get("timestamp_dt")
    if quote is not None and quote_time is not None and (datetime.now() - quote_time).total_seconds() <= 15:
        _paper_update(float(quote), None, None, quote_time.isoformat())
    with _paper_lock:
        snapshot = json.loads(json.dumps(_paper_state))
    snapshot["latest_quote"] = quote
    snapshot["latest_quote_time"] = quote_time.isoformat() if quote_time else None
    return {"ok": True, **snapshot}


@app.post("/api/paper-trading")
def paper_trading_control(payload: dict):
    action = str(payload.get("action", "")).lower()
    with _paper_lock:
        if action == "start":
            quantity = int(payload.get("quantity_lots", 1))
            if quantity < 1 or quantity > 100:
                raise HTTPException(status_code=400, detail="Quantity must be between 1 and 100 lots.")
            _paper_state["quantity_lots"] = quantity
            _paper_state["enabled"] = True
        elif action == "stop":
            _paper_state["enabled"] = False
        elif action == "quantity":
            quantity = int(payload.get("quantity_lots", 1))
            if quantity < 1 or quantity > 100:
                raise HTTPException(status_code=400, detail="Quantity must be between 1 and 100 lots.")
            _paper_state["quantity_lots"] = quantity
        else:
            raise HTTPException(status_code=400, detail="Use action start, stop, or quantity.")
        _paper_state["updated_at"] = datetime.now().isoformat(timespec="seconds")
        return {"ok": True, **json.loads(json.dumps(_paper_state))}


def _paper_update(price, trade_plan, signal, quote_time):
    """Mark and manage one paper NIFTY spot position; never sends broker orders."""
    if price is None:
        return
    now = datetime.now()
    price = float(price)
    with _paper_lock:
        if _paper_state["open_positions"]:
            pos = _paper_state["open_positions"][0]
            entry = float(pos["entry_price"])
            side = pos["signal"]
            stop = float(pos["stop_loss"])
            target_1 = float(pos["target_1"])
            target_2 = float(pos["target_2"])
            sign = 1 if side == "CE" else -1
            pos["last_price"] = round(price, 2)
            pos["last_price_time"] = quote_time or now.isoformat(timespec="seconds")
            pos["unrealised_points"] = round((price-entry)*sign*int(pos["quantity_lots"]), 2)
            # Track T1 independently; keep the position open for T2 or stop.
            t1_hit = price >= target_1 if side == "CE" else price <= target_1
            t2_hit = price >= target_2 if side == "CE" else price <= target_2
            stop_hit = price <= stop if side == "CE" else price >= stop
            if t1_hit and not pos.get("target_1_hit"):
                pos["target_1_hit"] = True
                pos["target_1_hit_at"] = now.isoformat(timespec="seconds")
            pos["target_2_hit"] = bool(pos.get("target_2_hit") or t2_hit)
            pos["stop_hit"] = bool(pos.get("stop_hit") or stop_hit)
            pos["monitoring"] = {
                "stop_loss": "HIT" if stop_hit else "ACTIVE",
                "target_1": "HIT" if pos["target_1_hit"] else "ACTIVE",
                "target_2": "HIT" if t2_hit else "ACTIVE"
            }
            exit_reason = "STOP" if stop_hit else "TARGET_2" if t2_hit else "EOD_EXIT" if now.strftime("%H:%M") >= "15:25" else None
            if exit_reason:
                pos["status"] = "CLOSED"
                pos["exit_reason"] = exit_reason
                pos["exit_price"] = round(price, 2)
                pos["closed_at"] = now.isoformat(timespec="seconds")
                pos["realised_points"] = pos["unrealised_points"]
                _paper_state["closed_positions"].append(pos.copy())
                _paper_state["closed_positions"] = _paper_state["closed_positions"][-100:]
                _paper_state["open_positions"] = []
        # One open position maximum and one entry per trading date. An open position
        # always blocks a new entry, even if a new scan signal appears concurrently.
        if (_paper_state["enabled"] and not _paper_state["open_positions"]
            and trade_plan and signal in ("CE", "PE")
            and trade_plan.get("signal_time") != _paper_state["last_signal_time"]
            and _paper_state.get("last_trade_date") != now.date().isoformat()
            and now.strftime("%H:%M") >= "09:30" and now.strftime("%H:%M") <= "11:40"):
            quantity = int(_paper_state["quantity_lots"])
            pos = {
                "instrument": "NIFTY 50 SPOT (PAPER)", "signal": signal,
                "side": trade_plan.get("side"), "quantity_lots": quantity,
                "entry_price": float(trade_plan["entry_reference"]),
                "stop_loss": float(trade_plan["stop_loss"]),
                "target_1": float(trade_plan["target_1"]),
                "target_2": float(trade_plan["target_2"]),
                "last_price": round(price, 2), "unrealised_points": round((price-float(trade_plan["entry_reference"]))* (1 if signal=="CE" else -1)*quantity, 2),
                "target_1_hit": False, "target_2_hit": False, "stop_hit": False,
                "monitoring": {"stop_loss":"ACTIVE","target_1":"ACTIVE","target_2":"ACTIVE"},
                "status": "OPEN", "opened_at": now.isoformat(timespec="seconds"),
                "last_price_time": quote_time or now.isoformat(timespec="seconds"),
                "signal_time": trade_plan.get("signal_time")
            }
            _paper_state["open_positions"] = [pos]
            _paper_state["last_signal_time"] = trade_plan.get("signal_time")
            _paper_state["last_trade_date"] = now.date().isoformat()
        _paper_state["updated_at"] = now.isoformat(timespec="seconds")

@app.get("/api/live")
def live():
    try:
        c = client()
        with _live_lock:
            snapshot = dict(_live)
            # Analyse completed one-minute candles only; never use the currently forming bar.
            candles = list(_live_candles)
        if snapshot["price"] is None:
            return {
                "ok": True, "connected": bool(c.ws_connected), "price": None,
                "signal": "WAIT", "timeframes": {},
                "historical_candles_loaded": bool(snapshot.get("history_loaded")),
                "candle_count_1m": len(candles),
                "message": "Historical candles are preloaded. Waiting for the first WebSocket tick.",
                "websocket_error": c.ws_error
            }
        frame = pd.DataFrame(candles)
        analysis = None
        frames = {}
        if len(frame) >= 15:
            frame["datetime"] = pd.to_datetime(frame["datetime"])
            frame = frame.sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)
            frames = build_timeframes(frame)
            analysis = analyze_smc_stack(frames)
        tf = _timeframe_view(analysis, frames) if analysis else _timeframe_view(None, {})
        checks = (analysis or {}).get("checks", {})
        concepts = [f"{k}: {'PASS' if x else 'WAIT'}" for k, x in checks.items()]
        age = max(0, (datetime.now() - datetime.fromisoformat(snapshot["timestamp"])).total_seconds())
        trade_plan = None
        if analysis and analysis.get("signal") in ("CE", "PE") and len(frame) >= 10:
            direction = analysis["signal"]
            last = frame.iloc[-1]
            entry = float(last["close"])  # reference price at completed signal candle close
            recent = frame.tail(10)
            ob = analysis.get("order_block_zone")
            if direction == "CE":
                stop = float(ob["low"]) if ob and float(ob["low"]) < entry else float(recent["low"].min())
                risk = entry - stop
                target = entry + 2.0 * risk
            else:
                stop = float(ob["high"]) if ob and float(ob["high"]) > entry else float(recent["high"].max())
                risk = stop - entry
                target = entry - 2.0 * risk
            if risk > 0:
                trade_plan = {
                    "side": "BUY CE" if direction == "CE" else "BUY PE",
                    "entry_reference": round(entry, 2),
                    "stop_loss": round(stop, 2),
                    "target_1": round(entry + risk if direction == "CE" else entry - risk, 2),
                    "target_2": round(target, 2),
                    "risk_points": round(risk, 2),
                    "risk_reward": "1:2",
                    "entry_basis": "Completed 1-minute signal candle close; actual execution may differ",
                    "signal_time": str(last["datetime"])
                }
        # Save each completed-candle analysis so later dashboard logins can review today's signals.
        if analysis and not frame.empty:
            _record_current_live_signal(frame, analysis, frames)
        if c.ws_connected and age <= 15:
            _paper_update(snapshot.get("price"), trade_plan, (analysis or {}).get("signal"), snapshot.get("timestamp"))
        fvg = [str(analysis.get("fvg_zone"))] if analysis and analysis.get("fvg_zone") else []
        obs = []
        return {"ok": True, "connected": c.ws_connected, "price": snapshot["price"],
                "quote_time": snapshot["timestamp"], "tick_age_seconds": round(age, 2),
                "stale": age > 15, "signal": (analysis or {}).get("signal", "WAIT"),
                "score": (analysis or {}).get("score", 0), "checks": (analysis or {}).get("checks", {}),
                "reasons": (analysis or {}).get("reasons", []), "fvg": fvg,
                "order_block": (analysis or {}).get("order_block_zone"), "idm": (analysis or {}).get("idm"),
                "trade_plan": trade_plan,
                "concepts": concepts, "timeframes": tf,
                "analysis_timestamp": tf.get("1m", {}).get("timestamp"),
                "historical_candles_loaded": bool(snapshot.get("history_loaded")),
                "historical_load_error": snapshot.get("history_error"),
                "candle_count_1m": len(frame),
                "candle_counts": {k: len(v) for k, v in frames.items()},
                "analysis_ready": bool(tf),
                "note": (
                    "SMC-only analysis uses completed 5m/2m/1m candles. Entry signals are restricted to 09:30-11:40 IST. "
                    "FVG, order block, IDM, BOS and MSS are heuristic price-action detections; no orders are placed."
                ),
                "websocket_error": c.ws_error}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Live feed read failed")
        raise HTTPException(status_code=400, detail=str(e)[:300])


@app.on_event("startup")
def startup_load_local_candles():
    count = _load_live_cache()
    if count:
        logger.info("Loaded %s completed 1m candles from persistent local cache", count)

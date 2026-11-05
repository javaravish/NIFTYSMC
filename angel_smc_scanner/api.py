from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from pathlib import Path
from collections import deque
import logging
import os
import re
import threading
import uuid
import math
import json
import time
import hashlib
import requests
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from angel_client import AngelClient, Credentials
from scanner import build_timeframes, analyze_smc_stack, historical_replay_all, build_trade_plan, evaluate_trade_plan

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nifty-smc-api")
IST = ZoneInfo("Asia/Kolkata")
ENTRY_START = dtime(9, 30)
ENTRY_END = dtime(11, 50)
SIGNAL_LOG_START = dtime(9, 30)
SIGNAL_LOG_END = dtime(15, 0)
MAX_PAPER_TRADES_PER_DAY = 3
FORCE_EXIT_TIME = dtime(15, 20)  # 10 minutes before NSE regular close
BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"
app = FastAPI(title="NIFTY SMC Research Lab API", version="3.18.0")
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
    "closed_positions": [], "last_signal_time": None, "last_trade_date": None,
    "daily_trade_count": 0, "daily_trade_date": None, "updated_at": None,
    "mode": "PAPER_ONLY", "last_error": None
}

PAPER_STATE_FILE = Path.home() / ".nifty_smc_research_lab" / "paper_trading_state.json"
PAPER_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

def _load_paper_state():
    try:
        if PAPER_STATE_FILE.exists():
            saved = json.loads(PAPER_STATE_FILE.read_text(encoding="utf-8"))
            if isinstance(saved, dict):
                for key in ("enabled", "quantity_lots", "open_positions", "closed_positions",
                            "last_signal_time", "last_trade_date", "daily_trade_count", "daily_trade_date", "updated_at", "last_error"):
                    if key in saved:
                        _paper_state[key] = saved[key]
                _paper_state["mode"] = "PAPER_ONLY"
                today = _now_ist().date().isoformat()
                if _paper_state.get("daily_trade_date") != today:
                    # Migrate older state files by counting today's already-opened paper trades.
                    positions = list(_paper_state.get("closed_positions") or []) + list(_paper_state.get("open_positions") or [])
                    migrated_count = sum(
                        1 for pos in positions
                        if str(pos.get("opened_at", ""))[:10] == today
                    )
                    _paper_state["daily_trade_date"] = today
                    _paper_state["daily_trade_count"] = min(MAX_PAPER_TRADES_PER_DAY, migrated_count)
    except Exception:
        logger.exception("Could not restore paper state")

def _save_paper_state():
    try:
        with _paper_lock:
            snapshot = json.loads(json.dumps(_paper_state))
        tmp = PAPER_STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(PAPER_STATE_FILE)
    except Exception:
        logger.exception("Could not persist paper state")

# Persistent intraday signal journal. Records one completed 1m scan per candle.
SIGNAL_LOG_DIR = Path.home() / ".nifty_smc_research_lab" / "live_signal_history"
SIGNAL_LOG_DIR.mkdir(parents=True, exist_ok=True)
_signal_log_lock = threading.RLock()
_signal_log_backfilled_date = None
# Angel One option scrip master is used only to resolve the ATM contract; no order API is called.
SCRIP_MASTER_URL = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"

def _now_ist():
    return datetime.now(IST)

def _in_entry_window(value=None):
    """Paper-entry window only: 09:30 through 11:50 IST."""
    value = value or _now_ist()
    if value.tzinfo is None:
        value = value.replace(tzinfo=IST)
    local = value.astimezone(IST).time()
    return ENTRY_START <= local <= ENTRY_END


def _in_signal_log_window(value=None):
    """Journal window: record every completed scan from 09:30 through 15:00 IST."""
    value = value or _now_ist()
    if value.tzinfo is None:
        value = value.replace(tzinfo=IST)
    local = value.astimezone(IST).time()
    return SIGNAL_LOG_START <= local <= SIGNAL_LOG_END

SCRIP_MASTER_FILE = Path.home() / ".nifty_smc_research_lab" / "OpenAPIScripMaster.json"
_scrip_master_lock = threading.RLock()
_scrip_master_rows = None
_nifty_spot_token = None
_option_tick_lock = threading.RLock()
_signal_option_tokens = set()  # option tokens subscribed for signal-only simulated premium tracking

def _signal_log_path(day):
    return SIGNAL_LOG_DIR / f"nifty_signals_{day}.json"


def _load_option_master(force=False):
    """Load Angel One's public instrument master with a persistent local cache."""
    global _scrip_master_rows
    with _scrip_master_lock:
        if _scrip_master_rows is not None and not force:
            return _scrip_master_rows
        rows = None
        cache_is_fresh = (SCRIP_MASTER_FILE.exists() and
                          (datetime.now().timestamp() - SCRIP_MASTER_FILE.stat().st_mtime) < 86400)
        if SCRIP_MASTER_FILE.exists() and not force and cache_is_fresh:
            try:
                rows = json.loads(SCRIP_MASTER_FILE.read_text(encoding="utf-8"))
            except Exception:
                rows = None
        if not rows:
            response = requests.get(SCRIP_MASTER_URL, timeout=25)
            response.raise_for_status()
            rows = response.json()
            if not isinstance(rows, list) or not rows:
                raise RuntimeError("Angel One scrip master returned an empty or invalid instrument list.")
            SCRIP_MASTER_FILE.parent.mkdir(parents=True, exist_ok=True)
            tmp = SCRIP_MASTER_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(rows, separators=(",", ":")), encoding="utf-8")
            tmp.replace(SCRIP_MASTER_FILE)
        _scrip_master_rows = rows
        return rows


def _expiry_date(value):
    try:
        return datetime.strptime(str(value).upper(), "%d%b%Y").date()
    except Exception:
        return None


def _resolve_atm_nifty_option(spot_price, signal):
    """Return the nearest-expiry, nearest-strike NIFTY index option contract."""
    if signal not in ("CE", "PE"):
        raise ValueError("Only CE or PE option contracts can be selected.")
    today = _now_ist().date()
    rows = _load_option_master()
    candidates = []
    for row in rows:
        if str(row.get("name", "")).upper() != "NIFTY":
            continue
        if str(row.get("exch_seg", "")).upper() != "NFO":
            continue
        if str(row.get("instrumenttype", "")).upper() != "OPTIDX":
            continue
        if str(row.get("symbol", "")).upper().endswith(signal) is False:
            continue
        expiry = _expiry_date(row.get("expiry"))
        if expiry and expiry >= today:
            try:
                strike_raw = float(row.get("strike"))
                # Angel One's instrument master stores option strike in paise.
                strike = strike_raw / 100.0 if strike_raw > 100000 else strike_raw
                lot_size = int(float(row.get("lotsize")))
                token = str(row.get("token", "")).strip()
                symbol = str(row.get("symbol", "")).strip()
                if token and symbol and lot_size > 0 and strike > 0:
                    candidates.append((expiry, abs(strike - float(spot_price)), strike,
                                       {"symbol": symbol, "token": token, "expiry": expiry.isoformat(),
                                        "strike": strike, "option_type": signal, "lot_size": lot_size,
                                        "exchange": "NFO", "exchange_type": 2}))
            except (TypeError, ValueError):
                continue
    if not candidates:
        raise RuntimeError("No current NIFTY OPTIDX contract found in Angel One's instrument master.")
    # Nearest valid expiry first, then closest available strike to spot.
    nearest_expiry = min(x[0] for x in candidates)
    same_expiry = [x for x in candidates if x[0] == nearest_expiry]
    return min(same_expiry, key=lambda x: (x[1], x[2]))[3]


def _signal_outcome_update(row, candle_frame):
    """Update live journal outcomes using the same evaluator as historical replay."""
    if (row.get("status") != "CONFIRMED" or not row.get("timestamp")
        or row.get("paper_trade_status") == "CLOSED"
        or row.get("outcome") not in (None, "", "OPEN", "TARGET 1 HIT", "SL HIT", "TARGET 2 HIT", "EOD EXIT")):
        return row
    plan = {"timestamp": row["timestamp"], "signal": row.get("signal"), "entry": row.get("entry"),
            "stop_loss": row.get("stop_loss"), "target_1": row.get("target_1"), "target_2": row.get("target_2")}
    result = evaluate_trade_plan(plan, candle_frame)
    row.update({"outcome": result.get("outcome", "OPEN"), "exit_price": result.get("exit_price"),
                "points_gained_lost": result.get("points_gained_lost"), "exit_time": result.get("exit_time"),
                "target_1_hit": bool(result.get("target_1_hit")),
                "target_1_hit_time": result.get("target_1_hit_time"), "outcome_note": result.get("outcome_note")})
    return row


def _update_today_signal_outcomes(candle_frame):
    """Refresh outcomes for all confirmed signals from today's local completed candles."""
    if candle_frame is None or candle_frame.empty:
        return
    day = _now_ist().date().isoformat()
    path = _signal_log_path(day)
    if not path.exists():
        return
    try:
        with _signal_log_lock:
            rows = json.loads(path.read_text(encoding="utf-8"))
            changed = False
            for row in rows:
                if row.get("status") == "CONFIRMED":
                    before = json.dumps(row, sort_keys=True)
                    _signal_outcome_update(row, candle_frame)
                    changed = changed or before != json.dumps(row, sort_keys=True)
            if changed:
                tmp = path.with_suffix(".tmp")
                tmp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
                tmp.replace(path)
    except Exception:
        logger.exception("Could not update today's signal outcomes")


def _journal_option_trade(pos):
    """Mirror the active/closed ATM option paper trade into its matching signal journal row."""
    signal_time = pos.get("signal_time")
    if not signal_time:
        return
    day = pd.Timestamp(signal_time).strftime("%Y-%m-%d")
    path = _signal_log_path(day)
    if not path.exists():
        return
    try:
        with _signal_log_lock:
            rows = json.loads(path.read_text(encoding="utf-8"))
            for row in rows:
                if (pos.get("trade_id") and row.get("trade_id") == pos.get("trade_id")) or row.get("timestamp") == signal_time:
                    row["paper_instrument"] = pos.get("option_symbol")
                    row["paper_expiry"] = pos.get("option_expiry")
                    row["paper_strike"] = pos.get("option_strike")
                    row["paper_entry_premium"] = pos.get("entry_price")
                    row["paper_exit_premium"] = pos.get("exit_price")
                    row["paper_points_gained_lost"] = pos.get("realised_option_points", pos.get("unrealised_option_points"))
                    row["paper_pnl_rupees"] = pos.get("realised_pnl_rupees", pos.get("unrealised_pnl_rupees"))
                    row["paper_trade_status"] = pos.get("status")
                    row["points_basis"] = "NIFTY spot points"
                    row["option_pnl_rupees"] = pos.get("realised_pnl_rupees", pos.get("unrealised_pnl_rupees"))
                    if pos.get("exit_reason"):
                        # Keep NIFTY spot outcome/exit_price/points_gained_lost from
                        # the independent spot-signal evaluator. Option values belong
                        # only in the paper_* fields.
                        row["paper_exit_reason"] = pos.get("exit_reason")
                        row["paper_exit_spot_reference"] = pos.get("exit_spot_price")
                    break
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(path)
    except Exception:
        logger.exception("Could not sync ATM option paper trade to signal journal")


def _signal_row(candle_frame, analysis, frames=None):
    if candle_frame is None or candle_frame.empty:
        return None
    last = candle_frame.iloc[-1]
    stamp = pd.Timestamp(last["datetime"])
    signal = str((analysis or {}).get("signal") or "WAIT").upper()
    checks = (analysis or {}).get("checks", {}) or {}
    reasons = (analysis or {}).get("reasons", []) or []
    plan = build_trade_plan(candle_frame, analysis)
    tf = _timeframe_view(analysis, frames or {}) if analysis else {}
    signal_trade_id = hashlib.sha256(f"{stamp.isoformat()}|{signal}".encode("utf-8")).hexdigest()[:20] if signal in ("CE", "PE") else None
    in_entry_window = ENTRY_START <= stamp.time() <= ENTRY_END
    in_scan_window = SIGNAL_LOG_START <= stamp.time() <= SIGNAL_LOG_END
    valid_signal = signal in ("CE", "PE") and plan is not None
    return {
        "trade_id": signal_trade_id,
        "entry_window": "ENTRY WINDOW" if in_entry_window else ("SCAN ONLY" if in_scan_window else "OUTSIDE SCAN WINDOW"),
        "scan_window": bool(in_scan_window), "eligible_for_entry": bool(in_entry_window and valid_signal),
        "timestamp": stamp.isoformat(), "time": stamp.strftime("%H:%M:%S"),
        "status": "CONFIRMED" if valid_signal else "WAIT", "signal": signal if valid_signal else "WAIT",
        "price": round(float(last["close"]), 2), "outcome": "OPEN" if valid_signal else None,
        "exit_price": None, "points_gained_lost": None, "exit_time": None,
        "entry": plan["entry"] if plan else None, "stop_loss": plan["stop_loss"] if plan else None,
        "target_1": plan["target_1"] if plan else None, "target_2": plan["target_2"] if plan else None,
        "target_1_hit": False, "target_1_hit_time": None,
        "checks": checks, "reasons": reasons, "timeframes": tf
    }

_signal_retention_last_run = 0.0

def _prune_signal_journal(force=False):
    """Keep the newest 10 confirmed CE/PE signal rows; preserve today's WAIT rows for scan counters."""
    global _signal_retention_last_run
    try:
        now_ts = time.time()
        if not force and now_ts - _signal_retention_last_run < 300:
            return
        today = _now_ist().date().isoformat()
        with _signal_log_lock:
            files = list(SIGNAL_LOG_DIR.glob("nifty_signals_*.json"))
            loaded = []
            for journal_file in files:
                try:
                    rows = json.loads(journal_file.read_text(encoding="utf-8"))
                    if isinstance(rows, list):
                        loaded.append((journal_file, rows))
                except Exception:
                    # Never delete or overwrite a journal that cannot be read safely.
                    logger.warning("Leaving unreadable signal journal untouched: %s", journal_file)
            confirmed = []
            for journal_file, rows in loaded:
                for row in rows:
                    if (not isinstance(row, dict)
                            or row.get("status") != "CONFIRMED"
                            or str(row.get("signal", "")).upper() not in ("CE", "PE")):
                        continue
                    try:
                        stamp = pd.Timestamp(row.get("timestamp"))
                        stamp = stamp.tz_localize(IST) if stamp.tzinfo is None else stamp.tz_convert(IST)
                        if SIGNAL_LOG_START <= stamp.time() <= SIGNAL_LOG_END:
                            confirmed.append((stamp, journal_file, row))
                    except Exception:
                        continue
            confirmed.sort(key=lambda item: item[0], reverse=True)
            keep_keys = {(str(path), row.get("timestamp")) for _, path, row in confirmed[:10]}
            for journal_file, rows in loaded:
                day_from_name = journal_file.stem.replace("nifty_signals_", "")
                kept = []
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    if row.get("status") == "CONFIRMED" and str(row.get("signal", "")).upper() in ("CE", "PE"):
                        if (str(journal_file), row.get("timestamp")) in keep_keys:
                            kept.append(row)
                    elif day_from_name == today and row.get("status") == "WAIT":
                        # Today's WAIT rows are retained only to keep the dashboard's scan counters accurate.
                        kept.append(row)
                if kept:
                    tmp = journal_file.with_suffix(".tmp")
                    tmp.write_text(json.dumps(kept, ensure_ascii=False, indent=2), encoding="utf-8")
                    tmp.replace(journal_file)
                else:
                    journal_file.unlink(missing_ok=True)
            _signal_retention_last_run = now_ts
    except Exception:
        logger.exception("Signal journal retention cleanup failed")

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
        preserve_fields = (
            "outcome", "exit_price", "points_gained_lost", "exit_time", "outcome_note",
            "target_1_hit", "target_1_hit_time", "points_basis", "paper_instrument", "paper_expiry", "paper_strike",
            "paper_entry_premium", "paper_exit_premium", "paper_points_gained_lost",
            "paper_pnl_rupees", "paper_trade_status", "option_pnl_rupees", "spot_exit_reference",
            "sim_option_symbol", "sim_option_token", "sim_option_expiry", "sim_option_strike",
            "sim_option_lot_size", "sim_premium_status", "sim_premium_source", "sim_premium_entry",
            "sim_premium_entry_time", "sim_premium_last", "sim_premium_last_time", "sim_premium_exit",
            "sim_premium_exit_time", "sim_premium_points", "sim_premium_rupees"
        )
        for row in rows:
            if row and row.get("timestamp"):
                previous = existing.get(row["timestamp"], {})
                for field in preserve_fields:
                    if field in previous and row.get(field) in (None, "", "OPEN"):
                        row[field] = previous[field]
                if previous != row:
                    existing[row["timestamp"]] = row
                    changed = True
        if changed or not path.exists():
            ordered = sorted(existing.values(), key=lambda r: r.get("timestamp", ""))
            path.write_text(json.dumps(ordered, ensure_ascii=False, indent=2), encoding="utf-8")
    _prune_signal_journal()

def _start_signal_premium_tracking(row):
    """Subscribe to real option LTP for every confirmed signal, independent of paper mode."""
    if not row or row.get("status") != "CONFIRMED" or not row.get("trade_id"):
        return
    try:
        contract = _resolve_atm_nifty_option(float(row.get("entry") or row.get("price")), row["signal"])
        row.update({
            "sim_option_symbol": contract["symbol"], "sim_option_token": contract["token"],
            "sim_option_expiry": contract["expiry"], "sim_option_strike": contract["strike"],
            "sim_option_lot_size": contract["lot_size"], "sim_premium_status": "WAITING FOR OPTION QUOTE",
            "sim_premium_source": "Angel One live option WebSocket LTP"
        })
        path = _signal_log_path(pd.Timestamp(row["timestamp"]).strftime("%Y-%m-%d"))
        with _signal_log_lock:
            rows = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
            for saved in rows:
                if saved.get("trade_id") == row["trade_id"]:
                    saved.update({k: v for k, v in row.items() if k.startswith("sim_")})
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(path)
        token = str(contract["token"])
        with _option_tick_lock:
            _signal_option_tokens.add(token)
        c = _angel_client
        if c and c.ws_connected:
            c.subscribe_market_tokens([token], exchange_type=2)
        else:
            row["sim_premium_status"] = "WAITING FOR ANGEL ONE WEBSOCKET"
    except Exception as exc:
        logger.warning("Could not start signal premium tracking for %s: %s", row.get("trade_id"), exc)
        row["sim_premium_status"] = "UNAVAILABLE: " + str(exc)[:160]


def _record_current_live_signal(candle_frame, analysis, frames):
    row = _signal_row(candle_frame, analysis, frames)
    if row and row.get("scan_window"):
        _save_signal_rows([row], pd.Timestamp(row["timestamp"]).strftime("%Y-%m-%d"))
        if row.get("status") == "CONFIRMED":
            _start_signal_premium_tracking(row)

def _backfill_today_signal_log():
    """Rebuild today's per-completed-minute journal from local cached candles; no broker request."""
    global _signal_log_backfilled_date
    today = _now_ist().date().isoformat()
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
            row = _signal_row(context, analysis, frames)
            if row and row.get("scan_window"):
                rows.append(row)
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
        now = _now_ist()
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
    token = str(message.get("token", "")).strip()
    with _paper_lock:
        active_option_token = (str(_paper_state["open_positions"][0].get("option_token"))
                               if _paper_state["open_positions"] else None)
    with _option_tick_lock:
        is_signal_option_token = token in _signal_option_tokens
    if token and ((active_option_token and token == active_option_token) or is_signal_option_token):
        _on_option_tick(message)
        return
    if _nifty_spot_token and token and token != str(_nifty_spot_token):
        return
    price_raw = message.get("last_traded_price", message.get("ltp"))
    if price_raw is None:
        return
    # SmartAPI index LTP is supplied in paise by the binary WebSocket parser.
    price = float(price_raw) / 100.0
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
            # "angel_one_connected" means the live Angel One market connection is active.
            # The JWT may intentionally remain available after disconnect so the API,
            # cached candles and dashboard continue running.
            "angel_one_connected": bool(_angel_client and _angel_client.ws_connected),
            "session_authenticated": bool(_angel_client and _angel_client.jwt),
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
    global _angel_client, _nifty_spot_token
    try:
        creds, v = credentials()
        _nifty_spot_token = str(v["ANGEL_NIFTY_TOKEN"])
        # Warm the option instrument master once at connection time. If unavailable,
        # spot scanner continues, but ATM option paper entries are safely blocked.
        try:
            _load_option_master()
        except Exception as master_exc:
            logger.warning("ATM option paper mode unavailable; instrument master load failed: %s", master_exc)
            with _paper_lock:
                _paper_state["last_error"] = "ATM option instrument master unavailable: " + str(master_exc)[:220]
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

@app.post("/api/disconnect")
def disconnect():
    """Disconnect Angel One WebSocket without stopping Uvicorn or deleting local data."""
    global _angel_client
    try:
        if _angel_client:
            _angel_client.close_market_websocket()
        with _live_lock:
            _live.update({"price": None, "timestamp": None, "timestamp_dt": None,
                          "sequence": None, "forming": None})
        return {"ok": True, "angel_one_connected": False,
                "message": "Angel One WebSocket disconnected. API and saved data remain available."}
    except Exception as exc:
        logger.exception("Angel One disconnect failed")
        raise HTTPException(status_code=400, detail=str(exc)[:250])


@app.get("/api/scanner/status")
def scanner_status():
    running = bool(_engine_thread and _engine_thread.is_alive() and not _engine_stop.is_set())
    return {"ok": True, "scanner_running": running,
            "angel_one_connected": bool(_angel_client and _angel_client.ws_connected),
            "api_running": True}


@app.post("/api/scanner/start")
def scanner_start():
    global _engine_thread
    if _engine_thread is not None and _engine_thread.is_alive() and not _engine_stop.is_set():
        return {"ok": True, "scanner_running": True, "message": "Scanner is already running."}
    _engine_stop.clear()
    _engine_thread = threading.Thread(target=_background_engine,
        name="nifty-smc-background-engine", daemon=True)
    _engine_thread.start()
    return {"ok": True, "scanner_running": True, "message": "Background SMC scanner started."}


@app.post("/api/scanner/stop")
def scanner_stop():
    _engine_stop.set()
    if _engine_thread and _engine_thread.is_alive():
        _engine_thread.join(timeout=5)
    return {"ok": True, "scanner_running": bool(_engine_thread and _engine_thread.is_alive()),
            "message": "Scanner stop requested. API and Angel One connection remain available."}


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
                "note": "SMC-only analysis from completed local candles: 5m structure → 2m FVG/OB/IDM → 1m entry. Signals recorded all day; eligible entries restricted to 09:30–11:50 IST. No orders placed."}
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


# Storage retention: preserve the existing 250-session replay window and its
# existing 45-calendar-day indicator warm-up margin. Cleanup runs only after a
# successful report build, never while a report is downloading/replaying.
HISTORY_CACHE_SESSIONS = 250
HISTORY_CACHE_WARMUP_CALENDAR_DAYS = 45
BACKTEST_RESULT_CACHE_KEEP = 3

def _prune_history_cache():
    """Remove only complete historical chunks older than the 250-session + warm-up window."""
    try:
        calendar_days = int(math.ceil(HISTORY_CACHE_SESSIONS * 7 / 5)) + HISTORY_CACHE_WARMUP_CALENDAR_DAYS
        cutoff = (_now_ist().replace(tzinfo=None) - timedelta(days=calendar_days)).date()
        pattern = re.compile(r"^nifty_1m_(\\d{8})_(\\d{8})\\.csv$")
        removed = 0
        for cache_file in CACHE_DIR.iterdir():
            match = pattern.match(cache_file.name)
            if not match or not cache_file.is_file():
                continue
            try:
                chunk_end = datetime.strptime(match.group(2), "%Y%m%d").date()
                # Keep the chunk intersecting the cutoff boundary and all newer chunks.
                if chunk_end <= cutoff:
                    cache_file.unlink()
                    removed += 1
            except (OSError, ValueError):
                logger.warning("Could not evaluate historical cache file %s", cache_file)
        if removed:
            logger.info("Historical cache retention removed %s expired chunks (cutoff %s)", removed, cutoff)
    except Exception:
        logger.exception("Historical cache retention cleanup failed")

def _prune_backtest_result_cache():
    """Keep the three newest valid SMC replay result caches; leave unrelated files alone."""
    try:
        candidates = []
        pattern = re.compile(r"^[a-f0-9]{64}_cost_[0-9]+\\.[0-9]{2}_smc_morning_v2\\.json$")
        for result_file in RESULT_CACHE_DIR.iterdir():
            if result_file.is_file() and pattern.match(result_file.name):
                try:
                    # Only manage files belonging to this known cache format.
                    json.loads(result_file.read_text(encoding="utf-8"))
                    candidates.append(result_file)
                except Exception:
                    logger.warning("Leaving unreadable backtest cache untouched: %s", result_file)
        candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        for stale_file in candidates[BACKTEST_RESULT_CACHE_KEEP:]:
            try:
                stale_file.unlink()
            except OSError:
                logger.warning("Could not remove obsolete backtest result cache %s", stale_file)
    except Exception:
        logger.exception("Backtest result cache cleanup failed")

def _run_storage_retention():
    _prune_history_cache()
    _prune_backtest_result_cache()


def _run_history_job(job_id, sessions, cost_points, start_date=None, end_date=None):
    def update(**kwargs):
        with _history_jobs_lock:
            _history_jobs[job_id].update(kwargs)
    try:
        c = client()
        _, env = credentials()
        # About 1.5 calendar days per NSE trading session, plus a warm-up margin.
        calendar_days = int(math.ceil(sessions * 7 / 5)) + 45
        end = datetime.combine(_now_ist().date(), dtime(15, 30))
        start = (end - timedelta(days=calendar_days)).replace(hour=0, minute=0)
        if start_date:
            start = datetime.combine(datetime.strptime(start_date, "%Y-%m-%d").date(), dtime(0, 0))
        if end_date:
            end = datetime.combine(datetime.strptime(end_date, "%Y-%m-%d").date(), dtime(15, 30))
        if start >= end:
            raise ValueError("start_date must be earlier than end_date.")
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
        if start_date or end_date:
            start_day = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else dates[0]
            end_day = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else dates[-1]
            selected_dates = [d for d in dates if start_day <= d <= end_day]
        else:
            selected_dates = dates[-sessions:]
        selected = combined[combined.datetime.dt.date.isin(selected_dates)].reset_index(drop=True)
        if len(selected_dates) < sessions:
            update(message=f"Only {len(selected_dates)} sessions available; requested {sessions}. Continuing with available data.")
        update(status="replaying", progress_pct=0, message="Preparing completed four-timeframe candles for shared replay",
               sessions_found=len(selected_dates), candle_count=len(selected))
        fingerprint = hashlib.sha256(pd.util.hash_pandas_object(selected, index=False).values.tobytes()).hexdigest()
        result_cache_file = RESULT_CACHE_DIR / f"{fingerprint}_cost_{float(cost_points):.2f}_smc_unified_v3.json"
        if result_cache_file.exists():
            try:
                cached_result = json.loads(result_cache_file.read_text(encoding="utf-8"))
                cached_result["summary"].update({"sessions_requested": sessions, "sessions_included": len(selected_dates),
                    "calendar_days_searched": calendar_days, "one_minute_candles": len(selected),
                    "cache_chunks_used": cache_hits, "download_chunks": len(chunks),
                    "cache_location": str(CACHE_DIR), "strict_four_timeframe": True,
                    "replay_engine": "cached_unified_live_matched_v3"})
                update(status="completed", progress_pct=100, message="Loaded completed report from local result cache",
                       result=cached_result, completed_at=datetime.now().isoformat())
                try:
                    os.utime(result_cache_file, None)
                except OSError:
                    pass
                _run_storage_retention()
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
            "replay_engine": "unified_live_matched_v3"})
        try:
            result_cache_file.write_text(json.dumps(result, separators=(",", ":")), encoding="utf-8")
        except Exception:
            logger.exception("Could not save replay result cache")
        update(status="completed", progress_pct=100, message="Historical backtest completed", result=result,
               completed_at=datetime.now().isoformat())
        _run_storage_retention()
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
def start_history(sessions: int = Query(250, ge=15, le=250), cost_points: float = Query(2.0, ge=0, le=100),
                  start_date: str | None = Query(None), end_date: str | None = Query(None)):
    if sessions not in _ALLOWED_SESSIONS:
        raise HTTPException(status_code=422, detail="sessions must be one of 15, 30, 60, 90, 180 or 250")
    for label, value in (("start_date", start_date), ("end_date", end_date)):
        if value:
            try:
                datetime.strptime(value, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=422, detail=f"{label} must use YYYY-MM-DD.")
    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=422, detail="start_date must not be after end_date.")
    client()
    job_id = uuid.uuid4().hex[:12]
    with _history_jobs_lock:
        _history_jobs[job_id] = {"job_id": job_id, "status": "queued", "progress_pct": 0,
            "message": "Queued", "sessions_requested": sessions, "start_date": start_date,
            "end_date": end_date, "cost_points": cost_points,
            "created_at": datetime.now().isoformat()}
    thread = threading.Thread(target=_run_history_job, args=(job_id, sessions, cost_points, start_date, end_date), daemon=True)
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



def _attach_paper_trade_details(signal_rows):
    """Decorate signal rows from persisted paper positions so the journal reflects live paper trades."""
    if not signal_rows:
        return signal_rows
    try:
        with _paper_lock:
            positions = list(_paper_state.get("open_positions", [])) + list(_paper_state.get("closed_positions", []))
        by_trade_id = {str(p.get("trade_id")): p for p in positions if p.get("trade_id")}
        by_signal_time = {}
        for pos in positions:
            signal_time = pos.get("signal_time")
            if signal_time:
                try:
                    stamp = pd.Timestamp(signal_time)
                    stamp = stamp.tz_localize(IST) if stamp.tzinfo is None else stamp.tz_convert(IST)
                    by_signal_time[stamp.isoformat()] = pos
                except Exception:
                    pass
        for row in signal_rows:
            pos = by_trade_id.get(str(row.get("trade_id"))) if row.get("trade_id") else None
            if pos is None and row.get("timestamp"):
                try:
                    stamp = pd.Timestamp(row.get("timestamp"))
                    stamp = stamp.tz_localize(IST) if stamp.tzinfo is None else stamp.tz_convert(IST)
                    pos = by_signal_time.get(stamp.isoformat())
                except Exception:
                    pass
            if pos is None:
                continue
            row["paper_instrument"] = pos.get("option_symbol") or row.get("paper_instrument")
            row["paper_expiry"] = pos.get("option_expiry") or row.get("paper_expiry")
            row["paper_strike"] = pos.get("option_strike") if pos.get("option_strike") is not None else row.get("paper_strike")
            row["paper_entry_premium"] = pos.get("entry_price") if pos.get("entry_price") is not None else row.get("paper_entry_premium")
            row["paper_exit_premium"] = pos.get("exit_price") if pos.get("exit_price") is not None else row.get("paper_exit_premium")
            row["paper_last_premium"] = pos.get("last_price") if pos.get("last_price") is not None else row.get("paper_last_premium")
            row["paper_points_gained_lost"] = pos.get("realised_option_points", pos.get("unrealised_option_points", row.get("paper_points_gained_lost")))
            row["paper_pnl_rupees"] = pos.get("realised_pnl_rupees", pos.get("unrealised_pnl_rupees", row.get("paper_pnl_rupees")))
            row["option_pnl_rupees"] = pos.get("realised_pnl_rupees", pos.get("unrealised_pnl_rupees", row.get("option_pnl_rupees")))
            row["paper_trade_status"] = pos.get("status") or row.get("paper_trade_status")
            row["points_basis"] = "NIFTY spot points"
            if pos.get("exit_reason"):
                # Do not mix option premium exit/P&L into NIFTY spot columns.
                row["paper_exit_reason"] = pos.get("exit_reason")
                row["paper_exit_spot_reference"] = pos.get("exit_spot_price")
        return signal_rows
    except Exception:
        logger.exception("Could not enrich signal journal with paper position state")
        return signal_rows


@app.get("/api/signals/today")
def today_signal_history():
    """Today's completed-candle journal, backfilled from local cache only."""
    _prune_signal_journal()
    day = _now_ist().date().isoformat()
    _backfill_today_signal_log()
    _update_today_signal_outcomes(_live_frame(include_forming=False))
    path = _signal_log_path(day)
    try:
        rows = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
    except Exception:
        rows = []
    # Only expose scans in the requested 09:30-15:00 IST journal window.
    journal_rows = []
    for row in rows:
        try:
            stamp = pd.Timestamp(row.get("timestamp"))
            if SIGNAL_LOG_START <= stamp.time() <= SIGNAL_LOG_END:
                journal_rows.append(row)
        except Exception:
            continue
    confirmed = [r for r in journal_rows if r.get("status") == "CONFIRMED"]

    # Section 4 displays the newest 10 confirmed signals across saved trading days.
    # Keep today's scan counters and full today's scan payload unchanged for compatibility.
    recent_confirmed = []
    try:
        with _signal_log_lock:
            journal_files = list(SIGNAL_LOG_DIR.glob("nifty_signals_*.json"))
            for journal_file in journal_files:
                try:
                    saved_rows = json.loads(journal_file.read_text(encoding="utf-8"))
                except Exception:
                    logger.warning("Skipping unreadable signal journal: %s", journal_file)
                    continue
                if not isinstance(saved_rows, list):
                    continue
                for saved_row in saved_rows:
                    if (not isinstance(saved_row, dict)
                            or saved_row.get("status") != "CONFIRMED"
                            or str(saved_row.get("signal", "")).upper() not in ("CE", "PE")):
                        continue
                    try:
                        signal_stamp = pd.Timestamp(saved_row.get("timestamp"))
                        if signal_stamp.tzinfo is None:
                            signal_stamp = signal_stamp.tz_localize(IST)
                        else:
                            signal_stamp = signal_stamp.tz_convert(IST)
                        if SIGNAL_LOG_START <= signal_stamp.time() <= SIGNAL_LOG_END:
                            recent_confirmed.append((signal_stamp, saved_row))
                    except Exception:
                        continue
        recent_confirmed.sort(key=lambda item: item[0], reverse=True)
        recent_confirmed = [row for _, row in recent_confirmed[:10]]
    except Exception:
        logger.exception("Could not build recent confirmed signal list")
        recent_confirmed = sorted(confirmed, key=lambda r: r.get("timestamp", ""), reverse=True)[:10]

    _attach_paper_trade_details(journal_rows)
    _attach_paper_trade_details(recent_confirmed)
    # Keep confirmed-only response semantics while carrying the matching paper trade state.
    return {
        "ok": True, "date": day, "timezone": "Asia/Kolkata",
        "scan_count": len(journal_rows), "confirmed_count": len(confirmed),
        "wait_count": max(0, len(journal_rows) - len(confirmed)),
        "confirmed_signals": confirmed, "recent_confirmed_signals": recent_confirmed,
        "recent_confirmed_count": len(recent_confirmed),
        # Latest Angel One NIFTY spot tick for live, unrealised journal P&L.
        "live_spot_price": _live.get("price"),
        "live_spot_timestamp": _live.get("timestamp"),
        "all_scans": journal_rows, "recent_scans": journal_rows,
        "scan_window": "09:30-15:00 IST",
        "paper_entry_window": "09:30-11:50 IST",
        "recent_signal_limit": 10,
        "recent_signal_scope": "latest confirmed CE/PE signals across saved trading days",
        "source": "local completed 1m candles and saved live scans; no historical broker request",
        "outcome_basis": "SL/targets use NIFTY spot levels; simulated signal premium P&L and paper P&L use actual Angel One option LTP ticks when available."
    }


@app.get("/api/paper-trading")
def paper_trading_status():
    _force_close_paper_positions()
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
    today = _now_ist().date().isoformat()
    snapshot["daily_trade_count"] = int(snapshot.get("daily_trade_count", 0)) if snapshot.get("daily_trade_date") == today else 0
    snapshot["daily_trade_limit"] = MAX_PAPER_TRADES_PER_DAY
    snapshot["entry_window"] = "09:30-11:50 IST"
    snapshot["signal_log_window"] = "09:30-15:00 IST"
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
        _paper_state["updated_at"] = _now_ist().isoformat(timespec="seconds")
        snapshot = json.loads(json.dumps(_paper_state))
    _save_paper_state()
    return {"ok": True, **snapshot}



def _paper_update(price, trade_plan, signal, quote_time):
    """Manage a paper ATM NIFTY option; SL/targets are NIFTY spot levels, P&L is option premium."""
    if price is None:
        return
    now = _now_ist()
    spot = float(price)
    subscribe_contract = None
    unsubscribe_contract = None
    with _paper_lock:
        if _paper_state["open_positions"]:
            pos = _paper_state["open_positions"][0]
            side = pos["signal"]
            sign = 1 if side == "CE" else -1
            pos["last_spot_price"] = round(spot, 2)
            pos["last_spot_time"] = quote_time or now.isoformat(timespec="seconds")
            sl, t1, t2 = float(pos["stop_loss"]), float(pos["target_1"]), float(pos["target_2"])
            stop_hit = spot <= sl if side == "CE" else spot >= sl
            t1_hit = spot >= t1 if side == "CE" else spot <= t1
            t2_hit = spot >= t2 if side == "CE" else spot <= t2
            if t1_hit and not pos.get("target_1_hit"):
                pos["target_1_hit"] = True
                pos["target_1_hit_at"] = now.isoformat(timespec="seconds")
            pos["target_2_hit"] = bool(pos.get("target_2_hit") or t2_hit)
            pos["stop_hit"] = bool(pos.get("stop_hit") or stop_hit)
            pos["monitoring"] = {
                "stop_loss": "HIT" if stop_hit else "ACTIVE",
                "target_1": "HIT" if pos["target_1_hit"] else "ACTIVE",
                "target_2": "HIT" if pos["target_2_hit"] else "ACTIVE"
            }
            if pos.get("entry_price") is not None and pos.get("last_price") is not None:
                premium_points = float(pos["last_price"]) - float(pos["entry_price"])
                lots = int(pos["quantity_lots"])
                lot_size = int(pos["lot_size"])
                pos["unrealised_option_points"] = round(premium_points, 2)
                pos["unrealised_pnl_rupees"] = round(premium_points * lots * lot_size, 2)
            exit_reason = "SL HIT" if stop_hit else "TARGET 2 HIT" if t2_hit else "EOD EXIT" if now.time() >= FORCE_EXIT_TIME else None
            if exit_reason:
                pos["status"] = "CLOSED"
                pos["exit_reason"] = exit_reason
                pos["exit_spot_price"] = round(spot, 2)
                pos["closed_at"] = now.isoformat(timespec="seconds")
                if pos.get("last_price") is not None:
                    pos["exit_price"] = round(float(pos["last_price"]), 2)
                    premium_points = float(pos["exit_price"]) - float(pos["entry_price"])
                    pos["realised_option_points"] = round(premium_points, 2)
                    pos["realised_pnl_rupees"] = round(premium_points * int(pos["quantity_lots"]) * int(pos["lot_size"]), 2)
                pos["realised_spot_points"] = round((spot - float(pos["spot_entry_reference"])) * sign, 2)
                _paper_state["closed_positions"].append(pos.copy())
                _paper_state["closed_positions"] = _paper_state["closed_positions"][-100:]
                _paper_state["open_positions"] = []
                unsubscribe_contract = {"token": pos.get("option_token"), "exchange_type": 2}
                _journal_option_trade(pos)
        # Maximum 3 paper entries per IST day, one open position at a time.
        # The entry window ends at 11:50; signal journaling continues until 15:00.
        today = now.date().isoformat()
        if _paper_state.get("daily_trade_date") != today:
            _paper_state["daily_trade_date"] = today
            _paper_state["daily_trade_count"] = 0
        if (_paper_state["enabled"] and not _paper_state["open_positions"]
            and int(_paper_state.get("daily_trade_count", 0)) < MAX_PAPER_TRADES_PER_DAY
            and trade_plan and signal in ("CE", "PE")
            and trade_plan.get("signal_time") != _paper_state["last_signal_time"]
            and _in_entry_window(now)):
            try:
                contract = _resolve_atm_nifty_option(spot, signal)
                quantity = int(_paper_state["quantity_lots"])
                pos = {
                    "instrument": "NIFTY ATM OPTION · PAPER",
                    "option_symbol": contract["symbol"], "option_token": contract["token"],
                    "option_expiry": contract["expiry"], "option_strike": contract["strike"],
                    "option_type": contract["option_type"], "lot_size": contract["lot_size"],
                    "exchange": "NFO", "signal": signal, "side": "BUY " + signal,
                    "quantity_lots": quantity, "entry_price": None, "last_price": None,
                    "trade_id": hashlib.sha256(f"{trade_plan.get('signal_time')}|{signal}".encode("utf-8")).hexdigest()[:20],
                    "stop_loss": float(trade_plan["stop_loss"]),
                    "target_1": float(trade_plan["target_1"]), "target_2": float(trade_plan["target_2"]),
                    "spot_entry_reference": float(trade_plan["entry_reference"]),
                    "last_spot_price": round(spot, 2), "unrealised_option_points": None,
                    "unrealised_pnl_rupees": None, "target_1_hit": False, "target_2_hit": False,
                    "stop_hit": False, "monitoring": {"stop_loss":"ACTIVE","target_1":"ACTIVE","target_2":"ACTIVE"},
                    "status": "PENDING OPTION QUOTE", "opened_at": now.isoformat(timespec="seconds"),
                    "last_price_time": None, "signal_time": trade_plan.get("signal_time"),
                    "entry_note": "Entry premium is set from the first live option WebSocket tick after signal."
                }
                _paper_state["open_positions"] = [pos]
                _paper_state["last_signal_time"] = trade_plan.get("signal_time")
                _paper_state["last_trade_date"] = today
                _paper_state["daily_trade_count"] = int(_paper_state.get("daily_trade_count", 0)) + 1
                _paper_state["last_error"] = None
                subscribe_contract = {"token": contract["token"], "exchange_type": 2}
                _journal_option_trade(pos)
            except Exception as exc:
                _paper_state["last_error"] = str(exc)[:300]
                logger.exception("Could not open ATM NIFTY paper option position")
        _paper_state["updated_at"] = now.isoformat(timespec="seconds")
    _save_paper_state()
    # Subscribe/unsubscribe outside the paper lock.
    try:
        c = _angel_client
        if c and c.ws_connected and subscribe_contract:
            c.subscribe_market_tokens([subscribe_contract["token"]], exchange_type=subscribe_contract["exchange_type"])
        if c and c.ws_connected and unsubscribe_contract and unsubscribe_contract.get("token"):
            c.unsubscribe_market_tokens([unsubscribe_contract["token"]], exchange_type=unsubscribe_contract["exchange_type"])
    except Exception as exc:
        with _paper_lock:
            _paper_state["last_error"] = "Option market-data subscription failed: " + str(exc)[:220]
        logger.exception("Option WebSocket subscription update failed")


def _update_signal_premium_tick(token, premium, stamp):
    """Persist real option quotes against signal rows; paper mode is not required."""
    if token not in _signal_option_tokens:
        return
    for path in SIGNAL_LOG_DIR.glob("nifty_signals_*.json"):
        try:
            with _signal_log_lock:
                rows = json.loads(path.read_text(encoding="utf-8"))
                changed = False
                for row in rows:
                    if str(row.get("sim_option_token") or "") != token:
                        continue
                    new_entry_quote = row.get("sim_premium_entry") is None
                    if new_entry_quote:
                        row["sim_premium_entry"] = round(float(premium), 2)
                        row["sim_premium_entry_time"] = stamp.isoformat(timespec="seconds")
                        row["sim_premium_status"] = "OPEN · SIMULATED"
                    row["sim_premium_last"] = round(float(premium), 2)
                    row["sim_premium_last_time"] = stamp.isoformat(timespec="seconds")
                    if row.get("sim_premium_entry") is not None:
                        pts = round(float(premium) - float(row["sim_premium_entry"]), 2)
                        row["sim_premium_points"] = pts
                        lots = max(1, int(_paper_state.get("quantity_lots", 1)))
                        lot_size = int(row.get("sim_option_lot_size") or 0)
                        row["sim_premium_rupees"] = round(pts * lots * lot_size, 2) if lot_size else None
                    outcome = str(row.get("outcome") or "OPEN").upper()
                    if (not new_entry_quote and outcome not in ("OPEN", "TARGET 1 HIT", "")
                            and row.get("sim_premium_entry") is not None):
                        row["sim_premium_exit"] = round(float(premium), 2)
                        row["sim_premium_exit_time"] = stamp.isoformat(timespec="seconds")
                        row["sim_premium_points"] = round(float(premium) - float(row["sim_premium_entry"]), 2)
                        lots = max(1, int(_paper_state.get("quantity_lots", 1)))
                        lot_size = int(row.get("sim_option_lot_size") or 0)
                        row["sim_premium_rupees"] = round(row["sim_premium_points"] * lots * lot_size, 2) if lot_size else None
                        row["sim_premium_status"] = "CLOSED · SIMULATED"
                    changed = True
                if changed:
                    tmp = path.with_suffix(".tmp")
                    tmp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
                    tmp.replace(path)
        except Exception:
            logger.exception("Could not persist simulated signal option quote")


def _on_option_tick(message):
    token = str(message.get("token", "")).strip()
    raw = message.get("last_traded_price")
    if not token or raw is None:
        return
    # SmartAPI WebSocket LTP values are integer paise for both index and option contracts.
    premium = float(raw) / 100.0
    stamp = _tick_time(message)
    _update_signal_premium_tick(token, premium, stamp)
    with _paper_lock:
        if not _paper_state["open_positions"]:
            return
        pos = _paper_state["open_positions"][0]
        if token != str(pos.get("option_token")):
            return
        if pos.get("entry_price") is None:
            pos["entry_price"] = round(premium, 2)
            pos["entry_price_time"] = stamp.isoformat(timespec="seconds")
            pos["status"] = "OPEN"
            pos["entry_note"] = "Simulated buy at first received option LTP; bid/ask and slippage are not modeled."
        pos["last_price"] = round(premium, 2)
        pos["last_price_time"] = stamp.isoformat(timespec="seconds")
        premium_points = premium - float(pos["entry_price"])
        pos["unrealised_option_points"] = round(premium_points, 2)
        pos["unrealised_pnl_rupees"] = round(premium_points * int(pos["quantity_lots"]) * int(pos["lot_size"]), 2)
        pos["updated_at"] = stamp.isoformat(timespec="seconds")
        _journal_option_trade(pos)
    _save_paper_state()


def _force_close_paper_positions(reason="EOD EXIT"):
    """Hard-close every paper position at 15:20 IST, even without fresh market ticks."""
    now = _now_ist()
    if now.time() < FORCE_EXIT_TIME:
        return
    closed_now = 0
    with _paper_lock:
        if not _paper_state["open_positions"]:
            return
        for pos in list(_paper_state["open_positions"]):
            pos["status"] = "CLOSED"
            pos["exit_reason"] = reason
            pos["closed_at"] = now.isoformat(timespec="seconds")
            pos["exit_note"] = "Forced paper close before NSE market close; used last available option premium if present."
            pos["exit_spot_price"] = pos.get("last_spot_price")
            if pos.get("last_price") is not None and pos.get("entry_price") is not None:
                pos["exit_price"] = round(float(pos["last_price"]), 2)
                pts = float(pos["exit_price"]) - float(pos["entry_price"])
                pos["realised_option_points"] = round(pts, 2)
                pos["realised_pnl_rupees"] = round(pts * int(pos["quantity_lots"]) * int(pos["lot_size"]), 2)
            else:
                pos["exit_price"] = None
                pos["realised_option_points"] = None
                pos["realised_pnl_rupees"] = None
                pos["exit_note"] += " No option premium was available; realised premium P&L is unknown, not zero."
            _paper_state["closed_positions"].append(pos.copy())
            closed_now += 1
            _journal_option_trade(pos)
        _paper_state["closed_positions"] = _paper_state["closed_positions"][-500:]
        _paper_state["open_positions"] = []
        _paper_state["updated_at"] = now.isoformat(timespec="seconds")
    _save_paper_state()
    logger.warning("Forced %s paper position(s) closed at %s IST", closed_now, now.isoformat())

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
            _update_today_signal_outcomes(frame)
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
                    "SMC-only analysis uses completed 5m/2m/1m candles. Eligible entries are restricted to 09:30-11:50 IST; out-of-window scans are retained for research. "
                    "FVG, order block, IDM, BOS and MSS are heuristic price-action detections; no orders are placed."
                ),
                "websocket_error": c.ws_error}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Live feed read failed")
        raise HTTPException(status_code=400, detail=str(e)[:300])


_engine_stop = threading.Event()
_engine_thread = None

def _background_engine():
    retry = 10
    while not _engine_stop.is_set():
        try:
            _force_close_paper_positions()
            # live() owns broker connection, candle processing, journal and paper updates.
            live()
            retry = 10
            _engine_stop.wait(2)
        except Exception as exc:
            logger.warning("Background scanner iteration failed: %s", str(exc)[:200])
            _engine_stop.wait(retry)
            retry = min(retry * 2, 300)

@app.on_event("startup")
def startup_load_local_candles():
    global _engine_thread
    count = _load_live_cache()
    _load_paper_state()
    _backfill_today_signal_log()
    if count:
        logger.info("Loaded %s completed 1m candles from persistent local cache", count)
    if _engine_thread is None or not _engine_thread.is_alive():
        _engine_stop.clear()
        _engine_thread = threading.Thread(target=_background_engine, name="nifty-smc-background-engine", daemon=True)
        _engine_thread.start()
        logger.info("Background scanner engine started.")

@app.on_event("shutdown")
def shutdown_background_engine():
    _engine_stop.set()

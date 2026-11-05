import pandas as pd

TIMEFRAMES = {"1m": 1, "2m": 2, "5m": 5}


def _resample_session(df, minutes):
    if df is None or df.empty:
        return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])
    x = df.copy()
    x["datetime"] = pd.to_datetime(x["datetime"])
    x = x.sort_values("datetime").drop_duplicates("datetime").set_index("datetime")
    # NSE session starts at 09:15. A bar is retained only when all its 1m candles exist.
    rule = f"{minutes}min"
    grouped = x.resample(rule, origin="start_day", offset="9h15min", label="left", closed="left").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    )
    counts = x["close"].resample(rule, origin="start_day", offset="9h15min", label="left", closed="left").count()
    grouped = grouped[counts == minutes].dropna(subset=["open", "high", "low", "close"])
    return grouped.reset_index()


def build_timeframes(one, five=None, fifteen=None):
    one = one.copy()
    if one.empty:
        empty = pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])
        return {"1m": empty.copy(), "2m": empty.copy(), "5m": empty.copy()}
    one["datetime"] = pd.to_datetime(one["datetime"])
    one = one.sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)
    return {"1m": one, "2m": _resample_session(one, 2), "5m": _resample_session(one, 5)}


def _body(row):
    return abs(float(row.close) - float(row.open))

def _atr(df, n=14):
    if len(df) < 2: return 0.0
    prev = df.close.shift(1)
    tr = pd.concat([(df.high-df.low).abs(), (df.high-prev).abs(), (df.low-prev).abs()], axis=1).max(axis=1)
    return float(tr.tail(n).mean())

def _structure_direction(df, n=20):
    """Conservative close-confirmed structure proxy; no future bars are used."""
    if len(df) < n + 2: return None
    x=df.tail(n+1); last=x.iloc[-1]; prior=x.iloc[:-1]
    hi=float(prior.high.max()); lo=float(prior.low.min())
    if float(last.close)>hi: return "LONG"
    if float(last.close)<lo: return "SHORT"
    # Trend context from confirmed range position and short/long close means.
    fast=float(x.close.tail(5).mean()); slow=float(x.close.tail(15).mean())
    mid=(hi+lo)/2
    if fast>slow and float(last.close)>mid: return "LONG"
    if fast<slow and float(last.close)<mid: return "SHORT"
    return None


def _find_recent_fvg(df, direction, lookback=12):
    """Return most recent unfilled directional 3-candle FVG zone."""
    if len(df) < 3:
        return None
    for i in range(len(df)-1, max(1, len(df)-lookback)-1, -1):
        a, c = df.iloc[i-2], df.iloc[i]
        if direction == "LONG" and float(c.low) > float(a.high):
            lo, hi = float(a.high), float(c.low)
        elif direction == "SHORT" and float(c.high) < float(a.low):
            lo, hi = float(c.high), float(a.low)
        else:
            continue
        later = df.iloc[i+1:]
        # Skip gaps already fully invalidated by a close through the far edge.
        invalid = (later.close < lo).any() if direction == "LONG" else (later.close > hi).any()
        if not invalid:
            return {"low": lo, "high": hi, "index": i}
    return None


def _find_order_block(df, direction, lookback=10):
    """Last opposite candle before a directional displacement candle."""
    if len(df) < 5:
        return None
    for i in range(len(df)-1, max(2, len(df)-lookback)-1, -1):
        row = df.iloc[i]
        prior = df.iloc[max(0, i-5):i]
        avg = float((prior.close-prior.open).abs().mean()) if len(prior) else 0
        if avg <= 0 or _body(row) < 1.3*avg:
            continue
        directional = (float(row.close)>float(row.open)) if direction=="LONG" else (float(row.close)<float(row.open))
        if not directional:
            continue
        candidates = df.iloc[max(0,i-5):i]
        candidates = candidates[candidates.close<candidates.open] if direction=="LONG" else candidates[candidates.close>candidates.open]
        if candidates.empty:
            continue
        ob = candidates.iloc[-1]
        lo, hi = float(ob.low), float(ob.high)
        later = df.iloc[i+1:]
        invalid = (later.close < lo).any() if direction=="LONG" else (later.close > hi).any()
        if not invalid:
            return {"low":lo,"high":hi,"index":i}
    return None


def _detect_idm(df, direction, lookback=12):
    """IDM proxy: internal liquidity sweep and close reclaim, using only completed bars."""
    if len(df) < lookback+2:
        return False, None
    last=df.iloc[-1]
    internal=df.iloc[-lookback-1:-1]
    # Use internal range excluding the most recent candle; wick must sweep and close back inside.
    hi=float(internal.high.max()); lo=float(internal.low.min())
    if direction=="LONG" and float(last.low)<lo and float(last.close)>lo and float(last.close)>float(last.open):
        return True, {"level":lo,"side":"sell-side inducement swept"}
    if direction=="SHORT" and float(last.high)>hi and float(last.close)<hi and float(last.close)<float(last.open):
        return True, {"level":hi,"side":"buy-side inducement swept"}
    return False, None


def analyze_smc_stack(frames, entry_time=None):
    """NIFTY SMC-only model: 5m structure -> 2m FVG/OB/IDM -> 1m trigger; no 15m dependency."""
    out={"signal":"WAIT","direction":None,"score":0,"checks":{},"reasons":[],
         "strategy":"SMC FVG-OB-IDM-BOS-MSS v2"}
    required=("5m","2m","1m")
    if any(tf not in frames or frames[tf] is None for tf in required) or len(frames["5m"])<11 or len(frames["2m"])<14 or len(frames["1m"])<4:
        out["reasons"].append("Waiting for completed data: 11 completed 5m candles for the prior-10-bar BOS check, 14 completed 2m candles and 4 completed 1m candles."); return out
    f={k:frames[k].sort_values("datetime").reset_index(drop=True) for k in required}
    f5,f2,f1=f["5m"],f["2m"],f["1m"]
    last5,last2,last1=f5.iloc[-1],f2.iloc[-1],f1.iloc[-1]
    # Structure break on 5m: close beyond the prior 10-bar dealing range.
    prior5=f5.iloc[-11:-1]
    ph,pl=float(prior5.high.max()),float(prior5.low.min())
    bos_long=float(last5.close)>ph
    bos_short=float(last5.close)<pl
    # MSS proxy: direction changes against the preceding short-term close slope and breaks structure.
    prior_slope=float(f5.close.iloc[-6:-1].iloc[-1]-f5.close.iloc[-6:-1].iloc[0])
    mss_long=bos_long and prior_slope<0
    mss_short=bos_short and prior_slope>0
    direction="LONG" if bos_long or mss_long else "SHORT" if bos_short or mss_short else None
    # If no fresh BOS/MSS on the current 5m bar, retain recent structure direction only
    # for locating zones; a signal still requires a valid 1m trigger and zone retest.
    if direction is None:
        direction=_structure_direction(f5,3)
    if direction not in ("LONG","SHORT"):
        direction=None
    fvg=_find_recent_fvg(f2,direction) if direction else None
    ob=_find_order_block(f2,direction) if direction else None
    def retested(zone, row):
        return bool(zone and float(row.low)<=zone["high"] and float(row.high)>=zone["low"])
    fvg_retest=retested(fvg,last2)
    ob_retest=retested(ob,last2)
    idm,idm_meta=_detect_idm(f2,direction,12) if direction else (False,None)
    # 1m confirmation: close breaks the previous three completed 1m bars in the bias direction.
    prior1=f1.iloc[-4:-1]
    trigger_long=direction=="LONG" and float(last1.close)>float(last1.open) and float(last1.close)>float(prior1.high.max())
    trigger_short=direction=="SHORT" and float(last1.close)<float(last1.open) and float(last1.close)<float(prior1.low.min())
    trigger=bool(trigger_long or trigger_short)
    stamp=pd.Timestamp(last1.datetime)
    clock=stamp.strftime("%H:%M")
    time_ok="09:30"<=clock<="11:40"
    structure_ok=bool(bos_long or bos_short or mss_long or mss_short or direction)
    zone_ok=bool(fvg_retest or ob_retest)
    # Main entry rule: 5m structure context + (FVG or OB retest) + IDM or structure break + 1m trigger + time window.
    liquidity_ok=bool(idm or bos_long or bos_short or mss_long or mss_short)
    qualified=bool(direction and structure_ok and zone_ok and liquidity_ok and trigger and time_ok)
    checks={
      "5m_BOS":bool(bos_long or bos_short),"5m_MSS":bool(mss_long or mss_short),
      "2m_FVG_detected":bool(fvg),"2m_FVG_retest":fvg_retest,
      "2m_order_block_detected":bool(ob),"2m_order_block_retest":ob_retest,
      "2m_IDM_sweep":idm,"1m_entry_confirmation":trigger,"entry_window_0930_1140":time_ok
    }
    score=sum(bool(v) for v in checks.values())
    out.update({"direction":direction,"signal":"CE" if qualified and direction=="LONG" else "PE" if qualified else "WAIT",
       "score":score,"checks":checks,"fvg_zone":fvg,"order_block_zone":ob,"idm":idm_meta,
       "minimum_score":"Structure + (FVG or OB) + (IDM or BOS/MSS) + 1m trigger + time window",
       "reasons":[f"5m structure direction: {direction or 'not established'}",
          f"BOS: {'yes' if checks['5m_BOS'] else 'no'}; MSS: {'yes' if checks['5m_MSS'] else 'no'}",
          f"2m FVG: {'retest' if fvg_retest else 'detected' if fvg else 'not found'}",
          f"2m order block: {'retest' if ob_retest else 'detected' if ob else 'not found'}",
          f"2m IDM: {'sweep/reclaim confirmed' if idm else 'not confirmed'}",
          f"1m trigger: {'confirmed' if trigger else 'waiting'}",
          f"Entry time {clock}: {'inside' if time_ok else 'outside'} 09:30–11:40"],
       "diagnostics":{"bos":int(bool(bos_long or bos_short)),"mss":int(bool(mss_long or mss_short)),
          "fvg_detected":int(bool(fvg)),"fvg_retest":int(fvg_retest),"ob_detected":int(bool(ob)),
          "ob_retest":int(ob_retest),"idm":int(idm),"trigger":int(trigger),"time_window":int(time_ok)}})
    return out




def build_trade_plan(candle_frame, analysis):
    """Build the exact spot trade plan used by both live journal and historical replay."""
    if candle_frame is None or candle_frame.empty:
        return None
    last = candle_frame.iloc[-1]
    stamp = pd.Timestamp(last["datetime"])
    signal = str((analysis or {}).get("signal") or "WAIT").upper()
    if signal not in ("CE", "PE"):
        return None
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
        return None
    return {"timestamp": stamp.isoformat(), "signal": signal, "entry": round(entry, 2),
            "stop_loss": round(stop, 2), "target_1": round(target_1, 2),
            "target_2": round(target_2, 2), "risk_points": round(risk, 2),
            "risk_reward": "1:1 / 1:2"}


def evaluate_trade_plan(plan, candle_frame):
    """Evaluate a shared plan against later completed 1m candles; conservative stop-first."""
    if not plan or candle_frame is None or candle_frame.empty:
        return {"outcome": "OPEN", "exit_price": None, "exit_time": None,
                "target_1_hit": False, "target_1_hit_time": None}
    stamp = pd.Timestamp(plan["timestamp"])
    day = stamp.date()
    side = plan["signal"]
    entry = float(plan["entry"])
    sl, t1, t2 = float(plan["stop_loss"]), float(plan["target_1"]), float(plan["target_2"])
    sign = 1 if side == "CE" else -1
    work = candle_frame.copy()
    work["datetime"] = pd.to_datetime(work["datetime"])
    later = work[(work["datetime"] > stamp) & (work["datetime"].dt.date == day)].sort_values("datetime")
    target1_seen = False
    target1_time = None
    for candle in later.itertuples(index=False):
        high, low = float(candle.high), float(candle.low)
        stop_hit = low <= sl if side == "CE" else high >= sl
        t1_hit = high >= t1 if side == "CE" else low <= t1
        t2_hit = high >= t2 if side == "CE" else low <= t2
        # OHLC cannot prove the intrabar sequence. Stop wins any same-candle conflict.
        if stop_hit:
            return {"outcome": "SL HIT", "exit_price": round(sl, 2),
                    "exit_time": pd.Timestamp(candle.datetime).isoformat(),
                    "points_gained_lost": round((sl-entry)*sign, 2),
                    "target_1_hit": target1_seen, "target_1_hit_time": target1_time,
                    "outcome_note": "Conservative stop-first rule; intrabar sequence is unknown."}
        if t2_hit:
            return {"outcome": "TARGET 2 HIT", "exit_price": round(t2, 2),
                    "exit_time": pd.Timestamp(candle.datetime).isoformat(),
                    "points_gained_lost": round((t2-entry)*sign, 2),
                    "target_1_hit": True, "target_1_hit_time": target1_time or pd.Timestamp(candle.datetime).isoformat()}
        if t1_hit and not target1_seen:
            target1_seen = True
            target1_time = pd.Timestamp(candle.datetime).isoformat()
    if not later.empty and pd.Timestamp(later.iloc[-1]["datetime"]).strftime("%H:%M") >= "15:25":
        last = later.iloc[-1]
        exit_price = float(last["close"])
        return {"outcome": "EOD EXIT", "exit_price": round(exit_price, 2),
                "exit_time": pd.Timestamp(last["datetime"]).isoformat(),
                "points_gained_lost": round((exit_price-entry)*sign, 2),
                "target_1_hit": target1_seen, "target_1_hit_time": target1_time}
    return {"outcome": "TARGET 1 HIT" if target1_seen else "OPEN",
            "exit_price": None, "exit_time": None, "points_gained_lost": None,
            "target_1_hit": target1_seen, "target_1_hit_time": target1_time}


def _summarise_replay(signals, cost_points, strategy):
    resolved=[x for x in signals if x.get("net_points") is not None]
    wins=sum(x["net_points"]>0 for x in resolved); losses=len(resolved)-wins
    targets=sum(x["outcome"]=="TARGET 2 HIT" for x in signals)
    stops=sum(str(x["outcome"]).startswith("STOP") for x in signals)
    eod=sum(x["outcome"]=="EOD_EXIT" for x in signals)
    net=sum(x["net_points"] for x in resolved)
    def side(side_name):
        rows=[x for x in signals if x["signal"]==side_name]
        done=[x for x in rows if x.get("net_points") is not None]
        return {"trades":len(rows),"resolved":len(done),"net_positive":sum(x["net_points"]>0 for x in done),
                "net_points":round(sum(x["net_points"] for x in done),2)}
    gross=sum(x["gross_points"] for x in resolved)
    neg=abs(sum(min(0,x["net_points"]) for x in resolved))
    return {"signals":signals,"summary":{"trades":len(signals),"resolved":len(resolved),"targets":targets,"stops":stops,
        "eod_exits":eod,"target_1_hits":sum(bool(x.get("target_1_hit")) for x in signals),
        "target_2_hits":sum(x.get("outcome")=="TARGET 2 HIT" for x in signals),
        "open":len(signals)-len(resolved),"profitable_after_costs":wins,
        "loss_or_nonpositive_after_costs":losses,"win_rate_pct":round(100*wins/len(resolved),2) if resolved else None,
        "target_hit_rate_pct":round(100*targets/len(signals),2) if signals else None,
        "profit_factor":round(sum(max(0,x["net_points"]) for x in resolved)/neg,3) if neg else None,
        "average_net_points":round(net/len(resolved),2) if resolved else None,"gross_points":round(gross,2),
        "estimated_cost_points":round(float(cost_points)*len(resolved),2),"net_points":round(net,2),
        "cost_points_per_trade":float(cost_points),"ce":side("BUY CE"),"pe":side("BUY PE"),"strategy":strategy,
        "note":"NIFTY spot-point research, not options-premium P&L. Resolved trades only are used for win rate."}}

def historical_replay_all(frames, max_trades=5000, cost_points=2.0, progress_callback=None):
    """Replay every confirmed live-style signal with the same entry/SL/T1/T2 and exit rules."""
    one = frames["1m"].sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True).copy()
    if len(one) < 100:
        return {"smc_pac": _summarise_replay([], cost_points, "smc_pac")}
    one["datetime"] = pd.to_datetime(one["datetime"])
    tf_frames = {k: v.copy().sort_values("datetime").reset_index(drop=True)
                 for k, v in frames.items() if k in ("1m", "2m", "5m")}
    completed = {}
    for tf, f in tf_frames.items():
        f["datetime"] = pd.to_datetime(f["datetime"])
        completed[tf] = (f["datetime"] + pd.to_timedelta(TIMEFRAMES[tf], unit="m")).to_numpy(dtype="datetime64[ns]")
    signals = []
    total = len(one)
    diagnostics = {k: 0 for k in ("bars_in_window", "bos", "mss", "fvg_detected", "fvg_retest", "ob_detected", "ob_retest", "idm", "trigger", "qualified")}
    for i in range(total):
        stamp = pd.Timestamp(one.datetime.iloc[i])
        clock = stamp.strftime("%H:%M")
        if not ("09:30" <= clock <= "11:40"):
            continue
        diagnostics["bars_in_window"] += 1
        if len(signals) >= max_trades:
            break
        close_time = stamp + pd.Timedelta(minutes=1)
        snapshots = {}
        for tf, f in tf_frames.items():
            pos = int(completed[tf].searchsorted(close_time.to_datetime64(), side="right"))
            snapshots[tf] = f.iloc[max(0, pos-80):pos]
        if len(snapshots.get("5m", [])) < 11 or len(snapshots.get("2m", [])) < 14 or len(snapshots.get("1m", [])) < 4:
            continue
        stack = analyze_smc_stack(snapshots)
        for k, v in stack.get("diagnostics", {}).items():
            if k in diagnostics:
                diagnostics[k] += int(bool(v))
        if stack.get("signal") not in ("CE", "PE"):
            continue
        diagnostics["qualified"] += 1
        signal_frame = one.iloc[:i+1].reset_index(drop=True)
        plan = build_trade_plan(signal_frame, stack)
        if not plan:
            continue
        evaluation = evaluate_trade_plan(plan, one)
        gross = evaluation.get("points_gained_lost")
        net = None if gross is None else round(float(gross) - float(cost_points), 2)
        signals.append({
            "time": plan["timestamp"], "signal": "BUY CE" if plan["signal"] == "CE" else "BUY PE",
            "entry": plan["entry"], "stop_loss": plan["stop_loss"],
            "target_1": plan["target_1"], "target_2": plan["target_2"], "target": plan["target_2"],
            "outcome": evaluation["outcome"], "target_1_hit": evaluation.get("target_1_hit", False),
            "target_1_hit_time": evaluation.get("target_1_hit_time"),
            "exit_price": evaluation.get("exit_price"), "exit_time": evaluation.get("exit_time"),
            "gross_points": gross, "cost_points": round(float(cost_points), 2), "net_points": net,
            "reason": "SMC FVG/OB/IDM/BOS/MSS: " + "; ".join(stack.get("reasons", [])),
            "confluence_score": stack.get("score", 0), "checks": stack.get("checks", {}),
            "risk_reward": "1:1 / 1:2", "strategy": "smc_pac", "signal_bar_time": plan["timestamp"]
        })
        if progress_callback and i % 500 == 0:
            progress_callback(round((i+1)*100/total, 1), i+1, total)
    if progress_callback:
        progress_callback(100, total, total)
    result = _summarise_replay(signals, cost_points, "smc_pac")
    result["summary"]["diagnostics"] = diagnostics
    result["summary"]["signal_window"] = "09:30–11:40 IST"
    result["summary"]["timeframes"] = "5m structure → 2m FVG/OB/IDM → 1m entry"
    result["summary"]["trade_plan"] = "Live-matched: signal candle close, OB/10-bar SL, Target 1 = 1R, Target 2 = 2R"
    result["summary"]["overlapping_signals"] = "Each confirmed signal is evaluated independently, matching the live journal; not a broker fill simulation."
    return {"smc_pac": result}


def historical_replay(frames, max_trades=500, cost_points=2.0, strategy="smc_pac"):
    """Backward-compatible SMC-only replay entry point."""
    return historical_replay_all(frames, max_trades=max_trades, cost_points=cost_points)["smc_pac"]

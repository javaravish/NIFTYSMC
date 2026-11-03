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



def _summarise_replay(signals, cost_points, strategy):
    resolved=[x for x in signals if x.get("net_points") is not None]
    wins=sum(x["net_points"]>0 for x in resolved); losses=len(resolved)-wins
    targets=sum(x["outcome"]=="TARGET" for x in signals)
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
        "eod_exits":eod,"open":len(signals)-len(resolved),"profitable_after_costs":wins,
        "loss_or_nonpositive_after_costs":losses,"win_rate_pct":round(100*wins/len(resolved),2) if resolved else None,
        "target_hit_rate_pct":round(100*targets/len(signals),2) if signals else None,
        "profit_factor":round(sum(max(0,x["net_points"]) for x in resolved)/neg,3) if neg else None,
        "average_net_points":round(net/len(resolved),2) if resolved else None,"gross_points":round(gross,2),
        "estimated_cost_points":round(float(cost_points)*len(resolved),2),"net_points":round(net,2),
        "cost_points_per_trade":float(cost_points),"ce":side("BUY CE"),"pe":side("BUY PE"),"strategy":strategy,
        "note":"NIFTY spot-point research, not options-premium P&L. Resolved trades only are used for win rate."}}

def historical_replay_all(frames, max_trades=5000, cost_points=2.0, progress_callback=None):
    """SMC-only replay; entries are permitted only 09:30-11:40 exchange local time."""
    one=frames["1m"].sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True).copy()
    if len(one)<100:
        return {"smc_pac":_summarise_replay([],cost_points,"smc_pac")}
    one["datetime"]=pd.to_datetime(one["datetime"])
    tf_frames={k:v.copy().sort_values("datetime").reset_index(drop=True) for k,v in frames.items() if k in ("1m","2m","5m")}
    completed={}
    for tf,f in tf_frames.items():
        f["datetime"]=pd.to_datetime(f["datetime"])
        completed[tf]=(f["datetime"]+pd.to_timedelta(TIMEFRAMES[tf],unit="m")).to_numpy(dtype="datetime64[ns]")
    session_last={}
    for idx,stamp in enumerate(one.datetime): session_last[stamp.date()]=idx
    signals=[]; traded_dates=set(); total=len(one); diagnostics={k:0 for k in
       ("bars_in_window","bos","mss","fvg_detected","fvg_retest","ob_detected","ob_retest","idm","trigger","qualified")}
    for i in range(0,total-1):
        stamp=pd.Timestamp(one.datetime.iloc[i]); day=stamp.date(); clock=stamp.strftime("%H:%M")
        if not ("09:30"<=clock<="11:40"):
            continue
        diagnostics["bars_in_window"]+=1
        if day in traded_dates or len(signals)>=max_trades:
            continue
        close_time=stamp+pd.Timedelta(minutes=1)
        snapshots={}
        for tf,f in tf_frames.items():
            pos=int(completed[tf].searchsorted(close_time.to_datetime64(),side="right"))
            snapshots[tf]=f.iloc[max(0,pos-80):pos]
        if len(snapshots.get("5m",[]))<5 or len(snapshots.get("2m",[]))<14 or len(snapshots.get("1m",[]))<4:
            continue
        stack=analyze_smc_stack(snapshots)
        for k,v in stack.get("diagnostics",{}).items():
            if k in diagnostics: diagnostics[k]+=int(bool(v))
        direction=stack["signal"]
        if direction not in ("CE","PE"):
            continue
        diagnostics["qualified"]+=1
        entry_idx=i+1; entry_bar=one.iloc[entry_idx]
        if pd.Timestamp(entry_bar.datetime).date()!=day or not ("09:30"<=pd.Timestamp(entry_bar.datetime).strftime("%H:%M")<="11:40"):
            continue
        entry=float(entry_bar.open)
        recent=one.iloc[max(0,i-9):i+1]
        stop=float(recent.low.min()) if direction=="CE" else float(recent.high.max())
        risk=entry-stop if direction=="CE" else stop-entry
        if risk<=0: continue
        target=entry+2*risk if direction=="CE" else entry-2*risk
        last_idx=session_last.get(day,entry_idx); outcome="OPEN / data end"; exit_price=exit_time=None; exit_idx=None
        for j in range(entry_idx,last_idx+1):
            bar=one.iloc[j]; lo=float(bar.low); hi=float(bar.high)
            hit_sl=lo<=stop if direction=="CE" else hi>=stop
            hit_tp=hi>=target if direction=="CE" else lo<=target
            if hit_sl:
                outcome="STOP (conservative same-bar rule)" if hit_tp else "STOP"; exit_price=stop; exit_time=str(bar.datetime); exit_idx=j; break
            if hit_tp:
                outcome="TARGET"; exit_price=target; exit_time=str(bar.datetime); exit_idx=j; break
        if exit_idx is None and pd.Timestamp(one.datetime.iloc[last_idx]).strftime("%H:%M")>="15:20":
            lastbar=one.iloc[last_idx]; outcome="EOD_EXIT"; exit_price=float(lastbar.close); exit_time=str(lastbar.datetime); exit_idx=last_idx
        gross=None if exit_price is None else ((exit_price-entry) if direction=="CE" else (entry-exit_price))
        net=None if gross is None else gross-float(cost_points)
        traded_dates.add(day)
        signals.append({"time":str(entry_bar.datetime),"signal":"BUY CE" if direction=="CE" else "BUY PE",
           "entry":round(entry,2),"stop_loss":round(stop,2),"target":round(target,2),"outcome":outcome,
           "exit_price":None if exit_price is None else round(exit_price,2),"exit_time":exit_time,
           "gross_points":None if gross is None else round(gross,2),"cost_points":round(float(cost_points),2),
           "net_points":None if net is None else round(net,2),"reason":"SMC FVG/OB/IDM/BOS/MSS: "+"; ".join(stack.get("reasons",[])),
           "confluence_score":stack.get("score",0),"checks":stack.get("checks",{}),"risk_reward":"1:2",
           "strategy":"smc_pac","signal_bar_time":str(stamp)})
        if exit_idx is not None:
            # Do not overlap positions; later entries still must be in the morning window.
            pass
        if progress_callback and i%500==0:
            progress_callback(round((i+1)*100/total,1),i+1,total)
    if progress_callback: progress_callback(100,total,total)
    result=_summarise_replay(signals,cost_points,"smc_pac")
    result["summary"]["diagnostics"]=diagnostics
    result["summary"]["signal_window"]="09:30–11:40 IST"
    result["summary"]["timeframes"]="5m structure → 2m FVG/OB/IDM → 1m entry"
    return {"smc_pac":result}



def historical_replay(frames, max_trades=500, cost_points=2.0, strategy="smc_pac"):
    """Backward-compatible SMC-only replay entry point."""
    return historical_replay_all(frames, max_trades=max_trades, cost_points=cost_points)["smc_pac"]

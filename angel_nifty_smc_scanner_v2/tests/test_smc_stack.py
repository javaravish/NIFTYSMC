import pandas as pd
from scanner import build_timeframes, analyze_smc_stack

def test_stack_waits_for_insufficient_data():
    empty=pd.DataFrame(columns=["datetime","open","high","low","close","volume"])
    result=analyze_smc_stack({"1m":empty,"2m":empty,"5m":empty})
    assert result["signal"]=="WAIT"
    assert "reasons" in result

def test_stack_exposes_smc_concepts_without_15m():
    rows=[]
    for day in pd.date_range("2026-01-05",periods=2,freq="B"):
        start=pd.Timestamp(day.date()).replace(hour=9,minute=15); price=25000.0
        for i in range(375):
            stamp=start+pd.Timedelta(minutes=i); close=price+(0.8 if (i//7)%2==0 else -0.5)
            rows.append({"datetime":stamp,"open":price,"high":max(price,close)+1,"low":min(price,close)-1,"close":close,"volume":100})
            price=close
    frames=build_timeframes(pd.DataFrame(rows))
    result=analyze_smc_stack({k:v.tail(80) for k,v in frames.items()})
    assert result["signal"] in ("WAIT","CE","PE")
    assert set(result["checks"])=={"5m_BOS","5m_MSS","2m_FVG_detected","2m_FVG_retest","2m_order_block_detected","2m_order_block_retest","2m_IDM_sweep","1m_entry_confirmation","entry_window_0930_1140"}
    assert "diagnostics" in result

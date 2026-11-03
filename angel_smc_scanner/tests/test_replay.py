import unittest
import pandas as pd
from scanner import build_timeframes, _resample_session, historical_replay, historical_replay_all

class ReplayTests(unittest.TestCase):
    def make_day(self, day):
        rows=[]
        for n in range(375):
            stamp=pd.Timestamp(f"{day} 09:15")+pd.Timedelta(minutes=n)
            price=25000+n*0.03
            rows.append({"datetime":stamp,"open":price,"high":price+1,"low":price-1,"close":price+0.2,"volume":100})
        return pd.DataFrame(rows)

    def test_build_timeframes_has_no_15m(self):
        frames=build_timeframes(self.make_day("2026-10-28"))
        assert set(frames)=={"1m","2m","5m"}

    def test_replay_returns_cost_and_diagnostics(self):
        one=pd.concat([self.make_day("2026-10-28"),self.make_day("2026-10-29")],ignore_index=True)
        result=historical_replay(build_timeframes(one),cost_points=2.0)
        assert "net_points" in result["summary"]
        assert "ce" in result["summary"] and "pe" in result["summary"]
        assert result["summary"]["cost_points_per_trade"]==2.0
        assert result["summary"]["signal_window"]=="09:30–11:40 IST"
        assert "diagnostics" in result["summary"]

    def test_only_smc_strategy_is_returned(self):
        one=self.make_day("2026-10-28")
        result=historical_replay_all(build_timeframes(one))
        assert set(result)=={"smc_pac"}


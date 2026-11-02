# NIFTY SMC Research Lab v3.0

Paper/research application inspired by documented concepts in LuxAlgo SMC, ICT Concepts and Price Action Concepts. It is an independent implementation; it does not contain LuxAlgo proprietary source code and is not affiliated with LuxAlgo.

## Strategy architecture
- 15m: directional market-structure context from completed candles.
- 5m: liquidity sweep/reclaim plus displacement and structure confirmation.
- 2m: fair-value-gap retest, displacement and premium/discount location.
- 1m: close-confirmed entry trigger.
- Higher-timeframe bars become available only after their close.
- One open position at a time; same-session exit; conservative stop-first assumption if stop and target hit in one bar.
- Session filter 09:30–15:00 IST.
- Paper/research only. No order-placement route or auto-trading.

## Concepts and implementation scope
SMC reference: internal/swing structure, BOS/CHoCH, OB/FVG, equal highs/lows and premium/discount. ICT reference: MSS, displacement, liquidity sweeps and session windows. PAC reference: price-action structure, liquidity grabs, inverse-FVG/imbalance and multi-timeframe levels. The current rule engine implements a measurable subset: structure bias, sweep/reclaim, displacement, FVG retest, premium/discount and 1m confirmation. Volumetric OB metrics, equal-high/low clustering, inverse FVG, breakers, killzone subwindows and Fibonacci OTE are not yet implemented as executable filters; they remain documented research backlog, not falsely advertised as complete.

## Run on Windows
1. Extract this ZIP into a new folder.
2. Open `angel_nifty_smc_scanner_v2`.
3. Copy `.env.example` to `.env` and fill Angel One settings locally. Never send `.env`, API key, PIN, TOTP, JWT or feed token to anyone.
4. `python -m pip install -r requirements.txt`
5. `python -m uvicorn api:app --host 127.0.0.1 --port 8000`
6. Open `index.html` in Chrome.
7. Run Historical Trade Report with 250 sessions.

## Backtest integrity
This is NIFTY spot-point research, not CE/PE option-premium P&L. Configured costs are spot-point assumptions, not broker charges. IV, Greeks, bid/ask, expiry, option liquidity, brokerage/taxes and realistic option fills are not modeled. Require out-of-sample walk-forward results and paper-forward testing. Do not tune repeatedly on the same test period.

## Performance statistics
Reports trade count, win rate, profit factor, average net points, net points, CE/PE split, stop/target/EOD outcomes and confluence checks. Fewer trades and no-trade days are expected. A 70% win rate is a research target, not guaranteed or currently validated. Evaluate expectancy and drawdown as well as win rate.

## API
- `POST /api/history/start?sessions=250&cost_points=2`
- `GET /api/history/status/{job_id}`
- `GET /api/history/result/{job_id}`
- `GET /api/scan?days=5`
- `GET /api/live` (when connected)
- `GET /docs`

## References
- LuxAlgo Smart Money Concepts: https://www.luxalgo.com/library/indicator/smart-money-concepts-smc/
- LuxAlgo ICT Concepts: https://www.luxalgo.com/library/indicator/ict-concepts/
- LuxAlgo Price Action Concepts: https://www.luxalgo.com/library/indicator/luxalgo-price-action-concepts/
- LuxAlgo PAC documentation: https://docs.luxalgo.com/platform/algos/price-action-concepts/introduction
- Independent SMC/ICT research (S&P 500 daily data; not NIFTY intraday evidence): https://github.com/harshatangirala/smc-ict-research

# NIFTY SMC Research Lab v3.13

Paper/research application inspired by public SMC, ICT and price-action concepts. It is an independent implementation; it does not contain proprietary LuxAlgo source code and is not affiliated with LuxAlgo.

## Current strategy architecture
- 5m: close-confirmed structure context and BOS/MSS proxy.
- 2m: FVG / order-block / IDM context and retest.
- 1m: close-confirmed entry trigger.
- No 15m calculation, Gainz-inspired strategy or Combined agreement mode is used in the current SMC-only application.
- Entry window: 09:30–11:50 IST; one trade maximum per session in historical replay.
- Higher-timeframe bars become available only after their close.
- Historical replay uses completed candles, next-bar entry, stop-first same-bar handling, and end-of-day exits.
- Paper/research only. The dashboard auto mode is paper-only; no broker order-placement route is implemented.

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


## v3.1 PAC additions and faster repeat backtests

- Added active PAC confluence checks: volume-aware order-block quality (with a candle-body proxy when broker volume is unavailable), breaker-block invalidation/retest, inverse-FVG invalidation/retest, and Fibonacci 61.8%-78.6% OTE location. These are transparent rule-based approximations, not proprietary LuxAlgo calculations or order-flow delta.
- The four PAC checks are included in the 12-point confluence score. A signal requires the core 15m bias, 5m sweep/MSS, 1m confirmation and session window plus at least 10/12 total checks. This is intentionally selective and may result in zero trades on some days.
- Historical data is cached locally under `~/.nifty_smc_research_lab/historical_cache` (on Windows: `%USERPROFILE%\.nifty_smc_research_lab\historical_cache`). Fixed 25-calendar-day cache windows now use stable epoch-aligned boundaries. Completed chunks are reused on subsequent runs; only the active/current chunk is fetched again so fresh candles can be included.
- Cache status: `GET /api/history/cache`. Clear cache only if needed: `POST /api/history/cache/clear`. Do not clear it routinely. Keep the `~/.nifty_smc_research_lab/historical_cache` (on Windows: `%USERPROFILE%\.nifty_smc_research_lab\historical_cache`) folder when updating the application.
- First run still needs to download the requested history. Later runs should be materially faster, subject to Angel One/API limits and the current chunk.
- This remains NIFTY spot research, not options-premium P&L. The 70% target is not guaranteed; evaluate out-of-sample results and realistic option costs before any live use.


## v3.3 Gainz-inspired experimental strategy (not original GainzAlgo V2)

This version adds a separate, transparent trend/pullback strategy inspired by publicly described indicator features. It is **not** the proprietary GainzAlgo V2 formula, and no original source code or exact calculation rules are claimed.

- 15m: EMA 9/21/55 directional trend.
- 5m: EMA alignment plus ADX >= 18 trend-strength filter.
- 2m: pullback/reclaim around EMA 21.
- 1m: close-confirmed local breakout, RSI threshold and session VWAP alignment.
- Session: 09:30–15:00 IST; only completed higher-timeframe bars are available in replay.
- Risk model: 1.2 × 1m ATR stop and 1.8R target; stop-first assumption if stop and target are both touched in one candle.
- Historical report runs SMC/PAC and Gainz-inspired variants on the same locally cached candles and shows a side-by-side comparison.
- `/api/scan` returns both `analysis` and `gainz_inspired_analysis`; `/api/live` exposes `gainz_inspired_signal` and its checks.

The Gainz-inspired variant is experimental. Do not infer profitability from indicator marketing claims. The report is still NIFTY spot-point research, not options-premium P&L. Paper-forward test and out-of-sample validation are required.


## v3.4 shared replay performance improvements

- The three independent strategy books (SMC/PAC, Gainz-inspired, Combined) are evaluated in one chronological candle pass. The SMC and Gainz signal engines are each evaluated once per eligible minute and their results are shared with the three books.
- Replay progress is now reported separately from download progress. During replay the percentage advances through the one-minute candles instead of remaining at 100% from the download stage.
- Completed report results are cached by a SHA-256 fingerprint of the normalized selected OHLCV dataset and cost assumption. Re-running the exact same dataset and costs loads the cached report. If newly fetched candles change the dataset, a new report is calculated.
- Result cache lives at `~/.nifty_smc_research_lab/backtest_results`; historical candle cache remains at `~/.nifty_smc_research_lab/historical_cache`. Both survive extraction of a newer application ZIP.
- The first run still must download missing history and calculate the report. The speed improvement has passed unit/smoke tests but has not yet been benchmarked against the user's full 250-session Angel One dataset.
- This is a computational optimization, not a change to the proprietary GainzAlgo formula. Gainz-inspired rules remain experimental.


## v3.5 SMC morning-session strategy

Gainz-inspired and combined strategy modes have been removed. 15-minute candle generation is removed. The strategy now uses 5m BOS/MSS context, 2m FVG and order-block zones, IDM sweep/reclaim, and 1m entry confirmation. New entry signals are accepted only from 09:30 through 11:50 IST. The report includes per-condition diagnostics. Existing historical candle cache remains in `%USERPROFILE%\.nifty_smc_research_lab\historical_cache`; report cache uses a new strategy version key.


### Live candle engine (v3.13)

The live chart and SMC scan now read a persistent local 1m candle buffer. WebSocket ticks form each 1m OHLC candle; only completed candles enter analysis. The API locally aggregates 2m and 5m bars. `/api/candles` and `/api/scan` no longer call Angel One historical candles. The historical endpoint is only used for one guarded warm-up when existing local/backtest cache is insufficient. Rate-limit errors trigger a five-minute cooldown; do not repeatedly click connect/scan to retry.

Persistent live candles are stored at `%USERPROFILE%\.nifty_smc_research_lab\live_1m_candles.csv`. Existing newest backtest cache chunks are also reused automatically. WebSocket reconnection uses exponential backoff.

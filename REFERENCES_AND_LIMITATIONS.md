# Research references and implementation notes

This application is an independent, rule-based research implementation. It does not reproduce proprietary LuxAlgo code and is not affiliated with LuxAlgo.

## Public concept references
1. LuxAlgo Smart Money Concepts: https://www.luxalgo.com/library/indicator/smart-money-concepts-smc/
   Describes internal/swing structure, BOS/CHoCH, order blocks, equal highs/lows, FVGs, MTF highs/lows and premium/discount.
2. LuxAlgo ICT Concepts: https://www.luxalgo.com/library/indicator/ict-concepts/
   Describes MSS/BOS, order blocks/breakers, FVG and other imbalances, buyside/sellside liquidity, displacement, killzones and opening gaps.
3. LuxAlgo Price Action Concepts: https://www.luxalgo.com/library/indicator/luxalgo-price-action-concepts/
   Describes structure, volumetric order blocks, liquidity grabs, FVG/inverse FVG, premium/discount, previous highs/lows and Fibonacci features.
4. LuxAlgo PAC documentation: https://docs.luxalgo.com/platform/algos/price-action-concepts/introduction
5. Independent SMC/ICT detector research: https://github.com/harshatangirala/smc-ict-research
   Its stated sample is S&P 500 constituents on daily OHLCV, not NIFTY intraday. Its conclusions must not be generalized directly to this application.

## Implemented in v3.0
- Completed-bar 15m directional context.
- 5m liquidity sweep/reclaim, displacement and close-confirmed break filter.
- 2m three-candle FVG candidate, retest/location and displacement filters.
- 1m close-confirmed micro-structure trigger.
- 09:30–15:00 IST entry window and at most one entry per trading date.
- No-lookahead higher-timeframe availability, single open position, conservative same-bar stop-first replay and same-session exits.

## Not yet implemented as executable rules
Volumetric OB buy/sell metrics, full breaker-block lifecycle, robust EQH/EQL clustering, inverse-FVG lifecycle, session-specific killzones, opening gaps, automatic chart patterns, and Fibonacci OTE. These are not claimed as active filters in v3.0.

## Important validation limits
The supplied 250-session replay data has not been run here because the user's Angel One credentials/data connection are local and private. The application must be run on the user's machine to obtain actual results. Even then, the current backtest uses NIFTY spot points and an assumed spot-point cost; it is not options premium P&L and does not model options Greeks, IV, bid/ask spread, slippage, expiry, option liquidity, brokerage and taxes. A 70% win rate is not established. Use walk-forward/out-of-sample and paper-forward tests.

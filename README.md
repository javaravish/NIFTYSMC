# NIFTY SMC Research Lab v3.16

Current strategy: SMC-only morning setup, using 5m structure (BOS/MSS) → 2m FVG / order-block / IDM context → 1m entry confirmation. Entry signals are restricted to 09:30–11:40 IST. Gainz-inspired and Combined modes have been removed. No 15m calculation is used by the current strategy.

This release updates the live data architecture: historical candles are reused from the persistent cache; WebSocket LTP ticks build completed 1m OHLC candles locally; 2m and 5m bars are aggregated locally; live chart and scan endpoints do not call Angel One historical data. A guarded warm-up request is attempted only when local cache is insufficient. The archive excludes `.env`, logs, Git history and Python bytecode. Copy `.env.example` to `.env` locally; never share credentials.


## v3.5 — SMC morning-session edition

- Removed Gainz-inspired and combined strategy outputs from the dashboard and API.
- Removed 15-minute candle aggregation and all 15m strategy dependencies.
- Strategy sequence: 5m structure (BOS/MSS) → 2m FVG / order-block / IDM context and retest → 1m entry confirmation.
- Signal generation is restricted to 09:30–11:40 exchange-local time (IST for NSE).
- One trade maximum per session remains enabled.
- Report diagnostics count bars in the window and detected BOS, MSS, FVG, FVG retest, order blocks, OB retest, IDM, 1m trigger and qualified entries.
- Historical OHLCV cache is retained. Report-cache key changed to `smc_morning_v2` so old Gainz/combined reports are not reused.
- IDM is implemented as a transparent internal-liquidity sweep/reclaim proxy; BOS/MSS are rule-based structural approximations, not an exact reproduction of any proprietary indicator.
- All tests pass. No live orders are placed.


## v3.6 dashboard update

- Dashboard sections reordered: Live NIFTY feed, separate Multi-timeframe SMC analysis, Price action & SMC map, Historical Trade Report, Research Engine, SMC concept coverage, Connection setup, and local Python scanner connection.
- Standardized section heading font family, size and weight.
- Live dashboard now displays an Entry reference, Stop-loss, Target 1 (1R), and Target 2 (2R) when the local API returns a valid SMC signal.
- Trade plan is based on completed 1-minute signal candle close and recent structure/order-block stop reference. These are NIFTY spot levels, not options premium prices or guaranteed fills. Manual verification only; no orders are placed.
- Live analysis excludes the currently forming 1-minute candle.
- Added EOD_EXIT to the historical report outcome filter.


## v3.7 dashboard usability update

- Added numbered section headings (1–8) and click-to-collapse behavior; clicking the heading text or +/- control toggles each section.
- Standardized dashboard section heading typography to 16px bold.
- Multi-timeframe SMC values are displayed in a single horizontal row with horizontal scrolling on narrow screens.
- Historical trade table shows at least ten rows in its own scroll area before scrolling.
- Trade details and price-level diagram are hidden until a trade is selected. Clicking the same selected trade closes the detail panel; the detail heading also toggles visibility.
- Existing API, strategy logic, persistent candle cache, and paper-only/manual-execution safety remain unchanged.


## v3.8 requested layout and controls

- Price action & SMC map and Signal monitor remain side-by-side in the same responsive section.
- Removed the duplicate inner Historical Trade Report title; the numbered outer section is the single title.
- Historical report iframe now sizes itself to its content to avoid excess blank space after the method note. Its internal trade table retains a 390px scroll area, enough for at least 10 rows before scrolling.
- Added section 3 Auto trading controls after Multi-timeframe SMC analysis. The Auto trading switch is visibly OFF and locked/disabled; it cannot place orders. Quantity can be set for future design/testing. Refresh status checks the read-only live endpoint.
- The current backend has no open-position endpoint, so the Open positions panel explicitly reports that broker positions are unavailable rather than inventing position data.
- Added a Connect to API button in section 9. It sends the same local HTTP POST request as `curl.exe -X POST http://127.0.0.1:8000/api/connect`, using the API URL field in the dashboard.
- All numbered section headings remain click-to-collapse with consistent 16px typography.


## v3.9 — Layout corrections and paper auto mode

- Auto controls are compact: Auto ON/OFF, quantity and refresh arranged in a single row on desktop.
- API retry button is placed inline after Test Python connection, matching the existing primary button styling.
- Price action chart and Signal monitor now align side-by-side; the section heading spans the full row.
- Auto mode is now a functional PAPER-ONLY toggle backed by `/api/paper-trading` GET/POST endpoints. Quantity is configurable from 1 to 100. The paper engine opens at most one NIFTY spot-reference paper position per trading date on a fresh confirmed signal, tracks stop/2R target and EOD exit, and displays paper open-position rows.
- This does not place Angel One orders, does not trade options, and does not calculate option-premium P/L. "Quantity (lots)" is a paper sizing field; displayed P/L is NIFTY spot points multiplied by the selected quantity, not rupees.
- The existing backend has no live broker positions endpoint and no live order-routing workflow. Real-money auto trading is therefore not implemented or enabled.
- Keep paper mode OFF until validated. Paper state is in memory and resets when the API process restarts.


## v3.10 — Historical report and unified SMC panel

- Added Download CSV to the Historical Trade Report. It exports the currently filtered trade rows and includes entry, stop, target, exit, P/L and available explanation/check fields.
- Increased the trade table's internal scroll viewport to 680 CSS pixels so at least ten rows can be visible before scrolling, including at the compact display scale shown in the user's screenshot.
- Adjusted the report iframe sizing to follow its content and expanded the auto-fit ceiling to avoid a large trailing blank region.
- Wrapped the Price action & SMC map and Signal monitor cards in one bordered parent section; the numbered heading now sits inside the same outer container.
- No strategy, broker, or order-execution logic changed from v3.9.


## v3.11 — Dashboard JavaScript null-reference fix

- Removed an obsolete legacy historical-report event handler that still referenced removed page elements (`load-history`, `history-status`, `history-results`, and related nodes). Historical reporting remains in the dedicated report iframe.
- The live refresh now uses the visible Python connection status as a fallback when the old `feed-health` element is absent.
- The error path checks that the status/detail elements exist before updating text.
- No backend, scanner, Angel One authentication, or trading logic changed.


## v3.12 — Historical table sizing and SMC chart overlays

- Historical trade table is hidden when no trades are present, avoiding an empty 10-row blank region.
- When 1–10 trades are present, all rows display without an internal scrollbar. When more than 10 trades are present, the table viewport is limited to approximately 10 visible rows and scrolls vertically.
- Removed fixed 680px minimum table height and reduced the iframe initial height; iframe now fits the report content so no large trailing gap remains after the method note.
- Replaced the static illustrative chart with real Angel One OHLC candles from `/api/candles`.
- Added chart annotations: three-candle FVG, displacement/opposite-candle order-block proxy, 10-bar BOS/MSS structure breaks, and 12-bar sweep/reclaim IDM proxy.
- Mouse wheel over the chart zooms in/out; the old zoom buttons are removed. Timeframe controls remain.
- SMC annotations are locally calculated heuristics on chart candles and are not claimed to replicate proprietary LuxAlgo/TradingView signals.
- No backend strategy, order, or paper-trading logic changed.


## v3.13 — Local live candle engine and rate-limit protection

This version changes the live data path to prevent chart refreshes and manual scans from repeatedly calling Angel One historical candles.

- `POST /api/connect` authenticates once, loads the persistent live cache and the newest two existing backtest cache chunks, and starts the WebSocket. If the local cache already contains at least 1,000 candles with a latest candle within seven days, the extra historical warm-up call is skipped. Otherwise, one guarded five-day warm-up request is attempted.
- Historical requests in `AngelClient` are serialized, spaced by at least three seconds, and put into a five-minute cooldown after a rate-limit/access-denied response. This is defensive throttling, not a claim about Angel One's published rate quota.
- Completed 1m candles are built locally from WebSocket LTP ticks. The currently forming minute is excluded from SMC analysis. Completed candles are persisted to `%USERPROFILE%\.nifty_smc_research_lab\live_1m_candles.csv`.
- The local candle cache loader also reuses the newest two `nifty_1m_*.csv` historical chunks created by the backtester. This provides SMC warm-up data without a fresh broker request when those files exist.
- `/api/candles` serves local cache candles and aggregates 2m/5m locally. The chart can poll this endpoint without consuming Angel One historical API requests.
- `/api/scan` now analyzes the same local completed-candle buffer and never calls the historical API. `/api/live` continues to read the same buffer.
- WebSocket reconnect uses bounded backoff (2, 4, 8, 16, then 30 seconds) after a disconnect.
- The SMC analyzer now waits for at least 11 completed 5m candles before evaluating its prior-10-bar BOS check, plus 14 completed 2m and 4 completed 1m candles.
- The dashboard scan output shows the actual aggregate checks/reasons rather than expecting per-timeframe objects that the endpoint does not return.

Limitations: LTP-mode ticks do not provide reliable traded volume, so locally formed candle volume is zero. If the WebSocket is unavailable and no cached history exists, the system must wait for new completed 1m candles and sufficient 2m/5m bars; it must show WAIT rather than fabricate candles. A rate-limit rejection during the one-time warm-up is reported and does not trigger repeated retries.

Paper auto mode remains paper-only. No broker order placement was added.


## v3.16 — Live dashboard synchronisation and paper-position management

- `/api/live` now returns dashboard-ready 5m, 2m and 1m objects with the latest completed-candle timestamp and findings.
- `/api/scan` returns the same timeframe-card structure so the scanner and dashboard use one consistent analysis result.
- Paper-position status refreshes mark-to-market from the latest fresh WebSocket quote; stale quotes are not used.
- Paper positions separately track stop-loss, Target 1 and Target 2 status. Target 1 is recorded as hit while the paper position remains open; Target 2 or stop-loss closes it.
- The existing one-open-position and one-entry-per-session guards remain in place. A new entry is blocked while a paper position is open.
- Dashboard cards display each timeframe's latest completed candle time. Paper position table shows stop, T1 and T2 monitoring separately.

Important: paper positions are in-memory in this version and will reset when the Python API process restarts. Record/close any open paper position before replacing the running API. No Angel One orders are sent. This is still a paper/research system and has not been validated for live trading.


## v3.16 dashboard refresh and signal monitor fixes
- Added a manual **Refresh analysis** button to section 2.
- Live quote, timeframe cards, trade plan and signal monitor refresh automatically every 2 seconds from `/api/live`; the manual button requests an immediate refresh.
- Signal monitor now recognizes backend `CE` / `PE` signal values and only marks a setup confirmed when a fresh quote and trade plan are present.
- Improved timeframe card spacing, wrapping and completed-candle timestamps.
- No change to SMC entry rules, historical cache, or broker order behavior.
- The UI refresh does not force a new candle: completed-candle analysis changes when a candle closes.


## v3.16 daily signal journal
- Section 2 refreshes every 2 seconds and retains the manual Refresh analysis button.
- Section 2A, Today's Signal History, records completed 1-minute scans and lists confirmed CE/PE signals with timestamps, entry, stop-loss and targets.
- `GET /api/signals/today` reconstructs today's scan history from local cached candles where available; it does not call Angel One historical API.
- Signal journal files are stored under `%USERPROFILE%\\.nifty_smc_research_lab\\live_signal_history`.
- Signal monitor uses WAITING labels and shows the missing-condition reasons when there is no confirmed setup.
- Corrected misplaced v3.15 CSS so it is rendered as styling, not raw text at the top of the page.


## v3.17 paper-position display fix
- The top Entry/SL/T1/T2 panel displays the active paper position's original entry and risk levels while a paper position is open, even if the latest scanner result returns to WAIT.
- CE/PE signal monitor shows OPEN · PAPER for the side of the active paper position until the backend closes it.
- Current scanner setup and open paper position are separate states. No broker orders are submitted.

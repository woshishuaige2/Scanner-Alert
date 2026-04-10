# Project Context

## Maintenance Rule

- `context.md` must be kept in sync with the live codebase.
- Any meaningful implementation change, workflow adjustment, strategy rule change, config change, or new trading/scanner behavior should also update `context.md` in the same work session.
- When helping on this repo, treat updating `context.md` as part of completing the task whenever the project behavior or mental model has changed.

This repo has 3 related but distinct workflows. They share Stage 1 scanner ideas, but Stage 2 / Stage 3 behavior is different depending on the file.

## 1) `realtime_multi_session_scanner.py`

Purpose:
- standalone scanner
- no trading
- used to monitor top gainers in real time and emit scanner alerts

What it does:
- loads top gainers from IBKR
- subscribes to live market data
- tracks price, VWAP, volume, session state, day gain %, news, float, avg daily volume
- applies scanner logic based on:
  - spread filter
  - price above VWAP
  - fast ignition / momentum behavior
- calculates alert score + grade
- shows terminal table with:
  - symbol
  - price
  - VWAP
  - GAIN%
  - volume
  - relative volume
  - alert grade
  - triggered conditions

Important notes:
- this is the reference scanner logic
- `run_realtime_trading.py` and `run_clean_momentum_sniper.py` should stay aligned with this Stage 1 scanner behavior
- premarket relative volume was fixed so premarket numerator uses full cumulative session volume from first observed premarket tick, not just post-start deltas
- alert score audit logs are written here and reused by the trading runners
- the Fast Ignition preliminary trigger now allows up to 1.0% retracement from the local high, loosened from 0.7% to better admit strong high-volume squeezes that pause slightly
- scanner scoring now includes a conservative multi-timeframe structure-damage penalty:
  - recent 15s / 30s / 1m drawdowns are cross-analyzed into one result
  - penalty output is `0`, `-1`, or `-2`
  - this subtracts from score only and does not hard-cap grades by itself
  - designed to make repeated squeeze alerts on damaged names more cautious without fully blocking repaired setups

Current concept:
- this file defines the shared scanner worldview
- if scanner logic changes, the trading runners should usually inherit the same Stage 1 logic

## 2) `run_realtime_trading.py`

Purpose:
- original in-process trading bot
- uses the shared scanner as Stage 1
- trades only after an alert is received and then a second entry filter passes

Workflow:
- Stage 1:
  - runs the same scanner logic from `realtime_multi_session_scanner.py`
- Stage 2:
  - queues qualified scanner alerts
  - current model is confirmation-heavy
  - after alert, it waits for a future entry setup rather than entering immediately
  - entry mode is:
    - continuation breakout for stronger alerts
    - base breakout for lower but still qualified alerts
- Stage 3:
  - sends order to `ExecutionEngine`
  - shared position management handles partial profit, stop updates, time exit, etc.

Key entry behavior:
- regular-hours only
- minimum alert grade threshold
- alert is queued, then live price is monitored
- setup can expire for reasons like:
  - lost VWAP
  - stale timeout
  - faded too far from high

Recent changes:
- terminal Stage 1 view now matches standalone scanner style better:
  - fixed 2 decimal price display
  - added GAIN% column
- pullback invalidation was improved:
  - fade threshold is adaptive based on extension
  - fade breach must persist briefly before canceling

Current concept:
- this bot is more conservative and confirmation-based
- it often feels delayed because it requires scanner confirmation first, then execution confirmation
- keep this path intact as the original breakout-style bot

## 3) `run_clean_momentum_sniper.py`

Purpose:
- separate experimental trading runner
- independent from the original trading bot
- still uses the same scanner universe / Stage 1 concepts
- designed for “clean momentum sniper” entries

High-level idea:
- Stage 1 scanner finds candidate symbols
- Stage 2 is not breakout-confirmation logic
- Stage 2 tries to identify an extremely clean higher-timeframe momentum phase
- entry can now be either:
  - a sudden flush inside strength
  - or an extreme-clean continuation entry with tighter stop constraints

Workflow:
- Stage 1:
  - uses shared scanner alerts from `realtime_multi_session_scanner.py`
  - only qualifying scanner alerts get queued into sniper Stage 2
- Stage 2:
  - analyzes clean trend on 15s + 30s structure, with an alternate 1m clean-squeeze path now enabled
  - 5s is only for sudden-flush trigger timing, not trend definition
  - evaluates:
    - impulse-anchored clean window
    - retracement vs impulse
    - red candle count on the selected structure path
    - volume expansion
    - short-term volume acceleration
    - support / EMA context
    - total fade from peak vs the allowed extension-based cap
    - sudden short-window flush size
    - stop validity
  - chooses the cleanest currently valid Stage 2 mode between:
    - `15s/30s` clean-trend mode
    - `1m` clean-squeeze mode
  - primary timeframe can qualify the setup, but the other timeframe now acts as a contradiction veto if it shows obvious structural weakness
  - enters only if context is extremely clean and either:
    - a sudden short-window flush occurs inside the intact squeeze
    - or an extreme-clean continuation setup still has a tight enough structure stop
- Stage 3:
  - uses shared `ExecutionEngine`
  - but sniper provides custom structure-based stop at entry
  - sniper also has its own structure-break exit behavior
  - sniper now also has a fast-fail post-entry exit if the flush does not bounce quickly

Important sniper rules:
- regular hours only for v1 was the original intent, but the live config currently allows PREMARKET / REGULAR / AFTERHOURS
- clean trend is based on higher-timeframe structure, not 5s
- higher-timeframe structure can now come from either:
  - 15s + 30s clean-trend logic
  - 1m clean-squeeze logic
- entry trigger is now a sudden short-window flush, not a generic micro-pullback-low rule
- current flush trigger in config is:
  - lookback: 1.2 seconds
  - minimum sudden drop: 1.0%
- continuation entry is now also allowed for extreme-clean setups if the structure stop remains tight enough
- current continuation stop cap in config is:
  - maximum stop distance: 2.0%
- current pullback band in config is:
  - maximum fade from peak is tiered by extension:
  - base cap: 3.0%
  - at 5% extension: 4.0%
  - at 10% extension: 5.5%
- structure-based stops now use a hybrid buffer:
  - percent buffer below structure
  - plus a minimum absolute dollar floor so low-priced names do not lose the buffer after cent rounding
- after a flush entry, the bot expects a quick bounce:
  - if no bounce arrives within a few seconds, it exits
  - if price keeps flushing lower before bouncing enough, it exits
- leveraged / fund-style products are now filtered from the scanner universe by:
  - manual symbol blacklist
  - keyword match on fundamental text such as ETF / ETN / 2X / 3X / Bull / Bear / Daily Target / Long / Short / Ultra
- structure break means failure of the selected active structure mode, not isolated 5s noise
- structure break must persist briefly before counting as invalid
- continuation entries are intentionally more selective than flush entries because they are more exposed to sharp air-pocket reversals

Recent changes:
- sniper clean-context logic was changed from a blunt rolling window to an impulse-anchored window
- sniper can now accept clean 1m squeeze structure as an alternate Stage 2 path
- sniper total-fade tolerance is now tiered by extension instead of using only a flat max-fade cap
- sniper entry trigger was changed from a micro-pullback-low rule to a sudden flush trigger
- sniper now supports a second entry style for extreme-clean continuation setups when stop distance is no more than 2.0%
- sniper now uses cross-timeframe veto logic so one timeframe can qualify, but another timeframe can block the trade if it shows obvious structural contradiction
- sniper now exits early if the flush entry does not produce an immediate enough bounce
- leveraged products such as LUNL are now excluded earlier at the shared scanner-universe stage
- Stage 2 now shows the first failed rule in the terminal when setup is on hold
- recent sniper events now show timestamps
- structure-based stops are preserved correctly through fill handling
- symbols with active sniper positions are now retained in scanner management even if they later become excluded, to avoid live trades falling out of monitoring during blacklist/exclusion churn
- `extreme_clean` is now stricter and more parabolic-focused:
  - `clean_passed` still means the structure has no major red flags
  - `extreme_clean` now additionally requires strict parabolic progression confirmation
  - for the `15s/30s` path:
    - `30s` acts as the main parabolic frame
    - `15s` must confirm with clean advancing candles
    - `1m` must avoid a nearby red-flag confirmation failure
  - for the `1m` path:
    - `1m` acts as the main parabolic frame
    - `30s` must confirm with clean advancing candles
    - `15s` must avoid a nearby red-flag confirmation failure
  - the latest main-frame candle now has to show strong body expansion and strong volume expansion versus the prior candle, plus a minimum dollar-volume floor
- sniper size now doubles when an entry passes the stricter `extreme_clean` filter
- Stage 2 entry branching is now split more cleanly:
  - flush entries require `clean_passed` plus flush / pullback / stop validation
  - continuation entries still require the stricter parabolic `extreme_clean` gate
  - this keeps flush entries available for strong structurally clean setups even if they do not qualify as rare parabolic continuations
- sniper now has stronger operational position safety:
  - on startup, it queries live broker positions for the configured account and auto-flattens any non-zero positions before the bot continues
  - if startup happens while the market is closed, it waits for the next tradable session and then resumes the flatten process before proceeding
  - on `Ctrl+C`, the bot now immediately submits close-all exits for tracked positions and also attempts to flatten any remaining broker positions before shutdown
  - design intent is that no position should be intentionally left open when the sniper process is no longer running
- Stage 1 alert grading was adjusted again after reviewing `RAYA`-style higher-timeframe squeezes:
  - the `B` cutoff was moved back to `6`
  - missing float data no longer suppresses score quality:
    - scanner now treats `float is None` the same as `<20M` for the +1 float point
    - this is meant to avoid IBKR fundamental-data gaps unfairly lowering alert quality

## Shared `ExecutionEngine` context

Both trading runners use `execution_engine.py`.

What it does:
- submits entry order + broker-side disaster stop
- tracks active positions
- handles:
  - partial profit taking
  - stop updates
  - trailing logic
  - time exits
  - volume-fade exits

Important shared behavior:
- first target is currently +5%
- partial exit fraction is 50%
- after partial, stop can move to breakeven+buffer and then trail

Recent important engine changes:
- engine now supports custom stop on entry, used by sniper
- engine now supports `structure_exit` and `reopen_weak_exit`
- volume-fade exits are no longer one-shot immediate exits:
  - first qualifying fade now arms a warning
  - exit only occurs if fade still persists after a short confirmation window
  - warning clears if price reclaims strength
- halt / market-pause handling was added:
  - uses stale updates + quote anomalies + spread/frozen-price clues
  - freezes active-hold timer during confirmed pause
  - adds reopen buffer after trading resumes
  - classifies reopen as strong or weak
  - keeps a separate wall-clock max hold cap
- exit-side `Code 201` rejects are now treated differently from entry rejects:
  - entry rejects can still blacklist the symbol
  - exit rejects do not blacklist the symbol
  - exit rejects now clear pending-exit state and are logged as `exit_order_rejected`
- position cleanup now correctly removes all order mappings for a symbol from `order_to_symbol`
- full exits now cancel their own remaining protective orders before cleanup so old stops cannot leak into future re-entries
- stale protective-order contamination was fixed:
  - same-symbol re-entries should no longer be closed by an old broker stop from a prior trade
- initial long stop handling now validates the stop against the actual fill price:
  - if the actual fill comes in below the precomputed stop, the initial stop is adjusted below the real fill
  - this prevents instant self-stopouts caused by stop inversion after slippage

## Recent live-session findings

Important issues observed during the `2026-04-09` clean sniper session:

- `SKYQ` at `09:41` was a real clean continuation setup, but it stopped out instantly because the pre-fill stop (`8.10`) sat above the actual fill (`8.09`)
- multiple zero-second / near-zero-second losses were caused by stale old stops hitting new re-entries:
  - `ONCO` at `10:42`
  - `ONCO` at `11:04`
  - `BBGI` at `14:38`
- `ELAB` and later `ONCO` exposed a separate operational problem where IBKR rejected sell exits with `Code 201` (`The contract is not available for short sale`), leaving positions open when they should have been closed

Current interpretation:

- some disappointing trades were genuine strategy losses
- but several of the most suspicious instant losses were mechanical workflow bugs, not bad setup quality
- the biggest remaining unresolved risk is broker-side sell rejection on live long positions

## Daily performance notes

- daily markdown summaries are now kept locally under:
  - `runtime_feedback/clean_momentum_sniper/performance_summaries/`
- a reusable template now exists there:
  - `_daily_summary_template.md`
- note:
  - `runtime_feedback/` is git-ignored, so these summaries are currently local workspace notes and are not pushed to GitHub unless that ignore rule is intentionally overridden

## Latest pushed code state

- latest pushed commit during this workstream:
  - `2ebe4dc` - `Fix trade lifecycle and stop handling`

## Design intent

Use these mental models:

- `realtime_multi_session_scanner.py`
  - source of truth for scanner behavior
  - observe and alert

- `run_realtime_trading.py`
  - original conservative trading path
  - alert first, then future breakout/base confirmation

- `run_clean_momentum_sniper.py`
  - newer experimental trading path
  - alert first, then higher-timeframe clean-structure entry inside an active impulse
  - can currently qualify from either 15s/30s structure or a 1m clean squeeze

## Current pain points / tuning themes

- original trading bot can feel delayed because it double-confirms moves
- sniper is closer to desired style, but still needs tuning on:
  - clean trend detection
  - continuation-entry guardrails
  - volume interpretation
  - impulse window detection
  - post-halt behavior
- standalone scanner still needs tuning on:
  - repeated same-symbol squeeze / recycle alerts after structural damage
  - alert selectivity when one name dominates the tape for long stretches

## 2026-04-09 scanner review + decisions

Today we reviewed recent alert-history runs and observed:

- the scanner can over-fire on the same symbol for long periods when a squeeze keeps retriggering
- many emitted audit entries are still `C` or `Below Threshold`, which makes review noisy
- some names can still earn strong scores after showing ugly intraday damage because momentum factors can outweigh structural weakness

Implemented today:

- Stage 1 scanner now applies a conservative combined structure-damage score penalty:
  - cross-analyzes recent `15s`, `30s`, and `1m` drawdowns
  - outputs `0`, `-1`, or `-2`
  - subtracts from score only
  - does not hard-cap letter grade or hard-block alerts by itself
- audit output now includes structure-damage debug so recent drawdown damage is explainable during alert review

Deferred optimization ideas discussed today:

- intraday memory layer:
  - idea is to maintain small per-symbol memory such as intraday high tests, clean breaks, failed breakouts, rejections, and time near high
  - intended use is a lightweight score adjustment to reward clean persistence and penalize repeated failed breakouts
  - decision: defer implementation until after validating the new structure-damage penalty in live sessions, so effects do not get mixed together
- low-risk refactor / modularization:
  - goal is cleaner internal layering without changing the daily user-facing entrypoints
  - keep these top-level workflows stable:
    - `realtime_multi_session_scanner.py`
    - `run_clean_momentum_sniper.py`
    - `summarize_alert_run.py`
    - `alert_history/`
  - safe direction later is to reorganize internals underneath those files rather than renaming the main files the user runs every day
  - decision: defer until after current scanner behavior is revalidated, to avoid stacking structural change on top of scoring change

## If starting a new Codex chat

Use this summary:

I have 3 workflows in this repo:

1. `realtime_multi_session_scanner.py`
- standalone scanner, no trading
- this is the shared Stage 1 scanner reference
- tracks top gainers, VWAP, gain %, volume, rel vol, and scanner alerts

2. `run_realtime_trading.py`
- original trading bot
- uses scanner alerts, then waits for a second breakout/base entry confirmation
- more conservative, often delayed by design

3. `run_clean_momentum_sniper.py`
- separate experimental sniper bot
- uses the same scanner universe, but Stage 2 is higher-timeframe clean-structure logic with either a sudden-flush entry or an extreme-clean continuation entry
- trend quality is based on either 15s + 30s or 1m structure, with 5s only for trigger timing
- uses structure-based stops and structure-break exits

Important shared context:
- scanner logic should stay aligned between standalone scanner and trading runners
- leveraged / fund-style products are now excluded at the shared Stage 1 universe level
- Stage 1 scanner now also applies a conservative score penalty for recent multi-timeframe structure damage instead of hard-blocking damaged symbols outright
- execution is handled by shared `execution_engine.py`
- execution engine now uses a warning/confirmation flow for volume-fade exits rather than an immediate one-shot exit
- halt / market-pause handling was recently added to execution engine

When helping me, keep the difference between these 3 workflows clear and do not merge their trading logic unless I explicitly ask.

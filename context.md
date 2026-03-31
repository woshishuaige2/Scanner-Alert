# Project Context

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
- entry is on controlled weakness inside strength, not on a later breakout

Workflow:
- Stage 1:
  - uses shared scanner alerts from `realtime_multi_session_scanner.py`
  - only qualifying scanner alerts get queued into sniper Stage 2
- Stage 2:
  - analyzes clean trend on 15s + 30s structure
  - 5s is only for pullback timing, not trend definition
  - evaluates:
    - impulse-anchored clean window
    - retracement vs impulse
    - red candle count on 15s / 30s
    - volume expansion
    - short-term volume acceleration
    - support / EMA context
    - pullback depth
    - stop validity
  - enters only if context is extremely clean
- Stage 3:
  - uses shared `ExecutionEngine`
  - but sniper provides custom structure-based stop at entry
  - sniper also has its own structure-break exit behavior

Important sniper rules:
- regular hours only for v1
- clean trend is based on 15s + 30s, not 5s
- entry is near 5s pullback low without waiting for reclaim confirmation
- structure break means 15s structure failure, not isolated 5s noise
- structure break must persist briefly before counting as invalid

Recent changes:
- sniper clean-context logic was changed from a blunt rolling window to an impulse-anchored window
- Stage 2 now shows the first failed rule in the terminal when setup is on hold
- recent sniper events now show timestamps
- structure-based stops are preserved correctly through fill handling

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
- halt / market-pause handling was added:
  - uses stale updates + quote anomalies + spread/frozen-price clues
  - freezes active-hold timer during confirmed pause
  - adds reopen buffer after trading resumes
  - classifies reopen as strong or weak
  - keeps a separate wall-clock max hold cap

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
  - alert first, then clean-trend pullback entry inside an active impulse

## Current pain points / tuning themes

- original trading bot can feel delayed because it double-confirms moves
- sniper is closer to desired style, but still needs tuning on:
  - clean trend detection
  - volume interpretation
  - impulse window detection
  - post-halt behavior

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
- uses the same scanner universe, but Stage 2 is clean-trend pullback logic
- trend quality is based on 15s + 30s, 5s only for timing
- uses structure-based stops and structure-break exits

Important shared context:
- scanner logic should stay aligned between standalone scanner and trading runners
- execution is handled by shared `execution_engine.py`
- halt / market-pause handling was recently added to execution engine

When helping me, keep the difference between these 3 workflows clear and do not merge their trading logic unless I explicitly ask.

"""
Ross Cameron-style Scanner Configuration
Togglable conditions for preliminary screening.
"""

# PRELIMINARY SCREENING TOGGLES
ENABLE_LOW_FLOAT_MED_RVOL = True
ENABLE_LOW_FLOAT_HIGH_RVOL_PRICE_20PLUS = True
ENABLE_LOW_FLOAT_VOLATILITY_HUNTER = False
ENABLE_MEDIUM_FLOAT_HIGH_RVOL_UNDER_20 = False
ENABLE_LOW_FLOAT_HIGH_RVOL = True
ENABLE_SQUEEZE_ALERT_10PCT_10MIN = True
ENABLE_SQUEEZE_ALERT_5PCT_5MIN = False

# THRESHOLDS
LOW_FLOAT_MAX = 20_000_000  # 20M shares
MED_FLOAT_MAX = 50_000_000  # 50M shares

MED_RVOL_MIN = 1.5          # 150% of daily average
HIGH_RVOL_MIN = 3.0         # 300% of daily average

# SQUEEZE ALERT CONDITIONS (Used by all scanners)
SQUEEZE_PCT_THRESHOLD = 5.0   # Percentage increase required
SQUEEZE_TIME_MINUTES = 5       # Time window in minutes

# VWAP BUFFER FILTER (Used by conditions.py)
VWAP_BUFFER_UNDER_1_PRICE_MAX = 1.0      # Price tier ceiling for the widest VWAP buffer
VWAP_BUFFER_UNDER_1_PCT = 5.0            # Require price to clear VWAP by 5% under $1
VWAP_BUFFER_UNDER_3_PRICE_MAX = 3.0      # Price tier ceiling for the medium VWAP buffer
VWAP_BUFFER_UNDER_3_PCT = 3.0            # Require price to clear VWAP by 3% from $1 to under $3
VWAP_BUFFER_OVER_3_PCT = 1.0             # Require price to clear VWAP by 1% at $3 and above
SPREAD_FILTER_UNDER_1_PRICE_MAX = 1.0    # Price tier ceiling for the widest spread allowance
SPREAD_FILTER_UNDER_1_MAX_PCT = 2.0      # Allow wider spreads under $1 for fast movers
SPREAD_FILTER_UNDER_3_PRICE_MAX = 3.0    # Price tier ceiling for the medium spread allowance
SPREAD_FILTER_UNDER_3_MAX_PCT = 1.0      # Allow moderate spreads from $1 to under $3
SPREAD_FILTER_OVER_3_MAX_PCT = 0.5       # Keep the tighter spread allowance at $3 and above

# ALERT RATING / QUALITY SCORING (Used by alert_rating.py and realtime_multi_session_scanner.py)
ALERT_MIN_SCORE_TO_NOTIFY = 3               # Suppress low-quality setups below C
ALERT_GRADE_A_MIN_SCORE = 9                 # Tightened A cutoff after validating cautious news scoring
ALERT_GRADE_B_MIN_SCORE = 6                 # Tightened B cutoff after validating cautious news scoring
ALERT_BREAKOUT_LOOKBACK_MINUTES = 10        # Rolling 1-minute candle lookback for local breakout scoring
ALERT_DRAWDOWN_LOOKBACK_MINUTES = 60        # Rolling 1-minute candle lookback
ALERT_DRAWDOWN_TOP_GREEN_CANDLE_COUNT = 3   # Average the strongest green bodies in the lookback
ALERT_DRAWDOWN_UPPER_BASE_PCT = 10.0        # Base threshold for the first drawdown quality point
ALERT_DRAWDOWN_LOWER_BASE_PCT = 5.0         # Base threshold for the second drawdown quality point
ALERT_DRAWDOWN_UPPER_VOL_MULTIPLIER = 0.70  # Multiplier applied to avg top green 1m body %
ALERT_DRAWDOWN_LOWER_VOL_MULTIPLIER = 0.35  # Multiplier applied to avg top green 1m body %
ALERT_MOMENTUM_MIN_SAMPLES_60S = 8          # Require dense recent tick coverage before scoring acceleration
ALERT_MOMENTUM_PRICE_MIN_PCT_5S = 0.8       # Minimum 5-second price move for multi-timeframe acceleration
ALERT_MOMENTUM_PRICE_MIN_PCT_15S = 1.5      # Minimum 15-second price move for multi-timeframe acceleration
ALERT_MOMENTUM_PRICE_MIN_PCT_30S = 2.3      # Minimum 30-second price move for multi-timeframe acceleration
ALERT_MOMENTUM_PRICE_MIN_PCT_60S = 3.2      # Minimum 60-second price move for multi-timeframe acceleration
ALERT_MOMENTUM_PRICE_RATE_5S_OVER_15S = 1.2   # 5s rate must exceed 15s rate by at least 20%
ALERT_MOMENTUM_PRICE_RATE_15S_OVER_30S = 1.05  # 15s rate must exceed 30s rate by at least 5%
ALERT_MOMENTUM_MIN_VOL_15S = 15000          # Minimum 15-second volume to avoid low-activity noise
ALERT_MOMENTUM_VOL_RATE_5S_OVER_15S = 1.2  # 5s volume rate must exceed 15s rate by at least 20%
ALERT_MOMENTUM_VOL_RATE_15S_OVER_30S = 1.05 # 15s volume rate must exceed 30s rate by at least 5%
MIN_AVG_DAILY_DOLLAR_VOLUME = 2_000_000    # Loose liquidity gate to avoid extremely thin symbols
NEWS_CATALYST_ENABLED = True                # Enable cautious headline-based news catalyst scoring
NEWS_CATALYST_MAX_HEADLINES = 10            # Limit IBKR headline fetch size per symbol
NEWS_CATALYST_POSITIVE_KEYWORDS = [
    "acquisition",
    "acquires",
    "approval",
    "authorizes",
    "award",
    "buyout",
    "collaboration",
    "commercial launch",
    "completed merger",
    "completes merger",
    "contract",
    "data",
    "definitive agreement",
    "earnings",
    "expands",
    "expansion",
    "fda",
    "guidance",
    "merger",
    "order",
    "orders",
    "partnership",
    "patent",
    "phase",
    "results",
    "strategic merger",
    "trial",
    "withdrawal of sec filing",
    "withdraws sec filing",
    "withdrawing sec filing",
]
NEWS_CATALYST_NEGATIVE_KEYWORDS = [
    "at-the-market",
    "atm",
    "bankruptcy",
    "chapter 11",
    "delisting",
    "dilution",
    "non-compliance",
    "offering",
    "private placement",
    "prospectus",
    "priced at-the-market",
    "registered direct",
    "resale",
    "shelf",
    "warrant",
    "warrants",
]
NEWS_CATALYST_IGNORE_KEYWORDS = [
    "after-market session",
    "conference",
    "fireside chat",
    "intraday session",
    "investor day",
    "market-moving news",
    "participation",
    "pre-market session",
    "presentation",
    "price target",
    "shares are trading higher",
    "stocks moving",
    "webcast",
    "why is",
]

# FAST IGNITION CONDITIONS (Used by realtime_multi_session_scanner.py)
FAST_IGNITION_PCT_5S = 0.8                # Minimum move over last 5 seconds
FAST_IGNITION_PCT_15S = 1.5             # Minimum move over last 15 seconds
FAST_IGNITION_VOLUME_MULTIPLIER = 2.0   # 5s burst volume vs trailing average
FAST_IGNITION_MAX_RETRACEMENT_PCT = 0.7 # Allowed pullback from local high

# DISCORD ALERT CONFIGURATION
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1474054384131248310/43pw1SxLPf2j9Rj1gRrh6SL3SZ9zuHhqn2QmRFZ-sZ7a_DnordIXbbHFKxyaGjYzoSAg"

# TWS CONNECTION CONFIGURATION
# 7496 for Live Trading, 7497 for Paper Trading
TWS_PORT = 7497

# IN-DEPTH FILTERING (Used in run_realtime_trading.py)
STRICT_MOMENTUM_REQUIRED = True
MIN_PRICE_SURGE_10S = 1.5   # 1.5% surge in 10s
MAX_DRAWDOWN_10S = 0.5      # 0.5% max drawdown
MIN_TREND_30S = 1.0         # 1.0% trend in 30s

# REAL-TIME TRADING ENTRY (Used in run_realtime_trading.py)
REALTIME_TRADE_MIN_ALERT_GRADE = "B"             # Only queue entries for B/A/A+ scanner alerts
REALTIME_BREAKOUT_MIN_ALERT_GRADE = "A"          # A and A+ can enter on continuation breakouts
REALTIME_MOMENTUM_OVERWRITE_MIN_SCORE = 8        # High-conviction alerts at or above this score can upgrade into momentum entry mode
REALTIME_TRADE_REGULAR_HOURS_ONLY = True         # Restrict new entries to the regular market session
REALTIME_ENTRY_BREAKOUT_BUFFER_PCT = 0.10        # Require price to clear the tracked high by a small buffer
REALTIME_ENTRY_MIN_EXTENSION_PCT = 0.20          # Require the setup to extend beyond the alert price before entry
REALTIME_ENTRY_BASE_LOOKBACK_SECONDS = 12        # Recent window used to detect a short base for grade-B setups
REALTIME_ENTRY_BASE_MIN_SECONDS = 5              # Minimum age of the setup before allowing a base-breakout entry
REALTIME_ENTRY_BASE_MAX_WIDTH_PCT = 0.80         # Base must stay relatively tight
REALTIME_ENTRY_BASE_MAX_DISTANCE_FROM_PEAK_PCT = 0.75  # Base must form close to the setup high
REALTIME_ENTRY_FAIL_BELOW_PEAK_PCT = 3.0         # Cancel setups that fade too far off the high
REALTIME_ENTRY_FAIL_BELOW_PEAK_PCT_AT_5_EXTENSION = 4.0   # Allow more fade once the setup has extended meaningfully
REALTIME_ENTRY_FAIL_BELOW_PEAK_PCT_AT_10_EXTENSION = 5.5  # Allow deeper pullbacks after very strong extensions
REALTIME_ENTRY_FAIL_BELOW_PEAK_PERSIST_SECONDS = 2.0      # Fade breach must persist before the setup is canceled
REALTIME_ENTRY_MAX_WAIT_SECONDS = 90             # Cancel stale setups that do not resolve quickly
REALTIME_TRADE_SYMBOL_COOLDOWN_SECONDS = 120     # Avoid immediate re-queue churn after an attempt

# CLEAN MOMENTUM SNIPER (Used in run_clean_momentum_sniper.py)
SNIPER_MIN_ALERT_GRADE = "B"                     # Use scanner alerts as the stage-1 universe gate
SNIPER_REGULAR_HOURS_ONLY = True                 # v1 only runs during regular hours
SNIPER_SYMBOL_COOLDOWN_SECONDS = 120             # Avoid immediate re-entry churn after an attempt
SNIPER_SETUP_MAX_WAIT_SECONDS = 90               # Drop stale sniper setups that stop progressing
SNIPER_15S_WINDOW_CANDLES = 8                    # Clean-phase evaluation window on 15s candles
SNIPER_30S_WINDOW_CANDLES = 5                    # Clean-phase evaluation window on 30s candles
SNIPER_MIN_IMPULSE_15S_CANDLES = 3               # Require a minimum 15s impulse length before judging cleanliness
SNIPER_MIN_IMPULSE_30S_CANDLES = 2               # Require at least some 30s confirmation for the active impulse
SNIPER_15S_EMA_PERIOD = 9                        # EMA context on 15s candles
SNIPER_15S_MAX_RED_FRACTION = 0.25               # 15s red-candle allowance inside the clean phase
SNIPER_30S_MAX_RED_CANDLES = 1                   # 30s red-candle allowance inside the clean phase
SNIPER_CLEAN_RETRACE_PREFERRED_PCT = 30.0        # Preferred max retracement vs prior impulse
SNIPER_CLEAN_RETRACE_HARD_PCT = 40.0             # Hard cutoff for clean-trend invalidation
SNIPER_MIN_15S_VOLUME_EXPANSION = 1.5            # Current 15s volume vs rolling 15s average
SNIPER_PULLBACK_MIN_FROM_PEAK_PCT = 0.4          # Ignore ultra-shallow pullbacks that are still extended
SNIPER_PULLBACK_MAX_FROM_PEAK_PCT = 3.0          # Pullback must stay controlled inside the trend
SNIPER_PULLBACK_NEAR_LOW_BUFFER_PCT = 0.20       # Entry must stay near the current 5s pullback low
SNIPER_STOP_BUFFER_PCT = 0.20                    # Small noise buffer below the chosen structure stop
SNIPER_MIN_STOP_DISTANCE_PCT = 0.35              # Avoid stops that sit unrealistically close to entry
SNIPER_MAX_STOP_DISTANCE_PCT = 3.0               # Skip setups whose valid structure stop is too far away
SNIPER_STRUCTURE_BREAK_PERSIST_SECONDS = 1.0     # 15s structure break must persist to count as invalid

# DYNAMIC EXIT MANAGEMENT (Used by execution_engine.py)
DYNAMIC_EXIT_PARTIAL_FRACTION = 0.5              # Fraction to sell at the first target before trailing the runner
DYNAMIC_EXIT_BREAKEVEN_OFFSET_PCT = 0.10         # After the partial, move the stop slightly above breakeven
DYNAMIC_EXIT_TRAIL_OFFSET_PCT = 2.0              # Trail the runner this far below the best price seen since entry
DYNAMIC_EXIT_MIN_STOP_UPDATE_PCT = 0.20          # Minimum improvement required before modifying the live stop
DYNAMIC_EXIT_MAX_HOLD_SECONDS = 300              # Maximum regular-hours hold time after fill before forcing a bot-managed exit
DYNAMIC_EXIT_MAX_WALL_CLOCK_HOLD_SECONDS = 1200  # Secondary max hold cap even if halts/pause detection freeze active-hold time
DYNAMIC_EXIT_MARKET_PAUSE_SUSPECT_SECONDS = 20   # Suspect a halt / market pause when updates stop for this long
DYNAMIC_EXIT_MARKET_PAUSE_CONFIRM_SECONDS = 45   # Large gaps count as confirmed pauses even without quote anomalies
DYNAMIC_EXIT_MARKET_PAUSE_ABNORMAL_SPREAD_PCT = 8.0  # Spread threshold used as a halt/market-pause signal
DYNAMIC_EXIT_REOPEN_BUFFER_SECONDS = 10          # Ignore dynamic exits briefly after a confirmed market pause resumes
DYNAMIC_EXIT_REOPEN_STRONG_BUFFER_PCT = 0.10     # Small cushion above the pre-pause price to classify a reopen as strong
DYNAMIC_EXIT_VOLUME_FADE_MIN_HOLD_SECONDS = 20   # Ignore early noise; only evaluate volume fade after this many seconds from fill
DYNAMIC_EXIT_VOLUME_FADE_WINDOW_SECONDS = 15     # Measure recent live volume over this trailing window
DYNAMIC_EXIT_VOLUME_FADE_FRACTION_OF_PEAK = 0.35 # Exit if recent volume rate falls below this fraction of the trade's post-entry peak rate
DYNAMIC_EXIT_VOLUME_FADE_MIN_RETRACE_PCT = 1.0   # Require some giveback from the high so volume fade alone does not cut strong trend holds

# RUNTIME FEEDBACK / TELEMETRY
RUNTIME_FEEDBACK_DIR_NAME = "runtime_feedback"   # Folder for structured event logs and live state snapshots
RUNTIME_FEEDBACK_TOP_SYMBOLS = 20                # Limit large state snapshots to the most relevant symbols
SCANNER_MONITOR_CAP = 50                         # Cap the live scanner universe to the strongest day gainers
SCANNER_REFRESH_INTERVAL_SECONDS = 30            # Refresh the gainer universe on this cadence
SCANNER_MAX_SYMBOL_CHANGES_PER_REFRESH = 3       # Limit how many symbols can rotate in/out per refresh
SCANNER_NEWS_REFRESH_INTERVAL_SECONDS = 300      # Refresh news for all monitored symbols on a slower cadence
TRADE_TRACE_SNAPSHOT_INTERVAL_SECONDS = 1.0      # Snapshot cadence for trade-centered feedback traces
TRADE_TRACE_PRE_ENTRY_SECONDS = 5                # Include this many seconds before a trade opens
TRADE_TRACE_POST_EXIT_SECONDS = 5                # Keep tracing this many seconds after a trade closes
TRADE_TRACE_BUFFER_SECONDS = 30                  # Rolling per-symbol buffer retained in memory for pre-entry context

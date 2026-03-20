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

# ALERT RATING / QUALITY SCORING (Used by alert_rating.py and realtime_multi_session_scanner.py)
ALERT_MIN_SCORE_TO_NOTIFY = 1               # Temporary floor while news catalyst scoring is unimplemented
ALERT_GRADE_A_MIN_SCORE = 7                 # Temporary A cutoff with news catalyst currently unavailable
ALERT_GRADE_B_MIN_SCORE = 4                 # Temporary B cutoff with news catalyst currently unavailable
ALERT_BREAKOUT_LOOKBACK_MINUTES = 10        # Rolling 1-minute candle lookback for local breakout scoring
ALERT_DRAWDOWN_LOOKBACK_MINUTES = 60        # Rolling 1-minute candle lookback
ALERT_DRAWDOWN_TOP_GREEN_CANDLE_COUNT = 3   # Average the strongest green bodies in the lookback
ALERT_DRAWDOWN_UPPER_BASE_PCT = 10.0        # Base threshold for the first drawdown quality point
ALERT_DRAWDOWN_LOWER_BASE_PCT = 5.0         # Base threshold for the second drawdown quality point
ALERT_DRAWDOWN_UPPER_VOL_MULTIPLIER = 0.70  # Multiplier applied to avg top green 1m body %
ALERT_DRAWDOWN_LOWER_VOL_MULTIPLIER = 0.35  # Multiplier applied to avg top green 1m body %

# FAST IGNITION CONDITIONS (Used by realtime_multi_session_scanner.py)
FAST_IGNITION_PCT_5S = 0.8              # Minimum move over last 5 seconds
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

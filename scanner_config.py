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

# IN-DEPTH FILTERING (Used in run_realtime_trading.py)
STRICT_MOMENTUM_REQUIRED = True
MIN_PRICE_SURGE_10S = 1.5   # 1.5% surge in 10s
MAX_DRAWDOWN_10S = 0.5      # 0.5% max drawdown
MIN_TREND_30S = 1.0         # 1.0% trend in 30s

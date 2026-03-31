"""
Real-time Broad Screening Tool - PREMARKET ENHANCED
Ross Cameron-style preliminary screening based on togglable conditions.
Enhanced with market session awareness and premarket-specific features.
"""
import time
import os
import re
from datetime import datetime, timedelta
from typing import List, Dict, Callable, Optional
from collections import deque
import xml.etree.ElementTree as ET
from conditions import MarketData, AlertConditionSet, PriceAboveVWAPCondition, FastIgnitionCondition
import platform
import pytz
from scanner_config import (
    ALERT_MIN_SCORE_TO_NOTIFY,
    ALERT_DRAWDOWN_LOOKBACK_MINUTES,
    RUNTIME_FEEDBACK_TOP_SYMBOLS,
    SCANNER_MONITOR_CAP,
    SCANNER_MAX_SYMBOL_CHANGES_PER_REFRESH,
    SCANNER_NEWS_REFRESH_INTERVAL_SECONDS,
    SCANNER_REFRESH_INTERVAL_SECONDS,
    SQUEEZE_PCT_THRESHOLD,
    SQUEEZE_TIME_MINUTES,
    FAST_IGNITION_PCT_5S,
    FAST_IGNITION_PCT_15S,
    FAST_IGNITION_VOLUME_MULTIPLIER,
    FAST_IGNITION_MAX_RETRACEMENT_PCT,
    NEWS_CATALYST_ENABLED,
    NEWS_CATALYST_IGNORE_KEYWORDS,
    NEWS_CATALYST_MAX_HEADLINES,
    NEWS_CATALYST_NEGATIVE_KEYWORDS,
    NEWS_CATALYST_POSITIVE_KEYWORDS,
    MIN_AVG_DAILY_DOLLAR_VOLUME,
    DISCORD_WEBHOOK_URL,
    TWS_PORT,
)
import requests
from top_gainers_fetcher import get_top_gainers
from alert_rating import calculate_alert_rating, get_breakout_debug_info, get_drawdown_debug_info, get_momentum_debug_info, get_alert_grade_rank
from runtime_feedback import RuntimeTelemetry

# Market session times (Eastern Time)
PREMARKET_START = (4, 0)   # 4:00 AM ET
MARKET_OPEN = (9, 30)      # 9:30 AM ET
MARKET_CLOSE = (16, 0)     # 4:00 PM ET
AFTERHOURS_END = (20, 0)   # 8:00 PM ET
ALERT_SCORE_AUDIT_FILE = os.path.join(os.path.dirname(__file__), "temp_alert_score_audit.log")
ALERT_SCORE_HISTORY_DIR = os.path.join(os.path.dirname(__file__), "alert_history")
CURRENT_RUN_AUDIT_FILE = None
SIGNAL_STATE_RESET_GAP_SECONDS = max(SQUEEZE_TIME_MINUTES * 60, 300)
RUNTIME_FEEDBACK_DIR = os.path.join(os.path.dirname(__file__), "runtime_feedback")


def format_compact_volume(value: float) -> str:
    """Format volume using M/K units without rounding small values down to 0.0M."""
    if value >= 1_000_000:
        return f"{value/1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value/1_000:.1f}K"
    return f"{value:.0f}"


def parse_ibkr_news_timestamp(raw_value: str) -> Optional[datetime]:
    """Parse IBKR historical news timestamps into Eastern-aware datetimes when possible."""
    if not raw_value:
        return None

    et_tz = pytz.timezone('US/Eastern')
    normalized = raw_value.replace("  ", " ").strip()
    compact = " ".join(normalized.split())

    for fmt in (
        "%Y%m%d %H:%M:%S",
        "%Y%m%d-%H:%M:%S",
        "%Y%m%d  %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S",
    ):
        try:
            parsed = datetime.strptime(compact, fmt)
            if parsed.tzinfo is not None:
                return parsed.astimezone(et_tz)
            return et_tz.localize(parsed)
        except ValueError:
            continue

    iso_candidate = compact.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(iso_candidate)
        if parsed.tzinfo is not None:
            return parsed.astimezone(et_tz)
        return et_tz.localize(parsed)
    except ValueError:
        pass

    # IBKR and some providers append timezone names or extra tokens after a parseable timestamp.
    prefix_match = re.match(r"^(\d{8}[- ]\d{2}:\d{2}:\d{2})", compact)
    if prefix_match:
        prefix = prefix_match.group(1)
        for fmt in ("%Y%m%d %H:%M:%S", "%Y%m%d-%H:%M:%S"):
            try:
                return et_tz.localize(datetime.strptime(prefix, fmt))
            except ValueError:
                continue

    text_match = re.match(r"^([A-Za-z]{3}, \d{2} [A-Za-z]{3} \d{4} \d{2}:\d{2}:\d{2}(?: [+-]\d{4})?)", compact)
    if text_match:
        text_prefix = text_match.group(1)
        for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S"):
            try:
                parsed = datetime.strptime(text_prefix, fmt)
                if parsed.tzinfo is not None:
                    return parsed.astimezone(et_tz)
                return et_tz.localize(parsed)
            except ValueError:
                continue

    return None


def classify_news_headline(headline: str) -> tuple[bool, str]:
    """Return whether a headline qualifies as a cautious positive catalyst."""
    headline_lower = headline.lower()

    for keyword in NEWS_CATALYST_NEGATIVE_KEYWORDS:
        if keyword in headline_lower:
            return False, f"Excluded keyword: {keyword}"

    for keyword in NEWS_CATALYST_IGNORE_KEYWORDS:
        if keyword in headline_lower:
            return False, f"Ignored keyword: {keyword}"

    for keyword in NEWS_CATALYST_POSITIVE_KEYWORDS:
        if keyword in headline_lower:
            return True, f"Matched keyword: {keyword}"

    return False, "No configured catalyst keyword matched"


def initialize_alert_score_audit_file():
    """Initialize the temp audit log and a permanent per-run history log."""
    global CURRENT_RUN_AUDIT_FILE
    start_time = datetime.now()
    os.makedirs(ALERT_SCORE_HISTORY_DIR, exist_ok=True)
    CURRENT_RUN_AUDIT_FILE = os.path.join(
        ALERT_SCORE_HISTORY_DIR,
        f"alert_score_audit_{start_time.strftime('%Y-%m-%d_%H-%M-%S')}.log",
    )
    header = (
        "# Alert Score Audit Log (temp)\n"
        f"# Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
        "# One block per triggered alert with scoring factor breakdown.\n\n"
    )
    history_header = (
        "# Alert Score Audit Log (history)\n"
        f"# Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
        "# One block per triggered alert with scoring factor breakdown.\n\n"
    )
    with open(ALERT_SCORE_AUDIT_FILE, "w", encoding="utf-8") as f:
        f.write(header)
    with open(CURRENT_RUN_AUDIT_FILE, "w", encoding="utf-8") as f:
        f.write(history_header)


def append_alert_score_audit(symbol: str, timestamp: datetime, session: str, trigger_reasons: List[str], monitor):
    """Append one triggered alert with point-by-point factors to the temp audit file."""
    trigger_str = ", ".join(trigger_reasons) if trigger_reasons else "Unknown"
    factors = monitor.alert_score_reasons if monitor.alert_score_reasons else []
    breakout_debug = monitor.alert_breakout_debug_info if monitor.alert_breakout_debug_info else None
    drawdown_debug = monitor.alert_drawdown_debug_info if monitor.alert_drawdown_debug_info else None
    momentum_debug = monitor.alert_momentum_debug_info if monitor.alert_momentum_debug_info else None
    news_debug = monitor.alert_news_debug_info if monitor.alert_news_debug_info else None
    header_line = f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] {symbol} | Session={session} | Grade={monitor.alert_grade} | Score={monitor.alert_score}"
    separator = "=" * max(len(header_line), 100)

    lines = [
        separator,
        header_line,
        separator,
        f"Trigger: {trigger_str}",
        "Score factors:",
    ]

    if factors:
        for factor in factors:
            lines.append(f"- {factor}")
    else:
        lines.append("- None")

    if breakout_debug:
        prior_local_high_str = (
            f"{breakout_debug['prior_local_high']:.4f}" if breakout_debug["prior_local_high"] is not None else "None"
        )
        prior_session_high_str = (
            f"{breakout_debug['prior_session_high']:.4f}" if breakout_debug["prior_session_high"] is not None else "None"
        )
        lines.extend(
            [
                "Breakout debug:",
                f"- Current 1m high: {breakout_debug['current_minute_high']:.4f}",
                f"- Prior {breakout_debug['lookback_minutes']}m high: {prior_local_high_str} | count={breakout_debug['prior_local_high_count']} | pass={breakout_debug['passed_local_breakout']}",
                f"- Prior session high: {prior_session_high_str} | count={breakout_debug['prior_session_high_count']} | pass={breakout_debug['passed_session_hod_breakout']}",
            ]
        )

    if drawdown_debug:
        top_green_body_str = ", ".join(f"{pct:.2f}%" for pct in drawdown_debug["top_green_1m_body_pcts"]) or "None"
        lines.extend(
            [
                "Drawdown debug:",
                f"- Observed max 1m H-L drawdown: {drawdown_debug['observed_max_drawdown_pct']:.2f}%",
                f"- Lookback: {drawdown_debug['lookback_minutes']}m",
                f"- Green 1m candles in lookback: {drawdown_debug['recent_green_body_count']}",
                f"- Top {drawdown_debug['top_green_candle_count']} green 1m body % values: {top_green_body_str}",
                f"- Volatility reference: {drawdown_debug['volatility_reference_pct']:.2f}%",
                f"- Upper threshold: max({drawdown_debug['upper_base_threshold_pct']:.2f}%, dynamic) = {drawdown_debug['upper_dynamic_threshold_pct']:.2f}% | pass={drawdown_debug['passed_upper_threshold']}",
                f"- Lower threshold: max({drawdown_debug['lower_base_threshold_pct']:.2f}%, dynamic) = {drawdown_debug['lower_dynamic_threshold_pct']:.2f}% | pass={drawdown_debug['passed_lower_threshold']}",
            ]
        )

    if momentum_debug:
        lines.extend(
            [
                "Momentum debug:",
                f"- Dense recent coverage: {momentum_debug['has_dense_coverage']}",
                f"- Price recent samples 60s: {momentum_debug['price_recent_samples_60s']}",
                f"- Volume recent samples 60s: {momentum_debug['volume_recent_samples_60s']}",
                f"- Price accel pass: {momentum_debug['price_accel_pass']}",
            ]
        )
        if all(momentum_debug["price_windows_pct"][label] is not None for label in ("5s", "15s", "30s", "60s")):
            lines.append(
                "- Price windows: "
                f"5s={momentum_debug['price_windows_pct']['5s']:.2f}% | "
                f"15s={momentum_debug['price_windows_pct']['15s']:.2f}% | "
                f"30s={momentum_debug['price_windows_pct']['30s']:.2f}% | "
                f"60s={momentum_debug['price_windows_pct']['60s']:.2f}%"
            )
            lines.append(
                "- Price rates: "
                f"5s={momentum_debug['price_rates_pct_per_sec']['5s']:.4f}%/s | "
                f"15s={momentum_debug['price_rates_pct_per_sec']['15s']:.4f}%/s | "
                f"30s={momentum_debug['price_rates_pct_per_sec']['30s']:.4f}%/s | "
                f"60s={momentum_debug['price_rates_pct_per_sec']['60s']:.4f}%/s"
            )
        else:
            lines.extend(
                [
                    "- Price windows: insufficient history",
                    "- Price rates: insufficient history",
                ]
            )
        lines.append(f"- Volume accel pass: {momentum_debug['volume_accel_pass']}")
        if all(momentum_debug["volume_windows"][label] is not None for label in ("5s", "15s", "30s", "60s")):
            lines.append(
                "- Volume windows: "
                f"5s={momentum_debug['volume_windows']['5s']:.0f} | "
                f"15s={momentum_debug['volume_windows']['15s']:.0f} | "
                f"30s={momentum_debug['volume_windows']['30s']:.0f} | "
                f"60s={momentum_debug['volume_windows']['60s']:.0f}"
            )
            lines.append(
                "- Volume rates: "
                f"5s={momentum_debug['volume_rates_per_sec']['5s']:.2f}/s | "
                f"15s={momentum_debug['volume_rates_per_sec']['15s']:.2f}/s | "
                f"30s={momentum_debug['volume_rates_per_sec']['30s']:.2f}/s | "
                f"60s={momentum_debug['volume_rates_per_sec']['60s']:.2f}/s"
            )
        else:
            lines.extend(
                [
                    "- Volume windows: insufficient history",
                    "- Volume rates: insufficient history",
                ]
            )
        lines.append(f"- Combined momentum pass: {momentum_debug['combined_momentum_pass']}")

    if news_debug:
        lines.extend(
            [
                "News debug:",
                f"- Enabled: {news_debug['enabled']}",
                f"- Headlines fetched: {news_debug['fetched_count']}",
                f"- Same-day headlines: {news_debug['same_day_count']}",
                f"- Meaningful match found: {news_debug['has_meaningful_news_today']}",
                f"- Match reason: {news_debug['match_reason']}",
            ]
        )
        if news_debug["same_day_headlines"]:
            lines.append("- Same-day headlines:")
            for headline in news_debug["same_day_headlines"]:
                lines.append(f"  * {headline}")

    lines.extend(["", "-" * len(separator), ""])

    block = "\n".join(lines)
    with open(ALERT_SCORE_AUDIT_FILE, "a", encoding="utf-8") as f:
        f.write(block)
    if CURRENT_RUN_AUDIT_FILE:
        with open(CURRENT_RUN_AUDIT_FILE, "a", encoding="utf-8") as f:
            f.write(block)

def get_market_session() -> str:
    """Determine current market session (PREMARKET, REGULAR, AFTERHOURS, CLOSED)"""
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz)
    current_time = (now_et.hour, now_et.minute)
    day_of_week = now_et.weekday()  # 0=Monday, 6=Sunday
    
    # Weekend check
    if day_of_week >= 5:  # Saturday or Sunday
        return "CLOSED"
    
    # Convert time to minutes for easier comparison
    current_minutes = current_time[0] * 60 + current_time[1]
    premarket_start_min = PREMARKET_START[0] * 60 + PREMARKET_START[1]
    market_open_min = MARKET_OPEN[0] * 60 + MARKET_OPEN[1]
    market_close_min = MARKET_CLOSE[0] * 60 + MARKET_CLOSE[1]
    afterhours_end_min = AFTERHOURS_END[0] * 60 + AFTERHOURS_END[1]
    
    if premarket_start_min <= current_minutes < market_open_min:
        return "PREMARKET"
    elif market_open_min <= current_minutes < market_close_min:
        return "REGULAR"
    elif market_close_min <= current_minutes < afterhours_end_min:
        return "AFTERHOURS"
    else:
        return "CLOSED"

def get_session_start_time() -> datetime:
    """Get the start time of the current market session"""
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz)
    session = get_market_session()
    
    if session == "PREMARKET":
        start_time = now_et.replace(hour=PREMARKET_START[0], minute=PREMARKET_START[1], second=0, microsecond=0)
    elif session == "REGULAR":
        start_time = now_et.replace(hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0, microsecond=0)
    elif session == "AFTERHOURS":
        start_time = now_et.replace(hour=MARKET_CLOSE[0], minute=MARKET_CLOSE[1], second=0, microsecond=0)
    else:  # CLOSED
        start_time = now_et
    
    return start_time

def get_next_10_minute_mark() -> datetime:
    """Return the next wall-clock 10-minute boundary in Eastern Time."""
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz)
    next_minute = ((now_et.minute // 10) + 1) * 10
    if next_minute >= 60:
        next_mark = now_et.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        next_mark = now_et.replace(minute=next_minute, second=0, microsecond=0)
    return next_mark

class RealtimeSymbolMonitor:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.price_history = deque(maxlen=600)  # 10 mins of data at 1s intervals
        self.volume_history = deque(maxlen=600)
        self.signal_volume_history = deque(maxlen=600)
        self.signal_volume_history_seeded = False
        self.last_update = None
        self.bid = 0.0
        self.ask = 0.0
        self.vwap = 0.0
        
        # Fundamental data
        self.float_shares = None
        self.avg_daily_volume = None
        self.relative_volume = 0.0
        self.rel_vol_1m = 0.0 # 1-minute relative volume fallback
        
        # Premarket-specific tracking
        self.session_volume = 0.0  # Volume for current session only
        self.minute_volumes = deque(maxlen=11) # Track last 11 minutes of volume for 1m-RelVol
        self.last_session = None
        self.day_start_price = None  # Track first price of the day for total % gain calculation
        self.last_day = None  # Track which day we're on

        # Intraday 1-minute candle drawdown tracking (high-to-low).
        self.current_minute_bucket = None
        self.current_minute_open = None
        self.current_minute_high = None
        self.current_minute_low = None
        self.current_minute_close = None
        self.max_intraday_1m_drawdown_pct = None
        self.green_1m_body_history = deque(maxlen=max(ALERT_DRAWDOWN_LOOKBACK_MINUTES * 3, 180))
        self.completed_1m_high_history = deque()
        
        # Screening results
        self.triggered_conditions = []
        self.last_alert_time = None  # Track when last alert triggered
        self.last_emitted_grade = None
        self.alert_score = 0
        self.alert_grade = "Below Threshold"
        self.alert_score_reasons = []
        self.alert_is_suppressed = False
        self.alert_breakout_debug_info = None
        self.alert_drawdown_debug_info = None
        self.alert_momentum_debug_info = None
        self.alert_news_debug_info = None
        self.preliminary_block_reason = ""
        
        # News catalyst tracking
        self.has_meaningful_news_today = False
        self.news_headlines_today = []
        self.news_match_reason = "News not checked yet"
        
        # Initialize the live alert trigger set.
        self.fast_condition_set = AlertConditionSet("Fast Ignition")
        self.fast_condition_set.add_condition(PriceAboveVWAPCondition())
        self.fast_condition_set.add_condition(
            FastIgnitionCondition(
                pct_threshold_5s=FAST_IGNITION_PCT_5S,
                pct_threshold_15s=FAST_IGNITION_PCT_15S,
                volume_multiplier=FAST_IGNITION_VOLUME_MULTIPLIER,
                max_retracement_pct=FAST_IGNITION_MAX_RETRACEMENT_PCT,
            )
        )

    def _reset_short_horizon_signal_state(self, price: float, now: datetime):
        """Drop short-horizon state after long inactivity gaps so delayed ticks cannot fabricate momentum."""
        self.price_history.clear()
        self.volume_history.clear()
        self.signal_volume_history.clear()
        self.signal_volume_history_seeded = False
        self.minute_volumes.clear()
        self.current_minute_bucket = now.replace(second=0, microsecond=0)
        self.current_minute_open = price
        self.current_minute_high = price
        self.current_minute_low = price
        self.current_minute_close = price
        self.max_intraday_1m_drawdown_pct = None
        self.green_1m_body_history.clear()
        self.completed_1m_high_history.clear()

    def seed_signal_volume_history_from_bars(self, bars) -> None:
        """Warm short-horizon signal volume history from recent bars without affecting live session accounting."""
        self.signal_volume_history.clear()
        running_cumulative_volume = 0.0
        for bar in bars:
            running_cumulative_volume += max(0.0, float(bar.volume))
            self.signal_volume_history.append((bar.date, running_cumulative_volume))
        self.signal_volume_history_seeded = bool(bars)

    def _append_signal_volume(self, timestamp: datetime, live_cumulative_volume: float) -> None:
        """Maintain a cumulative volume series for signal logic, aligning historical warmup to the first live tick."""
        if self.signal_volume_history_seeded and self.signal_volume_history:
            offset = live_cumulative_volume - self.signal_volume_history[-1][1]
            self.signal_volume_history = deque(
                ((ts, value + offset) for ts, value in self.signal_volume_history),
                maxlen=self.signal_volume_history.maxlen,
            )
            self.signal_volume_history_seeded = False

        self.signal_volume_history.append((timestamp, live_cumulative_volume))

    def update_market_data(self, price: float, volume: float, vwap: float, bid: float = 0, ask: float = 0):
        now = datetime.now()
        current_session = get_market_session()

        if self.last_update is not None:
            gap_seconds = (now - self.last_update).total_seconds()
            if gap_seconds > SIGNAL_STATE_RESET_GAP_SECONDS:
                print(
                    f"[STATE] {self.symbol}: Resetting short-horizon signal state after "
                    f"{gap_seconds:.0f}s inactivity gap"
                )
                self._reset_short_horizon_signal_state(price, now)

        # Get current date for day tracking
        et_tz = pytz.timezone('US/Eastern')
        now_et = datetime.now(et_tz)
        current_day = now_et.date()
        
        # Check if we've started a new trading day
        if self.last_day != current_day:
            # Only reset if we don't have a historical baseline
            # (historical baseline is more accurate than intraday first price)
            if self.day_start_price is None:
                self.day_start_price = price
            self.last_day = current_day
            self.signal_volume_history.clear()
            self.signal_volume_history_seeded = False
            self.current_minute_bucket = None
            self.current_minute_open = None
            self.current_minute_high = None
            self.current_minute_low = None
            self.current_minute_close = None
            self.max_intraday_1m_drawdown_pct = None
            self.green_1m_body_history.clear()
            self.completed_1m_high_history.clear()
        
        # Set day start price ONLY if not yet set (will use historical close if available)
        if self.day_start_price is None:
            self.day_start_price = price

        # Update 1-minute high/low buckets and the day's max high-to-low drawdown.
        minute_bucket = now.replace(second=0, microsecond=0)
        if self.current_minute_bucket != minute_bucket:
            if self.current_minute_high is not None and self.current_minute_low is not None and self.current_minute_high > 0:
                minute_drawdown_pct = ((self.current_minute_high - self.current_minute_low) / self.current_minute_high) * 100
                if self.max_intraday_1m_drawdown_pct is None:
                    self.max_intraday_1m_drawdown_pct = minute_drawdown_pct
                else:
                    self.max_intraday_1m_drawdown_pct = max(self.max_intraday_1m_drawdown_pct, minute_drawdown_pct)
            if self.current_minute_bucket is not None and self.current_minute_high is not None:
                self.completed_1m_high_history.append((self.current_minute_bucket, self.current_minute_high))
            if (
                self.current_minute_bucket is not None
                and self.current_minute_open is not None
                and self.current_minute_close is not None
                and self.current_minute_open > 0
            ):
                minute_body_pct = ((self.current_minute_close - self.current_minute_open) / self.current_minute_open) * 100
                if minute_body_pct > 0:
                    self.green_1m_body_history.append((self.current_minute_bucket, minute_body_pct))
            self.current_minute_bucket = minute_bucket
            self.current_minute_open = price
            self.current_minute_high = price
            self.current_minute_low = price
            self.current_minute_close = price
        else:
            self.current_minute_high = price if self.current_minute_high is None else max(self.current_minute_high, price)
            self.current_minute_low = price if self.current_minute_low is None else min(self.current_minute_low, price)
            self.current_minute_close = price
        
        # Initialize session state without wiping any warmup history. Only clear
        # when transitioning between two real sessions.
        if self.last_session is None:
            self.last_session = current_session
        elif self.last_session != current_session:
            print(f"[SESSION] {self.symbol}: Transitioned from {self.last_session} to {current_session}")
            self.last_session = current_session
            self.session_volume = 0.0
            # Clear price history on session transition to avoid cross-session comparisons.
            self.price_history.clear()
            self.volume_history.clear()
            self.signal_volume_history.clear()
            self.signal_volume_history_seeded = False
            self.current_minute_bucket = minute_bucket
            self.current_minute_open = price
            self.current_minute_high = price
            self.current_minute_low = price
            self.current_minute_close = price
            self.max_intraday_1m_drawdown_pct = None
            self.green_1m_body_history.clear()
            self.completed_1m_high_history.clear()

        self.price_history.append((now, price))
        self.volume_history.append((now, volume))
        self._append_signal_volume(now, volume)
        self.last_update = now
        self.bid = bid
        self.ask = ask
        self.vwap = vwap
        
        # Track session volume and 1-minute relative volume
        if len(self.volume_history) >= 2:
            volume_delta = volume - self.volume_history[-2][1]
            if volume_delta > 0:
                self.session_volume += volume_delta
                
                # Update minute volumes for fallback RelVol
                if not self.minute_volumes or (now - self.minute_volumes[-1][0]).total_seconds() >= 60:
                    self.minute_volumes.append([now, volume_delta])
                else:
                    self.minute_volumes[-1][1] += volume_delta
        
        # Calculate 1-minute Relative Volume (fallback)
        if len(self.minute_volumes) >= 2:
            current_min_vol = self.minute_volumes[-1][1]
            prev_minutes = list(self.minute_volumes)[:-1]
            avg_min_vol = sum(v[1] for v in prev_minutes) / len(prev_minutes)
            self.rel_vol_1m = current_min_vol / avg_min_vol if avg_min_vol > 0 else 0.0

        # Calculate Daily Relative Volume based on session
        if self.avg_daily_volume and self.avg_daily_volume > 0:
            if current_session == "PREMARKET":
                typical_premarket_vol = self.avg_daily_volume * 0.05
                self.relative_volume = self.session_volume / typical_premarket_vol if typical_premarket_vol > 0 else 0.0
            else:
                self.relative_volume = volume / self.avg_daily_volume
        else:
            # If no fundamental daily volume, use our 1m fallback as the main metric
            self.relative_volume = self.rel_vol_1m

    def _get_recent_green_1m_body_pcts(self) -> List[float]:
        """Return recent positive 1-minute candle body percentages for drawdown thresholding."""
        if self.last_update is None:
            return []

        lookback_start = self.last_update - timedelta(minutes=ALERT_DRAWDOWN_LOOKBACK_MINUTES)
        recent_green_body_pcts = [
            body_pct for bucket, body_pct in self.green_1m_body_history if bucket >= lookback_start
        ]
        if (
            self.current_minute_open is not None
            and self.current_minute_close is not None
            and self.current_minute_open > 0
        ):
            current_minute_body_pct = ((self.current_minute_close - self.current_minute_open) / self.current_minute_open) * 100
            if current_minute_body_pct > 0:
                recent_green_body_pcts.append(current_minute_body_pct)
        return recent_green_body_pcts

    def _get_completed_1m_highs(self) -> List[tuple]:
        """Return completed session-local 1-minute highs for breakout scoring."""
        return list(self.completed_1m_high_history)

    def _passes_liquidity_prefilter(self) -> bool:
        """Block obviously thin names before alert scoring."""
        if self.avg_daily_volume is None or not self.price_history:
            return True
        current_price = self.price_history[-1][1]
        if current_price is None or current_price <= 0:
            return True
        avg_daily_dollar_volume = self.avg_daily_volume * current_price
        return avg_daily_dollar_volume >= MIN_AVG_DAILY_DOLLAR_VOLUME

    def check_screening_conditions(self) -> List[str]:
        if not self.price_history:
            return []
            
        # Create MarketData object for the condition set
        data = MarketData(
            symbol=self.symbol,
            price=self.price_history[-1][1],
            volume=self.volume_history[-1][1] if self.volume_history else 0,
            vwap=self.vwap,
            timestamp=self.last_update,
            bid=self.bid,
            ask=self.ask,
            price_history=list(self.price_history),
            volume_history=list(self.signal_volume_history),
        )
        
        trigger_summary = None
        if self.fast_condition_set.check_all(data):
            trigger_summary = self.fast_condition_set.get_trigger_summary()
            self.preliminary_block_reason = ""
        else:
            self.preliminary_block_reason = self.fast_condition_set.last_block_reason

        if trigger_summary:
            if not self._passes_liquidity_prefilter():
                self.triggered_conditions = []
                self.alert_score = 0
                self.alert_grade = "Below Threshold"
                self.alert_score_reasons = [
                    f"Liquidity prefilter failed: avg daily dollar volume below ${MIN_AVG_DAILY_DOLLAR_VOLUME:,.0f}"
                ]
                self.alert_is_suppressed = True
                self.alert_breakout_debug_info = None
                self.alert_drawdown_debug_info = None
                self.alert_momentum_debug_info = None
                self.alert_news_debug_info = None
                self.preliminary_block_reason = ""
                return []

            # Include the in-progress current minute candle in drawdown quality scoring.
            effective_max_drawdown = self.max_intraday_1m_drawdown_pct
            if self.current_minute_high is not None and self.current_minute_low is not None and self.current_minute_high > 0:
                current_minute_drawdown = ((self.current_minute_high - self.current_minute_low) / self.current_minute_high) * 100
                if effective_max_drawdown is None:
                    effective_max_drawdown = current_minute_drawdown
                else:
                    effective_max_drawdown = max(effective_max_drawdown, current_minute_drawdown)
            
            # Cooldown passed or first alert - trigger it
            recent_green_1m_body_pcts = self._get_recent_green_1m_body_pcts()
            completed_1m_highs = self._get_completed_1m_highs()
            self.alert_breakout_debug_info = get_breakout_debug_info(
                current_minute_high=self.current_minute_high,
                current_minute_bucket=self.current_minute_bucket,
                completed_1m_highs=completed_1m_highs,
            )
            self.alert_drawdown_debug_info = get_drawdown_debug_info(
                max_intraday_1m_drawdown_pct=effective_max_drawdown,
                recent_green_1m_body_pcts=recent_green_1m_body_pcts,
            )
            self.alert_momentum_debug_info = get_momentum_debug_info(
                price_history=list(self.price_history),
                volume_history=list(self.signal_volume_history),
                last_update=self.last_update,
            )
            self.alert_score, self.alert_grade, self.alert_score_reasons = calculate_alert_rating(
                price_history=list(self.price_history),
                volume_history=list(self.signal_volume_history),
                last_update=self.last_update,
                relative_volume=self.relative_volume,
                float_shares=self.float_shares,
                current_price=self.price_history[-1][1],
                squeeze_pct_threshold=SQUEEZE_PCT_THRESHOLD,
                squeeze_time_minutes=SQUEEZE_TIME_MINUTES,
                breakout_debug_info=self.alert_breakout_debug_info,
                max_intraday_1m_drawdown_pct=effective_max_drawdown,
                recent_green_1m_body_pcts=recent_green_1m_body_pcts,
                has_meaningful_news_today=self.has_meaningful_news_today,
                momentum_debug_info=self.alert_momentum_debug_info,
            )
            self.alert_news_debug_info = {
                "enabled": NEWS_CATALYST_ENABLED,
                "fetched_count": len(self.news_headlines_today),
                "same_day_count": len(self.news_headlines_today),
                "has_meaningful_news_today": self.has_meaningful_news_today,
                "match_reason": self.news_match_reason,
                "same_day_headlines": [item["headline"] for item in self.news_headlines_today],
            }
            self.alert_is_suppressed = (
                self.alert_grade == "Below Threshold" or self.alert_score < ALERT_MIN_SCORE_TO_NOTIFY
            )
            should_emit = True
            if self.last_alert_time:
                seconds_since_last_alert = (self.last_update - self.last_alert_time).total_seconds()
                if seconds_since_last_alert < 60:
                    current_rank = get_alert_grade_rank(self.alert_grade)
                    prior_rank = get_alert_grade_rank(self.last_emitted_grade)
                    should_emit = current_rank > prior_rank

            if not should_emit:
                return []
            if self.alert_is_suppressed:
                self.triggered_conditions = []
                self.last_emitted_grade = self.alert_grade
                self.last_alert_time = self.last_update
                return [trigger_summary]
            self.triggered_conditions = [trigger_summary]
            self.last_emitted_grade = self.alert_grade
            self.last_alert_time = self.last_update
            return self.triggered_conditions
        else:
            # Keep triggered_conditions to show recent alerts, only clear after timeout
            if self.last_alert_time and (self.last_update - self.last_alert_time) > timedelta(minutes=5):
                self.triggered_conditions = []
                self.alert_score = 0
                self.alert_grade = "Below Threshold"
                self.alert_score_reasons = []
                self.alert_is_suppressed = False
                self.alert_breakout_debug_info = None
                self.alert_drawdown_debug_info = None
                self.alert_momentum_debug_info = None
                self.alert_news_debug_info = None
                self.last_emitted_grade = None
            return []

class RealtimeBroadScanner:
    def __init__(self, symbols: List[str]):
        self.symbols = symbols
        self.monitors = {s: RealtimeSymbolMonitor(s) for s in symbols}
        self.alert_callback = None
        self.recent_alerts = deque(maxlen=10)  # Store last 10 alerts
        self.last_vwap_sync_time = None
        self.last_session = None

    def on_preliminary_alert(self, callback: Callable):
        self.alert_callback = callback

    def check_session_transition(self):
        """Check if we need to resync VWAP due to session transition"""
        current_session = get_market_session()
        if self.last_session != current_session:
            print(f"[SCANNER] Market session changed: {self.last_session} -> {current_session}")
            self.last_session = current_session
            return True
        return False

    def update(self, symbol: str, price: float, volume: float, vwap: float, bid: float = 0, ask: float = 0):
        if symbol in self.monitors:
            monitor = self.monitors[symbol]
            monitor.update_market_data(price, volume, vwap, bid, ask)
            
            triggered = monitor.check_screening_conditions()
            if triggered and self.alert_callback:
                alert_time = datetime.now()
                if not monitor.alert_is_suppressed:
                    self.recent_alerts.append((alert_time, symbol, triggered, monitor.alert_grade, monitor.alert_score))
                self.alert_callback(symbol, alert_time, triggered, monitor)

    def load_fundamentals(self, tws_app):
        print("[SCANNER] Loading fundamental data for screening...")
        for symbol, monitor in self.monitors.items():
            # Try fundamental data first
            xml_data = tws_app.fetch_fundamental_data(symbol)
            if xml_data:
                try:
                    root = ET.fromstring(xml_data)
                    for ratio in root.findall(".//Ratio"):
                        field = ratio.get("FieldName")
                        if field == 'FLOAT':
                            monitor.float_shares = float(ratio.text)
                        elif field == 'VOL10DAVG':
                            monitor.avg_daily_volume = tws_app.normalize_stock_volume(ratio.text)
                            # Update TWS app data for volume correction reference
                            with tws_app.lock:
                                if symbol in tws_app.realtime_data:
                                    tws_app.realtime_data[symbol]['avg_daily_volume'] = monitor.avg_daily_volume
                    print(
                        f"[SCANNER] {symbol} Float: {monitor.float_shares/1e6:.1f}M, "
                        f"Avg Vol: {format_compact_volume(monitor.avg_daily_volume)}"
                    )
                except Exception as e:
                    print(f"[SCANNER] Error parsing fundamentals for {symbol}: {e}")
            
            # Fallback: Calculate avg volume from historical data if not available from fundamentals
            if monitor.avg_daily_volume is None:
                print(f"[SCANNER] {symbol} Fundamental data unavailable, calculating from historical data...")
                avg_vol = tws_app.fetch_avg_daily_volume(symbol, days=10)
                if avg_vol:
                    monitor.avg_daily_volume = avg_vol
                    # Update TWS app data for volume correction reference
                    with tws_app.lock:
                        if symbol in tws_app.realtime_data:
                            tws_app.realtime_data[symbol]['avg_daily_volume'] = avg_vol
                    print(f"[SCANNER] {symbol} Avg Vol (10-day): {format_compact_volume(avg_vol)}")
                else:
                    print(f"[SCANNER] {symbol} Could not calculate avg volume")

    def _load_symbol_news(self, symbol: str, monitor, tws_app, force_refresh: bool = False):
        """Load and classify same-day IBKR headlines for one symbol."""
        if not NEWS_CATALYST_ENABLED:
            monitor.has_meaningful_news_today = False
            monitor.news_headlines_today = []
            monitor.news_match_reason = "News catalyst disabled in config"
            return

        raw_headlines = tws_app.fetch_today_news_headlines(
            symbol,
            max_results=NEWS_CATALYST_MAX_HEADLINES,
            force_refresh=force_refresh,
        )
        et_tz = pytz.timezone('US/Eastern')
        today_et = datetime.now(et_tz).date()
        same_day_headlines = []
        match_reason = "No same-day headlines"
        has_meaningful_news = False

        for item in raw_headlines:
            headline_dt = parse_ibkr_news_timestamp(item.get("time", ""))
            if headline_dt is None:
                continue
            if headline_dt.astimezone(et_tz).date() != today_et:
                continue

            same_day_headlines.append(item)
            is_meaningful, reason = classify_news_headline(item.get("headline", ""))
            if is_meaningful and not has_meaningful_news:
                has_meaningful_news = True
                match_reason = f"{reason} | {item.get('headline', '')}"

        if not same_day_headlines and raw_headlines:
            match_reason = "Fetched headlines, but none were from today"
        elif same_day_headlines and not has_meaningful_news:
            match_reason = "Same-day headlines found, but none matched cautious catalyst rules"

        monitor.news_headlines_today = same_day_headlines
        monitor.has_meaningful_news_today = has_meaningful_news
        monitor.news_match_reason = match_reason

    def load_news(self, tws_app, force_refresh: bool = False):
        """Load cautious headline-based news catalyst data for all monitored symbols."""
        action = "Refreshing" if force_refresh else "Loading"
        print(f"[SCANNER] {action} news headlines for catalyst scoring...")
        for symbol, monitor in self.monitors.items():
            try:
                self._load_symbol_news(symbol, monitor, tws_app, force_refresh=force_refresh)
            except Exception as e:
                monitor.has_meaningful_news_today = False
                monitor.news_headlines_today = []
                monitor.news_match_reason = f"News load failed: {e}"
                print(f"[SCANNER] Error loading news for {symbol}: {e}")
    
    def load_previous_closes(self, tws_app):
        """Load previous day's closing prices for all symbols"""
        print("[SCANNER] Loading previous day's closing prices...")
        for symbol, monitor in self.monitors.items():
            try:
                # Use a small historical data request to get the last close
                close_price = tws_app.fetch_last_close(symbol)
                if close_price:
                    monitor.day_start_price = close_price
                    print(f"[SCANNER] {symbol} Prev Close: ${close_price:.2f}")
            except Exception as e:
                print(f"[SCANNER] Error loading prev close for {symbol}: {e}")

    def load_historical_prices(self, tws_app):
        """Load recent historical prices to populate history for squeeze detection"""
        print("[SCANNER] Loading historical intraday data for squeeze detection...")
        for symbol, monitor in self.monitors.items():
            try:
                # Request last 15 minutes of 1-minute bars
                bars = tws_app.fetch_historical_bars(symbol, duration="15 M", bar_size="1 min")
                for bar in bars:
                    # Warm only the price history. Volume history is live cumulative volume,
                    # so keep it live-only for session accounting and warm signal math separately.
                    monitor.price_history.append((bar.date, bar.close))
                monitor.seed_signal_volume_history_from_bars(bars)
                if bars:
                    print(f"[SCANNER] {symbol} Loaded {len(bars)} historical bars")
            except Exception as e:
                print(f"[SCANNER] Error loading history for {symbol}: {e}")

    def resync_vwap_all_symbols(self, tws_app):
        """Resync VWAP for all symbols from TWS"""
        print("[SCANNER] Resyncing VWAP for all symbols...")
        for symbol, monitor in self.monitors.items():
            try:
                vwap = tws_app.fetch_current_vwap(symbol)
                if vwap:
                    monitor.vwap = vwap
            except Exception as e:
                print(f"[SCANNER] Error resyncing VWAP for {symbol}: {e}")

def display_broad_screening(scanner: RealtimeBroadScanner):
    """Console display for the broad screening tool"""
    # Clear screen
    if platform.system() == "Windows":
        os.system('cls')
    else:
        os.system('clear')
        
    print("="*125)
    print(f"ROSS CAMERON-STYLE BROAD SCANNER | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Session: {get_market_session()}")
    print("="*125)
    print(f"{'SYMBOL':<8} | {'PRICE':<8} | {'VWAP':<8} | {'GAIN%':<8} | {'VOL':<10} | {'REL VOL':<8} | {'GRADE':<7} | {'TRIGGERED CONDITIONS'}")
    print("-"*125)
    
    # Sort symbols by gain percentage (decreasing order)
    def get_gain_pct(monitor):
        if monitor.price_history and monitor.day_start_price and monitor.day_start_price > 0:
            price = monitor.price_history[-1][1]
            return ((price - monitor.day_start_price) / monitor.day_start_price) * 100
        return 0.0
    
    sorted_monitors = sorted(scanner.monitors.items(), 
                           key=lambda x: get_gain_pct(x[1]), 
                           reverse=True)
    
    for symbol, monitor in sorted_monitors[:20]:  # Show top 20
        if not monitor.price_history:
            continue
            
        price = monitor.price_history[-1][1]
        vwap = monitor.vwap
        
        # Get current volume
        volume = monitor.volume_history[-1][1] if monitor.volume_history else 0
        
        vol_str = format_compact_volume(volume)
        
        gain_pct = 0.0
        if monitor.day_start_price and monitor.day_start_price > 0:
            gain_pct = ((price - monitor.day_start_price) / monitor.day_start_price) * 100
            
        rel_vol = monitor.relative_volume
        rel_vol_str = f"{rel_vol:>7.2f}x"
        if rel_vol == 0 and monitor.rel_vol_1m > 0:
            rel_vol_str = f"{monitor.rel_vol_1m:>7.2f}m" # 'm' indicates 1-minute fallback
            
        conditions = ", ".join(monitor.triggered_conditions) if monitor.triggered_conditions else ""
        
        # Highlight if conditions met
        if monitor.triggered_conditions:
            grade_str = f"{monitor.alert_grade} ({monitor.alert_score})"
            print(f"\033[92m{symbol:<8} | ${price:<7.2f} | ${vwap:<7.2f} | {gain_pct:>7.2f}% | {vol_str:>10} | {rel_vol_str} | {grade_str:<7} | {conditions}\033[0m")
        else:
            grade_str = f"{monitor.alert_grade} ({monitor.alert_score})" if monitor.alert_score else "-"
            print(f"{symbol:<8} | ${price:<7.2f} | ${vwap:<7.2f} | {gain_pct:>7.2f}% | {vol_str:>10} | {rel_vol_str} | {grade_str:<7} | {conditions}")

    print("\n" + "="*125)
    print("RECENT ALERTS:")
    # Show last 10 alerts instead of 5
    for timestamp, symbol, reasons, grade, score in list(scanner.recent_alerts)[-10:]:
        print(f"[{timestamp.strftime('%H:%M:%S')}] {symbol} [{grade} {score}]: {', '.join(reasons)}")


def build_scanner_runtime_state(scanner: RealtimeBroadScanner) -> Dict:
    def get_gain_pct(monitor) -> float:
        if monitor.price_history and monitor.day_start_price and monitor.day_start_price > 0:
            price = monitor.price_history[-1][1]
            return ((price - monitor.day_start_price) / monitor.day_start_price) * 100
        return 0.0

    sorted_monitors = sorted(
        scanner.monitors.items(),
        key=lambda item: get_gain_pct(item[1]),
        reverse=True,
    )
    top_symbols = []
    for symbol, monitor in sorted_monitors[:RUNTIME_FEEDBACK_TOP_SYMBOLS]:
        top_symbols.append({
            "symbol": symbol,
            "price": monitor.price_history[-1][1] if monitor.price_history else None,
            "vwap": monitor.vwap,
            "gain_pct": get_gain_pct(monitor),
            "relative_volume": monitor.relative_volume,
            "alert_grade": monitor.alert_grade,
            "alert_score": monitor.alert_score,
            "triggered_conditions": list(monitor.triggered_conditions),
            "preliminary_block_reason": monitor.preliminary_block_reason,
        })

    return {
        "session": get_market_session(),
        "monitored_symbols": sorted(list(scanner.monitors.keys())),
        "top_symbols": top_symbols,
        "recent_alerts": [
            {
                "timestamp": timestamp,
                "symbol": symbol,
                "reasons": list(reasons),
                "grade": grade,
                "score": score,
            }
            for timestamp, symbol, reasons, grade, score in list(scanner.recent_alerts)[-10:]
        ],
    }

def _get_monitor_gain_pct(monitor) -> float:
    if monitor.price_history and monitor.day_start_price and monitor.day_start_price > 0:
        price = monitor.price_history[-1][1]
        return ((price - monitor.day_start_price) / monitor.day_start_price) * 100
    return 0.0

def update_scanner_symbols(
    scanner: RealtimeBroadScanner,
    tws_app,
    current_symbols: List[str],
    market_data_callback: Optional[Callable] = None,
    protected_symbols: Optional[List[str]] = None,
    excluded_symbols: Optional[List[str]] = None,
    refresh_all_news: bool = False,
    max_symbols: int = SCANNER_MONITOR_CAP,
    max_symbol_changes: int = SCANNER_MAX_SYMBOL_CHANGES_PER_REFRESH,
) -> List[str]:
    """Fetch new top gainers and update the scanner's monitor list"""
    print("\n[SCANNER] Updating top gainers list...")
    new_symbols = get_top_gainers(top_n=max_symbols, use_ibkr=True, ibkr_port=TWS_PORT, force_refresh=True)
    ranked_new = list(dict.fromkeys(new_symbols))
    protected = set(protected_symbols or [])
    excluded = set(excluded_symbols or [])

    protected_kept = [s for s in current_symbols if s in protected and s not in excluded]
    current_regular = [s for s in current_symbols if s not in protected and s not in excluded]
    ranked_candidates = [s for s in ranked_new if s not in excluded and s not in protected]
    regular_capacity = max(0, max_symbols - len(protected_kept))
    target_regular = ranked_candidates[:regular_capacity]

    add_candidates = [s for s in target_regular if s not in current_regular]
    additions = []
    removals = []

    # If we have free regular slots, fill them first without forcing swaps.
    available_slots = max(0, regular_capacity - len(current_regular))
    if available_slots > 0 and add_candidates:
        fill_count = min(max_symbol_changes, available_slots, len(add_candidates))
        additions.extend(add_candidates[:fill_count])
        add_candidates = add_candidates[fill_count:]

    # Once regular slots are full, admit only a limited number of stronger newcomers
    # and evict the weakest currently monitored regular symbols.
    swap_budget = max(0, max_symbol_changes - len(additions))
    if swap_budget > 0 and add_candidates and current_regular:
        removable_pool = sorted(current_regular, key=lambda symbol: _get_monitor_gain_pct(scanner.monitors[symbol]))
        swap_count = min(swap_budget, len(add_candidates), len(removable_pool))
        removals.extend(removable_pool[:swap_count])
        additions.extend(add_candidates[:swap_count])

    # Enforce the configured cap even if the current list somehow drifted above it.
    overflow = max(0, len(current_regular) - len(removals) + len(additions) - regular_capacity)
    if overflow > 0:
        removable_pool = [
            symbol for symbol in sorted(current_regular, key=lambda s: _get_monitor_gain_pct(scanner.monitors[s]))
            if symbol not in removals
        ]
        removals.extend(removable_pool[:overflow])

    removed = [s for s in current_symbols if s in set(removals) or s in excluded]
    if removed:
        print(f"[SCANNER] Removing {len(removed)} symbols from monitor: {', '.join(sorted(removed))}")
        for s in removed:
            tws_app.unsubscribe_realtime_data(s)
            scanner.monitors.pop(s, None)
            if s in scanner.symbols:
                scanner.symbols.remove(s)

    added = [s for s in additions if s not in current_symbols]
    
    if added:
        print(f"[SCANNER] Adding {len(added)} new symbols to monitor: {', '.join(added)}")
        for s in added:
            scanner.monitors[s] = RealtimeSymbolMonitor(s)
            if s not in scanner.symbols:
                scanner.symbols.append(s)
            # Load fundamentals for new symbol
            xml_data = tws_app.fetch_fundamental_data(s)
            if xml_data:
                try:
                    root = ET.fromstring(xml_data)
                    for ratio in root.findall(".//Ratio"):
                        field = ratio.get("FieldName")
                        if field == 'FLOAT':
                            scanner.monitors[s].float_shares = float(ratio.text)
                        elif field == 'VOL10DAVG':
                            scanner.monitors[s].avg_daily_volume = tws_app.normalize_stock_volume(ratio.text)
                except: pass
            
            # Fallback for avg volume if fundamental data not available
            if scanner.monitors[s].avg_daily_volume is None:
                avg_vol = tws_app.fetch_avg_daily_volume(s, days=10)
                if avg_vol:
                    scanner.monitors[s].avg_daily_volume = avg_vol

            scanner._load_symbol_news(s, scanner.monitors[s], tws_app, force_refresh=True)
            
            # Load prev close
            close = tws_app.fetch_last_close(s)
            if close: scanner.monitors[s].day_start_price = close
            
            # Subscribe to data
            if market_data_callback is not None:
                tws_app.subscribe_market_data(s, market_data_callback(s))
            else:
                tws_app.subscribe_market_data(s, lambda sym, p, v, vw, ts, b, a: scanner.update(sym, p, v, vw, b, a))

    if NEWS_CATALYST_ENABLED and refresh_all_news:
        scanner.load_news(tws_app, force_refresh=True)

    remaining_regular = [
        symbol for symbol in current_regular
        if symbol not in removed and symbol not in excluded
    ]
    final_regular = [s for s in target_regular if s in remaining_regular or s in additions]
    final_regular.extend(
        s for s in remaining_regular
        if s not in final_regular
    )
    return protected_kept + final_regular[:regular_capacity]

def send_discord_alert(symbol, session, reasons, monitor):
    """Send a simple text alert to Discord with price and volume info."""
    if not DISCORD_WEBHOOK_URL:
        return

    reason_str = ", ".join(reasons) if reasons else "Unknown condition"
    
    # Format price and volume information
    price = monitor.price_history[-1][1] if monitor.price_history else 0.0
    vwap = monitor.vwap
    rel_vol = monitor.relative_volume
    session_vol = monitor.session_volume
    
    # Calculate gain from day start if available
    gain_pct = 0.0
    if monitor.day_start_price and monitor.day_start_price > 0:
        gain_pct = ((price - monitor.day_start_price) / monitor.day_start_price) * 100

    # Put the most important fields first so iPhone banner notifications
    # show grade, ticker, price, and daily gain before the rest.
    alert_icon = "🚀🚀🚀🚀" if monitor.alert_grade == "A+" else "🚀🚀🚀" if monitor.alert_grade == "A" else "🚀🚀" if monitor.alert_grade == "B" else "🚀"
    grade_label = f"Grade {monitor.alert_grade} ({monitor.alert_score})"
    message = (
        f"{alert_icon} {grade_label} | {symbol} | ${price:.2f} | {gain_pct:+.2f}%\n"
        f"{session} | Price ${price:.2f} | VWAP ${vwap:.2f} | Trigger: {reason_str}\n"
        f"{session} | RelVol: {rel_vol:.2f}x | Vol: {session_vol:,.0f}"
    )
    if monitor.alert_score_reasons:
        message += f"\nScore factors: {', '.join(monitor.alert_score_reasons)}"

    try:
        payload = {"content": message}
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        if response.status_code != 204:
            print(f"[WARNING] Discord alert failed with status {response.status_code}: {response.text}")
    except Exception as e:
        print(f"[WARNING] Error sending Discord alert: {e}")


def build_voice_announcement(symbol: str, session: str) -> str:
    session_label = session.replace("_", " ").title()
    spelled_symbol = " ".join(list(symbol.upper()))
    return f"{session_label} alert. {spelled_symbol}."

def run_standalone_scanner():
    telemetry = RuntimeTelemetry(component="scanner", base_dir=RUNTIME_FEEDBACK_DIR)
    # Setup TWS App
    from tws_data_fetcher import create_tws_data_app
    client_id = int(os.getenv("SCANNER_TWS_CLIENT_ID", "10"))
    print(f"[INIT] Connecting to TWS on port {TWS_PORT} with client ID {client_id}...")
    tws_app = create_tws_data_app("127.0.0.1", TWS_PORT, client_id=client_id)
    
    if not tws_app:
        print(f"[ERROR] Could not connect to TWS on port {TWS_PORT}.")
        print("[DEBUG] 1. Check if TWS or IB Gateway is running.")
        print(f"[DEBUG] 2. Check if the port in TWS matches {TWS_PORT} (Global Configuration -> API -> Settings).")
        print("[DEBUG] 3. Ensure 'Enable ActiveX and Socket Clients' is checked in TWS API settings.")
        print("[DEBUG] 4. If you are using Paper Trading, use port 7497. If Live, use 7496.")
        return
    
    # Wait for connection
    time.sleep(2)
    
    # Get dynamic top gainers list
    print("[INIT] Fetching top gainers list...")
    # Use IBKR scanner for most accurate gainers data
    SYMBOLS = get_top_gainers(
        top_n=SCANNER_MONITOR_CAP,
        use_ibkr=True,
        ibkr_port=TWS_PORT,
        force_refresh=True,
    )
    unique_symbols = list(dict.fromkeys(SYMBOLS))
    print(f"[INIT] Monitoring {len(unique_symbols)} symbols: {', '.join(unique_symbols[:10])}{'...' if len(unique_symbols) > 10 else ''}")
    telemetry.log_event(
        "scanner_started",
        symbols=unique_symbols,
        client_id=client_id,
        runtime_dir=telemetry.run_dir,
    )
    
    print("[INIT] Connecting to TWS for standalone scanner...")
    print(f"[INIT] Current market session: {get_market_session()}")
    
    scanner = RealtimeBroadScanner(unique_symbols)
    initialize_alert_score_audit_file()
    print(f"[INIT] Score audit log: {ALERT_SCORE_AUDIT_FILE}")

    def alert_handler(symbol, timestamp, reasons, monitor):
        session = get_market_session()
        voice_reason = reasons[0] if reasons else "Condition met"
        if monitor.alert_is_suppressed:
            alert_msg = f"{session} Below-threshold {symbol} triggered {voice_reason} [{monitor.alert_grade} {monitor.alert_score}]"
            print(f"[INFO] {alert_msg}")
        else:
            alert_msg = f"{session} Alert! {symbol} triggered {voice_reason}"
            print(f"[ALERT] {alert_msg}")
        telemetry.log_event(
            "scanner_alert",
            symbol=symbol,
            session=session,
            reasons=list(reasons),
            alert_grade=monitor.alert_grade,
            alert_score=monitor.alert_score,
            suppressed=monitor.alert_is_suppressed,
            price=monitor.price_history[-1][1] if monitor.price_history else None,
            vwap=monitor.vwap,
            relative_volume=monitor.relative_volume,
        )

        # Append a per-alert scoring breakdown for tuning and diagnostics.
        append_alert_score_audit(symbol, timestamp, session, reasons, monitor)

        if monitor.alert_is_suppressed:
            return

        # Send Discord Alert
        send_discord_alert(symbol, session, reasons, monitor)
        
        # Voice announcement - platform-specific
        try:
            voice_text = build_voice_announcement(symbol, session)
            if platform.system() == "Windows":
                # Windows: Use PowerShell with SAPI
                import subprocess
                escaped_text = voice_text.replace('"', '`"')
                ps_script = (
                    "Add-Type -AssemblyName System.Speech; "
                    "$synth = New-Object System.Speech.Synthesis.SpeechSynthesizer; "
                    f'$synth.Speak("{escaped_text}")'
                )
                subprocess.run(["powershell", "-Command", ps_script], 
                             capture_output=True, timeout=5)
            else:
                # Linux/Mac: Use espeak
                os.system(f'espeak \"{voice_text}\" 2>/dev/null')
        except Exception as e:
            print(f"[WARNING] Voice announcement failed: {e}")

    scanner.on_preliminary_alert(alert_handler)
    
    # Load Fundamentals
    scanner.load_fundamentals(tws_app)

    # Load cautious news headlines for catalyst scoring
    scanner.load_news(tws_app)
    
    # Load Previous Day's Closing Prices
    scanner.load_previous_closes(tws_app)
    
    # Load Historical Intraday Prices (for squeeze detection)
    scanner.load_historical_prices(tws_app)

    # Subscribe to Live Data
    print("[INIT] Subscribing to live market data (including extended hours)...")
    def create_callback(sym):
        return lambda s, p, v, vw, ts, b, a: scanner.update(s, price=p, volume=v, vwap=vw, bid=b, ask=a)

    for symbol in unique_symbols:
        tws_app.subscribe_market_data(symbol, create_callback(symbol))
    
    print("[INIT] Starting Standalone Scanner Interface...")
    time.sleep(2)
    
    # Session transition check interval
    last_session_check = datetime.now()
    et_tz = pytz.timezone('US/Eastern')
    next_symbol_update = datetime.now(et_tz) + timedelta(seconds=SCANNER_REFRESH_INTERVAL_SECONDS)
    next_news_refresh = datetime.now(et_tz) + timedelta(seconds=SCANNER_NEWS_REFRESH_INTERVAL_SECONDS)
    
    try:
        while True:
            # Check for session transitions every 60 seconds
            if (datetime.now() - last_session_check).total_seconds() > 60:
                if scanner.check_session_transition():
                    scanner.resync_vwap_all_symbols(tws_app)
                last_session_check = datetime.now()
            
            # Refresh the scanner list on the shared scanner cadence.
            if datetime.now(et_tz) >= next_symbol_update:
                refresh_all_news = datetime.now(et_tz) >= next_news_refresh
                unique_symbols = update_scanner_symbols(
                    scanner,
                    tws_app,
                    unique_symbols,
                    refresh_all_news=refresh_all_news,
                    max_symbols=SCANNER_MONITOR_CAP,
                    max_symbol_changes=SCANNER_MAX_SYMBOL_CHANGES_PER_REFRESH,
                )
                next_symbol_update = datetime.now(et_tz) + timedelta(seconds=SCANNER_REFRESH_INTERVAL_SECONDS)
                if refresh_all_news:
                    next_news_refresh = datetime.now(et_tz) + timedelta(seconds=SCANNER_NEWS_REFRESH_INTERVAL_SECONDS)
            
            telemetry.write_state(build_scanner_runtime_state(scanner))
            display_broad_screening(scanner)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[INFO] Scanner stopped.")
    finally:
        telemetry.log_event("scanner_stopped")
        tws_app.disconnect()

if __name__ == "__main__":
    run_standalone_scanner()

"""
Real-time Broad Screening Tool - PREMARKET ENHANCED
Ross Cameron-style preliminary screening based on togglable conditions.
Enhanced with market session awareness and premarket-specific features.
"""
import time
import os
from datetime import datetime, timedelta
from typing import List, Dict, Callable, Optional
from collections import deque
import xml.etree.ElementTree as ET
from conditions import MarketData, AlertConditionSet, PriceAboveVWAPCondition, SqueezeCondition, FastIgnitionCondition
import platform
import pytz
from scanner_config import (
    ALERT_MIN_SCORE_TO_NOTIFY,
    ALERT_DRAWDOWN_LOOKBACK_MINUTES,
    SQUEEZE_PCT_THRESHOLD,
    SQUEEZE_TIME_MINUTES,
    FAST_IGNITION_PCT_5S,
    FAST_IGNITION_PCT_15S,
    FAST_IGNITION_VOLUME_MULTIPLIER,
    FAST_IGNITION_MAX_RETRACEMENT_PCT,
    DISCORD_WEBHOOK_URL,
    TWS_PORT,
)
import requests
from top_gainers_fetcher import get_top_gainers
from alert_rating import calculate_alert_rating, get_breakout_debug_info, get_drawdown_debug_info

# Market session times (Eastern Time)
PREMARKET_START = (4, 0)   # 4:00 AM ET
MARKET_OPEN = (9, 30)      # 9:30 AM ET
MARKET_CLOSE = (16, 0)     # 4:00 PM ET
AFTERHOURS_END = (20, 0)   # 8:00 PM ET
ALERT_SCORE_AUDIT_FILE = os.path.join(os.path.dirname(__file__), "temp_alert_score_audit.log")


def initialize_alert_score_audit_file():
    """Initialize (truncate) the per-run score audit file in the project folder."""
    header = (
        "# Alert Score Audit Log (temp)\n"
        f"# Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        "# One block per triggered alert with scoring factor breakdown.\n\n"
    )
    with open(ALERT_SCORE_AUDIT_FILE, "w", encoding="utf-8") as f:
        f.write(header)


def append_alert_score_audit(symbol: str, timestamp: datetime, session: str, trigger_reasons: List[str], monitor):
    """Append one triggered alert with point-by-point factors to the temp audit file."""
    trigger_str = ", ".join(trigger_reasons) if trigger_reasons else "Unknown"
    factors = monitor.alert_score_reasons if monitor.alert_score_reasons else []
    breakout_debug = monitor.alert_breakout_debug_info if monitor.alert_breakout_debug_info else None
    drawdown_debug = monitor.alert_drawdown_debug_info if monitor.alert_drawdown_debug_info else None

    lines = [
        f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] {symbol} | Session={session} | Grade={monitor.alert_grade} | Score={monitor.alert_score}",
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

    lines.append("")

    with open(ALERT_SCORE_AUDIT_FILE, "a", encoding="utf-8") as f:
        f.write("\n".join(lines))

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
        self.alert_score = 0
        self.alert_grade = "C"
        self.alert_score_reasons = []
        self.alert_breakout_debug_info = None
        self.alert_drawdown_debug_info = None
        
        # Initialize Preliminary Condition Set (same thresholds for all sessions)
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

        self.confirmation_condition_set = AlertConditionSet("Preliminary")
        self.confirmation_condition_set.add_condition(PriceAboveVWAPCondition())
        self.confirmation_condition_set.add_condition(
            SqueezeCondition(pct_threshold=SQUEEZE_PCT_THRESHOLD, minutes=SQUEEZE_TIME_MINUTES)
        )

    def update_market_data(self, price: float, volume: float, vwap: float, bid: float = 0, ask: float = 0):
        now = datetime.now()
        current_session = get_market_session()
        
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
            volume_history=list(self.volume_history),
        )
        
        trigger_summary = None
        if self.fast_condition_set.check_all(data):
            trigger_summary = self.fast_condition_set.get_trigger_summary()
        elif self.confirmation_condition_set.check_all(data):
            trigger_summary = self.confirmation_condition_set.get_trigger_summary()

        if trigger_summary:
            # Check 60-second cooldown before triggering new alert
            if self.last_alert_time:
                seconds_since_last_alert = (self.last_update - self.last_alert_time).total_seconds()
                if seconds_since_last_alert < 60:
                    # Still in cooldown period, don't trigger new alert
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
            self.alert_score, self.alert_grade, self.alert_score_reasons = calculate_alert_rating(
                price_history=list(self.price_history),
                last_update=self.last_update,
                relative_volume=self.relative_volume,
                float_shares=self.float_shares,
                current_price=self.price_history[-1][1],
                squeeze_pct_threshold=SQUEEZE_PCT_THRESHOLD,
                squeeze_time_minutes=SQUEEZE_TIME_MINUTES,
                breakout_debug_info=self.alert_breakout_debug_info,
                max_intraday_1m_drawdown_pct=effective_max_drawdown,
                recent_green_1m_body_pcts=recent_green_1m_body_pcts,
            )
            if self.alert_score < ALERT_MIN_SCORE_TO_NOTIFY:
                self.triggered_conditions = []
                self.alert_score = 0
                self.alert_grade = "C"
                self.alert_score_reasons = []
                self.alert_breakout_debug_info = None
                self.alert_drawdown_debug_info = None
                return []
            self.triggered_conditions = [trigger_summary]
            self.last_alert_time = self.last_update
            return self.triggered_conditions
        else:
            # Keep triggered_conditions to show recent alerts, only clear after timeout
            if self.last_alert_time and (self.last_update - self.last_alert_time) > timedelta(minutes=5):
                self.triggered_conditions = []
                self.alert_score = 0
                self.alert_grade = "C"
                self.alert_score_reasons = []
                self.alert_breakout_debug_info = None
                self.alert_drawdown_debug_info = None
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
                            monitor.avg_daily_volume = float(ratio.text)
                            # Update TWS app data for volume correction reference
                            with tws_app.lock:
                                if symbol in tws_app.realtime_data:
                                    tws_app.realtime_data[symbol]['avg_daily_volume'] = monitor.avg_daily_volume
                    print(f"[SCANNER] {symbol} Float: {monitor.float_shares/1e6:.1f}M, Avg Vol: {monitor.avg_daily_volume/1e6:.1f}M")
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
                    print(f"[SCANNER] {symbol} Avg Vol (10-day): {avg_vol/1e6:.1f}M")
                else:
                    print(f"[SCANNER] {symbol} Could not calculate avg volume")
    
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
                    # Add bar data to monitor history
                    # We'll use the bar time and close price
                    monitor.price_history.append((bar.date, bar.close))
                    monitor.volume_history.append((bar.date, bar.volume))
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
        
        # Format volume for display (K = thousands, M = millions)
        if volume >= 1_000_000:
            vol_str = f"{volume/1_000_000:.2f}M"
        elif volume >= 1_000:
            vol_str = f"{volume/1_000:.1f}K"
        else:
            vol_str = f"{volume:.0f}"
        
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
    print("="*125)

def update_scanner_symbols(scanner: RealtimeBroadScanner, tws_app, current_symbols: List[str]) -> List[str]:
    """Fetch new top gainers and update the scanner's monitor list"""
    print("\n[SCANNER] Updating top gainers list...")
    new_symbols = get_top_gainers(top_n=20, use_ibkr=True, ibkr_port=TWS_PORT, force_refresh=True)
    unique_new = list(set(new_symbols))
    
    # Identify truly new symbols
    added = [s for s in unique_new if s not in current_symbols]
    
    if added:
        print(f"[SCANNER] Adding {len(added)} new symbols to monitor: {', '.join(added)}")
        for s in added:
            scanner.monitors[s] = RealtimeSymbolMonitor(s)
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
                            scanner.monitors[s].avg_daily_volume = float(ratio.text)
                except: pass
            
            # Fallback for avg volume if fundamental data not available
            if scanner.monitors[s].avg_daily_volume is None:
                avg_vol = tws_app.fetch_avg_daily_volume(s, days=10)
                if avg_vol:
                    scanner.monitors[s].avg_daily_volume = avg_vol
            
            # Load prev close
            close = tws_app.fetch_last_close(s)
            if close: scanner.monitors[s].day_start_price = close
            
            # Subscribe to data
            tws_app.subscribe_market_data(s, lambda sym, p, v, vw, ts, b, a: scanner.update(sym, p, v, vw, b, a))
            
    return list(set(current_symbols + unique_new))

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
    alert_icon = "🚀🚀🚀" if monitor.alert_grade == "A" else "🚀🚀" if monitor.alert_grade == "B" else "🚀"
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

def run_standalone_scanner():
    # Setup TWS App
    from tws_data_fetcher import create_tws_data_app
    client_id = int(os.getenv("SCANNER_TWS_CLIENT_ID", "11"))
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
    
    # Get dynamic top gainers list (updates every 10 minutes)
    print("[INIT] Fetching top gainers list...")
    # Use IBKR scanner for most accurate gainers data
    SYMBOLS = get_top_gainers(top_n=20, use_ibkr=True, ibkr_port=TWS_PORT, force_refresh=True)
    unique_symbols = list(set(SYMBOLS))
    print(f"[INIT] Monitoring {len(unique_symbols)} symbols: {', '.join(unique_symbols[:10])}{'...' if len(unique_symbols) > 10 else ''}")
    
    print("[INIT] Connecting to TWS for standalone scanner...")
    print(f"[INIT] Current market session: {get_market_session()}")
    
    scanner = RealtimeBroadScanner(unique_symbols)
    initialize_alert_score_audit_file()
    print(f"[INIT] Score audit log: {ALERT_SCORE_AUDIT_FILE}")

    def alert_handler(symbol, timestamp, reasons, monitor):
        session = get_market_session()
        voice_reason = reasons[0] if reasons else "Condition met"
        alert_msg = f"{session} Alert! {symbol} triggered {voice_reason}"
        print(f"[ALERT] {alert_msg}")

        # Append a per-alert scoring breakdown for tuning and diagnostics.
        append_alert_score_audit(symbol, timestamp, session, reasons, monitor)
        
        # Send Discord Alert
        send_discord_alert(symbol, session, reasons, monitor)
        
        # Voice announcement - platform-specific
        try:
            if platform.system() == "Windows":
                # Windows: Use PowerShell with SAPI
                import subprocess
                # Escape quotes for PowerShell
                ps_script = f'Add-Type -AssemblyName System.Speech; (New-Object System.Speech.Synthesis.SpeechSynthesizer).Speak("{symbol} {session} alert")'
                subprocess.run(["powershell", "-Command", ps_script], 
                             capture_output=True, timeout=5)
            else:
                # Linux/Mac: Use espeak
                os.system(f'espeak "{alert_msg}" 2>/dev/null')
        except Exception as e:
            print(f"[WARNING] Voice announcement failed: {e}")

    scanner.on_preliminary_alert(alert_handler)
    
    # Load Fundamentals
    scanner.load_fundamentals(tws_app)
    
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
    next_symbol_update = get_next_10_minute_mark()
    
    try:
        while True:
            # Check for session transitions every 60 seconds
            if (datetime.now() - last_session_check).total_seconds() > 60:
                if scanner.check_session_transition():
                    scanner.resync_vwap_all_symbols(tws_app)
                last_session_check = datetime.now()
            
            # Update the scanner list on wall-clock 10-minute boundaries.
            if datetime.now(pytz.timezone('US/Eastern')) >= next_symbol_update:
                unique_symbols = update_scanner_symbols(scanner, tws_app, unique_symbols)
                next_symbol_update = get_next_10_minute_mark()
            
            display_broad_screening(scanner)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[INFO] Scanner stopped.")
    finally:
        tws_app.disconnect()

if __name__ == "__main__":
    run_standalone_scanner()

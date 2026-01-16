"""
Alert Conditions Module
Defines base condition class and specific alert conditions for the scanner.
New conditions can be easily added by extending the AlertCondition class.

CENTRALIZED CONFIGURATION:
- PRICE_SURGE_THRESHOLD: Percentage change to trigger price surge alert
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any
from datetime import datetime, timedelta


# =============================================================================
# CENTRALIZED ALERT CONFIGURATION
# Configure these values to adjust alert sensitivity across all scanners
# =============================================================================

PRICE_SURGE_THRESHOLD = 2.0  # Percentage (e.g., 3.0 = 3% price increase)
VOLUME_SURGE_THRESHOLD = 5.0

# Two-Step Momentum Configuration
WINDOW_SEC = 5
THRESH_1 = 1.0
THRESH_2 = 1.5
MAX_SPREAD_PCT = 0.5

@dataclass
class MarketData:
    """Container for current market data"""
    symbol: str
    price: float
    volume: int
    vwap: float
    timestamp: datetime
    bid: float = 0.0
    ask: float = 0.0
    price_history: Dict[str, float] = None  # timestamp -> price
    volume_history: Dict[str, int] = None  # timestamp -> volume


class AlertCondition(ABC):
    """Base class for all alert conditions. Extend this to add new conditions."""
    
    def __init__(self, name: str):
        self.name = name
        self.triggered_reason = ""
    
    @abstractmethod
    def check(self, data: MarketData) -> bool:
        """
        Check if condition is met.
        
        Args:
            data: MarketData object with current market data
            
        Returns:
            bool: True if condition is triggered, False otherwise
        """
        pass
    
    def get_trigger_reason(self) -> str:
        """Return the reason why condition was triggered"""
        return self.triggered_reason


class PriceAboveVWAPCondition(AlertCondition):
    """Condition: Price is above VWAP"""
    
    def __init__(self):
        super().__init__("Price Above VWAP")
    
    def check(self, data: MarketData) -> bool:
        if data.price > data.vwap:
            self.triggered_reason = f"Price ${data.price:.2f} > VWAP ${data.vwap:.2f}"
            return True
        self.triggered_reason = ""
        return False


class PriceSurgeCondition(AlertCondition):
    """Condition: Huge surge in price in the last 10 seconds"""
    
    def __init__(self, surge_threshold: float = None):
        """
        Args:
            surge_threshold: Percentage increase threshold (uses PRICE_SURGE_THRESHOLD if None)
        """
        super().__init__("Price Surge (Last 10s)")
        self.surge_threshold = surge_threshold if surge_threshold is not None else PRICE_SURGE_THRESHOLD
        self.lookback_seconds = 10
    
    def check(self, data: MarketData) -> bool:
        if not data.price_history or len(data.price_history) < 2:
            self.triggered_reason = ""
            return False
        
        # Get prices from last 10 seconds
        cutoff_time = data.timestamp - timedelta(seconds=self.lookback_seconds)
        recent_prices = {
            ts: price for ts, price in data.price_history.items()
            if ts >= cutoff_time
        }
        
        if len(recent_prices) < 2:
            # Fallback for backtest (1-min bars)
            prices = sorted(data.price_history.items())
            if len(prices) >= 2:
                time_diff = (prices[-1][0] - prices[-2][0]).total_seconds()
                if time_diff >= 60:
                    p_now = data.price
                    p_prev = prices[-2][1]
                    pct_change = ((p_now - p_prev) / p_prev) * 100
                    if pct_change >= self.surge_threshold:
                        self.triggered_reason = f"Price surged {pct_change:.2f}% (1m fallback)"
                        return True
            
            self.triggered_reason = ""
            return False
        
        # Find lowest price in the window
        min_price = min(recent_prices.values())
        
        # Calculate percentage change
        if min_price == 0:
            self.triggered_reason = ""
            return False
        
        pct_change = ((data.price - min_price) / min_price) * 100
        
        if pct_change >= self.surge_threshold:
            self.triggered_reason = (
                f"Price surged {pct_change:.2f}% in last 10s "
                f"(${min_price:.2f} -> ${data.price:.2f})"
            )
            return True
        
        self.triggered_reason = ""
        return False


class VolumeSpike10sCondition(AlertCondition):
    """Condition: Current 10s volume > 5x average of past twenty 10s bars"""
    
    def __init__(self, spike_threshold: float = VOLUME_SURGE_THRESHOLD):
        """
        Args:
            spike_threshold: Volume multiplier threshold (default 5.0 = 5x)
        """
        super().__init__("Volume Spike (10s vs 20 bars)")
        self.spike_threshold = spike_threshold
    
    def check(self, data: MarketData) -> bool:
        if not data.volume_history or len(data.volume_history) < 21:
            self.triggered_reason = ""
            return False
        
        now = data.timestamp
        
        # Group volumes into 10-second windows
        ten_sec_windows = []
        sorted_times = sorted(data.volume_history.keys())
        
        current_window_start = None
        current_window_vol = 0
        
        for ts in sorted_times:
            vol = data.volume_history[ts]
            
            if current_window_start is None:
                current_window_start = ts
                current_window_vol = vol
            elif (ts - current_window_start).total_seconds() <= 10:
                current_window_vol += vol
            else:
                # Close current window and start new one
                ten_sec_windows.append(current_window_vol)
                current_window_start = ts
                current_window_vol = vol
        
        # Add the last window
        if current_window_vol > 0:
            ten_sec_windows.append(current_window_vol)
        
        # Need at least 21 windows (20 past + 1 current)
        if len(ten_sec_windows) < 21:
            self.triggered_reason = ""
            return False
        
        # Current 10s volume (most recent window)
        current_10s_vol = ten_sec_windows[-1]
        
        # Average of past 20 windows
        past_20_avg = sum(ten_sec_windows[-21:-1]) / 20
        
        if past_20_avg == 0:
            self.triggered_reason = ""
            return False
        
        ratio = current_10s_vol / past_20_avg
        
        if ratio >= self.spike_threshold:
            self.triggered_reason = (
                f"10s volume spike {ratio:.1f}x (current: {current_10s_vol:.0f} vs avg: {past_20_avg:.0f})"
            )
            return True
        
        self.triggered_reason = ""
        return False


class VolumeConfirmationCondition(AlertCondition):
    """Condition: Volume is sustained. Current 10s volume and previous 10s volume are both > 2x average."""
    
    def __init__(self, multiplier: float = 2.0):
        super().__init__("Volume Confirmation (Sustained)")
        self.multiplier = multiplier
    
    def check(self, data: MarketData) -> bool:
        if not data.volume_history or len(data.volume_history) < 22:
            return False
        
        sorted_vols = [v for k, v in sorted(data.volume_history.items())]
        current_vol = sorted_vols[-1]
        prev_vol = sorted_vols[-2]
        avg_vol = sum(sorted_vols[-22:-2]) / 20
        
        if avg_vol > 0 and current_vol > (avg_vol * self.multiplier) and prev_vol > (avg_vol * self.multiplier):
            self.triggered_reason = f"Sustained volume: Current {current_vol/avg_vol:.1f}x, Prev {prev_vol/avg_vol:.1f}x"
            return True
        return False


def passes_spread_filter(bid: float, ask: float, price: float) -> bool:
    """
    Check if the bid-ask spread is within acceptable limits.
    
    Args:
        bid: Current best bid price
        ask: Current best ask price
        price: Current last trade price
        
    Returns:
        bool: True if spread is <= MAX_SPREAD_PCT of price
    """
    if bid <= 0 or ask <= 0 or price <= 0:
        return True  # Default to True if data missing
        
    spread_pct = ((ask - bid) / price) * 100
    return spread_pct <= MAX_SPREAD_PCT


class TwoStepMomentumCondition(AlertCondition):
    """
    Condition: Two-step 10s momentum confirmation.
    r1 (t-20s to t-10s) >= t1 (0.7%)
    r2 (t-10s to t) >= t2 (0.9%)
    Current Price >= High of last 20s
    """
    
    def __init__(self, t1: float = THRESH_1, t2: float = THRESH_2, window: int = 10):
        super().__init__("Two-Step 10s Momentum")
        self.t1 = t1
        self.t2 = t2
        self.window = window
        self.logic_used = "None"
        
    def check(self, data: MarketData) -> bool:
        if not data.price_history or len(data.price_history) < 2:
            return False
            
        now = data.timestamp
        t_minus_10 = now - timedelta(seconds=self.window)
        t_minus_20 = now - timedelta(seconds=self.window * 2)
        
        # Get prices at key intervals
        prices = sorted(data.price_history.items())
        
        def get_price_at(target_ts, exact_match_only=False):
            # Find closest price at or before target_ts
            for ts, p in reversed(prices):
                if exact_match_only:
                    if abs((ts - target_ts).total_seconds()) < 1.0:
                        return p
                else:
                    if ts <= target_ts:
                        return p
            return None

        p_now = data.price
        
        # Aggression Filter: max return between consecutive data points in last 20s >= 0.5%
        max_spike_ret = 0.0
        recent_prices = [p for ts, p in prices if ts >= t_minus_20]
        
        for i in range(1, len(recent_prices)):
            ret = ((recent_prices[i] - recent_prices[i-1]) / recent_prices[i-1]) * 100
            max_spike_ret = max(max_spike_ret, ret)
        
        # Apply filter: at least one jump must be >= 0.5%
        # Note: In backtest (1-min bars), we skip this to avoid data resolution issues.
        # We check the time difference between the last two data points to detect backtest mode
        is_backtest = False
        if len(prices) >= 2:
            time_diff = (prices[-1][0] - prices[-2][0]).total_seconds()
            if time_diff >= 60:  # 1-minute bars or higher
                is_backtest = True

        if not is_backtest:
            # RELAXED FOR TESTING: Reduced from 0.5 to 0.05
            if max_spike_ret < 0.05:
                return False
        
        # For logging
        self.max_spike_ret = max_spike_ret

        # Rolling 20s High (excluding current price)
        prev_prices = [p for ts, p in prices if ts < now and ts >= t_minus_20]
        high_20s = max(prev_prices) if prev_prices else p_now

        # Try to find prices at 10s and 20s marks
        p_10 = get_price_at(t_minus_10, exact_match_only=False)
        p_20 = get_price_at(t_minus_20, exact_match_only=False)

        # High-resolution logic (5s/10s)
        if p_10 is not None and p_20 is not None and p_10 != p_now and p_20 != p_10:
            r1 = ((p_10 - p_20) / p_20) * 100
            r2 = ((p_now - p_10) / p_10) * 100
            
            if r1 >= self.t1 and r2 >= self.t2 and p_now >= high_20s:
                self.logic_used = "10s+10s"
                self.triggered_reason = f"Momentum: r1={r1:.2f}%, r2={r2:.2f}% | High20s: ${high_20s:.2f} | Aggression: {self.max_spike_ret:.2f}%"
                return True
        
        # Fallback for backtest (1-min bars): Check return from previous bar
        # Only use fallback if high-resolution logic didn't trigger
        if is_backtest and len(prices) >= 2:
            p_prev = prices[-2][1]
            ret = ((p_now - p_prev) / p_prev) * 100
            if ret >= (self.t1 + self.t2): # Combined threshold for 1-min bar
                self.logic_used = "1-min fallback"
                self.triggered_reason = f"Momentum (1m): ret={ret:.2f}%"
                return True
            
        return False


class AlertConditionSet:
    """Container for multiple conditions with AND logic"""
    
    def __init__(self, name: str):
        self.name = name
        self.conditions: list[AlertCondition] = []
        self.triggered_reasons: list[str] = []
    
    def add_condition(self, condition: AlertCondition) -> 'AlertConditionSet':
        """Add a condition to the set. Returns self for chaining."""
        self.conditions.append(condition)
        return self
    
    def check_all(self, data: MarketData) -> bool:
        """
        Check if conditions are met.
        Uses AND logic for mandatory filters (VWAP, Spread) 
        and OR logic for the specific alert conditions (Momentum, Surge, etc.)
        
        Args:
            data: MarketData object
            
        Returns:
            bool: True if mandatory filters pass AND at least one alert condition triggers
        """
        self.triggered_reasons = []
        
        # MANDATORY FILTER 1: Price must be above VWAP for any alert to trigger
        vwap_cond = PriceAboveVWAPCondition()
        if not vwap_cond.check(data):
            return False
            
        # MANDATORY FILTER 2: Spread filter
        if not passes_spread_filter(data.bid, data.ask, data.price):
            return False
            
        # Check alert conditions (OR logic: trigger if ANY of these are met)
        any_triggered = False
        for condition in self.conditions:
            # Skip if it's already the VWAP condition (to avoid double checking)
            if isinstance(condition, PriceAboveVWAPCondition):
                continue
                
            if condition.check(data):
                self.triggered_reasons.append(condition.get_trigger_reason())
                any_triggered = True
        
        # If at least one condition triggered, add VWAP reason and return True
        if any_triggered:
            self.triggered_reasons.insert(0, vwap_cond.get_trigger_reason())
            return True
            
        return False
    
    def get_trigger_summary(self) -> str:
        """Get summary of all triggered conditions"""
        return " | ".join(self.triggered_reasons)

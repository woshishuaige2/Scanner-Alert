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
THRESH_1 = 0.7
THRESH_2 = 0.9
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
    Condition: Two-step momentum confirmation.
    r1 (t-10s to t-5s) >= t1
    r2 (t-5s to t) >= t2
    Current Price >= High of last 10s
    """
    
    def __init__(self, t1: float = THRESH_1, t2: float = THRESH_2, window: int = WINDOW_SEC):
        super().__init__("Two-Step Momentum")
        self.t1 = t1
        self.t2 = t2
        self.window = window
        self.logic_used = "None"
        
    def check(self, data: MarketData) -> bool:
        if not data.price_history or len(data.price_history) < 2:
            return False
            
        now = data.timestamp
        t_minus_5 = now - timedelta(seconds=self.window)
        t_minus_10 = now - timedelta(seconds=self.window * 2)
        
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
        
        # Aggression Filter: max 1s return in last 10s >= 0.5%
        # This targets actual aggressive buying rather than slow grinds.
        # Note: In backtest with 10s bars, we skip this to avoid data resolution issues.
        max_1s_ret = 0.0
        recent_prices = [p for ts, p in prices if ts >= t_minus_10]
        is_backtest = len(recent_prices) < 5 # Real-time has ~10 points in 10s
        
        if not is_backtest:
            for i in range(1, len(recent_prices)):
                ret = ((recent_prices[i] - recent_prices[i-1]) / recent_prices[i-1]) * 100
                max_1s_ret = max(max_1s_ret, ret)
            
            if max_1s_ret < 0.5:
                return False

        # Try to find exact 5s and 10s marks for high-resolution data (Real-time)
        p_5_exact = get_price_at(t_minus_5, exact_match_only=True)
        p_10_exact = get_price_at(t_minus_10, exact_match_only=True)
        
        # Rolling 10s High (excluding current price)
        prev_prices = [p for ts, p in prices if ts < now and ts >= t_minus_10]
        high_10s = max(prev_prices) if prev_prices else p_now

        if p_5_exact is not None and p_10_exact is not None:
            r1 = ((p_5_exact - p_10_exact) / p_10_exact) * 100
            r2 = ((p_now - p_5_exact) / p_5_exact) * 100
            
            if r1 >= self.t1 and r2 >= self.t2 and p_now >= high_10s:
                self.logic_used = "5s+5s"
                self.triggered_reason = f"Momentum: r1={r1:.2f}%, r2={r2:.2f}% | High10s: ${high_10s:.2f} | Aggression: {max_1s_ret:.2f}%"
                return True
        
        # Fallback for lower resolution data (Backtest 10s bars)
        p_10_any = get_price_at(t_minus_10, exact_match_only=False)
        if p_10_any is not None:
            total_r = ((p_now - p_10_any) / p_10_any) * 100
            # Combined threshold (0.7 + 0.9 = 1.6%)
            if total_r >= (self.t1 + self.t2) and p_now >= high_10s:
                self.logic_used = "10s Fallback"
                self.triggered_reason = f"10s Momentum: {total_r:.2f}% (Combined) | Aggression: {max_1s_ret:.2f}%"
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
        Check if ALL conditions are met.
        
        Args:
            data: MarketData object
            
        Returns:
            bool: True only if all conditions are triggered
        """
        self.triggered_reasons = []
        
        # MANDATORY: Price must be above VWAP for any alert to trigger
        vwap_cond = PriceAboveVWAPCondition()
        if not vwap_cond.check(data):
            return False
            
        # MANDATORY: Spread filter
        if not passes_spread_filter(data.bid, data.ask, data.price):
            return False
            
        # Check all other conditions in the set
        for condition in self.conditions:
            # Skip if it's already the VWAP condition (to avoid double checking)
            if isinstance(condition, PriceAboveVWAPCondition):
                continue
                
            if condition.check(data):
                self.triggered_reasons.append(condition.get_trigger_reason())
            else:
                # print(f"[DEBUG] {data.symbol} @ {data.timestamp.strftime('%H:%M:%S')} failed {condition.name}")
                return False
        
        # Add VWAP reason at the beginning if other conditions also met
        if self.triggered_reasons:
            self.triggered_reasons.insert(0, vwap_cond.get_trigger_reason())
            return True
            
        return False
    
    def get_trigger_summary(self) -> str:
        """Get summary of all triggered conditions"""
        return " | ".join(self.triggered_reasons)

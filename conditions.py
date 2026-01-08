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
THRESH_1 = 0.7  # 0.7% for first window
THRESH_2 = 0.9  # 0.9% for second window (stronger)
MAX_SPREAD_PCT = 0.5  # Maximum allowed spread as percentage of price


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
        # If VWAP is 0, we treat it as "not available" and allow the trade
        # to avoid blocking trades when TWS doesn't provide VWAP
        if data.vwap <= 0:
            self.triggered_reason = "VWAP N/A (0.0)"
            return True
            
        if data.price > data.vwap:
            self.triggered_reason = f"Price ${data.price:.2f} > VWAP ${data.vwap:.2f}"
            return True
        self.triggered_reason = ""
        return False


class TwoStepMomentumCondition(AlertCondition):
    """
    Condition: Two-step confirmation across two consecutive 5-second windows.
    r1 (t-10s to t-5s) >= 0.7%
    r2 (t-5s to t) >= 0.9%
    Also requires current price >= high of the last 10 seconds.
    """
    
    def __init__(self, t1: float = THRESH_1, t2: float = THRESH_2, window: int = WINDOW_SEC):
        super().__init__("Two-Step Momentum")
        self.t1 = t1
        self.t2 = t2
        self.window = window
    
    def check(self, data: MarketData) -> bool:
        if not data.price_history or len(data.price_history) < 2:
            return False
            
        now = data.timestamp
        
        # Define windows
        w1_start = now - timedelta(seconds=self.window * 2)
        w1_end = now - timedelta(seconds=self.window)
        w2_start = w1_end
        w2_end = now
        
        # Get prices for each window
        # Use a small buffer for timestamp comparison to handle floating point/sampling issues
        buffer = timedelta(milliseconds=100)
        p_w1 = [p for ts, p in data.price_history.items() if w1_start - buffer <= ts <= w1_end + buffer]
        p_w2 = [p for ts, p in data.price_history.items() if w2_start - buffer <= ts <= w2_end + buffer]
        
        # Rolling 10s high (excluding current price)
        # We look at prices strictly before 'now'
        p_prev_10s = [p for ts, p in data.price_history.items() if w1_start - buffer <= ts < now - buffer]
        high_10s = max(p_prev_10s) if p_prev_10s else 0
        
        if not p_w1 or not p_w2:
            # If we don't have enough granular data (e.g. 10s bars in backtest),
            # we fallback to comparing current price vs 10s ago
            p_10s_ago = [p for ts, p in data.price_history.items() if w1_start - buffer <= ts <= w1_start + buffer]
            if p_10s_ago:
                total_return = ((data.price - p_10s_ago[0]) / p_10s_ago[0]) * 100
                # If total return is > sum of thresholds, we consider it a potential trigger
                if total_return >= (self.t1 + self.t2) and data.price >= high_10s:
                    r1 = self.t1 # Mock values for logging
                    r2 = total_return - self.t1
                else: return False
            else: return False
        else:
            # r1 = return from (t-10s -> t-5s)
            r1 = ((p_w1[-1] - p_w1[0]) / p_w1[0]) * 100 if len(p_w1) >= 1 else 0
            # r2 = return from (t-5s -> t)
            r2 = ((data.price - p_w2[0]) / p_w2[0]) * 100 if len(p_w2) >= 1 else 0
        
        # Check conditions
        is_triggered = r1 >= self.t1 and r2 >= self.t2 and data.price >= high_10s
        
        # Debug logging for potential triggers
        if r1 > 0.5 or r2 > 0.5:
            print(f"[DEBUG] {data.symbol} @ {data.timestamp.strftime('%H:%M:%S')} | r1: {r1:.2f}% (req: {self.t1}%), r2: {r2:.2f}% (req: {self.t2}%), Price: {data.price:.2f}, High10s: {high_10s:.2f}, Triggered: {is_triggered}")

        if is_triggered:
            # Calculate volume in last 10s if available
            vol_10s = sum(v for ts, v in data.volume_history.items() if w1_start <= ts <= now) if data.volume_history else 0
            
            self.triggered_reason = (
                f"SIGNAL: r1={r1:.2f}%, r2={r2:.2f}% | "
                f"Price: ${data.price:.2f} >= High10s: ${high_10s:.2f} | "
                f"Vol10s: {vol_10s:,.0f}"
            )
            return True
            
        return False

def passes_spread_filter(best_bid: float, best_ask: float, price: float) -> bool:
    """
    Checks if the spread is within the allowed percentage.
    Spread % = ((Ask - Bid) / Price) * 100
    """
    if best_bid <= 0 or best_ask <= 0 or price <= 0:
        return True  # Default to True if data is missing to avoid blocking
        
    spread_pct = ((best_ask - best_bid) / price) * 100
    return spread_pct <= MAX_SPREAD_PCT


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
            # print(f"[DEBUG] {data.symbol} failed VWAP: Price {data.price} <= VWAP {data.vwap}")
            return False
            
        # MANDATORY: Spread filter
        if not passes_spread_filter(data.bid, data.ask, data.price):
            # print(f"[DEBUG] {data.symbol} failed Spread: Bid {data.bid}, Ask {data.ask}, Price {data.price}")
            return False
            
        # Check all other conditions in the set
        for condition in self.conditions:
            # Skip if it's already the VWAP condition (to avoid double checking)
            if isinstance(condition, PriceAboveVWAPCondition):
                continue
                
            if condition.check(data):
                self.triggered_reasons.append(condition.get_trigger_reason())
            else:
                return False
        
        # Add VWAP reason at the beginning if other conditions also met
        if self.triggered_reasons:
            self.triggered_reasons.insert(0, vwap_cond.get_trigger_reason())
            return True
            
        return False
    
    def get_trigger_summary(self) -> str:
        """Get summary of all triggered conditions"""
        return " | ".join(self.triggered_reasons)

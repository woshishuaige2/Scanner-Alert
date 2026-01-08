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


@dataclass
class MarketData:
    """Container for current market data"""
    symbol: str
    price: float
    volume: int
    vwap: float
    timestamp: datetime
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


class ConsecutiveMomentumCondition(AlertCondition):
    """Condition: Price increases by >1% in two consecutive 5-second windows"""
    
    def __init__(self, threshold: float = 1.0):
        super().__init__("Consecutive Momentum (2x 5s > 1%)")
        self.threshold = threshold
    
    def check(self, data: MarketData) -> bool:
        if not data.price_history or len(data.price_history) < 3:
            return False
            
        now = data.timestamp
        
        # Get prices for windows: [now-5s to now] and [now-10s to now-5s]
        window1_start = now - timedelta(seconds=5)
        window2_start = now - timedelta(seconds=10)
        
        # Window 1 (Current 5s)
        w1_prices = [p for ts, p in data.price_history.items() if window1_start <= ts <= now]
        # Window 2 (Previous 5s)
        w2_prices = [p for ts, p in data.price_history.items() if window2_start <= ts < window1_start]
        
        if not w1_prices or not w2_prices:
            return False
            
        # Calculate gains in each window
        # For w1: current price vs price at start of w1
        w1_gain = ((data.price - w1_prices[0]) / w1_prices[0]) * 100
        # For w2: price at end of w2 vs price at start of w2
        w2_gain = ((w1_prices[0] - w2_prices[0]) / w2_prices[0]) * 100
        
        if w1_gain >= self.threshold and w2_gain >= self.threshold:
            self.triggered_reason = f"Momentum: W1 +{w1_gain:.2f}%, W2 +{w2_gain:.2f}%"
            return True
            
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

"""
Alert Conditions Module
Defines base condition class and specific alert conditions for the scanner.
New conditions can be easily added by extending the AlertCondition class.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta


# =============================================================================
# CENTRALIZED ALERT CONFIGURATION
# Configure these values to adjust alert sensitivity across all scanners
# =============================================================================

# Global defaults (can be overridden in condition constructors)
PRICE_SURGE_THRESHOLD = 2.0  # Priority 1: ret10 >= 2.0%
VOLUME_SURGE_THRESHOLD = 5.0
WINDOW_SEC = 5
THRESH_1 = 0.8  # Priority 2: ret5_now >= 0.8%
THRESH_2 = 1.6  # Priority 2: ret10 >= 1.6%
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
    price_history: Dict[datetime, float] = None  # timestamp -> price
    volume_history: Dict[datetime, int] = None  # timestamp -> volume


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
    """
    Priority 1: Price Surge (10s)
    Fire only if ALL are true:
    - ret10 >= +2.0%
    - ret30 >= +1.0% (context gate)
    """
    
    def __init__(self, ret10_thresh: float = 2.0, ret30_thresh: float = 1.0):
        super().__init__("Price Surge (10s)")
        self.ret10_thresh = ret10_thresh
        self.ret30_thresh = ret30_thresh
    
    def check(self, data: MarketData) -> bool:
        if not data.price_history or len(data.price_history) < 2:
            return False
            
        now = data.timestamp
        prices = sorted(data.price_history.items())
        
        def get_price_at(target_ts):
            for ts, p in reversed(prices):
                if ts <= target_ts:
                    return p
            return None

        p_now = data.price
        p_10 = get_price_at(now - timedelta(seconds=10))
        p_30 = get_price_at(now - timedelta(seconds=30))

        if p_10 is None or p_30 is None:
            # Fallback for backtest (1-min bars)
            if len(prices) >= 2:
                time_diff = (prices[-1][0] - prices[-2][0]).total_seconds()
                if time_diff >= 60:
                    p_prev = prices[-2][1]
                    ret = ((p_now - p_prev) / p_prev) * 100
                    if ret >= self.ret10_thresh:
                        self.triggered_reason = f"Price Surge (1m): ret={ret:.2f}%"
                        return True
            return False

        ret10 = ((p_now - p_10) / p_10) * 100
        ret30 = ((p_now - p_30) / p_30) * 100

        if ret10 >= self.ret10_thresh and ret30 >= self.ret30_thresh:
            self.triggered_reason = f"Price Surge: ret10={ret10:.2f}%, ret30={ret30:.2f}%"
            return True
            
        return False


class TwoStepMomentumCondition(AlertCondition):
    """
    Priority 2: Two-Step Momentum (5s + 5s)
    Fire only if ALL are true:
    - ret5_now >= +0.8%
    - ret10 >= +1.6% (simple “both steps worked” without separate prev bucket)
    - ret30 >= +1.0%
    - dd10 <= 0.6% (no-dump gate)
    """
    
    def __init__(self, ret5_thresh: float = 0.8, ret10_thresh: float = 1.6, ret30_thresh: float = 1.0, dd10_thresh: float = 0.6):
        super().__init__("Two-Step Momentum")
        self.ret5_thresh = ret5_thresh
        self.ret10_thresh = ret10_thresh
        self.ret30_thresh = ret30_thresh
        self.dd10_thresh = dd10_thresh
        self.logic_used = "None"
        
    def check(self, data: MarketData) -> bool:
        if not data.price_history or len(data.price_history) < 2:
            return False
            
        now = data.timestamp
        prices = sorted(data.price_history.items())
        
        def get_price_at(target_ts):
            for ts, p in reversed(prices):
                if ts <= target_ts:
                    return p
            return None

        p_now = data.price
        p_5 = get_price_at(now - timedelta(seconds=5))
        p_10 = get_price_at(now - timedelta(seconds=10))
        p_30 = get_price_at(now - timedelta(seconds=30))

        # Detect backtest mode (1-min bars)
        is_backtest = False
        if len(prices) >= 2:
            time_diff = (prices[-1][0] - prices[-2][0]).total_seconds()
            if time_diff >= 60:
                is_backtest = True

        if is_backtest:
            p_prev = prices[-2][1]
            ret = ((p_now - p_prev) / p_prev) * 100
            if ret >= self.ret10_thresh:
                self.logic_used = "1-min fallback"
                self.triggered_reason = f"Momentum (1m): ret={ret:.2f}%"
                return True
            return False

        if p_5 is None or p_10 is None or p_30 is None:
            return False

        # Calculate returns
        ret5 = ((p_now - p_5) / p_5) * 100
        ret10 = ((p_now - p_10) / p_10) * 100
        ret30 = ((p_now - p_30) / p_30) * 100

        # Calculate Drawdown in last 10s (no-dump gate)
        # dd10 = (High in last 10s - Current Price) / High in last 10s
        recent_prices = [p for ts, p in prices if ts >= (now - timedelta(seconds=10))]
        if not recent_prices:
            return False
        high10 = max(recent_prices)
        dd10 = ((high10 - p_now) / high10) * 100 if high10 > 0 else 0

        if (ret5 >= self.ret5_thresh and 
            ret10 >= self.ret10_thresh and 
            ret30 >= self.ret30_thresh and 
            dd10 <= self.dd10_thresh):
            
            self.logic_used = "5s+5s"
            self.triggered_reason = f"Momentum: ret5={ret5:.2f}%, ret10={ret10:.2f}%, ret30={ret30:.2f}%, dd10={dd10:.2f}%"
            return True
            
        return False


def passes_spread_filter(bid: float, ask: float, price: float) -> bool:
    """Check if the bid-ask spread is within acceptable limits."""
    if bid <= 0 or ask <= 0 or price <= 0:
        return True
    spread_pct = ((ask - bid) / price) * 100
    return spread_pct <= MAX_SPREAD_PCT


class AlertConditionSet:
    """Container for multiple conditions with OR logic for triggers and AND for filters"""
    
    def __init__(self, name: str):
        self.name = name
        self.conditions: List[AlertCondition] = []
        self.triggered_reasons: List[str] = []
    
    def add_condition(self, condition: AlertCondition) -> 'AlertConditionSet':
        self.conditions.append(condition)
        return self
    
    def check_all(self, data: MarketData) -> bool:
        self.triggered_reasons = []
        
        # MANDATORY FILTER 1: Price must be above VWAP
        vwap_cond = PriceAboveVWAPCondition()
        if not vwap_cond.check(data):
            return False
            
        # MANDATORY FILTER 2: Spread filter
        if not passes_spread_filter(data.bid, data.ask, data.price):
            return False
            
        # Check alert conditions (OR logic)
        any_triggered = False
        for condition in self.conditions:
            if isinstance(condition, PriceAboveVWAPCondition):
                continue
                
            if condition.check(data):
                self.triggered_reasons.append(condition.get_trigger_reason())
                any_triggered = True
        
        if any_triggered:
            self.triggered_reasons.insert(0, vwap_cond.get_trigger_reason())
            return True
            
        return False
    
    def get_trigger_summary(self) -> str:
        return " | ".join(self.triggered_reasons)

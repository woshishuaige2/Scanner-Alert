"""
Alert Conditions Module
Defines centralized screening conditions for the scanner.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta


# =============================================================================
# CENTRALIZED ALERT CONFIGURATION
# =============================================================================
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
    price_history: List[tuple] = None  # List of (timestamp, price)

class AlertCondition(ABC):
    """Base class for all alert conditions."""
    
    def __init__(self, name: str):
        self.name = name
        self.triggered_reason = ""
    
    @abstractmethod
    def check(self, data: MarketData) -> bool:
        pass
    
    def get_trigger_reason(self) -> str:
        return self.triggered_reason

class PriceAboveVWAPCondition(AlertCondition):
    """Condition: Price is above VWAP"""
    def __init__(self):
        super().__init__("Price Above VWAP")
    
    def check(self, data: MarketData) -> bool:
        if data.vwap > 0 and data.price > data.vwap:
            self.triggered_reason = f"Price ${data.price:.2f} > VWAP ${data.vwap:.2f}"
            return True
        return False

class SqueezeCondition(AlertCondition):
    """Condition: Price up X% in Y minutes"""
    def __init__(self, pct_threshold=10.0, minutes=5):
        super().__init__(f"Squeeze {pct_threshold}%/{minutes}m")
        self.pct_threshold = pct_threshold
        self.lookback_seconds = minutes * 60
    
    def check(self, data: MarketData) -> bool:
        if not data.price_history or len(data.price_history) < 2:
            return False
            
        now = data.timestamp
        target_ts = now - timedelta(seconds=self.lookback_seconds)
        
        # Find the oldest price within the lookback window
        old_price = None
        for ts, p in data.price_history:
            if ts >= target_ts:
                old_price = p
                break
        
        if old_price and old_price > 0:
            increase = (data.price - old_price) / old_price * 100
            if increase >= self.pct_threshold:
                self.triggered_reason = f"Up {increase:.2f}% in {self.lookback_seconds/60:.0f}m"
                return True
        return False

def passes_spread_filter(bid: float, ask: float, price: float) -> bool:
    """Check if the bid-ask spread is within acceptable limits."""
    if bid <= 0 or ask <= 0 or price <= 0:
        return True # Default to pass if data is missing
    spread_pct = ((ask - bid) / price) * 100
    return spread_pct <= MAX_SPREAD_PCT

class AlertConditionSet:
    """Container for conditions with AND logic for preliminary screening"""
    def __init__(self, name: str):
        self.name = name
        self.conditions: List[AlertCondition] = []
        self.triggered_reasons: List[str] = []
    
    def add_condition(self, condition: AlertCondition) -> 'AlertConditionSet':
        self.conditions.append(condition)
        return self
    
    def check_all(self, data: MarketData) -> bool:
        self.triggered_reasons = []
        
        # 1. Mandatory Spread Filter
        if not passes_spread_filter(data.bid, data.ask, data.price):
            return False
            
        # 2. Check all registered conditions (AND logic)
        for condition in self.conditions:
            if not condition.check(data):
                return False
            self.triggered_reasons.append(condition.get_trigger_reason())
        
        return len(self.conditions) > 0
    
    def get_trigger_summary(self) -> str:
        return " | ".join(self.triggered_reasons)

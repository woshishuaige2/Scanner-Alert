"""
Alert Conditions Module
Defines a unified, strict momentum condition for the scanner.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta


# =============================================================================
# CENTRALIZED ALERT CONFIGURATION
# =============================================================================

# Thresholds for the Unified Momentum Condition
RET10_THRESH = 2.0    # ret10 >= 2.0%
RET30_THRESH = 1.0    # ret30 >= 1.0%
DD10_THRESH = 0.4     # dd10 <= 0.4% (Strict No-Dump)
RET5_THRESH = 0.8     # ret5 >= 0.8% (Momentum Confirmation)

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
        if data.price > data.vwap:
            self.triggered_reason = f"Price ${data.price:.2f} > VWAP ${data.vwap:.2f}"
            return True
        self.triggered_reason = ""
        return False


class UnifiedMomentumCondition(AlertCondition):
    """
    Unified Strict Momentum Logic:
    Fire only if ALL are true:
    - ret10 >= 2.0%
    - ret30 >= 1.0%
    - dd10 <= 0.4%
    - ret5 >= 0.8%
    """
    
    def __init__(self, ret10=RET10_THRESH, ret30=RET30_THRESH, dd10=DD10_THRESH, ret5=RET5_THRESH):
        super().__init__("Unified Momentum")
        self.ret10_thresh = ret10
        self.ret30_thresh = ret30
        self.dd10_thresh = dd10
        self.ret5_thresh = ret5
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
            # In backtest, we use the ret10 threshold as the primary gate
            if ret >= self.ret10_thresh:
                self.logic_used = "1-min fallback"
                self.triggered_reason = f"Unified (1m): ret={ret:.2f}%"
                return True
            return False

        if p_5 is None or p_10 is None or p_30 is None:
            return False

        # Calculate returns
        ret5 = ((p_now - p_5) / p_5) * 100
        ret10 = ((p_now - p_10) / p_10) * 100
        ret30 = ((p_now - p_30) / p_30) * 100

        # Calculate Drawdown in last 10s (no-dump gate)
        recent_prices = [p for ts, p in prices if ts >= (now - timedelta(seconds=10))]
        high10 = max(recent_prices) if recent_prices else p_now
        dd10 = ((high10 - p_now) / high10) * 100 if high10 > 0 else 0

        # Unified Strict Check
        if (ret10 >= self.ret10_thresh and 
            ret30 >= self.ret30_thresh and 
            dd10 <= self.dd10_thresh and 
            ret5 >= self.ret5_thresh):
            
            self.logic_used = "Unified"
            self.triggered_reason = (f"Unified: ret10={ret10:.2f}%, ret30={ret30:.2f}%, "
                                     f"dd10={dd10:.2f}%, ret5={ret5:.2f}%")
            return True
            
        return False


def passes_spread_filter(bid: float, ask: float, price: float) -> bool:
    """Check if the bid-ask spread is within acceptable limits."""
    if bid <= 0 or ask <= 0 or price <= 0:
        return True
    spread_pct = ((ask - bid) / price) * 100
    return spread_pct <= MAX_SPREAD_PCT


class AlertConditionSet:
    """Container for conditions with AND logic for filters and triggers"""
    
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
            
        # Check alert conditions (All must pass for this unified setup)
        all_triggered = True
        for condition in self.conditions:
            if isinstance(condition, PriceAboveVWAPCondition):
                continue
                
            if not condition.check(data):
                all_triggered = False
                break
            else:
                self.triggered_reasons.append(condition.get_trigger_reason())
        
        if all_triggered and self.conditions:
            self.triggered_reasons.insert(0, vwap_cond.get_trigger_reason())
            return True
            
        return False
    
    def get_trigger_summary(self) -> str:
        return " | ".join(self.triggered_reasons)

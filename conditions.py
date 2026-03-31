"""
Alert Conditions Module
Defines centralized screening conditions for the scanner.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from scanner_config import (
    SPREAD_FILTER_OVER_3_MAX_PCT,
    SPREAD_FILTER_UNDER_1_MAX_PCT,
    SPREAD_FILTER_UNDER_1_PRICE_MAX,
    SPREAD_FILTER_UNDER_3_MAX_PCT,
    SPREAD_FILTER_UNDER_3_PRICE_MAX,
    VWAP_BUFFER_OVER_3_PCT,
    VWAP_BUFFER_UNDER_1_PCT,
    VWAP_BUFFER_UNDER_1_PRICE_MAX,
    VWAP_BUFFER_UNDER_3_PCT,
    VWAP_BUFFER_UNDER_3_PRICE_MAX,
)

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
    price_history: List[tuple] = field(default_factory=list)   # List[(timestamp, price)]
    volume_history: List[tuple] = field(default_factory=list)  # List[(timestamp, cumulative_volume)]

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

    def _get_required_buffer_pct(self, price: float) -> float:
        if price < VWAP_BUFFER_UNDER_1_PRICE_MAX:
            return VWAP_BUFFER_UNDER_1_PCT
        if price < VWAP_BUFFER_UNDER_3_PRICE_MAX:
            return VWAP_BUFFER_UNDER_3_PCT
        return VWAP_BUFFER_OVER_3_PCT
    
    def check(self, data: MarketData) -> bool:
        if data.vwap > 0:
            required_buffer_pct = self._get_required_buffer_pct(data.price)
            required_price = data.vwap * (1 + required_buffer_pct / 100.0)
            if data.price > required_price:
                self.triggered_reason = (
                    f"Price ${data.price:.2f} > VWAP ${data.vwap:.2f}"
                    f" + {required_buffer_pct:.1f}% buffer"
                )
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


class FastIgnitionCondition(AlertCondition):
    """Condition: very fast momentum + sustained volume with limited pullback."""

    def __init__(
        self,
        pct_threshold_5s: float = 1.0,
        pct_threshold_15s: float = 2.0,
        volume_multiplier: float = 2.0,
        max_retracement_pct: float = 0.8,
    ):
        super().__init__("Fast Ignition")
        self.pct_threshold_5s = pct_threshold_5s
        self.pct_threshold_15s = pct_threshold_15s
        self.volume_multiplier = volume_multiplier
        self.max_retracement_pct = max_retracement_pct

    def _find_first_after(self, history: List[tuple], target_ts: datetime) -> Optional[float]:
        for ts, value in history:
            if ts >= target_ts:
                return value
        return history[0][1] if history else None

    def check(self, data: MarketData) -> bool:
        if len(data.price_history) < 3 or len(data.volume_history) < 3:
            return False

        now = data.timestamp
        price_5s_ago = self._find_first_after(data.price_history, now - timedelta(seconds=5))
        price_15s_ago = self._find_first_after(data.price_history, now - timedelta(seconds=15))
        if not price_5s_ago or not price_15s_ago:
            return False

        pct_5s = ((data.price - price_5s_ago) / price_5s_ago) * 100 if price_5s_ago > 0 else 0.0
        pct_15s = ((data.price - price_15s_ago) / price_15s_ago) * 100 if price_15s_ago > 0 else 0.0
        if pct_5s < self.pct_threshold_5s or pct_15s < self.pct_threshold_15s:
            return False

        # Estimate 5-second burst volume vs trailing 60-second average 5-second bucket.
        vol_now = data.volume_history[-1][1]
        vol_5s_ago = self._find_first_after(data.volume_history, now - timedelta(seconds=5))
        vol_65s_ago = self._find_first_after(data.volume_history, now - timedelta(seconds=65))
        if vol_5s_ago is None or vol_65s_ago is None:
            return False

        burst_5s = max(0.0, vol_now - vol_5s_ago)
        trailing_60s = max(0.0, vol_5s_ago - vol_65s_ago)
        avg_5s_bucket = trailing_60s / 12.0 if trailing_60s > 0 else 0.0
        vol_mult = (burst_5s / avg_5s_bucket) if avg_5s_bucket > 0 else 0.0
        if vol_mult < self.volume_multiplier:
            return False

        # Keep pullback small from the local high in the last 15 seconds.
        recent_prices = [
            p for ts, p in data.price_history
            if ts >= now - timedelta(seconds=15)
        ]
        if not recent_prices:
            return False

        peak = max(recent_prices)
        retracement_pct = ((peak - data.price) / peak) * 100 if peak > 0 else 0.0
        if retracement_pct > self.max_retracement_pct:
            return False

        self.triggered_reason = (
            f"FastIgnition +{pct_5s:.2f}%/5s +{pct_15s:.2f}%/15s "
            f"Vol x{vol_mult:.2f} Retrace {retracement_pct:.2f}%"
        )
        return True

def get_max_spread_pct(price: float) -> float:
    if price < SPREAD_FILTER_UNDER_1_PRICE_MAX:
        return SPREAD_FILTER_UNDER_1_MAX_PCT
    if price < SPREAD_FILTER_UNDER_3_PRICE_MAX:
        return SPREAD_FILTER_UNDER_3_MAX_PCT
    return SPREAD_FILTER_OVER_3_MAX_PCT


def get_spread_filter_failure_reason(bid: float, ask: float, price: float) -> str:
    """Return a human-readable failure reason when the spread is too wide."""
    if bid <= 0 or ask <= 0 or price <= 0:
        return ""
    spread_pct = ((ask - bid) / price) * 100
    max_spread_pct = get_max_spread_pct(price)
    if spread_pct <= max_spread_pct:
        return ""
    return (
        f"Spread {spread_pct:.2f}% > allowed {max_spread_pct:.2f}% "
        f"(bid ${bid:.2f} ask ${ask:.2f} price ${price:.2f})"
    )


def passes_spread_filter(bid: float, ask: float, price: float) -> bool:
    """Check if the bid-ask spread is within acceptable limits."""
    return get_spread_filter_failure_reason(bid, ask, price) == ""

class AlertConditionSet:
    """Container for conditions with AND logic for preliminary screening"""
    def __init__(self, name: str):
        self.name = name
        self.conditions: List[AlertCondition] = []
        self.triggered_reasons: List[str] = []
        self.last_block_reason = ""
    
    def add_condition(self, condition: AlertCondition) -> 'AlertConditionSet':
        self.conditions.append(condition)
        return self
    
    def check_all(self, data: MarketData) -> bool:
        self.triggered_reasons = []
        self.last_block_reason = ""
        
        # 1. Mandatory Spread Filter
        spread_failure_reason = get_spread_filter_failure_reason(data.bid, data.ask, data.price)
        if spread_failure_reason:
            self.last_block_reason = spread_failure_reason
            return False
            
        # 2. Check all registered conditions (AND logic)
        for condition in self.conditions:
            if not condition.check(data):
                self.last_block_reason = condition.name
                return False
            self.triggered_reasons.append(condition.get_trigger_reason())
        
        return len(self.conditions) > 0
    
    def get_trigger_summary(self) -> str:
        return " | ".join(self.triggered_reasons)

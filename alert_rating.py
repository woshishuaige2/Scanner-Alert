"""
Alert rating utilities for triggered scanner setups.

Scoring rules (additive points):
- Relative volume: +1 if RelVol >= 5x, and +1 extra if RelVol >= 20x.
- Fresh high: +1 if current 1-minute candle high is at/above prior high.
- Float size: +1 if float < 20M shares.
- Squeeze strength (lookback window): +3 if >= 20%, +2 if >= 15%, +1 if >= configured threshold.
- Hold move quality: +1 if current price holds at least 50% of the move from base to peak.
- Intraday drawdown quality (1-minute candles):
    +1 if no candle has high-to-low drawdown >= 10%.
    +1 if no candle has high-to-low drawdown >= 5%.
- News catalyst: +2 if there is meaningful company-specific news today.

Grade mapping (max score 12):
- A: score >= 9
- B: score >= 6
- C: score < 6
"""

from datetime import timedelta
from typing import List, Optional, Tuple


def _is_current_minute_new_high(price_history, last_update) -> bool:
    """Return True when the current 1-minute candle high is a new high vs prior data."""
    current_minute_start = last_update.replace(second=0, microsecond=0)
    current_minute_prices = [p for ts, p in price_history if ts >= current_minute_start]
    prior_prices = [p for ts, p in price_history if ts < current_minute_start]

    if not current_minute_prices or not prior_prices:
        return False

    return max(current_minute_prices) >= max(prior_prices)


def calculate_alert_rating(
    price_history,
    last_update,
    relative_volume: float,
    float_shares,
    current_price: float,
    squeeze_pct_threshold: float,
    squeeze_time_minutes: int,
    max_intraday_1m_drawdown_pct: Optional[float] = None,
    has_meaningful_news_today: bool = False,
) -> Tuple[int, str, List[str]]:
    if not price_history or last_update is None:
        return 0, "C", []

    score = 0
    reasons: List[str] = []
    if relative_volume >= 20:
        score += 2
        reasons.append(f"RelVol {relative_volume:.2f}x (+2)")
    elif relative_volume >= 5:
        score += 1
        reasons.append(f"RelVol {relative_volume:.2f}x (+1)")

    if _is_current_minute_new_high(price_history, last_update):
        score += 1
        reasons.append("Fresh high 1m (+1)")

    if float_shares is not None and float_shares < 20_000_000:
        score += 1
        reasons.append(f"Float {float_shares/1e6:.1f}M (+1)")

    lookback_start = last_update - timedelta(minutes=squeeze_time_minutes)
    window_prices = [(ts, p) for ts, p in price_history if ts >= lookback_start]
    if window_prices:
        base_price = window_prices[0][1]
        peak_price = max(p for _, p in window_prices)
        if base_price > 0:
            squeeze_pct = ((peak_price - base_price) / base_price) * 100
            if squeeze_pct >= 20:
                score += 3
                reasons.append(f"Squeeze {squeeze_pct:.1f}% (+3)")
            elif squeeze_pct >= 15:
                score += 2
                reasons.append(f"Squeeze {squeeze_pct:.1f}% (+2)")
            elif squeeze_pct >= squeeze_pct_threshold:
                score += 1
                reasons.append(f"Squeeze {squeeze_pct:.1f}% (+1)")

            move = peak_price - base_price
            if move > 0:
                half_move_level = base_price + (move * 0.5)
                if current_price >= half_move_level:
                    score += 1
                    reasons.append("Held move (+1)")

    # Drawdown quality points based on max observed 1-minute high-to-low drawdown.
    if max_intraday_1m_drawdown_pct is not None:
        if max_intraday_1m_drawdown_pct < 10:
            score += 1
            reasons.append("No 1m H-L drawdown >=10% (+1)")
        if max_intraday_1m_drawdown_pct < 5:
            score += 1
            reasons.append("No 1m H-L drawdown >=5% (+1)")

    if has_meaningful_news_today:
        score += 2
        reasons.append("Meaningful news (+2)")

    if score >= 9:
        grade = "A"
    elif score >= 6:
        grade = "B"
    else:
        grade = "C"

    return score, grade, reasons

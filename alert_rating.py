"""
Alert rating utilities for triggered scanner setups.
"""

from datetime import timedelta
from typing import List, Tuple


def calculate_alert_rating(
    price_history,
    last_update,
    relative_volume: float,
    float_shares,
    current_price: float,
    squeeze_pct_threshold: float,
    squeeze_time_minutes: int,
) -> Tuple[int, str, List[str]]:
    if not price_history or last_update is None:
        return 0, "C", []

    score = 0
    reasons: List[str] = []
    prices = [p for _, p in price_history]

    if relative_volume >= 5:
        score += 3
        reasons.append(f"RelVol {relative_volume:.2f}x")
    elif relative_volume >= 2:
        score += 2
        reasons.append(f"RelVol {relative_volume:.2f}x")
    elif relative_volume >= 1:
        score += 1
        reasons.append(f"RelVol {relative_volume:.2f}x")

    if len(prices) >= 2 and current_price >= max(prices[:-1]):
        score += 1
        reasons.append("Fresh high")

    if float_shares is not None:
        if float_shares < 20_000_000:
            score += 2
            reasons.append(f"Float {float_shares/1e6:.1f}M")
        elif float_shares < 50_000_000:
            score += 1
            reasons.append(f"Float {float_shares/1e6:.1f}M")

    lookback_start = last_update - timedelta(minutes=squeeze_time_minutes)
    window_prices = [(ts, p) for ts, p in price_history if ts >= lookback_start]
    if window_prices:
        base_price = window_prices[0][1]
        peak_price = max(p for _, p in window_prices)
        if base_price > 0:
            squeeze_pct = ((peak_price - base_price) / base_price) * 100
            if squeeze_pct >= 20:
                score += 3
                reasons.append(f"Squeeze {squeeze_pct:.1f}%")
            elif squeeze_pct >= 15:
                score += 2
                reasons.append(f"Squeeze {squeeze_pct:.1f}%")
            elif squeeze_pct >= squeeze_pct_threshold:
                score += 1
                reasons.append(f"Squeeze {squeeze_pct:.1f}%")

            move = peak_price - base_price
            if move > 0:
                half_move_level = base_price + (move * 0.5)
                if current_price >= half_move_level:
                    score += 2
                    reasons.append("Held move")

    if score >= 8:
        grade = "A"
    elif score >= 5:
        grade = "B"
    else:
        grade = "C"

    return score, grade, reasons

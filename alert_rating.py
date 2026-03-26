"""
Alert rating utilities for triggered scanner setups.

Scoring rules (additive points):
- Relative volume: +1 if RelVol >= 5x, and +1 extra if RelVol >= 50x.
- Local breakout: +1 if current 1-minute candle high is above the highest completed
  1-minute candle high from the prior configured rolling lookback window.
- Session HOD breakout: +1 if current 1-minute candle high is above the highest
  completed 1-minute candle high seen earlier in the current session.
- Float size: +1 if float < 20M shares.
- Multi-timeframe momentum expansion: +2 if rolling 5s/15s/30s/60s price acceleration
  and volume acceleration both confirm a steepening move.
- Squeeze strength (lookback window): +2 if >= 20%, +1 if >= configured threshold.
- Hold move quality: +1 if current price holds at least 50% of the move from base to peak.
- Intraday drawdown quality (1-minute candles):
    Build a volatility reference from the average of the top configured number of green
    1-minute candle body percentages over the configured rolling lookback window.
    Upper threshold = max(configured base upper %, configured upper multiplier * vol reference).
    Lower threshold = max(configured base lower %, configured lower multiplier * vol reference).
    +1 if no candle has high-to-low drawdown >= upper threshold.
    +1 if no candle has high-to-low drawdown >= lower threshold.
- News catalyst: +1 if there is meaningful company-specific news today.

Current grade mapping (Max 14):
- A+: score >= 12
- A: score >= 9
- B: score >= 6
- C: score 3-5
- Below alert threshold: score < 3
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from scanner_config import (
    ALERT_MIN_SCORE_TO_NOTIFY,
    ALERT_BREAKOUT_LOOKBACK_MINUTES,
    ALERT_DRAWDOWN_LOOKBACK_MINUTES,
    ALERT_DRAWDOWN_LOWER_BASE_PCT,
    ALERT_DRAWDOWN_LOWER_VOL_MULTIPLIER,
    ALERT_DRAWDOWN_TOP_GREEN_CANDLE_COUNT,
    ALERT_DRAWDOWN_UPPER_BASE_PCT,
    ALERT_DRAWDOWN_UPPER_VOL_MULTIPLIER,
    ALERT_GRADE_A_MIN_SCORE,
    ALERT_GRADE_B_MIN_SCORE,
    ALERT_MOMENTUM_MIN_SAMPLES_60S,
    ALERT_MOMENTUM_MIN_VOL_15S,
    ALERT_MOMENTUM_PRICE_MIN_PCT_5S,
    ALERT_MOMENTUM_PRICE_MIN_PCT_15S,
    ALERT_MOMENTUM_PRICE_MIN_PCT_30S,
    ALERT_MOMENTUM_PRICE_MIN_PCT_60S,
    ALERT_MOMENTUM_PRICE_RATE_5S_OVER_15S,
    ALERT_MOMENTUM_PRICE_RATE_15S_OVER_30S,
    ALERT_MOMENTUM_VOL_RATE_5S_OVER_15S,
    ALERT_MOMENTUM_VOL_RATE_15S_OVER_30S,
)


def get_breakout_debug_info(
    current_minute_high: Optional[float],
    current_minute_bucket: Optional[datetime],
    completed_1m_highs: Optional[List[Tuple[datetime, float]]],
) -> Optional[Dict[str, Any]]:
    """Return local and session breakout debug data derived from completed 1-minute highs."""
    if current_minute_high is None or current_minute_bucket is None:
        return None

    completed_highs = [
        (bucket, high)
        for bucket, high in (completed_1m_highs or [])
        if bucket is not None and high is not None
    ]
    local_lookback_start = current_minute_bucket - timedelta(minutes=ALERT_BREAKOUT_LOOKBACK_MINUTES)
    prior_local_highs = [high for bucket, high in completed_highs if bucket >= local_lookback_start]
    prior_session_highs = [high for _, high in completed_highs]

    prior_local_high = max(prior_local_highs) if prior_local_highs else None
    prior_session_high = max(prior_session_highs) if prior_session_highs else None

    return {
        "lookback_minutes": ALERT_BREAKOUT_LOOKBACK_MINUTES,
        "current_minute_high": current_minute_high,
        "prior_local_high": prior_local_high,
        "prior_session_high": prior_session_high,
        "prior_local_high_count": len(prior_local_highs),
        "prior_session_high_count": len(prior_session_highs),
        "passed_local_breakout": prior_local_high is not None and current_minute_high > prior_local_high,
        "passed_session_hod_breakout": prior_session_high is not None and current_minute_high > prior_session_high,
    }


def _compute_dynamic_drawdown_thresholds(
    recent_green_1m_body_pcts: Optional[List[float]],
) -> Tuple[float, float]:
    """Return drawdown thresholds scaled by recent green 1-minute candle strength."""
    positive_bodies = sorted((pct for pct in (recent_green_1m_body_pcts or []) if pct > 0), reverse=True)
    top_bodies = positive_bodies[:ALERT_DRAWDOWN_TOP_GREEN_CANDLE_COUNT]
    volatility_reference_pct = sum(top_bodies) / len(top_bodies) if top_bodies else 0.0

    upper_threshold_pct = max(
        ALERT_DRAWDOWN_UPPER_BASE_PCT,
        ALERT_DRAWDOWN_UPPER_VOL_MULTIPLIER * volatility_reference_pct,
    )
    lower_threshold_pct = max(
        ALERT_DRAWDOWN_LOWER_BASE_PCT,
        ALERT_DRAWDOWN_LOWER_VOL_MULTIPLIER * volatility_reference_pct,
    )
    return upper_threshold_pct, lower_threshold_pct


def get_drawdown_debug_info(
    max_intraday_1m_drawdown_pct: Optional[float],
    recent_green_1m_body_pcts: Optional[List[float]],
) -> Optional[Dict[str, Any]]:
    """Return drawdown debug data used to score the dynamic quality checks."""
    if max_intraday_1m_drawdown_pct is None:
        return None

    positive_bodies = sorted((pct for pct in (recent_green_1m_body_pcts or []) if pct > 0), reverse=True)
    top_bodies = positive_bodies[:ALERT_DRAWDOWN_TOP_GREEN_CANDLE_COUNT]
    volatility_reference_pct = sum(top_bodies) / len(top_bodies) if top_bodies else 0.0
    upper_threshold_pct, lower_threshold_pct = _compute_dynamic_drawdown_thresholds(recent_green_1m_body_pcts)

    return {
        "lookback_minutes": ALERT_DRAWDOWN_LOOKBACK_MINUTES,
        "top_green_candle_count": ALERT_DRAWDOWN_TOP_GREEN_CANDLE_COUNT,
        "recent_green_body_count": len(positive_bodies),
        "top_green_1m_body_pcts": top_bodies,
        "volatility_reference_pct": volatility_reference_pct,
        "observed_max_drawdown_pct": max_intraday_1m_drawdown_pct,
        "upper_base_threshold_pct": ALERT_DRAWDOWN_UPPER_BASE_PCT,
        "lower_base_threshold_pct": ALERT_DRAWDOWN_LOWER_BASE_PCT,
        "upper_dynamic_threshold_pct": upper_threshold_pct,
        "lower_dynamic_threshold_pct": lower_threshold_pct,
        "passed_upper_threshold": max_intraday_1m_drawdown_pct < upper_threshold_pct,
        "passed_lower_threshold": max_intraday_1m_drawdown_pct < lower_threshold_pct,
    }


def get_alert_grade_rank(grade: Optional[str]) -> int:
    grade_order = {
        "Below Threshold": 0,
        "C": 1,
        "B": 2,
        "A": 3,
        "A+": 4,
    }
    return grade_order.get(grade or "", -1)


def _find_first_value_at_or_after(
    history: List[Tuple[datetime, float]],
    target_ts: datetime,
) -> Optional[float]:
    for ts, value in history:
        if ts >= target_ts:
            return value
    return None


def _compute_price_change_pct(
    price_history: List[Tuple[datetime, float]],
    last_update: datetime,
    window_seconds: int,
) -> Optional[float]:
    if not price_history:
        return None
    target_ts = last_update - timedelta(seconds=window_seconds)
    if price_history[0][0] > target_ts:
        return None
    baseline_price = _find_first_value_at_or_after(price_history, target_ts)
    current_price = price_history[-1][1]
    if baseline_price is None or baseline_price <= 0 or current_price <= 0:
        return None
    return ((current_price - baseline_price) / baseline_price) * 100


def _compute_window_volume(
    volume_history: List[Tuple[datetime, float]],
    last_update: datetime,
    window_seconds: int,
) -> Optional[float]:
    if not volume_history:
        return None
    target_ts = last_update - timedelta(seconds=window_seconds)
    if volume_history[0][0] > target_ts:
        return None
    baseline_volume = _find_first_value_at_or_after(volume_history, target_ts)
    current_volume = volume_history[-1][1]
    if baseline_volume is None or current_volume is None:
        return None
    return max(0.0, current_volume - baseline_volume)


def get_momentum_debug_info(
    price_history: List[Tuple[datetime, float]],
    volume_history: List[Tuple[datetime, float]],
    last_update: Optional[datetime],
) -> Optional[Dict[str, Any]]:
    """Return multi-timeframe price and volume acceleration data used for the momentum score."""
    if not price_history or not volume_history or last_update is None:
        return None

    price_recent_samples_60s = sum(1 for ts, _ in price_history if ts >= last_update - timedelta(seconds=60))
    volume_recent_samples_60s = sum(1 for ts, _ in volume_history if ts >= last_update - timedelta(seconds=60))
    has_dense_coverage = (
        price_recent_samples_60s >= ALERT_MOMENTUM_MIN_SAMPLES_60S
        and volume_recent_samples_60s >= ALERT_MOMENTUM_MIN_SAMPLES_60S
    )

    price_windows = {
        "5s": _compute_price_change_pct(price_history, last_update, 5),
        "15s": _compute_price_change_pct(price_history, last_update, 15),
        "30s": _compute_price_change_pct(price_history, last_update, 30),
        "60s": _compute_price_change_pct(price_history, last_update, 60),
    }
    price_rates = {
        "5s": price_windows["5s"] / 5 if price_windows["5s"] is not None else None,
        "15s": price_windows["15s"] / 15 if price_windows["15s"] is not None else None,
        "30s": price_windows["30s"] / 30 if price_windows["30s"] is not None else None,
        "60s": price_windows["60s"] / 60 if price_windows["60s"] is not None else None,
    }

    volume_windows = {
        "5s": _compute_window_volume(volume_history, last_update, 5),
        "15s": _compute_window_volume(volume_history, last_update, 15),
        "30s": _compute_window_volume(volume_history, last_update, 30),
        "60s": _compute_window_volume(volume_history, last_update, 60),
    }
    volume_rates = {
        "5s": volume_windows["5s"] / 5 if volume_windows["5s"] is not None else None,
        "15s": volume_windows["15s"] / 15 if volume_windows["15s"] is not None else None,
        "30s": volume_windows["30s"] / 30 if volume_windows["30s"] is not None else None,
        "60s": volume_windows["60s"] / 60 if volume_windows["60s"] is not None else None,
    }

    has_price_windows = all(price_windows[label] is not None for label in ("5s", "15s", "30s", "60s"))
    has_volume_windows = all(volume_windows[label] is not None for label in ("5s", "15s", "30s", "60s"))

    price_accel_pass = False
    if has_dense_coverage and has_price_windows:
        price_accel_pass = (
            price_windows["5s"] >= ALERT_MOMENTUM_PRICE_MIN_PCT_5S
            and price_windows["15s"] >= ALERT_MOMENTUM_PRICE_MIN_PCT_15S
            and price_windows["30s"] >= ALERT_MOMENTUM_PRICE_MIN_PCT_30S
            and price_windows["60s"] >= ALERT_MOMENTUM_PRICE_MIN_PCT_60S
            and price_rates["5s"] > price_rates["15s"] * ALERT_MOMENTUM_PRICE_RATE_5S_OVER_15S
            and price_rates["15s"] > price_rates["30s"] * ALERT_MOMENTUM_PRICE_RATE_15S_OVER_30S
            and price_rates["30s"] > price_rates["60s"]
        )

    volume_accel_pass = False
    if has_dense_coverage and has_volume_windows:
        volume_accel_pass = (
            volume_windows["15s"] > ALERT_MOMENTUM_MIN_VOL_15S
            and volume_rates["5s"] > volume_rates["15s"] * ALERT_MOMENTUM_VOL_RATE_5S_OVER_15S
            and volume_rates["15s"] > volume_rates["30s"] * ALERT_MOMENTUM_VOL_RATE_15S_OVER_30S
            and volume_rates["30s"] > volume_rates["60s"]
        )

    return {
        "price_recent_samples_60s": price_recent_samples_60s,
        "volume_recent_samples_60s": volume_recent_samples_60s,
        "has_dense_coverage": has_dense_coverage,
        "price_windows_pct": price_windows,
        "price_rates_pct_per_sec": price_rates,
        "price_accel_pass": price_accel_pass,
        "volume_windows": volume_windows,
        "volume_rates_per_sec": volume_rates,
        "volume_accel_pass": volume_accel_pass,
        "combined_momentum_pass": price_accel_pass and volume_accel_pass,
    }


def calculate_alert_rating(
    price_history,
    volume_history,
    last_update,
    relative_volume: float,
    float_shares,
    current_price: float,
    squeeze_pct_threshold: float,
    squeeze_time_minutes: int,
    breakout_debug_info: Optional[Dict[str, Any]] = None,
    max_intraday_1m_drawdown_pct: Optional[float] = None,
    recent_green_1m_body_pcts: Optional[List[float]] = None,
    has_meaningful_news_today: bool = False,
    momentum_debug_info: Optional[Dict[str, Any]] = None,
) -> Tuple[int, str, List[str]]:
    if not price_history or last_update is None:
        return 0, "Below Threshold", []

    score = 0
    reasons: List[str] = []
    if relative_volume >= 20:
        score += 2
        reasons.append(f"RelVol {relative_volume:.2f}x (+2)")
    elif relative_volume >= 5:
        score += 1
        reasons.append(f"RelVol {relative_volume:.2f}x (+1)")

    if breakout_debug_info and breakout_debug_info["passed_local_breakout"]:
        score += 1
        reasons.append(f"Local {ALERT_BREAKOUT_LOOKBACK_MINUTES}m breakout (+1)")
    if breakout_debug_info and breakout_debug_info["passed_session_hod_breakout"]:
        score += 1
        reasons.append("Session HOD breakout (+1)")

    if float_shares is not None and float_shares < 20_000_000:
        score += 1
        reasons.append(f"Float {float_shares/1e6:.1f}M (+1)")

    if momentum_debug_info is None:
        momentum_debug_info = get_momentum_debug_info(
            price_history=price_history,
            volume_history=volume_history or [],
            last_update=last_update,
        )
    if momentum_debug_info and momentum_debug_info["combined_momentum_pass"]:
        score += 2
        reasons.append("Multi-timeframe accel + volume expansion (+2)")

    lookback_start = last_update - timedelta(minutes=squeeze_time_minutes)
    window_prices = [(ts, p) for ts, p in price_history if ts >= lookback_start]
    if window_prices:
        base_price = window_prices[0][1]
        peak_price = max(p for _, p in window_prices)
        if base_price > 0:
            squeeze_pct = ((peak_price - base_price) / base_price) * 100
            if squeeze_pct >= 20:
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
        drawdown_debug_info = get_drawdown_debug_info(
            max_intraday_1m_drawdown_pct=max_intraday_1m_drawdown_pct,
            recent_green_1m_body_pcts=recent_green_1m_body_pcts,
        )
        if drawdown_debug_info["recent_green_body_count"] > 0:
            upper_drawdown_threshold_pct = drawdown_debug_info["upper_dynamic_threshold_pct"]
            lower_drawdown_threshold_pct = drawdown_debug_info["lower_dynamic_threshold_pct"]
            if max_intraday_1m_drawdown_pct < upper_drawdown_threshold_pct:
                score += 1
                reasons.append(f"No 1m H-L drawdown >={upper_drawdown_threshold_pct:.1f}% (+1)")
            if max_intraday_1m_drawdown_pct < lower_drawdown_threshold_pct:
                score += 1
                reasons.append(f"No 1m H-L drawdown >={lower_drawdown_threshold_pct:.1f}% (+1)")

    if has_meaningful_news_today:
        score += 1
        reasons.append("Meaningful news (+1)")

    if score >= 12:
        grade = "A+"
    elif score >= ALERT_GRADE_A_MIN_SCORE:
        grade = "A"
    elif score >= ALERT_GRADE_B_MIN_SCORE:
        grade = "B"
    elif score >= ALERT_MIN_SCORE_TO_NOTIFY:
        grade = "C"
    else:
        grade = "Below Threshold"

    return score, grade, reasons

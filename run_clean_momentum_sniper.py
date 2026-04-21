"""
Clean Momentum Sniper

Independent trading runner that reuses the standalone scanner as stage 1, then
applies a clean-trend / pullback-sniper stage 2 before delegating execution to
the shared ExecutionEngine.
"""
from __future__ import annotations

import math
import os
import platform
import signal
import subprocess
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pytz

import scanner_config as config
from alert_rating import get_alert_grade_rank
from execution_engine import ExecutionEngine
from realtime_multi_session_scanner import (
    RealtimeBroadScanner,
    append_alert_score_audit,
    build_scanner_runtime_state,
    build_voice_announcement,
    get_market_session,
    initialize_alert_score_audit_file,
    send_discord_alert,
    update_scanner_symbols,
)
from runtime_feedback import RuntimeTelemetry
from top_gainers_fetcher import get_top_gainers
from tws_data_fetcher import create_tws_data_app


INVESTMENT_PER_TRADE = 500.0
TP_PCT = 5.0
SL_PCT = 5.0
ACCOUNT_NUMBER = "DUO200259"
RUNTIME_FEEDBACK_DIR = os.path.join(os.path.dirname(__file__), config.RUNTIME_FEEDBACK_DIR_NAME)

should_exit = False
tws_app = None
active_executor = None
filtered_alerts = deque(maxlen=25)
shutdown_event = threading.Event()


def append_sniper_event(events: deque, message: str) -> None:
    events.appendleft((datetime.now(), message))


def _format_trade_span(entry_time: Optional[datetime], exit_time: datetime) -> str:
    if not isinstance(exit_time, datetime):
        return "--"
    if not isinstance(entry_time, datetime):
        return exit_time.strftime('%H:%M:%S')

    duration_seconds = max(0, int((exit_time - entry_time).total_seconds()))
    hours, remainder = divmod(duration_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return (
        f"{entry_time.strftime('%H:%M:%S')}->{exit_time.strftime('%H:%M:%S')} "
        f"({hours:02d}:{minutes:02d}:{seconds:02d})"
    )


def _format_trade_reference_timeframe(trade: Dict[str, Any]) -> str:
    structure_mode = (trade.get("structure_mode") or "").strip()
    if structure_mode:
        return structure_mode
    return "--"


def signal_handler(sig, frame):
    global should_exit
    print("\n[INFO] Graceful exit requested...")
    executor = active_executor
    if executor is not None:
        try:
            print("[INFO] Emergency exit: submitting close-all orders for open positions...")
            executor.close_all_positions(market_session=get_market_session())
            broker_positions = executor.tws_app.request_open_positions(account=ACCOUNT_NUMBER, timeout=5.0)
            if broker_positions:
                print("[INFO] Emergency exit: flattening any remaining broker positions...")
                executor.flatten_external_positions(
                    broker_positions,
                    market_session=get_market_session(),
                    reason="sigint_flatten",
                )
        except Exception as exc:
            print(f"[WARNING] Close-all on exit failed: {exc}")
    should_exit = True
    shutdown_event.set()


signal.signal(signal.SIGINT, signal_handler)


def entries_allowed_in_current_session() -> bool:
    current_session = get_market_session()
    if config.SNIPER_REGULAR_HOURS_ONLY:
        return current_session == "REGULAR"
    return current_session in {"PREMARKET", "REGULAR", "AFTERHOURS"}


def _session_close_cleanup_marker(now_et: datetime) -> Optional[str]:
    if now_et.hour == 9 and now_et.minute == 29:
        return "PREMARKET_CLOSE"
    if now_et.hour == 15 and now_et.minute == 59:
        return "REGULAR_CLOSE"
    if now_et.hour == 19 and now_et.minute == 59:
        return "AFTERHOURS_CLOSE"
    return None


def _floor_price_to_cent(price: float) -> float:
    return math.floor(price * 100.0) / 100.0


def _compute_buffered_structure_stop(current_price: float, chosen_stop_ref: Optional[float]) -> Optional[float]:
    if chosen_stop_ref is None or current_price <= 0:
        return None

    percent_buffer = chosen_stop_ref * (config.SNIPER_STOP_BUFFER_PCT / 100.0)
    absolute_buffer = max(0.0, config.SNIPER_STOP_BUFFER_MIN_DOLLARS)
    buffer_dollars = max(percent_buffer, absolute_buffer)
    raw_stop = chosen_stop_ref - buffer_dollars
    stop_price = _floor_price_to_cent(raw_stop)

    if stop_price >= chosen_stop_ref:
        stop_price = _floor_price_to_cent(chosen_stop_ref - 0.01)

    if stop_price <= 0:
        return None

    return stop_price


def _select_structure_stop(
    current_price: float,
    stop_ref_candidates: List[Optional[float]],
    min_distance_pct: float,
    max_distance_pct: float,
) -> tuple[Optional[float], Optional[float], Optional[float]]:
    valid_refs = sorted(
        {
            float(candidate)
            for candidate in stop_ref_candidates
            if candidate is not None and current_price > 0 and 0 < float(candidate) < current_price
        },
        reverse=True,
    )
    closest_candidate = (None, None, None)

    for stop_ref in valid_refs:
        stop_price = _compute_buffered_structure_stop(current_price, stop_ref)
        if stop_price is None:
            continue
        stop_distance_pct = ((current_price - stop_price) / current_price) * 100.0
        if closest_candidate == (None, None, None):
            closest_candidate = (stop_price, stop_distance_pct, stop_ref)
        if min_distance_pct <= stop_distance_pct <= max_distance_pct:
            return stop_price, stop_distance_pct, stop_ref

    return closest_candidate


def _format_display_price(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    rounded_2 = round(value, 2)
    if abs(value - rounded_2) < 0.00005:
        return f"${rounded_2:.2f}"
    return f"${value:.4f}".rstrip("0").rstrip(".")


def _next_main_loop_start(now_et: datetime) -> datetime:
    start_today = now_et.replace(hour=4, minute=0, second=0, microsecond=0)
    if now_et < start_today:
        return start_today
    return now_et


def _next_tradable_session_start(now_et: datetime) -> datetime:
    candidate = now_et.replace(hour=4, minute=0, second=0, microsecond=0)
    if now_et >= candidate:
        candidate += timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate


def _bucket_start(ts: datetime, timeframe_seconds: int) -> datetime:
    epoch = int(ts.timestamp())
    bucket = epoch - (epoch % timeframe_seconds)
    return datetime.fromtimestamp(bucket)


def _compute_ema(values: List[float], period: int) -> Optional[float]:
    if not values:
        return None
    if period <= 1:
        return values[-1]
    alpha = 2.0 / (period + 1.0)
    ema = values[0]
    for value in values[1:]:
        ema = (value * alpha) + (ema * (1 - alpha))
    return ema


def _compute_ema_series(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    if period <= 1:
        return list(values)
    alpha = 2.0 / (period + 1.0)
    series = [values[0]]
    ema = values[0]
    for value in values[1:]:
        ema = (value * alpha) + (ema * (1 - alpha))
        series.append(ema)
    return series


def _candle_is_green(candle: Dict[str, Any]) -> bool:
    return candle["close"] >= candle["open"]


def _candle_body_pct(candle: Dict[str, Any]) -> float:
    open_price = candle.get("open", 0.0) or 0.0
    if open_price <= 0:
        return 0.0
    return abs((candle["close"] - candle["open"]) / open_price) * 100.0


def _evaluate_parabolic_progression(
    main_candles: List[Dict[str, Any]],
    confirm_candles: List[Dict[str, Any]],
    *,
    current_price: float,
    main_label: str,
    confirm_label: str,
) -> Dict[str, Any]:
    result = {
        "passed": False,
        "failed_checks": [],
        "metrics": {},
    }

    if len(main_candles) < 3:
        result["failed_checks"].append(f"Need 3 {main_label} candles for parabolic confirmation")
        return result
    if len(confirm_candles) < 3:
        result["failed_checks"].append(f"Need 3 {confirm_label} candles for parabolic confirmation")
        return result

    recent_main = main_candles[-3:]
    recent_confirm = confirm_candles[-3:]
    latest_main = recent_main[-1]
    prior_main = recent_main[-2]

    latest_main_body_pct = _candle_body_pct(latest_main)
    prior_main_body_pct = _candle_body_pct(prior_main)
    latest_main_volume = float(latest_main.get("volume", 0.0) or 0.0)
    prior_main_volume = float(prior_main.get("volume", 0.0) or 0.0)
    latest_main_dollar_volume = latest_main_volume * max(0.0, current_price)

    main_all_green = all(_candle_is_green(candle) for candle in recent_main)
    main_advancing = all(
        recent_main[idx]["close"] > recent_main[idx - 1]["close"]
        and recent_main[idx]["high"] > recent_main[idx - 1]["high"]
        for idx in range(1, len(recent_main))
    )
    confirm_all_green = all(_candle_is_green(candle) for candle in recent_confirm)
    confirm_advancing = all(
        recent_confirm[idx]["close"] >= recent_confirm[idx - 1]["close"]
        and recent_confirm[idx]["high"] >= recent_confirm[idx - 1]["high"]
        for idx in range(1, len(recent_confirm))
    )
    latest_main_body_pass = latest_main_body_pct >= max(
        config.SNIPER_PARABOLIC_MAIN_MIN_BODY_PCT,
        prior_main_body_pct * config.SNIPER_PARABOLIC_MAIN_BODY_MULTIPLIER,
    )
    latest_main_volume_pass = (
        prior_main_volume > 0
        and latest_main_volume >= prior_main_volume * config.SNIPER_PARABOLIC_MAIN_VOLUME_MULTIPLIER
        and latest_main_dollar_volume >= config.SNIPER_PARABOLIC_MAIN_MIN_DOLLAR_VOLUME
    )

    if not main_all_green:
        result["failed_checks"].append(f"{main_label} parabolic sequence needs 3 green candles")
    if not main_advancing:
        result["failed_checks"].append(f"{main_label} parabolic sequence needs rising highs/closes")
    if not latest_main_body_pass:
        result["failed_checks"].append(
            f"{main_label} latest body {latest_main_body_pct:.2f}% lacks parabolic expansion vs prior {prior_main_body_pct:.2f}%"
        )
    if not latest_main_volume_pass:
        result["failed_checks"].append(
            f"{main_label} latest volume {latest_main_volume:,.0f} lacks parabolic expansion vs prior {prior_main_volume:,.0f}"
        )
    if not confirm_all_green:
        result["failed_checks"].append(f"{confirm_label} confirmation needs 3 green candles")
    if not confirm_advancing:
        result["failed_checks"].append(f"{confirm_label} confirmation needs rising highs/closes")

    result["passed"] = not result["failed_checks"]
    result["metrics"] = {
        "main_label": main_label,
        "confirm_label": confirm_label,
        "latest_main_body_pct": latest_main_body_pct,
        "prior_main_body_pct": prior_main_body_pct,
        "latest_main_volume": latest_main_volume,
        "prior_main_volume": prior_main_volume,
        "latest_main_dollar_volume": latest_main_dollar_volume,
        "main_all_green": main_all_green,
        "main_advancing": main_advancing,
        "confirm_all_green": confirm_all_green,
        "confirm_advancing": confirm_advancing,
        "latest_main_body_pass": latest_main_body_pass,
        "latest_main_volume_pass": latest_main_volume_pass,
    }
    return result


def _compute_volume_rate(volume_history: deque, window_seconds: int) -> Optional[float]:
    if len(volume_history) < 2 or window_seconds <= 0:
        return None

    latest_time, latest_volume = volume_history[-1]
    cutoff = latest_time.timestamp() - float(window_seconds)
    baseline_time, baseline_volume = volume_history[0]

    for ts, cumulative_volume in reversed(volume_history):
        if ts.timestamp() <= cutoff:
            baseline_time, baseline_volume = ts, cumulative_volume
            break

    elapsed_seconds = (latest_time - baseline_time).total_seconds()
    if elapsed_seconds <= 0:
        return None

    volume_delta = max(0.0, latest_volume - baseline_volume)
    return volume_delta / elapsed_seconds


def _slice_contiguous_history(
    history: deque,
    max_gap_seconds: int,
) -> List[Any]:
    if not history:
        return []

    contiguous = [history[-1]]
    for idx in range(len(history) - 2, -1, -1):
        current_ts, _ = history[idx]
        next_ts, _ = history[idx + 1]
        if (next_ts - current_ts).total_seconds() > max_gap_seconds:
            break
        contiguous.append(history[idx])
    contiguous.reverse()
    return contiguous


def _build_candles(
    price_history: deque,
    volume_history: deque,
    timeframe_seconds: int,
    candle_count: int,
) -> List[Dict[str, Any]]:
    if not price_history:
        return []

    latest_time = price_history[-1][0]
    lookback_start = latest_time - timedelta(seconds=timeframe_seconds * (candle_count + 3))
    recent_prices = [(ts, price) for ts, price in price_history if ts >= lookback_start]
    recent_volumes = [(ts, volume) for ts, volume in volume_history if ts >= lookback_start]
    if not recent_prices:
        return []

    candles: Dict[datetime, Dict[str, Any]] = {}
    for ts, price in recent_prices:
        bucket = _bucket_start(ts, timeframe_seconds)
        candle = candles.get(bucket)
        if candle is None:
            candles[bucket] = {
                "start": bucket,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 0.0,
            }
            continue
        candle["high"] = max(candle["high"], price)
        candle["low"] = min(candle["low"], price)
        candle["close"] = price

    if len(recent_volumes) >= 2:
        _, prev_volume = recent_volumes[0]
        for ts, cumulative_volume in recent_volumes[1:]:
            volume_delta = max(0.0, cumulative_volume - prev_volume)
            bucket = _bucket_start(ts, timeframe_seconds)
            candle = candles.get(bucket)
            if candle is not None:
                candle["volume"] += volume_delta
            prev_volume = cumulative_volume

    ordered = [candles[bucket] for bucket in sorted(candles.keys())]
    return ordered[-candle_count:]


def _find_recent_pivot_low(candles: List[Dict[str, Any]]) -> Optional[float]:
    if len(candles) < 3:
        return min((c["low"] for c in candles), default=None)

    completed = candles[:-1] if len(candles) >= 4 else candles
    for idx in range(len(completed) - 2, 0, -1):
        current = completed[idx]
        prev_candle = completed[idx - 1]
        next_candle = completed[idx + 1]
        if current["low"] < prev_candle["low"] and current["low"] <= next_candle["low"]:
            return current["low"]

    trailing = completed[-3:] if len(completed) >= 3 else completed
    return min((c["low"] for c in trailing), default=None)


def _find_recent_pivot_low_index(candles: List[Dict[str, Any]]) -> int:
    if len(candles) < 3:
        return max(0, len(candles) - 1)

    completed = candles[:-1] if len(candles) >= 4 else candles
    if not completed:
        return max(0, len(candles) - 1)

    peak_idx = max(range(len(completed)), key=lambda idx: completed[idx]["high"])
    for idx in range(peak_idx - 1, 0, -1):
        current = completed[idx]
        prev_candle = completed[idx - 1]
        next_candle = completed[idx + 1]
        if current["low"] < prev_candle["low"] and current["low"] <= next_candle["low"]:
            return idx

    return max(0, peak_idx - 2)


def _find_impulse_start_index(candles_15s: List[Dict[str, Any]], ema15_series: List[float]) -> int:
    if not candles_15s:
        return 0

    reclaim_idx = 0
    for idx in range(len(candles_15s) - 2, -1, -1):
        if idx < len(ema15_series) and candles_15s[idx]["close"] <= ema15_series[idx]:
            reclaim_idx = idx + 1
            break

    pivot_idx = _find_recent_pivot_low_index(candles_15s)
    start_idx = max(reclaim_idx, pivot_idx)
    return min(start_idx, max(0, len(candles_15s) - 1))


def _empty_clean_analysis() -> Dict[str, Any]:
    return {
        "enough_data": False,
        "clean_passed": False,
        "extreme_clean": False,
        "entry_ready": False,
        "reasons": [],
        "failed_checks": [],
        "first_failed_check": "",
        "entry_reason": "",
        "support_price": None,
        "support_label": "",
        "stop_price": None,
        "pullback_low": None,
        "ema_value": None,
        "ema_label": "",
        "impulse_start": None,
        "structure_mode": "",
        "metrics": {},
    }


def _score_clean_analysis(analysis: Dict[str, Any]) -> tuple:
    metrics = analysis.get("metrics", {})
    return (
        1 if analysis.get("entry_ready") else 0,
        1 if analysis.get("extreme_clean") else 0,
        1 if analysis.get("clean_passed") else 0,
        -len(analysis.get("failed_checks", [])),
        float(metrics.get("volume_expansion", 0.0) or 0.0),
        -float(metrics.get("retracement_pct_of_impulse", 1000.0) or 1000.0),
    )


def _clone_clean_analysis(analysis: Dict[str, Any]) -> Dict[str, Any]:
    cloned = dict(analysis)
    cloned["reasons"] = list(analysis.get("reasons", []))
    cloned["failed_checks"] = list(analysis.get("failed_checks", []))
    cloned["metrics"] = dict(analysis.get("metrics", {}))
    return cloned


def _classify_timeframe_alignment(analysis: Dict[str, Any]) -> Dict[str, Any]:
    structure_mode = analysis.get("structure_mode") or "unknown"
    failed_checks = list(analysis.get("failed_checks", []))
    metrics = analysis.get("metrics", {})
    contradictory_reasons: List[str] = []

    if structure_mode == "15s/30s":
        contradiction_prefixes = (
            "Retrace ",
            "15s red fraction ",
            "30s red candles ",
        )
        contradiction_exact = {
            "Back-to-back red 30s candles",
            "Price below EMA15",
            "Price below 15s support",
            "Impulse not advancing on 15s/30s",
        }
    elif structure_mode == "1m":
        contradiction_prefixes = (
            "Retrace ",
            "1m red candles ",
        )
        contradiction_exact = {
            "Back-to-back red 1m candles",
            "Price below EMA1m",
            "Price below 1m support",
            "Impulse not advancing on 1m",
        }
    else:
        contradiction_prefixes = tuple()
        contradiction_exact = set()

    for failed_check in failed_checks:
        if failed_check.startswith(contradiction_prefixes) or failed_check in contradiction_exact:
            contradictory_reasons.append(failed_check)

    fade_from_peak_pct = float(metrics.get("fade_from_peak_pct", 0.0) or 0.0)
    allowed_pullback_pct = float(metrics.get("allowed_pullback_pct", 0.0) or 0.0)
    if allowed_pullback_pct > 0 and fade_from_peak_pct > allowed_pullback_pct:
        contradictory_reasons.append(
            f"Total fade {fade_from_peak_pct:.2f}% exceeds cap {allowed_pullback_pct:.2f}%"
        )

    contradictory_reasons = list(dict.fromkeys(contradictory_reasons))
    if contradictory_reasons:
        return {
            "structure_mode": structure_mode,
            "status": "contradictory",
            "reasons": contradictory_reasons,
        }

    if analysis.get("clean_passed"):
        return {
            "structure_mode": structure_mode,
            "status": "supportive",
            "reasons": [],
        }

    return {
        "structure_mode": structure_mode,
        "status": "neutral",
        "reasons": [],
    }


def _get_allowed_sniper_pullback_pct(extension_pct: float) -> float:
    return min(
        config.SNIPER_PULLBACK_MAX_CAP_PCT,
        max(0.0, extension_pct * config.SNIPER_PULLBACK_EXTENSION_FACTOR),
    )


def _get_allowed_post_entry_adverse_pct(extension_pct: float) -> float:
    return min(
        config.SNIPER_POST_ENTRY_MAX_ADVERSE_CAP_PCT,
        max(
            config.SNIPER_POST_ENTRY_MAX_ADVERSE_PCT,
            extension_pct * config.SNIPER_POST_ENTRY_ADVERSE_EXTENSION_FACTOR,
        ),
    )


def _compute_flush_drop_pct(
    price_history: List[Any],
    now: datetime,
    current_price: float,
) -> tuple[float, Optional[float]]:
    lookback_start = now - timedelta(seconds=config.SNIPER_FLUSH_ENTRY_LOOKBACK_SECONDS)
    recent_prices = [price for ts, price in price_history if ts >= lookback_start]
    if len(recent_prices) < 2:
        return 0.0, None

    recent_high = max(recent_prices)
    if recent_high <= 0:
        return 0.0, None
    flush_drop_pct = max(0.0, ((recent_high - current_price) / recent_high) * 100.0)
    return flush_drop_pct, recent_high


def _analyze_fast_clean_move(
    state: SniperSymbolState,
    contiguous_price_history: List[Any],
    contiguous_volume_history: List[Any],
) -> Dict[str, Any]:
    analysis = _empty_clean_analysis()
    analysis["structure_mode"] = "15s/30s"

    candles_15s = _build_candles(
        contiguous_price_history,
        contiguous_volume_history,
        timeframe_seconds=15,
        candle_count=config.SNIPER_15S_WINDOW_CANDLES,
    )
    candles_30s = _build_candles(
        contiguous_price_history,
        contiguous_volume_history,
        timeframe_seconds=30,
        candle_count=config.SNIPER_30S_WINDOW_CANDLES,
    )
    candles_1m = _build_candles(
        contiguous_price_history,
        contiguous_volume_history,
        timeframe_seconds=60,
        candle_count=max(4, config.SNIPER_MIN_IMPULSE_1M_CANDLES + 1),
    )
    if len(candles_15s) < 4 or len(candles_30s) < 3:
        analysis["reasons"].append("Waiting for 15s/30s structure")
        return analysis

    analysis["enough_data"] = True
    current_price = state.price
    closes_15s = [candle["close"] for candle in candles_15s]
    ema15_series = _compute_ema_series(closes_15s, config.SNIPER_15S_EMA_PERIOD)
    ema15 = ema15_series[-1] if ema15_series else None
    impulse_start_idx = _find_impulse_start_index(candles_15s, ema15_series)
    impulse_15s = candles_15s[impulse_start_idx:]
    impulse_start_time = impulse_15s[0]["start"] if impulse_15s else candles_15s[0]["start"]
    impulse_30s = [candle for candle in candles_30s if candle["start"] >= impulse_start_time]
    if len(impulse_15s) < config.SNIPER_MIN_IMPULSE_15S_CANDLES or len(impulse_30s) < config.SNIPER_MIN_IMPULSE_30S_CANDLES:
        analysis["reasons"].append(
            f"Waiting for impulse window ({len(impulse_15s)}x15s, {len(impulse_30s)}x30s)"
        )
        analysis["impulse_start"] = impulse_start_time
        return analysis

    support_price = _find_recent_pivot_low(impulse_15s)
    recent_peak = max(candle["high"] for candle in impulse_15s)
    impulse_low = min(candle["low"] for candle in impulse_15s)
    impulse_size = max(0.0, recent_peak - impulse_low)
    retracement_pct_of_impulse = 100.0
    if impulse_size > 0:
        retracement_pct_of_impulse = max(0.0, ((recent_peak - current_price) / impulse_size) * 100.0)

    red_15_count = sum(1 for candle in impulse_15s if candle["close"] < candle["open"])
    red_30_count = sum(1 for candle in impulse_30s if candle["close"] < candle["open"])
    red_15_fraction = red_15_count / len(impulse_15s)
    has_back_to_back_red_30 = any(
        impulse_30s[idx]["close"] < impulse_30s[idx]["open"]
        and impulse_30s[idx - 1]["close"] < impulse_30s[idx - 1]["open"]
        for idx in range(1, len(impulse_30s))
    )

    current_15s_volume = impulse_15s[-1]["volume"]
    prior_15s_volumes = [candle["volume"] for candle in impulse_15s[:-1] if candle["volume"] > 0]
    avg_15s_volume = (sum(prior_15s_volumes) / len(prior_15s_volumes)) if prior_15s_volumes else 0.0
    volume_expansion = (current_15s_volume / avg_15s_volume) if avg_15s_volume > 0 else 0.0

    contiguous_volume_deque = deque(contiguous_volume_history, maxlen=len(contiguous_volume_history))
    vol_rate_5s = _compute_volume_rate(contiguous_volume_deque, 5)
    vol_rate_15s = _compute_volume_rate(contiguous_volume_deque, 15)
    vol_rate_30s = _compute_volume_rate(contiguous_volume_deque, 30)
    volume_accel_pass = (
        vol_rate_5s is not None
        and vol_rate_15s is not None
        and vol_rate_30s is not None
        and vol_rate_5s > vol_rate_15s > vol_rate_30s > 0
    )

    above_ema = ema15 is not None and current_price >= ema15
    above_support = support_price is not None and current_price >= support_price
    trend_advancing = impulse_15s[-1]["close"] > impulse_15s[0]["open"] and impulse_30s[-1]["close"] > impulse_30s[0]["open"]

    failed_checks: List[str] = []
    if retracement_pct_of_impulse > config.SNIPER_CLEAN_RETRACE_HARD_PCT:
        failed_checks.append(
            f"Retrace {retracement_pct_of_impulse:.1f}% > {config.SNIPER_CLEAN_RETRACE_HARD_PCT:.1f}% hard cutoff"
        )
    if red_15_fraction > config.SNIPER_15S_MAX_RED_FRACTION:
        failed_checks.append(
            f"15s red fraction {red_15_count}/{len(impulse_15s)} exceeds {config.SNIPER_15S_MAX_RED_FRACTION:.0%}"
        )
    if red_30_count > config.SNIPER_30S_MAX_RED_CANDLES:
        failed_checks.append(
            f"30s red candles {red_30_count}/{len(impulse_30s)} exceeds max {config.SNIPER_30S_MAX_RED_CANDLES}"
        )
    if has_back_to_back_red_30:
        failed_checks.append("Back-to-back red 30s candles")
    if volume_expansion < config.SNIPER_MIN_15S_VOLUME_EXPANSION:
        failed_checks.append(
            f"15s volume expansion {volume_expansion:.2f}x < {config.SNIPER_MIN_15S_VOLUME_EXPANSION:.2f}x"
        )
    if not volume_accel_pass:
        failed_checks.append("Vol accel failed (need 5s > 15s > 30s)")
    if not above_ema:
        failed_checks.append("Price below EMA15")
    if not above_support:
        failed_checks.append("Price below 15s support")
    if not trend_advancing:
        failed_checks.append("Impulse not advancing on 15s/30s")

    clean_passed = not failed_checks
    base_extreme_clean = (
        clean_passed
        and retracement_pct_of_impulse <= config.SNIPER_CLEAN_RETRACE_PREFERRED_PCT
        and red_30_count == 0
    )
    parabolic_check = _evaluate_parabolic_progression(
        main_candles=impulse_30s,
        confirm_candles=impulse_15s,
        current_price=current_price,
        main_label="30s",
        confirm_label="15s",
    )
    one_minute_no_red_flag = (
        len(candles_1m) >= 2
        and all(candle["close"] >= candle["open"] for candle in candles_1m[-2:])
        and candles_1m[-1]["close"] >= candles_1m[-2]["close"]
    )
    parabolic_failed_checks = list(parabolic_check["failed_checks"])
    if not one_minute_no_red_flag:
        parabolic_failed_checks.append("1m confirmation shows a red flag")
    extreme_clean = base_extreme_clean and not parabolic_failed_checks

    recent_prices_5s = [price for ts, price in contiguous_price_history if ts >= state.last_update - timedelta(seconds=5)]
    pullback_low = min(recent_prices_5s) if recent_prices_5s else current_price
    fade_from_peak_pct = ((recent_peak - current_price) / recent_peak) * 100 if recent_peak > 0 else 0.0
    extension_pct = ((recent_peak - impulse_low) / impulse_low) * 100 if impulse_low > 0 else 0.0
    allowed_pullback_pct = _get_allowed_sniper_pullback_pct(extension_pct)
    flush_drop_pct, flush_reference_price = _compute_flush_drop_pct(
        contiguous_price_history,
        state.last_update,
        current_price,
    )
    flush_triggered = flush_drop_pct >= config.SNIPER_FLUSH_ENTRY_MIN_DROP_PCT
    pullback_in_range = fade_from_peak_pct <= allowed_pullback_pct

    stop_ref_candidates = [pullback_low, support_price]
    stop_price, stop_distance_pct, chosen_stop_ref = _select_structure_stop(
        current_price,
        stop_ref_candidates,
        config.SNIPER_MIN_STOP_DISTANCE_PCT,
        config.SNIPER_MAX_STOP_DISTANCE_PCT,
    )

    valid_stop = (
        stop_price is not None
        and stop_distance_pct is not None
        and config.SNIPER_MIN_STOP_DISTANCE_PCT <= stop_distance_pct <= config.SNIPER_MAX_STOP_DISTANCE_PCT
    )
    continuation_stop_valid = (
        stop_price is not None
        and stop_distance_pct is not None
        and config.SNIPER_MIN_STOP_DISTANCE_PCT
        <= stop_distance_pct
        <= config.SNIPER_CONTINUATION_MAX_STOP_DISTANCE_PCT
    )
    flush_entry_ready = clean_passed and pullback_in_range and flush_triggered and valid_stop
    continuation_entry_ready = extreme_clean and pullback_in_range and (not flush_triggered) and continuation_stop_valid
    entry_ready = flush_entry_ready or continuation_entry_ready
    if not failed_checks:
        if flush_entry_ready or continuation_entry_ready:
            analysis["first_failed_check"] = ""
        elif flush_triggered and not valid_stop:
            analysis["first_failed_check"] = "Stop distance invalid for flush entry"
        elif (not flush_triggered) and base_extreme_clean and parabolic_failed_checks:
            analysis["first_failed_check"] = parabolic_failed_checks[0]
        elif base_extreme_clean and parabolic_failed_checks:
            analysis["first_failed_check"] = ""
        elif not flush_triggered and continuation_stop_valid:
            analysis["first_failed_check"] = ""
        elif not flush_triggered:
            analysis["first_failed_check"] = (
                f"Waiting for sudden flush >= {config.SNIPER_FLUSH_ENTRY_MIN_DROP_PCT:.2f}% "
                f"over {config.SNIPER_FLUSH_ENTRY_LOOKBACK_SECONDS:.1f}s or continuation stop <= "
                f"{config.SNIPER_CONTINUATION_MAX_STOP_DISTANCE_PCT:.2f}%"
            )
        elif not pullback_in_range:
            analysis["first_failed_check"] = (
                f"Total fade {fade_from_peak_pct:.2f}% exceeds cap {allowed_pullback_pct:.2f}%"
            )
        elif flush_triggered and not valid_stop:
            analysis["first_failed_check"] = "Stop distance invalid for flush entry"

    analysis["clean_passed"] = clean_passed
    analysis["extreme_clean"] = extreme_clean
    analysis["entry_ready"] = entry_ready
    analysis["failed_checks"] = failed_checks
    if not analysis["first_failed_check"]:
        analysis["first_failed_check"] = failed_checks[0] if failed_checks else ""
    analysis["support_price"] = support_price
    analysis["support_label"] = "15s support"
    analysis["pullback_low"] = pullback_low
    analysis["ema_value"] = ema15
    analysis["ema_label"] = "EMA15"
    analysis["stop_price"] = stop_price
    analysis["impulse_start"] = impulse_start_time
    analysis["metrics"] = {
        "structure_mode": "15s/30s",
        "impulse_15s_candles": len(impulse_15s),
        "impulse_30s_candles": len(impulse_30s),
        "retracement_pct_of_impulse": retracement_pct_of_impulse,
        "red_15_count": red_15_count,
        "red_30_count": red_30_count,
        "volume_expansion": volume_expansion,
        "vol_rate_5s": vol_rate_5s,
        "vol_rate_15s": vol_rate_15s,
        "vol_rate_30s": vol_rate_30s,
        "extension_pct": extension_pct,
        "fade_from_peak_pct": fade_from_peak_pct,
        "allowed_pullback_pct": allowed_pullback_pct,
        "flush_drop_pct": flush_drop_pct,
        "flush_reference_price": flush_reference_price,
        "stop_distance_pct": stop_distance_pct,
        "chosen_stop_ref": chosen_stop_ref,
        "flush_entry_ready": flush_entry_ready,
        "continuation_entry_ready": continuation_entry_ready,
        "entry_style": "flush" if flush_entry_ready else ("continuation" if continuation_entry_ready else ""),
        "base_extreme_clean": base_extreme_clean,
        "parabolic_check_passed": parabolic_check["passed"] and one_minute_no_red_flag,
        "parabolic_main_label": "30s",
        "parabolic_confirm_label": "15s",
        "parabolic_one_minute_no_red_flag": one_minute_no_red_flag,
        **{f"parabolic_{key}": value for key, value in parabolic_check["metrics"].items()},
    }
    analysis["reasons"] = [
        "Mode 15s/30s",
        f"Impulse start {impulse_start_time.strftime('%H:%M:%S')}",
        f"15s retrace {retracement_pct_of_impulse:.1f}% of impulse",
        f"15s red {red_15_count}/{len(impulse_15s)}",
        f"30s red {red_30_count}/{len(impulse_30s)}",
        f"Extension {extension_pct:.1f}% | pullback cap {allowed_pullback_pct:.1f}%",
        (
            f"Flush {flush_drop_pct:.2f}% over {config.SNIPER_FLUSH_ENTRY_LOOKBACK_SECONDS:.1f}s"
            if flush_reference_price is not None
            else "Waiting for sudden flush trigger"
        ),
        f"15s vol {volume_expansion:.2f}x avg",
        "Vol accel 5s>15s>30s" if volume_accel_pass else "Vol accel failed",
        (
            f"15s support ${support_price:.2f}, EMA15 ${ema15:.2f}"
            if support_price is not None and ema15 is not None
            else "15s support/EMA not ready"
        ),
    ]
    if failed_checks:
        analysis["reasons"].append(f"First fail: {failed_checks[0]}")
    elif base_extreme_clean and parabolic_failed_checks:
        analysis["reasons"].append(f"Parabolic fail: {parabolic_failed_checks[0]}")
    if flush_entry_ready:
        analysis["entry_reason"] = (
            f"15s/30s clean trend, sudden flush {flush_drop_pct:.2f}% from short-term high, "
            f"total fade {fade_from_peak_pct:.2f}% from peak, stop ${stop_price:.2f}"
        )
    elif continuation_entry_ready:
        analysis["entry_reason"] = (
            f"15s/30s extreme clean continuation, pullback {fade_from_peak_pct:.2f}% from peak, "
            f"stop ${stop_price:.2f} ({stop_distance_pct:.2f}% risk)"
        )
    return analysis


def _analyze_one_minute_clean_move(
    state: SniperSymbolState,
    contiguous_price_history: List[Any],
    contiguous_volume_history: List[Any],
) -> Dict[str, Any]:
    analysis = _empty_clean_analysis()
    analysis["structure_mode"] = "1m"
    if not config.SNIPER_1M_ENABLED:
        analysis["reasons"].append("1m clean-squeeze mode disabled")
        return analysis

    candles_1m = _build_candles(
        contiguous_price_history,
        contiguous_volume_history,
        timeframe_seconds=60,
        candle_count=config.SNIPER_1M_WINDOW_CANDLES,
    )
    candles_30s = _build_candles(
        contiguous_price_history,
        contiguous_volume_history,
        timeframe_seconds=30,
        candle_count=max(4, config.SNIPER_30S_WINDOW_CANDLES),
    )
    candles_15s = _build_candles(
        contiguous_price_history,
        contiguous_volume_history,
        timeframe_seconds=15,
        candle_count=max(4, config.SNIPER_15S_WINDOW_CANDLES),
    )
    if len(candles_1m) < 4:
        analysis["reasons"].append("Waiting for 1m structure")
        return analysis

    analysis["enough_data"] = True
    current_price = state.price
    closes_1m = [candle["close"] for candle in candles_1m]
    ema_1m_series = _compute_ema_series(closes_1m, config.SNIPER_1M_EMA_PERIOD)
    ema_1m = ema_1m_series[-1] if ema_1m_series else None
    impulse_start_idx = _find_impulse_start_index(candles_1m, ema_1m_series)
    impulse_1m = candles_1m[impulse_start_idx:]
    impulse_start_time = impulse_1m[0]["start"] if impulse_1m else candles_1m[0]["start"]
    if len(impulse_1m) < config.SNIPER_MIN_IMPULSE_1M_CANDLES:
        analysis["reasons"].append(f"Waiting for 1m impulse window ({len(impulse_1m)}x1m)")
        analysis["impulse_start"] = impulse_start_time
        return analysis

    support_price = _find_recent_pivot_low(impulse_1m)
    recent_peak = max(candle["high"] for candle in impulse_1m)
    impulse_low = min(candle["low"] for candle in impulse_1m)
    impulse_size = max(0.0, recent_peak - impulse_low)
    retracement_pct_of_impulse = 100.0
    if impulse_size > 0:
        retracement_pct_of_impulse = max(0.0, ((recent_peak - current_price) / impulse_size) * 100.0)

    red_1m_count = sum(1 for candle in impulse_1m if candle["close"] < candle["open"])
    has_back_to_back_red_1m = any(
        impulse_1m[idx]["close"] < impulse_1m[idx]["open"]
        and impulse_1m[idx - 1]["close"] < impulse_1m[idx - 1]["open"]
        for idx in range(1, len(impulse_1m))
    )

    current_1m_volume = impulse_1m[-1]["volume"]
    prior_1m_volumes = [candle["volume"] for candle in impulse_1m[:-1] if candle["volume"] > 0]
    avg_1m_volume = (sum(prior_1m_volumes) / len(prior_1m_volumes)) if prior_1m_volumes else 0.0
    volume_expansion = (current_1m_volume / avg_1m_volume) if avg_1m_volume > 0 else 0.0

    contiguous_volume_deque = deque(contiguous_volume_history, maxlen=len(contiguous_volume_history))
    vol_rate_15s = _compute_volume_rate(contiguous_volume_deque, 15)
    vol_rate_30s = _compute_volume_rate(contiguous_volume_deque, 30)
    vol_rate_60s = _compute_volume_rate(contiguous_volume_deque, 60)
    volume_accel_pass = (
        vol_rate_15s is not None
        and vol_rate_30s is not None
        and vol_rate_60s is not None
        and vol_rate_15s > vol_rate_30s > vol_rate_60s > 0
    )

    above_ema = ema_1m is not None and current_price >= ema_1m
    above_support = support_price is not None and current_price >= support_price
    trend_advancing = impulse_1m[-1]["close"] > impulse_1m[0]["open"]

    failed_checks: List[str] = []
    if retracement_pct_of_impulse > config.SNIPER_CLEAN_RETRACE_HARD_PCT:
        failed_checks.append(
            f"Retrace {retracement_pct_of_impulse:.1f}% > {config.SNIPER_CLEAN_RETRACE_HARD_PCT:.1f}% hard cutoff"
        )
    if red_1m_count > config.SNIPER_1M_MAX_RED_CANDLES:
        failed_checks.append(
            f"1m red candles {red_1m_count}/{len(impulse_1m)} exceeds max {config.SNIPER_1M_MAX_RED_CANDLES}"
        )
    if has_back_to_back_red_1m:
        failed_checks.append("Back-to-back red 1m candles")
    if volume_expansion < config.SNIPER_MIN_1M_VOLUME_EXPANSION:
        failed_checks.append(
            f"1m volume expansion {volume_expansion:.2f}x < {config.SNIPER_MIN_1M_VOLUME_EXPANSION:.2f}x"
        )
    if not volume_accel_pass:
        failed_checks.append("Vol accel failed (need 15s > 30s > 60s)")
    if not above_ema:
        failed_checks.append("Price below EMA1m")
    if not above_support:
        failed_checks.append("Price below 1m support")
    if not trend_advancing:
        failed_checks.append("Impulse not advancing on 1m")

    clean_passed = not failed_checks
    base_extreme_clean = (
        clean_passed
        and retracement_pct_of_impulse <= config.SNIPER_CLEAN_RETRACE_PREFERRED_PCT
        and red_1m_count == 0
    )
    parabolic_check = _evaluate_parabolic_progression(
        main_candles=impulse_1m,
        confirm_candles=candles_30s,
        current_price=current_price,
        main_label="1m",
        confirm_label="30s",
    )
    fifteen_second_no_red_flag = (
        len(candles_15s) >= 3
        and all(candle["close"] >= candle["open"] for candle in candles_15s[-3:])
        and candles_15s[-1]["close"] >= candles_15s[-2]["close"] >= candles_15s[-3]["close"]
    )
    parabolic_failed_checks = list(parabolic_check["failed_checks"])
    if not fifteen_second_no_red_flag:
        parabolic_failed_checks.append("15s confirmation shows a red flag")
    extreme_clean = base_extreme_clean and not parabolic_failed_checks

    recent_prices_5s = [price for ts, price in contiguous_price_history if ts >= state.last_update - timedelta(seconds=5)]
    pullback_low = min(recent_prices_5s) if recent_prices_5s else current_price
    fade_from_peak_pct = ((recent_peak - current_price) / recent_peak) * 100 if recent_peak > 0 else 0.0
    extension_pct = ((recent_peak - impulse_low) / impulse_low) * 100 if impulse_low > 0 else 0.0
    allowed_pullback_pct = _get_allowed_sniper_pullback_pct(extension_pct)
    flush_drop_pct, flush_reference_price = _compute_flush_drop_pct(
        contiguous_price_history,
        state.last_update,
        current_price,
    )
    flush_triggered = flush_drop_pct >= config.SNIPER_FLUSH_ENTRY_MIN_DROP_PCT
    pullback_in_range = fade_from_peak_pct <= allowed_pullback_pct

    stop_ref_candidates = [pullback_low, support_price]
    stop_price, stop_distance_pct, chosen_stop_ref = _select_structure_stop(
        current_price,
        stop_ref_candidates,
        config.SNIPER_MIN_STOP_DISTANCE_PCT,
        config.SNIPER_MAX_STOP_DISTANCE_PCT,
    )

    valid_stop = (
        stop_price is not None
        and stop_distance_pct is not None
        and config.SNIPER_MIN_STOP_DISTANCE_PCT <= stop_distance_pct <= config.SNIPER_MAX_STOP_DISTANCE_PCT
    )
    continuation_stop_valid = (
        stop_price is not None
        and stop_distance_pct is not None
        and config.SNIPER_MIN_STOP_DISTANCE_PCT
        <= stop_distance_pct
        <= config.SNIPER_CONTINUATION_MAX_STOP_DISTANCE_PCT
    )
    flush_entry_ready = clean_passed and pullback_in_range and flush_triggered and valid_stop
    continuation_entry_ready = extreme_clean and pullback_in_range and (not flush_triggered) and continuation_stop_valid
    entry_ready = flush_entry_ready or continuation_entry_ready
    if not failed_checks:
        if flush_entry_ready or continuation_entry_ready:
            analysis["first_failed_check"] = ""
        elif flush_triggered and not valid_stop:
            analysis["first_failed_check"] = "Stop distance invalid for flush entry"
        elif (not flush_triggered) and base_extreme_clean and parabolic_failed_checks:
            analysis["first_failed_check"] = parabolic_failed_checks[0]
        elif base_extreme_clean and parabolic_failed_checks:
            analysis["first_failed_check"] = ""
        elif not flush_triggered and continuation_stop_valid:
            analysis["first_failed_check"] = ""
        elif not flush_triggered:
            analysis["first_failed_check"] = (
                f"Waiting for sudden flush >= {config.SNIPER_FLUSH_ENTRY_MIN_DROP_PCT:.2f}% "
                f"over {config.SNIPER_FLUSH_ENTRY_LOOKBACK_SECONDS:.1f}s or continuation stop <= "
                f"{config.SNIPER_CONTINUATION_MAX_STOP_DISTANCE_PCT:.2f}%"
            )
        elif not pullback_in_range:
            analysis["first_failed_check"] = (
                f"Total fade {fade_from_peak_pct:.2f}% exceeds cap {allowed_pullback_pct:.2f}%"
            )
        elif flush_triggered and not valid_stop:
            analysis["first_failed_check"] = "Stop distance invalid for flush entry"

    analysis["clean_passed"] = clean_passed
    analysis["extreme_clean"] = extreme_clean
    analysis["entry_ready"] = entry_ready
    analysis["failed_checks"] = failed_checks
    if not analysis["first_failed_check"]:
        analysis["first_failed_check"] = failed_checks[0] if failed_checks else ""
    analysis["support_price"] = support_price
    analysis["support_label"] = "1m support"
    analysis["pullback_low"] = pullback_low
    analysis["ema_value"] = ema_1m
    analysis["ema_label"] = "EMA1m"
    analysis["stop_price"] = stop_price
    analysis["impulse_start"] = impulse_start_time
    analysis["metrics"] = {
        "structure_mode": "1m",
        "impulse_1m_candles": len(impulse_1m),
        "retracement_pct_of_impulse": retracement_pct_of_impulse,
        "red_1m_count": red_1m_count,
        "volume_expansion": volume_expansion,
        "vol_rate_15s": vol_rate_15s,
        "vol_rate_30s": vol_rate_30s,
        "vol_rate_60s": vol_rate_60s,
        "extension_pct": extension_pct,
        "fade_from_peak_pct": fade_from_peak_pct,
        "allowed_pullback_pct": allowed_pullback_pct,
        "flush_drop_pct": flush_drop_pct,
        "flush_reference_price": flush_reference_price,
        "stop_distance_pct": stop_distance_pct,
        "chosen_stop_ref": chosen_stop_ref,
        "flush_entry_ready": flush_entry_ready,
        "continuation_entry_ready": continuation_entry_ready,
        "entry_style": "flush" if flush_entry_ready else ("continuation" if continuation_entry_ready else ""),
        "base_extreme_clean": base_extreme_clean,
        "parabolic_check_passed": parabolic_check["passed"] and fifteen_second_no_red_flag,
        "parabolic_main_label": "1m",
        "parabolic_confirm_label": "30s",
        "parabolic_fifteen_second_no_red_flag": fifteen_second_no_red_flag,
        **{f"parabolic_{key}": value for key, value in parabolic_check["metrics"].items()},
    }
    analysis["reasons"] = [
        "Mode 1m squeeze",
        f"Impulse start {impulse_start_time.strftime('%H:%M:%S')}",
        f"1m retrace {retracement_pct_of_impulse:.1f}% of impulse",
        f"1m red {red_1m_count}/{len(impulse_1m)}",
        f"Extension {extension_pct:.1f}% | pullback cap {allowed_pullback_pct:.1f}%",
        (
            f"Flush {flush_drop_pct:.2f}% over {config.SNIPER_FLUSH_ENTRY_LOOKBACK_SECONDS:.1f}s"
            if flush_reference_price is not None
            else "Waiting for sudden flush trigger"
        ),
        f"1m vol {volume_expansion:.2f}x avg",
        "Vol accel 15s>30s>60s" if volume_accel_pass else "Vol accel failed",
        (
            f"1m support ${support_price:.2f}, EMA1m ${ema_1m:.2f}"
            if support_price is not None and ema_1m is not None
            else "1m support/EMA not ready"
        ),
    ]
    if failed_checks:
        analysis["reasons"].append(f"First fail: {failed_checks[0]}")
    elif base_extreme_clean and parabolic_failed_checks:
        analysis["reasons"].append(f"Parabolic fail: {parabolic_failed_checks[0]}")
    if flush_entry_ready:
        analysis["entry_reason"] = (
            f"1m clean squeeze, sudden flush {flush_drop_pct:.2f}% from short-term high, "
            f"total fade {fade_from_peak_pct:.2f}% from peak, stop ${stop_price:.2f}"
        )
    elif continuation_entry_ready:
        analysis["entry_reason"] = (
            f"1m extreme clean continuation, pullback {fade_from_peak_pct:.2f}% from peak, "
            f"stop ${stop_price:.2f} ({stop_distance_pct:.2f}% risk)"
        )
    return analysis


class SniperSymbolState:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.price_history = deque(maxlen=900)
        self.volume_history = deque(maxlen=900)
        self.price: Optional[float] = None
        self.previous_price: Optional[float] = None
        self.vwap: float = 0.0
        self.bid: float = 0.0
        self.ask: float = 0.0
        self.last_update: Optional[datetime] = None
        self.previous_update: Optional[datetime] = None
        self.last_gap_seconds: float = 0.0

    def update_market_data(
        self,
        price: float,
        volume: float,
        vwap: float,
        bid: float = 0.0,
        ask: float = 0.0,
    ) -> None:
        now = datetime.now()
        self.previous_price = self.price
        self.previous_update = self.last_update
        self.last_gap_seconds = (now - self.last_update).total_seconds() if self.last_update else 0.0
        self.price = price
        self.vwap = vwap
        self.bid = bid
        self.ask = ask
        self.last_update = now
        self.price_history.append((now, price))
        self.volume_history.append((now, volume))


def analyze_clean_move(state: SniperSymbolState) -> Dict[str, Any]:
    analysis = _empty_clean_analysis()
    if state.price is None or state.last_update is None or len(state.price_history) < 20 or len(state.volume_history) < 20:
        analysis["reasons"].append("Waiting for more live history")
        return analysis

    contiguous_price_history = _slice_contiguous_history(
        state.price_history,
        config.DYNAMIC_EXIT_MARKET_PAUSE_SUSPECT_SECONDS,
    )
    contiguous_volume_history = _slice_contiguous_history(
        state.volume_history,
        config.DYNAMIC_EXIT_MARKET_PAUSE_SUSPECT_SECONDS,
    )
    if len(contiguous_price_history) < 20 or len(contiguous_volume_history) < 20:
        analysis["reasons"].append("Waiting for post-gap history rebuild")
        if contiguous_price_history:
            analysis["impulse_start"] = contiguous_price_history[0][0]
        return analysis
    fast_analysis = _analyze_fast_clean_move(state, contiguous_price_history, contiguous_volume_history)
    candidate_analyses = [fast_analysis]
    if config.SNIPER_1M_ENABLED:
        candidate_analyses.append(
            _analyze_one_minute_clean_move(state, contiguous_price_history, contiguous_volume_history)
        )
    primary_analysis = _clone_clean_analysis(max(candidate_analyses, key=_score_clean_analysis))
    secondary_reviews = []
    contradictory_reviews = []

    for candidate_analysis in candidate_analyses:
        if candidate_analysis.get("structure_mode") == primary_analysis.get("structure_mode"):
            continue
        review = _classify_timeframe_alignment(candidate_analysis)
        secondary_reviews.append(review)
        if review["status"] == "contradictory":
            contradictory_reviews.append(review)

    primary_analysis["metrics"]["secondary_timeframe_reviews"] = secondary_reviews
    primary_analysis["metrics"]["secondary_timeframe_statuses"] = {
        review["structure_mode"]: review["status"] for review in secondary_reviews
    }
    if contradictory_reviews:
        primary_analysis["metrics"]["secondary_contradictions"] = contradictory_reviews

    if primary_analysis.get("entry_ready") and contradictory_reviews:
        veto_review = contradictory_reviews[0]
        veto_reason = (
            f"Secondary timeframe veto ({veto_review['structure_mode']}): "
            f"{'; '.join(veto_review['reasons'])}"
        )
        primary_analysis["entry_ready"] = False
        primary_analysis["entry_reason"] = ""
        primary_analysis["first_failed_check"] = veto_reason
        primary_analysis["reasons"].append(veto_reason)
        primary_analysis["metrics"]["secondary_timeframe_veto"] = True
    else:
        primary_analysis["metrics"]["secondary_timeframe_veto"] = False

    return primary_analysis


class CleanMomentumSniperManager:
    def __init__(self, telemetry: RuntimeTelemetry = None):
        self.pending_entries: Dict[str, Dict[str, Any]] = {}
        self.cooldowns: Dict[str, datetime] = {}
        self.structure_breaks: Dict[str, datetime] = {}
        self.telemetry = telemetry
        self.min_grade_rank = get_alert_grade_rank(config.SNIPER_MIN_ALERT_GRADE)

    def _log_event(self, event_type: str, **payload):
        if self.telemetry:
            self.telemetry.log_event(event_type, **payload)

    def _on_cooldown(self, symbol: str, now: datetime) -> bool:
        last_attempt = self.cooldowns.get(symbol)
        return last_attempt is not None and (now - last_attempt).total_seconds() < config.SNIPER_SYMBOL_COOLDOWN_SECONDS

    def _set_cooldown(self, symbol: str, now: datetime) -> None:
        self.cooldowns[symbol] = now

    def _compute_spread_pct(self, price: float, bid: float, ask: float) -> Optional[float]:
        if price <= 0 or bid <= 0 or ask <= 0:
            return None
        midpoint = (bid + ask) / 2.0
        if midpoint <= 0:
            return None
        return abs(ask - bid) / midpoint * 100.0

    def _classify_reopen(self, price: float, vwap: float, reference_price: float) -> str:
        if reference_price <= 0:
            return "unknown"
        strong_threshold = reference_price * (1 + config.DYNAMIC_EXIT_REOPEN_STRONG_BUFFER_PCT / 100.0)
        if price >= strong_threshold and (vwap <= 0 or price >= vwap):
            return "strong"
        return "weak"

    def _reset_pending_pause_state(self, candidate: Dict[str, Any]) -> None:
        candidate["paused_seconds"] = 0.0
        candidate["market_pause_state"] = "ACTIVE"
        candidate["market_pause_detected_at"] = None
        candidate["market_pause_gap_seconds"] = 0.0
        candidate["market_pause_reasons"] = []
        candidate["reopen_grace_until"] = None
        candidate["pre_pause_reference_price"] = None
        candidate["post_halt_classification"] = None

    def _get_effective_wait_seconds(self, candidate: Dict[str, Any], now: datetime) -> float:
        return max(
            0.0,
            (now - candidate["queued_at"]).total_seconds() - float(candidate.get("paused_seconds", 0.0)),
        )

    def _maybe_detect_pending_market_pause(
        self,
        symbol: str,
        candidate: Dict[str, Any],
        symbol_state: SniperSymbolState,
        now: datetime,
        filtered_events: deque,
    ) -> bool:
        if candidate.get("market_pause_state") == "HALT_CONFIRMED":
            return False

        gap_seconds = float(symbol_state.last_gap_seconds or 0.0)
        if gap_seconds < config.DYNAMIC_EXIT_MARKET_PAUSE_SUSPECT_SECONDS:
            return False

        price = symbol_state.price or 0.0
        bid = symbol_state.bid
        ask = symbol_state.ask
        prev_price = symbol_state.previous_price if symbol_state.previous_price and symbol_state.previous_price > 0 else price
        spread_pct = self._compute_spread_pct(price, bid, ask)
        missing_quotes = bid <= 0 or ask <= 0
        abnormal_spread = (
            spread_pct is not None and spread_pct >= config.DYNAMIC_EXIT_MARKET_PAUSE_ABNORMAL_SPREAD_PCT
        )
        frozen_price = prev_price > 0 and abs(price - prev_price) < max(0.0001, price * 0.0005)
        confirmed_gap = gap_seconds >= config.DYNAMIC_EXIT_MARKET_PAUSE_CONFIRM_SECONDS
        confirmed_pause = confirmed_gap or missing_quotes or abnormal_spread or frozen_price
        if not confirmed_pause:
            return False

        reasons = [f"gap {gap_seconds:.1f}s"]
        if missing_quotes:
            reasons.append("missing bid/ask")
        if abnormal_spread and spread_pct is not None:
            reasons.append(f"spread {spread_pct:.2f}%")
        if frozen_price:
            reasons.append("frozen price")

        candidate["paused_seconds"] = float(candidate.get("paused_seconds", 0.0)) + gap_seconds
        candidate["market_pause_state"] = "HALT_CONFIRMED"
        candidate["market_pause_detected_at"] = now
        candidate["market_pause_gap_seconds"] = gap_seconds
        candidate["market_pause_reasons"] = reasons
        candidate["pre_pause_reference_price"] = prev_price
        candidate["post_halt_classification"] = self._classify_reopen(price, symbol_state.vwap, prev_price)
        candidate["reopen_grace_until"] = now + timedelta(seconds=config.DYNAMIC_EXIT_REOPEN_BUFFER_SECONDS)
        append_sniper_event(
            filtered_events,
            f"{symbol} sniper pause detected ({', '.join(reasons)})",
        )
        self._log_event(
            "sniper_market_pause_confirmed",
            symbol=symbol,
            gap_seconds=round(gap_seconds, 1),
            reasons=list(reasons),
            paused_seconds=round(candidate["paused_seconds"], 1),
            reference_price=prev_price,
            reopen_classification=candidate["post_halt_classification"],
            reopen_buffer_seconds=config.DYNAMIC_EXIT_REOPEN_BUFFER_SECONDS,
        )
        return True

    def _maybe_invalidate_pending_setup(
        self,
        symbol: str,
        candidate: Dict[str, Any],
        symbol_state: SniperSymbolState,
        analysis: Dict[str, Any],
        now: datetime,
        filtered_events: deque,
    ) -> bool:
        current_price = symbol_state.price or 0.0
        alert_price = float(candidate.get("alert_price", 0.0) or 0.0)
        post_alert_high = max(float(candidate.get("post_alert_high", alert_price) or alert_price), current_price)
        candidate["post_alert_high"] = post_alert_high

        if analysis.get("entry_ready") or current_price <= 0:
            candidate["pending_support_break_started_at"] = None
            return False

        structure_mode = analysis.get("structure_mode") or ""
        support_price = analysis.get("support_price")
        support_label = analysis.get("support_label") or "support"

        if structure_mode in {"15s/30s", "1m"} and support_price and support_price > 0:
            support_break_threshold = support_price * (
                1 - config.SNIPER_PENDING_SUPPORT_BREAK_BUFFER_PCT / 100.0
            )
            if current_price < support_break_threshold:
                breach_started_at = candidate.get("pending_support_break_started_at")
                if breach_started_at is None:
                    candidate["pending_support_break_started_at"] = now
                    return False
                if (now - breach_started_at).total_seconds() >= config.SNIPER_PENDING_SUPPORT_BREAK_PERSIST_SECONDS:
                    self.pending_entries.pop(symbol, None)
                    self._set_cooldown(symbol, now)
                    reason = (
                        f"{structure_mode} pending setup broke {support_label} "
                        f"${support_price:.2f} by {config.SNIPER_PENDING_SUPPORT_BREAK_BUFFER_PCT:.2f}%"
                    )
                    append_sniper_event(filtered_events, f"{symbol} sniper setup invalidated")
                    self._log_event(
                        "sniper_setup_invalidated",
                        symbol=symbol,
                        reason="selected_mode_support_break",
                        detail=reason,
                        price=current_price,
                        support_price=support_price,
                        structure_mode=structure_mode,
                        effective_wait_seconds=round(self._get_effective_wait_seconds(candidate, now), 1),
                    )
                    return True
            else:
                candidate["pending_support_break_started_at"] = None
            return False

        candidate["pending_support_break_started_at"] = None
        if alert_price <= 0 or post_alert_high <= 0:
            return False

        post_alert_drop_pct = ((post_alert_high - current_price) / post_alert_high) * 100.0
        alert_loss_pct = ((alert_price - current_price) / alert_price) * 100.0
        if (
            post_alert_drop_pct >= config.SNIPER_PENDING_SEVERE_DROP_PCT
            and alert_loss_pct >= config.SNIPER_PENDING_ALERT_LOSS_BUFFER_PCT
        ):
            self.pending_entries.pop(symbol, None)
            self._set_cooldown(symbol, now)
            append_sniper_event(filtered_events, f"{symbol} sniper setup invalidated")
            self._log_event(
                "sniper_setup_invalidated",
                symbol=symbol,
                reason="severe_post_alert_damage",
                price=current_price,
                alert_price=alert_price,
                post_alert_high=post_alert_high,
                post_alert_drop_pct=post_alert_drop_pct,
                alert_loss_pct=alert_loss_pct,
                effective_wait_seconds=round(self._get_effective_wait_seconds(candidate, now), 1),
            )
            return True

        return False

    def discard_symbol(self, symbol: str) -> None:
        self.pending_entries.pop(symbol, None)
        self.cooldowns.pop(symbol, None)
        self.structure_breaks.pop(symbol, None)

    def get_pending_symbols(self) -> List[str]:
        return list(self.pending_entries.keys())

    def get_pending_entries(self) -> List[Dict[str, Any]]:
        rows = []
        for symbol, candidate in self.pending_entries.items():
            analysis = candidate.get("latest_analysis") or {}
            rows.append({
                "symbol": symbol,
                "grade": candidate.get("alert_grade"),
                "score": candidate.get("alert_score"),
                "queued_at": candidate.get("queued_at"),
                "alert_price": candidate.get("alert_price"),
                "effective_wait_seconds": self._get_effective_wait_seconds(candidate, datetime.now()),
                "paused_seconds": candidate.get("paused_seconds", 0.0),
                "clean_passed": analysis.get("clean_passed", False),
                "entry_ready": analysis.get("entry_ready", False),
                "entry_reason": analysis.get("entry_reason", ""),
                "first_failed_check": analysis.get("first_failed_check", ""),
                "structure_mode": analysis.get("structure_mode", ""),
                "market_pause_state": candidate.get("market_pause_state", "ACTIVE"),
                "reopen_grace_until": candidate.get("reopen_grace_until"),
                "post_halt_classification": candidate.get("post_halt_classification"),
                "market_pause_reasons": list(candidate.get("market_pause_reasons", [])),
            })
        return sorted(rows, key=lambda item: item["queued_at"], reverse=True)

    def queue_candidate_from_scanner_alert(
        self,
        alert_event: Dict[str, Any],
        symbol_state: Optional[SniperSymbolState],
        executor: ExecutionEngine,
        filtered_events: deque,
    ) -> None:
        symbol = alert_event.get("symbol")
        grade = alert_event.get("alert_grade")
        score = alert_event.get("alert_score")
        alert_price = alert_event.get("price") or (symbol_state.price if symbol_state and symbol_state.price else None)
        now = datetime.now()

        if not symbol:
            return
        if not entries_allowed_in_current_session():
            return
        if alert_event.get("suppressed"):
            return
        if get_alert_grade_rank(grade) < self.min_grade_rank:
            return
        if executor.is_position_active(symbol) or symbol in executor.get_blacklist():
            return
        if alert_price is None or self._on_cooldown(symbol, now):
            return

        existing = self.pending_entries.get(symbol)
        if existing:
            existing["queued_at"] = now
            existing["alert_price"] = alert_price
            existing["alert_grade"] = grade
            existing["alert_score"] = score
            existing["reasons"] = list(alert_event.get("reasons", []))
            existing["latest_analysis"] = None
            existing["post_alert_high"] = alert_price
            existing["pending_support_break_started_at"] = None
            self._reset_pending_pause_state(existing)
            return

        self.pending_entries[symbol] = {
            "queued_at": now,
            "alert_price": alert_price,
            "alert_grade": grade,
            "alert_score": score,
            "reasons": list(alert_event.get("reasons", [])),
            "latest_analysis": None,
            "post_alert_high": alert_price,
            "pending_support_break_started_at": None,
            "paused_seconds": 0.0,
            "market_pause_state": "ACTIVE",
            "market_pause_detected_at": None,
            "market_pause_gap_seconds": 0.0,
            "market_pause_reasons": [],
            "reopen_grace_until": None,
            "pre_pause_reference_price": None,
            "post_halt_classification": None,
        }
        append_sniper_event(filtered_events, f"{symbol} sniper queued [{grade} {score}] at ${alert_price:.2f}")
        self._log_event(
            "sniper_setup_queued",
            symbol=symbol,
            grade=grade,
            score=score,
            alert_price=alert_price,
            reasons=list(alert_event.get("reasons", [])),
        )

    def evaluate_symbol(
        self,
        symbol: str,
        symbol_state: SniperSymbolState,
        executor: ExecutionEngine,
        filtered_events: deque,
    ) -> None:
        candidate = self.pending_entries.get(symbol)
        if candidate is None or symbol_state.price is None:
            return

        now = datetime.now()
        if not entries_allowed_in_current_session():
            self.pending_entries.pop(symbol, None)
            return
        if executor.is_position_active(symbol) or symbol in executor.get_blacklist():
            self.pending_entries.pop(symbol, None)
            self._set_cooldown(symbol, now)
            return

        if self._maybe_detect_pending_market_pause(symbol, candidate, symbol_state, now, filtered_events):
            return

        if candidate.get("market_pause_state") == "HALT_CONFIRMED":
            reopen_grace_until = candidate.get("reopen_grace_until")
            if reopen_grace_until is not None and now < reopen_grace_until:
                return

            classification = candidate.get("post_halt_classification")
            candidate["market_pause_state"] = "ACTIVE"
            candidate["market_pause_detected_at"] = None
            candidate["reopen_grace_until"] = None
            self._log_event(
                "sniper_market_pause_resumed",
                symbol=symbol,
                reopen_classification=classification,
                price=symbol_state.price,
                vwap=symbol_state.vwap,
                effective_wait_seconds=round(self._get_effective_wait_seconds(candidate, now), 1),
            )
            if classification == "weak":
                self.pending_entries.pop(symbol, None)
                self._set_cooldown(symbol, now)
                append_sniper_event(filtered_events, f"{symbol} sniper setup canceled after weak reopen")
                self._log_event(
                    "sniper_setup_expired",
                    symbol=symbol,
                    reason="weak_reopen_after_halt",
                    effective_wait_seconds=round(self._get_effective_wait_seconds(candidate, now), 1),
                )
                return

        if self._get_effective_wait_seconds(candidate, now) > config.SNIPER_SETUP_MAX_WAIT_SECONDS:
            self.pending_entries.pop(symbol, None)
            self._set_cooldown(symbol, now)
            append_sniper_event(filtered_events, f"{symbol} sniper setup expired")
            self._log_event(
                "sniper_setup_expired",
                symbol=symbol,
                reason="setup_timeout",
                effective_wait_seconds=round(self._get_effective_wait_seconds(candidate, now), 1),
                paused_seconds=round(float(candidate.get("paused_seconds", 0.0)), 1),
            )
            return

        analysis = analyze_clean_move(symbol_state)
        candidate["latest_analysis"] = analysis
        self._log_event(
            "sniper_clean_context",
            symbol=symbol,
            clean_passed=analysis.get("clean_passed"),
            extreme_clean=analysis.get("extreme_clean"),
            entry_ready=analysis.get("entry_ready"),
            reasons=list(analysis.get("reasons", [])),
            metrics=dict(analysis.get("metrics", {})),
        )
        if self._maybe_invalidate_pending_setup(symbol, candidate, symbol_state, analysis, now, filtered_events):
            return
        if not analysis.get("entry_ready"):
            return

        stop_price = analysis.get("stop_price")
        if stop_price is None:
            return

        success = executor.execute_trade(
            symbol,
            symbol_state.price,
            stop_price=stop_price,
            market_session=get_market_session(),
            bid=symbol_state.bid,
            ask=symbol_state.ask,
            entry_context={
                **dict(analysis.get("metrics", {})),
                "structure_mode": analysis.get("structure_mode", ""),
                "size_multiplier": (
                    config.SNIPER_EXTREME_CLEAN_SIZE_MULTIPLIER
                    if analysis.get("extreme_clean")
                    else 1.0
                ),
            },
        )
        if success:
            self.pending_entries.pop(symbol, None)
            self._set_cooldown(symbol, now)
            append_sniper_event(filtered_events, f"{symbol} sniper entered at ${symbol_state.price:.2f} | {analysis['entry_reason']}")
            self._log_event(
                "sniper_entry_triggered",
                symbol=symbol,
                entry_price=symbol_state.price,
                stop_price=stop_price,
                alert_grade=candidate.get("alert_grade"),
                alert_score=candidate.get("alert_score"),
                entry_reason=analysis.get("entry_reason"),
                metrics=dict(analysis.get("metrics", {})),
            )

    def monitor_open_position(
        self,
        symbol: str,
        symbol_state: SniperSymbolState,
        executor: ExecutionEngine,
        filtered_events: deque,
    ) -> None:
        snapshot = executor.get_position_snapshot(symbol)
        if snapshot is None or snapshot.get("status") != "OPEN" or symbol_state.price is None:
            self.structure_breaks.pop(symbol, None)
            return
        if snapshot.get("market_pause_state") == "HALT_CONFIRMED":
            self.structure_breaks.pop(symbol, None)
            return
        reopen_grace_until = snapshot.get("reopen_grace_until")
        if reopen_grace_until is not None and datetime.now() < reopen_grace_until:
            self.structure_breaks.pop(symbol, None)
            return

        filled_at = snapshot.get("filled_at")
        actual_entry_price = snapshot.get("actual_entry_price") or snapshot.get("entry_price")
        highest_price = snapshot.get("highest_price") or actual_entry_price or 0.0
        now = datetime.now()
        if filled_at is not None and actual_entry_price and actual_entry_price > 0:
            held_seconds = (now - filled_at).total_seconds()
            bounce_target_price = actual_entry_price * (1 + config.SNIPER_POST_ENTRY_MIN_BOUNCE_PCT / 100.0)
            bounce_confirmed = highest_price >= bounce_target_price
            entry_extension_pct = float(snapshot.get("entry_extension_pct", 0.0) or 0.0)
            allowed_adverse_pct = _get_allowed_post_entry_adverse_pct(entry_extension_pct)
            adverse_price = actual_entry_price * (1 - allowed_adverse_pct / 100.0)
            if not bounce_confirmed:
                if current_price := symbol_state.price:
                    if current_price <= adverse_price:
                        if executor.submit_market_exit(symbol, role="flush_fail_exit"):
                            self.structure_breaks.pop(symbol, None)
                            append_sniper_event(
                                filtered_events,
                                f"{symbol} flush-fail exit at ${current_price:.2f} "
                                f"(no bounce, price kept falling below ${adverse_price:.2f}; "
                                f"allowed adverse {allowed_adverse_pct:.2f}% from entry)",
                            )
                            self._log_event(
                                "sniper_flush_fail_exit_submitted",
                                symbol=symbol,
                                reason="continued_flush_after_entry",
                                price=current_price,
                                actual_entry_price=actual_entry_price,
                                adverse_price=adverse_price,
                                allowed_adverse_pct=allowed_adverse_pct,
                                entry_extension_pct=entry_extension_pct,
                                held_seconds=round(held_seconds, 2),
                            )
                        return
                if held_seconds >= config.SNIPER_POST_ENTRY_BOUNCE_CHECK_SECONDS:
                    if executor.submit_market_exit(symbol, role="flush_fail_exit"):
                        self.structure_breaks.pop(symbol, None)
                        append_sniper_event(
                            filtered_events,
                            f"{symbol} flush-fail exit at ${symbol_state.price:.2f} "
                            f"(no {config.SNIPER_POST_ENTRY_MIN_BOUNCE_PCT:.2f}% bounce within "
                            f"{config.SNIPER_POST_ENTRY_BOUNCE_CHECK_SECONDS:.1f}s)",
                        )
                        self._log_event(
                            "sniper_flush_fail_exit_submitted",
                            symbol=symbol,
                            reason="no_immediate_bounce_after_entry",
                            price=symbol_state.price,
                            actual_entry_price=actual_entry_price,
                            bounce_target_price=bounce_target_price,
                            held_seconds=round(held_seconds, 2),
                        )
                    return

        analysis = analyze_clean_move(symbol_state)
        support_price = analysis.get("support_price")
        ema_value = analysis.get("ema_value")
        support_label = analysis.get("support_label") or "support"
        ema_label = analysis.get("ema_label") or "EMA"
        if support_price is None or ema_value is None:
            self.structure_breaks.pop(symbol, None)
            return

        current_price = symbol_state.price
        structure_broken = current_price < support_price and current_price < ema_value
        if not structure_broken:
            self.structure_breaks.pop(symbol, None)
            return

        breach_started_at = self.structure_breaks.get(symbol)
        if breach_started_at is None:
            self.structure_breaks[symbol] = now
            self._log_event(
                "sniper_structure_break_started",
                symbol=symbol,
                price=current_price,
                support_price=support_price,
                support_label=support_label,
                ema_label=ema_label,
                ema_value=ema_value,
                structure_mode=analysis.get("structure_mode"),
            )
            return

        if (now - breach_started_at).total_seconds() < config.SNIPER_STRUCTURE_BREAK_PERSIST_SECONDS:
            return

        if executor.submit_market_exit(symbol, role="structure_exit"):
            self.structure_breaks.pop(symbol, None)
            append_sniper_event(
                filtered_events,
                f"{symbol} structure exit submitted at ${current_price:.2f} "
                f"({support_label} ${support_price:.2f}, {ema_label} ${ema_value:.2f})",
            )
            self._log_event(
                "sniper_structure_exit_submitted",
                symbol=symbol,
                price=current_price,
                support_price=support_price,
                support_label=support_label,
                ema_label=ema_label,
                ema_value=ema_value,
                structure_mode=analysis.get("structure_mode"),
            )


def sniper_visualization(scanner_state, filtered_events, executor, sniper_manager: CleanMomentumSniperManager):
    os.system('cls' if os.name == 'nt' else 'clear')
    current_session = get_market_session()
    entry_status = "ENABLED" if entries_allowed_in_current_session() else "DISABLED"
    scanner_session = scanner_state.get("session", "UNKNOWN")
    top_symbols = scanner_state.get("top_symbols", [])

    print("=" * 132)
    print(
        f" CLEAN MOMENTUM SNIPER | {datetime.now().strftime('%H:%M:%S')} | "
        f"Local Session: {current_session} | Scanner Session: {scanner_session} | Entries: {entry_status} "
    )
    print("=" * 132)
    print(f"{'SYMBOL':<8} | {'PRICE':<10} | {'VWAP':<10} | {'GAIN%':<8} | {'RVOL':<10} | {'GRADE':<8} | {'ALERTS'}")
    print("-" * 132)
    if not top_symbols:
        print("No scanner data available yet.")
    else:
        for item in top_symbols[:config.RUNTIME_FEEDBACK_TOP_SYMBOLS]:
            price = _format_display_price(item.get("price"))
            vwap = _format_display_price(item.get("vwap")) if item.get("vwap") else "N/A"
            gain_pct = f"{item.get('gain_pct', 0.0):.2f}%"
            rvol = f"{item.get('relative_volume', 0.0):.2f}x"
            score = item.get("alert_score", 0)
            grade_value = item.get("alert_grade", "-")
            grade = f"{grade_value} ({score})" if score else "-"
            alerts = ", ".join(item.get("triggered_conditions", [])) or "--"
            print(f"{item['symbol']:<8} | {price:<10} | {vwap:<10} | {gain_pct:<8} | {rvol:<10} | {grade:<8} | {alerts}")

    print("\n" + "=" * 132)
    print(" STAGE 2: CLEAN-TREND PULLBACK FILTER")
    print("=" * 132)
    pending_entries = sniper_manager.get_pending_entries()
    if not pending_entries:
        print("  No clean sniper setups queued right now...")
    else:
        for pending in pending_entries[:6]:
            queued_time = pending["queued_at"].strftime('%H:%M:%S')
            clean_tag = "CLEAN" if pending["clean_passed"] else "WAIT"
            ready_tag = "READY" if pending["entry_ready"] else "HOLD"
            reopen_grace_until = pending.get("reopen_grace_until")
            if pending.get("market_pause_state") == "HALT_CONFIRMED":
                classification = pending.get("post_halt_classification") or "unknown"
                grace_label = (
                    f" until {reopen_grace_until.strftime('%H:%M:%S')}"
                    if reopen_grace_until is not None
                    else ""
                )
                reason = f"halt pause ({classification} reopen){grace_label}"
            else:
                mode = pending.get("structure_mode") or "pending"
                reason = pending["entry_reason"] or pending["first_failed_check"] or "--"
                reason = f"{mode}: {reason}"
            print(
                f"  [{clean_tag}/{ready_tag}] {pending['symbol']} [{pending['grade']} {pending['score']}] "
                f"alert ${pending['alert_price']:.2f} ({queued_time}) | {reason}"
            )

    print("\n" + "=" * 132)
    print(" STAGE 3: EXECUTION & POSITION TRACKING")
    print("=" * 132)
    active_pos = executor.get_active_positions_detailed()
    print(f"{'SYMBOL':<8} | {'STATUS':<12} | {'ENTRY':<10} | {'TP':<10} | {'SL':<10} | {'SHARES':<8} | {'TIME'}")
    print("-" * 132)
    if not active_pos:
        print("  None")
    for pos in active_pos:
        entry_disp = f"${pos['actual_entry']:.2f}" if pos['actual_entry'] else f"~${pos['entry']:.2f}"
        time_disp = pos['time'].strftime('%H:%M:%S')
        print(f"{pos['symbol']:<8} | {pos['status']:<12} | {entry_disp:<10} | ${pos['tp']:<10.2f} | ${pos['sl']:<10.2f} | {pos['shares']:<8} | {time_disp}")

    print("\n" + "-" * 132)
    print(f"{'RECENT SNIPER EVENTS':<132}")
    print("-" * 132)
    recent_events = list(filtered_events)
    if not recent_events:
        print("  No recent sniper events yet...")
    for event_time, event_message in recent_events:
        print(f"  {event_message} | {event_time.strftime('%H:%M:%S')}")

    print("\n" + "-" * 132)
    print(f"{'TRADE HISTORY (CLOSED / FAILED)':<132}")
    print("-" * 132)
    history = executor.get_trade_history()
    if not history:
        print("  No completed trades in this session.")
    for trade in reversed(history[-10:]):
        time_disp = _format_trade_span(trade.get('entry_time'), trade['time'])
        if trade['type'] == 'CLOSED':
            pnl = (trade['exit_price'] - trade['entry_price']) * trade['shares']
            pnl_pct = (trade['exit_price'] - trade['entry_price']) / trade['entry_price'] * 100 if trade['entry_price'] else 0.0
            details = f"{trade['exit_type']} Exit at ${trade['exit_price']:.2f} (P&L: ${pnl:.2f}, {pnl_pct:+.2f}%)"
            status_label = "CLOSED"
        elif trade['type'] == 'PARTIAL':
            pnl = (trade['exit_price'] - trade['entry_price']) * trade['shares']
            pnl_pct = (trade['exit_price'] - trade['entry_price']) / trade['entry_price'] * 100 if trade['entry_price'] else 0.0
            details = f"Partial at ${trade['exit_price']:.2f} ({trade['shares']} sh, P&L: ${pnl:.2f}, {pnl_pct:+.2f}%)"
            status_label = "PARTIAL"
        else:
            details = f"{trade['reason']} at ~${trade['entry_price']:.2f}"
            status_label = "FAILED"

        print(
            f"{trade['symbol']:<8} | "
            f"{status_label:<12} | "
            f"{details:<80} | {time_disp} | {_format_trade_reference_timeframe(trade)}"
        )
    print("=" * 132)


def build_sniper_runtime_state(
    scanner: RealtimeBroadScanner,
    symbol_states: Dict[str, SniperSymbolState],
    executor: ExecutionEngine,
    sniper_manager: CleanMomentumSniperManager,
    filtered_events: deque,
) -> Dict[str, Any]:
    return {
        "session": get_market_session(),
        "entries_enabled": entries_allowed_in_current_session(),
        "scanner": build_scanner_runtime_state(scanner),
        "tracked_symbols": sorted(symbol_states.keys()),
        "pending_entries": sniper_manager.get_pending_entries(),
        "active_positions": executor.get_active_positions_detailed(),
        "recent_trade_history": executor.get_trade_history()[-10:],
        "recent_filtered_alerts": list(filtered_events),
    }


def run_clean_momentum_sniper():
    global tws_app, active_executor
    telemetry = RuntimeTelemetry(component="clean_momentum_sniper", base_dir=RUNTIME_FEEDBACK_DIR)
    permanently_excluded_symbols = set()
    et_tz = pytz.timezone('US/Eastern')
    main_loop_start = _next_main_loop_start(datetime.now(et_tz))

    try:
        now_et = datetime.now(et_tz)
        if now_et < main_loop_start:
            wait_seconds = int((main_loop_start - now_et).total_seconds())
            if wait_seconds > 0:
                print(
                    f"[SCHEDULE] Startup paused until "
                    f"{main_loop_start.strftime('%Y-%m-%d %H:%M:%S %Z')} "
                    f"({wait_seconds // 60} min remaining)."
                )
                while not should_exit:
                    now_et = datetime.now(et_tz)
                    if now_et >= main_loop_start:
                        break
                    wait_timeout = min(30, max(1, int((main_loop_start - now_et).total_seconds())))
                    if shutdown_event.wait(wait_timeout):
                        break

        if should_exit:
            return

        tws_client_id = int(os.getenv("SNIPER_TWS_CLIENT_ID", "12"))
        tws_port = int(os.getenv("SNIPER_TWS_PORT", str(config.TWS_PORT)))
        print(f"[INIT] Connecting to TWS on port {tws_port} with client ID {tws_client_id}...")
        tws_app = create_tws_data_app(host="127.0.0.1", port=tws_port, client_id=tws_client_id)
        if not tws_app:
            print("[ERROR] Could not connect to TWS.")
            return

        executor = ExecutionEngine(
            tws_app=tws_app,
            account=ACCOUNT_NUMBER,
            tp_pct=TP_PCT,
            sl_pct=SL_PCT,
            investment_per_trade=INVESTMENT_PER_TRADE,
            telemetry=telemetry,
        )
        active_executor = executor

        open_positions = tws_app.request_open_positions(account=ACCOUNT_NUMBER, timeout=5.0)
        while open_positions and not should_exit:
            current_session = get_market_session()
            print("[SAFETY] Existing broker positions detected. Auto-flattening before sniper startup...")
            for item in open_positions:
                print(
                    f"  - {item['symbol']} | {item['secType']} | "
                    f"shares={item['position']:.0f} | avgCost=${item['avgCost']:.2f}"
                )

            if current_session == "CLOSED":
                next_resolution_start = _next_tradable_session_start(datetime.now(et_tz))
                print(
                    f"[SAFETY] Market is CLOSED. Waiting until "
                    f"{next_resolution_start.strftime('%Y-%m-%d %H:%M:%S %Z')} to flatten positions."
                )
                while not should_exit:
                    now_et = datetime.now(et_tz)
                    if now_et >= next_resolution_start:
                        break
                    wait_timeout = min(30, max(1, int((next_resolution_start - now_et).total_seconds())))
                    if shutdown_event.wait(wait_timeout):
                        break
                if should_exit or shutdown_event.is_set():
                    return
                open_positions = tws_app.request_open_positions(account=ACCOUNT_NUMBER, timeout=5.0)
                continue

            open_positions = executor.ensure_account_flat_before_startup(
                account=ACCOUNT_NUMBER,
                market_session=current_session,
                poll_timeout=5.0,
                max_attempts=12,
                pause_seconds=5.0,
            )
            if not open_positions:
                print("[SAFETY] Existing broker positions flattened. Proceeding with sniper startup.")
                break

            print("[SAFETY] Broker positions still remain after flatten attempts. Retrying shortly...")
            if shutdown_event.wait(10):
                return
            open_positions = tws_app.request_open_positions(account=ACCOUNT_NUMBER, timeout=5.0)

        if shutdown_event.wait(2):
            return

        print("[INIT] Fetching top gainers for clean momentum sniper...")
        symbols = get_top_gainers(
            top_n=config.SCANNER_MONITOR_CAP,
            use_ibkr=True,
            ibkr_port=tws_port,
            force_refresh=True,
        )
        unique_symbols = list(dict.fromkeys(symbols))
        print(f"[INIT] Monitoring {len(unique_symbols)} symbols: {', '.join(unique_symbols[:10])}{'...' if len(unique_symbols) > 10 else ''}")

        scanner = RealtimeBroadScanner(unique_symbols)
        initialize_alert_score_audit_file()

        sniper_manager = CleanMomentumSniperManager(telemetry=telemetry)
        symbol_states: Dict[str, SniperSymbolState] = {symbol: SniperSymbolState(symbol) for symbol in unique_symbols}

        telemetry.log_event("clean_sniper_started", tws_client_id=tws_client_id, tws_port=tws_port, runtime_dir=telemetry.run_dir)
        telemetry.log_event("scanner_started", symbols=unique_symbols, client_id=tws_client_id, runtime_dir=telemetry.run_dir)

        def handle_scanner_alert(symbol, timestamp, reasons, monitor):
            session = get_market_session()
            alert_event = {
                "symbol": symbol,
                "session": session,
                "reasons": list(reasons),
                "alert_grade": monitor.alert_grade,
                "alert_score": monitor.alert_score,
                "suppressed": monitor.alert_is_suppressed,
                "price": monitor.price_history[-1][1] if monitor.price_history else None,
                "vwap": monitor.vwap,
                "relative_volume": monitor.relative_volume,
            }
            telemetry.log_event("scanner_alert", **alert_event)
            append_alert_score_audit(symbol, timestamp, session, reasons, monitor)
            sniper_manager.queue_candidate_from_scanner_alert(alert_event, symbol_states.get(symbol), executor, filtered_alerts)

            if monitor.alert_is_suppressed:
                return

            send_discord_alert(symbol, session, reasons, monitor)
            try:
                voice_text = build_voice_announcement(symbol, session)
                if platform.system() == "Windows":
                    escaped_text = voice_text.replace('"', '`"')
                    ps_script = (
                        "Add-Type -AssemblyName System.Speech; "
                        "$synth = New-Object System.Speech.Synthesis.SpeechSynthesizer; "
                        f'$synth.Speak("{escaped_text}")'
                    )
                    subprocess.run(["powershell", "-Command", ps_script], capture_output=True, timeout=5)
                else:
                    os.system(f'espeak "{voice_text}" 2>/dev/null')
            except Exception as e:
                print(f"[WARNING] Voice announcement failed: {e}")

        scanner.on_preliminary_alert(handle_scanner_alert)

        print("[INIT] Loading scanner fundamentals/news/history...")
        excluded_symbols = scanner.load_fundamentals(tws_app)
        if excluded_symbols:
            excluded_set = set(excluded_symbols)
            unique_symbols = [symbol for symbol in unique_symbols if symbol not in excluded_set]
            symbol_states = {symbol: state for symbol, state in symbol_states.items() if symbol not in excluded_set}
        scanner.load_news(tws_app)
        scanner.load_previous_closes(tws_app)
        scanner.load_historical_prices(tws_app)

        def handle_market_update(symbol, price, volume, vwap, bid, ask):
            state = symbol_states.setdefault(symbol, SniperSymbolState(symbol))
            state.update_market_data(price=price, volume=volume, vwap=vwap, bid=bid, ask=ask)
            scanner.update(symbol, price=price, volume=volume, vwap=vwap, bid=bid, ask=ask)
            sniper_manager.evaluate_symbol(symbol, state, executor, filtered_alerts)
            executor.on_market_update(
                symbol,
                price=price,
                volume=volume,
                vwap=vwap,
                market_session=get_market_session(),
                bid=bid,
                ask=ask,
            )
            sniper_manager.monitor_open_position(symbol, state, executor, filtered_alerts)

        def create_market_data_callback(sym):
            return lambda s, p, v, vw, ts, b, a: handle_market_update(s, p, v, vw, b, a)

        def remove_symbols_from_scanner(symbols_to_remove, reason: str):
            nonlocal unique_symbols
            removed_any = False
            active_symbols = executor.get_active_position_symbols()
            for symbol in sorted(set(symbols_to_remove)):
                if symbol in active_symbols:
                    telemetry.log_event(
                        "scanner_symbol_retained",
                        symbol=symbol,
                        reason=f"{reason}_active_position",
                    )
                    continue
                if symbol not in scanner.monitors:
                    continue
                tws_app.unsubscribe_realtime_data(symbol)
                scanner.monitors.pop(symbol, None)
                if symbol in scanner.symbols:
                    scanner.symbols.remove(symbol)
                sniper_manager.discard_symbol(symbol)
                symbol_states.pop(symbol, None)
                unique_symbols = [tracked for tracked in unique_symbols if tracked != symbol]
                telemetry.log_event("scanner_symbol_removed", symbol=symbol, reason=reason)
                removed_any = True
            return removed_any

        print("[INIT] Subscribing to sniper market data...")
        for symbol in unique_symbols:
            tws_app.subscribe_market_data(symbol, create_market_data_callback(symbol))
            telemetry.log_event("market_data_subscribed", symbol=symbol)

        last_session_check = datetime.now()
        next_symbol_update = datetime.now(et_tz) + timedelta(seconds=config.SCANNER_REFRESH_INTERVAL_SECONDS)
        next_news_refresh = datetime.now(et_tz) + timedelta(seconds=config.SCANNER_NEWS_REFRESH_INTERVAL_SECONDS)
        eod_triggered = False

        while not should_exit:

            if (datetime.now() - last_session_check).total_seconds() > 60:
                if scanner.check_session_transition():
                    scanner.resync_vwap_all_symbols(tws_app)
                last_session_check = datetime.now()

            newly_blacklisted_symbols = executor.get_blacklist() - permanently_excluded_symbols
            if newly_blacklisted_symbols:
                permanently_excluded_symbols.update(newly_blacklisted_symbols)
                if remove_symbols_from_scanner(newly_blacklisted_symbols, reason="tws_blacklist"):
                    telemetry.log_event(
                        "scanner_symbols_updated",
                        monitored_symbols=sorted(scanner.monitors.keys()),
                        added_symbols=[],
                        removed_symbols=sorted(newly_blacklisted_symbols),
                        excluded_symbols=sorted(permanently_excluded_symbols),
                    )

            if datetime.now(et_tz) >= next_symbol_update:
                prior_symbols = set(scanner.monitors.keys())
                refresh_all_news = datetime.now(et_tz) >= next_news_refresh
                unique_symbols = update_scanner_symbols(
                    scanner,
                    tws_app,
                    unique_symbols,
                    market_data_callback=create_market_data_callback,
                    protected_symbols=sorted(
                        executor.get_active_position_symbols() | set(sniper_manager.get_pending_symbols())
                    ),
                    excluded_symbols=sorted(permanently_excluded_symbols),
                    refresh_all_news=refresh_all_news,
                    max_symbols=config.SCANNER_MONITOR_CAP,
                    max_symbol_changes=config.SCANNER_MAX_SYMBOL_CHANGES_PER_REFRESH,
                )
                added_symbols = set(scanner.monitors.keys()) - prior_symbols
                removed_symbols = prior_symbols - set(scanner.monitors.keys())
                for symbol in sorted(added_symbols):
                    symbol_states.setdefault(symbol, SniperSymbolState(symbol))
                    telemetry.log_event("market_data_subscribed", symbol=symbol)
                for symbol in sorted(removed_symbols):
                    sniper_manager.discard_symbol(symbol)
                    symbol_states.pop(symbol, None)
                    telemetry.log_event("market_data_unsubscribed", symbol=symbol)
                if added_symbols or removed_symbols:
                    telemetry.log_event(
                        "scanner_symbols_updated",
                        monitored_symbols=sorted(scanner.monitors.keys()),
                        added_symbols=sorted(added_symbols),
                        removed_symbols=sorted(removed_symbols),
                        excluded_symbols=sorted(permanently_excluded_symbols),
                    )
                next_symbol_update = datetime.now(et_tz) + timedelta(seconds=config.SCANNER_REFRESH_INTERVAL_SECONDS)
                if refresh_all_news:
                    next_news_refresh = datetime.now(et_tz) + timedelta(seconds=config.SCANNER_NEWS_REFRESH_INTERVAL_SECONDS)

            now_et = datetime.now(et_tz)
            cleanup_marker = _session_close_cleanup_marker(now_et)
            if cleanup_marker is not None and cleanup_marker != eod_triggered:
                print(f"[SESSION] {cleanup_marker} reached. Triggering final cleanup...")
                executor.close_all_positions(market_session=get_market_session())
                eod_triggered = cleanup_marker
            elif cleanup_marker is None:
                eod_triggered = None

            state_payload = build_sniper_runtime_state(
                scanner,
                symbol_states,
                executor,
                sniper_manager,
                filtered_alerts,
            )
            telemetry.write_state(state_payload)
            sniper_visualization(state_payload["scanner"], filtered_alerts, executor, sniper_manager)
            if shutdown_event.wait(1):
                break
    finally:
        active_executor = None
        telemetry.log_event("scanner_stopped")
        telemetry.log_event("clean_sniper_stopped")
        if tws_app is not None:
            tws_app.disconnect()
        print("[INFO] Clean momentum sniper stopped.")


if __name__ == "__main__":
    run_clean_momentum_sniper()

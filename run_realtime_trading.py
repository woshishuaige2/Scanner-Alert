"""
Real-Time Trading Bot
Runs the scanner and execution logic in one process so one trading_bot runtime
folder captures the full workflow.
"""
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

from alert_rating import get_alert_grade_rank
from execution_engine import ExecutionEngine
from realtime_multi_session_scanner import (
    RealtimeBroadScanner,
    append_alert_score_audit,
    build_scanner_runtime_state,
    build_voice_announcement,
    get_market_session,
    get_next_10_minute_mark,
    initialize_alert_score_audit_file,
    send_discord_alert,
    update_scanner_symbols,
)
from runtime_feedback import RuntimeTelemetry
from top_gainers_fetcher import get_top_gainers
from tws_data_fetcher import create_tws_data_app
import scanner_config as config


INVESTMENT_PER_TRADE = 500.0
TP_PCT = 5.0
SL_PCT = 5.0
ACCOUNT_NUMBER = "DUO200259"  # Replace with your IBKR paper trading account number.
RUNTIME_FEEDBACK_DIR = os.path.join(os.path.dirname(__file__), config.RUNTIME_FEEDBACK_DIR_NAME)

should_exit = False
tws_app = None
filtered_alerts = deque(maxlen=20)


def signal_handler(sig, frame):
    global should_exit
    print("\n[INFO] Graceful exit requested...")
    should_exit = True


signal.signal(signal.SIGINT, signal_handler)


def entries_allowed_in_current_session() -> bool:
    if not config.REALTIME_TRADE_REGULAR_HOURS_ONLY:
        return True
    return get_market_session() == "REGULAR"


class TradeSymbolState:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.price_history = deque(maxlen=256)
        self.price: Optional[float] = None
        self.vwap: float = 0.0
        self.bid: float = 0.0
        self.ask: float = 0.0
        self.last_update: Optional[datetime] = None

    def update_market_data(self, price: float, vwap: float, bid: float = 0.0, ask: float = 0.0):
        now = datetime.now()
        self.price = price
        self.vwap = vwap
        self.bid = bid
        self.ask = ask
        self.last_update = now
        self.price_history.append((now, price))

    def to_dict(self) -> Dict:
        return {
            "symbol": self.symbol,
            "price": self.price,
            "vwap": self.vwap,
            "bid": self.bid,
            "ask": self.ask,
            "last_update": self.last_update,
        }


class BreakoutEntryManager:
    """Queue Grade B+ scanner alerts and enter on continuation or short-base breakouts."""

    def __init__(self, telemetry: RuntimeTelemetry = None):
        self.pending_entries: Dict[str, Dict] = {}
        self.cooldowns: Dict[str, datetime] = {}
        self.lock = threading.Lock()
        self.min_grade_rank = get_alert_grade_rank(config.REALTIME_TRADE_MIN_ALERT_GRADE)
        self.breakout_grade_rank = get_alert_grade_rank(config.REALTIME_BREAKOUT_MIN_ALERT_GRADE)
        self.telemetry = telemetry

    def _log_event(self, event_type: str, **payload):
        if self.telemetry:
            self.telemetry.log_event(event_type, **payload)

    def _set_cooldown(self, symbol: str, now: datetime) -> None:
        self.cooldowns[symbol] = now

    def _on_cooldown(self, symbol: str, now: datetime) -> bool:
        last_attempt = self.cooldowns.get(symbol)
        if last_attempt is None:
            return False
        return (now - last_attempt).total_seconds() < config.REALTIME_TRADE_SYMBOL_COOLDOWN_SECONDS

    def _build_recent_prices(self, symbol_state: Optional[TradeSymbolState]) -> deque:
        recent_prices = deque(maxlen=64)
        if symbol_state:
            for ts, price in list(symbol_state.price_history)[-64:]:
                recent_prices.append((ts, price))
        return recent_prices

    def _determine_entry_mode(self, grade: Optional[str], score: Optional[int]) -> str:
        grade_rank = get_alert_grade_rank(grade)
        if grade_rank >= self.breakout_grade_rank:
            return "continuation_breakout"
        if score is not None and score >= config.REALTIME_MOMENTUM_OVERWRITE_MIN_SCORE:
            return "continuation_breakout"
        return "base_breakout"

    def _should_replace_pending_candidate(
        self,
        existing: Dict[str, Any],
        grade: Optional[str],
        score: Optional[int],
        entry_mode: str,
    ) -> bool:
        existing_grade_rank = get_alert_grade_rank(existing.get("alert_grade"))
        new_grade_rank = get_alert_grade_rank(grade)
        existing_score = existing.get("alert_score") or 0
        new_score = score or 0

        if entry_mode == "continuation_breakout" and existing.get("entry_mode") != "continuation_breakout":
            return new_grade_rank > existing_grade_rank or new_score >= max(
                existing_score + 1,
                config.REALTIME_MOMENTUM_OVERWRITE_MIN_SCORE,
            )

        if new_grade_rank > existing_grade_rank:
            return True
        if new_grade_rank == existing_grade_rank and new_score > existing_score:
            return True
        return False

    def get_pending_symbols(self) -> List[str]:
        with self.lock:
            return list(self.pending_entries.keys())

    def discard_symbol(self, symbol: str) -> None:
        with self.lock:
            self.pending_entries.pop(symbol, None)
            self.cooldowns.pop(symbol, None)

    def queue_candidate_from_scanner_alert(
        self,
        alert_event: Dict,
        symbol_state: Optional[TradeSymbolState],
        executor: ExecutionEngine,
        filtered_alerts,
    ) -> None:
        symbol = alert_event.get("symbol")
        grade = alert_event.get("alert_grade")
        score = alert_event.get("alert_score")
        alert_price = alert_event.get("price") or (symbol_state.price if symbol_state and symbol_state.price else None)
        reasons = alert_event.get("reasons", [])

        if not symbol:
            return
        if not entries_allowed_in_current_session():
            self._log_event("entry_skipped", symbol=symbol, reason="session_not_regular")
            return
        if alert_event.get("suppressed"):
            self._log_event("entry_skipped", symbol=symbol, reason="alert_suppressed")
            return
        if get_alert_grade_rank(grade) < self.min_grade_rank:
            self._log_event("entry_skipped", symbol=symbol, reason="grade_below_threshold", grade=grade, score=score)
            return
        if executor.is_position_active(symbol) or symbol in executor.get_blacklist():
            self._log_event("entry_skipped", symbol=symbol, reason="position_active_or_blacklisted")
            return
        if alert_price is None:
            self._log_event("entry_skipped", symbol=symbol, reason="missing_alert_price")
            return

        now = datetime.now()
        with self.lock:
            if self._on_cooldown(symbol, now):
                self._log_event("entry_skipped", symbol=symbol, reason="symbol_cooldown")
                return

            entry_mode = self._determine_entry_mode(grade, score)
            existing = self.pending_entries.get(symbol)
            if existing:
                if self._should_replace_pending_candidate(existing, grade, score, entry_mode):
                    prior_mode = existing.get("entry_mode")
                    prior_grade = existing.get("alert_grade")
                    prior_score = existing.get("alert_score")
                    self.pending_entries[symbol] = {
                        "queued_at": now,
                        "alert_price": alert_price,
                        "high_watermark": max(
                            alert_price,
                            symbol_state.price if symbol_state and symbol_state.price else alert_price,
                        ),
                        "alert_grade": grade,
                        "alert_score": score,
                        "reasons": list(reasons),
                        "entry_mode": entry_mode,
                        "recent_prices": self._build_recent_prices(symbol_state),
                    }
                    filtered_alerts.appendleft(
                        f"{symbol} replaced [{prior_grade} {prior_score}] {prior_mode.replace('_', ' ')} "
                        f"with [{grade} {score}] {entry_mode.replace('_', ' ')} at ${alert_price:.2f}"
                    )
                    self._log_event(
                        "entry_replaced",
                        symbol=symbol,
                        prior_grade=prior_grade,
                        prior_score=prior_score,
                        prior_entry_mode=prior_mode,
                        grade=grade,
                        score=score,
                        alert_price=alert_price,
                        entry_mode=entry_mode,
                        reasons=list(reasons),
                    )
                    return

                existing["queued_at"] = now
                existing["alert_price"] = alert_price
                existing["high_watermark"] = max(existing["high_watermark"], alert_price)
                existing["alert_grade"] = grade
                existing["alert_score"] = score
                existing["reasons"] = list(reasons)
                return

            recent_prices = self._build_recent_prices(symbol_state)

            self.pending_entries[symbol] = {
                "queued_at": now,
                "alert_price": alert_price,
                "high_watermark": max(alert_price, symbol_state.price if symbol_state and symbol_state.price else alert_price),
                "alert_grade": grade,
                "alert_score": score,
                "reasons": list(reasons),
                "entry_mode": entry_mode,
                "recent_prices": recent_prices,
            }

        filtered_alerts.appendleft(
            f"{symbol} queued [{grade} {score}] at ${alert_price:.2f} for {entry_mode.replace('_', ' ')}"
        )
        self._log_event(
            "entry_queued",
            symbol=symbol,
            grade=grade,
            score=score,
            alert_price=alert_price,
            entry_mode=entry_mode,
            reasons=list(reasons),
        )

    def evaluate_symbol(self, symbol: str, symbol_state: TradeSymbolState, executor: ExecutionEngine, filtered_alerts):
        if symbol_state.price is None:
            return

        action = None
        now = datetime.now()
        current_price = symbol_state.price
        current_vwap = symbol_state.vwap

        with self.lock:
            candidate = self.pending_entries.get(symbol)
            if candidate is None:
                return

            if not entries_allowed_in_current_session():
                self.pending_entries.pop(symbol, None)
                self._log_event("entry_expired", symbol=symbol, reason="session_not_regular")
                return

            if executor.is_position_active(symbol) or symbol in executor.get_blacklist():
                self.pending_entries.pop(symbol, None)
                self._set_cooldown(symbol, now)
                return

            if (now - candidate["queued_at"]).total_seconds() > config.REALTIME_ENTRY_MAX_WAIT_SECONDS:
                self.pending_entries.pop(symbol, None)
                self._set_cooldown(symbol, now)
                action = ("expired", f"{symbol} setup expired before a valid entry")
            else:
                prior_high = candidate["high_watermark"]
                candidate["recent_prices"].append((now, current_price))
                candidate["high_watermark"] = max(candidate["high_watermark"], current_price)
                high_watermark = candidate["high_watermark"]
                if high_watermark <= 0:
                    return

                extension_pct = ((high_watermark - candidate["alert_price"]) / candidate["alert_price"]) * 100 if candidate["alert_price"] > 0 else 0.0
                fade_from_peak_pct = ((high_watermark - current_price) / high_watermark) * 100

                if current_vwap > 0 and current_price < current_vwap:
                    self.pending_entries.pop(symbol, None)
                    self._set_cooldown(symbol, now)
                    action = ("expired", f"{symbol} setup lost VWAP before entry")
                elif fade_from_peak_pct > config.REALTIME_ENTRY_FAIL_BELOW_PEAK_PCT:
                    self.pending_entries.pop(symbol, None)
                    self._set_cooldown(symbol, now)
                    action = ("expired", f"{symbol} setup faded too far from the high")
                elif extension_pct < config.REALTIME_ENTRY_MIN_EXTENSION_PCT:
                    return
                elif candidate["entry_mode"] == "continuation_breakout":
                    breakout_trigger = prior_high * (1 + config.REALTIME_ENTRY_BREAKOUT_BUFFER_PCT / 100.0)
                    if prior_high > 0 and current_price >= breakout_trigger:
                        self.pending_entries.pop(symbol, None)
                        self._set_cooldown(symbol, now)
                        action = ("enter", current_price, candidate["alert_grade"], candidate["alert_score"])
                else:
                    setup_age_seconds = (now - candidate["queued_at"]).total_seconds()
                    base_window_start = now.timestamp() - config.REALTIME_ENTRY_BASE_LOOKBACK_SECONDS
                    base_prices = [price for ts, price in candidate["recent_prices"] if ts.timestamp() >= base_window_start]
                    if setup_age_seconds < config.REALTIME_ENTRY_BASE_MIN_SECONDS or len(base_prices) < 3:
                        return

                    base_high = max(base_prices)
                    base_low = min(base_prices)
                    if base_high <= 0:
                        return

                    base_width_pct = ((base_high - base_low) / base_high) * 100
                    base_distance_from_peak_pct = ((high_watermark - base_high) / high_watermark) * 100
                    base_breakout_trigger = base_high * (1 + config.REALTIME_ENTRY_BREAKOUT_BUFFER_PCT / 100.0)

                    if (
                        base_width_pct <= config.REALTIME_ENTRY_BASE_MAX_WIDTH_PCT
                        and base_distance_from_peak_pct <= config.REALTIME_ENTRY_BASE_MAX_DISTANCE_FROM_PEAK_PCT
                        and current_price >= base_breakout_trigger
                    ):
                        self.pending_entries.pop(symbol, None)
                        self._set_cooldown(symbol, now)
                        action = ("enter", current_price, candidate["alert_grade"], candidate["alert_score"])

        if action is None:
            return
        if action[0] == "expired":
            filtered_alerts.appendleft(action[1])
            self._log_event("entry_expired", symbol=symbol, reason=action[1])
            return

        _, entry_price, grade, score = action
        success = executor.execute_trade(symbol, entry_price)
        if success:
            filtered_alerts.appendleft(f"{symbol} entered at ${entry_price:.2f} [{grade} {score}]")
            self._log_event("entry_triggered", symbol=symbol, entry_price=entry_price, grade=grade, score=score)
        else:
            filtered_alerts.appendleft(f"{symbol} entry failed at ${entry_price:.2f} [{grade} {score}]")
            self._log_event("entry_failed", symbol=symbol, entry_price=entry_price, grade=grade, score=score)

    def get_pending_entries(self) -> List[Dict]:
        with self.lock:
            pending = []
            for symbol, candidate in self.pending_entries.items():
                pending.append({
                    "symbol": symbol,
                    "grade": candidate["alert_grade"],
                    "score": candidate["alert_score"],
                    "alert_price": candidate["alert_price"],
                    "peak_price": candidate["high_watermark"],
                    "queued_at": candidate["queued_at"],
                    "entry_mode": candidate["entry_mode"],
                })
            return sorted(pending, key=lambda item: item["queued_at"], reverse=True)

    def get_pending_entry_snapshot(self, symbol: str) -> Optional[Dict[str, Any]]:
        with self.lock:
            candidate = self.pending_entries.get(symbol)
            if candidate is None:
                return None
            return {
                "symbol": symbol,
                "queued_at": candidate["queued_at"],
                "alert_price": candidate["alert_price"],
                "high_watermark": candidate["high_watermark"],
                "alert_grade": candidate["alert_grade"],
                "alert_score": candidate["alert_score"],
                "reasons": list(candidate["reasons"]),
                "entry_mode": candidate["entry_mode"],
            }


def _compute_recent_price_change_pct(price_history: deque, now: datetime, window_seconds: int) -> Optional[float]:
    if not price_history:
        return None
    price_points = list(price_history)
    if not price_points:
        return None
    target_ts = now - timedelta(seconds=window_seconds)
    baseline_price = None
    for ts, price in price_points:
        if ts >= target_ts:
            baseline_price = price
            break
    if baseline_price is None:
        return None
    current_price = price_points[-1][1]
    if baseline_price <= 0 or current_price <= 0:
        return None
    return ((current_price - baseline_price) / baseline_price) * 100


class TradeTraceRecorder:
    def __init__(
        self,
        telemetry: RuntimeTelemetry,
        scanner: RealtimeBroadScanner,
        symbol_states: Dict[str, TradeSymbolState],
        entry_manager: BreakoutEntryManager,
        executor: ExecutionEngine,
    ):
        self.telemetry = telemetry
        self.scanner = scanner
        self.symbol_states = symbol_states
        self.entry_manager = entry_manager
        self.executor = executor
        self.interval_seconds = config.TRADE_TRACE_SNAPSHOT_INTERVAL_SECONDS
        self.pre_entry_seconds = config.TRADE_TRACE_PRE_ENTRY_SECONDS
        self.post_exit_seconds = config.TRADE_TRACE_POST_EXIT_SECONDS
        self.buffer_seconds = config.TRADE_TRACE_BUFFER_SECONDS
        self.recent_samples: Dict[str, deque] = {}
        self.last_sample_at: Dict[str, datetime] = {}
        self.last_market_data_time_by_symbol: Dict[str, datetime] = {}
        self.active_traces: Dict[str, Dict[str, Any]] = {}

    def _build_sample(self, symbol: str, now: datetime) -> Optional[Dict[str, Any]]:
        symbol_state = self.symbol_states.get(symbol)
        monitor = self.scanner.monitors.get(symbol)
        if symbol_state is None or symbol_state.last_update is None:
            return None

        pending_entry = self.entry_manager.get_pending_entry_snapshot(symbol)
        position = self.executor.get_position_snapshot(symbol)
        spread_pct = None
        if symbol_state.bid and symbol_state.ask and symbol_state.ask > 0:
            spread_pct = ((symbol_state.ask - symbol_state.bid) / symbol_state.ask) * 100

        return {
            "symbol": symbol,
            "sample_time": now,
            "market_data_time": symbol_state.last_update,
            "session": get_market_session(),
            "price": symbol_state.price,
            "bid": symbol_state.bid,
            "ask": symbol_state.ask,
            "spread_pct": spread_pct,
            "vwap": symbol_state.vwap,
            "price_change_5s_pct": _compute_recent_price_change_pct(symbol_state.price_history, now, 5),
            "price_change_15s_pct": _compute_recent_price_change_pct(symbol_state.price_history, now, 15),
            "price_change_30s_pct": _compute_recent_price_change_pct(symbol_state.price_history, now, 30),
            "relative_volume": monitor.relative_volume if monitor else None,
            "scanner_alert_grade": monitor.alert_grade if monitor else None,
            "scanner_alert_score": monitor.alert_score if monitor else None,
            "scanner_triggered_conditions": list(monitor.triggered_conditions) if monitor else [],
            "pending_entry": pending_entry is not None,
            "pending_entry_mode": pending_entry["entry_mode"] if pending_entry else None,
            "pending_alert_price": pending_entry["alert_price"] if pending_entry else None,
            "pending_high_watermark": pending_entry["high_watermark"] if pending_entry else None,
            "position_active": position is not None,
            "position_status": position["status"] if position else None,
            "position_entry_price": (
                position.get("actual_entry_price") or position.get("entry_price")
            ) if position else None,
            "position_stop_price": position.get("current_stop_price") if position else None,
            "position_partial_target_price": position.get("partial_target_price") if position else None,
            "position_remaining_shares": position.get("remaining_shares") if position else None,
            "position_highest_price": position.get("highest_price") if position else None,
            "position_partial_taken": position.get("partial_taken") if position else None,
        }

    def _buffer_sample(self, symbol: str, sample: Dict[str, Any], now: datetime) -> None:
        buffer = self.recent_samples.setdefault(symbol, deque())
        buffer.append(sample)
        cutoff = now - timedelta(seconds=self.buffer_seconds)
        while buffer and buffer[0]["sample_time"] < cutoff:
            buffer.popleft()

    def on_execution_event(self, event_type: str, payload: Dict[str, Any]) -> None:
        symbol = payload.get("symbol")
        if not symbol:
            return
        now = datetime.now()

        if event_type == "position_opened":
            trace_id = f"{symbol}_{payload.get('order_id', 'open')}_{int(now.timestamp())}"
            trace = {
                "trace_id": trace_id,
                "symbol": symbol,
                "opened_at": now,
                "entry_event": dict(payload),
                "post_exit_until": None,
                "exit_event": None,
            }
            self.active_traces[symbol] = trace
            self.telemetry.log_event(
                "trade_trace_started",
                trace_id=trace_id,
                symbol=symbol,
                opened_at=now,
                pre_entry_seconds=self.pre_entry_seconds,
                entry_event=dict(payload),
            )
            cutoff = now - timedelta(seconds=self.pre_entry_seconds)
            for sample in list(self.recent_samples.get(symbol, [])):
                if sample["sample_time"] >= cutoff:
                    self.telemetry.log_event(
                        "trade_trace_snapshot",
                        trace_id=trace_id,
                        phase="pre_entry",
                        **sample,
                    )
            return

        trace = self.active_traces.get(symbol)
        if trace is None:
            return

        if event_type == "partial_exit_filled":
            self.telemetry.log_event(
                "trade_trace_marker",
                trace_id=trace["trace_id"],
                symbol=symbol,
                marker_type="partial_exit",
                marker_event=dict(payload),
            )
            return

        if event_type == "position_closed":
            trace["exit_event"] = dict(payload)
            trace["post_exit_until"] = now + timedelta(seconds=self.post_exit_seconds)
            self.telemetry.log_event(
                "trade_trace_marker",
                trace_id=trace["trace_id"],
                symbol=symbol,
                marker_type="position_closed",
                marker_event=dict(payload),
            )

    def capture_snapshots(self) -> None:
        now = datetime.now()
        for symbol in list(self.symbol_states.keys()):
            sample = self._build_sample(symbol, now)
            if sample is None:
                continue

            market_data_time = sample["market_data_time"]
            last_market_data_time = self.last_market_data_time_by_symbol.get(symbol)
            if last_market_data_time is not None and market_data_time <= last_market_data_time:
                continue

            last_sample_at = self.last_sample_at.get(symbol)
            if last_sample_at and (now - last_sample_at).total_seconds() < self.interval_seconds:
                continue

            self._buffer_sample(symbol, sample, now)
            self.last_sample_at[symbol] = now
            self.last_market_data_time_by_symbol[symbol] = market_data_time

            trace = self.active_traces.get(symbol)
            if trace is None:
                continue

            phase = "post_exit" if trace.get("post_exit_until") is not None else "during_trade"
            self.telemetry.log_event(
                "trade_trace_snapshot",
                trace_id=trace["trace_id"],
                phase=phase,
                **sample,
            )

        completed_symbols = []
        for symbol, trace in self.active_traces.items():
            post_exit_until = trace.get("post_exit_until")
            if post_exit_until is not None and now >= post_exit_until:
                self.telemetry.log_event(
                    "trade_trace_completed",
                    trace_id=trace["trace_id"],
                    symbol=symbol,
                    opened_at=trace.get("opened_at"),
                    entry_event=trace.get("entry_event"),
                    exit_event=trace.get("exit_event"),
                    completed_at=now,
                )
                completed_symbols.append(symbol)
        for symbol in completed_symbols:
            self.active_traces.pop(symbol, None)

    def get_active_trace_summaries(self) -> List[Dict[str, Any]]:
        summaries = []
        for trace in self.active_traces.values():
            summaries.append({
                "trace_id": trace["trace_id"],
                "symbol": trace["symbol"],
                "opened_at": trace.get("opened_at"),
                "post_exit_until": trace.get("post_exit_until"),
                "has_exit_event": trace.get("exit_event") is not None,
            })
        return summaries


def unified_visualization(scanner_state, filtered_alerts, executor, entry_manager: BreakoutEntryManager):
    os.system('cls' if os.name == 'nt' else 'clear')
    current_session = get_market_session()
    entry_status = "ENABLED" if entries_allowed_in_current_session() else "DISABLED"
    scanner_session = scanner_state.get("session", "UNKNOWN")
    top_symbols = scanner_state.get("top_symbols", [])

    print("=" * 115)
    print(
        f" STAGE 1: IN-PROCESS SCANNER | {datetime.now().strftime('%H:%M:%S')} | "
        f"Local Session: {current_session} | Scanner Session: {scanner_session} | Entries: {entry_status} "
    )
    print("=" * 115)
    print(f"{'SYMBOL':<8} | {'PRICE':<10} | {'VWAP':<10} | {'RVOL':<10} | {'GRADE':<8} | {'ALERTS'}")
    print("-" * 115)
    if not top_symbols:
        print("No scanner data available yet.")
    else:
        for item in top_symbols[:config.RUNTIME_FEEDBACK_TOP_SYMBOLS]:
            price = f"${item['price']:.2f}" if item.get("price") is not None else "N/A"
            vwap = f"${item['vwap']:.2f}" if item.get("vwap") else "N/A"
            rvol = f"{item.get('relative_volume', 0.0):.2f}x"
            score = item.get("alert_score", 0)
            grade_value = item.get("alert_grade", "-")
            grade = f"{grade_value} ({score})" if score else "-"
            alerts = ", ".join(item.get("triggered_conditions", [])) or "--"
            print(f"{item['symbol']:<8} | {price:<10} | {vwap:<10} | {rvol:<10} | {grade:<8} | {alerts}")

    print("\n" + "=" * 115)
    print(" STAGE 2: GRADE B+ BREAKOUT ENTRY QUEUE")
    print("=" * 115)
    pending_entries = entry_manager.get_pending_entries()
    if pending_entries:
        for pending in pending_entries[:5]:
            queued_time = pending["queued_at"].strftime('%H:%M:%S')
            status = pending["entry_mode"].replace("_", " ")
            print(
                f"  [PENDING] {pending['symbol']} [{pending['grade']} {pending['score']}] "
                f"alert ${pending['alert_price']:.2f} peak ${pending['peak_price']:.2f} "
                f"{status} ({queued_time})"
            )
    else:
        print("  No grade-qualified setups queued right now...")

    if not filtered_alerts:
        print("  No recent queue / execution events yet...")
    for alert in filtered_alerts:
        print(f"  [FILTERED] {alert}")

    print("\n" + "=" * 115)
    print(" STAGE 3: TRADE EXECUTION & POSITION TRACKING")
    print("=" * 115)
    active_pos = executor.get_active_positions_detailed()
    print(f"{'ACTIVE POSITIONS':<115}")
    print(f"{'SYMBOL':<8} | {'STATUS':<12} | {'ENTRY':<10} | {'TP':<10} | {'SL':<10} | {'SHARES':<8} | {'TIME'}")
    print("-" * 115)
    if not active_pos:
        print("  None")
    for pos in active_pos:
        entry_disp = f"${pos['actual_entry']:.2f}" if pos['actual_entry'] else f"~${pos['entry']:.2f}"
        time_disp = pos['time'].strftime('%H:%M:%S')
        print(f"{pos['symbol']:<8} | {pos['status']:<12} | {entry_disp:<10} | ${pos['tp']:<10.2f} | ${pos['sl']:<10.2f} | {pos['shares']:<8} | {time_disp}")

    print("\n" + "-" * 115)
    print(f"{'TRADE HISTORY (CLOSED / FAILED)':<115}")
    print(f"{'SYMBOL':<8} | {'RESULT':<12} | {'DETAILS':<65} | {'TIME'}")
    print("-" * 115)
    history = executor.get_trade_history()
    if not history:
        print("  No completed trades in this session.")
    for trade in reversed(history[-10:]):
        time_disp = trade['time'].strftime('%H:%M:%S')
        if trade['type'] == 'CLOSED':
            pnl = (trade['exit_price'] - trade['entry_price']) * trade['shares']
            pnl_pct = (trade['exit_price'] - trade['entry_price']) / trade['entry_price'] * 100 if trade['entry_price'] else 0.0
            details = f"{trade['exit_type']} Exit at ${trade['exit_price']:.2f} (P&L: ${pnl:.2f}, {pnl_pct:+.2f}%)"
            print(f"{trade['symbol']:<8} | {'CLOSED':<12} | {details:<65} | {time_disp}")
        elif trade['type'] == 'PARTIAL':
            pnl = (trade['exit_price'] - trade['entry_price']) * trade['shares']
            pnl_pct = (trade['exit_price'] - trade['entry_price']) / trade['entry_price'] * 100 if trade['entry_price'] else 0.0
            details = f"Partial at ${trade['exit_price']:.2f} ({trade['shares']} sh, P&L: ${pnl:.2f}, {pnl_pct:+.2f}%)"
            print(f"{trade['symbol']:<8} | {'PARTIAL':<12} | {details:<65} | {time_disp}")
        else:
            details = f"{trade['reason']} at ~${trade['entry_price']:.2f}"
            print(f"{trade['symbol']:<8} | {'FAILED':<12} | {details:<65} | {time_disp}")
    print("=" * 115)


def build_trading_runtime_state(
    scanner: RealtimeBroadScanner,
    symbol_states: Dict[str, TradeSymbolState],
    executor,
    entry_manager,
    filtered_alerts,
    trace_recorder: TradeTraceRecorder,
):
    tracked = []
    for symbol, state in list(symbol_states.items())[:config.RUNTIME_FEEDBACK_TOP_SYMBOLS]:
        tracked.append(state.to_dict())
    scanner_state = build_scanner_runtime_state(scanner)
    return {
        "session": get_market_session(),
        "entries_enabled": entries_allowed_in_current_session(),
        "scanner": scanner_state,
        "tracked_symbols": tracked,
        "pending_entries": entry_manager.get_pending_entries(),
        "active_positions": executor.get_active_positions_detailed(),
        "active_trade_traces": trace_recorder.get_active_trace_summaries(),
        "recent_trade_history": executor.get_trade_history()[-10:],
        "recent_filtered_alerts": list(filtered_alerts),
    }


def run_trading_bot():
    global tws_app
    telemetry = RuntimeTelemetry(component="trading_bot", base_dir=RUNTIME_FEEDBACK_DIR)

    tws_client_id = int(os.getenv("TRADING_TWS_CLIENT_ID", "11"))
    tws_port = int(os.getenv("TRADING_TWS_PORT", str(config.TWS_PORT)))
    print(f"[INIT] Connecting to TWS on port {tws_port} with client ID {tws_client_id}...")
    tws_app = create_tws_data_app(host="127.0.0.1", port=tws_port, client_id=tws_client_id)
    if not tws_app:
        print("[ERROR] Could not connect to TWS.")
        return

    time.sleep(2)

    print("[INIT] Fetching top gainers for in-process scanner...")
    symbols = get_top_gainers(top_n=20, use_ibkr=True, ibkr_port=tws_port, force_refresh=True)
    unique_symbols = list(set(symbols))
    print(f"[INIT] Monitoring {len(unique_symbols)} symbols: {', '.join(unique_symbols[:10])}{'...' if len(unique_symbols) > 10 else ''}")

    scanner = RealtimeBroadScanner(unique_symbols)
    initialize_alert_score_audit_file()

    executor = ExecutionEngine(
        tws_app=tws_app,
        account=ACCOUNT_NUMBER,
        tp_pct=TP_PCT,
        sl_pct=SL_PCT,
        investment_per_trade=INVESTMENT_PER_TRADE,
        telemetry=telemetry,
    )
    entry_manager = BreakoutEntryManager(telemetry=telemetry)
    symbol_states: Dict[str, TradeSymbolState] = {symbol: TradeSymbolState(symbol) for symbol in unique_symbols}
    trace_recorder = TradeTraceRecorder(
        telemetry=telemetry,
        scanner=scanner,
        symbol_states=symbol_states,
        entry_manager=entry_manager,
        executor=executor,
    )
    executor.register_position_event_callback(trace_recorder.on_execution_event)

    telemetry.log_event(
        "trading_bot_started",
        tws_client_id=tws_client_id,
        tws_port=tws_port,
        runtime_dir=telemetry.run_dir,
    )
    telemetry.log_event(
        "scanner_started",
        symbols=unique_symbols,
        client_id=tws_client_id,
        runtime_dir=telemetry.run_dir,
    )

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
        state = symbol_states.get(symbol)
        entry_manager.queue_candidate_from_scanner_alert(alert_event, state, executor, filtered_alerts)

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
                subprocess.run(
                    ["powershell", "-Command", ps_script],
                    capture_output=True,
                    timeout=5,
                )
            else:
                os.system(f'espeak "{voice_text}" 2>/dev/null')
        except Exception as e:
            print(f"[WARNING] Voice announcement failed: {e}")

    scanner.on_preliminary_alert(handle_scanner_alert)

    print("[INIT] Loading scanner fundamentals/news/history...")
    scanner.load_fundamentals(tws_app)
    scanner.load_news(tws_app)
    scanner.load_previous_closes(tws_app)
    scanner.load_historical_prices(tws_app)

    def handle_market_update(symbol, price, volume, vwap, bid, ask):
        state = symbol_states.setdefault(symbol, TradeSymbolState(symbol))
        state.update_market_data(price=price, vwap=vwap, bid=bid, ask=ask)
        scanner.update(symbol, price=price, volume=volume, vwap=vwap, bid=bid, ask=ask)
        entry_manager.evaluate_symbol(symbol, state, executor, filtered_alerts)
        executor.on_market_update(symbol, price=price, volume=volume, vwap=vwap, market_session=get_market_session())

    def create_market_data_callback(sym):
        return lambda s, p, v, vw, ts, b, a: handle_market_update(s, p, v, vw, b, a)

    print("[INIT] Subscribing to scanner/trading market data...")
    for symbol in unique_symbols:
        tws_app.subscribe_market_data(symbol, create_market_data_callback(symbol))
        telemetry.log_event("market_data_subscribed", symbol=symbol)

    last_session_check = datetime.now()
    next_symbol_update = get_next_10_minute_mark()
    eod_triggered = False
    et_tz = pytz.timezone('US/Eastern')

    try:
        while not should_exit:
            if (datetime.now() - last_session_check).total_seconds() > 60:
                if scanner.check_session_transition():
                    scanner.resync_vwap_all_symbols(tws_app)
                last_session_check = datetime.now()

            if datetime.now(et_tz) >= next_symbol_update:
                prior_symbols = set(scanner.monitors.keys())
                unique_symbols = update_scanner_symbols(
                    scanner,
                    tws_app,
                    unique_symbols,
                    market_data_callback=create_market_data_callback,
                )
                added_symbols = set(scanner.monitors.keys()) - prior_symbols
                for symbol in sorted(added_symbols):
                    symbol_states.setdefault(symbol, TradeSymbolState(symbol))
                    telemetry.log_event("market_data_subscribed", symbol=symbol)
                if added_symbols:
                    telemetry.log_event(
                        "scanner_symbols_updated",
                        monitored_symbols=sorted(scanner.monitors.keys()),
                        added_symbols=sorted(added_symbols),
                    )
                next_symbol_update = get_next_10_minute_mark()

            now = datetime.now()
            if now.hour == 15 and now.minute == 59 and not eod_triggered:
                print("[EOD] 3:59 PM reached. Triggering final cleanup...")
                executor.close_all_positions()
                eod_triggered = True
            if now.hour == 16 and eod_triggered:
                eod_triggered = False

            trace_recorder.capture_snapshots()
            state_payload = build_trading_runtime_state(
                scanner,
                symbol_states,
                executor,
                entry_manager,
                filtered_alerts,
                trace_recorder,
            )
            telemetry.write_state(state_payload)
            unified_visualization(state_payload["scanner"], filtered_alerts, executor, entry_manager)
            time.sleep(1)
    finally:
        telemetry.log_event("scanner_stopped")
        telemetry.log_event("trading_bot_stopped")
        tws_app.disconnect()
        print("[INFO] Bot stopped.")


if __name__ == "__main__":
    run_trading_bot()

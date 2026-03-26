"""
Real-Time Trading Bot - Two-Stage Architecture
Stage 1: Preliminary Screening (via realtime_scanner.py using conditions.py)
Stage 2: In-depth Filtering & Execution (this module)
"""
import time
import sys
import signal
import threading
import os
from datetime import datetime
from collections import deque
from typing import List, Dict

from realtime_multi_session_scanner import RealtimeBroadScanner, get_market_session
from execution_engine import ExecutionEngine
from tws_data_fetcher import create_tws_data_app
from top_gainers_fetcher import get_top_gainers
from alert_rating import get_alert_grade_rank
import scanner_config as config

# CONFIGURATION
# SYMBOLS will be dynamically fetched from top gainers (updates every 10 min)
SYMBOLS = []  # Will be populated at runtime
INVESTMENT_PER_TRADE = 500.0
TP_PCT = 5.0  
SL_PCT = 5.0
ACCOUNT_NUMBER = "DUO200259" # !!! IMPORTANT: REPLACE WITH YOUR IBKR PAPER TRADING ACCOUNT NUMBER !!!

# Global state
should_exit = False
tws_app = None
filtered_alerts = deque(maxlen=5)

def signal_handler(sig, frame):
    global should_exit
    print("\n[INFO] Graceful exit requested...")
    should_exit = True

signal.signal(signal.SIGINT, signal_handler)


def entries_allowed_in_current_session() -> bool:
    if not config.REALTIME_TRADE_REGULAR_HOURS_ONLY:
        return True
    return get_market_session() == "REGULAR"

class BreakoutEntryManager:
    """Queue Grade B+ alerts and enter on continuation or short-base breakouts."""

    def __init__(self):
        self.pending_entries: Dict[str, Dict] = {}
        self.cooldowns: Dict[str, datetime] = {}
        self.lock = threading.Lock()
        self.min_grade_rank = get_alert_grade_rank(config.REALTIME_TRADE_MIN_ALERT_GRADE)
        self.breakout_grade_rank = get_alert_grade_rank(config.REALTIME_BREAKOUT_MIN_ALERT_GRADE)

    def _set_cooldown(self, symbol: str, now: datetime) -> None:
        self.cooldowns[symbol] = now

    def _on_cooldown(self, symbol: str, now: datetime) -> bool:
        last_attempt = self.cooldowns.get(symbol)
        if last_attempt is None:
            return False
        return (now - last_attempt).total_seconds() < config.REALTIME_TRADE_SYMBOL_COOLDOWN_SECONDS

    def queue_candidate(self, symbol, timestamp, reasons, monitor, executor: ExecutionEngine, filtered_alerts) -> None:
        if not entries_allowed_in_current_session():
            return
        if monitor.alert_is_suppressed:
            return
        if get_alert_grade_rank(monitor.alert_grade) < self.min_grade_rank:
            return
        if executor.is_position_active(symbol) or symbol in executor.get_blacklist():
            return
        if not monitor.price_history:
            return

        now = datetime.now()
        with self.lock:
            if self._on_cooldown(symbol, now):
                return

            current_price = monitor.price_history[-1][1]
            entry_mode = (
                "continuation_breakout"
                if get_alert_grade_rank(monitor.alert_grade) >= self.breakout_grade_rank
                else "base_breakout"
            )
            existing = self.pending_entries.get(symbol)
            if existing:
                # Refresh the setup if a newer alert arrives, but keep the best high seen so far.
                existing["queued_at"] = now
                existing["alert_time"] = timestamp
                existing["alert_price"] = current_price
                existing["high_watermark"] = max(existing["high_watermark"], current_price)
                existing["alert_grade"] = monitor.alert_grade
                existing["alert_score"] = monitor.alert_score
                existing["reasons"] = list(reasons)
                existing["entry_mode"] = entry_mode
                return

            self.pending_entries[symbol] = {
                "queued_at": now,
                "alert_time": timestamp,
                "alert_price": current_price,
                "high_watermark": current_price,
                "alert_grade": monitor.alert_grade,
                "alert_score": monitor.alert_score,
                "reasons": list(reasons),
                "entry_mode": entry_mode,
                "recent_prices": deque(maxlen=64),
            }

        filtered_alerts.appendleft(
            f"{symbol} queued [{monitor.alert_grade} {monitor.alert_score}] at ${current_price:.2f} for {entry_mode.replace('_', ' ')}"
        )

    def evaluate_symbol(self, symbol, monitor, executor: ExecutionEngine, filtered_alerts) -> None:
        if not monitor.price_history:
            return

        action = None
        now = datetime.now()
        current_price = monitor.price_history[-1][1]
        current_vwap = monitor.vwap

        with self.lock:
            candidate = self.pending_entries.get(symbol)
            if candidate is None:
                return

            if not entries_allowed_in_current_session():
                self.pending_entries.pop(symbol, None)
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
                historical_prices = list(candidate["recent_prices"])
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
                    base_prices = [price for ts, price in historical_prices if ts.timestamp() >= base_window_start]
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
            return

        _, entry_price, grade, score = action
        success = executor.execute_trade(symbol, entry_price)
        if success:
            filtered_alerts.appendleft(
                f"{symbol} entered at ${entry_price:.2f} [{grade} {score}]"
            )
        else:
            filtered_alerts.appendleft(
                f"{symbol} entry failed at ${entry_price:.2f} [{grade} {score}]"
            )

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

    def discard_symbol(self, symbol: str) -> None:
        with self.lock:
            self.pending_entries.pop(symbol, None)
            self.cooldowns.pop(symbol, None)

def unified_visualization(scanner, filtered_alerts, executor, tws_app, entry_manager: BreakoutEntryManager):
    """Unified console display showing all three stages with enhanced separation"""
    os.system('cls' if os.name == 'nt' else 'clear')
    current_session = get_market_session()
    entry_status = "ENABLED" if entries_allowed_in_current_session() else "DISABLED"
    
    # 1. Preliminary Screening Section
    print("="*115)
    print(
        f" STAGE 1: PRELIMINARY SCREENING (SQUEEZE + VWAP + SPREAD) | "
        f"{datetime.now().strftime('%H:%M:%S')} | Session: {current_session} | Entries: {entry_status} "
    )
    print("="*115)
    print(f"{'SYMBOL':<8} | {'PRICE':<10} | {'VWAP':<10} | {'FLOAT':<10} | {'RVOL':<10} | {'SCREENING ALERTS'}")
    print("-"*115)
    
    blacklist = executor.get_blacklist()
    
    for symbol in scanner.symbols:
        if symbol not in scanner.monitors:
            continue
        m = scanner.monitors[symbol]
        
        # Check TWS real-time data for VWAP and sync status
        tws_data = tws_app.realtime_data.get(symbol, {})
        is_syncing = tws_data.get('syncing', False)
        
        price = f"${m.price_history[-1][1]:.2f}" if m.price_history else "N/A"
        
        if is_syncing:
            vwap_disp = "SYNCING..."
        else:
            vwap_disp = f"${m.vwap:.2f}" if m.vwap > 0 else "N/A"
            
        float_shares = f"{m.float_shares/1e6:.1f}M" if m.float_shares else "N/A"
        rvol = f"{m.relative_volume:.2f}x"
        
        if symbol in blacklist:
            alerts = "[BLACKLISTED]"
        else:
            alerts = ", ".join(m.triggered_conditions) if m.triggered_conditions else "--"
            
        print(f"{symbol:<8} | {price:<10} | {vwap_disp:<10} | {float_shares:<10} | {rvol:<10} | {alerts}")
    
    # 2. In-Depth Filtered Alerts Section
    print("\n" + "="*115)
    print(" STAGE 2: GRADE B+ BREAKOUT ENTRY QUEUE")
    print("="*115)
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
        
    # 3. Trade Execution Log & Positions
    print("\n" + "="*115)
    print(" STAGE 3: TRADE EXECUTION & POSITION TRACKING")
    print("="*115)
    
    # Active Positions Sub-section
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
    
    # SEPARATOR LINE
    print("\n" + "-" * 115)
    
    # Trade History Sub-section
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
            pnl_pct = (trade['exit_price'] - trade['entry_price']) / trade['entry_price'] * 100
            details = f"{trade['exit_type']} Exit at ${trade['exit_price']:.2f} (P&L: ${pnl:.2f}, {pnl_pct:+.2f}%)"
            print(f"{trade['symbol']:<8} | {'CLOSED':<12} | {details:<65} | {time_disp}")
        elif trade['type'] == 'PARTIAL':
            pnl = (trade['exit_price'] - trade['entry_price']) * trade['shares']
            pnl_pct = (trade['exit_price'] - trade['entry_price']) / trade['entry_price'] * 100
            details = f"Partial at ${trade['exit_price']:.2f} ({trade['shares']} sh, P&L: ${pnl:.2f}, {pnl_pct:+.2f}%)"
            print(f"{trade['symbol']:<8} | {'PARTIAL':<12} | {details:<65} | {time_disp}")
        else:
            details = f"{trade['reason']} at ~${trade['entry_price']:.2f}"
            print(f"{trade['symbol']:<8} | {'FAILED':<12} | {details:<65} | {time_disp}")
    print("="*115)

def update_symbol_list(scanner, tws_app, current_symbols: List[str], market_update_handler, entry_manager: BreakoutEntryManager) -> List[str]:
    """Update the monitored symbol list with new top gainers"""
    # Get updated list
    new_symbols = list(set(get_top_gainers(top_n=20)))
    
    # Find differences
    current_set = set(current_symbols)
    new_set = set(new_symbols)
    
    symbols_to_add = new_set - current_set
    symbols_to_remove = current_set - new_set
    
    if not symbols_to_add and not symbols_to_remove:
        return current_symbols  # No changes
    
    print(f"[SYMBOL UPDATE] Adding {len(symbols_to_add)} new symbols, removing {len(symbols_to_remove)} old symbols")
    
    # Remove old symbols
    for symbol in symbols_to_remove:
        if symbol in scanner.monitors:
            tws_app.unsubscribe_realtime_data(symbol)
            del scanner.monitors[symbol]
            scanner.symbols.remove(symbol)
            entry_manager.discard_symbol(symbol)
            print(f"[SYMBOL UPDATE] Removed {symbol}")
    
    # Add new symbols
    def create_callback(sym):
        return lambda s, p, v, vw, ts, b, a: market_update_handler(s, p, v, vw, b, a)
    
    for symbol in symbols_to_add:
        from realtime_multi_session_scanner import RealtimeSymbolMonitor
        scanner.monitors[symbol] = RealtimeSymbolMonitor(symbol)
        scanner.symbols.append(symbol)
        tws_app.subscribe_market_data(symbol, create_callback(symbol))
        print(f"[SYMBOL UPDATE] Added {symbol}")
    
    # Load fundamentals for new symbols
    for symbol in symbols_to_add:
        xml_data = tws_app.fetch_fundamental_data(symbol)
        if xml_data:
            try:
                import xml.etree.ElementTree as ET
                root = ET.fromstring(xml_data)
                monitor = scanner.monitors[symbol]
                for ratio in root.findall(".//Ratio"):
                    field = ratio.get("FieldName")
                    if field == 'FLOAT':
                        monitor.float_shares = float(ratio.text)
                    elif field == 'VOL10DAVG':
                        monitor.avg_daily_volume = float(ratio.text)
                        # Update TWS app data for volume correction reference
                        with tws_app.lock:
                            if symbol in tws_app.realtime_data:
                                tws_app.realtime_data[symbol]['avg_daily_volume'] = monitor.avg_daily_volume
            except Exception:
                pass

        try:
            scanner._load_symbol_news(symbol, scanner.monitors[symbol], tws_app)
        except Exception:
            pass

        try:
            close = tws_app.fetch_last_close(symbol)
            if close:
                scanner.monitors[symbol].day_start_price = close
        except Exception:
            pass

        try:
            bars = tws_app.fetch_historical_bars(symbol, duration="15 M", bar_size="1 min")
            for bar in bars:
                scanner.monitors[symbol].price_history.append((bar.date, bar.close))
            scanner.monitors[symbol].seed_signal_volume_history_from_bars(bars)
        except Exception:
            pass
    
    return new_symbols

def run_trading_bot():
    global tws_app
    
    # Get dynamic top gainers list
    print("[INIT] Fetching top 20 gainers...")
    SYMBOLS = get_top_gainers(top_n=20)
    unique_symbols = list(set(SYMBOLS))
    print(f"[INIT] Monitoring {len(unique_symbols)} symbols: {', '.join(unique_symbols[:10])}{'...' if len(unique_symbols) > 10 else ''}")
    
    tws_client_id = int(os.getenv("TRADING_TWS_CLIENT_ID", "11"))
    tws_port = int(os.getenv("TRADING_TWS_PORT", str(config.TWS_PORT)))
    print(f"[INIT] Connecting to TWS on port {tws_port} with client ID {tws_client_id}...")
    tws_app = create_tws_data_app(host="127.0.0.1", port=tws_port, client_id=tws_client_id)
    if not tws_app:
        print("[ERROR] Could not connect to TWS.")
        return

    scanner = RealtimeBroadScanner(symbols=unique_symbols)
    executor = ExecutionEngine(
        tws_app=tws_app,
        account=ACCOUNT_NUMBER,
        tp_pct=TP_PCT,
        sl_pct=SL_PCT,
        investment_per_trade=INVESTMENT_PER_TRADE
    )
    
    entry_manager = BreakoutEntryManager()
    scanner.load_fundamentals(tws_app)
    scanner.load_news(tws_app)
    scanner.load_previous_closes(tws_app)
    scanner.load_historical_prices(tws_app)

    def preliminary_alert_handler(symbol, timestamp, reasons, monitor):
        entry_manager.queue_candidate(symbol, timestamp, reasons, monitor, executor, filtered_alerts)

    scanner.on_preliminary_alert(preliminary_alert_handler)

    print("[INIT] Subscribing to live market data...")
    def handle_market_update(symbol, price, volume, vwap, bid, ask):
        scanner.update(symbol, price=price, volume=volume, vwap=vwap, bid=bid, ask=ask)
        monitor = scanner.monitors.get(symbol)
        if monitor is not None:
            entry_manager.evaluate_symbol(symbol, monitor, executor, filtered_alerts)
        executor.on_market_update(symbol, price=price, vwap=vwap, market_session=get_market_session())

    def create_callback(sym):
        return lambda s, p, v, vw, ts, b, a: handle_market_update(s, p, v, vw, b, a)

    for symbol in unique_symbols:
        tws_app.subscribe_market_data(symbol, create_callback(symbol))
    
    print("[INIT] Starting Unified Trading Interface...")
    time.sleep(2)
    
    eod_triggered = False
    last_symbol_update = datetime.now()
    symbol_update_interval = 600  # Update every 10 minutes
    
    while not should_exit:
        now = datetime.now()
        
        # Periodic Symbol List Update (every 10 minutes)
        if (now - last_symbol_update).total_seconds() >= symbol_update_interval:
            unique_symbols = update_symbol_list(scanner, tws_app, unique_symbols, handle_market_update, entry_manager)
            last_symbol_update = now
        
        # EOD Cleanup Check (3:59 PM ET)
        if now.hour == 15 and now.minute == 59 and not eod_triggered:
            print("[EOD] 3:59 PM reached. Triggering final cleanup...")
            executor.close_all_positions()
            eod_triggered = True
        
        # Reset EOD trigger after market close
        if now.hour == 16 and eod_triggered:
            eod_triggered = False

        unified_visualization(scanner, filtered_alerts, executor, tws_app, entry_manager)
        time.sleep(1)

    tws_app.disconnect()
    print("[INFO] Bot stopped.")

if __name__ == "__main__":
    run_trading_bot()

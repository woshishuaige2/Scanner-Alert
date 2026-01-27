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

from realtime_scanner import RealtimeBroadScanner
from execution_engine import ExecutionEngine
from tws_data_fetcher import create_tws_data_app
import scanner_config as config

# CONFIGURATION
SYMBOLS = ["MOVE", "BNAI", "DRCT", "THH", "REVB", "MAXN"]
INVESTMENT_PER_TRADE = 100.0
TP_PCT = 1.0
SL_PCT = 10.0
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

class InDepthFilter:
    """Performs strict momentum filtering on symbols that passed preliminary screening"""
    @staticmethod
    def check(symbol, monitor, cooldown_tracker: Dict[str, datetime], executor: ExecutionEngine) -> bool:
        if not config.STRICT_MOMENTUM_REQUIRED:
            return True
            
        # Blacklist Check
        if symbol in executor.get_blacklist():
            return False

        # Cooldown Check (60 seconds)
        now = datetime.now()
        if symbol in cooldown_tracker:
            if (now - cooldown_tracker[symbol]).total_seconds() < 60:
                return False 
            
        # Current Stage 2 Logic (can be updated in conditions.py later)
        if len(monitor.price_history) < 10:
            return False
            
        current_price = monitor.price_history[-1][1]
        price_10s_ago = monitor.price_history[-10][1]
        surge = (current_price - price_10s_ago) / price_10s_ago * 100
        
        if surge < config.MIN_PRICE_SURGE_10S:
            return False
            
        prices_10s = [p for ts, p in list(monitor.price_history)[-10:]]
        max_p = max(prices_10s)
        drawdown = (max_p - current_price) / max_p * 100
        
        if drawdown > config.MAX_DRAWDOWN_10S:
            return False
            
        return True

def unified_visualization(scanner, filtered_alerts, executor, tws_app):
    """Unified console display showing all three stages with enhanced separation"""
    os.system('cls' if os.name == 'nt' else 'clear')
    
    # 1. Preliminary Screening Section
    print("="*115)
    print(f" STAGE 1: PRELIMINARY SCREENING (SQUEEZE + VWAP + SPREAD) | {datetime.now().strftime('%H:%M:%S')} ")
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
    print(" STAGE 2: IN-DEPTH FILTERED ALERTS (STRICT MOMENTUM)")
    print("="*115)
    if not filtered_alerts:
        print("  No symbols passed in-depth filtering yet...")
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
        else:
            details = f"{trade['reason']} at ~${trade['entry_price']:.2f}"
            print(f"{trade['symbol']:<8} | {'FAILED':<12} | {details:<65} | {time_disp}")
    print("="*115)

def run_trading_bot():
    global tws_app
    
    unique_symbols = list(set(SYMBOLS))
    
    print("[INIT] Connecting to TWS...")
    tws_app = create_tws_data_app(host="127.0.0.1", port=7497, client_id=888)
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
    
    in_depth_cooldown: Dict[str, datetime] = {}
    scanner.load_fundamentals(tws_app)

    def preliminary_alert_handler(symbol, timestamp, reasons, monitor):
        if executor.is_position_active(symbol):
            return 

        # Stage 2: In-depth Filtering
        if InDepthFilter.check(symbol, monitor, in_depth_cooldown, executor):
            in_depth_cooldown[symbol] = datetime.now()
            alert_msg = f"{symbol} passed strict momentum at ${monitor.price_history[-1][1]:.2f} ({timestamp.strftime('%H:%M:%S')})"
            
            # Stage 3: Execution
            success = executor.execute_trade(symbol, monitor.price_history[-1][1])
            if success:
                filtered_alerts.appendleft(alert_msg)

    scanner.on_preliminary_alert(preliminary_alert_handler)

    print("[INIT] Subscribing to live market data...")
    def create_callback(sym):
        return lambda s, p, v, vw, ts, b, a: scanner.update(s, price=p, volume=v, vwap=vw, bid=b, ask=a)

    for symbol in unique_symbols:
        tws_app.subscribe_market_data(symbol, create_callback(symbol))
    
    print("[INIT] Starting Unified Trading Interface...")
    time.sleep(2)
    
    eod_triggered = False
    
    while not should_exit:
        now = datetime.now()
        
        # EOD Cleanup Check (3:59 PM ET)
        if now.hour == 15 and now.minute == 59 and not eod_triggered:
            print("[EOD] 3:59 PM reached. Triggering final cleanup...")
            executor.close_all_positions()
            eod_triggered = True
        
        # Reset EOD trigger after market close
        if now.hour == 16 and eod_triggered:
            eod_triggered = False

        unified_visualization(scanner, filtered_alerts, executor, tws_app)
        time.sleep(1)

    tws_app.disconnect()
    print("[INFO] Bot stopped.")

if __name__ == "__main__":
    run_trading_bot()

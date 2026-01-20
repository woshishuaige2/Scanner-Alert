"""
Real-Time Trading Bot - Two-Stage Architecture
Stage 1: Preliminary Screening (via realtime_scanner.py)
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

from realtime_scanner import RealtimeBroadScanner, display_broad_screening
from execution_engine import ExecutionEngine
from tws_data_fetcher import create_tws_data_app
import scanner_config as config

# CONFIGURATION
SYMBOLS = ["IVF", "SHPH", "POLA", "CRVS", "CCHH"]
INVESTMENT_PER_TRADE = 100.0
TP_PCT = 1.0
SL_PCT = 10.0

# Global state
should_exit = False
tws_app = None
filtered_alerts = deque(maxlen=10)
trade_log = deque(maxlen=10)

def signal_handler(sig, frame):
    global should_exit
    print("\n[INFO] Graceful exit requested...")
    should_exit = True

signal.signal(signal.SIGINT, signal_handler)

class InDepthFilter:
    """Performs strict momentum filtering on symbols that passed preliminary screening"""
    @staticmethod
    def check(symbol, monitor, cooldown_tracker: Dict[str, datetime]) -> bool:
        if not config.STRICT_MOMENTUM_REQUIRED:
            return True
            
        # Cooldown Check (60 seconds)
        now = datetime.now()
        if symbol in cooldown_tracker:
            if (now - cooldown_tracker[symbol]).total_seconds() < 60:
                return False # Still in cooldown period
            
        # 1. Price Surge Check (e.g., 1.5% in 10s)
        if len(monitor.price_history) < 10:
            return False
            
        current_price = monitor.price_history[-1][1]
        price_10s_ago = monitor.price_history[-10][1]
        surge = (current_price - price_10s_ago) / price_10s_ago * 100
        
        if surge < config.MIN_PRICE_SURGE_10S:
            return False
            
        # 2. Drawdown Check (no more than 0.5% drop in last 10s)
        prices_10s = [p for ts, p in list(monitor.price_history)[-10:]]
        max_p = max(prices_10s)
        drawdown = (max_p - current_price) / max_p * 100
        
        if drawdown > config.MAX_DRAWDOWN_10S:
            return False
            
        return True

def unified_visualization(scanner, filtered_alerts, trade_log, executor):
    """Unified console display showing all three stages"""
    os.system('cls' if os.name == 'nt' else 'clear')
    
    # 1. Preliminary Screening Section
    print("="*100)
    print(f" STAGE 1: PRELIMINARY SCREENING (ROSS CAMERON STYLE) | {datetime.now().strftime('%H:%M:%S')} ")
    print("="*100)
    # Increased RVOL column width for better alignment
    print(f"{'SYMBOL':<8} | {'PRICE':<10} | {'FLOAT':<12} | {'RVOL':<15} | {'SCREENING ALERTS'}")
    print("-"*100)
    for symbol in SYMBOLS: # Use the global SYMBOLS list for display
        # Ensure the monitor exists before trying to access it
        if symbol not in scanner.monitors:
            print(f"{symbol:<8} | {'N/A':<10} | {'N/A':<12} | {'N/A':<15} | {'Monitor not initialized'}")
            continue
            
        m = scanner.monitors[symbol]
        price = f"${m.price_history[-1][1]:.2f}" if m.price_history else "N/A"
        float_str = f"{m.float_shares/1e6:.1f}M" if m.float_shares else "N/A"
        # Format RVOL to handle large numbers and align with new width
        rvol = f"{m.relative_volume:.2f}x"
        alerts = ", ".join(m.triggered_conditions) if m.triggered_conditions else "--"
        
        print(f"{symbol:<8} | {price:<10} | {float_str:<12} | {rvol:<15} | {alerts}")
    
    # 2. In-Depth Filtered Alerts Section
    print("\n" + "="*100)
    print(" STAGE 2: IN-DEPTH FILTERED ALERTS (STRICT MOMENTUM)")
    print("="*100)
    if not filtered_alerts:
        print("  No symbols passed in-depth filtering yet...")
    for alert in filtered_alerts:
        print(f"  [FILTERED] {alert}")
        
    # 3. Trade Log Section
    print("\n" + "="*100)
    print(" STAGE 3: TRADE EXECUTION LOG & POSITIONS")
    print("="*100)
    active_pos = executor.get_active_positions()
    print(f"  ACTIVE POSITIONS: {', '.join(active_pos) if active_pos else 'None'}")
    print("-" * 100)
    if not trade_log:
        print("  No trades executed in this session.")
    for log in trade_log:
        print(f"  [TRADE] {log}")
    print("="*100)

def run_trading_bot():
    global tws_app
    
    # Ensure all symbols are unique and valid before proceeding
    unique_symbols = list(set(SYMBOLS))
    
    print("[INIT] Connecting to TWS...")
    tws_app = create_tws_data_app(host="127.0.0.1", port=7497, client_id=888)
    if not tws_app:
        print("[ERROR] Could not connect to TWS.")
        return

    # Initialize Components
    scanner = RealtimeBroadScanner(symbols=unique_symbols)
    executor = ExecutionEngine(
        tws_app=tws_app,
        tp_pct=TP_PCT,
        sl_pct=SL_PCT,
        investment_per_trade=INVESTMENT_PER_TRADE
    )
    
    # Cooldown tracker for in-depth filter (symbol -> datetime)
    in_depth_cooldown: Dict[str, datetime] = {}
    
    # Load Fundamentals for Stage 1
    scanner.load_fundamentals(tws_app)

    # Define Alert Handler for Stage 1 -> Stage 2 transition
    def preliminary_alert_handler(symbol, timestamp, reasons, monitor):
        # Check if symbol is already in an active position (Rule 3)
        if executor.is_position_active(symbol):
            return # Skip in-depth check and execution if already trading this symbol

        # Stage 2: In-depth Filtering (Rule 2: 60s Cooldown)
        if InDepthFilter.check(symbol, monitor, in_depth_cooldown):
            
            # Update cooldown tracker
            in_depth_cooldown[symbol] = datetime.now()
            
            alert_msg = f"{symbol} passed strict momentum at ${monitor.price_history[-1][1]:.2f} ({timestamp.strftime('%H:%M:%S')})"
            if alert_msg not in filtered_alerts:
                filtered_alerts.appendleft(alert_msg)
                
                # Stage 3: Execution
                success = executor.execute_trade(symbol, monitor.price_history[-1][1])
                if success:
                    trade_log.appendleft(f"BUY {symbol} at ${monitor.price_history[-1][1]:.2f} | {timestamp.strftime('%H:%M:%S')}")

    scanner.on_preliminary_alert(preliminary_alert_handler)

    # Subscribe to Live Data
    print("[INIT] Subscribing to live market data...")
    def create_callback(sym):
        return lambda s, p, v, vw, ts, b, a: scanner.update(s, price=p, volume=v, vwap=vw, bid=b, ask=a)

    for symbol in unique_symbols:
        tws_app.subscribe_market_data(symbol, create_callback(symbol))
    
    print("[INIT] Starting Unified Trading Interface...")
    time.sleep(2)
    
    while not should_exit:
        unified_visualization(scanner, filtered_alerts, trade_log, executor)
        time.sleep(1)

    tws_app.disconnect()
    print("[INFO] Bot stopped.")

if __name__ == "__main__":
    run_trading_bot()

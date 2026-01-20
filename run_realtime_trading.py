"""
Real-Time Trading Bot
Integrates RealtimeAlertScanner with ExecutionEngine for automated paper trading.
"""
import time
import sys
import signal
import threading
from datetime import datetime
from collections import deque

from realtime_scanner import RealtimeAlertScanner, display_status_table
from execution_engine import ExecutionEngine
from tws_data_fetcher import create_tws_data_app

# CONFIGURATION
SYMBOLS = ["SPHL", "CJMB", "MLEC", "AUID", "AHMA"]
INVESTMENT_PER_TRADE = 1000.0
TP_PCT = 1.0
SL_PCT = 10.0

# Global state
should_exit = False
tws_app = None

def signal_handler(sig, frame):
    global should_exit
    print("\n[INFO] Graceful exit requested...")
    should_exit = True

signal.signal(signal.SIGINT, signal_handler)

def run_trading_bot():
    global tws_app
    
    print("="*80)
    print("      REAL-TIME TRADING BOT (PAPER TRADING)      ")
    print("="*80)
    print(f"✓ Symbols: {', '.join(SYMBOLS)}")
    print(f"✓ Strategy: TP {TP_PCT}% / SL {SL_PCT}%")
    print(f"✓ Investment: ${INVESTMENT_PER_TRADE} per trade")
    
    # 1. Connect to TWS
    print("\n[1] Connecting to TWS (Port 7497)...")
    tws_app = create_tws_data_app(host="127.0.0.1", port=7497, client_id=999)
    if not tws_app:
        print("[ERROR] Could not connect to TWS. Ensure TWS is running and API is enabled.")
        return

    # 2. Initialize Components
    scanner = RealtimeAlertScanner(symbols=SYMBOLS)
    executor = ExecutionEngine(
        tws_app=tws_app,
        tp_pct=TP_PCT,
        sl_pct=SL_PCT,
        investment_per_trade=INVESTMENT_PER_TRADE
    )
    
    last_alerts = deque(maxlen=10)
    state = {'last_alert_triggered': False}

    # 3. Define Alert Handler (Execution Trigger)
    def trading_alert_handler(symbol, timestamp, reasons, data):
        # Log the alert
        alert_msg = (
            f"TRADE ALERT: {symbol} at ${data.price:.2f}\n"
            f"Time: {timestamp.strftime('%H:%M:%S')}\n"
            f"Reasons: {reasons}"
        )
        last_alerts.appendleft(alert_msg)
        state['last_alert_triggered'] = True
        
        # EXECUTE TRADE
        executor.execute_trade(symbol, data.price)

    scanner.on_alert(trading_alert_handler)

    # 4. Load Baseline Data
    print("\n[2] Loading baseline historical data...")
    scanner.load_today_historical_bars(tws_app, bar_size="5 mins")

    # 5. Subscribe to Live Data
    print("\n[3] Subscribing to live market data...")
    def create_callback(sym):
        return lambda s, p, v, vw, ts, b, a: scanner.update(s, price=p, volume=v, vwap=vw, bid=b, ask=a)

    for symbol in SYMBOLS:
        tws_app.subscribe_market_data(symbol, create_callback(symbol))
    
    print("\n[4] Starting Trading Loop...")
    time.sleep(5) # Wait for initial data
    
    last_display_time = time.time()
    
    while not should_exit:
        current_time = time.time()
        
        # Update display if alert triggered or interval reached
        if state['last_alert_triggered'] or (current_time - last_display_time >= 5):
            state['last_alert_triggered'] = False
            display_status_table(scanner, last_alerts)
            
            # Show active positions below the table
            active_pos = executor.get_active_positions()
            if active_pos:
                print("\n" + "="*30)
                print(f" ACTIVE POSITIONS: {', '.join(active_pos)}")
                print("="*30)
            
            last_display_time = current_time
            
        time.sleep(0.1)

    # Cleanup
    print("\n[INFO] Disconnecting from TWS...")
    tws_app.disconnect()
    print("[INFO] Bot stopped.")

if __name__ == "__main__":
    run_trading_bot()

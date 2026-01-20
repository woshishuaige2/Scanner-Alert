import time
import threading
from datetime import datetime
from collections import deque
import random

# Import components
from realtime_scanner import RealtimeBroadScanner
from mock_tws import MockTWSDataApp, MockExecutionEngine, create_tws_data_app
from run_realtime_trading import SYMBOLS, InDepthFilter, unified_visualization, trade_log, filtered_alerts

# CONFIGURATION
INVESTMENT_PER_TRADE = 100.0
TP_PCT = 1.0
SL_PCT = 10.0

# Global state
should_exit = False
tws_app = None
scanner = None
executor = None

def simulate_market_data(scanner: RealtimeBroadScanner):
    """Simulate real-time market data updates for the symbols."""
    prices = {s: 10.0 + random.uniform(-0.5, 0.5) for s in SYMBOLS}
    volumes = {s: 1000000 + random.randint(-100000, 100000) for s in SYMBOLS}
    
    while not should_exit:
        for symbol in SYMBOLS:
            # Simulate price movement (slight random walk)
            prices[symbol] += random.uniform(-0.05, 0.05)
            prices[symbol] = max(1.0, prices[symbol]) # Keep price above 1
            
            # Simulate a spike for IVF to trigger the screening
            if symbol == "IVF" and random.random() < 0.2:
                prices[symbol] += 0.5 # 50 cent spike
            
            # Simulate volume update (cumulative volume is not used in the new scanner, just the incremental)
            # For simplicity, we'll pass a large number for cumulative volume
            scanner.update(
                symbol=symbol,
                price=prices[symbol],
                volume=volumes[symbol] + random.randint(100, 500), # Mock cumulative volume
                vwap=prices[symbol] * 0.99, # Mock VWAP
                bid=prices[symbol] - 0.01,
                ask=prices[symbol] + 0.01
            )
        
        time.sleep(0.1) # Update every 100ms

def run_test():
    global tws_app, scanner, executor
    
    print("[TEST] Starting Unified Visualization Test...")
    
    # 1. Connect to Mock TWS
    tws_app = MockTWSDataApp()
    tws_app.connect()

    # 2. Initialize Components
    scanner = RealtimeBroadScanner(symbols=SYMBOLS)
    executor = MockExecutionEngine(
        tws_app=tws_app,
        tp_pct=TP_PCT,
        sl_pct=SL_PCT,
        investment_per_trade=INVESTMENT_PER_TRADE
    )
    
    # 3. Load Fundamentals for Stage 1
    scanner.load_fundamentals(tws_app)

    # 4. Define Alert Handler for Stage 1 -> Stage 2 transition
    def preliminary_alert_handler(symbol, timestamp, reasons, monitor):
        # Stage 2: In-depth Filtering
        if InDepthFilter.check(symbol, monitor):
            alert_msg = f"{symbol} passed strict momentum at ${monitor.price_history[-1][1]:.2f} ({timestamp.strftime('%H:%M:%S')})"
            
            # Check if this alert is new to avoid spamming the deque
            if not any(symbol in a for a in filtered_alerts):
                filtered_alerts.appendleft(alert_msg)
                
                # Stage 3: Execution
                success = executor.execute_trade(symbol, monitor.price_history[-1][1])
                if success:
                    trade_log.appendleft(f"BUY {symbol} at ${monitor.price_history[-1][1]:.2f} | {timestamp.strftime('%H:%M:%S')}")

    scanner.on_preliminary_alert(preliminary_alert_handler)

    # 5. Start Market Data Simulation Thread
    data_thread = threading.Thread(target=simulate_market_data, args=(scanner,))
    data_thread.daemon = True
    data_thread.start()
    
    print("[TEST] Starting Visualization Loop...")
    time.sleep(1) # Wait for initial data
    
    # 6. Main Visualization Loop
    global should_exit
    try:
        while not should_exit:
            unified_visualization(scanner, filtered_alerts, trade_log, executor)
            time.sleep(1)
    except KeyboardInterrupt:
        should_exit = True
        print("\n[TEST] Test stopped by user.")
    
    tws_app.disconnect()
    print("[TEST] Test finished.")

if __name__ == "__main__":
    run_test()

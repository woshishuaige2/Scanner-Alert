
import sys
import os
from datetime import datetime
import time

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from backtest_scanner import BacktestAlertScanner
from tws_data_fetcher import create_tws_data_app

# NGROK CONFIGURATION (Change to 127.0.0.1 for local use)
NGROK_HOST = "8.tcp.ngrok.io"
NGROK_PORT = 18060
SYMBOLS = ["YCBD"]
BACKTEST_DATE = "2025-12-12"

# Scenarios: (Take Profit %, Stop Loss %)
SCENARIOS = [(2.0, 1.0), (4.0, 2.0), (10.0, 5.0), (20.0, 10.0), (1.0, 10.0)]

def run():
    print(f"\n[INFO] Connecting to TWS at {NGROK_HOST}:{NGROK_PORT}...", flush=True)
    tws_app = create_tws_data_app(host=NGROK_HOST, port=NGROK_PORT, client_id=997)
    if not tws_app:
        print("[ERROR] Could not connect to TWS.", flush=True)
        return
    
    scanner = BacktestAlertScanner(symbols=SYMBOLS, date=BACKTEST_DATE)
    print(f"[INFO] Fetching data for {', '.join(SYMBOLS)}...", flush=True)
    if not scanner.load_data_from_tws(tws_app):
        print("[ERROR] Failed to load data.", flush=True)
        tws_app.disconnect()
        return
    
    print("\n[INFO] Running backtest with 60s cooldown and Volume Confirmation...", flush=True)
    alerts = scanner.run_backtest()
    
    # 1. DETAILED ALERT LOG
    print("\n" + "="*80, flush=True)
    print(f"{'DETAILED ALERT LOG':^80}", flush=True)
    print("="*80, flush=True)
    
    for symbol in SYMBOLS:
        print(f"\n>>> {symbol}", flush=True)
        symbol_alerts = alerts.get(symbol, [])
        if not symbol_alerts:
            print("    No alerts triggered.", flush=True)
        else:
            for i, alert in enumerate(symbol_alerts):
                print(f"    [{i+1}] {alert.timestamp.strftime('%H:%M:%S')} | Price: ${alert.price:.2f} | VWAP: ${alert.vwap:.2f}", flush=True)
    
    # 2. WIN RATE SUMMARY
    print("\n" + "="*80, flush=True)
    print(f"{'WIN RATE SUMMARY (2:1 Reward-to-Risk)':^80}", flush=True)
    print("="*80, flush=True)
    
    header = f"{'SCENARIO':<20} | {'SYMBOL':<10} | {'ALERTS':<8} | {'WINS':<6} | {'LOSSES':<8} | {'WIN RATE':<10} | {'FINAL ASSET':<12}"
    print(header, flush=True)
    print("-" * 100, flush=True)
    
    for tp, sl in SCENARIOS:
        # Reset assets for each scenario to start fresh with $10000
        scanner.current_assets = {s: scanner.initial_asset for s in SYMBOLS}
        pl_results = scanner.calculate_pl(tp, sl)
        for symbol in SYMBOLS:
            res = pl_results.get(symbol, [])
            wins = len([r for r in res if r['outcome'] == "WIN"])
            losses = len([r for r in res if r['outcome'] == "LOSS"])
            total = wins + losses
            wr = (wins / total * 100) if total > 0 else 0
            
            # Get final asset for this symbol in this scenario
            final_asset = scanner.current_assets[symbol]
            
            row = f"TP:{tp:>4.1f}% / SL:{sl:>4.1f}% | {symbol:<10} | {len(res):<8} | {wins:<6} | {losses:<8} | {wr:>8.1f}% | ${final_asset:>10.2f}"
            print(row, flush=True)
        print("-" * 100, flush=True)
    
    print("\n[INFO] Backtest complete. Disconnecting...", flush=True)
    tws_app.disconnect()

if __name__ == "__main__":
    run()

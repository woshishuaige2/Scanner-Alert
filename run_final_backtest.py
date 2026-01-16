
import sys
import os
from datetime import datetime
import time

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from backtest_scanner import BacktestAlertScanner
from tws_data_fetcher import create_tws_data_app

# NGROK CONFIGURATION (Change to 127.0.0.1 for local use)
NGROK_HOST = "0.tcp.ngrok.io"
NGROK_PORT = 14317
SYMBOLS = ["SPHL"]
BACKTEST_DATE = "2026-01-15"

# Scenarios: (Take Profit %, Stop Loss %)
SCENARIOS = [(2.0, 1.0), (4.0, 2.0), (10.0, 5.0), (20.0, 10.0), (1.0, 10.0)]

def run():
    print(f"\n[INFO] Connecting to TWS at {NGROK_HOST}:{NGROK_PORT}...", flush=True)
    tws_app = create_tws_data_app(host=NGROK_HOST, port=NGROK_PORT, client_id=998)
    if not tws_app:
        print("[ERROR] Could not connect to TWS.", flush=True)
        return
    
    scanner = BacktestAlertScanner(symbols=SYMBOLS, date=BACKTEST_DATE)
    print(f"[INFO] Fetching data for {', '.join(SYMBOLS)}...", flush=True)
    # Using 10 secs for reliable high-resolution backtest
    if not scanner.load_data_from_tws(tws_app, bar_size="10 secs"):
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
                # Find logic used in conditions
                logic = "Unknown"
                for reason in alert.conditions_triggered:
                    if "Logic:" in reason:
                        logic = reason.split("Logic: ")[1]
                        break
                print(f"    [{i+1}] {alert.timestamp.strftime('%H:%M:%S')} | Price: ${alert.price:.2f} | VWAP: ${alert.vwap:.2f} | Logic: {logic}", flush=True)
    
    # 2. WIN RATE SUMMARY
    print("\n" + "="*80, flush=True)
    print(f"{'WIN RATE SUMMARY (2:1 Reward-to-Risk)':^80}", flush=True)
    print("="*80, flush=True)
    
    header = f"{'SCENARIO':<20} | {'SYMBOL':<10} | {'ALERTS':<8} | {'WINS':<6} | {'LOSSES':<8} | {'WIN RATE':<10} | {'COMMISSION':<12} | {'FINAL ASSET':<12}"
    print(header, flush=True)
    print("-" * 120, flush=True)
    
    for tp, sl in SCENARIOS:
        # Reset assets for each scenario to start fresh with $10000
        scanner.current_assets = {s: scanner.initial_asset for s in SYMBOLS}
        pl_results = scanner.calculate_pl(tp, sl)
        for symbol in SYMBOLS:
            res = pl_results.get(symbol, [])
            wins = len([r for r in res if r['outcome'] == "WIN"])
            losses = len([r for r in res if r['outcome'] == "LOSS"])
            total_comm = sum([r['commission'] for r in res])
            total = wins + losses
            wr = (wins / total * 100) if total > 0 else 0
            
            # Get final asset for this symbol in this scenario
            final_asset = scanner.current_assets[symbol]
            
            row = f"TP:{tp:>4.1f}% / SL:{sl:>4.1f}% | {symbol:<10} | {len(res):<8} | {wins:<6} | {losses:<8} | {wr:>8.1f}% | ${total_comm:>10.2f} | ${final_asset:>10.2f}"
            print(row, flush=True)
        print("-" * 120, flush=True)
    
    print("\n[INFO] Backtest complete. Disconnecting...", flush=True)
    tws_app.disconnect()

if __name__ == "__main__":
    run()

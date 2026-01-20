
import sys
import os
from datetime import datetime
import time
import json
import ast

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from backtest_scanner import BacktestAlertScanner
from tws_data_fetcher import create_tws_data_app

# NGROK CONFIGURATION (Change to 127.0.0.1 for local use)
NGROK_HOST = "0.tcp.ngrok.io"
NGROK_PORT = 14317

# INPUT FILE CONFIGURATION
INPUT_FILE = "days_with_more_than_2_symbols.txt"

# FALLBACK CONFIGURATION (Used if INPUT_FILE is missing or empty)
FALLBACK_SYMBOLS = ["SPHL"]
FALLBACK_DATE = "2026-01-15"

# Scenarios: (Take Profit %, Stop Loss %)
SCENARIOS = [(2.0, 1.0), (4.0, 2.0), (10.0, 5.0), (20.0, 10.0), (1.0, 10.0)]

def parse_input_file(file_path):
    """Parses the input file for dates and symbols."""
    batch_tasks = []
    # Ensure we look in the same directory as the script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    abs_path = os.path.join(script_dir, file_path)
    
    if not os.path.exists(abs_path):
        return batch_tasks
        
    try:
        with open(abs_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or ':' not in line:
                    continue
                
                date_str, symbols_str = line.split(':', 1)
                date_str = date_str.strip()
                try:
                    # Use ast.literal_eval to safely parse the list string
                    symbols = ast.literal_eval(symbols_str.strip())
                    if isinstance(symbols, list) and symbols:
                        batch_tasks.append((date_str, symbols))
                except Exception as e:
                    print(f"[WARN] Failed to parse symbols for {date_str}: {e}")
    except Exception as e:
        print(f"[ERROR] Error reading input file: {e}")
        
    return batch_tasks

def run_backtest_for_task(tws_app, date_str, symbols):
    """Runs a backtest for a specific date and set of symbols."""
    print(f"\n" + "="*80)
    print(f" BACKTESTING: {date_str} | SYMBOLS: {', '.join(symbols)}")
    print("="*80)
    
    scanner = BacktestAlertScanner(symbols=symbols, date=date_str)
    print(f"[INFO] Fetching data for {', '.join(symbols)}...", flush=True)
    
    # Using 10 secs for reliable high-resolution backtest
    if not scanner.load_data_from_tws(tws_app, bar_size="10 secs"):
        print(f"[ERROR] Failed to load data for {date_str}.", flush=True)
        return
    
    print(f"[INFO] Running backtest for {date_str}...", flush=True)
    alerts = scanner.run_backtest()
    
    # 1. DETAILED ALERT LOG
    print("\n" + "-"*40)
    print(f"{'DETAILED ALERT LOG':^40}")
    print("-"*40)
    
    for symbol in symbols:
        symbol_alerts = alerts.get(symbol, [])
        if not symbol_alerts:
            continue
            
        print(f"\n>>> {symbol}", flush=True)
        for i, alert in enumerate(symbol_alerts):
            logic = "Unknown"
            for reason in alert.conditions_triggered:
                if "Logic:" in reason:
                    logic = reason.split("Logic: ")[1]
                    break
            print(f"    [{i+1}] {alert.timestamp.strftime('%H:%M:%S')} | Price: ${alert.price:.2f} | VWAP: ${alert.vwap:.2f} | Logic: {logic}", flush=True)
    
    # 2. WIN RATE SUMMARY
    print("\n" + "-"*40)
    print(f"{'WIN RATE SUMMARY':^40}")
    print("-"*40)
    
    header = f"{'SCENARIO':<20} | {'SYMBOL':<10} | {'ALERTS':<8} | {'WINS':<6} | {'LOSSES':<8} | {'WIN RATE':<10} | {'FINAL ASSET':<12}"
    print(header, flush=True)
    
    for tp, sl in SCENARIOS:
        # Reset assets for each scenario
        scanner.current_assets = {s: scanner.initial_asset for s in symbols}
        pl_results = scanner.calculate_pl(tp, sl)
        for symbol in symbols:
            res = pl_results.get(symbol, [])
            wins = len([r for r in res if r['outcome'] == "WIN"])
            losses = len([r for r in res if r['outcome'] == "LOSS"])
            total = wins + losses
            wr = (wins / total * 100) if total > 0 else 0
            final_asset = scanner.current_assets[symbol]
            
            row = f"TP:{tp:>4.1f}% / SL:{sl:>4.1f}% | {symbol:<10} | {len(res):<8} | {wins:<6} | {losses:<8} | {wr:>8.1f}% | ${final_asset:>10.2f}"
            print(row, flush=True)

def run():
    # Try to parse input file
    batch_tasks = parse_input_file(INPUT_FILE)
    
    if not batch_tasks:
        print(f"[INFO] No batch tasks found in {INPUT_FILE}. Using fallback setup.")
        batch_tasks = [(FALLBACK_DATE, FALLBACK_SYMBOLS)]
    else:
        print(f"[INFO] Found {len(batch_tasks)} batch tasks in {INPUT_FILE}.")

    print(f"\n[INFO] Connecting to TWS at {NGROK_HOST}:{NGROK_PORT}...", flush=True)
    tws_app = create_tws_data_app(host=NGROK_HOST, port=NGROK_PORT, client_id=998)
    if not tws_app:
        print("[ERROR] Could not connect to TWS.", flush=True)
        return
    
    try:
        for date_str, symbols in batch_tasks:
            run_backtest_for_task(tws_app, date_str, symbols)
    finally:
        print("\n[INFO] All backtests complete. Disconnecting...", flush=True)
        tws_app.disconnect()

if __name__ == "__main__":
    run()

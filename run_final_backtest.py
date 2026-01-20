
import sys
import os
from datetime import datetime
import time
import json
import ast
from collections import defaultdict

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from backtest_scanner import BacktestAlertScanner
from tws_data_fetcher import create_tws_data_app

# NGROK CONFIGURATION
NGROK_HOST = "6.tcp.ngrok.io"
NGROK_PORT = 16386

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
                    symbols = ast.literal_eval(symbols_str.strip())
                    if isinstance(symbols, list) and symbols:
                        batch_tasks.append((date_str, symbols))
                except Exception as e:
                    print(f"[WARN] Failed to parse symbols for {date_str}: {e}")
    except Exception as e:
        print(f"[ERROR] Error reading input file: {e}")
        
    return batch_tasks

def run_backtest_for_task(tws_app, date_str, symbols):
    """Runs a backtest for a specific date and set of symbols and returns results."""
    print(f"\n" + "╔" + "═"*78 + "╗")
    print(f"║ {'BACKTESTING SESSION':^76} ║")
    print(f"║ DATE: {date_str:<10} | SYMBOLS: {', '.join(symbols):<51} ║")
    print("╚" + "═"*78 + "╝")
    
    scanner = BacktestAlertScanner(symbols=symbols, date=date_str)
    print(f"[INFO] Fetching data for {len(symbols)} symbols...", flush=True)
    
    if not scanner.load_data_from_tws(tws_app, bar_size="10 secs"):
        print(f"[ERROR] Failed to load data for {date_str}.", flush=True)
        return None
    
    print(f"[INFO] Running simulation...", flush=True)
    alerts = scanner.run_backtest()
    
    # Check if any alerts were triggered at all
    total_alerts = sum(len(a) for a in alerts.values())
    if total_alerts == 0:
        print(f"[INFO] No alerts triggered for any symbol on {date_str}.")
        return None

    # Store results for this task
    task_results = {
        'date': date_str,
        'scenarios': []
    }

    for tp, sl in SCENARIOS:
        scanner.current_assets = {s: scanner.initial_asset for s in symbols}
        pl_results = scanner.calculate_pl(tp, sl)
        
        scenario_data = {
            'tp': tp,
            'sl': sl,
            'symbol_stats': []
        }
        
        for symbol in symbols:
            res = pl_results.get(symbol, [])
            wins = len([r for r in res if r['outcome'] == "WIN"])
            losses = len([r for r in res if r['outcome'] == "LOSS"])
            total = wins + losses
            wr = (wins / total * 100) if total > 0 else 0
            final_asset = scanner.current_assets[symbol]
            
            scenario_data['symbol_stats'].append({
                'symbol': symbol,
                'alerts': len(res),
                'wins': wins,
                'losses': losses,
                'win_rate': wr,
                'final_asset': final_asset
            })
        
        task_results['scenarios'].append(scenario_data)
    
    return task_results

def display_aggregated_results(all_results):
    """Displays a clean, aggregated visualization of all backtest results."""
    if not all_results:
        print("\n[INFO] No results to display.")
        return

    print("\n" + "█"*80)
    print(f"█ {'FINAL AGGREGATED PERFORMANCE SUMMARY':^76} █")
    print("█"*80)

    # Group by Scenario to find the best ratio across all days/symbols
    scenario_totals = defaultdict(lambda: {'alerts': 0, 'wins': 0, 'losses': 0, 'profit': 0.0, 'count': 0})

    for task in all_results:
        print(f"\n[ DATE: {task['date']} ]")
        print("┌" + "─"*19 + "┬" + "─"*10 + "┬" + "─"*8 + "┬" + "─"*6 + "┬" + "─"*8 + "┬" + "─"*10 + "┬" + "─"*12 + "┐")
        print(f"│ {'SCENARIO':<17} │ {'SYMBOL':<8} │ {'ALERTS':<6} │ {'W':<4} │ {'L':<6} │ {'WIN %':<8} │ {'FINAL ASSET':<10} │")
        print("├" + "─"*19 + "┼" + "─"*10 + "┼" + "─"*8 + "┼" + "─"*6 + "┼" + "─"*8 + "┼" + "─"*10 + "┼" + "─"*12 + "┤")

        for scenario in task['scenarios']:
            tp, sl = scenario['tp'], scenario['sl']
            s_key = f"TP:{tp:>4.1f}% / SL:{sl:>4.1f}%"
            
            for stats in scenario['symbol_stats']:
                # Only show symbols that actually had alerts to reduce noise
                if stats['alerts'] > 0:
                    print(f"│ {s_key:<17} │ {stats['symbol']:<8} │ {stats['alerts']:<6} │ {stats['wins']:<4} │ {stats['losses']:<6} │ {stats['win_rate']:>6.1f}% │ ${stats['final_asset']:>9.2f} │")
                    
                    # Aggregate for final summary
                    scenario_totals[s_key]['alerts'] += stats['alerts']
                    scenario_totals[s_key]['wins'] += stats['wins']
                    scenario_totals[s_key]['losses'] += stats['losses']
                    scenario_totals[s_key]['profit'] += (stats['final_asset'] - 10000)
                    scenario_totals[s_key]['count'] += 1

        print("└" + "─"*19 + "┴" + "─"*10 + "┴" + "─"*8 + "┴" + "─"*6 + "┴" + "─"*8 + "┴" + "─"*10 + "┴" + "─"*12 + "┘")

    # Final Comparison Table
    print("\n" + "╔" + "═"*78 + "╗")
    print(f"║ {'RANKING BY SCENARIO (TOTAL PROFIT ACROSS ALL SAMPLES)':^76} ║")
    print("╠" + "═"*22 + "╦" + "═"*10 + "╦" + "═"*10 + "╦" + "═"*10 + "╦" + "═"*10 + "╦" + "═"*12 + "╣")
    print(f"║ {'SCENARIO':<20} ║ {'SAMPLES':<8} ║ {'ALERTS':<8} ║ {'WIN %':<8} ║ {'AVG P/L':<8} ║ {'TOT PROFIT':<10} ║")
    print("╠" + "═"*22 + "╬" + "═"*10 + "╬" + "═"*10 + "╬" + "═"*10 + "╬" + "═"*10 + "╬" + "═"*12 + "╣")

    # Sort by total profit
    sorted_scenarios = sorted(scenario_totals.items(), key=lambda x: x[1]['profit'], reverse=True)

    for s_key, data in sorted_scenarios:
        total_alerts = data['alerts']
        wr = (data['wins'] / total_alerts * 100) if total_alerts > 0 else 0
        avg_pl = (data['profit'] / data['count']) if data['count'] > 0 else 0
        print(f"║ {s_key:<20} ║ {data['count']:<8} ║ {total_alerts:<8} ║ {wr:>6.1f}% ║ ${avg_pl:>7.2f} ║ ${data['profit']:>9.2f} ║")

    print("╚" + "═"*22 + "╩" + "═"*10 + "╩" + "═"*10 + "╩" + "═"*10 + "╩" + "═"*10 + "╩" + "═"*12 + "╝")

def run():
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
    
    all_results = []
    try:
        for date_str, symbols in batch_tasks:
            res = run_backtest_for_task(tws_app, date_str, symbols)
            if res:
                all_results.append(res)
    finally:
        tws_app.disconnect()
        print("\n[INFO] Data fetching complete. Generating visualization...", flush=True)
        display_aggregated_results(all_results)

if __name__ == "__main__":
    run()


import sys
import os
from datetime import datetime
import time
import json
import ast
import math
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

def calculate_reliability_score(data):
    """
    Calculates a Reliability Index (0-100) based on:
    1. Sample Size (30%): Penalizes low alert counts.
    2. Profit Factor (40%): Ratio of Gross Profit to Gross Loss.
    3. Consistency (30%): Distribution across different samples (days/symbols).
    """
    alerts = data['alerts']
    wins = data['wins']
    losses = data['losses']
    profit = data['profit']
    samples = data['count']
    
    if alerts == 0: return 0, "N/A"
    
    # 1. Sample Size Score (0-30)
    # Linear scale up to 50 alerts
    size_score = min(30, (alerts / 50) * 30)
    
    # 2. Profit Factor Score (0-40)
    # We estimate gross profit/loss based on TP/SL and win/loss counts
    # Since we don't have exact gross figures here, we use a proxy
    win_rate = wins / alerts
    if win_rate == 1.0: pf = 5.0 # Cap for perfect win rate
    elif win_rate == 0: pf = 0.0
    else:
        # Simplified PF proxy: (WinRate * AvgWin) / (LossRate * AvgLoss)
        # We'll use the ratio of TP/SL from the scenario key if possible, 
        # but for now, we use a general consistency measure
        pf = (wins / max(1, losses))
    
    pf_score = min(40, (pf / 2.0) * 40) if profit > 0 else 0
    
    # 3. Consistency Score (0-30)
    # Measures how many unique day/symbol samples contributed
    consistency_score = min(30, (samples / 10) * 30)
    
    total_score = int(size_score + pf_score + consistency_score)
    
    # Verdict mapping
    if profit <= 0: verdict = "Negative"
    elif total_score > 80: verdict = "High (Robust)"
    elif total_score > 50: verdict = "Moderate"
    else: verdict = "Low (Unstable)"
    
    return total_score, verdict

def run_backtest_for_task(tws_app, date_str, symbols):
    """Runs a backtest for a specific date and set of symbols and returns results."""
    print(f"\n" + "╔" + "═"*78 + "╗")
    print(f"║ {'BACKTESTING SESSION':^76} ║")
    print(f"║ DATE: {date_str:<10} | SYMBOLS: {', '.join(symbols):<51} ║")
    print("╚" + "═"*78 + "╝")
    
    scanner = BacktestAlertScanner(symbols=symbols, date=date_str)
    
    if not scanner.load_data_from_tws(tws_app, bar_size="10 secs"):
        print(f"[ERROR] Failed to load data for {date_str}.", flush=True)
        return None
    
    alerts = scanner.run_backtest()
    
    task_results = {'date': date_str, 'scenarios': []}
    for tp, sl in SCENARIOS:
        scanner.current_assets = {s: scanner.initial_asset for s in symbols}
        # Apply 0.2% slippage simulation (0.1% on entry, 0.1% on exit)
        pl_results = scanner.calculate_pl(tp, sl, slippage_pct=0.2)
        
        scenario_data = {'tp': tp, 'sl': sl, 'symbol_stats': []}
        for symbol in symbols:
            res = pl_results.get(symbol, [])
            wins = len([r for r in res if r['outcome'] == "WIN"])
            losses = len([r for r in res if r['outcome'] == "LOSS"])
            total = wins + losses
            wr = (wins / total * 100) if total > 0 else 0
            final_asset = scanner.current_assets[symbol]
            
            scenario_data['symbol_stats'].append({
                'symbol': symbol, 'alerts': len(res), 'wins': wins, 
                'losses': losses, 'win_rate': wr, 'final_asset': final_asset
            })
        task_results['scenarios'].append(scenario_data)
    
    return task_results

def display_aggregated_results(all_results):
    """Displays a clean, aggregated visualization with Reliability Index."""
    if not all_results:
        print("\n[INFO] No results to display.")
        return

    scenario_totals = defaultdict(lambda: {'alerts': 0, 'wins': 0, 'losses': 0, 'profit': 0.0, 'count': 0})

    for task in all_results:
        print(f"\n[ DATE: {task['date']} ]")
        print("┌" + "─"*22 + "┬" + "─"*10 + "┬" + "─"*8 + "┬" + "─"*6 + "┬" + "─"*6 + "┬" + "─"*10 + "┬" + "─"*14 + "┐")
        print(f"│ {'SCENARIO':<20} │ {'SYMBOL':<8} │ {'ALERTS':<6} │ {'W':<4} │ {'L':<4} │ {'WIN %':<8} │ {'FINAL ASSET':<12} │")
        print("├" + "─"*22 + "┼" + "─"*10 + "┼" + "─"*8 + "┼" + "─"*6 + "┼" + "─"*6 + "┼" + "─"*10 + "┼" + "─"*14 + "┤")

        for scenario in task['scenarios']:
            s_key = f"TP:{scenario['tp']:>4.1f}% / SL:{scenario['sl']:>4.1f}%"
            for stats in scenario['symbol_stats']:
                if stats['alerts'] > 0:
                    print(f"│ {s_key:<20} │ {stats['symbol']:<8} │ {stats['alerts']:<6} │ {stats['wins']:<4} │ {stats['losses']:<4} │ {stats['win_rate']:>6.1f}% │ ${stats['final_asset']:>11.2f} │")
                    
                    # Aggregate for final summary
                    scenario_totals[s_key]['alerts'] += stats['alerts']
                    scenario_totals[s_key]['wins'] += stats['wins']
                    scenario_totals[s_key]['losses'] += stats['losses']
                    scenario_totals[s_key]['profit'] += (stats['final_asset'] - 10000)
                    scenario_totals[s_key]['count'] += 1
        print("└" + "─"*22 + "┴" + "─"*10 + "┴" + "─"*8 + "┴" + "─"*6 + "┴" + "─"*6 + "┴" + "─"*10 + "┴" + "─"*14 + "┘")

    print("\n" + "█"*106)
    print(f"█ {'FINAL AGGREGATED PERFORMANCE & RELIABILITY RANKING':^102} █")
    print("█"*106)

    print("\n" + "╔" + "═"*104 + "╗")
    header = f"║ {'SCENARIO':<22} ║ {'ALERTS':<8} ║ {'WIN %':<8} ║ {'TOT PROFIT':<14} ║ {'RELIABILITY':<14} ║ {'VERDICT':<18} ║"
    print(header)
    print("╠" + "═"*24 + "╬" + "═"*10 + "╬" + "═"*10 + "╬" + "═"*16 + "╬" + "═"*16 + "╬" + "═"*20 + "╣")

    sorted_scenarios = sorted(scenario_totals.items(), key=lambda x: x[1]['profit'], reverse=True)

    for s_key, data in sorted_scenarios:
        total_alerts = data['alerts']
        wr = (data['wins'] / total_alerts * 100) if total_alerts > 0 else 0
        score, verdict = calculate_reliability_score(data)
        
        profit_str = f"${data['profit']:>12.2f}"
        score_str = f"{score:>3}/100"
        
        print(f"║ {s_key:<22} ║ {total_alerts:<8} ║ {wr:>6.1f}% ║ {profit_str:<14} ║ {score_str:<14} ║ {verdict:<18} ║")

    print("╚" + "═"*24 + "╩" + "═"*10 + "╩" + "═"*10 + "╩" + "═"*16 + "╩" + "═"*16 + "╩" + "═"*20 + "╝")
    
    print("\n" + "═"*30)
    print("RELIABILITY INDEX CALCULATION:")
    print("1. Sample Size (30%): Points for total trade volume (target > 50 alerts).")
    print("2. Profit Factor (40%): Points for the ratio of wins vs losses.")
    print("3. Consistency (30%): Points for results spread across multiple days/symbols.")
    print("═"*30 + "\n")

def run():
    batch_tasks = parse_input_file(INPUT_FILE)
    if not batch_tasks:
        batch_tasks = [(FALLBACK_DATE, FALLBACK_SYMBOLS)]

    print(f"\n[INFO] Connecting to TWS at {NGROK_HOST}:{NGROK_PORT}...", flush=True)
    tws_app = create_tws_data_app(host=NGROK_HOST, port=NGROK_PORT, client_id=998)
    if not tws_app: return
    
    all_results = []
    try:
        for date_str, symbols in batch_tasks:
            res = run_backtest_for_task(tws_app, date_str, symbols)
            if res: all_results.append(res)
    finally:
        tws_app.disconnect()
        display_aggregated_results(all_results)

if __name__ == "__main__":
    run()

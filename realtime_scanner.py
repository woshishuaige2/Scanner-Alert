"""
Real-time Broad Screening Tool
Ross Cameron-style preliminary screening based on togglable conditions.
"""
import time
import os
from datetime import datetime, timedelta
from typing import List, Dict, Callable, Optional
from collections import deque
import scanner_config as config
import xml.etree.ElementTree as ET
from conditions import MarketData, AlertConditionSet, PriceAboveVWAPCondition, SqueezeCondition

# STANDALONE SCANNER CONFIGURATION
STANDALONE_SYMBOLS = ["QCLS", "BNAI", "DRCT", "THH", "REVB", "AUST"]

class RealtimeSymbolMonitor:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.price_history = deque(maxlen=600)  # 10 mins of data at 1s intervals
        self.volume_history = deque(maxlen=600)
        self.last_update = None
        self.bid = 0.0
        self.ask = 0.0
        self.vwap = 0.0
        
        # Fundamental data
        self.float_shares = None
        self.avg_daily_volume = None
        self.relative_volume = 0.0
        
        # Screening results
        self.triggered_conditions = []
        
        # Initialize Preliminary Condition Set (Spread + Squeeze only, VWAP disabled)
        self.condition_set = AlertConditionSet("Preliminary")
        # self.condition_set.add_condition(PriceAboveVWAPCondition())  # DISABLED: VWAP calculation issue
        self.condition_set.add_condition(SqueezeCondition(pct_threshold=1.0, minutes=3))

    def update_market_data(self, price: float, volume: float, vwap: float, bid: float = 0, ask: float = 0):
        now = datetime.now()
        self.price_history.append((now, price))
        self.volume_history.append((now, volume))
        self.last_update = now
        self.bid = bid
        self.ask = ask
        self.vwap = vwap
        
        # Calculate Relative Volume if we have avg daily volume
        if self.avg_daily_volume and self.avg_daily_volume > 0:
            self.relative_volume = volume / self.avg_daily_volume

    def check_screening_conditions(self) -> List[str]:
        if not self.price_history:
            return []
            
        # Create MarketData object for the condition set
        data = MarketData(
            symbol=self.symbol,
            price=self.price_history[-1][1],
            volume=self.volume_history[-1][1] if self.volume_history else 0,
            vwap=self.vwap,
            timestamp=self.last_update,
            bid=self.bid,
            ask=self.ask,
            price_history=list(self.price_history)
        )
        
        if self.condition_set.check_all(data):
            self.triggered_conditions = [self.condition_set.get_trigger_summary()]
            return self.triggered_conditions
        else:
            self.triggered_conditions = []
            return []

class RealtimeBroadScanner:
    def __init__(self, symbols: List[str]):
        self.symbols = symbols
        self.monitors = {s: RealtimeSymbolMonitor(s) for s in symbols}
        self.alert_callback = None

    def on_preliminary_alert(self, callback: Callable):
        self.alert_callback = callback

    def update(self, symbol: str, price: float, volume: float, vwap: float, bid: float = 0, ask: float = 0):
        if symbol in self.monitors:
            monitor = self.monitors[symbol]
            monitor.update_market_data(price, volume, vwap, bid, ask)
            
            triggered = monitor.check_screening_conditions()
            if triggered and self.alert_callback:
                self.alert_callback(symbol, datetime.now(), triggered, monitor)

    def load_fundamentals(self, tws_app):
        print("[SCANNER] Loading fundamental data for screening...")
        for symbol, monitor in self.monitors.items():
            xml_data = tws_app.fetch_fundamental_data(symbol)
            if xml_data:
                try:
                    root = ET.fromstring(xml_data)
                    for ratio in root.findall(".//Ratio"):
                        field = ratio.get("FieldName")
                        if field == 'FLOAT':
                            monitor.float_shares = float(ratio.text)
                        elif field == 'VOL10DAVG':
                            monitor.avg_daily_volume = float(ratio.text)
                    print(f"[SCANNER] {symbol} Float: {monitor.float_shares/1e6:.1f}M, Avg Vol: {monitor.avg_daily_volume/1e6:.1f}M")
                except Exception as e:
                    print(f"[SCANNER] Error parsing fundamentals for {symbol}: {e}")

def display_broad_screening(scanner: RealtimeBroadScanner, recent_alerts=None):
    os.system('cls' if os.name == 'nt' else 'clear')
    print("="*120)
    print(f"                ROSS CAMERON STYLE PRELIMINARY SCANNER | {datetime.now().strftime('%H:%M:%S')} ")
    print("="*120)
    print(f"{'SYMBOL':<8} | {'PRICE':<10} | {'VWAP':<10} | {'FLOAT':<12} | {'RVOL':<12} | {'SCREENING ALERTS'}")
    print("-"*120)
    
    for symbol in scanner.symbols:
        m = scanner.monitors[symbol]
        price = f"${m.price_history[-1][1]:.2f}" if m.price_history else "N/A"
        vwap_str = f"${m.vwap:.2f}" if m.vwap > 0 else "N/A"
        float_str = f"{m.float_shares/1e6:.1f}M" if m.float_shares else "N/A"
        rvol = f"{m.relative_volume:.2f}x"
        alerts = ", ".join(m.triggered_conditions) if m.triggered_conditions else "--"
        
        print(f"{symbol:<8} | {price:<10} | {vwap_str:<10} | {float_str:<12} | {rvol:<12} | {alerts}")
    
    # Recent Alerts Section
    print("\n" + "="*120)
    print(" RECENT TRIGGERED ALERTS (Last 5)")
    print("="*120)
    if recent_alerts and len(recent_alerts) > 0:
        for symbol, timestamp, reasons in recent_alerts:
            time_str = timestamp.strftime('%H:%M:%S')
            reasons_str = ', '.join(reasons)
            print(f"  [{time_str}] {symbol} - {reasons_str}")
    else:
        print("  No alerts triggered yet...")
    print("="*120)
    print("[INFO] Preliminary screening active. Waiting for triggers...")

def run_standalone_scanner():
    from tws_data_fetcher import create_tws_data_app
    
    unique_symbols = list(set(STANDALONE_SYMBOLS))
    
    print("[INIT] Connecting to TWS for standalone scanner...")
    tws_app = create_tws_data_app(host="127.0.0.1", port=7497, client_id=999)
    if not tws_app:
        print("[ERROR] Could not connect to TWS. Exiting.")
        return

    scanner = RealtimeBroadScanner(symbols=unique_symbols)
    
    # Recent alerts tracker (symbol -> (timestamp, reasons))
    recent_alerts = deque(maxlen=5)
    
    # Cooldown tracker (symbol -> last_trigger_time)
    alert_cooldown = {}
    
    # Voice Announcement Handler
    def alert_handler(symbol, timestamp, reasons, monitor):
        # 60-second cooldown check
        if symbol in alert_cooldown:
            time_since_last = (timestamp - alert_cooldown[symbol]).total_seconds()
            if time_since_last < 60:
                return  # Skip this alert, still in cooldown
        
        # Update cooldown timestamp
        alert_cooldown[symbol] = timestamp
        
        alert_msg = f"{symbol} at ${monitor.price_history[-1][1]:.2f} - {', '.join(reasons)}"
        print(f"[ALERT] {alert_msg}")
        recent_alerts.appendleft((symbol, timestamp, reasons))
        # Windows text-to-speech - only say symbol name
        if os.name == 'nt':  # Windows
            os.system(f'powershell -c "Add-Type -AssemblyName System.Speech; (New-Object System.Speech.Synthesis.SpeechSynthesizer).Speak(\'{symbol}\')"')
        else:  # Linux/Mac
            os.system(f'say {symbol} 2>/dev/null || espeak "{symbol}" 2>/dev/null')

    scanner.on_preliminary_alert(alert_handler)
    
    # Load Fundamentals
    scanner.load_fundamentals(tws_app)

    # Subscribe to Live Data
    print("[INIT] Subscribing to live market data...")
    def create_callback(sym):
        return lambda s, p, v, vw, ts, b, a: scanner.update(s, price=p, volume=v, vwap=vw, bid=b, ask=a)

    for symbol in unique_symbols:
        tws_app.subscribe_market_data(symbol, create_callback(symbol))
    
    print("[INIT] Starting Standalone Scanner Interface...")
    time.sleep(2)
    
    try:
        while True:
            display_broad_screening(scanner, recent_alerts)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[INFO] Scanner stopped.")
    finally:
        tws_app.disconnect()

if __name__ == "__main__":
    run_standalone_scanner()

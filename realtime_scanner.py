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

class RealtimeSymbolMonitor:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.price_history = deque(maxlen=600)  # 10 mins of data at 1s intervals
        self.volume_history = deque(maxlen=600)
        self.last_update = None
        
        # Fundamental data
        self.float_shares = None
        self.avg_daily_volume = None
        self.relative_volume = 0.0
        
        # Screening results
        self.triggered_conditions = []

    def update_market_data(self, price: float, volume: float, vwap: float):
        now = datetime.now()
        self.price_history.append((now, price))
        self.volume_history.append((now, volume))
        self.last_update = now
        
        # Calculate Relative Volume if we have avg daily volume
        if self.avg_daily_volume and self.avg_daily_volume > 0:
            self.relative_volume = volume / self.avg_daily_volume

    def check_screening_conditions(self) -> List[str]:
        triggered = []
        price = self.price_history[-1][1] if self.price_history else 0
        
        # 1. Low Float - Med Rel Vol
        if config.ENABLE_LOW_FLOAT_MED_RVOL:
            if self.float_shares and self.float_shares <= config.LOW_FLOAT_MAX:
                if self.relative_volume >= config.MED_RVOL_MIN:
                    triggered.append("Low Float - Med Rel Vol")

        # 2. Low Float - High Rel Vol - Price $20+
        if config.ENABLE_LOW_FLOAT_HIGH_RVOL_PRICE_20PLUS:
            if self.float_shares and self.float_shares <= config.LOW_FLOAT_MAX:
                if self.relative_volume >= config.HIGH_RVOL_MIN and price >= 20.0:
                    triggered.append("Low Float - High Rel Vol - Price $20+")

        # 3. Low Float - High Rel Vol
        if config.ENABLE_LOW_FLOAT_HIGH_RVOL:
            if self.float_shares and self.float_shares <= config.LOW_FLOAT_MAX:
                if self.relative_volume >= config.HIGH_RVOL_MIN:
                    triggered.append("Low Float - High Rel Vol")

        # 4. Squeeze Alert - Up 10% in 10min
        if config.ENABLE_SQUEEZE_ALERT_10PCT_10MIN:
            if len(self.price_history) > 60:
                old_price = self.price_history[0][1]
                if price >= old_price * 1.10:
                    triggered.append("Squeeze Alert - Up 10% in 10min")

        self.triggered_conditions = triggered
        return triggered

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
            monitor.update_market_data(price, volume, vwap)
            
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
                        field = ratio.get('FieldName')
                        if field == 'FLOAT':
                            monitor.float_shares = float(ratio.text)
                        elif field == 'VOL10DAVG':
                            monitor.avg_daily_volume = float(ratio.text)
                    print(f"[SCANNER] {symbol} Float: {monitor.float_shares/1e6:.1f}M, Avg Vol: {monitor.avg_daily_volume/1e6:.1f}M")
                except Exception as e:
                    print(f"[SCANNER] Error parsing fundamentals for {symbol}: {e}")

def display_broad_screening(scanner: RealtimeBroadScanner):
    os.system('cls' if os.name == 'nt' else 'clear')
    print("="*90)
    print(f"                ROSS CAMERON STYLE PRELIMINARY SCANNER | {datetime.now().strftime('%H:%M:%S')} ")
    print("="*90)
    print(f"{'SYMBOL':<8} | {'PRICE':<8} | {'FLOAT':<10} | {'RVOL':<6} | {'SCREENING ALERTS'}")
    print("-"*90)
    
    for symbol in scanner.symbols:
        m = scanner.monitors[symbol]
        price = f"${m.price_history[-1][1]:.2f}" if m.price_history else "N/A"
        float_str = f"{m.float_shares/1e6:.1f}M" if m.float_shares else "N/A"
        rvol = f"{m.relative_volume:.2f}x"
        alerts = ", ".join(m.triggered_conditions) if m.triggered_conditions else "--"
        
        print(f"{symbol:<8} | {price:<8} | {float_str:<10} | {rvol:<6} | {alerts}")
    print("="*90)
    print("[INFO] Preliminary screening active. Waiting for triggers...")

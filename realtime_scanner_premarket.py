"""
Real-time Broad Screening Tool - PREMARKET ENHANCED
Ross Cameron-style preliminary screening based on togglable conditions.
Enhanced with market session awareness and premarket-specific features.
"""
import time
import os
from datetime import datetime, timedelta
from typing import List, Dict, Callable, Optional
from collections import deque
import scanner_config as config
import xml.etree.ElementTree as ET
from conditions import MarketData, AlertConditionSet, PriceAboveVWAPCondition, SqueezeCondition
import platform
import pytz

# Market session times (Eastern Time)
PREMARKET_START = (4, 0)   # 4:00 AM ET
MARKET_OPEN = (9, 30)      # 9:30 AM ET
MARKET_CLOSE = (16, 0)     # 4:00 PM ET
AFTERHOURS_END = (20, 0)   # 8:00 PM ET

def get_market_session() -> str:
    """Determine current market session (PREMARKET, REGULAR, AFTERHOURS, CLOSED)"""
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz)
    current_time = (now_et.hour, now_et.minute)
    day_of_week = now_et.weekday()  # 0=Monday, 6=Sunday
    
    # Weekend check
    if day_of_week >= 5:  # Saturday or Sunday
        return "CLOSED"
    
    # Convert time to minutes for easier comparison
    current_minutes = current_time[0] * 60 + current_time[1]
    premarket_start_min = PREMARKET_START[0] * 60 + PREMARKET_START[1]
    market_open_min = MARKET_OPEN[0] * 60 + MARKET_OPEN[1]
    market_close_min = MARKET_CLOSE[0] * 60 + MARKET_CLOSE[1]
    afterhours_end_min = AFTERHOURS_END[0] * 60 + AFTERHOURS_END[1]
    
    if premarket_start_min <= current_minutes < market_open_min:
        return "PREMARKET"
    elif market_open_min <= current_minutes < market_close_min:
        return "REGULAR"
    elif market_close_min <= current_minutes < afterhours_end_min:
        return "AFTERHOURS"
    else:
        return "CLOSED"

def get_session_start_time() -> datetime:
    """Get the start time of the current market session"""
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz)
    session = get_market_session()
    
    if session == "PREMARKET":
        start_time = now_et.replace(hour=PREMARKET_START[0], minute=PREMARKET_START[1], second=0, microsecond=0)
    elif session == "REGULAR":
        start_time = now_et.replace(hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0, microsecond=0)
    elif session == "AFTERHOURS":
        start_time = now_et.replace(hour=MARKET_CLOSE[0], minute=MARKET_CLOSE[1], second=0, microsecond=0)
    else:  # CLOSED
        start_time = now_et
    
    return start_time

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
        
        # Premarket-specific tracking
        self.session_volume = 0.0  # Volume for current session only
        self.last_session = None
        
        # Screening results
        self.triggered_conditions = []
        self.last_alert_time = None  # Track when last alert triggered
        
        # Initialize Preliminary Condition Set with session-aware thresholds
        self.condition_set = AlertConditionSet("Preliminary")
        self.condition_set.add_condition(PriceAboveVWAPCondition())
        self.update_squeeze_condition()

    def update_squeeze_condition(self):
        """Update squeeze condition based on market session"""
        session = get_market_session()
        
        # Clear existing squeeze conditions
        self.condition_set.conditions = [c for c in self.condition_set.conditions 
                                         if not isinstance(c, SqueezeCondition)]
        
        # Add session-appropriate squeeze condition
        if session == "PREMARKET":
            # Premarket: More lenient thresholds due to lower liquidity
            self.condition_set.add_condition(SqueezeCondition(pct_threshold=5.0, minutes=5))
        else:
            # Regular hours: Standard thresholds
            self.condition_set.add_condition(SqueezeCondition(pct_threshold=10.0, minutes=5))

    def update_market_data(self, price: float, volume: float, vwap: float, bid: float = 0, ask: float = 0):
        now = datetime.now()
        current_session = get_market_session()
        
        # Check if we've transitioned to a new session
        if self.last_session != current_session:
            print(f"[SESSION] {self.symbol}: Transitioned from {self.last_session} to {current_session}")
            self.last_session = current_session
            self.session_volume = 0.0
            self.update_squeeze_condition()
            # Clear price history on session transition to avoid cross-session comparisons
            self.price_history.clear()
            self.volume_history.clear()
        
        self.price_history.append((now, price))
        self.volume_history.append((now, volume))
        self.last_update = now
        self.bid = bid
        self.ask = ask
        self.vwap = vwap
        
        # Track session volume
        if len(self.volume_history) >= 2:
            volume_delta = volume - self.volume_history[-2][1]
            if volume_delta > 0:
                self.session_volume += volume_delta
        
        # Calculate Relative Volume based on session
        if self.avg_daily_volume and self.avg_daily_volume > 0:
            if current_session == "PREMARKET":
                # For premarket, compare to typical premarket volume (assume ~5% of daily)
                typical_premarket_vol = self.avg_daily_volume * 0.05
                if typical_premarket_vol > 0:
                    self.relative_volume = self.session_volume / typical_premarket_vol
                else:
                    self.relative_volume = 0.0
            else:
                # Regular hours: use standard daily volume comparison
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
            self.last_alert_time = self.last_update
            return self.triggered_conditions
        else:
            # Keep triggered_conditions to show recent alerts, only clear after timeout
            if self.last_alert_time and (self.last_update - self.last_alert_time) > timedelta(minutes=5):
                self.triggered_conditions = []
            return []

class RealtimeBroadScanner:
    def __init__(self, symbols: List[str]):
        self.symbols = symbols
        self.monitors = {s: RealtimeSymbolMonitor(s) for s in symbols}
        self.alert_callback = None
        self.recent_alerts = deque(maxlen=10)  # Store last 10 alerts
        self.last_vwap_sync_time = None
        self.last_session = None

    def on_preliminary_alert(self, callback: Callable):
        self.alert_callback = callback

    def check_session_transition(self):
        """Check if we need to resync VWAP due to session transition"""
        current_session = get_market_session()
        if self.last_session != current_session:
            print(f"[SCANNER] Market session changed: {self.last_session} -> {current_session}")
            self.last_session = current_session
            return True
        return False

    def update(self, symbol: str, price: float, volume: float, vwap: float, bid: float = 0, ask: float = 0):
        if symbol in self.monitors:
            monitor = self.monitors[symbol]
            monitor.update_market_data(price, volume, vwap, bid, ask)
            
            triggered = monitor.check_screening_conditions()
            if triggered and self.alert_callback:
                alert_time = datetime.now()
                self.recent_alerts.append((alert_time, symbol, triggered))
                self.alert_callback(symbol, alert_time, triggered, monitor)

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

    def resync_vwap_all_symbols(self, tws_app):
        """Resync VWAP for all symbols (called on session transitions)"""
        print("[SCANNER] Resyncing VWAP for all symbols after session transition...")
        for symbol in self.symbols:
            try:
                tws_app.sync_vwap_from_start_of_day(symbol)
            except Exception as e:
                print(f"[SCANNER] Error resyncing VWAP for {symbol}: {e}")
        time.sleep(2)  # Give time for sync to complete

def display_broad_screening(scanner: RealtimeBroadScanner):
    os.system('cls' if os.name == 'nt' else 'clear')
    
    # Get current market session
    session = get_market_session()
    session_color = {
        "PREMARKET": "🌅",
        "REGULAR": "🔔",
        "AFTERHOURS": "🌙",
        "CLOSED": "🚫"
    }
    session_icon = session_color.get(session, "")
    
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz)
    time_str = now_et.strftime('%H:%M:%S ET')
    
    print("="*120)
    print(f"     ROSS CAMERON STYLE PRELIMINARY SCANNER | {time_str} | {session_icon} {session} SESSION")
    print("="*120)
    print(f"{'SYMBOL':<8} | {'PRICE':<10} | {'FLOAT':<12} | {'RVOL':<12} | {'SESSION VOL':<12} | {'SCREENING ALERTS'}")
    print("-"*120)
    
    for symbol in scanner.symbols:
        m = scanner.monitors[symbol]
        price = f"${m.price_history[-1][1]:.2f}" if m.price_history else "N/A"
        float_str = f"{m.float_shares/1e6:.1f}M" if m.float_shares else "N/A"
        rvol = f"{m.relative_volume:.2f}x" if m.relative_volume > 0 else "N/A"
        session_vol = f"{m.session_volume:,.0f}" if m.session_volume > 0 else "0"
        
        # Show alerts with timestamp if recently triggered
        if m.triggered_conditions and m.last_alert_time:
            time_ago = datetime.now() - m.last_alert_time
            mins_ago = int(time_ago.total_seconds() / 60)
            secs_ago = int(time_ago.total_seconds() % 60)
            if mins_ago > 0:
                time_str = f"({mins_ago}m{secs_ago}s ago)"
            else:
                time_str = f"({secs_ago}s ago)"
            alerts = f"{', '.join(m.triggered_conditions)} {time_str}"
        else:
            alerts = "--"
        
        print(f"{symbol:<8} | {price:<10} | {float_str:<12} | {rvol:<12} | {session_vol:<12} | {alerts}")
    
    print("="*120)
    
    # Display recently triggered alerts
    if scanner.recent_alerts:
        print(f"\n🔔 RECENTLY TRIGGERED ALERTS ({session} SESSION):")
        print("-"*120)
        for alert_time, symbol, reasons in list(scanner.recent_alerts)[-5:]:  # Show last 5
            time_str = alert_time.strftime('%H:%M:%S')
            reason_str = ", ".join(reasons)
            print(f"  [{time_str}] {symbol}: {reason_str}")
        print("="*120)
    
    # Session-specific info
    if session == "PREMARKET":
        print("[INFO] PREMARKET MODE: Using 5% squeeze threshold (lower liquidity)")
        print("[INFO] RVOL compared to typical premarket volume (~5% of daily average)")
    elif session == "REGULAR":
        print("[INFO] REGULAR HOURS: Using 10% squeeze threshold")
    elif session == "CLOSED":
        print("[WARNING] Market is CLOSED. Scanner will activate when premarket opens at 4:00 AM ET")
    
    print("[INFO] Preliminary screening active. Waiting for triggers...")

def run_standalone_scanner():
    from tws_data_fetcher import create_tws_data_app
    from top_gainers_fetcher import get_top_gainers
    
    # Get dynamic top gainers list (updates every 10 minutes)
    print("[INIT] Fetching top gainers list...")
    SYMBOLS = get_top_gainers(top_n=20)
    unique_symbols = list(set(SYMBOLS))
    print(f"[INIT] Monitoring {len(unique_symbols)} symbols: {', '.join(unique_symbols[:10])}{'...' if len(unique_symbols) > 10 else ''}")
    
    print("[INIT] Connecting to TWS for standalone scanner...")
    print(f"[INIT] Current market session: {get_market_session()}")
    tws_app = create_tws_data_app(host="127.0.0.1", port=7497, client_id=999)
    if not tws_app:
        print("[ERROR] Could not connect to TWS. Exiting.")
        return

    scanner = RealtimeBroadScanner(symbols=unique_symbols)
    scanner.last_session = get_market_session()
    
    # Voice Announcement Handler
    def alert_handler(symbol, timestamp, reasons, monitor):
        session = get_market_session()
        # We only want the core reason for voice, not the detailed price strings
        voice_reason = "Squeeze detected" if "Squeeze" in str(reasons) else "Momentum alert"
        alert_msg = f"{session} Alert! {symbol} triggered {voice_reason}"
        print(f"[ALERT] {alert_msg}")
        
        # Voice announcement - platform-specific
        try:
            if platform.system() == "Windows":
                # Windows: Use PowerShell with SAPI
                import subprocess
                # Escape quotes for PowerShell
                ps_script = f'Add-Type -AssemblyName System.Speech; (New-Object System.Speech.Synthesis.SpeechSynthesizer).Speak("{symbol} {session} alert")'
                subprocess.run(["powershell", "-Command", ps_script], 
                             capture_output=True, timeout=5)
            else:
                # Linux/Mac: Use espeak
                os.system(f'espeak "{alert_msg}" 2>/dev/null')
        except Exception as e:
            print(f"[WARNING] Voice announcement failed: {e}")

    scanner.on_preliminary_alert(alert_handler)
    
    # Load Fundamentals
    scanner.load_fundamentals(tws_app)

    # Subscribe to Live Data
    print("[INIT] Subscribing to live market data (including extended hours)...")
    def create_callback(sym):
        return lambda s, p, v, vw, ts, b, a: scanner.update(s, price=p, volume=v, vwap=vw, bid=b, ask=a)

    for symbol in unique_symbols:
        tws_app.subscribe_market_data(symbol, create_callback(symbol))
    
    print("[INIT] Starting Standalone Scanner Interface...")
    time.sleep(2)
    
    # Session transition check interval
    last_session_check = datetime.now()
    
    try:
        while True:
            # Check for session transitions every 60 seconds
            if (datetime.now() - last_session_check).total_seconds() > 60:
                if scanner.check_session_transition():
                    scanner.resync_vwap_all_symbols(tws_app)
                last_session_check = datetime.now()
            
            display_broad_screening(scanner)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[INFO] Scanner stopped.")
    finally:
        tws_app.disconnect()

if __name__ == "__main__":
    run_standalone_scanner()

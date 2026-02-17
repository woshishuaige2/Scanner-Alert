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
import xml.etree.ElementTree as ET
from conditions import MarketData, AlertConditionSet, PriceAboveVWAPCondition, SqueezeCondition
import platform
import pytz
from scanner_config import SQUEEZE_PCT_THRESHOLD, SQUEEZE_TIME_MINUTES
from top_gainers_fetcher import get_top_gainers

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
        self.day_start_price = None  # Track first price of the day for total % gain calculation
        self.last_day = None  # Track which day we're on
        
        # Screening results
        self.triggered_conditions = []
        self.last_alert_time = None  # Track when last alert triggered
        
        # Initialize Preliminary Condition Set (same thresholds for all sessions)
        self.condition_set = AlertConditionSet("Preliminary")
        self.condition_set.add_condition(PriceAboveVWAPCondition())
        self.condition_set.add_condition(SqueezeCondition(pct_threshold=SQUEEZE_PCT_THRESHOLD, minutes=SQUEEZE_TIME_MINUTES))

    def update_market_data(self, price: float, volume: float, vwap: float, bid: float = 0, ask: float = 0):
        now = datetime.now()
        current_session = get_market_session()
        
        # Get current date for day tracking
        et_tz = pytz.timezone('US/Eastern')
        now_et = datetime.now(et_tz)
        current_day = now_et.date()
        
        # Check if we've started a new trading day
        if self.last_day != current_day:
            # Only reset if we don't have a historical baseline
            # (historical baseline is more accurate than intraday first price)
            if self.day_start_price is None:
                self.day_start_price = price
            self.last_day = current_day
        
        # Set day start price ONLY if not yet set (will use historical close if available)
        if self.day_start_price is None:
            self.day_start_price = price
        
        # Check if we've transitioned to a new session
        if self.last_session != current_session:
            print(f"[SESSION] {self.symbol}: Transitioned from {self.last_session} to {current_session}")
            self.last_session = current_session
            self.session_volume = 0.0
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
            # Check 60-second cooldown before triggering new alert
            if self.last_alert_time:
                seconds_since_last_alert = (self.last_update - self.last_alert_time).total_seconds()
                if seconds_since_last_alert < 60:
                    # Still in cooldown period, don't trigger new alert
                    return []
            
            # Cooldown passed or first alert - trigger it
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
    
    def load_previous_closes(self, tws_app):
        """Load previous day's closing prices for all symbols"""
        print("[SCANNER] Loading previous day's closing prices...")
        for symbol, monitor in self.monitors.items():
            try:
                # Fetch 2 days of historical data to get yesterday's close
                # bars[-2] = yesterday's completed day, bars[-1] = today's incomplete bar
                bars = tws_app.fetch_historical_bars(
                    symbol=symbol,
                    end_date=datetime.now(),
                    duration="2 D",
                    bar_size="1 day",
                    what_to_show="TRADES"
                )
                if bars and len(bars) >= 2:
                    # Use bars[-2] to get yesterday's completed close (not today's incomplete bar)
                    monitor.day_start_price = bars[-2]['close']
                    print(f"[SCANNER] {symbol} previous close: ${monitor.day_start_price:.2f} (from {len(bars)} bars)")
                elif bars and len(bars) == 1:
                    # Fallback: only one bar available
                    monitor.day_start_price = bars[0]['close']
                    print(f"[SCANNER] {symbol} previous close (fallback): ${monitor.day_start_price:.2f} (1 bar only)")
                else:
                    print(f"[SCANNER] {symbol} could not fetch previous close (no bars received)")
            except Exception as e:
                print(f"[SCANNER] Error fetching previous close for {symbol}: {e}")
    
    def load_historical_prices(self, tws_app):
        """Load recent intraday price history to enable squeeze detection on startup"""
        print("[SCANNER] Loading historical intraday prices (last 1 hour)...")
        for symbol, monitor in self.monitors.items():
            try:
                # Fetch last 1 hour of 1-minute bars
                bars = tws_app.fetch_historical_bars(
                    symbol=symbol,
                    end_date=datetime.now(),
                    duration="1 H",
                    bar_size="1 min",
                    what_to_show="TRADES"
                )
                if bars:
                    # Populate price_history with historical data
                    for bar in bars:
                        bar_time = bar.get('timestamp', datetime.now())
                        bar_close = bar.get('close', 0)
                        if bar_close > 0:
                            monitor.price_history.append((bar_time, bar_close))
                    print(f"[SCANNER] {symbol} loaded {len(bars)} historical price points")
                else:
                    print(f"[SCANNER] {symbol} no historical prices available")
            except Exception as e:
                print(f"[SCANNER] Error loading historical prices for {symbol}: {e}")

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
    
    print("="*126)
    print(f"     ROSS CAMERON STYLE PRELIMINARY SCANNER | {time_str} | {session_icon} {session} SESSION")
    print("="*126)
    print(f"{'SYMBOL':<8} | {'DAY %':<8} | {'PRICE':<10} | {'FLOAT':<12} | {'RVOL':<12} | {'SESSION VOL':<12} | {'SCREENING ALERTS'}")
    print("-"*126)
    
    # Calculate gain % for each symbol and sort by it
    symbol_gains = []
    for symbol in scanner.symbols:
        m = scanner.monitors[symbol]
        if m.price_history and m.day_start_price and m.day_start_price > 0:
            current_price = m.price_history[-1][1]
            gain_pct = ((current_price - m.day_start_price) / m.day_start_price) * 100
        else:
            gain_pct = 0.0
        symbol_gains.append((symbol, gain_pct))
    
    # Sort by gain percentage (descending)
    symbol_gains.sort(key=lambda x: x[1], reverse=True)
    
    for symbol, gain_pct in symbol_gains:
        m = scanner.monitors[symbol]
        gain_str = f"{gain_pct:+.2f}%" if m.price_history and m.day_start_price else "N/A"
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
        
        print(f"{symbol:<8} | {gain_str:<8} | {price:<10} | {float_str:<12} | {rvol:<12} | {session_vol:<12} | {alerts}")
    
    print("="*126)
    
    # Display recently triggered alerts
    if scanner.recent_alerts:
        print(f"\n🔔 RECENTLY TRIGGERED ALERTS ({session} SESSION):")
        print("-"*126)
        for alert_time, symbol, reasons in list(scanner.recent_alerts)[-5:]:  # Show last 5
            time_str = alert_time.strftime('%H:%M:%S')
            reason_str = ", ".join(reasons)
            print(f"  [{time_str}] {symbol}: {reason_str}")
        print("="*126)
    
    # Session-specific info
    if session == "PREMARKET":
        print(f"[INFO] PREMARKET MODE: Squeeze threshold {SQUEEZE_PCT_THRESHOLD}% in {SQUEEZE_TIME_MINUTES} min")
        print("[INFO] RVOL compared to typical premarket volume (~5% of daily average)")
    elif session == "REGULAR":
        print(f"[INFO] REGULAR HOURS: Squeeze threshold {SQUEEZE_PCT_THRESHOLD}% in {SQUEEZE_TIME_MINUTES} min")
    elif session == "CLOSED":
        print("[WARNING] Market is CLOSED. Scanner will activate when premarket opens at 4:00 AM ET")
    
    print("[INFO] Preliminary screening active. Waiting for triggers...")

def update_scanner_symbols(scanner: RealtimeBroadScanner, tws_app, current_symbols: List[str]) -> List[str]:
    """Update the monitored symbol list with new top gainers"""
    # Get updated list
    new_symbols = list(set(get_top_gainers(top_n=20)))
    
    # Find differences
    current_set = set(current_symbols)
    new_set = set(new_symbols)
    
    symbols_to_add = new_set - current_set
    symbols_to_remove = current_set - new_set
    
    if not symbols_to_add and not symbols_to_remove:
        return current_symbols  # No changes
    
    print(f"\n[SYMBOL UPDATE] Adding {len(symbols_to_add)} new, removing {len(symbols_to_remove)} old symbols")
    
    # Remove old symbols
    for symbol in symbols_to_remove:
        if symbol in scanner.monitors:
            tws_app.unsubscribe_realtime_data(symbol)
            del scanner.monitors[symbol]
            scanner.symbols.remove(symbol)
    
    # Add new symbols
    def create_callback(sym):
        return lambda s, p, v, vw, ts, b, a: scanner.update(s, price=p, volume=v, vwap=vw, bid=b, ask=a)
    
    for symbol in symbols_to_add:
        scanner.monitors[symbol] = RealtimeSymbolMonitor(symbol)
        scanner.symbols.append(symbol)
        tws_app.subscribe_market_data(symbol, create_callback(symbol))
    
    # Load fundamentals for new symbols
    if symbols_to_add:
        print(f"[SYMBOL UPDATE] Loading fundamentals for {len(symbols_to_add)} new symbols...")
        for symbol in symbols_to_add:
            monitor = scanner.monitors[symbol]
            
            # Load fundamentals
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
                except Exception:
                    pass
            
            # Load previous close
            try:
                bars = tws_app.fetch_historical_bars(
                    symbol=symbol,
                    end_date=datetime.now(),
                    duration="2 D",
                    bar_size="1 day",
                    what_to_show="TRADES"
                )
                if bars and len(bars) >= 2:
                    # Use bars[-2] to get yesterday's completed close
                    monitor.day_start_price = bars[-2]['close']
                elif bars and len(bars) == 1:
                    # Fallback: only one bar available
                    monitor.day_start_price = bars[0]['close']
            except Exception:
                pass
            
            # Load historical intraday prices
            try:
                bars = tws_app.fetch_historical_bars(
                    symbol=symbol,
                    end_date=datetime.now(),
                    duration="1 H",
                    bar_size="1 min",
                    what_to_show="TRADES"
                )
                if bars:
                    for bar in bars:
                        bar_time = bar.get('timestamp', datetime.now())
                        bar_close = bar.get('close', 0)
                        if bar_close > 0:
                            monitor.price_history.append((bar_time, bar_close))
            except Exception:
                pass
    
    if symbols_to_add or symbols_to_remove:
        print(f"[SYMBOL UPDATE] Now monitoring: {', '.join(new_symbols[:10])}{'...' if len(new_symbols) > 10 else ''}")
    
    return new_symbols

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
    
    # Load Previous Day's Closing Prices
    scanner.load_previous_closes(tws_app)
    
    # Load Historical Intraday Prices (for squeeze detection)
    scanner.load_historical_prices(tws_app)

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
    last_symbol_update = datetime.now()
    symbol_update_interval = 600  # Update every 10 minutes
    
    try:
        while True:
            # Check for session transitions every 60 seconds
            if (datetime.now() - last_session_check).total_seconds() > 60:
                if scanner.check_session_transition():
                    scanner.resync_vwap_all_symbols(tws_app)
                last_session_check = datetime.now()
            
            # Periodic Symbol List Update (every 10 minutes)
            if (datetime.now() - last_symbol_update).total_seconds() >= symbol_update_interval:
                unique_symbols = update_scanner_symbols(scanner, tws_app, unique_symbols)
                last_symbol_update = datetime.now()
            
            display_broad_screening(scanner)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[INFO] Scanner stopped.")
    finally:
        tws_app.disconnect()

if __name__ == "__main__":
    run_standalone_scanner()

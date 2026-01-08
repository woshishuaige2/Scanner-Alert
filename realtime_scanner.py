import threading
import time
from datetime import datetime, timedelta
from collections import deque
import sys
import signal
import pyttsx3

from conditions import (
    AlertCondition,
    AlertConditionSet,
    MarketData,
    PriceAboveVWAPCondition,
    TwoStepMomentumCondition,
    VolumeSpike10sCondition,
    THRESH_1,
    THRESH_2,
    WINDOW_SEC
)

# Import TWS integration - REQUIRED
try:
    from tws_data_fetcher import create_tws_data_app, TWSDataApp
except ImportError:
    print("[Error] tws_data_fetcher.py not found. Please ensure it is in the same directory.")
    sys.exit(1)


class RealtimeSymbolMonitor:
    """Monitors a single symbol in real-time and checks conditions"""
    
    def __init__(self, symbol: str, condition_set: AlertConditionSet, max_history_size: int = 1000):
        self.symbol = symbol
        self.condition_set = condition_set
        
        # Data tracking
        self.price_history = deque(maxlen=max_history_size)
        self.volume_history = deque(maxlen=max_history_size)
        
        # Cumulative VWAP tracking (like Webull)
        self.cumulative_pv = 0.0  # Sum of (price * volume)
        self.cumulative_volume = 0.0  # Sum of volume
        
        self.last_price = None
        self.last_volume = None  # Cumulative daily volume
        self.last_vwap = None  # Cumulative day VWAP
        self.last_bar_volume = None  # Last bar's incremental volume
        self.last_bid = 0.0
        self.last_ask = 0.0
        self.last_update = None
        self.lock = threading.Lock()
        
        # Alert tracking
        self.last_alert_time = None
    
    def update_market_data(self, price: float, volume: int, vwap: float = None, bid: float = None, ask: float = None):
        """Update market data for this symbol"""
        with self.lock:
            timestamp = datetime.now()
            
            # Convert to float (TWS returns Decimal types)
            price = float(price)
            volume = float(volume)
            if bid is not None: self.last_bid = float(bid)
            if ask is not None: self.last_ask = float(ask)
            
            # For tick-by-tick updates, volume is cumulative daily volume
            # Calculate incremental volume for this update
            if self.last_volume is not None:
                volume_increment = volume - self.last_volume
            else:
                volume_increment = volume
            
            # Update cumulative VWAP (like Webull)
            if volume_increment > 0:
                self.cumulative_pv += price * volume_increment
                self.cumulative_volume += volume_increment
                if self.cumulative_volume > 0:
                    self.last_vwap = self.cumulative_pv / self.cumulative_volume
            
            self.price_history.append((timestamp, price))
            self.volume_history.append((timestamp, volume_increment))
            
            self.last_price = price
            self.last_volume = volume  # Store cumulative volume
            self.last_bar_volume = volume_increment  # Store this bar's volume
            self.last_update = timestamp
    
    def check_conditions(self) -> Optional[MarketData]:
        """Check if all conditions are met for this symbol"""
        with self.lock:
            if self.last_price is None:
                return None
            
            # Convert history deques to dicts
            price_dict = {ts: price for ts, price in self.price_history}
            volume_dict = {ts: vol for ts, vol in self.volume_history}
            
            md = MarketData(
                symbol=self.symbol,
                price=self.last_price,
                volume=self.last_bar_volume,
                vwap=self.last_vwap,
                timestamp=self.last_update,
                bid=self.last_bid,
                ask=self.last_ask,
                price_history=price_dict,
                volume_history=volume_dict
            )
            
            if self.condition_set.check_all(md):
                return md
            return None

    def get_volume_spike_ratio(self) -> float:
        """Calculate volume spike ratio (current 10s vs avg of past 20 bars)"""
        with self.lock:
            if not self.volume_history or len(self.volume_history) < 50:
                return 0.0
            
            now = datetime.now()
            
            # Get volume in last 10 seconds (current window)
            current_10s_vol = sum(
                vol for ts, vol in self.volume_history
                if (now - ts).total_seconds() <= 10
            )
            
            # Get volume in previous 200 seconds (20 x 10s bars)
            past_200s_vol = sum(
                vol for ts, vol in self.volume_history
                if 10 < (now - ts).total_seconds() <= 210
            )
            
            past_20_avg = past_200s_vol / 20
            
            if past_20_avg > 0:
                return current_10s_vol / past_20_avg
            return 0.0


class RealtimeAlertScanner:
    """Main scanner class that manages multiple symbol monitors"""
    
    def __init__(self, symbols: list[str]):
        self.symbols = symbols
        self.monitors: Dict[str, RealtimeSymbolMonitor] = {}
        self.alert_callbacks = []
        self.alert_cooldown = timedelta(seconds=30)
        self.last_alert_time = {s: None for s in symbols}
        
        # Initialize monitors with default conditions
        self._initialize_monitors()
    
    def _initialize_monitors(self):
        """Initialize monitors with default condition set"""
        for symbol in self.symbols:
            # Create default condition set
            condition_set = AlertConditionSet(f"{symbol}_default")
            condition_set.add_condition(PriceAboveVWAPCondition())
            condition_set.add_condition(TwoStepMomentumCondition(t1=THRESH_1, t2=THRESH_2, window=WINDOW_SEC))  
            condition_set.add_condition(VolumeSpike10sCondition())  
            
            self.monitors[symbol] = RealtimeSymbolMonitor(symbol, condition_set)
    
    def load_today_historical_bars(self, tws_app, bar_size: str = "5 mins"):
        """
        Load today's historical intraday bars for all symbols to establish baseline.
        This helps with VWAP and volume average calculations.
        """
        print(f"\n[INFO] Loading today's historical data for {len(self.symbols)} symbols...")
        
        for symbol in self.symbols:
            # Fetch bars from today's open
            bars = tws_app.fetch_historical_bars(symbol, duration="1 D", bar_size=bar_size)
            
            if bars:
                print(f"  - {symbol}: Loaded {len(bars)} bars")
                monitor = self.monitors[symbol]
                
                # Update monitor with historical data
                # We use the 'average' price for VWAP baseline
                for bar in bars:
                    monitor.update_market_data(
                        price=bar['close'],
                        volume=bar['volume'],
                        vwap=bar['average']
                    )
            else:
                print(f"  - {symbol}: No historical data found")

    def on_alert(self, callback):
        """Register a callback for alerts"""
        self.alert_callbacks.append(callback)
    
    def start(self, tws_app):
        """Start the real-time scanning loop"""
        print(f"\n[INFO] Starting real-time scan for: {', '.join(self.symbols)}")
        
        # 1. Subscribe to real-time market data
        for symbol in self.symbols:
            tws_app.subscribe_market_data(symbol, self._market_data_callback)
        
        # 2. Main monitoring loop
        try:
            while True:
                self._check_all_monitors()
                time.sleep(1)  # Check every second
        except KeyboardInterrupt:
            print("\n[INFO] Stopping scanner...")
        finally:
            for symbol in self.symbols:
                tws_app.unsubscribe_market_data(symbol)

    def _market_data_callback(self, symbol, price, volume, vwap, timestamp):
        """Callback for TWS market data updates"""
        if symbol in self.monitors:
            # In a real scenario, we would also get bid/ask from TWS
            # For now, we pass None for bid/ask as they are not yet implemented in tws_data_fetcher
            self.monitors[symbol].update_market_data(price, volume, vwap)

    def _check_all_monitors(self):
        """Check conditions for all monitored symbols"""
        for symbol, monitor in self.monitors.items():
            md = monitor.check_conditions()
            
            if md:
                now = datetime.now()
                last_alert = self.last_alert_time[symbol]
                
                if last_alert is None or (now - last_alert) >= self.alert_cooldown:
                    self.last_alert_time[symbol] = now
                    reasons = monitor.condition_set.get_trigger_summary()
                    
                    # Trigger all callbacks
                    for callback in self.alert_callbacks:
                        callback(symbol, md.timestamp, reasons, md)


def display_status_table(scanner, alerts_list=None):
    """Display a real-time status table of all monitored symbols"""
    import os
    os.system('cls' if os.name == 'nt' else 'clear')
    
    print("="*105)
    print(f"{'STOCK REAL-TIME SCANNER':^105}")
    print("="*105)
    print(f"{'SYMBOL':<8} | {'PRICE':<10} | {'VWAP':<10} | {'VOL SPIKE':<10} | {'LAST UPDATE':<20} | {'STATUS'}")
    print("-" * 105)
    
    for symbol in scanner.symbols:
        monitor = scanner.monitors[symbol]
        with monitor.lock:
            try:
                price = f"${monitor.last_price:.2f}" if monitor.last_price else "WAITING..."
                vwap = f"${monitor.last_vwap:.2f}" if monitor.last_vwap else "WAITING..."
                spike = f"{monitor.get_volume_spike_ratio():.1fx}"
                update = monitor.last_update.strftime("%H:%M:%S") if monitor.last_update else "N/A"
                
                # Simple status indicator
                status = "OK"
                if monitor.last_price and monitor.last_vwap:
                    if monitor.last_price > monitor.last_vwap:
                        status = "🟢 ABOVE VWAP"
                    else:
                        status = "🔴 BELOW VWAP"
                
                print(f"{symbol:<8} | {price:<10} | {vwap:<10} | {spike:<10} | {update:<20} | {status}")
            except Exception as e:
                print(f"{symbol:<8} ERROR: {str(e)[:50]}")
    
    print("-"*105)
    
    # Show last 7 alerts if present
    if alerts_list and len(alerts_list) > 0:
        print("\n" + "!"*105)
        print(f"🚨 RECENT ALERTS (Last {len(alerts_list)} triggers, most recent first) 🚨")
        print("!"*105)
        for alert in list(alerts_list):
            print(alert)
            print("-"*105)
        print("!"*105)
    print("\n[INFO] Table updates every 5 seconds | Press Ctrl+C to stop\n")


# Example usage
if __name__ == "__main__":
    # Configuration
    NGROK_HOST = "6.tcp.ngrok.io"
    NGROK_PORT = 13103
    
    print("Enter symbols to monitor (comma separated, e.g. AAPL,TSLA,NVDA):")
    input_str = sys.stdin.readline().strip()
    if not input_str:
        symbols = ["AAPL", "TSLA", "NVDA"]
    else:
        symbols = [s.strip().upper() for s in input_str.split(",")]
    
    # Create TWS App
    tws_app = create_tws_data_app(host=NGROK_HOST, port=NGROK_PORT, client_id=10)
    
    # Create scanner
    scanner = RealtimeAlertScanner(symbols=symbols)
    
    # Track last alert for display
    last_alert_info = {'message': None, 'triggered': False}
    
    from collections import deque
    last_alerts = deque(maxlen=7)  # Stores alert messages (increased to 7)
    last_alert_triggered = False

    tts_engine = pyttsx3.init()

    def alert_handler(symbol, timestamp, reasons, data):
        # 1. Voice announce the symbol name
        try:
            tts_engine.say(symbol)
            tts_engine.runAndWait()
        except Exception as e:
            print(f"[Voice Error] {e}")
        # 2. Display in console (handled by table update)
        # 3. Add to last_alerts
        alert_msg = (
            f"Symbol: {symbol}\n"
            f"Time: {timestamp}\n"
            f"Price: ${data.price:.2f} | Volume: {data.volume:,} | VWAP: ${data.vwap:.2f}\n"
            f"Conditions: {reasons}"
        )
        last_alerts.appendleft(alert_msg)
        global last_alert_triggered
        last_alert_triggered = True

    scanner.on_alert(alert_handler)
    
    # Load baseline data
    scanner.load_today_historical_bars(tws_app)
    
    # Start scanning in a separate thread
    scan_thread = threading.Thread(target=scanner.start, args=(tws_app,), daemon=True)
    scan_thread.start()
    
    # Main display loop
    try:
        while True:
            display_status_table(scanner, last_alerts)
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n[INFO] Exiting...")

"""
TWS Data Fetcher for Alert Scanner
Fetches both historical and real-time data from IBKR TWS API.
"""
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import TickerId, TickAttrib, BarData
from ibapi.ticktype import TickTypeEnum
from datetime import datetime, timedelta
from typing import List, Dict, Callable, Optional
from collections import deque
import threading
import time


def tick_type_str(tickType):
    """Return a human-friendly string for tickType across ibapi versions."""
    try:
        if hasattr(TickTypeEnum, 'toStr'):
            return TickTypeEnum.toStr(tickType)
        if hasattr(TickTypeEnum, 'to_str'):
            return TickTypeEnum.to_str(tickType)
        if hasattr(tickType, 'name'):
            return tickType.name
        try:
            return TickTypeEnum(tickType).name
        except Exception:
            return str(tickType)
    except Exception:
        return str(tickType)


class TWSDataApp(EClient, EWrapper):
    """
    TWS Application for fetching historical and real-time market data.
    Enhanced for alert scanner with VWAP calculation.
    """
    
    def __init__(self):
        EClient.__init__(self, self)
        self.next_order_id = None
        self.req_id_counter = 2000
        self.connected = False
        self.lock = threading.Lock()
        
        # Historical data storage
        self.historical_data = {}  # reqId -> list of bars
        self.historical_complete = {}  # reqId -> bool
        
        # Real-time data storage
        self.realtime_callbacks = {}  # reqId -> (symbol, callback)
        self.realtime_data = {}  # symbol -> {price, bid, ask, last_size, bid_size, ask_size, volume, vwap}
        self.contracts = {}  # symbol -> Contract
        
    def nextValidId(self, orderId: int):
        """Called when connection is established"""
        self.next_order_id = orderId
        self.connected = True
        print(f"[TWS] Connected. Next valid order ID: {orderId}")
        
    def error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson="", *args):
        """Error handler - accepts variable arguments for compatibility across ibapi versions"""
        # Suppress common info/warning messages that don't affect functionality
        suppressed_codes = [
            2104, 2106, 2107, 2119, 2158,  # Market data farm connection messages
            2106,  # HMDS data farm connection
            2158,  # Sec-def data farm connection
        ]
        if errorCode in suppressed_codes:
            return
        if errorCode == 10167:  # Displaying delayed market data
            print(f"[TWS] Using delayed market data (live subscription may be needed)")
            return
        # Only show actual errors (code >= 500) or important warnings
        if errorCode >= 500 or errorCode in [1100, 1101, 1102, 1300]:
            print(f"[TWS Error] ReqId: {reqId}, Code: {errorCode}, Msg: {errorString}")
        
    def historicalData(self, reqId: int, bar: BarData):
        """Receive historical bar data"""
        with self.lock:
            if reqId not in self.historical_data:
                self.historical_data[reqId] = []
            
            # Get VWAP - attribute name varies by ibapi version
            vwap = 0.0
            if hasattr(bar, 'average'):
                vwap = bar.average
            elif hasattr(bar, 'wap'):
                vwap = bar.wap
            else:
                # Fallback: calculate simple average of high and low
                vwap = (bar.high + bar.low) / 2.0
            
            self.historical_data[reqId].append({
                'date': bar.date,
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume,
                'average': vwap,  # VWAP
                'barCount': bar.barCount
            })
    
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        """Called when historical data is complete"""
        with self.lock:
            self.historical_complete[reqId] = True
        print(f"[TWS] Historical data complete for reqId {reqId} ({start} to {end})")
    
    def tickPrice(self, reqId: TickerId, tickType: int, price: float, attrib: TickAttrib):
        """Handle price ticks"""
        with self.lock:
            if reqId not in self.realtime_callbacks:
                return
            symbol, callback = self.realtime_callbacks[reqId]
            
            if symbol not in self.realtime_data:
                self.realtime_data[symbol] = {
                    'price': 0.0, 'bid': 0.0, 'ask': 0.0,
                    'last_size': 0, 'bid_size': 0, 'ask_size': 0,
                    'volume': 0, 'vwap': 0.0
                }
            
            tt = tick_type_str(tickType)
            if tt == 'LAST':
                self.realtime_data[symbol]['price'] = price
            elif tt == 'BID':
                self.realtime_data[symbol]['bid'] = price
            elif tt == 'ASK':
                self.realtime_data[symbol]['ask'] = price
    
    def tickSize(self, reqId: TickerId, tickType: int, size: int):
        """Handle size ticks"""
        with self.lock:
            if reqId not in self.realtime_callbacks:
                return
            symbol, callback = self.realtime_callbacks[reqId]
            
            if symbol not in self.realtime_data:
                self.realtime_data[symbol] = {
                    'price': 0.0, 'bid': 0.0, 'ask': 0.0,
                    'last_size': 0, 'bid_size': 0, 'ask_size': 0,
                    'volume': 0, 'vwap': 0.0
                }
        
        tt = tick_type_str(tickType)
        
        if tt == 'LAST_SIZE':
            with self.lock:
                self.realtime_data[symbol]['last_size'] = size
        elif tt == 'BID_SIZE':
            with self.lock:
                self.realtime_data[symbol]['bid_size'] = size
        elif tt == 'ASK_SIZE':
            with self.lock:
                self.realtime_data[symbol]['ask_size'] = size
        elif tt == 'VOLUME':
            with self.lock:
                old_volume = self.realtime_data[symbol]['volume']
                self.realtime_data[symbol]['volume'] = size
                
                # Trigger callback when we have price and volume update
                price = self.realtime_data[symbol]['price']
                if price > 0:
                    # Initialize cumulative tracking if not present
                    if 'cumulative_pv' not in self.realtime_data[symbol]:
                        self.realtime_data[symbol]['cumulative_pv'] = 0.0
                        self.realtime_data[symbol]['cumulative_volume'] = 0.0
                    
                    # Calculate incremental volume
                    current_daily_volume = size
                    last_daily_volume = self.realtime_data[symbol].get('last_daily_volume', 0)
                    volume_increment = current_daily_volume - last_daily_volume
                    
                    if volume_increment > 0:
                        self.realtime_data[symbol]['cumulative_pv'] += price * volume_increment
                        self.realtime_data[symbol]['cumulative_volume'] += volume_increment
                        self.realtime_data[symbol]['last_daily_volume'] = current_daily_volume
                    
                    # Calculate accurate cumulative VWAP
                    if self.realtime_data[symbol]['cumulative_volume'] > 0:
                        vwap = self.realtime_data[symbol]['cumulative_pv'] / self.realtime_data[symbol]['cumulative_volume']
                    else:
                        vwap = price
                    
                    self.realtime_data[symbol]['vwap'] = vwap
                    
                    # Call callback with updated data
                    bid = self.realtime_data[symbol].get('bid', 0.0)
                    ask = self.realtime_data[symbol].get('ask', 0.0)
                    callback(symbol, price, current_daily_volume, vwap, datetime.now(), bid, ask)
    
    def get_next_req_id(self):
        """Get next request ID"""
        with self.lock:
            req_id = self.req_id_counter
            self.req_id_counter += 1
        return req_id
    
    def fetch_historical_bars(
        self,
        symbol: str,
        end_date: datetime,
        duration: str = "1 D",
        bar_size: str = "10 secs",
        what_to_show: str = "TRADES"
    ) -> List[Dict]:
        """Fetch historical bar data from TWS."""
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        
        req_id = self.get_next_req_id()
        
        with self.lock:
            self.historical_data[req_id] = []
            self.historical_complete[req_id] = False
        
        end_date_str = end_date.strftime("%Y%m%d %H:%M:%S") + " US/Eastern"
        
        self.reqHistoricalData(
            reqId=req_id,
            contract=contract,
            endDateTime=end_date_str,
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow=what_to_show,
            useRTH=1,
            formatDate=1,
            keepUpToDate=False,
            chartOptions=[]
        )
        
        timeout = 30.0
        waited = 0.0
        while waited < timeout:
            with self.lock:
                if self.historical_complete.get(req_id, False):
                    return self.historical_data[req_id]
            time.sleep(0.1)
            waited += 0.1
        return []

    def subscribe_market_data(self, symbol: str, callback: Callable):
        """Subscribe to real-time market data"""
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        
        req_id = self.get_next_req_id()
        with self.lock:
            self.realtime_callbacks[req_id] = (symbol, callback)
            self.contracts[symbol] = contract
            
        self.reqMktData(req_id, contract, "", False, False, [])
        print(f"[TWS] Subscribed to {symbol} (reqId: {req_id})")

    def unsubscribe_market_data(self, symbol: str):
        """Unsubscribe from real-time market data"""
        with self.lock:
            for req_id, (s, _) in list(self.realtime_callbacks.items()):
                if s == symbol:
                    self.cancelMktData(req_id)
                    del self.realtime_callbacks[req_id]
                    print(f"[TWS] Unsubscribed from {symbol}")


def create_tws_data_app(host: str, port: int, client_id: int) -> TWSDataApp:
    """Create and connect TWS application"""
    app = TWSDataApp()
    app.connect(host, port, client_id)
    
    # Start app thread
    thread = threading.Thread(target=app.run, daemon=True)
    thread.start()
    
    # Wait for connection
    timeout = 5.0
    waited = 0.0
    while not app.connected and waited < timeout:
        time.sleep(0.1)
        waited += 0.1
        
    if not app.connected:
        print(f"[Error] Failed to connect to TWS at {host}:{port}")
        return None
        
    return app

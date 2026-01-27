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
        
        # Order tracking
        self.order_status_callbacks = [] # List of callbacks for order updates
        
        # Fundamental data storage
        self.fundamental_data = {} # symbol -> XML string
        self.fundamental_events = {} # symbol -> threading.Event
        
    def nextValidId(self, orderId: int):
        """Called when connection is established"""
        self.next_order_id = orderId
        self.connected = True
        print(f"[TWS] Connected. Next valid order ID: {orderId}")

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        """Handle order status updates"""
        for callback in self.order_status_callbacks:
            callback(orderId, status, filled, remaining, avgFillPrice, parentId)

    def openOrder(self, orderId, contract, order, orderState):
        """Handle open order updates"""
        pass

    def execDetails(self, reqId, contract, execution):
        """Handle execution details"""
        pass

    def fundamentalData(self, reqId: int, data: str):
        """Receive fundamental data XML"""
        with self.lock:
            self.fundamental_data[reqId] = data
            if reqId in self.fundamental_events:
                self.fundamental_events[reqId].set()
        
    def error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson="", *args):
        """Error handler - accepts variable arguments for compatibility across ibapi versions"""
        
        # Log all errors and warnings to a file for debugging
        with open("tws_errors.log", "a") as f:
            f.write(f"{datetime.now()}: ReqId={reqId}, Code={errorCode}, Msg={errorString}\n")

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
        if errorCode >= 500 or errorCode in [1100, 1101, 1102, 1300, 201]:
            # Try to find the symbol associated with this reqId
            symbol_info = ""
            with self.lock:
                if reqId in self.realtime_callbacks:
                    symbol_info = f" ({self.realtime_callbacks[reqId][0]})"
            print(f"[TWS Error] ReqId: {reqId}{symbol_info}, Code: {errorCode}, Msg: {errorString}")
        
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
        print(f"[TWS] Historical data complete for reqId {reqId}")
    
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
                    'volume': 0, 'vwap': 0.0, 'syncing': False
                }
            
        tt = tick_type_str(tickType)
        
        if tt == 'LAST':
            with self.lock:
                self.realtime_data[symbol]['price'] = price
        elif tt == 'BID':
            with self.lock:
                self.realtime_data[symbol]['bid'] = price
        elif tt == 'ASK':
            with self.lock:
                self.realtime_data[symbol]['ask'] = price
        elif tt == 'RT_VWAP':
            # This is the real-time VWAP provided directly by TWS
            with self.lock:
                self.realtime_data[symbol]['vwap'] = price
    
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
                    'volume': 0, 'vwap': 0.0, 'syncing': False
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
                self.realtime_data[symbol]['volume'] = size
                
                # Trigger callback when we have price and volume update
                price = self.realtime_data[symbol]['price']
                if price > 0:
                    # If TWS hasn't provided RT_VWAP yet, calculate our own as fallback
                    if self.realtime_data[symbol]['vwap'] == 0:
                        # Initialize cumulative tracking if not present
                        if 'cumulative_pv' not in self.realtime_data[symbol]:
                            self.realtime_data[symbol]['cumulative_pv'] = 0.0
                            self.realtime_data[symbol]['cumulative_volume'] = 0.0
                        
                        current_daily_volume = size
                        last_daily_volume = self.realtime_data[symbol].get('last_daily_volume', 0)
                        volume_increment = current_daily_volume - last_daily_volume
                        
                        if volume_increment > 0:
                            self.realtime_data[symbol]['cumulative_pv'] += price * volume_increment
                            self.realtime_data[symbol]['cumulative_volume'] += volume_increment
                            self.realtime_data[symbol]['last_daily_volume'] = current_daily_volume
                        
                        if self.realtime_data[symbol]['cumulative_volume'] > 0:
                            self.realtime_data[symbol]['vwap'] = self.realtime_data[symbol]['cumulative_pv'] / self.realtime_data[symbol]['cumulative_volume']
                    
                    vwap = self.realtime_data[symbol]['vwap']
                    bid = self.realtime_data[symbol].get('bid', 0.0)
                    ask = self.realtime_data[symbol].get('ask', 0.0)
                    callback(symbol, price, size, vwap, datetime.now(), bid, ask)
    
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
        bar_size: str = "1 min",
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
            useRTH=0, # Use 0 to include pre-market for accurate VWAP
            formatDate=1,
            keepUpToDate=False,
            chartOptions=[]
        )
        
        timeout = 30.0
        waited = 0.0
        while waited < timeout:
            with self.lock:
                if self.historical_complete.get(req_id, False):
                    break
            time.sleep(0.1)
            waited += 0.1
        
        with self.lock:
            bars = self.historical_data.get(req_id, [])
            if req_id in self.historical_data: del self.historical_data[req_id]
            if req_id in self.historical_complete: del self.historical_complete[req_id]
        return bars

    def sync_vwap_from_start_of_day(self, symbol: str):
        """
        Synchronize VWAP by fetching all intraday bars since pre-market start.
        This ensures our calculated VWAP matches charts like Webull.
        """
        print(f"[TWS] Synchronizing historical VWAP for {symbol}...")
        with self.lock:
            if symbol not in self.realtime_data:
                self.realtime_data[symbol] = {'price': 0.0, 'vwap': 0.0, 'syncing': True}
            else:
                self.realtime_data[symbol]['syncing'] = True

        # Fetch 1-minute bars for the current day
        # We use a 1-day duration which will give us all bars for the current session including pre-market
        bars = self.fetch_historical_bars(symbol, datetime.now(), duration="1 D", bar_size="1 min")
        
        if not bars:
            print(f"[TWS] No historical bars found for {symbol}. VWAP will start from current price.")
            with self.lock:
                self.realtime_data[symbol]['syncing'] = False
            return

        total_pv = 0.0
        total_volume = 0.0
        
        for bar in bars:
            # bar['average'] is the WAP (Weighted Average Price) for that bar
            # bar['volume'] is the volume for that bar
            total_pv += bar['average'] * bar['volume']
            total_volume += bar['volume']
            
        if total_volume > 0:
            vwap = total_pv / total_volume
            with self.lock:
                self.realtime_data[symbol]['vwap'] = vwap
                self.realtime_data[symbol]['cumulative_pv'] = total_pv
                self.realtime_data[symbol]['cumulative_volume'] = total_volume
                self.realtime_data[symbol]['last_daily_volume'] = total_volume # Approximation
                self.realtime_data[symbol]['syncing'] = False
            print(f"[TWS] {symbol} synced. Historical VWAP: ${vwap:.2f} (Volume: {total_volume:,.0f})")
        else:
            with self.lock:
                self.realtime_data[symbol]['syncing'] = False

    def subscribe_market_data(self, symbol: str, callback: Callable):
        """Subscribe to real-time market data."""
        # First, sync historical VWAP in a separate thread to not block
        threading.Thread(target=self.sync_vwap_from_start_of_day, args=(symbol,), daemon=True).start()

        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        
        with self.lock:
            self.contracts[symbol] = contract
        
        req_id = self.get_next_req_id()
        with self.lock:
            self.realtime_callbacks[req_id] = (symbol, callback)
            if symbol not in self.realtime_data:
                self.realtime_data[symbol] = {
                    'price': 0.0, 'bid': 0.0, 'ask': 0.0,
                    'last_size': 0, 'bid_size': 0, 'ask_size': 0,
                    'volume': 0, 'vwap': 0.0, 'syncing': True
                }
        
        self.reqMarketDataType(1)
        # genericTickList 233 is for RT_VWAP
        self.reqMktData(
            reqId=req_id,
            contract=contract,
            genericTickList="233",
            snapshot=False,
            regulatorySnapshot=False,
            mktDataOptions=[]
        )
        print(f"[TWS] Subscribed to real-time data for {symbol} (reqId: {req_id})")
    
    def unsubscribe_realtime_data(self, symbol: str):
        """Unsubscribe from real-time market data"""
        req_id_to_cancel = None
        with self.lock:
            for req_id, (sym, _) in self.realtime_callbacks.items():
                if sym == symbol:
                    req_id_to_cancel = req_id
                    break
        
        if req_id_to_cancel:
            self.cancelMktData(req_id_to_cancel)
            with self.lock:
                if req_id_to_cancel in self.realtime_callbacks: del self.realtime_callbacks[req_id_to_cancel]
                if symbol in self.realtime_data: del self.realtime_data[symbol]
            print(f"[TWS] Unsubscribed from {symbol}")

    def fetch_fundamental_data(self, symbol: str, report_type: str = "ReportSnapshot") -> Optional[str]:
        """Fetch fundamental data XML for a symbol"""
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        
        req_id = self.get_next_req_id()
        event = threading.Event()
        with self.lock:
            self.fundamental_events[req_id] = event
            
        self.reqFundamentalData(req_id, contract, report_type, [])
        if event.wait(timeout=10.0):
            with self.lock:
                data = self.fundamental_data.get(req_id)
                del self.fundamental_data[req_id]
                del self.fundamental_events[req_id]
                return data
        else:
            with self.lock:
                if req_id in self.fundamental_events: del self.fundamental_events[req_id]
            return None


def create_tws_data_app(host="127.0.0.1", port=7497, client_id=0) -> Optional[TWSDataApp]:
    """Create and connect a TWS data application."""
    app = TWSDataApp()
    try:
        app.connect(host, port, client_id)
    except Exception as e:
        print(f"[TWS] Connection error: {e}")
        return None
    
    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()
    
    timeout = 10.0
    waited = 0.0
    while not app.connected and waited < timeout:
        time.sleep(0.1)
        waited += 0.1
    
    if not app.connected:
        return None
    return app

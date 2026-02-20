"""
IBKR Native Scanner for Top Gainers
Uses TWS API's built-in market scanner to get top gainers directly from IBKR.
Supports both regular hours and premarket scanning.
"""
import time
import threading
from typing import List, Optional
from datetime import datetime
import pytz
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.scanner import ScannerSubscription
from ibapi.contract import ContractDetails

class IBKRScannerApp(EWrapper, EClient):
    """IBKR Scanner using native TWS scanner API"""
    
    def __init__(self):
        EClient.__init__(self, self)
        EWrapper.__init__(self)
        
        self.scanner_results = []
        self.scanner_complete = False
        self.connected = False
        self.next_valid_id = None
        self.lock = threading.Lock()
        
    def nextValidId(self, orderId: int):
        """Called when connection is established"""
        self.next_valid_id = orderId
        self.connected = True
        print(f"[IBKR SCANNER] Connected. Next valid ID: {orderId}")
    
    def scannerData(self, reqId: int, rank: int, contractDetails: ContractDetails, 
                    distance: str, benchmark: str, projection: str, legsStr: str):
        """Receive scanner results"""
        symbol = contractDetails.contract.symbol
        with self.lock:
            self.scanner_results.append({
                'rank': rank + 1,  # Convert from 0-based to 1-based
                'symbol': symbol,
                'contract': contractDetails.contract,
                'distance': distance,
                'benchmark': benchmark
            })
        print(f"[IBKR SCANNER] {rank + 1}: {symbol}")
    
    def scannerDataEnd(self, reqId: int):
        """Called when scanner data is complete"""
        with self.lock:
            self.scanner_complete = True
        print(f"[IBKR SCANNER] Scanner data complete ({len(self.scanner_results)} results)")
    
    def error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson=""):
        """Error handler"""
        # Suppress common info messages
        if errorCode in [2104, 2106, 2107, 2119, 2158]:
            return
        if errorCode >= 500:
            print(f"[IBKR SCANNER ERROR] ReqId: {reqId}, Code: {errorCode}, Msg: {errorString}")


def get_top_gainers_ibkr(top_n: int = 20, host: str = "127.0.0.1", port: int = 7497, 
                         client_id: int = 9999, premarket: bool = False) -> List[str]:
    """
    Get top gainers using IBKR's native scanner API.
    
    Args:
        top_n: Number of top gainers to fetch (max 50)
        host: TWS/IB Gateway host
        port: TWS/IB Gateway port (7497 for paper, 7496 for live)
        client_id: Unique client ID
        premarket: If True, try to get premarket movers (may have limited results)
    
    Returns:
        List of stock symbols
    """
    print(f"[IBKR SCANNER] Fetching top {top_n} gainers from IBKR...")
    
    # Create scanner app
    scanner_app = IBKRScannerApp()
    
    try:
        # Connect to TWS
        scanner_app.connect(host, port, client_id)
        
        # Start API thread
        api_thread = threading.Thread(target=scanner_app.run, daemon=True)
        api_thread.start()
        
        # Wait for connection
        timeout = 10
        waited = 0
        while not scanner_app.connected and waited < timeout:
            time.sleep(0.1)
            waited += 0.1
        
        if not scanner_app.connected:
            print("[IBKR SCANNER] Failed to connect to TWS")
            return []
        
        # Create scanner subscription
        scan_sub = ScannerSubscription()
        scan_sub.numberOfRows = min(top_n, 50)  # IBKR limit is 50
        scan_sub.instrument = 'STK'  # Stocks
        scan_sub.locationCode = 'STK.US.MAJOR'  # US major exchanges
        scan_sub.scanCode = 'TOP_PERC_GAIN'  # Top percentage gainers
        
        # For premarket, we can try different scan codes, but IBKR scanner
        # may not have dedicated premarket scans. The regular scan will
        # show premarket movers during premarket hours.
        if premarket:
            print("[IBKR SCANNER] Note: Using standard gainers scan (IBKR scanner shows premarket data during premarket hours)")
        
        # Request scanner data
        req_id = 7000
        scanner_app.reqScannerSubscription(req_id, scan_sub, [], [])
        
        # Wait for results
        timeout = 10  # Reduced timeout for startup gainer fetching
        waited = 0
        while not scanner_app.scanner_complete and waited < timeout:
            time.sleep(0.1)
            waited += 0.1
        
        if not scanner_app.scanner_complete:
            print("[IBKR SCANNER] Scanner results timed out, continuing with partial results...")
        
        # Cancel subscription
        scanner_app.cancelScannerSubscription(req_id)
        
        # Disconnect
        scanner_app.disconnect()
        
        # Extract symbols
        with scanner_app.lock:
            symbols = [result['symbol'] for result in scanner_app.scanner_results]
        
        if symbols:
            print(f"[IBKR SCANNER] Successfully fetched {len(symbols)} symbols from IBKR")
            return symbols
        else:
            print("[IBKR SCANNER] No results from IBKR scanner")
            return []
            
    except Exception as e:
        print(f"[IBKR SCANNER] Error: {e}")
        try:
            scanner_app.disconnect()
        except:
            pass
        return []


def get_top_gainers_ibkr_with_fallback(top_n: int = 20, host: str = "127.0.0.1", 
                                       port: int = 7497, client_id: int = 9999) -> List[str]:
    """
    Get top gainers from IBKR with web scraping fallback.
    
    Args:
        top_n: Number of top gainers to fetch
        host: TWS/IB Gateway host
        port: TWS/IB Gateway port
        client_id: Unique client ID
    
    Returns:
        List of stock symbols
    """
    # Try IBKR scanner first
    symbols = get_top_gainers_ibkr(top_n=top_n, host=host, port=port, client_id=client_id)
    
    # If IBKR fails, fall back to web scraping
    if not symbols or len(symbols) < 5:
        print("[IBKR SCANNER] IBKR scanner returned insufficient results, falling back to web scraping...")
        from top_gainers_fetcher import TopGainersFetcher
        fetcher = TopGainersFetcher(top_n=top_n)
        symbols = fetcher.fetch_gainers_yahoo_api()
        
        if not symbols or len(symbols) < 5:
            symbols = fetcher.fetch_gainers_finviz()
        
        if not symbols or len(symbols) < 5:
            print("[IBKR SCANNER] All methods failed, using fallback symbols")
            symbols = ["AAPL", "TSLA", "NVDA", "AMD", "MSFT", "GOOGL", "META", "AMZN"]
    
    return symbols[:top_n]


if __name__ == "__main__":
    """Test the IBKR scanner"""
    print("="*80)
    print("IBKR NATIVE SCANNER TEST")
    print("="*80)
    
    print("\n1. Testing IBKR scanner (requires TWS/IB Gateway running)...")
    symbols = get_top_gainers_ibkr(top_n=20, port=7497, client_id=9999)
    
    if symbols:
        print(f"\n   SUCCESS: Fetched {len(symbols)} symbols from IBKR:")
        print(f"   {', '.join(symbols)}")
    else:
        print("\n   FAILED: Could not fetch from IBKR")
        print("   Make sure TWS or IB Gateway is running on port 7497")
        print("   and API connections are enabled in TWS settings")
    
    print("\n" + "="*80)
    print("TEST COMPLETE")
    print("="*80)

"""
Dynamic Top Gainers Fetcher
Fetches top gaining stocks and updates periodically.
Provides a centralized symbol list for realtime_scanner and run_realtime_trading.

Priority:
1. IBKR native scanner (best, uses your existing TWS connection)
2. Yahoo Finance API (robust fallback, real-time data)
3. Fallback hardcoded list (last resort)
"""
import requests
import time
import threading
from typing import List, Optional
from datetime import datetime
import pytz

class TopGainersFetcher:
    """Fetches and maintains a list of top gaining stocks"""
    
    def __init__(self, top_n: int = 20, update_interval_minutes: int = 10, 
                 use_ibkr: bool = True, ibkr_host: str = "127.0.0.1", 
                 ibkr_port: int = 7497, ibkr_client_id: int = 9999):
        """
        Initialize the top gainers fetcher.
        """
        self.top_n = top_n
        self.update_interval = update_interval_minutes * 60
        self.use_ibkr = use_ibkr
        self.ibkr_host = ibkr_host
        self.ibkr_port = ibkr_port
        self.ibkr_client_id = ibkr_client_id
        
        self.symbols: List[str] = []
        self.last_update: Optional[datetime] = None
        self.lock = threading.Lock()
        self.running = False
        self.update_thread = None
        
        # Fallback symbols in case all fetches fail
        self.fallback_symbols = ["AAPL", "TSLA", "NVDA", "AMD", "MSFT", "GOOGL", "META", "AMZN"]
        
    def fetch_gainers_ibkr(self) -> List[str]:
        """Fetch top gainers from IBKR native scanner."""
        try:
            from ibkr_scanner import get_top_gainers_ibkr
            from realtime_scanner_premarket import get_market_session
            
            session = get_market_session()
            is_premarket = (session == "PREMARKET")
            
            symbols = get_top_gainers_ibkr(
                top_n=self.top_n,
                host=self.ibkr_host,
                port=self.ibkr_port,
                client_id=self.ibkr_client_id,
                premarket=is_premarket
            )
            return symbols if symbols else []
        except Exception as e:
            print(f"[GAINERS] IBKR fetch failed: {e}")
            return []
    
    def fetch_gainers_yahoo_api(self) -> List[str]:
        """Fetch top gainers using Yahoo Finance API endpoint."""
        try:
            url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
            params = {'scrIds': 'day_gainers', 'count': self.top_n}
            headers = {'User-Agent': 'Mozilla/5.0'}
            
            response = requests.get(url, params=params, headers=headers, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            symbols = []
            if 'finance' in data and 'result' in data['finance']:
                results = data['finance']['result']
                if results:
                    quotes = results[0].get('quotes', [])
                    for quote in quotes[:self.top_n]:
                        symbol = quote.get('symbol', '')
                        if symbol and '.' not in symbol and '^' not in symbol and len(symbol) <= 5:
                            symbols.append(symbol)
            return symbols
        except Exception as e:
            print(f"[GAINERS] Yahoo API fetch failed: {e}")
            return []
    
    def update_symbols(self) -> bool:
        """Update the symbol list from available sources."""
        et_tz = pytz.timezone('US/Eastern')
        now_et = datetime.now(et_tz)
        print(f"[GAINERS] Updating list at {now_et.strftime('%H:%M:%S ET')}...")
        
        new_symbols = []
        if self.use_ibkr:
            new_symbols = self.fetch_gainers_ibkr()
            if new_symbols: print("[GAINERS] Used IBKR Scanner")
            
        if not new_symbols:
            new_symbols = self.fetch_gainers_yahoo_api()
            if new_symbols: print("[GAINERS] Used Yahoo API Fallback")
            
        if not new_symbols:
            print("[GAINERS] Using hardcoded fallback")
            new_symbols = self.fallback_symbols
            
        with self.lock:
            self.symbols = new_symbols[:self.top_n]
            self.last_update = now_et
            print(f"[GAINERS] Symbols: {', '.join(self.symbols[:10])}...")
            return True
    
    def get_symbols(self) -> List[str]:
        with self.lock: return self.symbols.copy()
    
    def start_auto_update(self):
        if self.running: return
        self.update_symbols()
        self.running = True
        self.update_thread = threading.Thread(target=self._update_loop, daemon=True)
        self.update_thread.start()
    
    def _update_loop(self):
        while self.running:
            time.sleep(self.update_interval)
            if self.running: self.update_symbols()

    def stop_auto_update(self):
        self.running = False

# Global instance
_global_fetcher = None

def get_top_gainers(top_n: int = 20, use_ibkr: bool = True, ibkr_port: int = 7497) -> List[str]:
    global _global_fetcher
    if _global_fetcher is None:
        _global_fetcher = TopGainersFetcher(top_n=top_n, use_ibkr=use_ibkr, ibkr_port=ibkr_port)
    if not _global_fetcher.running:
        _global_fetcher.start_auto_update()
    return _global_fetcher.get_symbols()

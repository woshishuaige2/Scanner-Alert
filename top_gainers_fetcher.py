"""
Dynamic Top Gainers Fetcher
Fetches top gaining stocks and updates periodically.
Provides a centralized symbol list for realtime_scanner and run_realtime_trading.

Priority:
1. IBKR native scanner (best, uses your existing TWS connection)
2. Yahoo Finance API (good, real-time data)
3. Finviz (backup)
4. Fallback hardcoded list
"""
import requests
from bs4 import BeautifulSoup
import time
import threading
from typing import List, Optional, Dict
from datetime import datetime
import pytz
import json
import re

class TopGainersFetcher:
    """Fetches and maintains a list of top gaining stocks"""
    
    def __init__(self, top_n: int = 20, update_interval_minutes: int = 10, 
                 use_ibkr: bool = True, ibkr_host: str = "127.0.0.1", 
                 ibkr_port: int = 7497, ibkr_client_id: int = 9999):
        """
        Initialize the top gainers fetcher.
        
        Args:
            top_n: Number of top gainers to track (default: 20)
            update_interval_minutes: How often to refresh the list (default: 10)
            use_ibkr: Try IBKR scanner first (default: True)
            ibkr_host: TWS/IB Gateway host (default: 127.0.0.1)
            ibkr_port: TWS/IB Gateway port (default: 7497 for paper)
            ibkr_client_id: Client ID for IBKR scanner (default: 9999)
        """
        self.top_n = top_n
        self.update_interval = update_interval_minutes * 60  # Convert to seconds
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
        """
        Fetch top gainers from IBKR native scanner.
        This is the best method as it uses your existing TWS connection.
        Returns list of stock symbols.
        """
        try:
            from ibkr_scanner import get_top_gainers_ibkr
            
            # Check if we're in premarket
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
            
            if symbols:
                print(f"[GAINERS] Fetched {len(symbols)} gainers from IBKR scanner")
                return symbols
            else:
                return []
                
        except Exception as e:
            print(f"[GAINERS] Error fetching from IBKR: {e}")
            return []
    
    def fetch_gainers_finviz(self) -> List[str]:
        """
        Fetch top gainers from Finviz screener.
        Returns list of stock symbols.
        """
        try:
            url = "https://finviz.com/screener.ashx?v=111&s=ta_topgainers&ft=4"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            symbols = []
            
            # Finviz screener table
            # Look for all links that point to quote pages
            for link in soup.find_all('a', {'class': 'tab-link'}):
                symbol = link.text.strip()
                if symbol and len(symbol) <= 5 and symbol.isalpha():
                    symbols.append(symbol)
                    if len(symbols) >= self.top_n:
                        break
            
            if symbols:
                print(f"[GAINERS] Fetched {len(symbols)} gainers from Finviz")
                return symbols
            else:
                print(f"[GAINERS] Finviz returned no symbols")
                return []
                
        except Exception as e:
            print(f"[GAINERS] Error fetching from Finviz: {e}")
            return []
    
    def fetch_gainers_yahoo_api(self) -> List[str]:
        """
        Fetch top gainers using Yahoo Finance API endpoint.
        Returns list of stock symbols.
        """
        try:
            # Yahoo Finance uses a screener API
            url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
            params = {
                'scrIds': 'day_gainers',
                'count': self.top_n
            }
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            
            symbols = []
            if 'finance' in data and 'result' in data['finance']:
                results = data['finance']['result']
                if results and len(results) > 0:
                    quotes = results[0].get('quotes', [])
                    for quote in quotes[:self.top_n]:
                        symbol = quote.get('symbol', '')
                        # Filter out non-US stocks (contain dots or special chars)
                        if symbol and '.' not in symbol and '^' not in symbol and len(symbol) <= 5:
                            symbols.append(symbol)
            
            if symbols:
                print(f"[GAINERS] Fetched {len(symbols)} gainers from Yahoo API")
                return symbols
            else:
                print(f"[GAINERS] Yahoo API returned no symbols")
                return []
                
        except Exception as e:
            print(f"[GAINERS] Error fetching from Yahoo API: {e}")
            return []
    
    def update_symbols(self) -> bool:
        """
        Update the symbol list from gainers sources.
        Tries multiple sources in order of preference.
        Returns True if successful, False otherwise.
        """
        et_tz = pytz.timezone('US/Eastern')
        now_et = datetime.now(et_tz)
        
        print(f"[GAINERS] Updating top gainers list at {now_et.strftime('%H:%M:%S ET')}...")
        
        # Try sources in order of preference
        sources = []
        
        # IBKR scanner is best (uses existing TWS connection, real-time, premarket aware)
        if self.use_ibkr:
            sources.append(('IBKR Scanner', self.fetch_gainers_ibkr))
        
        # Web sources as fallback
        sources.extend([
            ('Yahoo API', self.fetch_gainers_yahoo_api),
            ('Finviz', self.fetch_gainers_finviz),
        ])
        
        new_symbols = []
        for source_name, fetch_func in sources:
            try:
                new_symbols = fetch_func()
                if len(new_symbols) >= 5:  # Minimum threshold
                    print(f"[GAINERS] Successfully fetched from {source_name}")
                    break
            except Exception as e:
                print(f"[GAINERS] {source_name} failed: {e}")
                continue
        
        # If all sources fail, use fallback
        if len(new_symbols) < 5:
            print(f"[GAINERS] All sources failed, using fallback list")
            new_symbols = self.fallback_symbols
        
        # Update the shared symbol list
        with self.lock:
            self.symbols = new_symbols[:self.top_n]
            self.last_update = now_et
            print(f"[GAINERS] Updated symbol list ({len(self.symbols)} symbols): {', '.join(self.symbols[:10])}{'...' if len(self.symbols) > 10 else ''}")
            return True
    
    def get_symbols(self) -> List[str]:
        """
        Get the current list of top gainer symbols.
        Thread-safe.
        """
        with self.lock:
            return self.symbols.copy()
    
    def get_last_update_time(self) -> Optional[datetime]:
        """Get the timestamp of the last successful update"""
        with self.lock:
            return self.last_update
    
    def _update_loop(self):
        """Background thread that periodically updates the symbol list"""
        while self.running:
            self.update_symbols()
            
            # Sleep in small intervals to allow quick shutdown
            elapsed = 0
            while elapsed < self.update_interval and self.running:
                time.sleep(1)
                elapsed += 1
    
    def start_auto_update(self):
        """Start automatic periodic updates in background thread"""
        if self.running:
            print("[GAINERS] Auto-update already running")
            return
        
        print(f"[GAINERS] Starting auto-update (every {self.update_interval // 60} minutes)")
        
        # Do initial update
        self.update_symbols()
        
        # Start background thread
        self.running = True
        self.update_thread = threading.Thread(target=self._update_loop, daemon=True)
        self.update_thread.start()
    
    def stop_auto_update(self):
        """Stop automatic updates"""
        if not self.running:
            return
        
        print("[GAINERS] Stopping auto-update...")
        self.running = False
        if self.update_thread:
            self.update_thread.join(timeout=5)
        print("[GAINERS] Auto-update stopped")
    
    def force_update(self):
        """Force an immediate update of the symbol list"""
        print("[GAINERS] Forcing immediate update...")
        return self.update_symbols()


# Global instance for easy access across modules
_global_fetcher: Optional[TopGainersFetcher] = None

def get_global_fetcher(top_n: int = 20, update_interval_minutes: int = 10,
                       use_ibkr: bool = True, ibkr_port: int = 7497) -> TopGainersFetcher:
    """
    Get or create the global top gainers fetcher instance.
    
    Args:
        top_n: Number of top gainers to track
        update_interval_minutes: Update frequency in minutes
        use_ibkr: Try IBKR scanner first
        ibkr_port: TWS/IB Gateway port (7497 for paper, 7496 for live)
    
    Returns:
        TopGainersFetcher instance
    """
    global _global_fetcher
    if _global_fetcher is None:
        _global_fetcher = TopGainersFetcher(
            top_n=top_n, 
            update_interval_minutes=update_interval_minutes,
            use_ibkr=use_ibkr,
            ibkr_port=ibkr_port
        )
    return _global_fetcher

def get_top_gainers(top_n: int = 20, use_ibkr: bool = True, ibkr_port: int = 7497) -> List[str]:
    """
    Convenience function to get current top gainers.
    Creates and starts auto-update if not already running.
    
    Args:
        top_n: Number of top gainers to return
        use_ibkr: Try IBKR scanner first (requires TWS/IB Gateway running)
        ibkr_port: TWS/IB Gateway port (7497 for paper, 7496 for live)
    
    Returns:
        List of stock symbols
    """
    fetcher = get_global_fetcher(top_n=top_n, use_ibkr=use_ibkr, ibkr_port=ibkr_port)
    
    # Start auto-update if not running
    if not fetcher.running:
        fetcher.start_auto_update()
    
    return fetcher.get_symbols()


if __name__ == "__main__":
    """Test the fetcher"""
    print("="*80)
    print("TOP GAINERS FETCHER TEST")
    print("="*80)
    
    # Create fetcher with IBKR enabled
    fetcher = TopGainersFetcher(top_n=20, use_ibkr=True, ibkr_port=7497)
    
    # Test IBKR scanner
    print("\n1. Testing IBKR scanner (requires TWS running on port 7497)...")
    symbols = fetcher.fetch_gainers_ibkr()
    if symbols:
        print(f"   SUCCESS: {symbols[:10]}")
    else:
        print(f"   FAILED or TWS not running")
    
    # Test Yahoo API
    print("\n2. Testing Yahoo API...")
    symbols = fetcher.fetch_gainers_yahoo_api()
    print(f"   Fetched {len(symbols)} symbols: {symbols[:10]}")
    
    # Test Finviz
    print("\n3. Testing Finviz...")
    symbols = fetcher.fetch_gainers_finviz()
    print(f"   Fetched {len(symbols)} symbols: {symbols[:10]}")
    
    # Test unified update
    print("\n4. Testing unified update (tries all sources in priority order)...")
    fetcher.update_symbols()
    final_symbols = fetcher.get_symbols()
    print(f"   Final list: {final_symbols}")
    
    print("\n" + "="*80)
    print("TEST COMPLETE")
    print("="*80)

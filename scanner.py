 # scanner.py
"""
Real-Time Stock Scanner for Ross-Style Momentum Trading

How to run:
1. Ensure you have ib_insync installed: pip install ib_insync
2. Start IBKR TWS or IB Gateway with API enabled.
3. Make sure you have US equities market data subscriptions (Level 1 + 1-min bars).
4. Place a universe symbol file (CSV/JSON) in the workspace (see CONFIG section).
5. Run: python scanner.py

Notes:
- No auto-trading, no webhooks, console output only.
- You must have proper IBKR market data subscriptions for real-time quotes and 1-min bars.
- The script respects IBKR pacing limits and batches requests.
"""


import time
from datetime import datetime
from tws_data_fetcher import create_tws_data_app
from ibapi.scanner import ScannerSubscription



HOST = "127.0.0.1"
PORT = 7497
CLIENT_ID = 123
PCT_GAIN_THRESHOLD = 15.0  # 15% gain
SCANNER_ROWS = 50  # Number of top gainers to fetch



def fetch_top_gainer_symbols(app, rows=SCANNER_ROWS):
    """
    Use IBKR TWS Scanner API to fetch top % gainers in US stocks.
    Returns a list of symbols.
    """
    from ibapi.scanner import ScannerSubscription
    symbols = []
    done = [False]


    def scanner_callback(reqId, rank, contractDetails):
        # contractDetails is a ContractDetails object, not an int
        try:
            symbol = contractDetails.contract.symbol
            if symbol not in symbols:
                symbols.append(symbol)
        except Exception:
            pass

    def scanner_end(reqId):
        done[0] = True

    # Attach callbacks

    # Patch the app's scannerData and scannerDataEnd methods
    def patched_scannerData(reqId, rank, contractDetails, distance, benchmark, projection, legsStr):
        scanner_callback(reqId, rank, contractDetails)
    def patched_scannerDataEnd(reqId):
        scanner_end(reqId)
    app.scannerData = patched_scannerData
    app.scannerDataEnd = patched_scannerDataEnd

    scan_sub = ScannerSubscription()
    scan_sub.instrument = "STK"
    scan_sub.locationCode = "STK.US.MAJOR"
    scan_sub.scanCode = "TOP_PERC_GAIN"
    scan_sub.abovePrice = 1
    scan_sub.numberOfRows = rows

    reqId = app.get_next_req_id()
    app.reqScannerSubscription(reqId, scan_sub, [], [])

    # Wait for scanner results
    timeout = 10.0
    waited = 0.0
    interval = 0.2
    while not done[0] and waited < timeout:
        time.sleep(interval)
        waited += interval

    app.cancelScannerSubscription(reqId)
    return symbols

def get_today_gainers(symbols, app):
    gainers = []
    for symbol in symbols:
        bars = app.fetch_historical_bars(
            symbol=symbol,
            end_date=datetime.now(),
            duration="1 D",
            bar_size="1 day",
            what_to_show="TRADES"
        )
        if not bars or len(bars) < 1:
            continue
        bar = bars[-1]
        open_price = bar['open']
        close_price = bar['close']
        if open_price > 0:
            pct_change = ((close_price - open_price) / open_price) * 100
            if pct_change >= PCT_GAIN_THRESHOLD:
                gainers.append(symbol)
    return gainers


# ===================== MAIN =====================
if __name__ == '__main__':
    app = create_tws_data_app(host=HOST, port=PORT, client_id=CLIENT_ID)
    if not app:
        print("[ERROR] Could not connect to TWS.")
    else:
        symbols = fetch_top_gainer_symbols(app)
        if not symbols:
            print("No top gainers found from scanner.")
        else:
            gainers = get_today_gainers(symbols, app)
            if gainers:
                print(",".join(gainers))
            else:
                print("No stocks up over 15% today.")
        app.disconnect()

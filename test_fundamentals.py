import time
import threading
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

class FundamentalApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.data = {}

    def fundamentalData(self, reqId: int, data: str):
        print(f"[TWS] Received Fundamental Data for reqId {reqId}")
        self.data[reqId] = data

    def error(self, reqId, errorCode, errorString, *args):
        print(f"[TWS Error] {errorCode}: {errorString}")

def test_fundamentals():
    app = FundamentalApp()
    app.connect("127.0.0.1", 7497, 123)
    
    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()
    
    time.sleep(2)
    
    if not app.isConnected():
        print("Failed to connect to TWS")
        return

    symbols = ["AAPL", "TSLA"]
    for i, symbol in enumerate(symbols):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        
        print(f"Requesting ReportSnapshot for {symbol}...")
        app.reqFundamentalData(i, contract, "ReportSnapshot", [])
        time.sleep(2)

    time.sleep(5)
    for reqId, xml_data in app.data.items():
        print(f"\n--- Data for reqId {reqId} ---\n")
        print(xml_data[:500] + "...") # Print first 500 chars

    app.disconnect()

if __name__ == "__main__":
    test_fundamentals()

import threading
import time
from typing import List, Dict, Optional, Callable

# --- Mock TWSDataApp ---
class MockTWSDataApp:
    def __init__(self, host="127.0.0.1", port=7497, client_id=0):
        self.connected = True
        self.lock = threading.Lock()
        self.market_data_callbacks: Dict[str, Callable] = {}
        self.fundamental_data = {
            "IVF": '<ReportSnapshot><Ratios Type="ShareStats"><Ratio FieldName="FLOAT">15000000</Ratio><Ratio FieldName="VOL10DAVG">1000000</Ratio></Ratios></ReportSnapshot>',
            "SHPH": '<ReportSnapshot><Ratios Type="ShareStats"><Ratio FieldName="FLOAT">5000000</Ratio><Ratio FieldName="VOL10DAVG">500000</Ratio></Ratios></ReportSnapshot>',
            "POLA": '<ReportSnapshot><Ratios Type="ShareStats"><Ratio FieldName="FLOAT">30000000</Ratio><Ratio FieldName="VOL10DAVG">2000000</Ratio></Ratios></ReportSnapshot>',
            "CRVS": '<ReportSnapshot><Ratios Type="ShareStats"><Ratio FieldName="FLOAT">10000000</Ratio><Ratio FieldName="VOL10DAVG">800000</Ratio></Ratios></ReportSnapshot>',
            "CCHH": '<ReportSnapshot><Ratios Type="ShareStats"><Ratio FieldName="FLOAT">25000000</Ratio><Ratio FieldName="VOL10DAVG">1500000</Ratio></Ratios></ReportSnapshot>',
        }

    def connect(self):
        print("[MOCK TWS] Connected.")
        return True

    def disconnect(self):
        print("[MOCK TWS] Disconnected.")

    def subscribe_market_data(self, symbol: str, callback: Callable):
        self.market_data_callbacks[symbol] = callback
        print(f"[MOCK TWS] Subscribed to {symbol}")

    def fetch_fundamental_data(self, symbol: str, report_type: str = "ReportSnapshot") -> Optional[str]:
        # Simulate a 1-second delay for fetching data
        time.sleep(0.1)
        return self.fundamental_data.get(symbol)

    def get_next_req_id(self):
        return int(time.time() * 1000)

# --- Mock ExecutionEngine ---
class MockExecutionEngine:
    def __init__(self, tws_app, tp_pct, sl_pct, investment_per_trade):
        self.tws_app = tws_app
        self.tp_pct = tp_pct
        self.sl_pct = sl_pct
        self.investment_per_trade = investment_per_trade
        self.active_positions: Dict[str, float] = {} # symbol -> entry_price
        self.trade_count = 0

    def execute_trade(self, symbol: str, entry_price: float) -> bool:
        if symbol in self.active_positions:
            return False # Already in a position
        
        self.trade_count += 1
        
        # Simulate a successful trade execution
        self.active_positions[symbol] = entry_price
        print(f"[MOCK EXEC] Executed BUY for {symbol} at ${entry_price:.2f}")
        return True

    def get_active_positions(self) -> List[str]:
        return list(self.active_positions.keys())

    def simulate_trade_close(self, symbol: str, close_price: float):
        if symbol in self.active_positions:
            entry_price = self.active_positions.pop(symbol)
            pnl = (close_price - entry_price) / entry_price * 100
            print(f"[MOCK EXEC] Closed {symbol}. PnL: {pnl:.2f}%")
            return pnl
        return None

def create_tws_data_app(*args, **kwargs):
    return MockTWSDataApp()

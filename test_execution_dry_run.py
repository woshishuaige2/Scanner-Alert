"""
Dry-run test for Execution Engine.
Simulates TWS behavior to verify order placement and tracking logic.
"""
import time
from datetime import datetime
from execution_engine import ExecutionEngine

class MockTWS:
    def __init__(self):
        self.next_order_id = 1000
        self.order_status_callbacks = []
        self.placed_orders = []

    def placeOrder(self, orderId, contract, order):
        self.placed_orders.append((orderId, contract.symbol, order.action, order.orderType))
        # Simulate asynchronous fill
        if order.orderType == "MKT":
            # Fill parent order after a short delay
            def fill():
                time.sleep(0.5)
                for cb in self.order_status_callbacks:
                    cb(orderId, 'Filled', order.totalQuantity, 0, 100.0, 0)
            import threading
            threading.Thread(target=fill).start()

def test_dry_run():
    print("Starting Execution Engine Dry-Run...")
    mock_tws = MockTWS()
    executor = ExecutionEngine(mock_tws, tp_pct=1.0, sl_pct=10.0, investment_per_trade=1000.0)
    
    # Simulate a trade trigger
    symbol = "SPHL"
    price = 10.0
    executor.execute_trade(symbol, price)
    
    print(f"Orders placed: {len(mock_tws.placed_orders)}")
    for oid, sym, action, otype in mock_tws.placed_orders:
        print(f"  Order {oid}: {action} {sym} {otype}")
        
    # Wait for simulated fill
    time.sleep(1.0)
    
    active = executor.get_active_positions()
    print(f"Active Positions: {active}")
    
    if any("OPEN" in p for p in active):
        print("\n[SUCCESS] Execution engine correctly tracked the simulated fill.")
    else:
        print("\n[FAIL] Execution engine did not track the fill correctly.")

if __name__ == "__main__":
    test_dry_run()

"""
Execution Engine for IBKR Paper Trading
Handles order placement, position tracking, and risk management (TP/SL).
"""
import threading
import time
from datetime import datetime
from typing import Dict, Optional
from ibapi.contract import Contract
from ibapi.order import Order

class ExecutionEngine:
    def __init__(self, tws_app, tp_pct: float = 1.0, sl_pct: float = 10.0, investment_per_trade: float = 1000.0):
        self.tws_app = tws_app
        self.tp_pct = tp_pct
        self.sl_pct = sl_pct
        self.investment_per_trade = investment_per_trade
        
        # Position tracking: symbol -> {entry_price, shares, tp_price, sl_price, order_ids}
        self.positions: Dict[str, Dict] = {}
        # Order ID tracking: order_id -> symbol
        self.order_to_symbol: Dict[int, str] = {}
        self.lock = threading.Lock()
        
        # Register order status callback
        self.tws_app.order_status_callbacks.append(self._on_order_status)
        
    def _create_contract(self, symbol: str) -> Contract:
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        return contract

    def _on_order_status(self, orderId, status, filled, remaining, avgFillPrice, parentId):
        """Callback for order status updates from TWS"""
        with self.lock:
            if orderId not in self.order_to_symbol:
                return
            
            symbol = self.order_to_symbol[orderId]
            if symbol not in self.positions:
                return
                
            pos = self.positions[symbol]
            
            # If parent order is filled, position is officially OPEN
            if orderId == pos['parent_id'] and status == 'Filled':
                if pos['status'] != 'OPEN':
                    pos['status'] = 'OPEN'
                    pos['actual_entry_price'] = avgFillPrice
                    print(f"[EXEC] >>> POSITION OPEN: {symbol} at ${avgFillPrice:.2f} <<<")
            
            # If any of the exit orders (TP or SL) are filled, position is CLOSED
            if orderId in [pos['tp_id'], pos['sl_id']] and status == 'Filled':
                print(f"[EXEC] >>> POSITION CLOSED: {symbol} at ${avgFillPrice:.2f} ({status}) <<<")
                del self.positions[symbol]
                # Clean up order mapping
                to_del = [oid for oid, sym in self.order_to_symbol.items() if sym == symbol]
                for oid in to_del:
                    del self.order_to_symbol[oid]

    def execute_trade(self, symbol: str, entry_price: float):
        """Execute a new trade with bracket orders (TP and SL)"""
        with self.lock:
            if symbol in self.positions:
                return

            # Calculate shares and bracket prices
            shares = int(self.investment_per_trade / entry_price)
            if shares <= 0:
                print(f"[EXEC] Investment too low for {symbol}. Skipping.")
                return
                
            tp_price = round(entry_price * (1 + self.tp_pct / 100), 2)
            sl_price = round(entry_price * (1 - self.sl_pct / 100), 2)
            
            # Create orders
            parent_id = self.tws_app.next_order_id
            self.tws_app.next_order_id += 3
            
            contract = self._create_contract(symbol)
            
            # 1. Parent Market Order
            parent = Order()
            parent.orderId = parent_id
            parent.action = "BUY"
            parent.orderType = "MKT"
            parent.totalQuantity = shares
            parent.transmit = False
            
            # 2. Take Profit Limit Order
            tp_order = Order()
            tp_order.orderId = parent_id + 1
            tp_order.action = "SELL"
            tp_order.orderType = "LMT"
            tp_order.totalQuantity = shares
            tp_order.lmtPrice = tp_price
            tp_order.parentId = parent_id
            tp_order.transmit = False
            
            # 3. Stop Loss Order
            sl_order = Order()
            sl_order.orderId = parent_id + 2
            sl_order.action = "SELL"
            sl_order.orderType = "STP"
            sl_order.totalQuantity = shares
            sl_order.auxPrice = sl_price
            sl_order.parentId = parent_id
            sl_order.transmit = True
            
            # Track position and orders
            self.positions[symbol] = {
                'entry_price': entry_price,
                'shares': shares,
                'tp_price': tp_price,
                'sl_price': sl_price,
                'parent_id': parent_id,
                'tp_id': tp_order.orderId,
                'sl_id': sl_order.orderId,
                'status': 'SUBMITTED',
                'time': datetime.now()
            }
            self.order_to_symbol[parent_id] = symbol
            self.order_to_symbol[tp_order.orderId] = symbol
            self.order_to_symbol[sl_order.orderId] = symbol
            
            # Place orders
            self.tws_app.placeOrder(parent.orderId, contract, parent)
            self.tws_app.placeOrder(tp_order.orderId, contract, tp_order)
            self.tws_app.placeOrder(sl_order.orderId, contract, sl_order)
            
            print(f"[EXEC] Bracket Order Submitted for {symbol}: {shares} shares")
            print(f"       Target Entry: ~${entry_price:.2f} | TP: ${tp_price:.2f} | SL: ${sl_price:.2f}")

    def get_active_positions(self):
        with self.lock:
            return [f"{s} ({p['status']})" for s, p in self.positions.items()]

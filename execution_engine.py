"""
Execution Engine for IBKR Paper Trading
Handles order placement, position tracking, and risk management (TP/SL).
"""
import threading
import time
import sys
from datetime import datetime
from typing import Dict, Optional, List, Set
from ibapi.contract import Contract
from ibapi.order import Order

class ExecutionEngine:
    def __init__(self, tws_app, account: str, tp_pct: float = 1.0, sl_pct: float = 10.0, investment_per_trade: float = 1000.0):
        self.tws_app = tws_app
        self.account = account
        self.tp_pct = tp_pct
        self.sl_pct = sl_pct
        self.investment_per_trade = investment_per_trade
        
        # Position tracking: symbol -> {entry_price, shares, tp_price, sl_price, order_ids, status, time}
        self.positions: Dict[str, Dict] = {}
        # Order ID tracking: order_id -> symbol
        self.order_to_symbol: Dict[int, str] = {}
        # Trade History: List of completed or failed trade records
        self.trade_history: List[Dict] = []
        # Blacklisted symbols: symbols rejected by TWS due to permissions/margin
        self.blacklist: Set[str] = set()
        
        self.lock = threading.Lock()
        
        # Register order status callback
        self.tws_app.order_status_callbacks.append(self._on_order_status)
        # Register error callback to detect rejections
        self.tws_app.error_callbacks = getattr(self.tws_app, 'error_callbacks', [])
        self.tws_app.error_callbacks.append(self._on_tws_error)
        
    def _create_contract(self, symbol: str) -> Contract:
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        return contract

    def _on_tws_error(self, reqId: int, errorCode: int, errorString: str):
        """Detect rejections and blacklist symbols"""
        # Error 201: Order rejected
        # Common reasons: No Trading Permission, Margin concern, etc.
        if errorCode == 201:
            with self.lock:
                # Try to find the symbol for this reqId
                symbol = self.order_to_symbol.get(reqId)
                if symbol:
                    print(f"[EXEC] CRITICAL: {symbol} rejected by TWS ({errorString}). Blacklisting for this session.")
                    self.blacklist.add(symbol)
                    
                    # If we had a pending position, move it to history as FAILED
                    if symbol in self.positions and self.positions[symbol]['status'] == 'SUBMITTED':
                        pos = self.positions[symbol]
                        self.trade_history.append({
                            'symbol': symbol,
                            'type': 'FAILED',
                            'reason': f"REJECTED: {errorString[:30]}...",
                            'entry_price': pos['entry_price'],
                            'time': datetime.now()
                        })
                        self._cleanup_position(symbol)

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
            
            # If any order is rejected or cancelled
            if status in ['Inactive', 'Cancelled', 'ApiCancelled']:
                if pos['status'] == 'SUBMITTED':
                    # Trade failed before opening
                    self.trade_history.append({
                        'symbol': symbol,
                        'type': 'FAILED',
                        'reason': status,
                        'entry_price': pos['entry_price'],
                        'time': datetime.now()
                    })
                    print(f"[EXEC] Order {orderId} for {symbol} FAILED ({status}).")
                    self._cleanup_position(symbol)
                elif pos['status'] == 'OPEN':
                    # This might happen if an exit order is cancelled manually
                    print(f"[EXEC] Warning: Exit order {orderId} for {symbol} was {status}.")

            # If any of the exit orders (TP or SL) are filled, position is CLOSED
            if orderId in [pos['tp_id'], pos['sl_id']] and status == 'Filled':
                exit_type = 'TP' if orderId == pos['tp_id'] else 'SL'
                print(f"[EXEC] >>> POSITION CLOSED: {symbol} via {exit_type} at ${avgFillPrice:.2f} <<<")
                
                self.trade_history.append({
                    'symbol': symbol,
                    'type': 'CLOSED',
                    'exit_type': exit_type,
                    'entry_price': pos.get('actual_entry_price', pos['entry_price']),
                    'exit_price': avgFillPrice,
                    'shares': pos['shares'],
                    'time': datetime.now()
                })
                self._cleanup_position(symbol)

    def _cleanup_position(self, symbol: str):
        """Internal helper to clean up position tracking"""
        if symbol in self.positions:
            del self.positions[symbol]
            # Clean up order mapping
            to_del = [oid for oid, sym in self.order_to_symbol.items() if sym == symbol]
            for oid in to_del:
                del self.order_to_symbol[oid]

    def execute_trade(self, symbol: str, entry_price: float):
        """Execute a new trade with bracket orders (TP and SL)"""
        with self.lock:
            if symbol in self.positions:
                return True
            
            if symbol in self.blacklist:
                print(f"[EXEC] Skipping {symbol} - symbol is blacklisted due to prior TWS rejection.")
                return False

            # Calculate shares and bracket prices
            shares = int(self.investment_per_trade / entry_price)
            if shares <= 0:
                print(f"[EXEC] Investment too low for {symbol}. Skipping.")
                return False
                
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
            parent.tif = "DAY"
            parent.account = self.account
            parent.outsideRth = True
            
            # 2. Take Profit Limit Order
            tp_order = Order()
            tp_order.orderId = parent_id + 1
            tp_order.action = "SELL"
            tp_order.orderType = "LMT"
            tp_order.totalQuantity = shares
            tp_order.lmtPrice = tp_price
            tp_order.parentId = parent_id
            tp_order.ocaGroup = f"OCA_{parent_id}"
            tp_order.ocaType = 1
            tp_order.transmit = False
            tp_order.outsideRth = True
            
            # 3. Stop Loss Order
            sl_order = Order()
            sl_order.orderId = parent_id + 2
            sl_order.action = "SELL"
            sl_order.orderType = "STP"
            sl_order.totalQuantity = shares
            sl_order.auxPrice = sl_price
            sl_order.parentId = parent_id
            sl_order.ocaGroup = f"OCA_{parent_id}"
            sl_order.ocaType = 1
            sl_order.transmit = True
            sl_order.outsideRth = True

            # Fix for TWS Error Codes 10268 & 10269
            for o in [parent, tp_order, sl_order]:
                o.eTradeOnly = False
                o.firmQuoteOnly = False
            
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
            print(f"[EXEC] Submitted Parent Order {parent.orderId} for {symbol}")
            self.tws_app.placeOrder(tp_order.orderId, contract, tp_order)
            print(f"[EXEC] Submitted TP Order {tp_order.orderId} for {symbol}")
            self.tws_app.placeOrder(sl_order.orderId, contract, sl_order)
            print(f"[EXEC] Submitted SL Order {sl_order.orderId} for {symbol}")
            
            print(f"[EXEC] Bracket Order Submitted for {symbol}: {shares} shares")
            return True

    def is_position_active(self, symbol: str) -> bool:
        """Check if a position is currently active or pending for a symbol"""
        with self.lock:
            return symbol in self.positions

    def close_all_positions(self):
        """Close all open positions and cancel pending orders (EOD cleanup)"""
        with self.lock:
            if not self.positions:
                print("[EXEC] No active positions to close for EOD.")
                return

            print(f"[EXEC] EOD Cleanup: Closing {len(self.positions)} positions...")
            # We need to iterate over a copy of keys because _cleanup_position deletes from self.positions
            symbols = list(self.positions.keys())
            
            for symbol in symbols:
                pos = self.positions[symbol]
                contract = self._create_contract(symbol)
                
                # 1. Cancel all pending bracket orders
                for oid in [pos['tp_id'], pos['sl_id']]:
                    self.tws_app.cancelOrder(oid)
                
                # 2. If position is OPEN, submit a Market Order to close it
                if pos['status'] == 'OPEN':
                    close_order = Order()
                    close_order.action = "SELL"
                    close_order.orderType = "MKT"
                    close_order.totalQuantity = pos['shares']
                    close_order.account = self.account
                    close_order.outsideRth = True # Ensure it can execute in after-hours if slightly late
                    close_order.transmit = True
                    
                    # Fix for TWS Error Codes 10268 & 10269
                    close_order.eTradeOnly = False
                    close_order.firmQuoteOnly = False
                    
                    new_oid = self.tws_app.next_order_id
                    self.tws_app.next_order_id += 1
                    
                    self.tws_app.placeOrder(new_oid, contract, close_order)
                    print(f"[EXEC] EOD: Submitted Market Sell for {symbol} ({pos['shares']} shares)")
                
                # 3. Move to history and cleanup
                self.trade_history.append({
                    'symbol': symbol,
                    'type': 'CLOSED',
                    'exit_type': 'EOD',
                    'entry_price': pos.get('actual_entry_price', pos['entry_price']),
                    'exit_price': 0.0, # Will be updated by fill if we tracked it, but EOD is final
                    'shares': pos['shares'],
                    'time': datetime.now()
                })
                self._cleanup_position(symbol)

    def get_active_positions_detailed(self) -> List[Dict]:
        """Returns detailed list of active positions for visualization"""
        with self.lock:
            details = []
            for symbol, pos in self.positions.items():
                details.append({
                    'symbol': symbol,
                    'status': pos['status'],
                    'entry': pos['entry_price'],
                    'actual_entry': pos.get('actual_entry_price'),
                    'tp': pos['tp_price'],
                    'sl': pos['sl_price'],
                    'shares': pos['shares'],
                    'time': pos['time']
                })
            return details

    def get_trade_history(self) -> List[Dict]:
        """Returns the history of closed or failed trades"""
        with self.lock:
            return list(self.trade_history)
    
    def get_blacklist(self) -> Set[str]:
        """Returns the set of blacklisted symbols"""
        with self.lock:
            return set(self.blacklist)

"""
Execution Engine for IBKR Paper Trading
Handles order placement, position tracking, and risk management (TP/SL).
"""
import threading
from collections import deque
from datetime import datetime
from typing import Dict, List, Set
from ibapi.contract import Contract
from ibapi.order import Order
import scanner_config as config

class ExecutionEngine:
    def __init__(self, tws_app, account: str, tp_pct: float = 1.0, sl_pct: float = 10.0, investment_per_trade: float = 1000.0, telemetry=None):
        self.tws_app = tws_app
        self.account = account
        self.tp_pct = tp_pct
        self.sl_pct = sl_pct
        self.investment_per_trade = investment_per_trade
        self.telemetry = telemetry
        
        # Position tracking: symbol -> position state
        self.positions: Dict[str, Dict] = {}
        # Order ID tracking: order_id -> metadata dict
        self.order_to_symbol: Dict[int, Dict] = {}
        # Trade History: List of completed or failed trade records
        self.trade_history: List[Dict] = []
        # Blacklisted symbols: symbols rejected by TWS due to permissions/margin
        self.blacklist: Set[str] = set()
        self.position_event_callbacks = []
        
        self.lock = threading.Lock()
        
        # Register order status callback
        self.tws_app.order_status_callbacks.append(self._on_order_status)
        # Register error callback to detect rejections
        self.tws_app.error_callbacks = getattr(self.tws_app, 'error_callbacks', [])
        self.tws_app.error_callbacks.append(self._on_tws_error)
        
    @staticmethod
    def _round_price(price: float) -> float:
        return round(price, 2)

    def _create_contract(self, symbol: str) -> Contract:
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        return contract

    def _register_order(self, order_id: int, symbol: str, role: str):
        self.order_to_symbol[order_id] = {
            'symbol': symbol,
            'role': role,
            'reported_filled': 0.0,
        }

    @staticmethod
    def _compute_recent_volume_rate(volume_history: deque, window_seconds: int):
        if len(volume_history) < 2 or window_seconds <= 0:
            return None

        latest_time, latest_volume = volume_history[-1]
        cutoff = latest_time.timestamp() - float(window_seconds)
        baseline_time, baseline_volume = volume_history[0]

        for ts, cumulative_volume in reversed(volume_history):
            if ts.timestamp() <= cutoff:
                baseline_time, baseline_volume = ts, cumulative_volume
                break

        elapsed_seconds = (latest_time - baseline_time).total_seconds()
        if elapsed_seconds <= 0:
            return None

        volume_delta = max(0.0, latest_volume - baseline_volume)
        return volume_delta / elapsed_seconds

    def _log_event(self, event_type: str, **payload):
        if self.telemetry:
            self.telemetry.log_event(event_type, **payload)

    def _emit_position_event(self, event_type: str, **payload):
        for callback in list(self.position_event_callbacks):
            try:
                callback(event_type, payload)
            except Exception:
                pass

    def register_position_event_callback(self, callback):
        self.position_event_callbacks.append(callback)

    def _build_stop_order(self, order_id: int, parent_id: int, quantity: int, stop_price: float) -> Order:
        stop_order = Order()
        stop_order.orderId = order_id
        stop_order.action = "SELL"
        stop_order.orderType = "STP"
        stop_order.totalQuantity = quantity
        stop_order.auxPrice = self._round_price(stop_price)
        stop_order.parentId = parent_id
        stop_order.account = self.account
        stop_order.tif = "DAY"
        stop_order.outsideRth = True
        stop_order.transmit = True
        stop_order.eTradeOnly = False
        stop_order.firmQuoteOnly = False
        return stop_order

    def _build_market_sell_order(self, order_id: int, quantity: int) -> Order:
        order = Order()
        order.orderId = order_id
        order.action = "SELL"
        order.orderType = "MKT"
        order.totalQuantity = quantity
        order.account = self.account
        order.tif = "DAY"
        order.outsideRth = False
        order.transmit = True
        order.eTradeOnly = False
        order.firmQuoteOnly = False
        return order

    def _on_tws_error(self, reqId: int, errorCode: int, errorString: str):
        """Detect rejections and blacklist symbols"""
        # Error 201: Order rejected
        # Common reasons: No Trading Permission, Margin concern, etc.
        if errorCode == 201:
            with self.lock:
                meta = self.order_to_symbol.get(reqId)
                if meta:
                    symbol = meta['symbol']
                    print(f"[EXEC] CRITICAL: {symbol} rejected by TWS ({errorString}). Blacklisting for this session.")
                    self.blacklist.add(symbol)
                    self._log_event(
                        "order_rejected",
                        symbol=symbol,
                        order_id=reqId,
                        error_code=errorCode,
                        error_string=errorString,
                    )
                    
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
                    elif symbol in self.positions:
                        pos = self.positions[symbol]
                        if pos.get('active_exit_order_id') == reqId:
                            pos['exit_pending'] = False
                            pos['active_exit_order_id'] = None

    def _on_order_status(self, orderId, status, filled, remaining, avgFillPrice, parentId):
        """Callback for order status updates from TWS"""
        with self.lock:
            meta = self.order_to_symbol.get(orderId)
            if meta is None:
                return
            
            symbol = meta['symbol']
            if symbol not in self.positions:
                return
                
            pos = self.positions[symbol]
            role = meta['role']
            filled_qty = float(filled) if filled is not None else 0.0
            delta_filled = max(0.0, filled_qty - meta.get('reported_filled', 0.0))
            meta['reported_filled'] = filled_qty
            
            # Parent fill can arrive as partial first; treat any positive fill as OPEN.
            if role == 'parent' and (status == 'Filled' or filled_qty > 0):
                if pos['status'] != 'OPEN':
                    pos['status'] = 'OPEN'
                    pos['filled_at'] = datetime.now()
                    pos['actual_entry_price'] = avgFillPrice if avgFillPrice > 0 else pos['entry_price']
                    pos['shares'] = int(filled_qty) if filled_qty > 0 else pos['shares']
                    pos['remaining_shares'] = pos['shares']
                    pos['partial_target_price'] = self._round_price(pos['actual_entry_price'] * (1 + self.tp_pct / 100))
                    pos['current_stop_price'] = self._round_price(pos['actual_entry_price'] * (1 - self.sl_pct / 100))
                    pos['highest_price'] = pos['actual_entry_price']
                    print(f"[EXEC] >>> POSITION OPEN: {symbol} at ${pos['actual_entry_price']:.2f} <<<")
                    self._log_event(
                        "position_opened",
                        symbol=symbol,
                        order_id=orderId,
                        actual_entry_price=pos['actual_entry_price'],
                        shares=pos['shares'],
                    )
                    self._emit_position_event(
                        "position_opened",
                        symbol=symbol,
                        order_id=orderId,
                        actual_entry_price=pos['actual_entry_price'],
                        shares=pos['shares'],
                    )
                    self._submit_stop_update_locked(symbol, pos['current_stop_price'], pos['remaining_shares'])
            
            # Handle cancelled/inactive states carefully.
            if status in ['Inactive', 'Cancelled', 'ApiCancelled']:
                if role == 'parent':
                    if pos['status'] == 'SUBMITTED':
                        self.trade_history.append({
                            'symbol': symbol,
                            'type': 'FAILED',
                            'reason': status,
                            'entry_price': pos['entry_price'],
                            'time': datetime.now()
                        })
                        print(f"[EXEC] Parent order {orderId} for {symbol} FAILED ({status}).")
                        self._log_event(
                            "entry_failed",
                            symbol=symbol,
                            order_id=orderId,
                            reason=status,
                            entry_price=pos['entry_price'],
                        )
                        self._cleanup_position(symbol)
                elif role in ['partial_exit', 'final_exit', 'eod_exit', 'time_exit', 'volume_fade_exit']:
                    if pos.get('active_exit_order_id') == orderId:
                        pos['exit_pending'] = False
                        pos['active_exit_order_id'] = None

            if delta_filled > 0 and role in ['partial_exit', 'final_exit', 'eod_exit', 'time_exit', 'volume_fade_exit']:
                exit_price = avgFillPrice if avgFillPrice > 0 else pos.get('last_price', pos.get('actual_entry_price', pos['entry_price']))
                exit_shares = min(int(delta_filled), pos['remaining_shares'])
                pos['remaining_shares'] = max(0, pos['remaining_shares'] - exit_shares)
                pos['exit_pending'] = False
                if pos.get('active_exit_order_id') == orderId:
                    pos['active_exit_order_id'] = None

                if role == 'partial_exit':
                    pos['partial_taken'] = True
                    self.trade_history.append({
                        'symbol': symbol,
                        'type': 'PARTIAL',
                        'exit_type': 'PARTIAL',
                        'entry_price': pos.get('actual_entry_price', pos['entry_price']),
                        'exit_price': exit_price,
                        'shares': exit_shares,
                        'time': datetime.now()
                    })
                    print(f"[EXEC] >>> PARTIAL EXIT: {symbol} sold {exit_shares} at ${exit_price:.2f} <<<")
                    self._log_event(
                        "partial_exit_filled",
                        symbol=symbol,
                        order_id=orderId,
                        exit_price=exit_price,
                        shares=exit_shares,
                        remaining_shares=pos['remaining_shares'],
                    )
                    self._emit_position_event(
                        "partial_exit_filled",
                        symbol=symbol,
                        order_id=orderId,
                        exit_price=exit_price,
                        shares=exit_shares,
                        remaining_shares=pos['remaining_shares'],
                    )
                    if pos['remaining_shares'] > 0:
                        breakeven_stop = self._round_price(
                            pos['actual_entry_price'] * (1 + config.DYNAMIC_EXIT_BREAKEVEN_OFFSET_PCT / 100)
                        )
                        tighter_stop = max(pos['current_stop_price'], breakeven_stop)
                        self._submit_stop_update_locked(symbol, tighter_stop, pos['remaining_shares'])
                    else:
                        self._cleanup_position(symbol)
                else:
                    if role == 'eod_exit':
                        exit_type = "EOD"
                    elif role == 'time_exit':
                        exit_type = "TIME"
                    elif role == 'volume_fade_exit':
                        exit_type = "VOL_FADE"
                    else:
                        exit_type = "BOT"
                    self.trade_history.append({
                        'symbol': symbol,
                        'type': 'CLOSED',
                        'exit_type': exit_type,
                        'entry_price': pos.get('actual_entry_price', pos['entry_price']),
                        'exit_price': exit_price,
                        'shares': exit_shares,
                        'time': datetime.now()
                    })
                    print(f"[EXEC] >>> POSITION CLOSED: {symbol} via {exit_type} at ${exit_price:.2f} <<<")
                    self._log_event(
                        "position_closed",
                        symbol=symbol,
                        order_id=orderId,
                        exit_type=exit_type,
                        exit_price=exit_price,
                        shares=exit_shares,
                    )
                    self._emit_position_event(
                        "position_closed",
                        symbol=symbol,
                        order_id=orderId,
                        exit_type=exit_type,
                        exit_price=exit_price,
                        shares=exit_shares,
                    )
                    self._cleanup_position(symbol)

            if role == 'stop' and status == 'Filled':
                exit_price = avgFillPrice if avgFillPrice > 0 else pos.get('current_stop_price', pos['entry_price'])
                exit_shares = pos['remaining_shares'] if pos['remaining_shares'] > 0 else pos['shares']
                self.trade_history.append({
                    'symbol': symbol,
                    'type': 'CLOSED',
                    'exit_type': 'SL',
                    'entry_price': pos.get('actual_entry_price', pos['entry_price']),
                    'exit_price': exit_price,
                    'shares': exit_shares,
                    'time': datetime.now()
                })
                print(f"[EXEC] >>> POSITION CLOSED: {symbol} via SL at ${exit_price:.2f} <<<")
                self._log_event(
                    "position_closed",
                    symbol=symbol,
                    order_id=orderId,
                    exit_type="SL",
                    exit_price=exit_price,
                    shares=exit_shares,
                )
                self._emit_position_event(
                    "position_closed",
                    symbol=symbol,
                    order_id=orderId,
                    exit_type="SL",
                    exit_price=exit_price,
                    shares=exit_shares,
                )
                self._cleanup_position(symbol)

    def _cleanup_position(self, symbol: str):
        """Internal helper to clean up position tracking"""
        if symbol in self.positions:
            del self.positions[symbol]
            to_del = [oid for oid, sym in self.order_to_symbol.items() if sym == symbol]
            for oid in to_del:
                del self.order_to_symbol[oid]

    def _submit_stop_update_locked(self, symbol: str, new_stop_price: float, quantity: int):
        pos = self.positions[symbol]
        if quantity <= 0:
            return
        new_stop_price = self._round_price(new_stop_price)
        pos['current_stop_price'] = new_stop_price
        stop_order = self._build_stop_order(
            order_id=pos['stop_id'],
            parent_id=pos['parent_id'],
            quantity=quantity,
            stop_price=new_stop_price,
        )
        self.tws_app.placeOrder(stop_order.orderId, self._create_contract(symbol), stop_order)
        print(f"[EXEC] Updated disaster stop for {symbol}: {quantity} shares at ${new_stop_price:.2f}")
        self._log_event(
            "stop_updated",
            symbol=symbol,
            stop_order_id=stop_order.orderId,
            stop_price=new_stop_price,
            quantity=quantity,
        )

    def _submit_market_exit_locked(self, symbol: str, quantity: int, role: str):
        pos = self.positions[symbol]
        if quantity <= 0 or pos.get('exit_pending'):
            return False

        order_id = self.tws_app.next_order_id
        self.tws_app.next_order_id += 1
        order = self._build_market_sell_order(order_id, quantity)
        self._register_order(order_id, symbol, role)
        pos['exit_pending'] = True
        pos['active_exit_order_id'] = order_id
        self.tws_app.placeOrder(order.orderId, self._create_contract(symbol), order)
        print(f"[EXEC] Submitted {role} order {order_id} for {symbol}: {quantity} shares")
        self._log_event(
            "exit_order_submitted",
            symbol=symbol,
            order_id=order_id,
            role=role,
            quantity=quantity,
        )
        return True

    def execute_trade(self, symbol: str, entry_price: float):
        """Execute a new trade with a broker-side disaster stop and bot-managed exits."""
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
                
            partial_target_price = self._round_price(entry_price * (1 + self.tp_pct / 100))
            stop_price = self._round_price(entry_price * (1 - self.sl_pct / 100))
            
            parent_id = self.tws_app.next_order_id
            stop_id = parent_id + 1
            self.tws_app.next_order_id += 2
            
            contract = self._create_contract(symbol)
            
            parent = Order()
            parent.orderId = parent_id
            parent.action = "BUY"
            parent.orderType = "MKT"
            parent.totalQuantity = shares
            parent.transmit = False
            parent.tif = "DAY"
            parent.account = self.account
            parent.outsideRth = False
            parent.eTradeOnly = False
            parent.firmQuoteOnly = False
            
            stop_order = self._build_stop_order(
                order_id=stop_id,
                parent_id=parent_id,
                quantity=shares,
                stop_price=stop_price,
            )
            
            self.positions[symbol] = {
                'entry_price': entry_price,
                'shares': shares,
                'remaining_shares': shares,
                'partial_target_price': partial_target_price,
                'current_stop_price': stop_price,
                'parent_id': parent_id,
                'stop_id': stop_id,
                'status': 'SUBMITTED',
                'submitted_at': datetime.now(),
                'filled_at': None,
                'actual_entry_price': None,
                'highest_price': entry_price,
                'last_price': entry_price,
                'last_vwap': 0.0,
                'partial_taken': False,
                'exit_pending': False,
                'active_exit_order_id': None,
                'volume_history': deque(maxlen=128),
                'peak_volume_rate_15s': 0.0,
            }
            self._register_order(parent_id, symbol, 'parent')
            self._register_order(stop_id, symbol, 'stop')
            
            self.tws_app.placeOrder(parent.orderId, contract, parent)
            print(f"[EXEC] Submitted Parent Order {parent.orderId} for {symbol}")
            self.tws_app.placeOrder(stop_order.orderId, contract, stop_order)
            print(f"[EXEC] Submitted disaster stop {stop_order.orderId} for {symbol}")
            
            print(f"[EXEC] Entry submitted for {symbol}: {shares} shares | first target ${partial_target_price:.2f} | stop ${stop_price:.2f}")
            self._log_event(
                "entry_submitted",
                symbol=symbol,
                parent_order_id=parent_id,
                stop_order_id=stop_id,
                entry_price=entry_price,
                shares=shares,
                first_target=partial_target_price,
                stop_price=stop_price,
            )
            return True

    def on_market_update(self, symbol: str, price: float, volume: float = 0.0, vwap: float = 0.0, market_session: str = "REGULAR"):
        with self.lock:
            pos = self.positions.get(symbol)
            if pos is None or pos['status'] != 'OPEN':
                return

            now = datetime.now()
            pos['last_price'] = price
            pos['last_vwap'] = vwap
            pos['highest_price'] = max(pos['highest_price'], price)
            if volume > 0:
                pos['volume_history'].append((now, volume))

            if market_session != "REGULAR" or pos.get('exit_pending'):
                return

            filled_at = pos.get('filled_at')
            if (
                filled_at is not None
                and config.DYNAMIC_EXIT_MAX_HOLD_SECONDS > 0
                and (now - filled_at).total_seconds() >= config.DYNAMIC_EXIT_MAX_HOLD_SECONDS
            ):
                if self._submit_market_exit_locked(symbol, pos['remaining_shares'], 'time_exit'):
                    self._log_event(
                        "time_exit_submitted",
                        symbol=symbol,
                        max_hold_seconds=config.DYNAMIC_EXIT_MAX_HOLD_SECONDS,
                        held_seconds=round((now - filled_at).total_seconds(), 1),
                        shares=pos['remaining_shares'],
                    )
                return

            recent_volume_rate = self._compute_recent_volume_rate(
                pos['volume_history'],
                config.DYNAMIC_EXIT_VOLUME_FADE_WINDOW_SECONDS,
            )
            if recent_volume_rate is not None:
                pos['peak_volume_rate_15s'] = max(pos.get('peak_volume_rate_15s', 0.0), recent_volume_rate)

            if (
                not pos['partial_taken']
                and filled_at is not None
                and recent_volume_rate is not None
                and config.DYNAMIC_EXIT_VOLUME_FADE_MIN_HOLD_SECONDS > 0
                and (now - filled_at).total_seconds() >= config.DYNAMIC_EXIT_VOLUME_FADE_MIN_HOLD_SECONDS
            ):
                peak_volume_rate = pos.get('peak_volume_rate_15s', 0.0)
                high_price = pos.get('highest_price', 0.0)
                fade_from_peak_pct = ((high_price - price) / high_price) * 100 if high_price > 0 else 0.0
                if (
                    peak_volume_rate > 0
                    and recent_volume_rate <= peak_volume_rate * config.DYNAMIC_EXIT_VOLUME_FADE_FRACTION_OF_PEAK
                    and fade_from_peak_pct >= config.DYNAMIC_EXIT_VOLUME_FADE_MIN_RETRACE_PCT
                ):
                    if self._submit_market_exit_locked(symbol, pos['remaining_shares'], 'volume_fade_exit'):
                        self._log_event(
                            "volume_fade_exit_submitted",
                            symbol=symbol,
                            held_seconds=round((now - filled_at).total_seconds(), 1),
                            recent_volume_rate=recent_volume_rate,
                            peak_volume_rate=peak_volume_rate,
                            fade_from_peak_pct=fade_from_peak_pct,
                            shares=pos['remaining_shares'],
                        )
                    return

            if not pos['partial_taken']:
                partial_qty = int(round(pos['shares'] * config.DYNAMIC_EXIT_PARTIAL_FRACTION))
                partial_qty = max(1, partial_qty)
                partial_qty = min(partial_qty, max(1, pos['remaining_shares'] - 1)) if pos['remaining_shares'] > 1 else pos['remaining_shares']
                if price >= pos['partial_target_price'] and partial_qty > 0 and partial_qty < pos['remaining_shares'] + 1:
                    self._submit_market_exit_locked(symbol, partial_qty, 'partial_exit')
                return

            if pos['remaining_shares'] <= 0:
                return

            breakeven_stop = self._round_price(
                pos['actual_entry_price'] * (1 + config.DYNAMIC_EXIT_BREAKEVEN_OFFSET_PCT / 100)
            )
            trailing_stop = self._round_price(
                pos['highest_price'] * (1 - config.DYNAMIC_EXIT_TRAIL_OFFSET_PCT / 100)
            )
            candidate_stop = max(pos['current_stop_price'], breakeven_stop, trailing_stop)
            min_step_price = pos['current_stop_price'] * (1 + config.DYNAMIC_EXIT_MIN_STOP_UPDATE_PCT / 100)
            if candidate_stop >= min_step_price:
                self._submit_stop_update_locked(symbol, candidate_stop, pos['remaining_shares'])

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
                
                # 1. Cancel all pending stop/exit orders
                self.tws_app.cancelOrder(pos['stop_id'])
                if pos.get('active_exit_order_id'):
                    self.tws_app.cancelOrder(pos['active_exit_order_id'])
                
                # 2. If position is OPEN, submit a Market Order to close it
                if pos['status'] == 'OPEN':
                    close_order = Order()
                    close_order.action = "SELL"
                    close_order.orderType = "MKT"
                    close_order.totalQuantity = pos['remaining_shares']
                    close_order.account = self.account
                    close_order.outsideRth = True # Ensure it can execute in after-hours if slightly late
                    close_order.transmit = True
                    
                    # Fix for TWS Error Codes 10268 & 10269
                    close_order.eTradeOnly = False
                    close_order.firmQuoteOnly = False
                    
                    new_oid = self.tws_app.next_order_id
                    self.tws_app.next_order_id += 1
                    self._register_order(new_oid, symbol, 'eod_exit')
                    pos['exit_pending'] = True
                    pos['active_exit_order_id'] = new_oid
                    
                    self.tws_app.placeOrder(new_oid, contract, close_order)
                    print(f"[EXEC] EOD: Submitted Market Sell for {symbol} ({pos['remaining_shares']} shares)")
                    self._log_event(
                        "eod_exit_submitted",
                        symbol=symbol,
                        order_id=new_oid,
                        shares=pos['remaining_shares'],
                    )
                    continue

                # Non-open positions can be cleaned up after pending orders are cancelled.
                self._cleanup_position(symbol)

    def get_active_positions_detailed(self) -> List[Dict]:
        """Returns detailed list of active positions for visualization"""
        with self.lock:
            details = []
            for symbol, pos in self.positions.items():
                details.append({
                    'symbol': symbol,
                    'status': pos['status'],
                    'entry': pos.get('actual_entry_price') or pos['entry_price'],
                    'actual_entry': pos.get('actual_entry_price'),
                    'tp': pos['partial_target_price'],
                    'sl': pos['current_stop_price'],
                    'shares': pos['remaining_shares'],
                    'time': pos.get('filled_at') or pos.get('submitted_at')
                })
            return details

    def get_trade_history(self) -> List[Dict]:
        """Returns the history of closed or failed trades"""
        with self.lock:
            return list(self.trade_history)

    def get_active_position_symbols(self) -> Set[str]:
        with self.lock:
            return set(self.positions.keys())

    def get_position_snapshot(self, symbol: str):
        with self.lock:
            pos = self.positions.get(symbol)
            if pos is None:
                return None
            return dict(pos)
    
    def get_blacklist(self) -> Set[str]:
        """Returns the set of blacklisted symbols"""
        with self.lock:
            return set(self.blacklist)

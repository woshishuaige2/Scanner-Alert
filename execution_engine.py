"""
Execution Engine for IBKR Paper Trading
Handles order placement, position tracking, and risk management (TP/SL).
"""
import math
import threading
from collections import deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
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

    @staticmethod
    def _floor_price(price: float) -> float:
        return math.floor(price * 100.0) / 100.0

    def _normalize_initial_long_stop(self, desired_stop_price: float, actual_entry_price: float) -> float:
        normalized_stop = self._round_price(desired_stop_price)
        if actual_entry_price <= 0:
            return normalized_stop
        if normalized_stop < actual_entry_price:
            return normalized_stop
        safe_stop = self._floor_price(actual_entry_price - 0.01)
        return max(0.01, self._round_price(safe_stop))

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
    def _compute_spread_pct(bid: float, ask: float, reference_price: float) -> Optional[float]:
        if bid <= 0 or ask <= 0 or reference_price <= 0:
            return None
        return ((ask - bid) / reference_price) * 100.0

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

    @staticmethod
    def _get_trade_structure_mode(pos: Dict) -> str:
        entry_context = pos.get('entry_context') or {}
        return entry_context.get('structure_mode', '') or ''

    def register_position_event_callback(self, callback):
        self.position_event_callbacks.append(callback)

    def _build_stop_order(self, order_id: int, parent_id: Optional[int], quantity: int, stop_price: float) -> Order:
        stop_order = Order()
        stop_order.orderId = order_id
        stop_order.action = "SELL"
        stop_order.orderType = "STP"
        stop_order.totalQuantity = quantity
        stop_order.auxPrice = self._round_price(stop_price)
        if parent_id is not None and parent_id > 0:
            stop_order.parentId = parent_id
        stop_order.account = self.account
        stop_order.tif = "DAY"
        stop_order.outsideRth = True
        stop_order.transmit = True
        stop_order.eTradeOnly = False
        stop_order.firmQuoteOnly = False
        return stop_order

    def _build_limit_buy_order(self, order_id: int, quantity: int, limit_price: float, outside_rth: bool) -> Order:
        order = Order()
        order.orderId = order_id
        order.action = "BUY"
        order.orderType = "LMT"
        order.totalQuantity = quantity
        order.lmtPrice = self._round_price(limit_price)
        order.transmit = True
        order.tif = "DAY"
        order.account = self.account
        order.outsideRth = outside_rth
        order.eTradeOnly = False
        order.firmQuoteOnly = False
        return order

    def _build_limit_sell_order(self, order_id: int, quantity: int, limit_price: float, outside_rth: bool) -> Order:
        order = Order()
        order.orderId = order_id
        order.action = "SELL"
        order.orderType = "LMT"
        order.totalQuantity = quantity
        order.lmtPrice = self._round_price(limit_price)
        order.transmit = True
        order.tif = "DAY"
        order.account = self.account
        order.outsideRth = outside_rth
        order.eTradeOnly = False
        order.firmQuoteOnly = False
        return order

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

    def _cancel_pending_entry_locked(self, symbol: str, pos: Dict, reason: str):
        if pos.get('entry_cancel_requested'):
            return False
        parent_id = pos.get('parent_id')
        if parent_id is None:
            return False
        pos['entry_cancel_requested'] = True
        pos['pending_cancel_reason'] = reason
        self.tws_app.cancelOrder(parent_id)
        self._log_event(
            "entry_cancel_requested",
            symbol=symbol,
            order_id=parent_id,
            reason=reason,
        )
        return True

    def _preferred_sell_limit_price(self, pos: Dict) -> float:
        bid = pos.get('last_bid', 0.0)
        ask = pos.get('last_ask', 0.0)
        price = pos.get('last_price', 0.0)
        if bid and bid > 0:
            return self._round_price(bid)
        if price and price > 0:
            return self._round_price(price)
        if ask and ask > 0:
            return self._round_price(ask)
        return self._round_price(pos.get('entry_price', 0.0))

    def _on_tws_error(self, reqId: int, errorCode: int, errorString: str):
        """Detect rejections and blacklist symbols"""
        # Error 201: Order rejected
        # Common reasons: No Trading Permission, Margin concern, etc.
        if errorCode == 201:
            with self.lock:
                meta = self.order_to_symbol.get(reqId)
                if meta:
                    symbol = meta['symbol']
                    role = meta.get('role')
                    self._log_event(
                        "order_rejected",
                        symbol=symbol,
                        order_id=reqId,
                        role=role,
                        error_code=errorCode,
                        error_string=errorString,
                    )

                    # Entry-side rejections should blacklist the symbol so we do not
                    # keep attempting new opens that TWS has already denied.
                    if role == 'parent':
                        print(f"[EXEC] CRITICAL: {symbol} entry rejected by TWS ({errorString}). Blacklisting for this session.")
                        self.blacklist.add(symbol)

                        # If we had a pending position, move it to history as FAILED.
                        if symbol in self.positions and self.positions[symbol]['status'] == 'SUBMITTED':
                            pos = self.positions[symbol]
                            self.trade_history.append({
                                'symbol': symbol,
                                'type': 'FAILED',
                                'reason': f"REJECTED: {errorString[:30]}...",
                                'entry_price': pos['entry_price'],
                                'entry_time': pos.get('submitted_at'),
                                'time': datetime.now(),
                                'structure_mode': self._get_trade_structure_mode(pos),
                            })
                            self._cleanup_position(symbol)
                        return

                    # Exit-side rejections are critical, but blacklisting here would
                    # orphan a live position by removing it from the monitored set.
                    print(f"[EXEC] WARNING: Exit order {reqId} for {symbol} rejected by TWS ({errorString}).")
                    if symbol in self.positions:
                        pos = self.positions[symbol]
                        if pos.get('active_exit_order_id') == reqId:
                            pos['exit_pending'] = False
                            pos['active_exit_order_id'] = None
                            pos['last_exit_rejection_at'] = datetime.now()
                            pos['last_exit_rejection_reason'] = errorString
                            pos['last_exit_rejection_role'] = role
                            self._log_event(
                                "exit_order_rejected",
                                symbol=symbol,
                                order_id=reqId,
                                role=role,
                                error_string=errorString,
                                remaining_shares=pos.get('remaining_shares'),
                            )

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
            
            # Parent fill can arrive as partial first; keep remaining share/stop state in sync.
            if role == 'parent' and (status == 'Filled' or filled_qty > 0):
                prior_shares = int(pos.get('filled_shares', 0))
                total_filled_shares = int(math.floor(filled_qty)) if filled_qty > 0 else prior_shares
                added_shares = max(0, total_filled_shares - prior_shares)
                if pos['status'] != 'OPEN':
                    pos['status'] = 'OPEN'
                    pos['filled_at'] = datetime.now()
                    pos['actual_entry_price'] = avgFillPrice if avgFillPrice > 0 else pos['entry_price']
                    pos['filled_shares'] = total_filled_shares
                    pos['shares'] = total_filled_shares if total_filled_shares > 0 else pos.get('requested_shares', pos['shares'])
                    pos['remaining_shares'] = pos['shares']
                    pos['partial_target_price'] = self._round_price(pos['actual_entry_price'] * (1 + self.tp_pct / 100))
                    desired_stop_price = self._round_price(
                        pos.get('current_stop_price')
                        or (pos['actual_entry_price'] * (1 - self.sl_pct / 100))
                    )
                    pos['current_stop_price'] = self._normalize_initial_long_stop(
                        desired_stop_price,
                        pos['actual_entry_price'],
                    )
                    pos['highest_price'] = pos['actual_entry_price']
                    print(f"[EXEC] >>> POSITION OPEN: {symbol} at ${pos['actual_entry_price']:.2f} <<<")
                    if pos['current_stop_price'] != desired_stop_price:
                        self._log_event(
                            "initial_stop_adjusted_for_fill",
                            symbol=symbol,
                            desired_stop_price=desired_stop_price,
                            adjusted_stop_price=pos['current_stop_price'],
                            actual_entry_price=pos['actual_entry_price'],
                            order_id=orderId,
                        )
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
                    pos['entry_fill_started_at'] = datetime.now()
                    self._submit_stop_update_locked(symbol, pos['current_stop_price'], pos['remaining_shares'])
                else:
                    if avgFillPrice > 0:
                        pos['actual_entry_price'] = avgFillPrice
                        pos['partial_target_price'] = self._round_price(pos['actual_entry_price'] * (1 + self.tp_pct / 100))
                    if added_shares > 0:
                        pos['filled_shares'] = total_filled_shares
                        pos['shares'] = total_filled_shares
                        pos['remaining_shares'] = pos.get('remaining_shares', 0) + added_shares
                        pos['entry_fill_started_at'] = pos.get('entry_fill_started_at') or datetime.now()
                        self._submit_stop_update_locked(symbol, pos['current_stop_price'], pos['remaining_shares'])
                        self._log_event(
                            "entry_partial_fill_updated",
                            symbol=symbol,
                            order_id=orderId,
                            total_filled_shares=pos['shares'],
                            remaining_shares=pos['remaining_shares'],
                            actual_entry_price=pos.get('actual_entry_price'),
                        )
            
            # Handle cancelled/inactive states carefully.
            if status in ['Inactive', 'Cancelled', 'ApiCancelled']:
                if role == 'parent':
                    if pos['status'] == 'SUBMITTED':
                        failure_reason = pos.get('pending_cancel_reason') or status
                        self.trade_history.append({
                            'symbol': symbol,
                            'type': 'FAILED',
                            'reason': failure_reason,
                            'entry_price': pos['entry_price'],
                            'entry_time': pos.get('submitted_at'),
                            'time': datetime.now(),
                            'structure_mode': self._get_trade_structure_mode(pos),
                        })
                        print(f"[EXEC] Parent order {orderId} for {symbol} FAILED ({failure_reason}).")
                        self._log_event(
                            "entry_failed",
                            symbol=symbol,
                            order_id=orderId,
                            reason=failure_reason,
                            entry_price=pos['entry_price'],
                        )
                        self._cancel_protective_orders_locked(pos, filled_order_id=orderId)
                        self._cleanup_position(symbol)
                    elif pos['status'] == 'OPEN':
                        pos['entry_cancel_requested'] = False
                        pos['entry_remainder_cancelled'] = True
                        self._log_event(
                            "entry_remainder_cancelled",
                            symbol=symbol,
                            order_id=orderId,
                            reason=pos.get('pending_cancel_reason') or status,
                            filled_shares=pos.get('filled_shares', 0),
                        )
                elif role in ['partial_exit', 'final_exit', 'eod_exit', 'time_exit', 'wall_clock_exit', 'volume_fade_exit', 'structure_exit', 'reopen_weak_exit', 'flush_fail_exit']:
                    if pos.get('active_exit_order_id') == orderId:
                        pos['exit_pending'] = False
                        pos['active_exit_order_id'] = None

            if delta_filled > 0 and role in ['partial_exit', 'final_exit', 'eod_exit', 'time_exit', 'wall_clock_exit', 'volume_fade_exit', 'structure_exit', 'reopen_weak_exit', 'flush_fail_exit', 'extended_hours_stop_exit', 'session_close_exit']:
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
                        'entry_time': pos.get('filled_at') or pos.get('submitted_at'),
                        'time': datetime.now(),
                        'structure_mode': self._get_trade_structure_mode(pos),
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
                    elif role == 'wall_clock_exit':
                        exit_type = "WALL_TIME"
                    elif role == 'volume_fade_exit':
                        exit_type = "VOL_FADE"
                    elif role == 'structure_exit':
                        exit_type = "STRUCTURE"
                    elif role == 'reopen_weak_exit':
                        exit_type = "REOPEN_WEAK"
                    elif role == 'flush_fail_exit':
                        exit_type = "FLUSH_FAIL"
                    elif role == 'extended_hours_stop_exit':
                        exit_type = "EH_STOP"
                    elif role == 'session_close_exit':
                        exit_type = "SESSION_CLOSE"
                    else:
                        exit_type = "BOT"
                    if pos['remaining_shares'] > 0:
                        self._log_event(
                            "exit_partial_fill_updated",
                            symbol=symbol,
                            order_id=orderId,
                            exit_type=exit_type,
                            exit_price=exit_price,
                            shares=exit_shares,
                            remaining_shares=pos['remaining_shares'],
                        )
                        self._emit_position_event(
                            "exit_partial_fill_updated",
                            symbol=symbol,
                            order_id=orderId,
                            exit_type=exit_type,
                            exit_price=exit_price,
                            shares=exit_shares,
                            remaining_shares=pos['remaining_shares'],
                        )
                    else:
                        self.trade_history.append({
                            'symbol': symbol,
                            'type': 'CLOSED',
                            'exit_type': exit_type,
                            'entry_price': pos.get('actual_entry_price', pos['entry_price']),
                            'exit_price': exit_price,
                            'shares': exit_shares,
                            'entry_time': pos.get('filled_at') or pos.get('submitted_at'),
                            'time': datetime.now(),
                            'structure_mode': self._get_trade_structure_mode(pos),
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
                        self._cancel_protective_orders_locked(pos, filled_order_id=orderId)
                        self._cleanup_position(symbol)

            if delta_filled > 0 and role == 'stop':
                exit_price = avgFillPrice if avgFillPrice > 0 else pos.get('current_stop_price', pos['entry_price'])
                exit_shares = min(int(delta_filled), pos['remaining_shares'] if pos['remaining_shares'] > 0 else pos['shares'])
                pos['remaining_shares'] = max(0, pos['remaining_shares'] - exit_shares)
                if pos['remaining_shares'] > 0:
                    self._log_event(
                        "stop_partial_fill_updated",
                        symbol=symbol,
                        order_id=orderId,
                        exit_price=exit_price,
                        shares=exit_shares,
                        remaining_shares=pos['remaining_shares'],
                    )
                    self._emit_position_event(
                        "stop_partial_fill_updated",
                        symbol=symbol,
                        order_id=orderId,
                        exit_price=exit_price,
                        shares=exit_shares,
                        remaining_shares=pos['remaining_shares'],
                    )
                else:
                    self._cancel_protective_orders_locked(pos, filled_order_id=orderId)
                    self.trade_history.append({
                        'symbol': symbol,
                        'type': 'CLOSED',
                        'exit_type': 'SL',
                        'entry_price': pos.get('actual_entry_price', pos['entry_price']),
                        'exit_price': exit_price,
                        'shares': exit_shares,
                        'entry_time': pos.get('filled_at') or pos.get('submitted_at'),
                        'time': datetime.now(),
                        'structure_mode': self._get_trade_structure_mode(pos),
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
            to_del = [oid for oid, meta in self.order_to_symbol.items() if meta.get('symbol') == symbol]
            for oid in to_del:
                del self.order_to_symbol[oid]

    def _cancel_protective_orders_locked(self, pos: Dict, *, filled_order_id: Optional[int] = None):
        stop_id = pos.get('stop_id')
        if pos.get('uses_broker_stop', True) and stop_id is not None and stop_id != filled_order_id:
            self.tws_app.cancelOrder(stop_id)

        active_exit_order_id = pos.get('active_exit_order_id')
        if active_exit_order_id is not None and active_exit_order_id != filled_order_id:
            self.tws_app.cancelOrder(active_exit_order_id)

    def _submit_stop_update_locked(self, symbol: str, new_stop_price: float, quantity: int):
        pos = self.positions[symbol]
        if quantity <= 0:
            return
        new_stop_price = self._round_price(new_stop_price)
        pos['current_stop_price'] = new_stop_price
        if not pos.get('uses_broker_stop', True):
            self._log_event(
                "synthetic_stop_updated",
                symbol=symbol,
                stop_price=new_stop_price,
                quantity=quantity,
            )
            return
        stop_order = self._build_stop_order(
            order_id=pos['stop_id'],
            parent_id=pos.get('stop_parent_id'),
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

    def _submit_limit_exit_locked(self, symbol: str, quantity: int, role: str, limit_price: Optional[float] = None):
        pos = self.positions[symbol]
        if quantity <= 0 or pos.get('exit_pending'):
            return False
        order_id = self.tws_app.next_order_id
        self.tws_app.next_order_id += 1
        if limit_price is None or limit_price <= 0:
            limit_price = self._preferred_sell_limit_price(pos)
        order = self._build_limit_sell_order(order_id, quantity, limit_price, outside_rth=True)
        self._register_order(order_id, symbol, role)
        pos['exit_pending'] = True
        pos['active_exit_order_id'] = order_id
        self.tws_app.placeOrder(order.orderId, self._create_contract(symbol), order)
        self._log_event(
            "exit_order_submitted",
            symbol=symbol,
            order_id=order_id,
            role=role,
            quantity=quantity,
            limit_price=limit_price,
        )
        return True

    def _submit_market_exit_locked(self, symbol: str, quantity: int, role: str):
        pos = self.positions[symbol]
        if quantity <= 0 or pos.get('exit_pending'):
            return False

        # Extended-hours positions should exit with a sell limit using the
        # current bid/last price path; regular-hours market sells can hang or
        # be cancelled premarket/afterhours.
        if not pos.get('uses_broker_stop', True):
            return self._submit_limit_exit_locked(symbol, quantity, role)

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

    def execute_trade(
        self,
        symbol: str,
        entry_price: float,
        stop_price: float = None,
        market_session: str = "REGULAR",
        bid: float = 0.0,
        ask: float = 0.0,
        entry_context: Optional[Dict] = None,
    ):
        """Execute a new trade with a broker-side disaster stop and bot-managed exits."""
        with self.lock:
            if symbol in self.positions:
                return True
            
            if symbol in self.blacklist:
                print(f"[EXEC] Skipping {symbol} - symbol is blacklisted due to prior TWS rejection.")
                return False

            # Calculate shares and bracket prices
            position_size_multiplier = float((entry_context or {}).get('size_multiplier', 1.0) or 1.0)
            shares = int((self.investment_per_trade * position_size_multiplier) / entry_price)
            if shares <= 0:
                print(f"[EXEC] Investment too low for {symbol}. Skipping.")
                return False
                
            partial_target_price = self._round_price(entry_price * (1 + self.tp_pct / 100))
            if stop_price is None:
                stop_price = self._round_price(entry_price * (1 - self.sl_pct / 100))
            else:
                stop_price = self._round_price(stop_price)

            is_extended_hours = market_session in {"PREMARKET", "AFTERHOURS"}
            spread_pct = self._compute_spread_pct(bid, ask, entry_price)
            limit_price = None
            parent_order_type = "MKT"
            should_attach_initial_stop = True

            if is_extended_hours:
                if bid <= 0 or ask <= 0:
                    print(f"[EXEC] Skipping {symbol} - missing bid/ask for extended-hours limit entry.")
                    self._log_event(
                        "entry_rejected_preflight",
                        symbol=symbol,
                        reason="missing_bid_ask_extended_hours",
                        market_session=market_session,
                    )
                    return False
                if spread_pct is not None and spread_pct > config.EXTENDED_HOURS_ENTRY_SPREAD_MAX_PCT:
                    print(
                        f"[EXEC] Skipping {symbol} - spread {spread_pct:.2f}% exceeds "
                        f"extended-hours max {config.EXTENDED_HOURS_ENTRY_SPREAD_MAX_PCT:.2f}%."
                    )
                    self._log_event(
                        "entry_rejected_preflight",
                        symbol=symbol,
                        reason="spread_too_wide_extended_hours",
                        market_session=market_session,
                        spread_pct=spread_pct,
                        max_spread_pct=config.EXTENDED_HOURS_ENTRY_SPREAD_MAX_PCT,
                    )
                    return False
                limit_reference = max(entry_price, ask)
                limit_price = self._round_price(
                    limit_reference * (1 + config.EXTENDED_HOURS_ENTRY_LIMIT_BUFFER_PCT / 100.0)
                )
                parent_order_type = "LMT"
                should_attach_initial_stop = False
            
            parent_id = self.tws_app.next_order_id
            stop_id = parent_id + 1
            self.tws_app.next_order_id += 2
            
            contract = self._create_contract(symbol)
            if parent_order_type == "LMT":
                parent = self._build_limit_buy_order(parent_id, shares, limit_price, outside_rth=True)
            else:
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

            stop_order = None
            if should_attach_initial_stop:
                stop_order = self._build_stop_order(
                    order_id=stop_id,
                    parent_id=parent_id,
                    quantity=shares,
                    stop_price=stop_price,
                )
            
            self.positions[symbol] = {
                'entry_price': entry_price,
                'entry_context': dict(entry_context or {}),
                'position_size_multiplier': position_size_multiplier,
                'entry_extension_pct': float((entry_context or {}).get('extension_pct', 0.0) or 0.0),
                'shares': shares,
                'requested_shares': shares,
                'filled_shares': 0,
                'remaining_shares': shares,
                'partial_target_price': partial_target_price,
                'current_stop_price': stop_price,
                'parent_id': parent_id,
                'stop_id': stop_id,
                'stop_parent_id': parent_id if should_attach_initial_stop else None,
                'entry_order_type': parent_order_type,
                'entry_limit_price': limit_price,
                'market_session_at_entry': market_session,
                'uses_broker_stop': not is_extended_hours,
                'pending_cancel_reason': None,
                'entry_cancel_requested': False,
                'entry_remainder_cancelled': False,
                'entry_fill_started_at': None,
                'status': 'SUBMITTED',
                'submitted_at': datetime.now(),
                'filled_at': None,
                'actual_entry_price': None,
                'highest_price': entry_price,
                'last_price': entry_price,
                'last_vwap': 0.0,
                'last_bid': 0.0,
                'last_ask': 0.0,
                'last_market_update_at': None,
                'active_hold_seconds': 0.0,
                'market_pause_state': 'ACTIVE',
                'market_pause_detected_at': None,
                'market_pause_gap_seconds': 0.0,
                'market_pause_reasons': [],
                'reopen_grace_until': None,
                'pre_pause_reference_price': None,
                'post_halt_classification': None,
                'partial_taken': False,
                'exit_pending': False,
                'active_exit_order_id': None,
                'last_exit_rejection_at': None,
                'last_exit_rejection_reason': None,
                'last_exit_rejection_role': None,
                'volume_history': deque(maxlen=128),
                'peak_volume_rate_15s': 0.0,
                'volume_fade_warning_at': None,
                'volume_fade_warning_high_price': None,
            }
            self._register_order(parent_id, symbol, 'parent')
            self._register_order(stop_id, symbol, 'stop')
            
            self.tws_app.placeOrder(parent.orderId, contract, parent)
            print(f"[EXEC] Submitted Parent Order {parent.orderId} for {symbol}")
            if stop_order is not None:
                self.tws_app.placeOrder(stop_order.orderId, contract, stop_order)
                print(f"[EXEC] Submitted disaster stop {stop_order.orderId} for {symbol}")
            
            if parent_order_type == "LMT":
                print(
                    f"[EXEC] Extended-hours entry submitted for {symbol}: {shares} shares "
                    f"limit ${limit_price:.2f} | first target ${partial_target_price:.2f} | stop ${stop_price:.2f}"
                )
            else:
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
                market_session=market_session,
                entry_order_type=parent_order_type,
                entry_limit_price=limit_price,
                spread_pct=spread_pct,
                size_multiplier=position_size_multiplier,
            )
            return True

    def submit_market_exit(self, symbol: str, role: str = 'final_exit'):
        with self.lock:
            pos = self.positions.get(symbol)
            if pos is None or pos['status'] != 'OPEN':
                return False
            success = self._submit_market_exit_locked(symbol, pos['remaining_shares'], role)
            if success:
                self._log_event(
                    "manual_exit_submitted",
                    symbol=symbol,
                    role=role,
                    shares=pos['remaining_shares'],
                )
            return success

    def _classify_reopen(self, price: float, vwap: float, reference_price: float) -> str:
        if reference_price <= 0:
            return "unknown"
        strong_threshold = reference_price * (1 + config.DYNAMIC_EXIT_REOPEN_STRONG_BUFFER_PCT / 100.0)
        if price >= strong_threshold and (vwap <= 0 or price >= vwap):
            return "strong"
        return "weak"

    def _maybe_detect_market_pause_locked(
        self,
        symbol: str,
        pos: Dict,
        now: datetime,
        price: float,
        bid: float,
        ask: float,
        vwap: float,
    ) -> bool:
        last_update_at = pos.get('last_market_update_at')
        if last_update_at is None:
            return False

        gap_seconds = (now - last_update_at).total_seconds()
        if gap_seconds < config.DYNAMIC_EXIT_MARKET_PAUSE_SUSPECT_SECONDS:
            pos['active_hold_seconds'] = pos.get('active_hold_seconds', 0.0) + max(0.0, gap_seconds)
            return False

        prev_price = pos.get('last_price', price)
        spread_pct = self._compute_spread_pct(bid, ask, price)
        missing_quotes = bid <= 0 or ask <= 0
        abnormal_spread = spread_pct is not None and spread_pct >= config.DYNAMIC_EXIT_MARKET_PAUSE_ABNORMAL_SPREAD_PCT
        frozen_price = abs(price - prev_price) < max(0.0001, price * 0.0005)
        confirmed_gap = gap_seconds >= config.DYNAMIC_EXIT_MARKET_PAUSE_CONFIRM_SECONDS

        reasons = [f"gap {gap_seconds:.1f}s"]
        if missing_quotes:
            reasons.append("missing bid/ask")
        if abnormal_spread:
            reasons.append(f"spread {spread_pct:.2f}%")
        if frozen_price:
            reasons.append("frozen price")

        confirmed_pause = confirmed_gap or missing_quotes or abnormal_spread or frozen_price
        if not confirmed_pause:
            pos['active_hold_seconds'] = pos.get('active_hold_seconds', 0.0) + max(0.0, gap_seconds)
            return False

        reference_price = pos.get('last_price', price)
        pos['market_pause_state'] = 'HALT_CONFIRMED'
        pos['market_pause_detected_at'] = now
        pos['market_pause_gap_seconds'] = gap_seconds
        pos['market_pause_reasons'] = reasons
        pos['pre_pause_reference_price'] = reference_price
        pos['post_halt_classification'] = self._classify_reopen(price, vwap, reference_price)
        pos['reopen_grace_until'] = now + timedelta(seconds=config.DYNAMIC_EXIT_REOPEN_BUFFER_SECONDS)
        self._log_event(
            "market_pause_confirmed",
            symbol=symbol,
            gap_seconds=round(gap_seconds, 1),
            reasons=list(reasons),
            reference_price=reference_price,
            reopen_classification=pos['post_halt_classification'],
            reopen_buffer_seconds=config.DYNAMIC_EXIT_REOPEN_BUFFER_SECONDS,
        )
        return True

    def on_market_update(
        self,
        symbol: str,
        price: float,
        volume: float = 0.0,
        vwap: float = 0.0,
        market_session: str = "REGULAR",
        bid: float = 0.0,
        ask: float = 0.0,
    ):
        with self.lock:
            pos = self.positions.get(symbol)
            if pos is None:
                return

            now = datetime.now()
            if pos['status'] == 'SUBMITTED':
                if (
                    pos.get('entry_order_type') == 'LMT'
                    and config.EXTENDED_HOURS_ENTRY_MAX_WAIT_SECONDS > 0
                    and (now - pos.get('submitted_at', now)).total_seconds() >= config.EXTENDED_HOURS_ENTRY_MAX_WAIT_SECONDS
                ):
                    self._cancel_pending_entry_locked(symbol, pos, "extended_hours_limit_timeout")
                elif (
                    pos.get('entry_order_type') == 'LMT'
                    and market_session in {"PREMARKET", "AFTERHOURS"}
                ):
                    spread_pct = self._compute_spread_pct(bid, ask, price)
                    if (
                        spread_pct is not None
                        and spread_pct > config.EXTENDED_HOURS_ENTRY_SPREAD_MAX_PCT
                    ):
                        self._cancel_pending_entry_locked(symbol, pos, "extended_hours_spread_blew_out")
                return

            if pos['status'] != 'OPEN':
                return

            if (
                pos.get('entry_order_type') == 'LMT'
                and not pos.get('entry_remainder_cancelled')
                and pos.get('filled_shares', 0) > 0
                and pos.get('filled_shares', 0) < pos.get('requested_shares', 0)
                and config.EXTENDED_HOURS_PARTIAL_FILL_CLEANUP_SECONDS > 0
            ):
                fill_started_at = pos.get('entry_fill_started_at') or pos.get('filled_at')
                if (
                    fill_started_at is not None
                    and (now - fill_started_at).total_seconds() >= config.EXTENDED_HOURS_PARTIAL_FILL_CLEANUP_SECONDS
                ):
                    self._cancel_pending_entry_locked(symbol, pos, "extended_hours_partial_fill_cleanup")

            pause_detected = self._maybe_detect_market_pause_locked(symbol, pos, now, price, bid, ask, vwap)
            pos['last_price'] = price
            pos['last_vwap'] = vwap
            pos['last_bid'] = bid
            pos['last_ask'] = ask
            pos['last_market_update_at'] = now
            pos['highest_price'] = max(pos['highest_price'], price)
            warning_high_price = pos.get('volume_fade_warning_high_price')
            if warning_high_price is not None and price >= warning_high_price:
                pos['volume_fade_warning_at'] = None
                pos['volume_fade_warning_high_price'] = None
                self._log_event(
                    "volume_fade_warning_cleared",
                    symbol=symbol,
                    price=price,
                    reclaimed_high_price=warning_high_price,
                )
            if volume > 0:
                pos['volume_history'].append((now, volume))

            if pos.get('exit_pending'):
                return

            if market_session in {"PREMARKET", "AFTERHOURS"} and price <= pos.get('current_stop_price', 0):
                if self._submit_limit_exit_locked(symbol, pos['remaining_shares'], 'extended_hours_stop_exit'):
                    self._log_event(
                        "extended_hours_stop_exit_submitted",
                        symbol=symbol,
                        stop_price=pos.get('current_stop_price'),
                        bid=bid,
                        ask=ask,
                        price=price,
                        shares=pos['remaining_shares'],
                    )
                return

            filled_at = pos.get('filled_at')
            if (
                filled_at is not None
                and config.DYNAMIC_EXIT_MAX_WALL_CLOCK_HOLD_SECONDS > 0
                and (now - filled_at).total_seconds() >= config.DYNAMIC_EXIT_MAX_WALL_CLOCK_HOLD_SECONDS
            ):
                if self._submit_market_exit_locked(symbol, pos['remaining_shares'], 'wall_clock_exit'):
                    self._log_event(
                        "wall_clock_exit_submitted",
                        symbol=symbol,
                        wall_clock_hold_seconds=round((now - filled_at).total_seconds(), 1),
                        max_wall_clock_hold_seconds=config.DYNAMIC_EXIT_MAX_WALL_CLOCK_HOLD_SECONDS,
                        shares=pos['remaining_shares'],
                    )
                return

            if pause_detected:
                return

            if pos.get('market_pause_state') == 'HALT_CONFIRMED':
                reopen_grace_until = pos.get('reopen_grace_until')
                if reopen_grace_until is not None and now < reopen_grace_until:
                    return

                classification = pos.get('post_halt_classification')
                pos['market_pause_state'] = 'ACTIVE'
                pos['market_pause_detected_at'] = None
                pos['reopen_grace_until'] = None
                self._log_event(
                    "market_pause_resumed",
                    symbol=symbol,
                    reopen_classification=classification,
                    price=price,
                    vwap=vwap,
                )
                if classification == 'weak':
                    if self._submit_market_exit_locked(symbol, pos['remaining_shares'], 'reopen_weak_exit'):
                        self._log_event(
                            "reopen_weak_exit_submitted",
                            symbol=symbol,
                            price=price,
                            vwap=vwap,
                            reference_price=pos.get('pre_pause_reference_price'),
                            shares=pos['remaining_shares'],
                        )
                    return

            if (
                filled_at is not None
                and config.DYNAMIC_EXIT_MAX_HOLD_SECONDS > 0
                and pos.get('active_hold_seconds', 0.0) >= config.DYNAMIC_EXIT_MAX_HOLD_SECONDS
            ):
                if self._submit_market_exit_locked(symbol, pos['remaining_shares'], 'time_exit'):
                    self._log_event(
                        "time_exit_submitted",
                        symbol=symbol,
                        max_hold_seconds=config.DYNAMIC_EXIT_MAX_HOLD_SECONDS,
                        held_seconds=round(pos.get('active_hold_seconds', 0.0), 1),
                        wall_clock_seconds=round((now - filled_at).total_seconds(), 1),
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
                    warning_at = pos.get('volume_fade_warning_at')
                    if warning_at is None:
                        pos['volume_fade_warning_at'] = now
                        pos['volume_fade_warning_high_price'] = high_price
                        self._log_event(
                            "volume_fade_warning_armed",
                            symbol=symbol,
                            held_seconds=round((now - filled_at).total_seconds(), 1),
                            recent_volume_rate=recent_volume_rate,
                            peak_volume_rate=peak_volume_rate,
                            fade_from_peak_pct=fade_from_peak_pct,
                            warning_high_price=high_price,
                            confirm_seconds=config.DYNAMIC_EXIT_VOLUME_FADE_CONFIRM_SECONDS,
                        )
                        return

                    warning_age_seconds = (now - warning_at).total_seconds()
                    if warning_age_seconds < config.DYNAMIC_EXIT_VOLUME_FADE_CONFIRM_SECONDS:
                        return

                    if self._submit_market_exit_locked(symbol, pos['remaining_shares'], 'volume_fade_exit'):
                        self._log_event(
                            "volume_fade_exit_submitted",
                            symbol=symbol,
                            held_seconds=round((now - filled_at).total_seconds(), 1),
                            recent_volume_rate=recent_volume_rate,
                            peak_volume_rate=peak_volume_rate,
                            fade_from_peak_pct=fade_from_peak_pct,
                            warning_age_seconds=round(warning_age_seconds, 1),
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

    def close_all_positions(self, market_session: str = "REGULAR"):
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
                if pos.get('uses_broker_stop', True) and pos.get('stop_id') is not None:
                    self.tws_app.cancelOrder(pos['stop_id'])
                if pos.get('active_exit_order_id'):
                    self.tws_app.cancelOrder(pos['active_exit_order_id'])

                if pos['status'] == 'SUBMITTED':
                    self._cancel_pending_entry_locked(symbol, pos, 'session_close_pending_entry')
                    continue
                
                # 2. If position is OPEN, submit a Market Order to close it
                if pos['status'] == 'OPEN':
                    if market_session in {"PREMARKET", "AFTERHOURS"}:
                        limit_price = self._preferred_sell_limit_price(pos)
                        if self._submit_limit_exit_locked(symbol, pos['remaining_shares'], 'session_close_exit', limit_price=limit_price):
                            print(f"[EXEC] Session close: Submitted Limit Sell for {symbol} ({pos['remaining_shares']} shares) at ${limit_price:.2f}")
                            self._log_event(
                                "session_close_exit_submitted",
                                symbol=symbol,
                                shares=pos['remaining_shares'],
                                limit_price=limit_price,
                                market_session=market_session,
                            )
                    else:
                        close_order = Order()
                        close_order.action = "SELL"
                        close_order.orderType = "MKT"
                        close_order.totalQuantity = pos['remaining_shares']
                        close_order.account = self.account
                        close_order.outsideRth = True
                        close_order.transmit = True
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

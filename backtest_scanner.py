
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
import json

from conditions import (
    AlertConditionSet,
    MarketData,
    PriceAboveVWAPCondition,
    UnifiedMomentumCondition
)

# Import TWS integration - REQUIRED
try:
    from tws_data_fetcher import create_tws_data_app, TWSDataApp
except ImportError:
    print("[ERROR] TWS integration not available. Install ibapi: pip install ibapi")
    exit(1)

@dataclass
class BacktestAlert:
    """Container for a triggered alert during backtest"""
    symbol: str
    timestamp: datetime
    price: float
    volume: int
    vwap: float
    conditions_triggered: List[str]
    
    def to_dict(self) -> Dict:
        return {
            'symbol': self.symbol,
            'timestamp': self.timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            'price': f"${self.price:.2f}",
            'volume': f"{self.volume:,}",
            'vwap': f"${self.vwap:.2f}",
            'conditions': self.conditions_triggered
        }

class BacktestSymbolData:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.data: List[Dict] = []
    
    def add_candle(self, timestamp, open_p, high, low, close, volume, vwap):
        self.data.append({
            'timestamp': timestamp, 'open': open_p, 'high': high, 'low': low, 
            'close': close, 'volume': volume, 'vwap': vwap
        })

class BacktestAlertScanner:
    def __init__(self, symbols: List[str], date: str):
        self.symbols = symbols
        self.date = datetime.strptime(date, "%Y-%m-%d") if isinstance(date, str) else date
        self.symbol_data: Dict[str, BacktestSymbolData] = {s: BacktestSymbolData(s) for s in symbols}
        self.alerts: Dict[str, List[BacktestAlert]] = {s: [] for s in symbols}
        self.last_alert_time: Dict[str, datetime] = {s: None for s in symbols}
        self.alert_cooldown = timedelta(seconds=60)
        self.condition_sets: Dict[str, AlertConditionSet] = {}
        
        # Mock Trading Assets
        self.initial_asset = 10000.0
        self.trade_investment = 1000.0
        self.commission_per_trade = 1.0  # $1 minimum commission
        self.current_assets: Dict[str, float] = {s: self.initial_asset for s in symbols}
        
        self._initialize_condition_sets()
    
    def _initialize_condition_sets(self):
        for symbol in self.symbols:
            cs = AlertConditionSet(f"{symbol}_backtest")
            # Unified Strict Momentum Logic
            cs.add_condition(UnifiedMomentumCondition())
            self.condition_sets[symbol] = cs

    def add_candle(self, symbol, ts, o, h, l, c, v, vwap):
        self.symbol_data[symbol].add_candle(ts, o, h, l, c, v, vwap)

    def load_data_from_tws(self, tws_app, bar_size="1 min", duration="1 D"):
        # TWS expects US/Eastern for historical data
        end_dt = datetime.combine(self.date.date(), datetime.strptime("16:00:00", "%H:%M:%S").time())
        success = True
        for symbol in self.symbols:
            print(f"[INFO] Requesting {bar_size} bars for {symbol}...")
            bars = tws_app.fetch_historical_bars(symbol, end_dt, duration, bar_size, "TRADES")
            if not bars:
                success = False; continue
            for bar in bars:
                try:
                    ds = bar['date']
                    if ' ' in ds:
                        parts = ds.split()
                        if len(parts) >= 3 and '/' in parts[-1]: ds = ' '.join(parts[:-1])
                    bdt = datetime.strptime(ds, "%Y%m%d %H:%M:%S") if len(ds) > 8 else datetime.strptime(ds, "%Y%m%d")
                    if bdt.date() == self.date.date():
                        self.add_candle(symbol, bdt, bar['open'], bar['high'], bar['low'], bar['close'], bar['volume'], bar['average'])
                except: continue
        return success

    def run_backtest(self):
        for symbol in self.symbols:
            candles = sorted(self.symbol_data[symbol].data, key=lambda x: x['timestamp'])
            price_history = {}
            volume_history = {}
            
            # Cumulative tracking for accurate VWAP
            cumulative_pv = 0.0
            cumulative_volume = 0.0
            
            for candle in candles:
                ts = candle['timestamp']
                price = candle['close']
                volume = candle['volume']
                
                # Update cumulative VWAP
                cumulative_pv += price * volume
                cumulative_volume += volume
                current_vwap = cumulative_pv / cumulative_volume if cumulative_volume > 0 else 0.0
                
                price_history[ts] = price
                volume_history[ts] = volume
                
                # Mock bid/ask for backtest spread filter
                md = MarketData(
                    symbol=symbol, 
                    price=price, 
                    volume=volume, 
                    vwap=current_vwap, 
                    timestamp=ts, 
                    bid=price-0.01, 
                    ask=price+0.01,
                    price_history=price_history, 
                    volume_history=volume_history
                )
                cs = self.condition_sets[symbol]
                if cs.check_all(md):
                    last = self.last_alert_time[symbol]
                    if last is None or (ts - last) >= self.alert_cooldown:
                        # Capture logic used from conditions
                        logic_used = "Unknown"
                        for cond in cs.conditions:
                            if isinstance(cond, UnifiedMomentumCondition):
                                logic_used = cond.logic_used
                                break
                        
                        reasons = cs.triggered_reasons[:]
                        reasons.append(f"Logic: {logic_used}")
                        
                        alert = BacktestAlert(symbol, ts, price, volume, current_vwap, reasons)
                        self.alerts[symbol].append(alert)
                        self.last_alert_time[symbol] = ts
        return self.alerts

    def calculate_pl(self, tp_pct: float, sl_pct: float, slippage_pct: float = 0.2):
        """
        Calculate P/L for each alert based on subsequent candles and update assets.
        Includes commission and simulated slippage.
        """
        results = {s: [] for s in self.symbols}
        for symbol in self.symbols:
            candles = sorted(self.symbol_data[symbol].data, key=lambda x: x['timestamp'])
            for alert in self.alerts[symbol]:
                # Apply entry slippage (buying higher than alert price)
                entry_price = alert.price * (1 + slippage_pct / 100)
                
                tp_price = entry_price * (1 + tp_pct / 100)
                sl_price = entry_price * (1 - sl_pct / 100)
                
                outcome = "OPEN"
                exit_price = entry_price
                exit_time = None
                
                # Look at subsequent candles
                for candle in candles:
                    if candle['timestamp'] <= alert.timestamp: continue
                    
                    if candle['high'] >= tp_price:
                        outcome = "WIN"
                        # Apply exit slippage for WIN (selling lower than TP price)
                        exit_price = tp_price * (1 - slippage_pct / 100)
                        exit_time = candle['timestamp']
                        break
                    elif candle['low'] <= sl_price:
                        outcome = "LOSS"
                        # Apply exit slippage for LOSS (selling lower than SL price)
                        exit_price = sl_price * (1 - slippage_pct / 100)
                        exit_time = candle['timestamp']
                        break
                
                # Calculate Mock Trading Result
                # Investment: $1000
                shares = self.trade_investment / entry_price
                gross_pl = (exit_price - entry_price) * shares
                
                # Tiered Commission: $0.005 per share, $1 minimum, max 1% of trade value
                def calculate_commission(shares, price):
                    comm = shares * 0.005
                    trade_value = shares * price
                    max_comm = trade_value * 0.01
                    return min(max_comm, max(1.0, comm))

                entry_commission = calculate_commission(shares, entry_price)
                exit_commission = calculate_commission(shares, exit_price)
                total_commission = entry_commission + exit_commission
                net_pl = gross_pl - total_commission
                
                # Update current assets for this symbol
                self.current_assets[symbol] += net_pl
                
                results[symbol].append({
                    'alert': alert, 
                    'outcome': outcome, 
                    'entry': entry_price, 
                    'exit': exit_price, 
                    'time': exit_time,
                    'net_pl': net_pl,
                    'commission': total_commission,
                    'final_asset': self.current_assets[symbol]
                })
        return results

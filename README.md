# Scanner-Alert: Ross Cameron Style Trading Scanner

Automated stock scanner and trading system with dynamic top gainers tracking, premarket support, and IBKR integration.

## Features

### Dynamic Symbol List
- **IBKR Native Scanner**: Uses TWS API's built-in market scanner for real-time top gainers
- **Automatic Updates**: Refreshes symbol list every 10 minutes
- **Premarket Support**: Detects market session and adjusts behavior accordingly
- **Fallback Sources**: Yahoo Finance API and Finviz as backup sources
- **No Manual Configuration**: Symbols are automatically fetched and updated

### Market Session Awareness
- **4 Sessions Detected**: PREMARKET (4:00-9:30 AM ET), REGULAR (9:30 AM-4:00 PM ET), AFTERHOURS (4:00-8:00 PM ET), CLOSED
- **Session-Specific Thresholds**: 5% squeeze in premarket, 10% in regular hours
- **VWAP Reset**: Automatically resyncs VWAP at session boundaries
- **Volume Tracking**: Separate volume tracking per session

### Two-Stage Screening
1. **Preliminary Screening**: Price above VWAP + Squeeze detection + Spread filter
2. **In-Depth Filtering**: Strict momentum requirements (configurable)
3. **Execution**: Automated order placement with TP/SL

## Quick Start

### Prerequisites
1. **IBKR Account** with TWS or IB Gateway installed
2. **Python 3.7+** with required packages
3. **Market Data Subscriptions** (including extended hours for premarket)

### Installation

```bash
# Clone repository
git clone https://github.com/woshishuaige2/Scanner-Alert.git
cd Scanner-Alert

# Install dependencies
pip3 install ibapi pytz requests beautifulsoup4
```

### Configuration

1. **Enable API in TWS/IB Gateway**:
   - File → Global Configuration → API → Settings
   - Enable "Enable ActiveX and Socket Clients"
   - Port: 7497 (paper trading) or 7496 (live)

2. **Update Account Number** in `run_realtime_trading.py`:
   ```python
   ACCOUNT_NUMBER = "YOUR_ACCOUNT_NUMBER"  # Line 27
   ```

3. **Adjust Trading Parameters** (optional):
   ```python
   INVESTMENT_PER_TRADE = 500.0  # Line 24
   TP_PCT = 5.0  # Take profit %
   SL_PCT = 5.0  # Stop loss %
   ```

### Usage

#### Realtime Scanner (Premarket-Ready)
```bash
python3 realtime_scanner_premarket.py
```

Features:
- Monitors top 20 gainers (auto-updated every 10 min)
- Shows market session (🌅 PREMARKET, 🔔 REGULAR, etc.)
- Real-time VWAP, volume, and alerts
- Voice alerts (optional)

#### Trading Bot (Full Automation)
```bash
python3 run_realtime_trading.py
```

Features:
- All scanner features plus automated trading
- Two-stage filtering (preliminary + in-depth)
- Automatic order execution with TP/SL
- Position tracking and P&L monitoring
- End-of-day cleanup (3:59 PM ET)

## Architecture

### Symbol List Flow
```
IBKR Scanner (priority 1)
    ↓ (if fails)
Yahoo Finance API (priority 2)
    ↓ (if fails)
Finviz (priority 3)
    ↓ (if fails)
Fallback List (AAPL, TSLA, NVDA, etc.)
```

### Screening Flow
```
Top 20 Gainers (auto-fetched)
    ↓
Preliminary Screening (realtime_scanner.py)
  - Price > VWAP
  - Squeeze detection (5% premarket, 10% regular)
  - Spread filter (< 0.5%)
    ↓
In-Depth Filtering (run_realtime_trading.py)
  - Momentum check (1.5% surge in 10s)
  - Drawdown check (< 0.5% in 10s)
  - Cooldown (60s between alerts)
    ↓
Execution (execution_engine.py)
  - Order placement
  - TP/SL monitoring
  - Position management
```

## Files

### Core Modules
- **`top_gainers_fetcher.py`**: Dynamic symbol list fetcher with auto-update
- **`ibkr_scanner.py`**: IBKR native scanner using TWS API
- **`realtime_scanner_premarket.py`**: Enhanced scanner with premarket support
- **`run_realtime_trading.py`**: Full trading bot with execution
- **`tws_data_fetcher.py`**: TWS API wrapper for market data
- **`execution_engine.py`**: Order execution and position management
- **`conditions.py`**: Alert conditions (VWAP, Squeeze, Spread)
- **`scanner_config.py`**: Configuration parameters

### Legacy/Reference
- **`realtime_scanner.py`**: Original scanner (no premarket features)
- **`backtest_scanner.py`**: Historical backtesting
- **`run_final_backtest.py`**: Backtest runner

## Configuration

### Top Gainers Fetcher
```python
# In top_gainers_fetcher.py or when calling get_top_gainers()
get_top_gainers(
    top_n=20,              # Number of symbols to track
    use_ibkr=True,         # Use IBKR scanner (recommended)
    ibkr_port=7497         # TWS port (7497=paper, 7496=live)
)
```

### Scanner Thresholds
```python
# In scanner_config.py
MAX_SPREAD_PCT = 0.5           # Maximum bid-ask spread
MIN_PRICE_SURGE_10S = 1.5      # Minimum 10s price surge %
MAX_DRAWDOWN_10S = 0.5         # Maximum 10s drawdown %
```

### Premarket vs Regular Hours
The scanner automatically adjusts:
- **Premarket (4:00-9:30 AM ET)**: 5% squeeze threshold, RVOL vs premarket volume
- **Regular (9:30 AM-4:00 PM ET)**: 10% squeeze threshold, RVOL vs daily volume

## How It Works

### 1. Symbol List Updates
Every 10 minutes, the fetcher:
1. Tries IBKR scanner (if TWS connected)
2. Falls back to Yahoo Finance API
3. Falls back to Finviz
4. Uses hardcoded fallback if all fail

### 2. Market Session Detection
Every 60 seconds, the scanner:
1. Checks current market session
2. Resyncs VWAP if session changed
3. Updates squeeze thresholds
4. Clears cross-session data

### 3. Preliminary Screening
Every second, for each symbol:
1. Check if price > VWAP
2. Check if 5%/10% squeeze detected
3. Check if spread < 0.5%
4. Trigger alert if all pass

### 4. In-Depth Filtering
When preliminary alert triggers:
1. Check 10s momentum (1.5% surge)
2. Check 10s drawdown (< 0.5%)
3. Check cooldown (60s since last alert)
4. Pass to execution if all pass

### 5. Execution
When in-depth filter passes:
1. Calculate position size ($500 / price)
2. Place market order
3. Set TP (+5%) and SL (-5%)
4. Monitor position
5. Close at TP, SL, or 3:59 PM ET

## Premarket Trading

The scanner is fully premarket-ready:

### Premarket Features
- **Session Detection**: Automatically knows it's premarket
- **Lower Thresholds**: 5% squeeze (vs 10% regular hours)
- **RVOL Adjustment**: Compares to typical premarket volume (~5% of daily)
- **VWAP Reset**: Resyncs at 4:00 AM ET
- **UI Indicators**: Shows 🌅 PREMARKET SESSION

### Testing Premarket
1. Start TWS/IB Gateway before 4:00 AM ET
2. Ensure extended hours data is enabled
3. Run: `python3 realtime_scanner_premarket.py`
4. Verify session shows "🌅 PREMARKET"
5. Watch for VWAP resync at 9:30 AM ET

## Troubleshooting

### "Could not connect to TWS"
- Ensure TWS or IB Gateway is running
- Check port (7497 for paper, 7496 for live)
- Enable API in TWS settings

### "No gainers found"
- IBKR scanner requires TWS connection
- Falls back to web scraping automatically
- Check internet connection

### "VWAP shows 0.00"
- Wait 2-3 seconds for initial sync
- Check TWS historical data permissions
- Verify extended hours data enabled

### "No data updating"
- Check TWS market data subscriptions
- Verify symbols are valid
- Check `tws_errors.log` for errors

## Advanced Usage

### Custom Symbol List
To use a fixed symbol list instead of dynamic fetching:
```python
# In realtime_scanner_premarket.py or run_realtime_trading.py
# Comment out the get_top_gainers() line and use:
SYMBOLS = ["AAPL", "TSLA", "NVDA"]  # Your custom list
```

### Adjust Update Frequency
```python
# In top_gainers_fetcher.py
fetcher = TopGainersFetcher(
    top_n=20,
    update_interval_minutes=5  # Update every 5 minutes instead of 10
)
```

### Disable IBKR Scanner
```python
# Use only web scraping (no TWS required for symbol list)
get_top_gainers(use_ibkr=False)
```

## Performance

### Resource Usage
- **CPU**: Low (< 5% on modern systems)
- **Memory**: ~50-100 MB
- **Network**: Minimal (symbol updates every 10 min, real-time data streaming)

### Limits
- **IBKR Scanner**: Max 50 results per scan, 10 active scans
- **Symbol List**: Recommended 20-30 symbols for optimal performance
- **Update Frequency**: Minimum 5 minutes recommended

## Safety Features

### Risk Management
- **Position Sizing**: Fixed dollar amount per trade
- **Stop Loss**: Automatic SL at -5% (configurable)
- **Take Profit**: Automatic TP at +5% (configurable)
- **EOD Cleanup**: All positions closed at 3:59 PM ET
- **Blacklist**: Failed symbols blacklisted for session

### Error Handling
- **Connection Loss**: Graceful disconnect and error logging
- **Invalid Data**: Fallback to previous valid data
- **API Errors**: Logged to `tws_errors.log`

## License

MIT License - See LICENSE file for details

## Support

For issues, questions, or contributions:
- GitHub Issues: https://github.com/woshishuaige2/Scanner-Alert/issues
- IBKR API Documentation: https://interactivebrokers.github.io/tws-api/

## Disclaimer

This software is for educational purposes only. Trading stocks involves risk. Past performance does not guarantee future results. Always test with paper trading before using real money.

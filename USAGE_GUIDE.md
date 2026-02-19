# Quick Usage Guide

## What Changed

### Before
You had to manually edit symbol lists in two places:
```python
# In realtime_scanner.py
SYMBOLS = ["TWNP", "FAT", "VIVS", "RPGL"]

# In run_realtime_trading.py
SYMBOLS = ["TCGL", "SGN", "CATX", "XPON", "SER", "FEED"]
```

### After
**Everything is automatic!** The scanner now:
1. Fetches top 20 gainers from IBKR (or Yahoo/Finviz as fallback)
2. Updates the list every 10 minutes
3. Works in both premarket and regular hours
4. No manual configuration needed

## Quick Start

### 1. Run the Scanner (Premarket-Ready)
```bash
python3 realtime_multi_session_scanner.py
```

**What it does:**
- Automatically fetches top 20 gainers
- Updates list every 10 minutes
- Shows market session (PREMARKET/REGULAR/AFTERHOURS)
- Real-time alerts when conditions trigger

**What you'll see:**
```
[INIT] Fetching top gainers list...
[GAINERS] Updating top gainers list at 09:30:00 ET...
[GAINERS] Fetched 20 gainers from IBKR scanner
[GAINERS] Updated symbol list (20 symbols): EMAT, TPH, RIVN, MGA, COIN...
[INIT] Monitoring 20 symbols: EMAT, TPH, RIVN, MGA, COIN...
[INIT] Connecting to TWS for standalone scanner...
```

### 2. Run the Trading Bot
```bash
python3 run_realtime_trading.py
```

**What it does:**
- Everything the scanner does, PLUS
- Automated order execution
- Position management with TP/SL
- End-of-day cleanup

**What you'll see:**
```
[INIT] Fetching top 20 gainers...
[GAINERS] Updating top gainers list at 09:30:00 ET...
[GAINERS] Fetched 20 gainers from IBKR scanner
[INIT] Monitoring 20 symbols: EMAT, TPH, RIVN, MGA, COIN...
[INIT] Connecting to TWS...
```

## How It Works

### Symbol List Priority
1. **IBKR Scanner** (best - uses your TWS connection, real-time)
2. **Yahoo Finance API** (good - real-time web data)
3. **Finviz** (backup - web scraping)
4. **Fallback List** (last resort - AAPL, TSLA, NVDA, etc.)

### Auto-Update Cycle
```
Start scanner
    ↓
Fetch top 20 gainers (tries IBKR → Yahoo → Finviz)
    ↓
Monitor symbols for 10 minutes
    ↓
Fetch updated top 20 gainers
    ↓
Update monitored symbols
    ↓
Repeat...
```

### Premarket Support
The scanner automatically detects market session:
- **4:00-9:30 AM ET**: PREMARKET mode (5% squeeze threshold)
- **9:30 AM-4:00 PM ET**: REGULAR mode (10% squeeze threshold)
- **4:00-8:00 PM ET**: AFTERHOURS mode
- **Other times**: CLOSED

## Configuration (Optional)

### Change Number of Symbols
```python
# In realtime_multi_session_scanner.py or run_realtime_trading.py
# Change this line:
SYMBOLS = get_top_gainers(top_n=20)

# To:
SYMBOLS = get_top_gainers(top_n=30)  # Monitor 30 symbols instead
```

### Change Update Frequency
```python
# In top_gainers_fetcher.py
# Default is 10 minutes, change to 5 minutes:
fetcher = TopGainersFetcher(
    top_n=20,
    update_interval_minutes=5  # Update every 5 minutes
)
```

### Disable IBKR Scanner (Use Only Web Sources)
```python
# If you don't want to use IBKR scanner (e.g., TWS not running)
SYMBOLS = get_top_gainers(use_ibkr=False)
```

### Use Fixed Symbol List (Old Behavior)
```python
# If you want to go back to manual symbol list:
# Comment out this line:
# SYMBOLS = get_top_gainers(top_n=20)

# And use:
SYMBOLS = ["AAPL", "TSLA", "NVDA"]  # Your custom list
```

## What to Expect

### First Run
```
[INIT] Fetching top gainers list...
[GAINERS] Updating top gainers list at 09:30:00 ET...
[GAINERS] Fetching top 20 gainers from IBKR...
[IBKR SCANNER] Connected. Next valid ID: 1
[IBKR SCANNER] 1: EMAT
[IBKR SCANNER] 2: TPH
...
[IBKR SCANNER] Scanner data complete (20 results)
[GAINERS] Fetched 20 gainers from IBKR scanner
[GAINERS] Successfully fetched from IBKR Scanner
[GAINERS] Updated symbol list (20 symbols): EMAT, TPH, RIVN...
[GAINERS] Starting auto-update (every 10 minutes)
```

### After 10 Minutes
```
[GAINERS] Updating top gainers list at 09:40:00 ET...
[GAINERS] Fetching top 20 gainers from IBKR...
[IBKR SCANNER] Connected. Next valid ID: 2
...
[GAINERS] Updated symbol list (20 symbols): RIVN, COIN, MGA...
```

### If IBKR Scanner Fails
```
[GAINERS] Updating top gainers list at 09:30:00 ET...
[IBKR SCANNER] Failed to connect to TWS
[GAINERS] IBKR Scanner returned no symbols
[GAINERS] Fetched 20 gainers from Yahoo API
[GAINERS] Successfully fetched from Yahoo API
[GAINERS] Updated symbol list (20 symbols): EMAT, TPH, RIVN...
```

## Troubleshooting

### "IBKR scanner returned no symbols"
**Cause**: TWS/IB Gateway not running or API not enabled  
**Solution**: 
1. Start TWS or IB Gateway
2. Enable API in settings (File → Global Configuration → API)
3. Scanner will automatically fall back to Yahoo/Finviz

### "All sources failed, using fallback list"
**Cause**: No internet connection or all sources down  
**Solution**: 
1. Check internet connection
2. Scanner will use fallback symbols (AAPL, TSLA, NVDA, etc.)
3. Will retry on next update cycle (10 minutes)

### "Symbol list not updating"
**Cause**: Auto-update thread may have stopped  
**Solution**: 
1. Restart the scanner
2. Check console for error messages
3. Verify internet connection

## Advanced Features

### Monitor Update Status
The scanner logs every update:
```
[GAINERS] Updated symbol list (20 symbols): EMAT, TPH, RIVN...
```

Look for this message every 10 minutes to confirm updates are working.

### Check Last Update Time
```python
# In your code:
from top_gainers_fetcher import get_global_fetcher
fetcher = get_global_fetcher()
last_update = fetcher.get_last_update_time()
print(f"Last update: {last_update}")
```

### Force Immediate Update
```python
# In your code:
from top_gainers_fetcher import get_global_fetcher
fetcher = get_global_fetcher()
fetcher.force_update()  # Updates immediately instead of waiting 10 min
```

## Best Practices

1. **Let IBKR Scanner Run**: It's the best source, uses your existing TWS connection
2. **Don't Set Update Too Frequent**: 10 minutes is optimal, 5 minutes minimum
3. **Monitor Console Logs**: Watch for update messages every 10 minutes
4. **Check Premarket Session**: Verify scanner shows "🌅 PREMARKET" during premarket hours
5. **Test Before Live Trading**: Always test with paper trading first

## Summary

**You don't need to do anything!** Just run the scanner and it will:
- ✅ Automatically fetch top gainers
- ✅ Update every 10 minutes
- ✅ Work in premarket and regular hours
- ✅ Fall back to other sources if IBKR fails
- ✅ Handle session transitions automatically

**No manual symbol list configuration needed!**

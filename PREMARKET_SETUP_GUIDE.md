# Premarket Scanner Setup Guide

## Overview

The enhanced `realtime_scanner_premarket.py` is now **fully premarket-ready** with the following improvements:

### ✅ Key Features Added

1. **Market Session Awareness**
   - Automatically detects: PREMARKET (4:00-9:30 AM ET), REGULAR (9:30 AM-4:00 PM ET), AFTERHOURS (4:00-8:00 PM ET), CLOSED
   - Displays current session prominently in the UI with icons (🌅 PREMARKET, 🔔 REGULAR, 🌙 AFTERHOURS, 🚫 CLOSED)

2. **Session-Specific VWAP Handling**
   - VWAP automatically resyncs when transitioning between sessions
   - Prevents cross-session VWAP contamination
   - Uses TWS historical data to calculate accurate session VWAP

3. **Premarket-Optimized Thresholds**
   - **PREMARKET**: 5% squeeze threshold (accounts for lower liquidity)
   - **REGULAR HOURS**: 10% squeeze threshold (standard)
   - Automatically switches thresholds based on current session

4. **Session Volume Tracking**
   - Tracks volume separately for each session
   - Resets volume counters on session transitions
   - Shows session-specific volume in the display

5. **Premarket-Adjusted RVOL**
   - During premarket: Compares to typical premarket volume (~5% of daily average)
   - During regular hours: Compares to full daily average
   - Provides more accurate relative volume readings for premarket

6. **Automatic Session Transition Handling**
   - Checks for session changes every 60 seconds
   - Automatically resyncs VWAP when transitioning (e.g., premarket → regular hours)
   - Clears price history to prevent cross-session momentum calculations

---

## Installation

### Prerequisites

1. **Interactive Brokers TWS or IB Gateway** running and connected
2. **Market data subscriptions** that include extended hours (premarket/afterhours)
3. **Python 3.7+** with the following packages:

```bash
pip3 install ibapi pytz
```

Or use the requirements file:

```bash
pip3 install -r requirements_premarket.txt
```

---

## Usage

### Running the Scanner

```bash
python3 realtime_scanner_premarket.py
```

### What You'll See

```
================================================================================
     ROSS CAMERON STYLE PRELIMINARY SCANNER | 07:30:45 ET | 🌅 PREMARKET SESSION
================================================================================
SYMBOL   | PRICE      | FLOAT        | RVOL         | SESSION VOL  | SCREENING ALERTS
------------------------------------------------------------------------------------------------------------------------
TWNP     | $2.45      | 15.2M        | 2.34x        | 125,430      | Price $2.45 > VWAP $2.38 | Up 6.2% in 5m (45s ago)
FAT      | $1.89      | 22.1M        | 1.87x        | 89,200       | --
VIVS     | $3.12      | 18.5M        | 3.21x        | 201,500      | --
RPGL     | $0.95      | 12.8M        | 1.45x        | 45,800       | --
================================================================================

🔔 RECENTLY TRIGGERED ALERTS (PREMARKET SESSION):
------------------------------------------------------------------------------------------------------------------------
  [07:29:00] TWNP: Price $2.45 > VWAP $2.38 | Up 6.2% in 5m
================================================================================
[INFO] PREMARKET MODE: Using 5% squeeze threshold (lower liquidity)
[INFO] RVOL compared to typical premarket volume (~5% of daily average)
[INFO] Preliminary screening active. Waiting for triggers...
```

---

## Configuration

### Editing Symbol List

Edit line 363 in `realtime_scanner_premarket.py`:

```python
SYMBOLS = ["TWNP", "FAT", "VIVS", "RPGL"]  # Add your symbols here
```

### Adjusting Thresholds

Edit the squeeze thresholds in the `update_squeeze_condition()` method (lines 102-109):

```python
if session == "PREMARKET":
    # Premarket: More lenient thresholds due to lower liquidity
    self.condition_set.add_condition(SqueezeCondition(pct_threshold=5.0, minutes=5))
else:
    # Regular hours: Standard thresholds
    self.condition_set.add_condition(SqueezeCondition(pct_threshold=10.0, minutes=5))
```

### Adjusting Premarket RVOL Calculation

Edit line 144 in `realtime_scanner_premarket.py`:

```python
typical_premarket_vol = self.avg_daily_volume * 0.05  # Default: 5% of daily volume
```

Change `0.05` to a different percentage based on your observations.

---

## Testing Before Premarket

Run the test script to verify all features work correctly:

```bash
python3 test_premarket_features.py
```

This will:
- Test session detection logic at various times
- Show your current market session
- Display session transition times
- Provide a testing checklist

---

## Troubleshooting

### Issue: Scanner shows "CLOSED" during premarket

**Solution**: Check your system timezone. The scanner uses Eastern Time (ET). Verify with:

```bash
python3 -c "from datetime import datetime; import pytz; print(datetime.now(pytz.timezone('US/Eastern')))"
```

### Issue: No data updating during premarket

**Solutions**:
1. Ensure TWS/IB Gateway is running and connected
2. Verify you have **extended hours market data** subscriptions
3. Check TWS settings: Enable "Extended Trading Hours" in market data settings
4. Check the `tws_errors.log` file for connection issues

### Issue: VWAP seems incorrect

**Solution**: The scanner automatically syncs VWAP from the start of the current session. If VWAP looks wrong:
1. Wait 2-3 seconds after startup for initial sync
2. Check TWS connection (historical data must be available)
3. Verify `useRTH=0` is set in `tws_data_fetcher.py` line 270 (already done)

### Issue: Voice alerts not working

**Solutions**:
- **Windows**: Ensure PowerShell is available (should be by default)
- **Linux/Mac**: Install espeak: `sudo apt-get install espeak` (Linux) or `brew install espeak` (Mac)
- Voice alerts are optional and won't affect scanner functionality

### Issue: Scanner doesn't transition between sessions

**Solution**: The scanner checks for session transitions every 60 seconds. If you need immediate transition detection, restart the scanner.

---

## Comparison: Original vs Premarket-Enhanced

| Feature | Original Scanner | Premarket-Enhanced Scanner |
|---------|------------------|----------------------------|
| Market session detection | ❌ No | ✅ Yes (4 sessions) |
| Session-specific VWAP | ❌ No | ✅ Yes (resyncs on transition) |
| Premarket thresholds | ❌ No | ✅ Yes (5% vs 10%) |
| Session volume tracking | ❌ No | ✅ Yes (resets per session) |
| Premarket RVOL adjustment | ❌ No | ✅ Yes (~5% of daily) |
| Extended hours data | ✅ Yes (`useRTH=0`) | ✅ Yes (maintained) |
| Session transition handling | ❌ No | ✅ Yes (auto-resync) |
| UI session indicator | ❌ No | ✅ Yes (with icons) |

---

## Critical Fixes Applied

### 1. Market Session Detection
- Added `get_market_session()` function with Eastern Time awareness
- Handles weekends and market holidays
- Updates UI to show current session

### 2. VWAP Session Reset
- VWAP now resyncs at session boundaries (4:00 AM, 9:30 AM, 4:00 PM)
- Prevents overnight VWAP contamination
- Uses TWS historical data for accurate calculation

### 3. Volume Tracking
- Added `session_volume` to track volume within current session only
- Resets on session transitions
- Provides accurate session-specific volume data

### 4. Premarket RVOL
- Compares to typical premarket volume (5% of daily average) during premarket
- Switches to daily average comparison during regular hours
- More accurate signals for low-liquidity premarket conditions

### 5. Dynamic Squeeze Thresholds
- 5% threshold during premarket (lower liquidity, smaller moves are significant)
- 10% threshold during regular hours (higher liquidity, need bigger moves)
- Automatically updates on session transitions

### 6. Price History Management
- Clears price history on session transitions
- Prevents cross-session momentum calculations
- Ensures squeeze detection only looks at current session data

---

## Tomorrow's Testing Plan

### Before Market Opens (3:30-4:00 AM ET)

1. Start TWS/IB Gateway
2. Verify connection and market data subscriptions
3. Run test script: `python3 test_premarket_features.py`
4. Should show "CLOSED" session

### At Premarket Open (4:00 AM ET)

1. Start scanner: `python3 realtime_scanner_premarket.py`
2. Verify UI shows "🌅 PREMARKET SESSION"
3. Check that data is updating (price, volume, VWAP)
4. Confirm RVOL is calculated (should be relative to premarket volume)

### During Premarket (4:00-9:30 AM ET)

1. Monitor for squeeze alerts (5% threshold)
2. Verify VWAP values look reasonable
3. Check session volume is accumulating
4. Test voice alerts if enabled

### At Market Open (9:30 AM ET)

1. Watch for session transition message in console
2. Verify UI changes to "🔔 REGULAR SESSION"
3. Confirm VWAP resyncs (check console for sync messages)
4. Verify squeeze threshold changes to 10%
5. Check that session volume resets to 0

### During Regular Hours (9:30 AM - 4:00 PM ET)

1. Confirm scanner continues working normally
2. Verify RVOL now compares to daily average (not premarket)
3. Monitor for 10% squeeze alerts

---

## Files Created/Modified

### New Files
- `realtime_scanner_premarket.py` - Enhanced scanner with full premarket support
- `test_premarket_features.py` - Test suite for premarket features
- `requirements_premarket.txt` - Python dependencies
- `PREMARKET_SETUP_GUIDE.md` - This guide
- `premarket_analysis.md` - Detailed analysis of issues found

### Original Files (Unchanged)
- `realtime_scanner.py` - Original scanner (kept as backup)
- `tws_data_fetcher.py` - Already had `useRTH=0`, no changes needed
- `conditions.py` - No changes needed
- `scanner_config.py` - No changes needed

---

## Support

If you encounter issues:

1. Check `tws_errors.log` for TWS connection errors
2. Run `test_premarket_features.py` to verify session detection
3. Verify TWS/IB Gateway settings for extended hours data
4. Check system time and timezone settings

---

## Next Steps

After successful premarket testing:

1. **Integrate with Trading System**: Use `run_realtime_trading.py` with premarket-aware logic
2. **Add Premarket-Specific Conditions**: Create conditions optimized for premarket behavior
3. **Historical Premarket Analysis**: Backtest strategies specifically for premarket hours
4. **Alert Customization**: Add premarket-specific alert sounds or notifications

---

**Ready to test tomorrow! The scanner is now fully premarket-compatible.** 🚀

# 🚀 Quick Start Guide for Tomorrow's Premarket Testing

## Before You Start (3:30-4:00 AM ET)

### 1. Start TWS/IB Gateway
- Launch TWS or IB Gateway
- Log in to your paper trading account
- Ensure you're connected (green status indicator)

### 2. Verify Extended Hours Data
- TWS Menu → Global Configuration → Market Data
- Ensure "Display extended trading hours data" is **checked**
- Confirm you have market data subscriptions for your symbols

### 3. Quick System Check
```bash
cd ~/Scanner-Alert
python3 test_premarket_features.py
```
Should show: `✓ ALL TESTS PASSED`

---

## At 4:00 AM ET (Premarket Open)

### Start the Scanner
```bash
cd ~/Scanner-Alert
python3 realtime_scanner_premarket.py
```

### What You Should See Immediately
```
[INIT] Connecting to TWS for standalone scanner...
[INIT] Current market session: PREMARKET
[TWS] Connected. Next valid order ID: XXXX
[SCANNER] Loading fundamental data for screening...
[TWS] Subscribing to live market data (including extended hours)...
```

### Expected Display
```
================================================================================
     ROSS CAMERON STYLE PRELIMINARY SCANNER | 04:00:15 ET | 🌅 PREMARKET SESSION
================================================================================
SYMBOL   | PRICE      | FLOAT        | RVOL         | SESSION VOL  | SCREENING ALERTS
------------------------------------------------------------------------------------------------------------------------
TWNP     | $X.XX      | XX.XM        | X.XXx        | XXX,XXX      | --
...
```

---

## What to Watch For

### ✅ Good Signs
- Session indicator shows **🌅 PREMARKET SESSION**
- Time displays with **ET** timezone
- Prices update every second
- VWAP values appear (may take 2-3 seconds to sync)
- Session volume accumulates
- RVOL shows values (compared to premarket volume)

### ⚠️ Warning Signs
- Session shows "CLOSED" → Check system time/timezone
- Prices don't update → Check TWS connection and market data subscriptions
- VWAP stays at 0.00 → Wait 5 seconds; if still 0, check TWS historical data permissions
- "Error 354" or "Error 10197" → Need extended hours market data subscription

---

## Testing Checklist

### During Premarket (4:00-9:30 AM ET)
- [ ] Scanner shows "PREMARKET SESSION"
- [ ] Prices update in real-time
- [ ] VWAP values are reasonable (close to current price)
- [ ] Session volume accumulates
- [ ] RVOL shows values (will be relative to typical premarket volume)
- [ ] If a stock moves 5%+ in 5 minutes above VWAP, alert triggers
- [ ] Voice alert works (optional, Windows only by default)

### At 9:30 AM ET (Market Open)
- [ ] Console shows: `[SCANNER] Market session changed: PREMARKET -> REGULAR`
- [ ] Display changes to "🔔 REGULAR SESSION"
- [ ] Console shows: `[SCANNER] Resyncing VWAP for all symbols...`
- [ ] Session volume resets to 0
- [ ] Squeeze threshold changes to 10% (check info message)
- [ ] Scanner continues running smoothly

---

## Common Issues & Quick Fixes

### Issue: "Could not connect to TWS"
**Fix:** 
1. Ensure TWS/IB Gateway is running
2. Check port: Default is 7497 (paper trading) or 7496 (live)
3. TWS Menu → Edit → Global Configuration → API → Settings
   - Enable "Enable ActiveX and Socket Clients"
   - Check port number matches (7497)

### Issue: No data updating
**Fix:**
1. Check TWS market data subscriptions
2. Verify symbols are valid and trading
3. Check `tws_errors.log` for error messages
4. Ensure "Extended Trading Hours" is enabled in TWS

### Issue: VWAP shows 0.00
**Fix:**
1. Wait 5 seconds for initial sync
2. Check TWS historical data permissions
3. Verify `useRTH=0` in tws_data_fetcher.py line 270 (already set)

### Issue: Session shows "CLOSED"
**Fix:**
1. Check system time: `date`
2. Verify timezone: `timedatectl` (Linux) or system settings (Windows)
3. Test session detection: `python3 test_premarket_features.py`

### Issue: Voice alerts not working
**Fix:**
- Windows: Should work by default (uses PowerShell)
- Linux: `sudo apt-get install espeak`
- Mac: `brew install espeak`
- Not critical for scanner functionality

---

## Stopping the Scanner

Press `Ctrl+C` to stop gracefully. You should see:
```
[INFO] Scanner stopped.
```

---

## Quick Commands Reference

```bash
# Start scanner
python3 realtime_scanner_premarket.py

# Run tests
python3 test_premarket_features.py

# Verify imports
python3 verify_imports.py

# Check syntax
python3 -m py_compile realtime_scanner_premarket.py

# View errors (if any)
cat tws_errors.log
```

---

## Expected Behavior Summary

| Time | Session | Squeeze Threshold | RVOL Comparison | VWAP Sync |
|------|---------|-------------------|-----------------|-----------|
| 4:00-9:30 AM | 🌅 PREMARKET | 5% in 5 min | vs ~5% daily avg | At 4:00 AM |
| 9:30 AM-4:00 PM | 🔔 REGULAR | 10% in 5 min | vs daily avg | At 9:30 AM |
| 4:00-8:00 PM | 🌙 AFTERHOURS | 10% in 5 min | vs daily avg | At 4:00 PM |
| Other times | 🚫 CLOSED | N/A | N/A | N/A |

---

## If Everything Works

You should see:
1. ✅ Real-time price updates
2. ✅ Accurate VWAP calculations
3. ✅ Session-appropriate RVOL
4. ✅ Alerts when conditions trigger (5% squeeze + price > VWAP)
5. ✅ Smooth transition at 9:30 AM to regular hours

**If all above work → Scanner is production-ready for premarket trading! 🎉**

---

## Support Files

- **Full Guide:** `PREMARKET_SETUP_GUIDE.md`
- **Changes Summary:** `CHANGES_SUMMARY.md`
- **Issue Analysis:** `premarket_analysis.md`
- **Test Script:** `test_premarket_features.py`

---

## Emergency Fallback

If the enhanced scanner has issues, use the original:
```bash
python3 realtime_scanner.py
```
(Note: Original doesn't have premarket-specific features)

---

**You're all set! Good luck with tomorrow's premarket testing! 🚀📈**

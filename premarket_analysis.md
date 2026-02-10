# Premarket Scanner Analysis

## Critical Issues Identified

### 1. **No Market Hours Check**
**Problem**: The scanner runs continuously without checking if it's premarket, regular hours, or after-hours.
- No time-based filtering or market session awareness
- Could miss premarket-specific behavior patterns
- No indication to user what session they're in

**Impact**: HIGH - Scanner won't distinguish premarket from regular hours

---

### 2. **VWAP Calculation Issues for Premarket**
**Problem**: VWAP calculation in `tws_data_fetcher.py` uses cumulative tracking that starts from when the scanner starts.
- Line 212-228: VWAP is calculated cumulatively from scanner start time
- In premarket, VWAP should reset at 4:00 AM ET (premarket start) or 9:30 AM ET (regular market open)
- Current implementation will carry over VWAP from previous session if scanner runs overnight

**Impact**: CRITICAL - VWAP values will be incorrect for premarket analysis

---

### 3. **Volume Tracking Issues**
**Problem**: Volume tracking doesn't account for premarket vs regular hours volume.
- Line 204-206 in `tws_data_fetcher.py`: Volume is tracked as cumulative daily volume
- Relative Volume (RVOL) calculation uses daily average volume (line 49-50 in `realtime_scanner.py`)
- Premarket volume is typically much lower, making RVOL comparisons misleading

**Impact**: HIGH - RVOL alerts may be inaccurate during premarket

---

### 4. **Historical Data Fetching**
**Problem**: No explicit handling for premarket historical data.
- `fetch_historical_bars()` uses "TRADES" data by default
- Premarket may have sparse trade data, need to ensure TWS returns premarket bars
- No specification of RTH (Regular Trading Hours) vs extended hours

**Impact**: MEDIUM - May not get premarket historical data for backtesting

---

### 5. **Market Data Subscription**
**Problem**: Real-time data subscription doesn't specify extended hours.
- Line 207 in `realtime_scanner.py`: `subscribe_market_data()` called without extended hours flag
- TWS may not stream premarket data without proper subscription settings

**Impact**: CRITICAL - May not receive premarket data at all

---

### 6. **Squeeze Condition Lookback**
**Problem**: 5-minute and 10-minute lookbacks may not work well in premarket.
- Premarket has lower liquidity and fewer trades
- 5-minute squeeze detection (line 37) may trigger on low-volume moves
- No adjustment for premarket volatility characteristics

**Impact**: MEDIUM - May generate false signals or miss real opportunities

---

### 7. **Fundamental Data Loading**
**Problem**: Fundamental data loading happens once at startup.
- Line 199 in `realtime_scanner.py`: `load_fundamentals()` called once
- If scanner runs overnight, data becomes stale
- No refresh mechanism

**Impact**: LOW - Fundamentals don't change intraday, but good practice to refresh

---

### 8. **Display Timestamp**
**Problem**: Display shows time but not market session.
- Line 119: Shows current time but no indication of market session
- User won't know if they're looking at premarket, regular, or after-hours data

**Impact**: LOW - Usability issue, not functional

---

## Recommended Fixes

### Priority 1 (CRITICAL - Must Fix)
1. Add market session detection (premarket 4:00-9:30 AM ET, regular 9:30 AM-4:00 PM ET, after-hours 4:00-8:00 PM ET)
2. Fix VWAP calculation to reset at session boundaries
3. Add extended hours flag to market data subscription
4. Display current market session in UI

### Priority 2 (HIGH - Should Fix)
5. Adjust RVOL calculation for premarket (compare to historical premarket volume, not daily average)
6. Add premarket-specific volume tracking
7. Add configuration for premarket vs regular hours thresholds

### Priority 3 (MEDIUM - Nice to Have)
8. Add premarket-specific squeeze thresholds
9. Add historical data fetch with extended hours option
10. Add session transition notifications

---

## Testing Checklist for Tomorrow
- [ ] Scanner connects to TWS successfully
- [ ] Market session displays correctly (should show "PREMARKET")
- [ ] Real-time data updates during premarket hours
- [ ] VWAP resets at 4:00 AM ET
- [ ] Volume tracking works correctly
- [ ] Alerts trigger appropriately for premarket conditions
- [ ] Display updates every second
- [ ] Voice alerts work (if enabled)

# Scanner-Alert

IBKR-connected scanner and trading workspace for three main workflows:

1. `realtime_multi_session_scanner.py`: live alert scanning across premarket, regular session, and after-hours
2. `run_realtime_trading.py`: live scanner plus stricter stage-2 filtering and order execution
3. `summarize_alert_run.py`: post-run summary and plots from `temp_alert_score_audit.log`

## Prerequisites

- Python environment with the required packages installed
- TWS or IB Gateway running
- IBKR API enabled in TWS / Gateway
- Correct TWS port configured in [scanner_config.py](/c:/Users/china/Desktop/12%20Week%20Plan/Algo%20trading/ibkr/Scanner-Alert/scanner_config.py)
- Extended-hours market data if you want premarket / after-hours coverage

## TWS / IBKR Setup

In TWS:

1. Go to `File -> Global Configuration -> API -> Settings`
2. Enable `ActiveX and Socket Clients`
3. Use port `7497` for paper trading or `7496` for live unless you have changed it

If the scripts fail to connect, verify the port in [scanner_config.py](/c:/Users/china/Desktop/12%20Week%20Plan/Algo%20trading/ibkr/Scanner-Alert/scanner_config.py) matches TWS.

## Workflow 1: Live Scanner

Script: [realtime_multi_session_scanner.py](/c:/Users/china/Desktop/12%20Week%20Plan/Algo%20trading/ibkr/Scanner-Alert/realtime_multi_session_scanner.py)

Use this when you want alerts only, without automated trading.

What it does:

- Pulls a dynamic top-gainers list
- Subscribes to live market data
- Tracks session-aware signals across `PREMARKET`, `REGULAR`, and `AFTERHOURS`
- Scores alerts and writes each triggered alert to `temp_alert_score_audit.log`
- Sends Discord alerts if configured

Run:

```powershell
python .\realtime_multi_session_scanner.py
```

Primary outputs:

- Console display of monitored symbols and recent alerts
- Audit log at [temp_alert_score_audit.log](/c:/Users/china/Desktop/12%20Week%20Plan/Algo%20trading/ibkr/Scanner-Alert/temp_alert_score_audit.log)

## Workflow 2: Live Trading

Script: [run_realtime_trading.py](/c:/Users/china/Desktop/12%20Week%20Plan/Algo%20trading/ibkr/Scanner-Alert/run_realtime_trading.py)

Use this when you want the scanner to feed a stricter stage-2 filter and place trades.

What it does:

- Reuses the realtime scanner as stage 1
- Applies stricter momentum checks before execution
- Places and manages orders through the execution engine
- Tracks active positions and trade history in the terminal

Before running:

- Confirm the account and sizing settings in [run_realtime_trading.py](/c:/Users/china/Desktop/12%20Week%20Plan/Algo%20trading/ibkr/Scanner-Alert/run_realtime_trading.py)
- Review risk settings such as `INVESTMENT_PER_TRADE`, `TP_PCT`, `SL_PCT`, and account number

Run:

```powershell
python .\run_realtime_trading.py
```

## Workflow 3: Alert Summary And Plots

Script: [summarize_alert_run.py](/c:/Users/china/Desktop/12%20Week%20Plan/Algo%20trading/ibkr/Scanner-Alert/summarize_alert_run.py)

Use this after a scanner run to review what fired and when.

What it does:

- Parses `temp_alert_score_audit.log`
- Counts total alerts
- Aggregates score-factor hits by category
- Generates time-based plots and a text summary
- Supports optional date, time-window, and symbol filters

Run all symbols:

```powershell
python .\summarize_alert_run.py
```

Run a specific date / time window:

```powershell
python .\summarize_alert_run.py --date 2026-03-23 --start 08:00 --end 13:00
```

Run a specific symbol and keep outputs separate:

```powershell
python .\summarize_alert_run.py --symbol UGRO --output-dir out\alert_run_summary_UGRO
```

Generated outputs:

- `out\alert_run_summary\summary.txt`
- `out\alert_run_summary\alerts_cumulative.png`
- `out\alert_run_summary\alerts_per_15min.png`
- `out\alert_run_summary\factor_hits_cumulative.png`
- `out\alert_run_summary\factor_totals.png`

If you use a custom `--output-dir`, the same files are written there instead.

## Common Files

- [scanner_config.py](/c:/Users/china/Desktop/12%20Week%20Plan/Algo%20trading/ibkr/Scanner-Alert/scanner_config.py): thresholds, ports, and scanner-level config
- [conditions.py](/c:/Users/china/Desktop/12%20Week%20Plan/Algo%20trading/ibkr/Scanner-Alert/conditions.py): preliminary alert conditions
- [alert_rating.py](/c:/Users/china/Desktop/12%20Week%20Plan/Algo%20trading/ibkr/Scanner-Alert/alert_rating.py): alert scoring and factor logic
- [tws_data_fetcher.py](/c:/Users/china/Desktop/12%20Week%20Plan/Algo%20trading/ibkr/Scanner-Alert/tws_data_fetcher.py): TWS connectivity and data subscriptions
- [execution_engine.py](/c:/Users/china/Desktop/12%20Week%20Plan/Algo%20trading/ibkr/Scanner-Alert/execution_engine.py): order execution and position management

## Troubleshooting

### Cannot connect to TWS

- Make sure TWS or IB Gateway is running
- Verify API access is enabled
- Verify port settings match

### No scanner updates

- Check your IBKR market-data subscriptions
- Confirm the symbols are receiving quotes in TWS
- Review [tws_errors.log](/c:/Users/china/Desktop/12%20Week%20Plan/Algo%20trading/ibkr/Scanner-Alert/tws_errors.log)

### No plots from `summarize_alert_run.py`

- Install `matplotlib` in the active environment
- Check the output folder printed by the script
- If using `--symbol`, also use `--output-dir` if you want a separate folder for that symbol

## Disclaimer

This project is for educational and research use. Automated trading is risky. Test in paper trading before using real money.

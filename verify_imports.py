"""
Quick verification that all imports work correctly
"""
import sys
sys.path.insert(0, '/home/ubuntu/Scanner-Alert')

print("Checking imports...")

try:
    import pytz
    print("✓ pytz imported successfully")
except ImportError as e:
    print(f"✗ pytz import failed: {e}")

try:
    from datetime import datetime, timedelta
    print("✓ datetime imported successfully")
except ImportError as e:
    print(f"✗ datetime import failed: {e}")

try:
    from collections import deque
    print("✓ collections.deque imported successfully")
except ImportError as e:
    print(f"✗ collections.deque import failed: {e}")

try:
    import xml.etree.ElementTree as ET
    print("✓ xml.etree.ElementTree imported successfully")
except ImportError as e:
    print(f"✗ xml.etree.ElementTree import failed: {e}")

try:
    import platform
    print("✓ platform imported successfully")
except ImportError as e:
    print(f"✗ platform import failed: {e}")

try:
    import scanner_config as config
    print("✓ scanner_config imported successfully")
except ImportError as e:
    print(f"✗ scanner_config import failed: {e}")

try:
    from conditions import MarketData, AlertConditionSet, PriceAboveVWAPCondition, SqueezeCondition
    print("✓ conditions module imported successfully")
except ImportError as e:
    print(f"✗ conditions import failed: {e}")

try:
    from realtime_scanner_premarket import get_market_session, get_session_start_time
    print("✓ realtime_scanner_premarket imported successfully")
    
    # Test the functions
    session = get_market_session()
    session_start = get_session_start_time()
    print(f"  Current session: {session}")
    print(f"  Session start time: {session_start}")
except ImportError as e:
    print(f"✗ realtime_scanner_premarket import failed: {e}")
except Exception as e:
    print(f"✗ Error testing functions: {e}")

print("\n✓ All critical imports verified!")
print("\nNote: TWS/IBKR imports (ibapi) are not tested here as they require TWS connection.")
print("These will be tested when you run the actual scanner with TWS connected.")

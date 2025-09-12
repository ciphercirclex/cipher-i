import analysechart_m
import updateorders
import MetaTrader5 as mt5
import time
from datetime import datetime, timedelta
import pytz
import json
import os

# Path configuration
MARKETS_JSON_PATH = r"C:\xampp\htdocs\CIPHER\cipher i\bouncestream\chart\base.json"

# Initialize global variables
MARKETS = []
TIMEFRAMES = []
LOGIN_ID = None
PASSWORD = None
SERVER = None
TERMINAL_PATH = None

# Function to load markets, timeframes, and credentials from JSON
def load_markets_and_timeframes(json_path):
    """Load MARKETS, TIMEFRAMES, and CREDENTIALS from base.json file."""
    global MARKETS, TIMEFRAMES, LOGIN_ID, PASSWORD, SERVER, TERMINAL_PATH
    try:
        if not os.path.exists(json_path):
            raise FileNotFoundError(f"Markets JSON file not found at: {json_path}")
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        # Load markets and timeframes
        MARKETS = data.get("MARKETS", [])
        TIMEFRAMES = data.get("TIMEFRAMES", [])
        if not MARKETS or not TIMEFRAMES:
            raise ValueError("MARKETS or TIMEFRAMES not found in base.json or are empty")
        
        # Load credentials
        credentials = data.get("CREDENTIALS", {})
        LOGIN_ID = credentials.get("LOGIN_ID", None)
        PASSWORD = credentials.get("PASSWORD", None)
        SERVER = credentials.get("SERVER", None)
        TERMINAL_PATH = credentials.get("TERMINAL_PATH", None)
        
        # Validate credentials
        if not all([LOGIN_ID, PASSWORD, SERVER, TERMINAL_PATH]):
            raise ValueError("One or more credentials (LOGIN_ID, PASSWORD, SERVER, TERMINAL_PATH) not found in base.json")
        
        print(f"Loaded MARKETS: {MARKETS}")
        print(f"Loaded TIMEFRAMES: {TIMEFRAMES}")
        print(f"Loaded CREDENTIALS: LOGIN_ID={LOGIN_ID}, SERVER={SERVER}, TERMINAL_PATH={TERMINAL_PATH}")
        return MARKETS, TIMEFRAMES
    except Exception as e:
        print(f"Error loading base.json: {e}")
        return [], []
# Load markets, timeframes, and credentials at startup
MARKETS, TIMEFRAMES = load_markets_and_timeframes(MARKETS_JSON_PATH)

def candletimeleft(market, timeframe, candle_time, min_time_left=3.1):
    # Initialize MT5
    print(f"[Process-{market}] Initializing MT5 for {market}")
    for attempt in range(3):
        if mt5.initialize(path=TERMINAL_PATH, timeout=60000):
            break
        print(f"[Process-{market}] Attempt {attempt + 1}/3: Failed to initialize MT5 terminal. Error: {mt5.last_error()}")
        time.sleep(5)
    else:
        print(f"[Process-{market}] Failed to initialize MT5 terminal after 3 attempts")
        return None, None
    for _ in range(5):
        if mt5.terminal_info() is not None:
            break
        print(f"[Process-{market}] Waiting for MT5 terminal to fully initialize...")
        time.sleep(2)
    else:
        print(f"[Process-{market}] MT5 terminal not ready")
        mt5.shutdown()
        return None, None
    for attempt in range(3):
        if mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=60000):
            print(f"[Process-{market}] Successfully logged in to MT5")
            break
        error_code, error_message = mt5.last_error()
        print(f"[Process-{market}] Attempt {attempt + 1}/3: Failed to log in to MT5. Error code: {error_code}, Message: {error_message}")
        time.sleep(5)
    else:
        print(f"[Process-{market}] Failed to log in to MT5 after 3 attempts")
        mt5.shutdown()
        return None, None

    try:
        # Fetch current candle
        if not mt5.symbol_select(market, True):
            print(f"[Process-{market}] Failed to select market: {market}, error: {mt5.last_error()}")
            return None, None
        while True:
            for attempt in range(3):
                candles = mt5.copy_rates_from_pos(market, mt5.TIMEFRAME_M5, 0, 1)
                if candles is None or len(candles) == 0:
                    print(f"[Process-{market}] Attempt {attempt + 1}/3: Failed to fetch candle data for {market} (M5), error: {mt5.last_error()}")
                    time.sleep(2)
                    continue
                current_time = datetime.now(pytz.UTC)
                candle_time_dt = datetime.fromtimestamp(candles[0]['time'], tz=pytz.UTC)
                if (current_time - candle_time_dt).total_seconds() > 6 * 60:  # 6 minutes for M5
                    print(f"[Process-{market}] Attempt {attempt + 1}/3: Candle for {market} (M5) is too old (time: {candle_time_dt})")
                    time.sleep(2)
                    continue
                candle_time = candles[0]['time']
                break
            else:
                print(f"[Process-{market}] Failed to fetch recent candle data for {market} (M5) after 3 attempts")
                return None, None

            if timeframe.upper() != "M5":
                print(f"[Process-{market}] Only M5 timeframe is supported, received {timeframe}")
                return None, None

            candle_datetime = datetime.fromtimestamp(candle_time, tz=pytz.UTC)
            minutes_per_candle = 5
            total_minutes = (candle_datetime.hour * 60 + candle_datetime.minute)
            remainder = total_minutes % minutes_per_candle
            last_candle_start = candle_datetime - timedelta(minutes=remainder, seconds=candle_datetime.second, microseconds=candle_datetime.microsecond)
            next_close_time = last_candle_start + timedelta(minutes=minutes_per_candle)
            current_time = datetime.now(pytz.UTC)
            time_left = (next_close_time - current_time).total_seconds() / 60.0
            if time_left <= 0:
                next_close_time += timedelta(minutes=minutes_per_candle)
                time_left = (next_close_time - current_time).total_seconds() / 60.0
            print(f"[Process-{market}] Candle time: {candle_datetime}, Next close: {next_close_time}, Time left: {time_left:.2f} minutes")
            
            if time_left > min_time_left:
                return time_left, next_close_time
            else:
                print(f"[Process-{market}] Time left ({time_left:.2f} minutes) is <= {min_time_left} minutes, returning None to restart sequence")
                return None, None
            
    finally:
        mt5.shutdown()
    
def run_analysechart_m1():
    """Run the analysechart_m script."""
    try:
        analysechart_m.main1()
        print("analysechart_m completed.")
    except Exception as e:
        print(f"Error in analysechart_m: {e}")
def run_analysechart_m2():
    """Run the analysechart_m script."""
    try:
        analysechart_m.main2()
        print("analysechart_m completed.")
    except Exception as e:
        print(f"Error in analysechart_m: {e}")


def run_updateorders():
    """Run the updateorders script."""
    try:
        updateorders.main()
        print("updateorders completed.")
    except Exception as e:
        print(f"Error in updateorders: {e}")
def fetchlotsizeandrisk():
    """Run the analysechart_m script."""
    try:
        updateorders.executefetchlotsizeandrisk()
        print("analysechart_m completed.")
    except Exception as e:
        print(f"Error in analysechart_m: {e}")

def execute(mode="loop"):
    """Execute the scripts sequentially with the specified mode: 'loop' or 'once'."""
    if mode not in ["loop", "once"]:
        raise ValueError("Invalid mode. Use 'loop' or 'once'.")
    
    # Verify that credentials and markets were loaded
    if not all([LOGIN_ID, PASSWORD, SERVER, TERMINAL_PATH]):
        print("Credentials not properly loaded from base.json. Exiting.")
        return
    if not MARKETS:
        print("No markets defined in MARKETS list. Exiting.")
        return
    
    def execute_orderfree_markets():
        def run_sequential():
            """Helper function to run analysechart_m and updateorders sequentially with candle time checks."""
            default_market = MARKETS[0]  # Use first market from MARKETS list
            timeframe = "M5"
            fetchlotsizeandrisk()
            
            while True:
                # First candle check before updateorders
                start_time = datetime.now(pytz.UTC)  # Capture system time at the start
                time_left, next_close_time = candletimeleft(default_market, timeframe, None)
                if time_left is None or next_close_time is None:
                    print(f"[Process-{default_market}] Insufficient time left for {default_market} (M5). Restarting sequence.")
                    time.sleep(5)  # Small delay before restarting
                    continue
                initial_time_left = time_left  # Store initial time left for operation time calculation
                print(f"[Process-{default_market}] Time left for M5 candle: {time_left:.2f} minutes. Running updateorders.")
                run_updateorders()  # Run updateorders first

                # Second candle check before analysechart_m
                time_left, next_close_time = candletimeleft(default_market, timeframe, None)
                if time_left is None or next_close_time is None:
                    print(f"[Process-{default_market}] Insufficient time left for {default_market} (M5). Restarting sequence.")
                    time.sleep(5)  # Small delay before restarting
                    continue
                print(f"[Process-{default_market}] Time left for M5 candle: {time_left:.2f} minutes. Running analysechart_m.")
                run_analysechart_m2()  # Run analysechart_m second

                # Third candle check before updateorders
                time_left, next_close_time = candletimeleft(default_market, timeframe, None)
                if time_left is None or next_close_time is None:
                    print(f"[Process-{default_market}] Insufficient time left for {default_market} (M5). Restarting sequence.")
                    time.sleep(5)  # Small delay before restarting
                    continue
                print(f"[Process-{default_market}] Time left for M5 candle: {time_left:.2f} minutes. Running updateorders.")
                run_updateorders()  # Run updateorders third

                print("Both scripts completed successfully.")
                return time_left, start_time, initial_time_left  # Return values for final print

        try:
            if mode == "loop":
                while True:
                    result = run_sequential()
                    if result:
                        return result  # Return result for aggregation
                    print("Restarting entire sequence...")
                    time.sleep(5)  # Small delay before restarting the loop
            else:  # mode == "once"
                return run_sequential()  # Return result for aggregation

        except Exception as e:
            print(f"Error in main loop: {e}")
            return None

    def execute_charts_identified():
        def run_sequential():
            """Helper function to run analysechart_m and updateorders sequentially with candle time checks."""
            default_market = MARKETS[0]  # Use first market from MARKETS list
            timeframe = "M5"
            
            while True:
                # First candle check before updateorders
                start_time = datetime.now(pytz.UTC)  # Capture system time at the start
                time_left, next_close_time = candletimeleft(default_market, timeframe, None)
                if time_left is None or next_close_time is None:
                    print(f"[Process-{default_market}] Insufficient time left for {default_market} (M5). Restarting sequence.")
                    time.sleep(5)  # Small delay before restarting
                    continue
                initial_time_left = time_left  # Store initial time left for operation time calculation
                print(f"[Process-{default_market}] Time left for M5 candle: {time_left:.2f} minutes. Running updateorders.")
                run_updateorders()  # Run updateorders first

                # Second candle check before analysechart_m
                time_left, next_close_time = candletimeleft(default_market, timeframe, None)
                if time_left is None or next_close_time is None:
                    print(f"[Process-{default_market}] Insufficient time left for {default_market} (M5). Restarting sequence.")
                    time.sleep(5)  # Small delay before restarting
                    continue
                print(f"[Process-{default_market}] Time left for M5 candle: {time_left:.2f} minutes. Running analysechart_m.")
                run_analysechart_m1()  # Run analysechart_m second

                # Third candle check before updateorders
                time_left, next_close_time = candletimeleft(default_market, timeframe, None)
                if time_left is None or next_close_time is None:
                    print(f"[Process-{default_market}] Insufficient time left for {default_market} (M5). Restarting sequence.")
                    time.sleep(5)  # Small delay before restarting
                    continue
                print(f"[Process-{default_market}] Time left for M5 candle: {time_left:.2f} minutes. Running updateorders.")
                run_updateorders()  # Run updateorders third

                print("Both scripts completed successfully.")
                return time_left, start_time, initial_time_left  # Return values for final print

        try:
            if mode == "loop":
                while True:
                    result = run_sequential()
                    if result:
                        return result  # Return result for aggregation
                    print("Restarting entire sequence...")
                    time.sleep(5)  # Small delay before restarting the loop
            else:  # mode == "once"
                return run_sequential()  # Return result for aggregation

        except Exception as e:
            print(f"Error in main loop: {e}")
            return None

    try:
        if mode == "loop":
            while True:
                # Execute both functions and collect results
                result_orderfree = execute_orderfree_markets()
                result_charts = execute_charts_identified()
                
                # Process results for timing calculation
                total_operation_time = 0
                earliest_start_time = None
                final_time_left = None
                
                if result_orderfree:
                    time_left_of, start_time_of, initial_time_left_of = result_orderfree
                    operation_time_of = initial_time_left_of - time_left_of
                    total_operation_time += operation_time_of
                    if earliest_start_time is None or start_time_of < earliest_start_time:
                        earliest_start_time = start_time_of
                    final_time_left = time_left_of
                
                if result_charts:
                    time_left_ci, start_time_ci, initial_time_left_ci = result_charts
                    operation_time_ci = initial_time_left_ci - time_left_ci
                    total_operation_time += operation_time_ci
                    if earliest_start_time is None or start_time_ci < earliest_start_time:
                        earliest_start_time = start_time_ci
                    final_time_left = time_left_ci if final_time_left is None else min(final_time_left, time_left_ci)
                
                # Print combined timing information
                if earliest_start_time and final_time_left is not None:
                    print(f"Started time of candle: {earliest_start_time}")
                    print(f"Candle time left: {final_time_left:.2f} minutes")
                    print(f"Total operated within time: {total_operation_time:.2f} minutes")
                
                print("Restarting entire sequence...")
                time.sleep(5)  # Small delay before restarting the loop
        else:  # mode == "once"
            # Execute both functions and collect results
            result_orderfree = execute_orderfree_markets()
            result_charts = execute_charts_identified()
            
            print("Execution completed (once mode).")
            
            # Process results for timing calculation
            total_operation_time = 0
            earliest_start_time = None
            final_time_left = None
            
            if result_orderfree:
                time_left_of, start_time_of, initial_time_left_of = result_orderfree
                operation_time_of = initial_time_left_of - time_left_of
                total_operation_time += operation_time_of
                if earliest_start_time is None or start_time_of < earliest_start_time:
                    earliest_start_time = start_time_of
                final_time_left = time_left_of
            
            if result_charts:
                time_left_ci, start_time_ci, initial_time_left_ci = result_charts
                operation_time_ci = initial_time_left_ci - time_left_ci
                total_operation_time += operation_time_ci
                if earliest_start_time is None or start_time_ci < earliest_start_time:
                    earliest_start_time = start_time_ci
                final_time_left = time_left_ci if final_time_left is None else min(final_time_left, time_left_ci)
            
            # Print combined timing information
            if earliest_start_time and final_time_left is not None:
                print(f"Started time of candle: {earliest_start_time}")
                print(f"Candle time left: {final_time_left:.2f} minutes")
                print(f"Total operated within time: {total_operation_time:.2f} minutes")

    except Exception as e:
        print(f"Error in main loop: {e}")
        
if __name__ == "__main__":
    # Example: Change to "once" or "loop" as needed
    execute(mode="once")
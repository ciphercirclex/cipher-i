import analysechart_m
import updateorders
import MetaTrader5 as mt5
import time
from datetime import datetime, timedelta
import pytz
import json
import os

# Path configuration
MARKETS_JSON_PATH = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\base.json"
BASE_PROCESSING_FOLDER = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\processing"
BACTHES_MARKETS_PATH = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\batches"

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

def check_and_save_verification_status(batchnumber=20):
    """Check verification.json for all markets and save the list of verified markets to allbatchmarkets.json, grouped by batches."""
    try:
        print("===== Checking Verification Status for All Markets =====")
        
        # Define the path for verification.json files
        FETCHCHART_DESTINATION_PATH = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\fetched"
        output_json_path = os.path.join(BACTHES_MARKETS_PATH, "allbatchmarkets.json")
        
        # Initialize list for markets with all timeframes verified
        markets_with_alltimeframes_verified = []
        
        # Required timeframes to check
        required_timeframes = ["m5", "m15", "m30", "h1", "h4"]
        
        # Check each market
        for market in MARKETS:
            market_folder_name = market.replace(" ", "_")
            verification_file = os.path.join(FETCHCHART_DESTINATION_PATH, market_folder_name, "verification.json")
            
            try:
                if not os.path.exists(verification_file):
                    print(f"Verification file not found for {market}: {verification_file}")
                    continue
                
                with open(verification_file, 'r') as f:
                    verification_data = json.load(f)
                
                # Check if all required timeframes are "chart_identified" and "all_timeframes" is "verified"
                all_timeframes_verified = all(
                    verification_data.get(tf) == "chart_identified" for tf in required_timeframes
                ) and verification_data.get("all_timeframes") == "verified"
                
                if all_timeframes_verified:
                    print(f"All timeframes in verification.json for {market} are 'chart_identified' and 'all_timeframes' is 'verified'")
                    markets_with_alltimeframes_verified.append(market)
                else:
                    print(f"Not all timeframes in verification.json for {market} are 'chart_identified' or 'all_timeframes' is not 'verified'")
            
            except Exception as e:
                print(f"Error reading verification.json for {market}: {e}")
                continue
        
        # Group markets into batches
        batches = {}
        for i in range(0, len(markets_with_alltimeframes_verified), batchnumber):
            batch_key = f"batch{i // batchnumber + 1}"
            batches[batch_key] = markets_with_alltimeframes_verified[i:i + batchnumber]
        
        # Prepare the output JSON
        result = {
            "status": "success",
            "message": "Verification check completed successfully",
            "batches": batches,
            "total_markets_verified": len(markets_with_alltimeframes_verified),
            "batch_size": batchnumber,
            "total_batches": len(batches),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Save the results to allbatchmarkets.json
        try:
            os.makedirs(BACTHES_MARKETS_PATH, exist_ok=True)
            with open(output_json_path, 'w') as f:
                json.dump(result, f, indent=4)
            print(f"Verification status saved to {output_json_path}")
        except Exception as e:
            print(f"Error saving verification status to {output_json_path}: {e}")
            result["save_status"] = "failed"
            result["save_message"] = f"Error saving to {output_json_path}: {str(e)}"
        
        # Print summary
        print("===== Verification Status Summary =====")
        print(f"Total markets checked: {len(MARKETS)}")
        print(f"Verified markets: {len(markets_with_alltimeframes_verified)}")
        print(f"Unverified markets: {len(MARKETS) - len(markets_with_alltimeframes_verified)}")
        print(f"Total batches created: {len(batches)} with batch size: {batchnumber}")
        for batch_key, batch_markets in batches.items():
            print(f"{batch_key}: {len(batch_markets)} markets - {batch_markets}")
        print("=====================================")
        
        return result
    
    except Exception as e:
        print(f"Unexpected error in check_and_save_verification_status: {e}")
        return {
            "status": "failed",
            "message": f"Unexpected error: {str(e)}",
            "batches": {},
            "total_markets_verified": 0,
            "batch_size": batchnumber,
            "total_batches": 0,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

def candletimeleft(market, timeframe, candle_time, min_time_left):
    """Generic function to calculate time left for a candle in the specified timeframe."""
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
                mt5_timeframe = mt5.TIMEFRAME_M15 if timeframe.upper() == "M15" else mt5.TIMEFRAME_M5
                max_age = 16 * 60 if timeframe.upper() == "M15" else 6 * 60  # Max age in seconds
                candles = mt5.copy_rates_from_pos(market, mt5_timeframe, 0, 1)
                if candles is None or len(candles) == 0:
                    print(f"[Process-{market}] Attempt {attempt + 1}/3: Failed to fetch candle data for {market} ({timeframe}), error: {mt5.last_error()}")
                    time.sleep(2)
                    continue
                current_time = datetime.now(pytz.UTC)
                candle_time_dt = datetime.fromtimestamp(candles[0]['time'], tz=pytz.UTC)
                if (current_time - candle_time_dt).total_seconds() > max_age:
                    print(f"[Process-{market}] Attempt {attempt + 1}/3: Candle for {market} ({timeframe}) is too old (time: {candle_time_dt})")
                    time.sleep(2)
                    continue
                candle_time = candles[0]['time']
                break
            else:
                print(f"[Process-{market}] Failed to fetch recent candle data for {market} ({timeframe}) after 3 attempts")
                return None, None

            if timeframe.upper() not in ["M5", "M15"]:
                print(f"[Process-{market}] Only M5 and M15 timeframes are supported, received {timeframe}")
                return None, None

            candle_datetime = datetime.fromtimestamp(candle_time, tz=pytz.UTC)
            minutes_per_candle = 15 if timeframe.upper() == "M15" else 5
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
    """Run the analysechart_m script for M15 timeframe."""
    try:
        analysechart_m.main()
        print("analysechart_m (M15) completed.")
    except Exception as e:
        print(f"Error in analysechart_m (M15): {e}")

def run_analysechart_m2():
    """Run the analysechart_m script for M5 timeframe."""
    try:
        analysechart_m.process_5minutes_timeframe()
        print("analysechart_m (M5) completed.")
    except Exception as e:
        print(f"Error in analysechart_m (M5): {e}")

def run_updateorders():
    """Run the updateorders script for M15 timeframe."""
    try:
        updateorders.main()
        print("updateorders (M15) completed.")
    except Exception as e:
        print(f"Error in updateorders (M15): {e}")

def run_updateorders2():
    """Run the updateorders script for M5 timeframe."""
    try:
        updateorders.process_5minutes_timeframe()
        print("updateorders (M5) completed.")
    except Exception as e:
        print(f"Error in updateorders (M5): {e}")

def run_updateorders3():
    """Run the updateorders script for M5 timeframe."""
    try:
        updateorders.lockpendingorders()
        print("updateorders (M5) completed.")
    except Exception as e:
        print(f"Error in updateorders (M5): {e}")

def fetchlotsizeandrisk():
    """Run the fetchlotsizeandrisk function from updateorders."""
    try:
        updateorders.executefetchlotsizeandrisk()
        print("fetchlotsizeandrisk completed.")
    except Exception as e:
        print(f"Error in fetchlotsizeandrisk: {e}")

def insertpendingorderstodb():
    """Run the fetchlotsizeandrisk function from updateorders."""
    try:
        updateorders.executeinsertpendingorderstodb()
        print("fetchlotsizeandrisk completed.")
    except Exception as e:
        print(f"Error in fetchlotsizeandrisk: {e}")

def write_batch_to_json(batch_key, batch_markets):
    """Write a single batch to batchbybatch.json."""
    try:
        batch_json_path = os.path.join(BACTHES_MARKETS_PATH, "batchbybatch.json")
        batch_data = {
            "status": "success",
            "message": f"Batch {batch_key} written successfully",
            "current_batch": batch_key,
            "markets": batch_markets,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        os.makedirs(BACTHES_MARKETS_PATH, exist_ok=True)
        with open(batch_json_path, 'w') as f:
            json.dump(batch_data, f, indent=4)
        print(f"Batch {batch_key} written to {batch_json_path}")
    except Exception as e:
        print(f"Error writing batch {batch_key} to batchbybatch.json: {e}")

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
    
    # Check verification status for all markets and save to allbatchmarkets.json
    verification_result = check_and_save_verification_status(batchnumber=60)
    if verification_result.get("status") == "failed":
        print(f"Failed to check verification status: {verification_result.get('message', 'Unknown error')}")
        return
    
    # Check if there are any verified markets
    verified_markets_count = verification_result.get("total_markets_verified", 0)
    if verified_markets_count == 0:
        print("No markets have all timeframes verified. Exiting to prevent processing unverified markets.")
        return
    else:
        print(f"Found {verified_markets_count} verified markets across {verification_result.get('total_batches', 0)} batches. Proceeding with execution.")

    fetchlotsizeandrisk()
    
    # Load batches from allbatchmarkets.json
    all_batches_path = os.path.join(BACTHES_MARKETS_PATH, "allbatchmarkets.json")
    try:
        with open(all_batches_path, 'r') as f:
            all_batches_data = json.load(f)
        batches = all_batches_data.get("batches", {})
        total_batches = all_batches_data.get("total_batches", 0)
    except Exception as e:
        print(f"Error reading allbatchmarkets.json: {e}")
        return

    # Initialize variables to track overall execution times
    overall_start_time_ci = None
    overall_start_time_5m = None
    overall_end_time_ci = None
    overall_end_time_5m = None

    def execute_charts_identified(): 
        """Helper function to run analysechart_m and updateorders sequentially for M15 timeframe."""
        default_market = MARKETS[0]  # Use first market from MARKETS list
        timeframe = "M15"
        
        while True:
            # First candle check before updateorders
            start_time = datetime.now(pytz.UTC)
            time_left, next_close_time = candletimeleft(default_market, timeframe, None, min_time_left=8)
            if time_left is None or next_close_time is None:
                print(f"[Process-{default_market}] Insufficient time left for {default_market} (M15). Restarting sequence.")
                time.sleep(5)
                continue
            initial_time_left = time_left
            print(f"[Process-{default_market}] Time left for M15 candle: {time_left:.2f} minutes. Running updateorders.")
            run_updateorders()

            # Second candle check before analysechart_m
            time_left, next_close_time = candletimeleft(default_market, timeframe, None, min_time_left=5)
            if time_left is None or next_close_time is None:
                print(f"[Process-{default_market}] Insufficient time left for {default_market} (M15). Restarting sequence.")
                time.sleep(5)
                continue
            print(f"[Process-{default_market}] Time left for M15 candle: {time_left:.2f} minutes. Running analysechart_m.")
            run_analysechart_m1()

            # Third candle check before updateorders
            time_left, next_close_time = candletimeleft(default_market, timeframe, None, min_time_left=4)
            if time_left is None or next_close_time is None:
                print(f"[Process-{default_market}] Insufficient time left for {default_market} (M15). Restarting sequence.")
                time.sleep(5)
                continue
            print(f"[Process-{default_market}] Time left for M15 candle: {time_left:.2f} minutes. Running updateorders.")
            run_updateorders()

            print("Charts identified (M15) completed successfully.")
            return time_left, start_time, initial_time_left, datetime.now(pytz.UTC)

    def execute_5minutes_markets():
        """Helper function to run analysechart_m and updateorders sequentially for M5 timeframe."""
        default_market = MARKETS[0]  # Use first market from MARKETS list
        timeframe = "M5"
        
        while True:
            # First candle check before updateorders
            start_time = datetime.now(pytz.UTC)
            time_left, next_close_time = candletimeleft(default_market, timeframe, None, min_time_left=4.1)
            if time_left is None or next_close_time is None:
                print(f"[Process-{default_market}] Insufficient time left for {default_market} (M5). Restarting sequence.")
                time.sleep(5)
                continue
            initial_time_left = time_left
            print(f"[Process-{default_market}] Time left for M5 candle: {time_left:.2f} minutes. Running updateorders.")
            run_updateorders2()

            # Second candle check before analysechart_m
            time_left, next_close_time = candletimeleft(default_market, timeframe, None, min_time_left=1)
            if time_left is None or next_close_time is None:
                print(f"[Process-{default_market}] Insufficient time left for {default_market} (M5). Restarting sequence.")
                time.sleep(5)
                continue
            print(f"[Process-{default_market}] Time left for M5 candle: {time_left:.2f} minutes. Running analysechart_m.")
            run_analysechart_m2()

            # Third candle check before updateorders
            time_left, next_close_time = candletimeleft(default_market, timeframe, None, min_time_left=0.5)
            if time_left is None or next_close_time is None:
                print(f"[Process-{default_market}] Insufficient time left for {default_market} (M5). Restarting sequence.")
                time.sleep(5)
                continue
            print(f"[Process-{default_market}] Time left for M5 candle: {time_left:.2f} minutes. Running updateorders.")
            run_updateorders2()
            run_updateorders3()
            insertpendingorderstodb()

            print("5 minutes markets (M5) completed successfully.")
            return time_left, start_time, initial_time_left, datetime.now(pytz.UTC)

    try:
        # Process each batch sequentially
        for batch_index in range(1, total_batches + 1):
            batch_key = f"batch{batch_index}"
            batch_markets = batches.get(batch_key, [])
            if not batch_markets:
                print(f"No markets found for {batch_key}. Skipping.")
                continue
            
            print(f"\n=== Processing {batch_key} ===")
            # Write current batch to batchbybatch.json
            write_batch_to_json(batch_key, batch_markets)
            
            # Execute both functions and collect results
            result_charts = execute_charts_identified()
            result_5min = execute_5minutes_markets()
            
            # Process results for output
            if result_charts and result_5min:
                time_left_ci, start_time_ci, initial_time_left_ci, end_time_ci = result_charts
                time_left_5m, start_time_5m, initial_time_left_5m, end_time_5m = result_5min
                
                # Update overall start and end times
                if overall_start_time_ci is None:
                    overall_start_time_ci = start_time_ci
                if overall_start_time_5m is None:
                    overall_start_time_5m = start_time_5m
                overall_end_time_ci = end_time_ci
                overall_end_time_5m = end_time_5m
                
                # Print batch-specific output
                print(f"\nbatch {batch_index}")
                print("Chart Identifier (M15):")
                print(f"Start time for chart identifier: {start_time_ci}")
                print(f"Remaining time: {time_left_ci:.2f} minutes")
                print(f"Chart identifier operated within: {(initial_time_left_ci - time_left_ci):.2f} minutes")
                print("\n5 Minutes Markets (M5):")
                print(f"Start time for 5 minutes markets: {start_time_5m}")
                print(f"Remaining time: {time_left_5m:.2f} minutes")
                print(f"5 minutes markets operated within: {(initial_time_left_5m - time_left_5m):.2f} minutes")
                print("\n")
                
            print(f"Completed {batch_key}. Moving to next batch...")
            time.sleep(5)
        
        # Print overall summary for all batches
        if overall_start_time_ci and overall_end_time_ci and overall_start_time_5m and overall_end_time_5m:
            print("\nall batch")
            print("Chart Identifier (M15):")
            print(f"Start time for chart identifier: {overall_start_time_ci}")
            print(f"Chart identifier operated within: {((overall_end_time_ci - overall_start_time_ci).total_seconds() / 60.0):.2f} minutes")
            print("\n5 Minutes Markets (M5):")
            print(f"Start time for 5 minutes markets: {overall_start_time_5m}")
            print(f"5 minutes markets operated within: {((overall_end_time_5m - overall_start_time_5m).total_seconds() / 60.0):.2f} minutes")
            print("\n")
        
        print(f"All {total_batches} batches processed. Script execution completed.")
        
        # Empty allbatchmarkets.json and batchbybatch.json
        try:
            all_batches_path = os.path.join(BACTHES_MARKETS_PATH, "allbatchmarkets.json")
            batch_json_path = os.path.join(BACTHES_MARKETS_PATH, "batchbybatch.json")
            empty_data = {
                "status": "cleared",
                "message": "File cleared after execution",
                "batches": {},
                "total_markets_verified": 0,
                "batch_size": 0,
                "total_batches": 0,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            with open(all_batches_path, 'w') as f:
                json.dump(empty_data, f, indent=4)
            print(f"Cleared {all_batches_path}")
            with open(batch_json_path, 'w') as f:
                json.dump({
                    "status": "cleared",
                    "message": "File cleared after execution",
                    "current_batch": "",
                    "markets": [],
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                }, f, indent=4)
            print(f"Cleared {batch_json_path}")
        except Exception as e:
            print(f"Error clearing JSON files: {e}")
        
    except Exception as e:
        print(f"Error in main loop: {e}")

if __name__ == "__main__":
    execute(mode="loop")
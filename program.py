import analysechart_m
import updateorders
import MetaTrader5 as mt5
import time
from datetime import datetime, timedelta
import pytz
import json
import os
import connectwithinfinitydb as db

TIMEFRAME_MAPPING = {
    "M5": mt5.TIMEFRAME_M5,
    "M15": mt5.TIMEFRAME_M15,
    "M30": mt5.TIMEFRAME_M30,
    "H1": mt5.TIMEFRAME_H1,
    "H4": mt5.TIMEFRAME_H4
}
# Add this mapping at the top of updateorders.py, near TIMEFRAME_MAPPING
DB_TIMEFRAME_MAPPING = {
    "M5": "5minutes",
    "M15": "15minutes",
    "M30": "30minutes",
    "H1": "1Hour",
    "H4": "4Hour"
}
MAX_RETRIES = 5
RETRY_DELAY = 3

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
        print("insertion of pendingorders  completed.")
    except Exception as e:
        print(f"Error in inserting pendingorders: {e}")

def orders_status():
    def get_activemarket_signals() -> bool:
        """Fetch all records from cipherbouncestream_signals, calculate days old for each record, and save to activemarketsignals.json."""
        print("Fetching all records from cipherbouncestream_signals, calculating days old, and saving to activemarketsignals.json", "INFO")
        
        # Initialize error log list
        error_log = []
        activemarketsignals_path = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\batches\activemarketsignals.json"
        error_json_path = os.path.join(BASE_PROCESSING_FOLDER, "getactivemarketsignalserror.json")
        
        # Helper function to save errors to JSON
        def save_errors():
            try:
                with open(error_json_path, 'w') as f:
                    json.dump(error_log, f, indent=4)
                print(f"Errors saved to {error_json_path}", "INFO")
            except Exception as e:
                print(f"Failed to save errors to {error_json_path}: {str(e)}", "ERROR")
        
        # Get current date in Africa/Lagos timezone
        current_date = datetime.now(pytz.timezone('Africa/Lagos')).date()
        
        # Helper function to calculate days old
        def calculate_days_old(created_at: str) -> int:
            try:
                # Extract date from created_at (format: YYYY-MM-DD HH:MM:SS)
                created_date = datetime.strptime(created_at.split(' ')[0], '%Y-%m-%d').date()
                # Calculate difference in days
                days_old = (current_date - created_date).days
                return days_old
            except Exception as e:
                print(f"Error calculating days old for created_at {created_at}: {str(e)}", "ERROR")
                return 0  # Default to 0 if parsing fails
        
        # Fetch all records from cipherbouncestream_signals (active table)
        try:
            fetch_active_query = """
                SELECT pair, timeframe, order_type, entry_price, exit_price,
                    ratio_0_5_price, ratio_1_price, ratio_2_price, profit_price, created_at
                FROM cipherbouncestream_signals
                ORDER BY pair ASC, created_at ASC
            """
            active_result = db.execute_query(fetch_active_query)
            print(f"Raw query result for fetching all records from active table: {json.dumps(active_result, indent=2)}", "DEBUG")
            
            active_pairs = []
            if isinstance(active_result, dict):
                if active_result.get('status') != 'success':
                    error_log.append({
                        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                        "error": f"Active table query failed: {active_result.get('message', 'No message provided')}"
                    })
                    save_errors()
                    print(f"Active table query failed: {active_result.get('message', 'No message provided')}", "ERROR")
                    return False
                rows = active_result.get('data', {}).get('rows', []) or active_result.get('results', [])
                active_pairs = [
                    {
                        "market": row['pair'],
                        "timeframe": row['timeframe'],
                        "order_type": row['order_type'],
                        "entry_price": float(row['entry_price']),
                        "exit_price": float(row['exit_price']),
                        "ratio_0_5_price": float(row['ratio_0_5_price']),
                        "ratio_1_price": float(row['ratio_1_price']),
                        "ratio_2_price": float(row['ratio_2_price']),
                        "profit_price": float(row['profit_price']),
                        "old_range": row['created_at'],
                        "days_old": calculate_days_old(row['created_at'])
                    }
                    for row in rows if all(row.get(key) is not None for key in [
                        'pair', 'timeframe', 'order_type', 'entry_price', 'exit_price',
                        'ratio_0_5_price', 'ratio_1_price', 'ratio_2_price', 'profit_price', 'created_at'
                    ])
                ]
            elif isinstance(active_result, list):
                active_pairs = [
                    {
                        "market": row['pair'],
                        "timeframe": row['timeframe'],
                        "order_type": row['order_type'],
                        "entry_price": float(row['entry_price']),
                        "exit_price": float(row['exit_price']),
                        "ratio_0_5_price": float(row['ratio_0_5_price']),
                        "ratio_1_price": float(row['ratio_1_price']),
                        "ratio_2_price": float(row['ratio_2_price']),
                        "profit_price": float(row['profit_price']),
                        "old_range": row['created_at'],
                        "days_old": calculate_days_old(row['created_at'])
                    }
                    for row in active_result if all(row.get(key) is not None for key in [
                        'pair', 'timeframe', 'order_type', 'entry_price', 'exit_price',
                        'ratio_0_5_price', 'ratio_1_price', 'ratio_2_price', 'profit_price', 'created_at'
                    ])
                ]
            else:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Invalid result format for active table: Expected dict or list, got {type(active_result)}"
                })
                save_errors()
                print(f"Invalid result format for active table: Expected dict or list, got {type(active_result)}", "ERROR")
                return False
            
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Error fetching records from active table: {str(e)}"
            })
            save_errors()
            print(f"Error fetching records from active table: {str(e)}", "ERROR")
            return False
        
        # Save all records to activemarketsignals.json
        activemarketsignals_data = {
            "MARKETS": active_pairs
        }
        try:
            os.makedirs(os.path.dirname(activemarketsignals_path), exist_ok=True)
            with open(activemarketsignals_path, 'w') as f:
                json.dump(activemarketsignals_data, f, indent=4)
            print(f"Successfully saved {len(active_pairs)} records to {activemarketsignals_path}", "SUCCESS")
            return True
        
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Failed to save records to {activemarketsignals_path}: {str(e)}"
            })
            save_errors()
            print(f"Failed to save records to {activemarketsignals_path}: {str(e)}", "ERROR")
            return False
    
    def get_processed_markets() -> bool:
        """Fetch all fields for unique pairs with their oldest created_at from cipher_processed_bouncestreamsignals and cipherbouncestream_signals,
        remove overlapping pairs from processed table, calculate days old for each pair, and save to processedmarkets.json and activemarkets.json."""
        print("Fetching all fields for unique pairs with oldest created_at from cipher_processed_bouncestreamsignals and cipherbouncestream_signals, "
            "removing overlapping pairs from processed table, calculating days old, saving to processedmarkets.json and activemarkets.json", "INFO")
        
        # Initialize error log list
        error_log = []
        nextbatch_path = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\batches\processedmarkets.json"
        activemarkets_path = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\batches\activemarkets.json"
        error_json_path = os.path.join(BASE_PROCESSING_FOLDER, "getprocessedmarketserror.json")
        
        # Helper function to save errors to JSON
        def save_errors():
            try:
                with open(error_json_path, 'w') as f:
                    json.dump(error_log, f, indent=4)
                print(f"Errors saved to {error_json_path}", "INFO")
            except Exception as e:
                print(f"Failed to save errors to {error_json_path}: {str(e)}", "ERROR")
        
        # Get current date in Africa/Lagos timezone
        current_date = datetime.now(pytz.timezone('Africa/Lagos')).date()
        
        # Helper function to calculate days old
        def calculate_days_old(created_at: str) -> int:
            try:
                # Extract date from created_at (format: YYYY-MM-DD HH:MM:SS)
                created_date = datetime.strptime(created_at.split(' ')[0], '%Y-%m-%d').date()
                # Calculate difference in days
                days_old = (current_date - created_date).days
                return days_old
            except Exception as e:
                print(f"Error calculating days old for created_at {created_at}: {str(e)}", "ERROR")
                return 0  # Default to 0 if parsing fails
        
        # Fetch all fields for unique pairs with oldest created_at from cipherbouncestream_signals (active table)
        try:
            fetch_active_query = """
                SELECT pair, timeframe, order_type, entry_price, exit_price,
                    ratio_0_5_price, ratio_1_price, ratio_2_price, profit_price, MIN(created_at) AS created_at
                FROM cipherbouncestream_signals
                GROUP BY pair, timeframe, order_type, entry_price, exit_price,
                        ratio_0_5_price, ratio_1_price, ratio_2_price, profit_price
                ORDER BY pair ASC, created_at ASC
            """
            active_result = db.execute_query(fetch_active_query)
            print(f"Raw query result for fetching unique pairs from active table: {json.dumps(active_result, indent=2)}", "DEBUG")
            
            active_pairs = []
            if isinstance(active_result, dict):
                if active_result.get('status') != 'success':
                    error_log.append({
                        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                        "error": f"Active table query failed: {active_result.get('message', 'No message provided')}"
                    })
                    save_errors()
                    print(f"Active table query failed: {active_result.get('message', 'No message provided')}", "ERROR")
                    return False
                rows = active_result.get('data', {}).get('rows', []) or active_result.get('results', [])
                active_pairs = [
                    {
                        "market": row['pair'],
                        "timeframe": row['timeframe'],
                        "order_type": row['order_type'],
                        "entry_price": float(row['entry_price']),
                        "exit_price": float(row['exit_price']),
                        "ratio_0_5_price": float(row['ratio_0_5_price']),
                        "ratio_1_price": float(row['ratio_1_price']),
                        "ratio_2_price": float(row['ratio_2_price']),
                        "profit_price": float(row['profit_price']),
                        "old_range": row['created_at'],
                        "days_old": calculate_days_old(row['created_at'])
                    }
                    for row in rows if all(row.get(key) is not None for key in [
                        'pair', 'timeframe', 'order_type', 'entry_price', 'exit_price',
                        'ratio_0_5_price', 'ratio_1_price', 'ratio_2_price', 'profit_price', 'created_at'
                    ])
                ]
            elif isinstance(active_result, list):
                active_pairs = [
                    {
                        "market": row['pair'],
                        "timeframe": row['timeframe'],
                        "order_type": row['order_type'],
                        "entry_price": float(row['entry_price']),
                        "exit_price": float(row['exit_price']),
                        "ratio_0_5_price": float(row['ratio_0_5_price']),
                        "ratio_1_price": float(row['ratio_1_price']),
                        "ratio_2_price": float(row['ratio_2_price']),
                        "profit_price": float(row['profit_price']),
                        "old_range": row['created_at'],
                        "days_old": calculate_days_old(row['created_at'])
                    }
                    for row in active_result if all(row.get(key) is not None for key in [
                        'pair', 'timeframe', 'order_type', 'entry_price', 'exit_price',
                        'ratio_0_5_price', 'ratio_1_price', 'ratio_2_price', 'profit_price', 'created_at'
                    ])
                ]
            else:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Invalid result format for active table: Expected dict or list, got {type(active_result)}"
                })
                save_errors()
                print(f"Invalid result format for active table: Expected dict or list, got {type(active_result)}", "ERROR")
                return False
            
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Error fetching unique pairs from active table: {str(e)}"
            })
            save_errors()
            print(f"Error fetching unique pairs from active table: {str(e)}", "ERROR")
            return False
        
        # Fetch all fields for unique pairs with oldest created_at from cipher_processed_bouncestreamsignals (processed table)
        try:
            fetch_processed_query = """
                SELECT pair, timeframe, order_type, entry_price, exit_price,
                    ratio_0_5_price, ratio_1_price, ratio_2_price, profit_price, message, MIN(created_at) AS created_at
                FROM cipher_processed_bouncestreamsignals
                GROUP BY pair, timeframe, order_type, entry_price, exit_price,
                        ratio_0_5_price, ratio_1_price, ratio_2_price, profit_price, message
                ORDER BY pair ASC, created_at ASC
            """
            processed_result = db.execute_query(fetch_processed_query)
            print(f"Raw query result for fetching unique pairs from processed table: {json.dumps(processed_result, indent=2)}", "DEBUG")
            
            processed_pairs = []
            if isinstance(processed_result, dict):
                if processed_result.get('status') != 'success':
                    error_log.append({
                        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                        "error": f"Processed table query failed: {processed_result.get('message', 'No message provided')}"
                    })
                    save_errors()
                    print(f"Processed table query failed: {processed_result.get('message', 'No message provided')}", "ERROR")
                    return False
                rows = processed_result.get('data', {}).get('rows', []) or processed_result.get('results', [])
                processed_pairs = [
                    {
                        "market": row['pair'],
                        "timeframe": row['timeframe'],
                        "order_type": row['order_type'],
                        "entry_price": float(row['entry_price']),
                        "exit_price": float(row['exit_price']),
                        "ratio_0_5_price": float(row['ratio_0_5_price']),
                        "ratio_1_price": float(row['ratio_1_price']),
                        "ratio_2_price": float(row['ratio_2_price']),
                        "profit_price": float(row['profit_price']),
                        "message": row['message'] if row['message'] is not None else "N/A",
                        "old_range": row['created_at'],
                        "days_old": calculate_days_old(row['created_at'])
                    }
                    for row in rows if all(row.get(key) is not None for key in [
                        'pair', 'timeframe', 'order_type', 'entry_price', 'exit_price',
                        'ratio_0_5_price', 'ratio_1_price', 'ratio_2_price', 'profit_price', 'created_at'
                    ])
                ]
            elif isinstance(processed_result, list):
                processed_pairs = [
                    {
                        "market": row['pair'],
                        "timeframe": row['timeframe'],
                        "order_type": row['order_type'],
                        "entry_price": float(row['entry_price']),
                        "exit_price": float(row['exit_price']),
                        "ratio_0_5_price": float(row['ratio_0_5_price']),
                        "ratio_1_price": float(row['ratio_1_price']),
                        "ratio_2_price": float(row['ratio_2_price']),
                        "profit_price": float(row['profit_price']),
                        "message": row['message'] if row['message'] is not None else "N/A",
                        "old_range": row['created_at'],
                        "days_old": calculate_days_old(row['created_at'])
                    }
                    for row in processed_result if all(row.get(key) is not None for key in [
                        'pair', 'timeframe', 'order_type', 'entry_price', 'exit_price',
                        'ratio_0_5_price', 'ratio_1_price', 'ratio_2_price', 'profit_price', 'created_at'
                    ])
                ]
            else:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Invalid result format for processed table: Expected dict or list, got {type(processed_result)}"
                })
                save_errors()
                print(f"Invalid result format for processed table: Expected dict or list, got {type(processed_result)}", "ERROR")
                return False
            
            # Identify overlapping pairs (present in both active and processed tables)
            active_pair_names = [pair['market'] for pair in active_pairs]
            overlapping_pairs = [pair['market'] for pair in processed_pairs if pair['market'] in active_pair_names]
            
            # Delete overlapping pairs from cipher_processed_bouncestreamsignals
            if overlapping_pairs:
                try:
                    DELETE_BATCH_SIZE = 80
                    for i in range(0, len(overlapping_pairs), DELETE_BATCH_SIZE):
                        batch = overlapping_pairs[i:i + DELETE_BATCH_SIZE]
                        batch_number = i // DELETE_BATCH_SIZE + 1
                        # Escape single quotes in pair names
                        escaped_pairs = [pair.replace("'", "''") for pair in batch]
                        delete_query = f"""
                            DELETE FROM cipher_processed_bouncestreamsignals
                            WHERE pair IN ({','.join(f"'{pair}'" for pair in escaped_pairs)})
                        """
                        result = db.execute_query(delete_query)
                        print(f"Raw query result for deleting batch {batch_number} from processed table: {json.dumps(result, indent=2)}", "DEBUG")
                        
                        if isinstance(result, dict):
                            if result.get('status') != 'success':
                                error_log.append({
                                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                                    "error": f"Delete query failed for batch {batch_number}: {result.get('message', 'No message provided')}"
                                })
                                save_errors()
                                print(f"Delete query failed for batch {batch_number}: {result.get('message', 'No message provided')}", "ERROR")
                                return False
                            affected_rows = result.get('results', {}).get('affected_rows', 0)
                            print(f"Successfully deleted {affected_rows} overlapping pairs from cipher_processed_bouncestreamsignals in batch {batch_number}", "SUCCESS")
                        else:
                            error_log.append({
                                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                                "error": f"Invalid result format for delete query in batch {batch_number}: Expected dict, got {type(result)}"
                            })
                            save_errors()
                            print(f"Invalid result format for delete query in batch {batch_number}: Expected dict, got {type(result)}", "ERROR")
                            return False
                    
                except Exception as e:
                    error_log.append({
                        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                        "error": f"Error deleting overlapping pairs from processed table: {str(e)}"
                    })
                    save_errors()
                    print(f"Error deleting overlapping pairs from processed table: {str(e)}", "ERROR")
                    return False
            
            # Update processed_pairs to exclude overlapping pairs
            processed_pairs = [pair for pair in processed_pairs if pair['market'] not in active_pair_names]
            
            # Save processed pairs to processedmarkets.json
            nextbatch_data = {
                "MARKETS": processed_pairs
            }
            try:
                os.makedirs(os.path.dirname(nextbatch_path), exist_ok=True)
                with open(nextbatch_path, 'w') as f:
                    json.dump(nextbatch_data, f, indent=4)
                print(f"Successfully saved {len(processed_pairs)} unique pairs to {nextbatch_path}", "SUCCESS")
            except Exception as e:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Failed to save pairs to {nextbatch_path}: {str(e)}"
                })
                save_errors()
                print(f"Failed to save pairs to {nextbatch_path}: {str(e)}", "ERROR")
                return False
                
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Error fetching unique pairs from processed table: {str(e)}"
            })
            save_errors()
            print(f"Error fetching unique pairs from processed table: {str(e)}", "ERROR")
            return False
        
        # Save active pairs to activemarkets.json
        activemarkets_data = {
            "MARKETS": active_pairs
        }
        try:
            os.makedirs(os.path.dirname(activemarkets_path), exist_ok=True)
            with open(activemarkets_path, 'w') as f:
                json.dump(activemarkets_data, f, indent=4)
            print(f"Successfully saved {len(active_pairs)} unique pairs to {activemarkets_path}", "SUCCESS")
            return True
        
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Failed to save pairs to {activemarkets_path}: {str(e)}"
            })
            save_errors()
            print(f"Failed to save pairs to {activemarkets_path}: {str(e)}", "ERROR")
            return False      
 
    def filter_oldest_markets() -> bool:
        """Filter markets with days_old >= 4 from activemarketsignals.json and save to activeoldestsignals.json."""
        print("Filtering markets with days_old >= 4 from activemarketsignals.json and saving to activeoldestsignals.json", "INFO")
        
        # Initialize error log list
        error_log = []
        activemarketsignals_path = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\batches\activemarketsignals.json"
        activeoldestsignals_path = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\batches\activeoldestsignals.json"
        error_json_path = os.path.join(BASE_PROCESSING_FOLDER, "filteroldestmarketserror.json")
        
        # Helper function to save errors to JSON
        def save_errors():
            try:
                with open(error_json_path, 'w') as f:
                    json.dump(error_log, f, indent=4)
                print(f"Errors saved to {error_json_path}", "INFO")
            except Exception as e:
                print(f"Failed to save errors to {error_json_path}: {str(e)}", "ERROR")
        
        # Read activemarketsignals.json
        try:
            with open(activemarketsignals_path, 'r') as f:
                data = json.load(f)
            print(f"Successfully read {activemarketsignals_path}", "INFO")
            
            # Filter markets where days_old >= 4, preserving all fields
            oldest_markets = [
                market for market in data.get('MARKETS', [])
                if market.get('days_old', 0) >= 4 and all(key in market for key in [
                    'market', 'timeframe', 'order_type', 'entry_price', 'exit_price',
                    'ratio_0_5_price', 'ratio_1_price', 'ratio_2_price', 'profit_price', 'old_range', 'days_old'
                ])
            ]
            print(f"Filtered {len(oldest_markets)} markets with days_old >= 4", "INFO")
            
            # Save filtered markets to activeoldestsignals.json
            activeoldestsignals_data = {
                "MARKETS": oldest_markets
            }
            try:
                os.makedirs(os.path.dirname(activeoldestsignals_path), exist_ok=True)
                with open(activeoldestsignals_path, 'w') as f:
                    json.dump(activeoldestsignals_data, f, indent=4)
                print(f"Successfully saved {len(oldest_markets)} records to {activeoldestsignals_path}", "SUCCESS")
                return True
            except Exception as e:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Failed to save records to {activeoldestsignals_path}: {str(e)}"
                })
                save_errors()
                print(f"Failed to save records to {activeoldestsignals_path}: {str(e)}", "ERROR")
                return False
        
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Failed to read {activemarketsignals_path}: {str(e)}"
            })
            save_errors()
            print(f"Failed to read {activemarketsignals_path}: {str(e)}", "ERROR")
            return False
        
    def insertoldestsignalstodb() -> bool:
        """Insert oldest signals from activeoldestsignals.json into cipher_processed_bouncestreamsignals table, removing duplicates."""
        json_path = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\batches\activeoldestsignals.json"
        print("Inserting oldest signals into cipher_processed_bouncestreamsignals table", "INFO")
        
        # Initialize error log list
        error_log = []
        
        # Define error log file path
        error_json_path = os.path.join(BASE_PROCESSING_FOLDER, "insertoldestsignalserror.json")
        
        # Helper function to save errors to JSON
        def save_errors():
            try:
                with open(error_json_path, 'w') as f:
                    json.dump(error_log, f, indent=4)
                print(f"Errors saved to {error_json_path}", "INFO")
            except Exception as e:
                print(f"Failed to save errors to {error_json_path}: {str(e)}", "ERROR")
        
        # Load oldest signals from JSON
        try:
            if not os.path.exists(json_path):
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Oldest signals JSON file not found at: {json_path}"
                })
                save_errors()
                print(f"Oldest signals JSON file not found at: {json_path}", "ERROR")
                return False
            
            with open(json_path, 'r') as f:
                data = json.load(f)
            
            signals = data.get('MARKETS', [])
            if not signals:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": "No signals found in activeoldestsignals.json or 'MARKETS' key is missing"
                })
                save_errors()
                print("No signals found in activeoldestsignals.json or 'MARKETS' key is missing", "INFO")
                return True  # No signals to process, but not an error
            
            print(f"Loaded {len(signals)} oldest signals from {json_path}", "INFO")
            
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Error loading activeoldestsignals.json: {str(e)}"
            })
            save_errors()
            print(f"Error loading activeoldestsignals.json: {str(e)}", "ERROR")
            return False
        
        # Fetch existing signals from cipher_processed_bouncestreamsignals
        fetch_query = """
            SELECT pair, timeframe, order_type, entry_price, created_at
            FROM cipher_processed_bouncestreamsignals
        """
        try:
            result = db.execute_query(fetch_query)
            print(f"Raw query result for fetching signals: {json.dumps(result, indent=2)}", "DEBUG")
            
            existing_signals = []
            if isinstance(result, dict):
                if result.get('status') != 'success':
                    error_log.append({
                        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                        "error": f"Query failed: {result.get('message', 'No message provided')}"
                    })
                    save_errors()
                    print(f"Query failed: {result.get('message', 'No message provided')}", "ERROR")
                    return False
                existing_signals = result.get('data', {}).get('rows', []) or result.get('results', [])
            elif isinstance(result, list):
                existing_signals = result
            else:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Invalid result format: Expected dict or list, got {type(result)}"
                })
                save_errors()
                print(f"Invalid result format: Expected dict or list, got {type(result)}", "ERROR")
                return False
            
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Error fetching existing signals: {str(e)}"
            })
            save_errors()
            print(f"Error fetching existing signals: {str(e)}", "ERROR")
            return False
        
        # Process JSON signals and validate
        json_signal_keys = set()
        valid_signals = []
        
        # Define maximum allowed value for numeric fields
        MAX_NUMERIC_VALUE = 9999999999.99
        MIN_NUMERIC_VALUE = -9999999999.99
        
        for signal in signals:
            try:
                # Extract fields from JSON
                pair = signal.get('market', 'N/A')
                timeframe = signal.get('timeframe', 'N/A')
                order_type = signal.get('order_type', 'N/A')
                entry_price = float(signal.get('entry_price', 0.0))
                exit_price = float(signal.get('exit_price', 0.0))
                ratio_0_5_price = float(signal.get('ratio_0_5_price', 0.0))
                ratio_1_price = float(signal.get('ratio_1_price', 0.0))
                ratio_2_price = float(signal.get('ratio_2_price', 0.0))
                profit_price = float(signal.get('profit_price', 0.0))
                days_old = int(signal.get('days_old', 0))
                old_range = signal.get('old_range', 'N/A')
                message = f"Order is {days_old} old"
                
                # Validate required fields
                if any(key not in signal for key in ['market', 'timeframe', 'order_type', 'entry_price']):
                    raise ValueError("Missing required fields in JSON signal")
                
                # Validate numeric fields
                for field_name, value in [
                    ('entry_price', entry_price),
                    ('exit_price', exit_price),
                    ('ratio_0_5_price', ratio_0_5_price),
                    ('ratio_1_price', ratio_1_price),
                    ('ratio_2_price', ratio_2_price),
                    ('profit_price', profit_price)
                ]:
                    if not (MIN_NUMERIC_VALUE <= value <= MAX_NUMERIC_VALUE):
                        raise ValueError(f"{field_name} out of range: {value}")
                
                signal_key = (pair, timeframe, order_type, entry_price, old_range)
                if signal_key in json_signal_keys:
                    error_log.append({
                        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                        "error": f"Duplicate signal in JSON: {pair}, {timeframe}, {order_type}, {entry_price}, {old_range}"
                    })
                    print(f"Duplicate signal in JSON: {pair}, {timeframe}, {order_type}, {entry_price}, {old_range}", "WARNING")
                    continue
                json_signal_keys.add(signal_key)
                
                valid_signals.append({
                    'pair': pair,
                    'timeframe': timeframe,
                    'order_type': order_type,
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'ratio_0_5_price': ratio_0_5_price,
                    'ratio_1_price': ratio_1_price,
                    'ratio_2_price': ratio_2_price,
                    'profit_price': profit_price,
                    'message': message,
                    'old_range': old_range
                })
                
            except (ValueError, TypeError) as e:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Invalid data format in signal {signal.get('market', 'unknown')}: {str(e)}"
                })
                print(f"Invalid data format in signal {signal.get('market', 'unknown')}: {str(e)}", "ERROR")
                continue
        
        # Identify duplicates in cipher_processed_bouncestreamsignals
        db_signal_keys = {}
        duplicates_to_remove = []
        
        for signal in existing_signals:
            try:
                pair = signal.get('pair', 'N/A')
                timeframe = signal.get('timeframe', 'N/A')
                order_type = signal.get('order_type', 'N/A')
                entry_price = float(signal.get('entry_price', 0.0))
                created_at = signal.get('created_at', '')
                
                signal_key = (pair, timeframe, order_type, entry_price)
                
                if signal_key in db_signal_keys:
                    if created_at < db_signal_keys[signal_key]['created_at']:
                        duplicates_to_remove.append(db_signal_keys[signal_key])
                        db_signal_keys[signal_key] = signal
                    else:
                        duplicates_to_remove.append(signal)
                else:
                    db_signal_keys[signal_key] = signal
            except (ValueError, TypeError) as e:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Invalid data in existing signal {signal.get('pair', 'unknown')}: {str(e)}"
                })
                print(f"Invalid data in existing signal {signal.get('pair', 'unknown')}: {str(e)}", "ERROR")
                continue
        
        # Initialize counters for batch processing
        insert_batch_counts = []
        duplicate_batch_counts = []
        
        # Batch delete duplicates from cipher_processed_bouncestreamsignals
        DELETE_BATCH_SIZE = 80
        if duplicates_to_remove:
            for i in range(0, len(duplicates_to_remove), DELETE_BATCH_SIZE):
                batch = duplicates_to_remove[i:i + DELETE_BATCH_SIZE]
                batch_number = i // DELETE_BATCH_SIZE + 1
                try:
                    delete_conditions = []
                    for signal in batch:
                        pair = signal['pair'].replace("'", "''")
                        timeframe = signal['timeframe'].replace("'", "''")
                        order_type = signal['order_type'].replace("'", "''")
                        entry_price = float(signal['entry_price'])
                        created_at = signal['created_at'].replace("'", "''")
                        condition = (
                            f"(pair = '{pair}' AND timeframe = '{timeframe}' AND "
                            f"order_type = '{order_type}' AND entry_price = {entry_price} AND created_at = '{created_at}')"
                        )
                        delete_conditions.append(condition)
                    
                    delete_query = f"DELETE FROM cipher_processed_bouncestreamsignals WHERE {' OR '.join(delete_conditions)}"
                    result = db.execute_query(delete_query)
                    affected_rows = result.get('results', {}).get('affected_rows', 0) if isinstance(result, dict) else 0
                    duplicate_batch_counts.append((batch_number, affected_rows))
                    print(f"Successfully removed {affected_rows} duplicate signals in batch {batch_number} from cipher_processed_bouncestreamsignals", "SUCCESS")
                except Exception as e:
                    error_log.append({
                        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                        "error": f"Failed to batch delete duplicates in batch {batch_number} from cipher_processed_bouncestreamsignals: {str(e)}"
                    })
                    save_errors()
                    print(f"Failed to batch delete duplicates in batch {batch_number} from cipher_processed_bouncestreamsignals: {str(e)}", "ERROR")
                    return False
        
        # Prepare batch INSERT query for new valid signals (in chunks of 80)
        BATCH_SIZE = 80
        sql_query_base = """
            INSERT INTO cipher_processed_bouncestreamsignals (
                pair, timeframe, order_type, entry_price, exit_price,
                ratio_0_5_price, ratio_1_price, ratio_2_price, 
                profit_price, message
            ) VALUES 
        """
        value_strings = []
        
        for signal in valid_signals:
            try:
                pair = signal['pair']
                timeframe = signal['timeframe']
                order_type = signal['order_type']
                entry_price = signal['entry_price']
                exit_price = signal['exit_price']
                ratio_0_5_price = signal['ratio_0_5_price']
                ratio_1_price = signal['ratio_1_price']
                ratio_2_price = signal['ratio_2_price']
                profit_price = signal['profit_price']
                message = signal['message']
                
                # Re-validate numeric fields
                for field_name, value in [
                    ('entry_price', entry_price),
                    ('exit_price', exit_price),
                    ('ratio_0_5_price', ratio_0_5_price),
                    ('ratio_1_price', ratio_1_price),
                    ('ratio_2_price', ratio_2_price),
                    ('profit_price', profit_price)
                ]:
                    if not (MIN_NUMERIC_VALUE <= value <= MAX_NUMERIC_VALUE):
                        raise ValueError(f"{field_name} out of range: {value}")
                
                signal_key = (pair, timeframe, order_type, entry_price)
                
                if signal_key not in db_signal_keys:
                    pair_escaped = pair.replace("'", "''")
                    timeframe_escaped = timeframe.replace("'", "''")
                    order_type_escaped = order_type.replace("'", "''")
                    message_escaped = message.replace("'", "''")
                    value_string = (
                        f"('{pair_escaped}', '{timeframe_escaped}', '{order_type_escaped}', {entry_price}, {exit_price}, "
                        f"{ratio_0_5_price}, {ratio_1_price}, {ratio_2_price}, {profit_price}, '{message_escaped}')"
                    )
                    value_strings.append(value_string)
            except (ValueError, TypeError) as e:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Invalid data format in signal {signal.get('pair', 'unknown')}: {str(e)}"
                })
                print(f"Invalid data format in signal {signal.get('pair', 'unknown')}: {str(e)}", "ERROR")
                continue
        
        if not value_strings:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": "No new valid oldest signals to insert after processing"
            })
            save_errors()
            print(f"No new valid oldest signals to insert after processing", "INFO")
            return True
        
        # Execute batch INSERT in chunks of BATCH_SIZE with retries
        success = True
        for i in range(0, len(value_strings), BATCH_SIZE):
            batch = value_strings[i:i + BATCH_SIZE]
            batch_number = i // BATCH_SIZE + 1
            sql_query = sql_query_base + ", ".join(batch)
            
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    result = db.execute_query(sql_query)
                    print(f"Raw query result for inserting batch {batch_number}: {json.dumps(result, indent=2)}", "DEBUG")
                    
                    if not isinstance(result, dict):
                        error_log.append({
                            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                            "error": f"Invalid result format on attempt {attempt} for batch {batch_number}: Expected dict, got {type(result)}"
                        })
                        save_errors()
                        print(f"Invalid result format on attempt {attempt} for batch {batch_number}: Expected dict, got {type(result)}", "ERROR")
                        success = False
                        continue
                    
                    if result.get('status') != 'success':
                        error_log.append({
                            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                            "error": f"Query failed on attempt {attempt} for batch {batch_number}: {result.get('message', 'No message provided')}"
                        })
                        save_errors()
                        print(f"Query failed on attempt {attempt} for batch {batch_number}: {result.get('message', 'No message provided')}", "ERROR")
                        success = False
                        continue
                    
                    affected_rows = result.get('results', {}).get('affected_rows', 0)
                    insert_batch_counts.append((batch_number, affected_rows))
                    print(f"Successfully inserted {affected_rows} oldest signals in batch {batch_number}", "SUCCESS")
                    break
                    
                except Exception as e:
                    error_log.append({
                        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                        "error": f"Exception on attempt {attempt} for batch {batch_number}: {str(e)}"
                    })
                    save_errors()
                    print(f"Exception on attempt {attempt} for batch {batch_number}: {str(e)}", "ERROR")
                    
                    if attempt < MAX_RETRIES:
                        delay = RETRY_DELAY * (2 ** (attempt - 1))
                        print(f"Retrying batch {batch_number} after {delay} seconds...", "INFO")
                        time.sleep(delay)
                    else:
                        error_log.append({
                            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                            "error": f"Max retries reached for batch {batch_number}"
                        })
                        save_errors()
                        print(f"Max retries reached for batch {batch_number}", "ERROR")
                        success = False
        
        # Log batch processing counts
        for batch_number, count in duplicate_batch_counts:
            print(f"Duplicate batch {batch_number} processed: {count}", "INFO")
        for batch_number, count in insert_batch_counts:
            print(f"Insert batch {batch_number} processed: {count}", "INFO")
        
        if not success:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": "Function failed due to errors in one or more batches"
            })
            save_errors()
            print(f"Function failed due to errors in one or more batches", "ERROR")
            return False
        
        print(f"All batches processed successfully", "SUCCESS")
        return True

    def delete_oldestorders_fromdb() -> bool:
        """Delete oldest signals from cipherbouncestream_signals table based on activeoldestsignals.json
        and rewrite activemarkets.json with remaining signals."""
        json_path = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\batches\activeoldestsignals.json"
        activemarkets_path = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\batches\activemarkets.json"
        print("Deleting oldest signals from cipherbouncestream_signals table and rewriting activemarkets.json", "INFO")
        
        # Initialize error log list
        error_log = []
        
        # Define error log file path
        error_json_path = os.path.join(BASE_PROCESSING_FOLDER, "deleteoldestsignalserror.json")
        
        # Helper function to save errors to JSON
        def save_errors():
            try:
                with open(error_json_path, 'w') as f:
                    json.dump(error_log, f, indent=4)
                print(f"Errors saved to {error_json_path}", "INFO")
            except Exception as e:
                print(f"Failed to save errors to {error_json_path}: {str(e)}", "ERROR")
        
        # Helper function to calculate days old
        def calculate_days_old(created_at: str) -> int:
            try:
                current_date = datetime.now(pytz.timezone('Africa/Lagos')).date()
                created_date = datetime.strptime(created_at.split(' ')[0], '%Y-%m-%d').date()
                days_old = (current_date - created_date).days
                return days_old
            except Exception as e:
                print(f"Error calculating days old for created_at {created_at}: {str(e)}", "ERROR")
                return 0
        
        # Load oldest signals from JSON
        try:
            if not os.path.exists(json_path):
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Oldest signals JSON file not found at: {json_path}"
                })
                save_errors()
                print(f"Oldest signals JSON file not found at: {json_path}", "ERROR")
                return False
            
            with open(json_path, 'r') as f:
                data = json.load(f)
            
            signals = data.get('MARKETS', [])
            if not signals:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": "No signals found in activeoldestsignals.json or 'MARKETS' key is missing"
                })
                save_errors()
                print("No signals found in activeoldestsignals.json or 'MARKETS' key is missing", "INFO")
                # Still proceed to ensure activemarkets.json is up-to-date
                signals = []
            
            print(f"Loaded {len(signals)} oldest signals from {json_path}", "INFO")
            
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Error loading activeoldestsignals.json: {str(e)}"
            })
            save_errors()
            print(f"Error loading activeoldestsignals.json: {str(e)}", "ERROR")
            return False
        
        # Process JSON signals and validate
        json_signal_keys = set()
        valid_signals = []
        
        # Define maximum allowed value for numeric fields
        MAX_NUMERIC_VALUE = 9999999999.99
        MIN_NUMERIC_VALUE = -9999999999.99
        
        for signal in signals:
            try:
                # Extract fields from JSON
                pair = signal.get('market', 'N/A')
                timeframe = signal.get('timeframe', 'N/A')
                order_type = signal.get('order_type', 'N/A')
                entry_price = float(signal.get('entry_price', 0.0))
                exit_price = float(signal.get('exit_price', 0.0))
                ratio_0_5_price = float(signal.get('ratio_0_5_price', 0.0))
                ratio_1_price = float(signal.get('ratio_1_price', 0.0))
                ratio_2_price = float(signal.get('ratio_2_price', 0.0))
                profit_price = float(signal.get('profit_price', 0.0))
                old_range = signal.get('old_range', 'N/A')
                
                # Validate required fields
                if any(key not in signal for key in ['market', 'timeframe', 'order_type', 'entry_price']):
                    raise ValueError("Missing required fields in JSON signal")
                
                # Validate numeric fields
                for field_name, value in [
                    ('entry_price', entry_price),
                    ('exit_price', exit_price),
                    ('ratio_0_5_price', ratio_0_5_price),
                    ('ratio_1_price', ratio_1_price),
                    ('ratio_2_price', ratio_2_price),
                    ('profit_price', profit_price)
                ]:
                    if not (MIN_NUMERIC_VALUE <= value <= MAX_NUMERIC_VALUE):
                        raise ValueError(f"{field_name} out of range: {value}")
                
                signal_key = (pair, timeframe, order_type, entry_price, old_range)
                if signal_key in json_signal_keys:
                    error_log.append({
                        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                        "error": f"Duplicate signal in JSON: {pair}, {timeframe}, {order_type}, {entry_price}, {old_range}"
                    })
                    print(f"Duplicate signal in JSON: {pair}, {timeframe}, {order_type}, {entry_price}, {old_range}", "WARNING")
                    continue
                json_signal_keys.add(signal_key)
                
                valid_signals.append({
                    'pair': pair,
                    'timeframe': timeframe,
                    'order_type': order_type,
                    'entry_price': entry_price,
                    'old_range': old_range
                })
                
            except (ValueError, TypeError) as e:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Invalid data format in signal {signal.get('market', 'unknown')}: {str(e)}"
                })
                print(f"Invalid data format in signal {signal.get('market', 'unknown')}: {str(e)}", "ERROR")
                continue
        
        # Batch delete signals from cipherbouncestream_signals
        DELETE_BATCH_SIZE = 80
        delete_batch_counts = []
        success = True
        
        if valid_signals:
            for i in range(0, len(valid_signals), DELETE_BATCH_SIZE):
                batch = valid_signals[i:i + DELETE_BATCH_SIZE]
                batch_number = i // DELETE_BATCH_SIZE + 1
                try:
                    delete_conditions = []
                    for signal in batch:
                        pair = signal['pair'].replace("'", "''")
                        timeframe = signal['timeframe'].replace("'", "''")
                        order_type = signal['order_type'].replace("'", "''")
                        entry_price = float(signal['entry_price'])
                        old_range = signal['old_range'].replace("'", "''")
                        condition = (
                            f"(pair = '{pair}' AND timeframe = '{timeframe}' AND "
                            f"order_type = '{order_type}' AND entry_price = {entry_price} AND created_at = '{old_range}')"
                        )
                        delete_conditions.append(condition)
                    
                    delete_query = f"DELETE FROM cipherbouncestream_signals WHERE {' OR '.join(delete_conditions)}"
                    for attempt in range(1, MAX_RETRIES + 1):
                        try:
                            result = db.execute_query(delete_query)
                            print(f"Raw query result for deleting batch {batch_number}: {json.dumps(result, indent=2)}", "DEBUG")
                            
                            if not isinstance(result, dict):
                                error_log.append({
                                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                                    "error": f"Invalid result format on attempt {attempt} for batch {batch_number}: Expected dict, got {type(result)}"
                                })
                                save_errors()
                                print(f"Invalid result format on attempt {attempt} for batch {batch_number}: Expected dict, got {type(result)}", "ERROR")
                                success = False
                                continue
                            
                            if result.get('status') != 'success':
                                error_log.append({
                                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                                    "error": f"Query failed on attempt {attempt} for batch {batch_number}: {result.get('message', 'No message provided')}"
                                })
                                save_errors()
                                print(f"Query failed on attempt {attempt} for batch {batch_number}: {result.get('message', 'No message provided')}", "ERROR")
                                success = False
                                continue
                            
                            affected_rows = result.get('results', {}).get('affected_rows', 0)
                            delete_batch_counts.append((batch_number, affected_rows))
                            print(f"Successfully deleted {affected_rows} signals in batch {batch_number} from cipherbouncestream_signals", "SUCCESS")
                            break
                        
                        except Exception as e:
                            error_log.append({
                                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                                "error": f"Exception on attempt {attempt} for batch {batch_number}: {str(e)}"
                            })
                            save_errors()
                            print(f"Exception on attempt {attempt} for batch {batch_number}: {str(e)}", "ERROR")
                            
                            if attempt < MAX_RETRIES:
                                delay = RETRY_DELAY * (2 ** (attempt - 1))
                                print(f"Retrying batch {batch_number} after {delay} seconds...", "INFO")
                                time.sleep(delay)
                            else:
                                error_log.append({
                                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                                    "error": f"Max retries reached for batch {batch_number}"
                                })
                                save_errors()
                                print(f"Max retries reached for batch {batch_number}", "ERROR")
                                success = False
                
                except Exception as e:
                    error_log.append({
                        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                        "error": f"Failed to process batch {batch_number}: {str(e)}"
                    })
                    save_errors()
                    print(f"Failed to process batch {batch_number}: {str(e)}", "ERROR")
                    success = False
        
        # Fetch remaining signals from cipherbouncestream_signals
        try:
            fetch_query = """
                SELECT pair, timeframe, order_type, entry_price, exit_price,
                    ratio_0_5_price, ratio_1_price, ratio_2_price, profit_price, created_at
                FROM cipherbouncestream_signals
                ORDER BY pair ASC, created_at ASC
            """
            result = db.execute_query(fetch_query)
            print(f"Raw query result for fetching remaining signals: {json.dumps(result, indent=2)}", "DEBUG")
            
            remaining_signals = []
            if isinstance(result, dict):
                if result.get('status') != 'success':
                    error_log.append({
                        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                        "error": f"Query failed for remaining signals: {result.get('message', 'No message provided')}"
                    })
                    save_errors()
                    print(f"Query failed for remaining signals: {result.get('message', 'No message provided')}", "ERROR")
                    return False
                rows = result.get('data', {}).get('rows', []) or result.get('results', [])
                remaining_signals = [
                    {
                        "market": row['pair'],
                        "timeframe": row['timeframe'],
                        "order_type": row['order_type'],
                        "entry_price": float(row['entry_price']),
                        "exit_price": float(row['exit_price']),
                        "ratio_0_5_price": float(row['ratio_0_5_price']),
                        "ratio_1_price": float(row['ratio_1_price']),
                        "ratio_2_price": float(row['ratio_2_price']),
                        "profit_price": float(row['profit_price']),
                        "old_range": row['created_at'],
                        "days_old": calculate_days_old(row['created_at'])
                    }
                    for row in rows if all(row.get(key) is not None for key in [
                        'pair', 'timeframe', 'order_type', 'entry_price', 'exit_price',
                        'ratio_0_5_price', 'ratio_1_price', 'ratio_2_price', 'profit_price', 'created_at'
                    ])
                ]
            elif isinstance(result, list):
                remaining_signals = [
                    {
                        "market": row['pair'],
                        "timeframe": row['timeframe'],
                        "order_type": row['order_type'],
                        "entry_price": float(row['entry_price']),
                        "exit_price": float(row['exit_price']),
                        "ratio_0_5_price": float(row['ratio_0_5_price']),
                        "ratio_1_price": float(row['ratio_1_price']),
                        "ratio_2_price": float(row['ratio_2_price']),
                        "profit_price": float(row['profit_price']),
                        "old_range": row['created_at'],
                        "days_old": calculate_days_old(row['created_at'])
                    }
                    for row in result if all(row.get(key) is not None for key in [
                        'pair', 'timeframe', 'order_type', 'entry_price', 'exit_price',
                        'ratio_0_5_price', 'ratio_1_price', 'ratio_2_price', 'profit_price', 'created_at'
                    ])
                ]
            else:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Invalid result format for remaining signals: Expected dict or list, got {type(result)}"
                })
                save_errors()
                print(f"Invalid result format for remaining signals: Expected dict or list, got {type(result)}", "ERROR")
                return False
            
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Error fetching remaining signals: {str(e)}"
            })
            save_errors()
            print(f"Error fetching remaining signals: {str(e)}", "ERROR")
            return False
        
        # Save remaining signals to activemarkets.json
        activemarkets_data = {
            "MARKETS": remaining_signals
        }
        try:
            os.makedirs(os.path.dirname(activemarkets_path), exist_ok=True)
            with open(activemarkets_path, 'w') as f:
                json.dump(activemarkets_data, f, indent=4)
            print(f"Successfully saved {len(remaining_signals)} remaining signals to {activemarkets_path}", "SUCCESS")
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Failed to save remaining signals to {activemarkets_path}: {str(e)}"
            })
            save_errors()
            print(f"Failed to save remaining signals to {activemarkets_path}: {str(e)}", "ERROR")
            return False
        
        # Log batch processing counts
        for batch_number, count in delete_batch_counts:
            print(f"Delete batch {batch_number} processed: {count} signals", "INFO")
        
        if not success:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": "Function failed due to errors in one or more batches"
            })
            save_errors()
            print(f"Function failed due to errors in one or more batches", "ERROR")
            return False
        
        print(f"All batches processed successfully, activemarkets.json updated with {len(remaining_signals)} signals", "SUCCESS")
        return True


    get_processed_markets()
    get_activemarket_signals()
    filter_oldest_markets()
    insertoldestsignalstodb()
    delete_oldestorders_fromdb()

def validatesignals():
    """Run the fetchlotsizeandrisk function from updateorders."""
    try:
        orders_status()
        updateorders.validatesignals()
        print("validation completed.")
    except Exception as e:
        print(f"Error in validating signals: {e}")

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

def save_verified_and_skipped_markets():
    """Check verification.json for all markets and save passed markets to passedmarkets.json and skipped markets to skippedmarkets.json."""
    try:
        print("===== Saving Verified and Skipped Markets =====")
        
        # Define paths
        FETCHCHART_DESTINATION_PATH = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\fetched"
        passed_json_path = os.path.join(BACTHES_MARKETS_PATH, "passedmarkets.json")
        skipped_json_path = os.path.join(BACTHES_MARKETS_PATH, "skippedmarkets.json")
        
        # Initialize lists for passed and skipped markets
        passed_markets = []
        skipped_markets = []
        
        # Required timeframes to check
        required_timeframes = ["m5", "m15", "m30", "h1", "h4"]
        
        # Check each market
        for market in MARKETS:
            market_folder_name = market.replace(" ", "_")
            verification_file = os.path.join(FETCHCHART_DESTINATION_PATH, market_folder_name, "verification.json")
            
            try:
                if not os.path.exists(verification_file):
                    print(f"Verification file not found for {market}: {verification_file}")
                    skipped_markets.append({
                        "market": market,
                        "reason": f"Verification file not found: {verification_file}"
                    })
                    continue
                
                with open(verification_file, 'r') as f:
                    verification_data = json.load(f)
                
                # Check if all required timeframes are "chart_identified" and "all_timeframes" is "verified"
                all_timeframes_verified = all(
                    verification_data.get(tf) == "chart_identified" for tf in required_timeframes
                ) and verification_data.get("all_timeframes") == "verified"
                
                if all_timeframes_verified:
                    print(f"All timeframes in verification.json for {market} are 'chart_identified' and 'all_timeframes' is 'verified'")
                    passed_markets.append({
                        "market": market,
                        "status": "passed",
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                    })
                else:
                    print(f"Not all timeframes in verification.json for {market} are 'chart_identified' or 'all_timeframes' is not 'verified'")
                    skipped_markets.append({
                        "market": market,
                        "reason": "Not all timeframes are 'chart_identified' or 'all_timeframes' is not 'verified'",
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                    })
            
            except Exception as e:
                print(f"Error reading verification.json for {market}: {e}")
                skipped_markets.append({
                    "market": market,
                    "reason": f"Error reading verification.json: {str(e)}",
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                })
                continue
        
        # Save passed markets to passedmarkets.json
        try:
            os.makedirs(BACTHES_MARKETS_PATH, exist_ok=True)
            passed_data = {
                "status": "success",
                "message": "Passed markets saved successfully",
                "markets": passed_markets,
                "total_passed": len(passed_markets),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            with open(passed_json_path, 'w') as f:
                json.dump(passed_data, f, indent=4)
            print(f"Passed markets saved to {passed_json_path}")
        except Exception as e:
            print(f"Error saving passed markets to {passed_json_path}: {e}")
            passed_data = {
                "status": "failed",
                "message": f"Error saving passed markets: {str(e)}",
                "markets": [],
                "total_passed": 0,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
        
        # Save skipped markets to skippedmarkets.json
        try:
            os.makedirs(BACTHES_MARKETS_PATH, exist_ok=True)
            skipped_data = {
                "status": "success",
                "message": "Skipped markets saved successfully",
                "markets": skipped_markets,
                "total_skipped": len(skipped_markets),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            with open(skipped_json_path, 'w') as f:
                json.dump(skipped_data, f, indent=4)
            print(f"Skipped markets saved to {skipped_json_path}")
        except Exception as e:
            print(f"Error saving skipped markets to {skipped_json_path}: {e}")
            skipped_data = {
                "status": "failed",
                "message": f"Error saving skipped markets: {str(e)}",
                "markets": [],
                "total_skipped": 0,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
        
        # Print summary
        print("===== Verified and Skipped Markets Summary =====")
        print(f"Total markets checked: {len(MARKETS)}")
        print(f"Passed markets: {len(passed_markets)}")
        print(f"Skipped markets: {len(skipped_markets)}")
        print("Passed markets:", [m["market"] for m in passed_markets])
        print("Skipped markets:", [m["market"] for m in skipped_markets])
        print("=====================================")
        
        return {
            "status": "success" if passed_data["status"] == "success" and skipped_data["status"] == "success" else "partial_success",
            "message": "Verification check for passed and skipped markets completed",
            "total_markets_checked": len(MARKETS),
            "passed_markets": len(passed_markets),
            "skipped_markets": len(skipped_markets),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    
    except Exception as e:
        print(f"Unexpected error in save_verified_and_skipped_markets: {e}")
        return {
            "status": "failed",
            "message": f"Unexpected error: {str(e)}",
            "total_markets_checked": 0,
            "passed_markets": 0,
            "skipped_markets": 0,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    
def mark_verification_status():
    """Update verification.json for each market based on passedmarkets.json, activemarkets.json, and processedmarkets.json, setting all_timeframes accordingly."""
    try:
        print("===== Marking Verification Status for Markets =====")
        
        # Define paths
        FETCHCHART_DESTINATION_PATH = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\fetched"
        activemarkets_path = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\batches\activemarkets.json"
        processedmarkets_path = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\batches\processedmarkets.json"
        passedmarkets_path = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\batches\passedmarkets.json"
        error_json_path = os.path.join(BASE_PROCESSING_FOLDER, "mark_verification_status_error.json")
        
        # Initialize error log
        error_log = []
        
        # Helper function to save errors to JSON
        def save_errors():
            try:
                os.makedirs(BASE_PROCESSING_FOLDER, exist_ok=True)
                with open(error_json_path, 'w') as f:
                    json.dump(error_log, f, indent=4)
                print(f"Errors saved to {error_json_path}")
            except Exception as e:
                print(f"Failed to save errors to {error_json_path}: {str(e)}")
        
        # Load passedmarkets.json
        try:
            if not os.path.exists(passedmarkets_path):
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"passedmarkets.json not found at: {passedmarkets_path}"
                })
                save_errors()
                print(f"passedmarkets.json not found at: {passedmarkets_path}")
                return False
            with open(passedmarkets_path, 'r') as f:
                passed_data = json.load(f)
            passed_markets = {market['market']: market for market in passed_data.get('markets', [])}
            print(f"Loaded {len(passed_markets)} markets from passedmarkets.json")
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Error loading passedmarkets.json: {str(e)}"
            })
            save_errors()
            print(f"Error loading passedmarkets.json: {str(e)}")
            return False
        
        # Load activemarkets.json
        try:
            if not os.path.exists(activemarkets_path):
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"activemarkets.json not found at: {activemarkets_path}"
                })
                save_errors()
                print(f"activemarkets.json not found at: {activemarkets_path}")
                return False
            with open(activemarkets_path, 'r') as f:
                active_data = json.load(f)
            active_markets = {market['market']: market['timeframe'] for market in active_data.get('MARKETS', [])}
            print(f"Loaded {len(active_markets)} markets from activemarkets.json")
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Error loading activemarkets.json: {str(e)}"
            })
            save_errors()
            print(f"Error loading activemarkets.json: {str(e)}")
            return False
        
        # Load processedmarkets.json
        try:
            if not os.path.exists(processedmarkets_path):
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"processedmarkets.json not found at: {processedmarkets_path}"
                })
                save_errors()
                print(f"processedmarkets.json not found at: {processedmarkets_path}")
                return False
            with open(processedmarkets_path, 'r') as f:
                processed_data = json.load(f)
            processed_markets = {market['market']: market['timeframe'] for market in processed_data.get('MARKETS', [])}
            print(f"Loaded {len(processed_markets)} markets from processedmarkets.json")
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Error loading processedmarkets.json: {str(e)}"
            })
            save_errors()
            print(f"Error loading processedmarkets.json: {str(e)}")
            return False
        
        # Required timeframes in verification.json
        required_timeframes = ["m5", "m15", "m30", "h1", "h4"]
        
        # Process each market in passedmarkets.json
        for market in passed_markets:
            market_folder_name = market.replace(" ", "_")
            verification_file = os.path.join(FETCHCHART_DESTINATION_PATH, market_folder_name, "verification.json")
            
            try:
                # Load existing verification.json
                if not os.path.exists(verification_file):
                    error_log.append({
                        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                        "error": f"Verification file not found for {market}: {verification_file}"
                    })
                    print(f"Verification file not found for {market}: {verification_file}")
                    continue
                
                with open(verification_file, 'r') as f:
                    verification_data = json.load(f)
                
                # Initialize updated verification data
                updated_verification_data = verification_data.copy()
                is_active = False
                is_processed = False
                
                # Check each timeframe
                for tf in required_timeframes:
                    db_tf = DB_TIMEFRAME_MAPPING.get(tf.upper(), tf).lower()  # Map timeframe to database format
                    if market in active_markets and active_markets[market].lower() == db_tf:
                        updated_verification_data[tf] = "active"
                        is_active = True
                    elif market in processed_markets and processed_markets[market].lower() == db_tf:
                        updated_verification_data[tf] = "order_free"
                        is_processed = True
                    else:
                        updated_verification_data[tf] = "order_free"  # Set to order_free if not in active or processed
                        is_processed = True  # Treat as processed to set all_timeframes to order_free
                
                # Update all_timeframes field
                if is_active:
                    updated_verification_data["all_timeframes"] = "verified_and_active"
                else:
                    updated_verification_data["all_timeframes"] = "order_free"
                
                # Save updated verification.json
                os.makedirs(os.path.dirname(verification_file), exist_ok=True)
                with open(verification_file, 'w') as f:
                    json.dump(updated_verification_data, f, indent=4)
                print(f"Updated verification.json for {market}: {updated_verification_data}")
                
            except Exception as e:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Error updating verification.json for {market}: {str(e)}"
                })
                print(f"Error updating verification.json for {market}: {str(e)}")
                continue
        
        # Process remaining markets in MARKETS that are not in passedmarkets.json
        for market in MARKETS:
            if market not in passed_markets:
                market_folder_name = market.replace(" ", "_")
                verification_file = os.path.join(FETCHCHART_DESTINATION_PATH, market_folder_name, "verification.json")
                
                try:
                    if not os.path.exists(verification_file):
                        error_log.append({
                            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                            "error": f"Verification file not found for {market}: {verification_file}"
                        })
                        print(f"Verification file not found for {market}: {verification_file}")
                        continue
                    
                    with open(verification_file, 'r') as f:
                        verification_data = json.load(f)
                    
                    # Preserve existing verification data for markets not in passedmarkets.json
                    updated_verification_data = verification_data.copy()
                    os.makedirs(os.path.dirname(verification_file), exist_ok=True)
                    with open(verification_file, 'w') as f:
                        json.dump(updated_verification_data, f, indent=4)
                    print(f"Preserved verification.json for {market} (not in passedmarkets.json): {updated_verification_data}")
                    
                except Exception as e:
                    error_log.append({
                        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                        "error": f"Error preserving verification.json for {market}: {str(e)}"
                    })
                    print(f"Error preserving verification.json for {market}: {str(e)}")
                    continue
        
        # Save any errors to error log
        if error_log:
            save_errors()
        
        # Summary
        print("===== Verification Status Marking Summary =====")
        print(f"Total markets processed: {len(MARKETS)}")
        print(f"Passed markets processed: {len(passed_markets)}")
        print(f"Active markets marked: {len(active_markets)}")
        print(f"Processed markets marked: {len(processed_markets)}")
        print(f"Errors encountered: {len(error_log)}")
        print("=====================================")
        
        return {
            "status": "success" if not error_log else "partial_success",
            "message": "Verification status marking completed",
            "total_markets_processed": len(MARKETS),
            "passed_markets_processed": len(passed_markets),
            "active_markets_marked": len(active_markets),
            "processed_markets_marked": len(processed_markets),
            "errors": len(error_log),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    
    except Exception as e:
        error_log = [{
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "error": f"Unexpected error in mark_verification_status: {str(e)}"
        }]
        save_errors()
        print(f"Unexpected error in mark_verification_status: {str(e)}")
        return {
            "status": "failed",
            "message": f"Unexpected error: {str(e)}",
            "total_markets_processed": 0,
            "passed_markets_processed": 0,
            "active_markets_marked": 0,
            "processed_markets_marked": 0,
            "errors": 1,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

def execute(mode="loop"):
    """Execute the scripts sequentially with the specified mode: 'loop' or 'once'."""
    if mode not in ["loop", "once"]:
        raise ValueError("Invalid mode. Use 'loop' or 'once'.")
    
    # Define path for processedbatches.json
    processed_batches_path = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\batches\processedbatches.json"
    
    # Clear processedbatches.json at the start of execution
    try:
        os.makedirs(os.path.dirname(processed_batches_path), exist_ok=True)
        with open(processed_batches_path, 'w') as f:
            json.dump({"stages": []}, f, indent=4)
        print(f"Cleared {processed_batches_path} at script start.")
    except Exception as e:
        print(f"Error clearing {processed_batches_path}: {e}")
        return

    # Helper function to append stage to processedbatches.json
    def log_batch_stage(stage_message):
        try:
            with open(processed_batches_path, 'r') as f:
                data = json.load(f)
            data["stages"].append({
                "stage": stage_message,
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00')
            })
            with open(processed_batches_path, 'w') as f:
                json.dump(data, f, indent=4)
            print(f"Logged stage: {stage_message} to {processed_batches_path}")
        except Exception as e:
            print(f"Error logging stage '{stage_message}' to {processed_batches_path}: {e}")

    # Verify that credentials and markets were loaded
    if not all([LOGIN_ID, PASSWORD, SERVER, TERMINAL_PATH]):
        print("Credentials not properly loaded from base.json. Exiting.")
        log_batch_stage("Execution failed: Credentials not properly loaded from base.json")
        return
    if not MARKETS:
        print("No markets defined in MARKETS list. Exiting.")
        log_batch_stage("Execution failed: No markets defined in MARKETS list")
        return
    
    # Check verification status for all markets and save to allbatchmarkets.json
    verification_result = check_and_save_verification_status(batchnumber=60)
    if verification_result.get("status") == "failed":
        print(f"Failed to check verification status: {verification_result.get('message', 'Unknown error')}")
        log_batch_stage(f"Execution failed: Failed to check verification status - {verification_result.get('message', 'Unknown error')}")
        return
    
    # Check if there are any verified markets
    verified_markets_count = verification_result.get("total_markets_verified", 0)
    if verified_markets_count == 0:
        print("No markets have all timeframes verified. Exiting to prevent processing unverified markets.")
        log_batch_stage("Execution failed: No markets have all timeframes verified")
        return
    else:
        print(f"Found {verified_markets_count} verified markets across {verification_result.get('total_batches', 0)} batches. Proceeding with execution.")
        log_batch_stage(f"Found {verified_markets_count} verified markets across {verification_result.get('total_batches', 0)} batches")

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
        log_batch_stage(f"Execution failed: Error reading allbatchmarkets.json - {str(e)}")
        return

    def process_all_batches():
        """Helper function to process all batches once and return execution times."""
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
                validatesignals()
                insertpendingorderstodb()

                print("5 minutes markets (M5) completed successfully.")
                return time_left, start_time, initial_time_left, datetime.now(pytz.UTC)

        # Process each batch sequentially
        for batch_index in range(1, total_batches + 1):
            batch_key = f"batch{batch_index}"
            batch_markets = batches.get(batch_key, [])
            if not batch_markets:
                print(f"No markets found for {batch_key}. Skipping.")
                log_batch_stage(f"No markets found for {batch_key}. Skipping")
                continue
            
            print(f"\n=== Processing {batch_key} ===")
            log_batch_stage(f"{batch_key} started")
            # Write current batch to batchbybatch.json
            write_batch_to_json(batch_key, batch_markets)
            log_batch_stage(f"{batch_key} (current stage)")
            
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
            
            # Mark verification status after batch completion
            mark_result = mark_verification_status()
            if mark_result.get("status") == "failed":
                print(f"Failed to mark verification status after {batch_key}: {mark_result.get('message', 'Unknown error')}")
                log_batch_stage(f"Failed to mark verification status after {batch_key}: {mark_result.get('message', 'Unknown error')}")
            else:
                print(f"Verification status marking completed after {batch_key}: {mark_result.get('message', 'Success')}")
                log_batch_stage(f"Verification status marking completed after {batch_key}: {mark_result.get('message', 'Success')}")
            
            print(f"Completed {batch_key}. Moving to next batch...")
            log_batch_stage(f"{batch_key} completed, moving to next batch")
            time.sleep(5)
        
        return overall_start_time_ci, overall_end_time_ci, overall_start_time_5m, overall_end_time_5m

    try:
        if mode == "once":
            # Execute all batches once
            overall_start_time_ci, overall_end_time_ci, overall_start_time_5m, overall_end_time_5m = process_all_batches()
            
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
            
            print(f"All {total_batches} batches processed. Script execution completed in 'once' mode.")
            log_batch_stage(f"Mode is once, all batch processed")

        elif mode == "loop":
            # Continuously loop through all batches
            execution_count = 0
            while True:
                execution_count += 1
                print(f"\n=== Starting Execution Cycle {execution_count} ===")
                log_batch_stage(f"Starting Execution Cycle {execution_count}")
                overall_start_time_ci, overall_end_time_ci, overall_start_time_5m, overall_end_time_5m = process_all_batches()
                
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
                
                print(f"All {total_batches} batches processed. Completed Execution Cycle {execution_count} in 'loop' mode. Restarting...")
                log_batch_stage(f"Mode is loop, finished batches and restarting from 1")
                time.sleep(5)  # Brief pause before restarting the loop

    except Exception as e:
        print(f"Error in main loop: {e}")
        log_batch_stage(f"Error in main loop: {str(e)}")       
       
if __name__ == "__main__":
    execute(mode="once")
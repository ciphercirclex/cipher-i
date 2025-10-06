import json
import os
import multiprocessing
import time
from typing import Dict, Optional, List, Tuple
import pandas as pd
import MetaTrader5 as mt5
from datetime import datetime, timezone,  timedelta
import pytz
from colorama import Fore, Style, init
import logging
from typing import Tuple, Optional, Dict
import shutil
import connectwithinfinitydb as db

# Initialize colorama for colored console output
init()

# Configure Logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler()  # Console output only
    ]
)
logger = logging.getLogger(__name__)

# Suppress WebDriver-related logs
for name in ['webdriver_manager', 'selenium', 'urllib3', 'selenium.webdriver']:
    logging.getLogger(name).setLevel(logging.WARNING)

# Logging Helper Function
def log_and_print(message, level="INFO"):
    """Helper function to print formatted messages with color coding and spacing."""
    indent = "    "
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    level_colors = {
        "INFO": Fore.CYAN,
        "SUCCESS": Fore.GREEN,
        "WARNING": Fore.YELLOW,
        "ERROR": Fore.RED,
        "TITLE": Fore.MAGENTA,
        "DEBUG": Fore.LIGHTBLACK_EX
    }
    log_level = "INFO" if level in ["TITLE", "SUCCESS"] else level
    color = level_colors.get(level, Fore.WHITE)
    formatted_message = f"[ {timestamp} ] │ {level:7} │ {indent}{message}"
    print(f"{color}{formatted_message}{Style.RESET_ALL}")
    logger.log(getattr(logging, log_level), message)

# Configuration
MAX_RETRIES = 5
RETRY_DELAY = 3

# Initialize global credentials as None
LOGIN_ID = None
PASSWORD = None
SERVER = None
TERMINAL_PATH = None
MARKETS = []
TIMEFRAMES = []
CREDENTIALS = {}

# Base paths
BASE_PROCESSING_FOLDER = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\processing"
BASE_OUTPUT_FOLDER = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\orders"
BASE_ERROR_FOLDER = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\orders\debugs"
FETCHCHART_DESTINATION_PATH = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\fetched"
MARKETS_JSON_PATH = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\base.json"

# Timeframe mapping
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

# Function to load markets, timeframes, and credentials from JSON
def load_markets_and_timeframes(json_path):
    """Load MARKETS, TIMEFRAMES, and CREDENTIALS from base.json file."""
    global LOGIN_ID, PASSWORD, SERVER, TERMINAL_PATH, MARKETS, TIMEFRAMES, CREDENTIALS
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
        CREDENTIALS = data.get("CREDENTIALS", {})
        LOGIN_ID = CREDENTIALS.get("LOGIN_ID", None)
        PASSWORD = CREDENTIALS.get("PASSWORD", None)
        SERVER = CREDENTIALS.get("SERVER", None)
        TERMINAL_PATH = CREDENTIALS.get("TERMINAL_PATH", None)
        
        # Validate credentials
        if not all([LOGIN_ID, PASSWORD, SERVER, TERMINAL_PATH]):
            raise ValueError("One or more credentials (LOGIN_ID, PASSWORD, SERVER, TERMINAL_PATH) not found in base.json")
        
        log_and_print(f"Loaded MARKETS: {MARKETS}", "INFO")
        log_and_print(f"Loaded TIMEFRAMES: {TIMEFRAMES}", "INFO")
        log_and_print(f"Loaded CREDENTIALS: LOGIN_ID={LOGIN_ID}, SERVER={SERVER}, TERMINAL_PATH={TERMINAL_PATH}", "INFO")
        return MARKETS, TIMEFRAMES, CREDENTIALS
    except Exception as e:
        log_and_print(f"Error loading base.json: {str(e)}", "ERROR")
        return [], [], {}
# Load markets, timeframes, and credentials at startup
MARKETS, TIMEFRAMES, CREDENTIALS = load_markets_and_timeframes(MARKETS_JSON_PATH)

def candletimeleft(market, timeframe, candle_time, min_time_left):
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
                candles = mt5.copy_rates_from_pos(market, mt5.TIMEFRAME_M15, 0, 1)
                if candles is None or len(candles) == 0:
                    print(f"[Process-{market}] Attempt {attempt + 1}/3: Failed to fetch candle data for {market} (M15), error: {mt5.last_error()}")
                    time.sleep(2)
                    continue
                current_time = datetime.now(pytz.UTC)
                candle_time_dt = datetime.fromtimestamp(candles[0]['time'], tz=pytz.UTC)
                if (current_time - candle_time_dt).total_seconds() > 16 * 60:  # 16 minutes for M15
                    print(f"[Process-{market}] Attempt {attempt + 1}/3: Candle for {market} (M15) is too old (time: {candle_time_dt})")
                    time.sleep(2)
                    continue
                candle_time = candles[0]['time']
                break
            else:
                print(f"[Process-{market}] Failed to fetch recent candle data for {market} (M15) after 3 attempts")
                return None, None

            if timeframe.upper() != "M15":
                print(f"[Process-{market}] Only M15 timeframe is supported, received {timeframe}")
                return None, None

            candle_datetime = datetime.fromtimestamp(candle_time, tz=pytz.UTC)
            minutes_per_candle = 15
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
                print(f"[Process-{market}] Time left ({time_left:.2f} minutes) is <= {min_time_left} minutes, waiting for next candle")
                time_to_wait = (next_close_time - current_time).total_seconds() + 5  # Wait until next candle starts
                time.sleep(time_to_wait)
                continue
            
    finally:
        mt5.shutdown()
def candletimeleft_5minutes(market, candle_time, min_time_left):
    """Check the time left for the current 5-minute (M5) candle for a given market."""
    # Initialize MT5
    print(f"[Process-{market}] Initializing MT5 for {market} (M5)")
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

            # Process M5 timeframe
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
                print(f"[Process-{market}] Time left ({time_left:.2f} minutes) is <= {min_time_left} minutes, waiting for next candle")
                time_to_wait = (next_close_time - current_time).total_seconds() + 5  # Wait until next candle starts
                time.sleep(time_to_wait)
                continue
            
    finally:
        mt5.shutdown()


def normalize_timeframe(timeframe: str) -> str:
    """Normalize timeframe input to standard format (e.g., '5m' -> 'M5', '4h' -> 'H4')."""
    timeframe = timeframe.lower().strip()
    timeframe_map = {
        '5m': 'm5', 'm5': 'M5',
        '15m': 'm15', 'm15': 'M15',
        '30m': 'm30', 'm30': 'M30',
        '1h': 'h1', 'h1': 'H1',
        '4h': 'h4', 'h4': 'H4'
    }
    normalized = timeframe_map.get(timeframe, timeframe.upper())
    if normalized not in TIMEFRAME_MAPPING:
        log_and_print(f"Invalid timeframe format: {timeframe}, normalized to {normalized}", "ERROR")
        return None
    return normalized

def fetch_candle_data(market: str, timeframe: str) -> tuple[Optional[Dict], Optional[str], Dict]:
    """Fetch candle data from MT5 for a specific market and timeframe, returning data, path, and status report."""
    status_report = {
        "market": market,
        "timeframe": timeframe,
        "status": "failed",
        "message": "",
        "candle_count": 0,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    log_and_print(f"Fetching candle data for market={market}, timeframe={timeframe}", "INFO")
    
    # Ensure no existing MT5 connections interfere
    mt5.shutdown()

    # Initialize MT5 terminal with explicit path and timeout
    for attempt in range(MAX_RETRIES):
        if mt5.initialize(path=TERMINAL_PATH, timeout=60000):
            log_and_print(f"Successfully initialized MT5 terminal for {market} {timeframe}", "SUCCESS")
            break
        error_code, error_message = mt5.last_error()
        log_and_print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to initialize MT5 terminal for {market} {timeframe}. Error: {error_code}, {error_message}", "ERROR")
        time.sleep(RETRY_DELAY)
    else:
        error_msg = f"Failed to initialize MT5 terminal for {market} {timeframe} after {MAX_RETRIES} attempts"
        log_and_print(error_msg, "ERROR")
        status_report["message"] = error_msg
        return None, None, status_report

    # Wait for terminal to be fully ready
    for _ in range(5):
        if mt5.terminal_info() is not None:
            log_and_print(f"MT5 terminal fully initialized for {market} {timeframe}", "DEBUG")
            break
        log_and_print(f"Waiting for MT5 terminal to fully initialize for {market} {timeframe}...", "INFO")
        time.sleep(2)
    else:
        error_msg = f"MT5 terminal not ready for {market} {timeframe}"
        log_and_print(error_msg, "ERROR")
        status_report["message"] = error_msg
        mt5.shutdown()
        return None, None, status_report

    # Attempt login with retries
    for attempt in range(MAX_RETRIES):
        if mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=60000):
            log_and_print(f"Successfully logged in to MT5 for {market} {timeframe}", "SUCCESS")
            break
        error_code, error_message = mt5.last_error()
        log_and_print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to log in to MT5 for {market} {timeframe}. Error code: {error_code}, Message: {error_message}", "ERROR")
        time.sleep(RETRY_DELAY)
    else:
        error_msg = f"Failed to log in to MT5 for {market} {timeframe} after {MAX_RETRIES} attempts"
        log_and_print(error_msg, "ERROR")
        status_report["message"] = error_msg
        mt5.shutdown()
        return None, None, status_report

    # Select market symbol
    if not mt5.symbol_select(market, True):
        error_msg = f"Failed to select market: {market}, error: {mt5.last_error()}"
        log_and_print(error_msg, "ERROR")
        status_report["message"] = error_msg
        mt5.shutdown()
        return None, None, status_report

    # Get timeframe
    mt5_timeframe = TIMEFRAME_MAPPING.get(timeframe)
    if not mt5_timeframe:
        error_msg = f"Invalid timeframe {timeframe} for {market}"
        log_and_print(error_msg, "ERROR")
        status_report["message"] = error_msg
        mt5.shutdown()
        return None, None, status_report

    # Fetch candle data
    candles = mt5.copy_rates_from_pos(market, mt5_timeframe, 1, 500)
    if candles is None or len(candles) < 500:
        error_msg = f"Failed to fetch candle data for {market} {timeframe}, error: {mt5.last_error()}"
        log_and_print(error_msg, "ERROR")
        status_report["message"] = error_msg
        mt5.shutdown()
        return None, None, status_report

    df = pd.DataFrame(candles)
    candle_data = {}
    for i in range(len(candles)):
        candle = df.iloc[i]
        position = 500 - i  # Position 1 is most recent completed candle, 500 is oldest
        candle_details = {
            "Time": str(pd.to_datetime(candle['time'], unit='s')),
            "Open": float(candle['open']),
            "High": float(candle['high']),
            "Low": float(candle['low']),
            "Close": float(candle['close'])
        }
        candle_data[f"Candle_{position}"] = candle_details

    status_report["candle_count"] = len(candles)
    log_and_print(f"Verifying candle indexing for {market} {timeframe}: Candle_1 Time={candle_data.get('Candle_1', {}).get('Time', 'N/A')}, Candle_2 Time={candle_data.get('Candle_2', {}).get('Time', 'N/A')}", "DEBUG")

    # Save candle data to JSON
    formatted_market_name = market.replace(" ", "_")
    json_dir = os.path.join(BASE_OUTPUT_FOLDER, formatted_market_name, timeframe.lower())
    os.makedirs(json_dir, exist_ok=True)
    json_file_path = os.path.join(json_dir, "candle_data.json")

    if os.path.exists(json_file_path):
        os.remove(json_file_path)
        log_and_print(f"Existing {json_file_path} deleted", "INFO")

    try:
        with open(json_file_path, "w") as json_file:
            json.dump(candle_data, json_file, indent=4)
        log_and_print(f"Candle details saved to {json_file_path} with {len(candle_data)} candles", "SUCCESS")
        status_report["status"] = "success"
        status_report["message"] = f"Fetched and saved {len(candle_data)} candles to {json_file_path}"
    except Exception as e:
        error_msg = f"Error saving candle data for {market} {timeframe}: {e}"
        log_and_print(error_msg, "ERROR")
        status_report["message"] = error_msg
        mt5.shutdown()
        return None, None, status_report

    mt5.shutdown()
    return candle_data, json_dir, status_report

def match_trendline_with_candle_data(candle_data: Dict, json_dir: str, market: str, timeframe: str) -> Tuple[bool, Optional[str], str, Dict]:
    """Match pending order data with candle data, save to pricecandle.json, and return status report."""
    status_report = {
        "market": market,
        "timeframe": timeframe,
        "status": "failed",
        "message": "",
        "trendline_count": 0,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    # Normalize timeframe
    normalized_timeframe = normalize_timeframe(timeframe)
    if normalized_timeframe is None:
        error_message = f"Invalid timeframe {timeframe} for {market}"
        log_and_print(error_message, "ERROR")
        status_report["message"] = error_message
        return False, error_message, "failed", status_report
    
    formatted_market_name = market.replace(" ", "_")
    pending_json_path = os.path.join(BASE_PROCESSING_FOLDER, formatted_market_name, normalized_timeframe.lower(), "pendingorder.json")

    # Check if pendingorder.json exists
    if not os.path.exists(pending_json_path):
        error_message = f"Pending order JSON file not found at {pending_json_path} for {market} {normalized_timeframe}"
        log_and_print(error_message, "INFO")
        status_report["message"] = error_message
        status_report["status"] = "no_pending_orders"
        pricecandle_json_path = os.path.join(json_dir, "pricecandle.json")
        try:
            if os.path.exists(pricecandle_json_path):
                os.remove(pricecandle_json_path)
                log_and_print(f"Existing {pricecandle_json_path} deleted", "INFO")
            with open(pricecandle_json_path, 'w') as f:
                json.dump([], f, indent=4)
            log_and_print(f"Empty pricecandle.json saved for {market} {normalized_timeframe}", "INFO")
            status_report["message"] = f"No pending orders found; empty pricecandle.json saved for {market} {normalized_timeframe}"
        except Exception as e:
            error_message = f"Error saving empty pricecandle.json for {market} {normalized_timeframe}: {str(e)}"
            log_and_print(error_message, "ERROR")
            status_report["message"] = error_message
            return False, error_message, "failed", status_report
        return False, error_message, "no_pending_orders", status_report

    # Read pendingorder.json
    try:
        with open(pending_json_path, 'r') as f:
            pending_data = json.load(f)
    except Exception as e:
        error_message = f"Error reading pending order JSON file at {pending_json_path} for {market} {normalized_timeframe}: {str(e)}"
        log_and_print(error_message, "ERROR")
        status_report["message"] = error_message
        return False, error_message, "failed", status_report

    # Check if pending_data is empty
    if not pending_data:
        error_message = f"No pending orders in pendingorder.json for {market} {normalized_timeframe}"
        log_and_print(error_message, "INFO")
        status_report["message"] = error_message
        status_report["status"] = "no_pending_orders"
        pricecandle_json_path = os.path.join(json_dir, "pricecandle.json")
        try:
            if os.path.exists(pricecandle_json_path):
                os.remove(pricecandle_json_path)
                log_and_print(f"Existing {pricecandle_json_path} deleted", "INFO")
            with open(pricecandle_json_path, 'w') as f:
                json.dump([], f, indent=4)
            log_and_print(f"Empty pricecandle.json saved for {market} {normalized_timeframe}", "INFO")
            status_report["message"] = f"No pending orders in pendingorder.json; empty pricecandle.json saved for {market} {normalized_timeframe}"
        except Exception as e:
            error_message = f"Error saving empty pricecandle.json for {market} {normalized_timeframe}: {str(e)}"
            log_and_print(error_message, "ERROR")
            status_report["message"] = error_message
            return False, error_message, "failed", status_report
        return False, error_message, "no_pending_orders", status_report

    matched_data = []
    warnings = []

    def get_position_number_from_label(label: str) -> Optional[int]:
        if label == "invalid" or not label:
            return None
        label_clean = label.replace(" order holder", "")
        try:
            return int(label_clean[2:])  # Extract number after 'PL' or 'PH'
        except (ValueError, IndexError):
            return None

    for trendline in pending_data:
        trend_type = trendline.get("type", "")
        sender = trendline.get("sender", {})
        receiver = trendline.get("receiver", {})

        sender_pos = sender.get("position_number")
        receiver_pos = receiver.get("position_number")
        order_type = receiver.get("order_type", "").lower()

        matched_entry = {
            "type": trend_type,
            "sender": {
                "candle_color": sender.get("candle_color"),
                "position_number": sender_pos,
                "sender_arrow_number": sender.get("sender_arrow_number")
            },
            "receiver": {
                "candle_color": receiver.get("candle_color"),
                "position_number": receiver_pos,
                "order_type": order_type,
                "order_status": receiver.get("order_status"),
                "Breakout_parent": receiver.get("Breakout_parent"),
                "order_parent": receiver.get("order_parent"),
                "actual_orderparent": receiver.get("actual_orderparent"),
                "reassigned_orderparent": receiver.get("reassigned_orderparent"),
                "receiver_contractcandle_arrownumber": receiver.get("receiver_contractcandle_arrownumber")
            }
        }

        # Add sender candle data
        sender_candle_key = f"Candle_{sender_pos}"
        if sender_candle_key in candle_data:
            sender_candle = candle_data[sender_candle_key]
            matched_entry["sender"].update({
                "Time": sender_candle["Time"],
                "Open": sender_candle["Open"],
                "High": sender_candle["High"],
                "Low": sender_candle["Low"],
                "Close": sender_candle["Close"]
            })
        else:
            warning = f"No candle data found for sender position {sender_pos} in {market} {normalized_timeframe}"
            log_and_print(warning, "WARNING")
            warnings.append(warning)

        # Add receiver candle data
        receiver_candle_key = f"Candle_{receiver_pos}"
        if receiver_candle_key in candle_data:
            receiver_candle = candle_data[receiver_candle_key]
            matched_entry["receiver"].update({
                "Time": receiver_candle["Time"],
                "Open": receiver_candle["Open"],
                "High": receiver_candle["High"],
                "Low": receiver_candle["Low"],
                "Close": receiver_candle["Close"]
            })
        else:
            warning = f"No candle data found for receiver position {receiver_pos} in {market} {normalized_timeframe}"
            log_and_print(warning, "WARNING")
            warnings.append(warning)

        # Process order holder
        order_parent = receiver.get("order_parent", "invalid")
        actual_orderparent = receiver.get("actual_orderparent", "invalid")
        reassigned_orderparent = receiver.get("reassigned_orderparent", "none")

        order_holder_label = None
        order_holder_pos = None

        if "order holder" in order_parent:
            order_holder_label = order_parent
            order_holder_pos = get_position_number_from_label(order_parent)
        elif "order holder" in actual_orderparent:
            order_holder_label = actual_orderparent
            order_holder_pos = get_position_number_from_label(actual_orderparent)
        elif reassigned_orderparent != "none" and "order holder" in reassigned_orderparent:
            order_holder_label = reassigned_orderparent
            order_holder_pos = get_position_number_from_label(reassigned_orderparent)

        if order_holder_pos is not None:
            order_holder_candle_key = f"Candle_{order_holder_pos}"
            if order_holder_candle_key in candle_data:
                order_holder_candle = candle_data[order_holder_candle_key]
                matched_entry["order_holder"] = {
                    "label": order_holder_label,
                    "position_number": order_holder_pos,
                    "Time": order_holder_candle["Time"],
                    "Open": order_holder_candle["Open"],
                    "High": order_holder_candle["High"],
                    "Low": order_holder_candle["Low"],
                    "Close": order_holder_candle["Close"]
                }
            else:
                warning = f"No candle data found for order holder position {order_holder_pos} in {market} {normalized_timeframe}"
                log_and_print(warning, "WARNING")
                warnings.append(warning)
                matched_entry["order_holder"] = {
                    "label": order_holder_label,
                    "position_number": order_holder_pos
                }
        else:
            warning = f"No order holder found for trendline in {market} {normalized_timeframe}"
            log_and_print(warning, "WARNING")
            warnings.append(warning)
            matched_entry["order_holder"] = {
                "label": "none",
                "position_number": None
            }

        # Process Breakout_parent
        breakout_parent_label = receiver.get("Breakout_parent", "invalid")
        breakout_parent_pos = None

        if breakout_parent_label != "invalid" and breakout_parent_label:
            breakout_parent_pos = get_position_number_from_label(breakout_parent_label)

        if breakout_parent_pos is not None:
            breakout_parent_candle_key = f"Candle_{breakout_parent_pos}"
            breakout_parent_entry = {
                "label": breakout_parent_label,
                "position_number": breakout_parent_pos
            }
            if breakout_parent_candle_key in candle_data:
                breakout_parent_candle = candle_data[breakout_parent_candle_key]
                breakout_parent_entry.update({
                    "Time": breakout_parent_candle["Time"],
                    "Open": breakout_parent_candle["Open"],
                    "High": breakout_parent_candle["High"],
                    "Low": breakout_parent_candle["Low"],
                    "Close": breakout_parent_candle["Close"]
                })
            else:
                warning = f"No candle data found for Breakout_parent position {breakout_parent_pos} in {market} {normalized_timeframe}"
                log_and_print(warning, "WARNING")
                warnings.append(warning)

            # Fetch the candle right after Breakout_parent
            next_candle_pos = breakout_parent_pos - 1
            next_candle_key = f"Candle_{next_candle_pos}"
            if next_candle_key in candle_data:
                next_candle = candle_data[next_candle_key]
                breakout_parent_entry["candle_rightafter_Breakoutparent"] = {
                    "position_number": next_candle_pos,
                    "Time": next_candle["Time"],
                    "Open": next_candle["Open"],
                    "High": next_candle["High"],
                    "Low": next_candle["Low"],
                    "Close": next_candle["Close"]
                }
            else:
                warning = f"No candle data found for position {next_candle_pos} (right after Breakout_parent) in {market} {normalized_timeframe}"
                log_and_print(warning, "WARNING")
                warnings.append(warning)
                breakout_parent_entry["candle_rightafter_Breakoutparent"] = {
                    "position_number": next_candle_pos,
                    "Time": None,
                    "Open": None,
                    "High": None,
                    "Low": None,
                    "Close": None
                }

            matched_entry["Breakout_parent"] = breakout_parent_entry
        else:
            warning = f"No Breakout_parent found or invalid for trendline in {market} {normalized_timeframe}"
            log_and_print(warning, "WARNING")
            warnings.append(warning)
            matched_entry["Breakout_parent"] = {
                "label": "none",
                "position_number": None,
                "candle_rightafter_Breakoutparent": {
                    "position_number": None,
                    "Time": None,
                    "Open": None,
                    "High": None,
                    "Low": None,
                    "Close": None
                }
            }

        matched_data.append(matched_entry)

    status_report["trendline_count"] = len(matched_data)
    if warnings:
        status_report["warnings"] = warnings

    pricecandle_json_path = os.path.join(json_dir, "pricecandle.json")
    if os.path.exists(pricecandle_json_path):
        os.remove(pricecandle_json_path)
        log_and_print(f"Existing {pricecandle_json_path} deleted", "INFO")

    try:
        with open(pricecandle_json_path, 'w') as f:
            json.dump(matched_data, f, indent=4)
        log_and_print(f"Matched pending order and candle data saved to {pricecandle_json_path} with {len(matched_data)} trendlines for {market} {normalized_timeframe}", "SUCCESS")
        status_report["status"] = "success"
        status_report["message"] = f"Matched {len(matched_data)} trendlines and saved to {pricecandle_json_path}"
    except Exception as e:
        error_message = f"Error saving pricecandle.json for {market} {normalized_timeframe}: {str(e)}"
        log_and_print(error_message, "ERROR")
        status_report["message"] = error_message
        return False, error_message, "failed", status_report

    return True, None, "success", status_report

def match_mostrecent_candle(market: str, timeframe: str, json_dir: str) -> Tuple[bool, Optional[str], str, Dict]:
    """Match the most recent completed candle with candle data, save to matchedcandles.json, and return status report."""
    status_report = {
        "market": market,
        "timeframe": timeframe,
        "status": "failed",
        "message": "",
        "candles_matched": 0,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "warnings": []
    }
    
    log_and_print(f"Matching most recent completed candle for market={market}, timeframe={timeframe}", "INFO")
    
    # Define file paths
    formatted_market_name = market.replace(" ", "_")
    mostrecent_json_path = os.path.join(FETCHCHART_DESTINATION_PATH, formatted_market_name, timeframe.lower(), "mostrecent_completedcandle.json")
    candle_data_json_path = os.path.join(json_dir, "candle_data.json")
    matched_candles_json_path = os.path.join(json_dir, "matchedcandles.json")
    
    # Check if both JSON files exist
    if not os.path.exists(mostrecent_json_path):
        error_message = f"mostrecent_completedcandle.json not found at {mostrecent_json_path} for {market} {timeframe}"
        log_and_print(error_message, "ERROR")
        status_report["message"] = error_message
        return False, error_message, "failed", status_report
    if not os.path.exists(candle_data_json_path):
        error_message = f"candle_data.json not found at {candle_data_json_path} for {market} {timeframe}"
        log_and_print(error_message, "ERROR")
        status_report["message"] = error_message
        return False, error_message, "failed", status_report
    
    try:
        # Load most recent completed candle
        with open(mostrecent_json_path, 'r') as f:
            mostrecent_data = json.load(f)
        
        # Load candle data
        with open(candle_data_json_path, 'r') as f:
            candle_data = json.load(f)
        
        # Extract timestamp from most recent completed candle
        mostrecent_timestamp_str = mostrecent_data.get('time')
        if not mostrecent_timestamp_str:
            error_message = f"No timestamp found in mostrecent_completedcandle.json for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            status_report["message"] = error_message
            return False, error_message, "failed", status_report
        
        # Parse timestamp (ISO format: "2025-08-28T13:15:00+00:00")
        try:
            mostrecent_timestamp = datetime.fromisoformat(mostrecent_timestamp_str.replace('Z', '+00:00'))
        except ValueError as e:
            error_message = f"Invalid timestamp format in mostrecent_completedcandle.json: {mostrecent_timestamp_str}, error: {e}"
            log_and_print(error_message, "ERROR")
            status_report["message"] = error_message
            return False, error_message, "failed", status_report
        
        # Prepare data for most recent candle
        mostrecent_candle = {
            "market": mostrecent_data.get('market'),
            "timeframe": mostrecent_data.get('timeframe'),
            "timestamp": mostrecent_timestamp_str,
            "open": mostrecent_data.get('open_price'),
            "close": mostrecent_data.get('close_price'),
            "high": mostrecent_data.get('high_price'),
            "low": mostrecent_data.get('low_price')
        }
        
        # Find matching or nearest candle
        matched_candle = None
        min_time_diff = float('inf')
        candles_inbetween = 0
        match_result_status = "none"
        
        for candle_key, candle in candle_data.items():
            candle_timestamp_str = candle.get('Time')
            if not candle_timestamp_str:
                warning = f"No timestamp found for {candle_key} in candle_data.json for {market} {timeframe}"
                log_and_print(warning, "WARNING")
                status_report["warnings"].append(warning)
                continue
            
            try:
                # Parse candle timestamp and make it offset-aware (assume UTC)
                candle_timestamp = datetime.strptime(candle_timestamp_str, "%Y-%m-%d %H:%M:%S")
                candle_timestamp = candle_timestamp.replace(tzinfo=timezone.utc)
            except ValueError as e:
                warning = f"Invalid timestamp format for {candle_key} in candle_data.json: {candle_timestamp_str}, error: {e}"
                log_and_print(warning, "WARNING")
                status_report["warnings"].append(warning)
                continue
            
            # Calculate time difference
            time_diff = (mostrecent_timestamp - candle_timestamp).total_seconds()
            abs_time_diff = abs(time_diff)
            
            # Check for exact match (within 1 second to account for minor differences)
            if abs_time_diff < 1:
                matched_candle = {
                    "market": market,
                    "timeframe": timeframe,
                    "timestamp": candle_timestamp_str,
                    "open": float(candle.get('Open')),
                    "close": float(candle.get('Close')),
                    "high": float(candle.get('High')),
                    "low": float(candle.get('Low'))
                }
                match_result_status = "samematch"
                candles_inbetween = 0
                break
            
            # Update nearest match
            if abs_time_diff < min_time_diff:
                min_time_diff = abs_time_diff
                matched_candle = {
                    "market": market,
                    "timeframe": timeframe,
                    "timestamp": candle_timestamp_str,
                    "open": float(candle.get('Open')),
                    "close": float(candle.get('Close')),
                    "high": float(candle.get('High')),
                    "low": float(candle.get('Low'))
                }
                match_result_status = "nearestahead" if time_diff < 0 else "nearestbehind"
                timeframe_minutes = {
                    "M5": 5, "M15": 15, "M30": 30, "H1": 60, "H4": 240
                }.get(timeframe, 5)
                candles_inbetween = int(abs_time_diff / (timeframe_minutes * 60))
        
        if not matched_candle:
            error_message = f"No matching or nearby candle found for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            status_report["message"] = error_message
            return False, error_message, "failed", status_report
        
        # Update status report for successful match
        status_report["candles_matched"] = 1
        status_report["match_result_status"] = match_result_status
        status_report["candles_inbetween"] = candles_inbetween
        
        # Prepare matched candles data
        matched_candles_data = {
            "from most recent": mostrecent_candle,
            "with candledata": matched_candle,
            "candles_inbetween": str(candles_inbetween),
            "match_result_status": match_result_status
        }
        
        # Save to matchedcandles.json
        if os.path.exists(matched_candles_json_path):
            os.remove(matched_candles_json_path)
            log_and_print(f"Existing {matched_candles_json_path} deleted", "INFO")
        
        try:
            with open(matched_candles_json_path, 'w') as f:
                json.dump(matched_candles_data, f, indent=4)
            log_and_print(f"Matched candles data saved to {matched_candles_json_path} for {market} {timeframe}", "SUCCESS")
            status_report["status"] = "success"
            status_report["message"] = f"Matched 1 candle with status '{match_result_status}' and {candles_inbetween} candles in between"
            return True, None, "success", status_report
        except Exception as e:
            error_message = f"Error saving matchedcandles.json for {market} {timeframe}: {e}"
            log_and_print(error_message, "ERROR")
            status_report["message"] = error_message
            return False, error_message, "failed", status_report
    
    except Exception as e:
        error_message = f"Error matching most recent candle for {market} {timeframe}: {e}"
        log_and_print(error_message, "ERROR")
        status_report["message"] = error_message
        return False, error_message, "failed", status_report
    
def save_new_mostrecent_completed_candle(market: str, timeframe: str, json_dir: str) -> Tuple[bool, Optional[str], str, Dict]:
    """Fetch and save the most recent completed candle for a market and timeframe, return status report."""
    status_report = {
        "market": market,
        "timeframe": timeframe,
        "status": "failed",
        "message": "",
        "candles_fetched": 0,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "warnings": []
    }
    
    log_and_print(f"Fetching most recent completed candle for market={market}, timeframe={timeframe}", "INFO")

    # Ensure no existing MT5 connections interfere
    mt5.shutdown()

    # Initialize MT5 terminal with retries
    for attempt in range(MAX_RETRIES):
        if mt5.initialize(path=TERMINAL_PATH, timeout=60000):
            log_and_print(f"Successfully initialized MT5 terminal for most recent candle {market} {timeframe}", "SUCCESS")
            break
        error_code, error_message = mt5.last_error()
        warning = f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to initialize MT5 for most recent candle {market} {timeframe}. Error: {error_code}, {error_message}"
        log_and_print(warning, "ERROR")
        status_report["warnings"].append(warning)
        time.sleep(RETRY_DELAY)
    else:
        error_message = f"Failed to initialize MT5 terminal for most recent candle {market} {timeframe} after {MAX_RETRIES} attempts"
        log_and_print(error_message, "ERROR")
        status_report["message"] = error_message
        return False, error_message, "failed", status_report

    # Wait for terminal to be fully ready
    for _ in range(5):
        if mt5.terminal_info() is not None:
            log_and_print(f"MT5 terminal fully initialized for most recent candle {market} {timeframe}", "DEBUG")
            break
        warning = f"Waiting for MT5 terminal to fully initialize for most recent candle {market} {timeframe}..."
        log_and_print(warning, "INFO")
        status_report["warnings"].append(warning)
        time.sleep(2)
    else:
        error_message = f"MT5 terminal not ready for most recent candle {market} {timeframe}"
        log_and_print(error_message, "ERROR")
        status_report["message"] = error_message
        mt5.shutdown()
        return False, error_message, "failed", status_report

    # Attempt login with retries
    for attempt in range(MAX_RETRIES):
        if mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=60000):
            log_and_print(f"Successfully logged in to MT5 for most recent candle {market} {timeframe}", "SUCCESS")
            break
        error_code, error_message = mt5.last_error()
        warning = f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to log in to MT5 for most recent candle {market} {timeframe}. Error code: {error_code}, Message: {error_message}"
        log_and_print(warning, "ERROR")
        status_report["warnings"].append(warning)
        time.sleep(RETRY_DELAY)
    else:
        error_message = f"Failed to log in to MT5 for most recent candle {market} {timeframe} after {MAX_RETRIES} attempts"
        log_and_print(error_message, "ERROR")
        status_report["message"] = error_message
        mt5.shutdown()
        return False, error_message, "failed", status_report

    # Select market symbol
    if not mt5.symbol_select(market, True):
        error_message = f"Failed to select market for most recent candle: {market}, error: {mt5.last_error()}"
        log_and_print(error_message, "ERROR")
        status_report["message"] = error_message
        mt5.shutdown()
        return False, error_message, "failed", status_report

    # Get timeframe
    mt5_timeframe = TIMEFRAME_MAPPING.get(timeframe)
    if not mt5_timeframe:
        error_message = f"Invalid timeframe {timeframe} for most recent candle {market}"
        log_and_print(error_message, "ERROR")
        status_report["message"] = error_message
        mt5.shutdown()
        return False, error_message, "failed", status_report

    # Fetch the most recent completed candle (position 1)
    new_mostrecent_candle = mt5.copy_rates_from_pos(market, mt5_timeframe, 1, 1)
    if new_mostrecent_candle is None or len(new_mostrecent_candle) == 0:
        error_message = f"Failed to fetch most recent completed candle for {market} {timeframe}, error: {mt5.last_error()}"
        log_and_print(error_message, "ERROR")
        status_report["message"] = error_message
        mt5.shutdown()
        return False, error_message, "failed", status_report

    # Prepare candle data
    candle = new_mostrecent_candle[0]
    open_price = float(candle['open'])
    close_price = float(candle['close'])
    candle_color = "green" if close_price > open_price else "red" if close_price < open_price else "neutral"
    new_mostrecent_candle_data = {
        "market": market,
        "timeframe": timeframe,
        "open_price": open_price,
        "close_price": close_price,
        "high_price": float(candle['high']),
        "low_price": float(candle['low']),
        "time": str(pd.to_datetime(candle['time'], unit='s')),
        "color": candle_color
    }

    # Update status report
    status_report["candles_fetched"] = 1
    status_report["candle_time"] = new_mostrecent_candle_data["time"]

    # Save to JSON
    json_file_path = os.path.join(json_dir, "newmostrecent_completedcandle.json")
    if os.path.exists(json_file_path):
        os.remove(json_file_path)
        log_and_print(f"Existing {json_file_path} deleted", "INFO")

    try:
        with open(json_file_path, "w") as json_file:
            json.dump(new_mostrecent_candle_data, json_file, indent=4)
        log_and_print(f"Most recent completed candle saved to {json_file_path}", "SUCCESS")
        status_report["status"] = "success"
        status_report["message"] = f"Saved most recent completed candle at {new_mostrecent_candle_data['time']} for {market} {timeframe}"
        mt5.shutdown()
        return True, None, "success", status_report
    except Exception as e:
        error_message = f"Error saving most recent completed candle for {market} {timeframe}: {e}"
        log_and_print(error_message, "ERROR")
        status_report["message"] = error_message
        mt5.shutdown()
        return False, error_message, "failed", status_report
    
def calculate_candles_inbetween(market: str, timeframe: str, json_dir: str) -> Tuple[bool, Optional[str], str, Dict]:
    """Calculate the number of candles between newmostrecent_completedcandle.json and matchedcandles.json 'with candledata', return status report."""
    status_report = {
        "market": market,
        "timeframe": timeframe,
        "status": "failed",
        "message": "",
        "candles_inbetween": 0,
        "plus_newmostrecent": 0,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "warnings": []
    }
    
    log_and_print(f"Calculating candles in between for market={market}, timeframe={timeframe}", "INFO")
    
    # Define file paths
    formatted_market_name = market.replace(" ", "_")
    newmostrecent_json_path = os.path.join(json_dir, "newmostrecent_completedcandle.json")
    matched_candles_json_path = os.path.join(json_dir, "matchedcandles.json")
    output_json_path = os.path.join(json_dir, "candlesamountinbetween.json")
    
    # Check if both JSON files exist
    if not os.path.exists(newmostrecent_json_path):
        error_message = f"newmostrecent_completedcandle.json not found at {newmostrecent_json_path} for {market} {timeframe}"
        log_and_print(error_message, "ERROR")
        status_report["message"] = error_message
        return False, error_message, "failed", status_report
    if not os.path.exists(matched_candles_json_path):
        error_message = f"matchedcandles.json not found at {matched_candles_json_path} for {market} {timeframe}"
        log_and_print(error_message, "ERROR")
        status_report["message"] = error_message
        return False, error_message, "failed", status_report
    
    try:
        # Load new most recent completed candle
        with open(newmostrecent_json_path, 'r') as f:
            newmostrecent_data = json.load(f)
        
        # Load matched candles data
        with open(matched_candles_json_path, 'r') as f:
            matched_candles_data = json.load(f)
        
        # Extract timestamp from new most recent completed candle
        newmostrecent_timestamp_str = newmostrecent_data.get('time')
        if not newmostrecent_timestamp_str:
            error_message = f"No timestamp found in newmostrecent_completedcandle.json for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            status_report["message"] = error_message
            return False, error_message, "failed", status_report
        
        # Extract timestamp from matched candles "with candledata"
        matched_candle = matched_candles_data.get('with candledata', {})
        matched_timestamp_str = matched_candle.get('timestamp')
        if not matched_timestamp_str:
            error_message = f"No timestamp found in matchedcandles.json 'with candledata' for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            status_report["message"] = error_message
            return False, error_message, "failed", status_report
        
        # Parse timestamps
        try:
            newmostrecent_timestamp = datetime.strptime(newmostrecent_timestamp_str, "%Y-%m-%d %H:%M:%S")
            newmostrecent_timestamp = newmostrecent_timestamp.replace(tzinfo=timezone.utc)
            
            matched_timestamp = datetime.strptime(matched_timestamp_str, "%Y-%m-%d %H:%M:%S")
            matched_timestamp = matched_timestamp.replace(tzinfo=timezone.utc)
        except ValueError as e:
            error_message = f"Invalid timestamp format in JSON files for {market} {timeframe}: {e}"
            log_and_print(error_message, "ERROR")
            status_report["message"] = error_message
            return False, error_message, "failed", status_report
        
        # Calculate time difference in seconds
        time_diff = (newmostrecent_timestamp - matched_timestamp).total_seconds()
        
        # Determine timeframe duration in minutes
        timeframe_minutes = {
            "M5": 5,
            "M15": 15,
            "M30": 30,
            "H1": 60,
            "H4": 240
        }.get(timeframe, 5)
        
        # Calculate number of candles in between
        candles_inbetween = int(abs(time_diff) / (timeframe_minutes * 60))
        plusmostrecent = candles_inbetween + 1
        
        # Update status report
        status_report["candles_inbetween"] = candles_inbetween
        status_report["plus_newmostrecent"] = plusmostrecent
        
        # Prepare output data
        output_data = {
            "market": market,
            "timeframe": timeframe,
            "candles in between": str(candles_inbetween),
            "plus_newmostrecent": str(plusmostrecent),
            "new number position for matched candle data": str(plusmostrecent),
            "new most recent": {
                "timestamp newmostrecent": newmostrecent_timestamp_str,
                "newmostrecent high": float(newmostrecent_data.get('high_price', 0)),
                "newmostrecent low": float(newmostrecent_data.get('low_price', 0))
            },
            "matched candle data": {
                "timestamp of matched candledata": matched_timestamp_str,
                "matched candledata high": float(matched_candle.get('high', 0)),
                "matched candledata low": float(matched_candle.get('low', 0))
            }
        }
        
        # Save to candlesamountinbetween.json
        if os.path.exists(output_json_path):
            os.remove(output_json_path)
            log_and_print(f"Existing {output_json_path} deleted", "INFO")
        
        try:
            with open(output_json_path, 'w') as f:
                json.dump(output_data, f, indent=4)
            log_and_print(f"Candles in between data saved to {output_json_path} for {market} {timeframe}", "SUCCESS")
            status_report["status"] = "success"
            status_report["message"] = f"Calculated {candles_inbetween} candles in between, plus {plusmostrecent} including new most recent"
            return True, None, "success", status_report
        except Exception as e:
            error_message = f"Error saving candlesamountinbetween.json for {market} {timeframe}: {e}"
            log_and_print(error_message, "ERROR")
            status_report["message"] = error_message
            return False, error_message, "failed", status_report
    
    except Exception as e:
        error_message = f"Error calculating candles in between for {market} {timeframe}: {e}"
        log_and_print(error_message, "ERROR")
        status_report["message"] = error_message
        return False, error_message, "failed", status_report
    
def candleafterbreakoutparent_to_currentprice(market: str, timeframe: str, json_dir: str) -> Tuple[bool, Optional[str], str, Dict]:
    """Fetch candles from the candle after Breakout_parent to the current price candle, save to JSON, and return status report."""
    status_report = {
        "market": market,
        "timeframe": timeframe,
        "status": "failed",
        "message": "",
        "trendlines_processed": 0,
        "total_candles_fetched": 0,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "warnings": []
    }
    
    log_and_print(f"Fetching candles from after Breakout_parent to current price for market={market}, timeframe={timeframe}", "INFO")
    
    # Define file paths
    pricecandle_json_path = os.path.join(json_dir, "pricecandle.json")
    output_json_path = os.path.join(json_dir, "candlesafterbreakoutparent.json")
    invalid_markets_json_path = os.path.join(r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\orders", "candleafterbreakoutcandle.json")
    
    # Initialize invalid markets data
    invalid_markets_data = {}
    if os.path.exists(invalid_markets_json_path):
        try:
            with open(invalid_markets_json_path, 'r') as f:
                invalid_markets_data = json.load(f)
        except Exception as e:
            log_and_print(f"Error loading existing {invalid_markets_json_path}: {e}", "ERROR")
            status_report["warnings"].append(f"Error loading existing {invalid_markets_json_path}: {e}")
            invalid_markets_data.setdefault("error_loading_invalid_markets_json", []).append(f"{market}_{timeframe.lower()}")

    market_key = f"{market}_{timeframe.lower()}"
    
    def save_invalid_markets(error_key: str):
        """Helper function to save invalid_markets_data to JSON with deduplication."""
        if market_key not in invalid_markets_data.get(error_key, []):
            invalid_markets_data.setdefault(error_key, []).append(market_key)
        try:
            with open(invalid_markets_json_path, 'w') as f:
                json.dump(invalid_markets_data, f, indent=4)
            log_and_print(f"Updated invalid markets data to {invalid_markets_json_path} for {error_key}", "INFO")
        except Exception as e:
            log_and_print(f"Error saving invalid markets to {invalid_markets_json_path}: {e}", "ERROR")
            status_report["warnings"].append(f"Error saving invalid markets: {e}")
            invalid_markets_data.setdefault("error_saving_invalid_markets_json", []).append(market_key)
            try:
                with open(invalid_markets_json_path, 'w') as f:
                    json.dump(invalid_markets_data, f, indent=4)
            except Exception as e2:
                log_and_print(f"Critical error saving invalid markets after failure: {e2}", "ERROR")
                status_report["warnings"].append(f"Critical error saving invalid markets: {e2}")

    # Check if pricecandle.json exists
    if not os.path.exists(pricecandle_json_path):
        error_message = f"pricecandle.json not found at {pricecandle_json_path} for {market} {timeframe}"
        log_and_print(error_message, "ERROR")
        status_report["message"] = error_message
        save_invalid_markets("missing_pricecandle_json")
        return False, error_message, "failed", status_report
    
    try:
        # Load pricecandle.json
        with open(pricecandle_json_path, 'r') as f:
            pricecandle_data = json.load(f)
        
        if not pricecandle_data or not isinstance(pricecandle_data, list):
            error_message = f"pricecandle.json is empty or invalid for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            status_report["message"] = error_message
            save_invalid_markets("invalid_pricecandle_json")
            return False, error_message, "failed", status_report

        # Initialize MT5
        mt5.shutdown()
        for attempt in range(MAX_RETRIES):
            if mt5.initialize(path=TERMINAL_PATH, timeout=60000):
                log_and_print(f"Successfully initialized MT5 terminal for candles after Breakout_parent {market} {timeframe}", "SUCCESS")
                break
            error_code, error_message = mt5.last_error()
            warning = f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to initialize MT5 for candles after Breakout_parent {market} {timeframe}. Error: {error_code}, {error_message}"
            log_and_print(warning, "ERROR")
            status_report["warnings"].append(warning)
            if attempt == MAX_RETRIES - 1:
                error_message = f"Failed to initialize MT5 terminal for candles after Breakout_parent {market} {timeframe} after {MAX_RETRIES} attempts"
                log_and_print(error_message, "ERROR")
                status_report["message"] = error_message
                save_invalid_markets("mt5_initialization_failed")
                return False, error_message, "failed", status_report
            time.sleep(RETRY_DELAY)

        # Wait for terminal to be fully ready
        for _ in range(5):
            if mt5.terminal_info() is not None:
                log_and_print(f"MT5 terminal fully initialized for candles after Breakout_parent {market} {timeframe}", "DEBUG")
                break
            warning = f"Waiting for MT5 terminal to fully initialize for candles after Breakout_parent {market} {timeframe}..."
            log_and_print(warning, "INFO")
            status_report["warnings"].append(warning)
            time.sleep(2)
        else:
            error_message = f"MT5 terminal not ready for candles after Breakout_parent {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            status_report["message"] = error_message
            save_invalid_markets("mt5_terminal_not_ready")
            mt5.shutdown()
            return False, error_message, "failed", status_report

        # Attempt login
        for attempt in range(MAX_RETRIES):
            if mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=60000):
                log_and_print(f"Successfully logged in to MT5 for candles after Breakout_parent {market} {timeframe}", "SUCCESS")
                break
            error_code, error_message = mt5.last_error()
            warning = f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to log in to MT5 for candles after Breakout_parent {market} {timeframe}. Error: {error_code}, {error_message}"
            log_and_print(warning, "ERROR")
            status_report["warnings"].append(warning)
            if attempt == MAX_RETRIES - 1:
                error_message = f"Failed to log in to MT5 for candles after Breakout_parent {market} {timeframe} after {MAX_RETRIES} attempts"
                log_and_print(error_message, "ERROR")
                status_report["message"] = error_message
                save_invalid_markets("mt5_login_failed")
                mt5.shutdown()
                return False, error_message, "failed", status_report

        # Select market symbol
        if not mt5.symbol_select(market, True):
            error_message = f"Failed to select market for candles after Breakout_parent: {market}, error: {mt5.last_error()}"
            log_and_print(error_message, "ERROR")
            status_report["message"] = error_message
            save_invalid_markets("market_selection_failed")
            mt5.shutdown()
            return False, error_message, "failed", status_report

        # Get timeframe
        mt5_timeframe = TIMEFRAME_MAPPING.get(timeframe)
        if not mt5_timeframe:
            error_message = f"Invalid timeframe {timeframe} for candles after Breakout_parent {market}"
            log_and_print(error_message, "ERROR")
            status_report["message"] = error_message
            save_invalid_markets("invalid_timeframe")
            mt5.shutdown()
            return False, error_message, "failed", status_report

        # Initialize output data
        candles_data = []
        trendlines_processed = 0
        total_candles_fetched = 0
        valid_trendlines = False
        
        # Process each trendline in pricecandle.json
        for trendline in pricecandle_data:
            if not isinstance(trendline, dict):
                warning = f"Invalid trendline format in pricecandle.json for {market} {timeframe}"
                log_and_print(warning, "WARNING")
                status_report["warnings"].append(warning)
                save_invalid_markets("invalid_trendline_format")
                continue

            breakout_parent = trendline.get("Breakout_parent", {})
            breakout_parent_pos = breakout_parent.get("position_number")
            breakout_candle = breakout_parent.get("candle_rightafter_Breakoutparent", {})
            order_type = trendline.get("receiver", {}).get("order_type", "").lower()
            order_holder = trendline.get("order_holder", {})
            
            if not isinstance(breakout_parent, dict) or not isinstance(breakout_candle, dict) or not isinstance(order_holder, dict):
                warning = f"Invalid structure for Breakout_parent, candle_rightafter_Breakoutparent, or order_holder in {market} {timeframe}"
                log_and_print(warning, "WARNING")
                status_report["warnings"].append(warning)
                save_invalid_markets("invalid_trendline_structure")
                continue

            if breakout_parent_pos is None or breakout_candle.get("position_number") is None:
                warning = f"No valid Breakout_parent or candle_rightafter_Breakoutparent for trendline in {market} {timeframe}"
                log_and_print(warning, "WARNING")
                status_report["warnings"].append(warning)
                save_invalid_markets("invalid_trendline_data")
                continue

            # Get the position number of the candle right after Breakout_parent
            start_pos = breakout_candle.get("position_number")
            if start_pos is None or not isinstance(start_pos, (int, float)):
                warning = f"Invalid position number for candle_rightafter_Breakoutparent in {market} {timeframe}"
                log_and_print(warning, "WARNING")
                status_report["warnings"].append(warning)
                save_invalid_markets("invalid_position_number")
                continue

            # Validate order_type
            if order_type not in ["long", "short"]:
                warning = f"Invalid order_type {order_type} for trendline in {market} {timeframe}"
                log_and_print(warning, "WARNING")
                status_report["warnings"].append(warning)
                save_invalid_markets("invalid_order_type")
                order_type = None
                order_holder_entry = None
            else:
                order_holder_entry = float(order_holder.get("Low", 0)) if order_type == "short" else float(order_holder.get("High", 0)) if order_type == "long" else None
                if order_holder_entry == 0 or order_holder.get("position_number") is None:
                    warning = f"No valid order holder data for trendline in {market} {timeframe}"
                    log_and_print(warning, "WARNING")
                    status_report["warnings"].append(warning)
                    save_invalid_markets("invalid_order_holder")
                    order_holder_entry = None
                else:
                    valid_trendlines = True  # Mark that at least one valid trendline was found

            # Fetch candles from start_pos to position 0
            try:
                num_candles = start_pos - 1
                if num_candles <= 0:
                    warning = f"No candles to fetch (start_pos={start_pos}) for {market} {timeframe}"
                    log_and_print(warning, "WARNING")
                    status_report["warnings"].append(warning)
                    save_invalid_markets("no_candles_to_fetch")
                    candles = []
                else:
                    candles = mt5.copy_rates_from_pos(market, mt5_timeframe, 1, num_candles)
                    if candles is None or len(candles) == 0:
                        warning = f"Failed to fetch candles from position {start_pos} to 1 for {market} {timeframe}, error: {mt5.last_error()}"
                        log_and_print(warning, "ERROR")
                        status_report["warnings"].append(warning)
                        save_invalid_markets("candle_fetch_failed")
                        continue

                # Prepare candle data
                trendline_candles = []
                for i, candle in enumerate(candles):
                    position = start_pos - i
                    try:
                        trendline_candles.append({
                            "position_number": position,
                            "Time": str(pd.to_datetime(candle['time'], unit='s')),
                            "Open": float(candle['open']),
                            "High": float(candle['high']),
                            "Low": float(candle['low']),
                            "Close": float(candle['close'])
                        })
                    except (KeyError, ValueError) as e:
                        warning = f"Invalid candle data at position {position} for {market} {timeframe}: {e}"
                        log_and_print(warning, "WARNING")
                        status_report["warnings"].append(warning)
                        save_invalid_markets("invalid_candle_data")
                        continue

                # Fetch current (incomplete) candle (position 0)
                current_candle = mt5.copy_rates_from_pos(market, mt5_timeframe, 0, 1)
                if current_candle is None or len(current_candle) == 0:
                    warning = f"Failed to fetch current candle for {market} {timeframe}, error: {mt5.last_error()}"
                    log_and_print(warning, "WARNING")
                    status_report["warnings"].append(warning)
                    save_invalid_markets("current_candle_fetch_failed")
                    current_candle_data = {
                        "position_number": 0,
                        "Time": None,
                        "Open": None
                    }
                else:
                    try:
                        current_candle_data = {
                            "position_number": 0,
                            "Time": str(pd.to_datetime(current_candle[0]['time'], unit='s')),
                            "Open": float(current_candle[0]['open'])
                        }
                    except (KeyError, ValueError) as e:
                        warning = f"Invalid current candle data for {market} {timeframe}: {e}"
                        log_and_print(warning, "WARNING")
                        status_report["warnings"].append(warning)
                        save_invalid_markets("invalid_current_candle_data")
                        current_candle_data = {
                            "position_number": 0,
                            "Time": None,
                            "Open": None
                        }

                trendline_candles.append(current_candle_data)
                
                # Add to output data
                candles_data.append({
                    "trendline": {
                        "type": trendline.get("type"),
                        "Breakout_parent_position": breakout_parent_pos,
                        "candle_rightafter_Breakoutparent_position": breakout_candle.get("position_number"),
                        "order_type": order_type,
                        "Order_holder_entry": order_holder_entry
                    },
                    "candles": trendline_candles
                })
                
                trendlines_processed += 1
                total_candles_fetched += len(trendline_candles)

            except Exception as e:
                warning = f"Error fetching candles for trendline in {market} {timeframe}: {e}"
                log_and_print(warning, "ERROR")
                status_report["warnings"].append(warning)
                save_invalid_markets("trendline_processing_error")
                continue

        # Check if no valid trendlines were processed
        if not valid_trendlines and trendlines_processed == 0:
            warning = f"No valid trendlines found for {market} {timeframe}"
            log_and_print(warning, "WARNING")
            status_report["warnings"].append(warning)
            save_invalid_markets("no_valid_trendlines")

        # Save to candlesafterbreakoutparent.json
        if os.path.exists(output_json_path):
            try:
                os.remove(output_json_path)
                log_and_print(f"Existing {output_json_path} deleted", "INFO")
            except Exception as e:
                warning = f"Error deleting existing {output_json_path} for {market} {timeframe}: {e}"
                log_and_print(warning, "ERROR")
                status_report["warnings"].append(warning)
                save_invalid_markets("error_deleting_output_json")

        if not candles_data:
            log_and_print(f"No candles data to save for {market} {timeframe}. Saving empty candlesafterbreakoutparent.json", "INFO")
            try:
                with open(output_json_path, 'w') as f:
                    json.dump(candles_data, f, indent=4)
                log_and_print(f"Empty candlesafterbreakoutparent.json saved to {output_json_path} for {market} {timeframe}", "INFO")
                status_report["status"] = "success"
                status_report["message"] = f"No candles data fetched; saved empty candlesafterbreakoutparent.json"
                save_invalid_markets("no_candles_data")
                mt5.shutdown()
                return True, None, "success", status_report
            except Exception as e:
                error_message = f"Error saving empty candlesafterbreakoutparent.json for {market} {timeframe}: {e}"
                log_and_print(error_message, "ERROR")
                status_report["message"] = error_message
                save_invalid_markets("save_empty_candles_failed")
                mt5.shutdown()
                return False, error_message, "failed", status_report

        try:
            with open(output_json_path, 'w') as f:
                json.dump(candles_data, f, indent=4)
            log_and_print(f"Candles after Breakout_parent saved to {output_json_path} for {market} {timeframe}", "SUCCESS")
            status_report["status"] = "success"
            status_report["message"] = f"Fetched {total_candles_fetched} candles for {trendlines_processed} trendlines"
            save_invalid_markets("success")  # Log successful markets
            mt5.shutdown()
            return True, None, "success", status_report
        except Exception as e:
            error_message = f"Error saving candlesafterbreakoutparent.json for {market} {timeframe}: {e}"
            log_and_print(error_message, "ERROR")
            status_report["message"] = error_message
            save_invalid_markets("save_candles_failed")
            mt5.shutdown()
            return False, error_message, "failed", status_report

    except Exception as e:
        error_message = f"Error processing candles after Breakout_parent for {market} {timeframe}: {e}"
        log_and_print(error_message, "ERROR")
        status_report["message"] = error_message
        save_invalid_markets("general_processing_error")
        mt5.shutdown()
        return False, error_message, "failed", status_report
    
def fetchlotsizeandriskallowed(json_dir: str = BASE_OUTPUT_FOLDER) -> bool:
    """Fetch all lot size and allowed risk data from ciphercontracts_lotsizeandrisk table and save to lotsizes.json."""
    log_and_print("Fetching all lot size and allowed risk data", "INFO")
    
    # Initialize error log list
    error_log = []
    
    # Define error log file path
    error_json_path = os.path.join(BASE_ERROR_FOLDER, "fetchlotsizeandriskerror.json")
    
    # Helper function to save errors to JSON
    def save_errors():
        try:
            with open(error_json_path, 'w') as f:
                json.dump(error_log, f, indent=4)
            log_and_print(f"Errors saved to {error_json_path}", "INFO")
        except Exception as e:
            log_and_print(f"Failed to save errors to {error_json_path}: {str(e)}", "ERROR")
    
    # SQL query to fetch all rows
    sql_query = """
        SELECT id, pair, timeframe, lot_size, allowed_risk, created_at
        FROM ciphercontracts_lotsizeandrisk
    """
    
    # Create output directory if it doesn't exist
    if not os.path.exists(json_dir):
        try:
            os.makedirs(json_dir)
            log_and_print(f"Created output directory: {json_dir}", "INFO")
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Error creating directory {json_dir}: {str(e)}"
            })
            save_errors()
            log_and_print(f"Error creating directory {json_dir}: {str(e)}", "ERROR")
            return False
    
    # Execute query with retries
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = db.execute_query(sql_query)
            log_and_print(f"Raw query result for lot size and risk: {json.dumps(result, indent=2)}", "DEBUG")
            
            if not isinstance(result, dict):
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Invalid result format on attempt {attempt}: Expected dict, got {type(result)}"
                })
                save_errors()
                log_and_print(f"Invalid result format on attempt {attempt}: Expected dict, got {type(result)}", "ERROR")
                continue
                
            if result.get('status') != 'success':
                error_message = result.get('message', 'No message provided')
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Query failed on attempt {attempt}: {error_message}"
                })
                save_errors()
                log_and_print(f"Query failed on attempt {attempt}: {error_message}", "ERROR")
                continue
                
            # Handle both 'data' and 'results' keys
            rows = None
            if 'data' in result and 'rows' in result['data'] and isinstance(result['data']['rows'], list):
                rows = result['data']['rows']
            elif 'results' in result and isinstance(result['results'], list):
                rows = result['results']
            else:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Invalid or missing rows in result on attempt {attempt}: {json.dumps(result, indent=2)}"
                })
                save_errors()
                log_and_print(f"Invalid or missing rows in result on attempt {attempt}: {json.dumps(result, indent=2)}", "ERROR")
                continue
            
            # Prepare data for single JSON file
            data = []
            for row in rows:
                data.append({
                    'id': int(row.get('id', 0)),
                    'pair': row.get('pair', 'N/A'),
                    'timeframe': row.get('timeframe', 'N/A'),
                    'lot_size': float(row.get('lot_size', 0.0)) if row.get('lot_size') is not None else None,
                    'allowed_risk': float(row.get('allowed_risk', 0.0)) if row.get('allowed_risk') is not None else None,
                    'created_at': row.get('created_at', 'N/A')
                })
            
            # Define output path for single JSON file
            output_json_path = os.path.join(json_dir, "lotsizes.json")
            
            # Delete existing file if it exists
            if os.path.exists(output_json_path):
                try:
                    os.remove(output_json_path)
                    log_and_print(f"Existing {output_json_path} deleted", "INFO")
                except Exception as e:
                    error_log.append({
                        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                        "error": f"Error deleting existing {output_json_path}: {str(e)}"
                    })
                    save_errors()
                    log_and_print(f"Error deleting existing {output_json_path}: {str(e)}", "ERROR")
                    return False
            
            # Save to JSON
            try:
                with open(output_json_path, 'w') as f:
                    json.dump(data, f, indent=4)
                log_and_print(f"Lot size and allowed risk data saved to {output_json_path}", "SUCCESS")
                return True
            except Exception as e:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Error saving {output_json_path}: {str(e)}"
                })
                save_errors()
                log_and_print(f"Error saving {output_json_path}: {str(e)}", "ERROR")
                return False
                
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Exception on attempt {attempt}: {str(e)}"
            })
            save_errors()
            log_and_print(f"Exception on attempt {attempt}: {str(e)}", "ERROR")
            
        if attempt < MAX_RETRIES:
            delay = RETRY_DELAY * (2 ** (attempt - 1))
            log_and_print(f"Retrying after {delay} seconds...", "INFO")
            time.sleep(delay)
        else:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": "Max retries reached for fetching lot size and risk data"
            })
            save_errors()
            log_and_print("Max retries reached for fetching lot size and risk data", "ERROR")
            return False
    
    error_log.append({
        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
        "error": "Function exited without success"
    })
    save_errors()
    return False
def executefetchlotsizeandrisk():
    """Execute the fetchlotsizeandriskallowed function."""
    if not fetchlotsizeandriskallowed():
        log_and_print("Failed to fetch lot size and allowed risk data. Exiting.", "ERROR")
        return False
    return True

def getorderholderpriceswithlotsizeandrisk(market: str, timeframe: str, json_dir: str) -> tuple[bool, dict]:
    """Fetch order holder prices, calculate exit and profit prices using lot size and allowed risk from centralized lotsizeandrisk.json, and save to calculatedprices.json."""
    log_and_print(f"Calculating order holder prices with lot size and risk for market={market}, timeframe={timeframe}", "INFO")
    
    # Initialize status report
    status_report = {
        "market": market,
        "timeframe": timeframe,
        "status": "failed",
        "message": "",
        "orders_processed": 0,
        "warnings": [],
        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
        "verified_order_count": 0
    }
    
    # Define file paths
    pricecandle_json_path = os.path.join(json_dir, "pricecandle.json")
    lotsizeandrisk_json_path = os.path.join(BASE_OUTPUT_FOLDER, "lotsizeandrisk.json")
    output_json_path = os.path.join(json_dir, "calculatedprices.json")
    error_json_path = os.path.join(BASE_OUTPUT_FOLDER, "getorderholderpriceserrors.json")
    
    # Initialize error log list
    error_log = []
    
    # Helper function to save errors to JSON
    def save_errors():
        try:
            with open(error_json_path, 'w') as f:
                json.dump(error_log, f, indent=4)
            log_and_print(f"Errors saved to {error_json_path}", "INFO")
        except Exception as e:
            log_and_print(f"Failed to save errors to {error_json_path}: {str(e)}", "ERROR")
            status_report["warnings"].append(f"Failed to save errors: {str(e)}")
    
    # Check if pricecandle.json exists
    if not os.path.exists(pricecandle_json_path):
        error_log.append({
            "timestamp": status_report["timestamp"],
            "market": market,
            "timeframe": timeframe,
            "error": f"pricecandle.json not found at {pricecandle_json_path}"
        })
        save_errors()
        log_and_print(f"pricecandle.json not found at {pricecandle_json_path} for {market} {timeframe}", "ERROR")
        status_report["message"] = f"pricecandle.json not found at {pricecandle_json_path}"
        return False, status_report
    
    # Check if lotsizeandrisk.json exists
    if not os.path.exists(lotsizeandrisk_json_path):
        error_log.append({
            "timestamp": status_report["timestamp"],
            "market": market,
            "timeframe": timeframe,
            "error": f"lotsizeandrisk.json not found at {lotsizeandrisk_json_path}"
        })
        save_errors()
        log_and_print(f"lotsizeandrisk.json not found at {lotsizeandrisk_json_path} for {market} {timeframe}", "ERROR")
        status_report["message"] = f"lotsizeandrisk.json not found at {lotsizeandrisk_json_path}"
        return False, status_report
    
    try:
        # Initialize MT5 to fetch market-specific data
        mt5.shutdown()
        for attempt in range(MAX_RETRIES):
            if mt5.initialize(path=TERMINAL_PATH, timeout=60000):
                log_and_print(f"Successfully initialized MT5 for {market} {timeframe}", "SUCCESS")
                break
            error_code, error_message = mt5.last_error()
            error_log.append({
                "timestamp": status_report["timestamp"],
                "market": market,
                "timeframe": timeframe,
                "error": f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to initialize MT5. Error: {error_code}, {error_message}"
            })
            save_errors()
            log_and_print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to initialize MT5 for {market} {timeframe}. Error: {error_code}, {error_message}", "ERROR")
            time.sleep(RETRY_DELAY)
        else:
            error_log.append({
                "timestamp": status_report["timestamp"],
                "market": market,
                "timeframe": timeframe,
                "error": f"Failed to initialize MT5 for {market} {timeframe} after {MAX_RETRIES} attempts"
            })
            save_errors()
            log_and_print(f"Failed to initialize MT5 for {market} {timeframe} after {MAX_RETRIES} attempts", "ERROR")
            status_report["message"] = f"Failed to initialize MT5 after {MAX_RETRIES} attempts"
            return False, status_report

        # Login to MT5
        for attempt in range(MAX_RETRIES):
            if mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=60000):
                log_and_print(f"Successfully logged in to MT5 for {market} {timeframe}", "SUCCESS")
                break
            error_code, error_message = mt5.last_error()
            error_log.append({
                "timestamp": status_report["timestamp"],
                "market": market,
                "timeframe": timeframe,
                "error": f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to log in to MT5. Error: {error_code}, {error_message}"
            })
            save_errors()
            log_and_print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to log in to MT5 for {market} {timeframe}. Error: {error_code}, {error_message}", "ERROR")
            time.sleep(RETRY_DELAY)
        else:
            error_log.append({
                "timestamp": status_report["timestamp"],
                "market": market,
                "timeframe": timeframe,
                "error": f"Failed to log in to MT5 for {market} {timeframe} after {MAX_RETRIES} attempts"
            })
            save_errors()
            log_and_print(f"Failed to log in to MT5 for {market} {timeframe} after {MAX_RETRIES} attempts", "ERROR")
            status_report["message"] = f"Failed to log in to MT5 after {MAX_RETRIES} attempts"
            mt5.shutdown()
            return False, status_report

        # Select market symbol
        if not mt5.symbol_select(market, True):
            error_log.append({
                "timestamp": status_report["timestamp"],
                "market": market,
                "timeframe": timeframe,
                "error": f"Failed to select market: {market}, error: {mt5.last_error()}"
            })
            save_errors()
            log_and_print(f"Failed to select market: {market}, error: {mt5.last_error()}", "ERROR")
            status_report["message"] = f"Failed to select market: {market}"
            mt5.shutdown()
            return False, status_report

        # Fetch symbol info for pip size and contract size
        symbol_info = mt5.symbol_info(market)
        if not symbol_info:
            error_log.append({
                "timestamp": status_report["timestamp"],
                "market": market,
                "timeframe": timeframe,
                "error": f"Failed to fetch symbol info for {market}"
            })
            save_errors()
            log_and_print(f"Failed to fetch symbol info for {market}", "ERROR")
            status_report["message"] = f"Failed to fetch symbol info for {market}"
            mt5.shutdown()
            return False, status_report

        # Determine pip size and decimal places
        pip_size = symbol_info.point
        digits = symbol_info.digits
        contract_size = symbol_info.trade_contract_size

        # Load pricecandle.json
        with open(pricecandle_json_path, 'r') as f:
            pricecandle_data = json.load(f)
        
        # Load lotsizeandrisk.json
        with open(lotsizeandrisk_json_path, 'r') as f:
            lotsizeandrisk_data = json.load(f)
        
        # Log available pairs and timeframes for debugging
        available_pairs_timeframes = [(entry.get("pair", ""), entry.get("timeframe", "")) for entry in lotsizeandrisk_data]
        log_and_print(f"Available pairs and timeframes in lotsizeandrisk.json: {available_pairs_timeframes}", "DEBUG")
        
        # Normalize input timeframe
        normalized_timeframe = normalize_timeframe(timeframe)
        if normalized_timeframe is None:
            error_log.append({
                "timestamp": status_report["timestamp"],
                "market": market,
                "timeframe": timeframe,
                "error": f"Invalid timeframe: {timeframe}"
            })
            save_errors()
            log_and_print(f"Invalid timeframe: {timeframe}", "ERROR")
            status_report["message"] = f"Invalid timeframe: {timeframe}"
            mt5.shutdown()
            return False, status_report
        
        # Map normalized timeframe to database format
        db_timeframe = DB_TIMEFRAME_MAPPING.get(normalized_timeframe, normalized_timeframe.lower())
        
        # Filter lot size and risk data for the specific market and timeframe
        matching_lot_size = None
        for lot_entry in lotsizeandrisk_data:
            entry_pair = lot_entry.get("pair", "").lower()
            entry_timeframe = lot_entry.get("timeframe", "").lower()
            if entry_pair == market.lower() and entry_timeframe == db_timeframe.lower():
                matching_lot_size = lot_entry
                break
        
        if not matching_lot_size:
            error_log.append({
                "timestamp": status_report["timestamp"],
                "market": market,
                "timeframe": timeframe,
                "error": f"No matching lot size and risk data found for pair={market}, timeframe={db_timeframe}"
            })
            save_errors()
            log_and_print(f"No matching lot size and risk data found for {market} {timeframe} (normalized to pair={market.lower()}, timeframe={db_timeframe})", "WARNING")
            status_report["warnings"].append(f"No matching lot size and risk data for pair={market}, timeframe={db_timeframe}")
            mt5.shutdown()
            return False, status_report
        
        # Initialize output data
        calculated_prices = []
        
        # Process each trendline in pricecandle.json
        for trendline in pricecandle_data:
            order_holder = trendline.get("order_holder", {})
            order_type = trendline.get("receiver", {}).get("order_type", "").lower()
            trendline_type = trendline.get("type", "unknown")
            
            # Check if order holder is valid
            order_holder_position = order_holder.get("position_number")
            if order_holder.get("label", "none") == "none" or order_holder_position is None:
                error_log.append({
                    "timestamp": status_report["timestamp"],
                    "market": market,
                    "timeframe": timeframe,
                    "trendline_type": trendline_type,
                    "error": f"No valid order holder: label={order_holder.get('label', 'none')}, position_number={order_holder_position}"
                })
                save_errors()
                log_and_print(f"No valid order holder for trendline {trendline_type} in {market} {timeframe}", "INFO")
                status_report["warnings"].append(f"No valid order holder for trendline {trendline_type}")
                continue
            
            # Validate order_type
            if order_type not in ["long", "short"]:
                error_log.append({
                    "timestamp": status_report["timestamp"],
                    "market": market,
                    "timeframe": timeframe,
                    "trendline_type": trendline_type,
                    "error": f"Invalid order_type {order_type}"
                })
                save_errors()
                log_and_print(f"Invalid order_type {order_type} for trendline {trendline_type} in {market} {timeframe}", "WARNING")
                status_report["warnings"].append(f"Invalid order_type {order_type} for trendline {trendline_type}")
                continue
            
            # Get entry price based on order type
            entry_price = float(order_holder.get("Low", 0)) if order_type == "short" else float(order_holder.get("High", 0)) if order_type == "long" else None
            if entry_price == 0:
                error_log.append({
                    "timestamp": status_report["timestamp"],
                    "market": market,
                    "timeframe": timeframe,
                    "trendline_type": trendline_type,
                    "error": f"No valid entry price for order holder: Low={order_holder.get('Low', 'N/A')}, High={order_holder.get('High', 'N/A')}"
                })
                save_errors()
                log_and_print(f"No valid entry price for order holder in trendline {trendline_type} in {market} {timeframe}", "WARNING")
                status_report["warnings"].append(f"No valid entry price for trendline {trendline_type}")
                continue
            
            # Extract lot size and allowed risk
            lot_size = float(matching_lot_size.get("lot_size", 0))
            allowed_risk = float(matching_lot_size.get("allowed_risk", 0))
            if lot_size <= 0 or allowed_risk <= 0:
                error_log.append({
                    "timestamp": status_report["timestamp"],
                    "market": market,
                    "timeframe": timeframe,
                    "trendline_type": trendline_type,
                    "error": f"Invalid lot_size {lot_size} or allowed_risk {allowed_risk}"
                })
                save_errors()
                log_and_print(f"Invalid lot_size {lot_size} or allowed_risk {allowed_risk} for trendline {trendline_type} in {market} {timeframe}", "WARNING")
                status_report["warnings"].append(f"Invalid lot_size {lot_size} or allowed_risk {allowed_risk} for trendline {trendline_type}")
                continue
            
            # Calculate pip value
            pip_value = lot_size * contract_size * pip_size
            if market.endswith("JPY"):
                current_price = mt5.symbol_info_tick(market).bid
                if current_price > 0:
                    pip_value = pip_value / current_price
                else:
                    error_log.append({
                        "timestamp": status_report["timestamp"],
                        "market": market,
                        "timeframe": timeframe,
                        "trendline_type": trendline_type,
                        "error": f"Failed to fetch current price for {market} to adjust pip value"
                    })
                    save_errors()
                    log_and_print(f"Failed to fetch current price for {market} to adjust pip value", "WARNING")
                    status_report["warnings"].append(f"Failed to fetch current price for {market} to adjust pip value")
                    pip_value = lot_size * 10  # Fallback
            
            # Calculate risk in pips
            risk_in_pips = allowed_risk / pip_value if pip_value != 0 else 0
            if risk_in_pips <= 0:
                error_log.append({
                    "timestamp": status_report["timestamp"],
                    "market": market,
                    "timeframe": timeframe,
                    "trendline_type": trendline_type,
                    "error": f"Invalid risk_in_pips {risk_in_pips}. pip_value={pip_value}, allowed_risk={allowed_risk}"
                })
                save_errors()
                log_and_print(f"Invalid risk_in_pips {risk_in_pips} for trendline {trendline_type} in {market} {timeframe}", "WARNING")
                status_report["warnings"].append(f"Invalid risk_in_pips {risk_in_pips} for trendline {trendline_type}")
                continue
            
            # Calculate exit_price, 1:0.5_price, 1:1_price, 1:2_price, and profit_price
            reward_to_risk_ratios = {
                "1:0.5": 0.5,
                "1:1": 1,
                "1:2": 2,
                "1:3": 3
            }
            if order_type == "short":
                exit_price = entry_price + (risk_in_pips * pip_size)
                price_1_0_5 = entry_price - (risk_in_pips * reward_to_risk_ratios["1:0.5"] * pip_size)
                price_1_1 = entry_price - (risk_in_pips * reward_to_risk_ratios["1:1"] * pip_size)
                price_1_2 = entry_price - (risk_in_pips * reward_to_risk_ratios["1:2"] * pip_size)
                profit_price = entry_price - (risk_in_pips * reward_to_risk_ratios["1:3"] * pip_size)
            else:  # order_type == "long"
                exit_price = entry_price - (risk_in_pips * pip_size)
                price_1_0_5 = entry_price + (risk_in_pips * reward_to_risk_ratios["1:0.5"] * pip_size)
                price_1_1 = entry_price + (risk_in_pips * reward_to_risk_ratios["1:1"] * pip_size)
                price_1_2 = entry_price + (risk_in_pips * reward_to_risk_ratios["1:2"] * pip_size)
                profit_price = entry_price + (risk_in_pips * reward_to_risk_ratios["1:3"] * pip_size)
            
            # Round prices to market-specific decimal places
            entry_price = round(entry_price, digits)
            exit_price = round(exit_price, digits)
            price_1_0_5 = round(price_1_0_5, digits)
            price_1_1 = round(price_1_1, digits)
            price_1_2 = round(price_1_2, digits)
            profit_price = round(profit_price, digits)
            
            # Validate calculated prices
            if exit_price <= 0 or price_1_0_5 <= 0 or price_1_1 <= 0 or price_1_2 <= 0 or profit_price <= 0:
                error_log.append({
                    "timestamp": status_report["timestamp"],
                    "market": market,
                    "timeframe": timeframe,
                    "trendline_type": trendline_type,
                    "error": f"Invalid prices: entry={entry_price}, exit={exit_price}, 1:0.5={price_1_0_5}, 1:1={price_1_1}, 1:2={price_1_2}, profit={profit_price}"
                })
                save_errors()
                log_and_print(f"Invalid prices: exit={exit_price}, 1:0.5={price_1_0_5}, 1:1={price_1_1}, 1:2={price_1_2}, profit={profit_price} for trendline {trendline_type} in {market} {timeframe}", "ERROR")
                status_report["warnings"].append(f"Invalid prices for trendline {trendline_type}")
                continue
            
            # Prepare calculated price entry
            calculated_entry = {
                "id": matching_lot_size.get("id"),
                "market": market,
                "pair": matching_lot_size.get("pair"),
                "timeframe": timeframe,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "1:0.5_price": price_1_0_5,
                "1:1_price": price_1_1,
                "1:2_price": price_1_2,
                "profit_price": profit_price,
                "lot_size": lot_size,
                "order_type": "sell_limit" if order_type == "short" else "buy_limit",
                "trendline_type": trendline_type,
                "order_holder_position": order_holder_position,
                "pip_size": pip_size,
                "risk_in_pips": round(risk_in_pips, 2),
                "pip_value": round(pip_value, 4)
            }
            
            calculated_prices.append(calculated_entry)
            status_report["orders_processed"] += 1
            log_and_print(
                f"Calculated prices for trendline {trendline_type}: entry={entry_price}, exit={exit_price}, "
                f"1:0.5={price_1_0_5}, 1:1={price_1_1}, 1:2={price_1_2}, profit={profit_price}, "
                f"lot_size={lot_size}, order_type={calculated_entry['order_type']}, "
                f"risk_in_pips={risk_in_pips}, pip_value={pip_value} in {market} {timeframe}",
                "DEBUG"
            )
        
        # Log the number of calculated prices
        error_log.append({
            "timestamp": status_report["timestamp"],
            "market": market,
            "timeframe": timeframe,
            "error": f"Processed {len(pricecandle_data)} trendlines, {len(calculated_prices)} valid entries calculated"
        })
        save_errors()
        
        # Save to calculatedprices.json
        if os.path.exists(output_json_path):
            try:
                os.remove(output_json_path)
                log_and_print(f"Existing {output_json_path} deleted", "INFO")
            except Exception as e:
                error_log.append({
                    "timestamp": status_report["timestamp"],
                    "market": market,
                    "timeframe": timeframe,
                    "error": f"Error deleting existing {output_json_path}: {str(e)}"
                })
                save_errors()
                log_and_print(f"Error deleting existing {output_json_path}: {str(e)}", "ERROR")
                status_report["message"] = f"Error deleting existing {output_json_path}: {str(e)}"
                mt5.shutdown()
                return False, status_report
        
        if not calculated_prices:
            error_log.append({
                "timestamp": status_report["timestamp"],
                "market": market,
                "timeframe": timeframe,
                "error": f"No valid calculated prices to save for {market} {timeframe}"
            })
            save_errors()
            log_and_print(f"No valid calculated prices to save for {market} {timeframe}. Saving empty file.", "INFO")
            try:
                with open(output_json_path, 'w') as f:
                    json.dump([], f, indent=4)
                log_and_print(f"Empty calculatedprices.json saved to {output_json_path} for {market} {timeframe}", "SUCCESS")
                # Verify saved file
                with open(output_json_path, 'r') as f:
                    saved_data = json.load(f)
                status_report["verified_order_count"] = len(saved_data)
                status_report["status"] = "success"
                status_report["message"] = f"Empty calculatedprices.json saved for {market} {timeframe}"
                mt5.shutdown()
                return True, status_report
            except Exception as e:
                error_log.append({
                    "timestamp": status_report["timestamp"],
                    "market": market,
                    "timeframe": timeframe,
                    "error": f"Error saving empty calculatedprices.json: {str(e)}"
                })
                save_errors()
                log_and_print(f"Error saving empty calculatedprices.json for {market} {timeframe}: {str(e)}", "ERROR")
                status_report["message"] = f"Error saving empty calculatedprices.json: {str(e)}"
                mt5.shutdown()
                return False, status_report
        
        try:
            with open(output_json_path, 'w') as f:
                json.dump(calculated_prices, f, indent=4)
            log_and_print(
                f"Saved {len(calculated_prices)} calculated price entries to {output_json_path} for {market} {timeframe}",
                "SUCCESS"
            )
            # Verify saved file
            with open(output_json_path, 'r') as f:
                saved_data = json.load(f)
            status_report["verified_order_count"] = len(saved_data)
            status_report["status"] = "success"
            status_report["message"] = f"Saved {len(calculated_prices)} calculated price entries for {market} {timeframe}"
            mt5.shutdown()
            return True, status_report
        except Exception as e:
            error_log.append({
                "timestamp": status_report["timestamp"],
                "market": market,
                "timeframe": timeframe,
                "error": f"Error saving calculatedprices.json: {str(e)}"
            })
            save_errors()
            log_and_print(f"Error saving calculatedprices.json for {market} {timeframe}: {str(e)}", "ERROR")
            status_report["message"] = f"Error saving calculatedprices.json: {str(e)}"
            mt5.shutdown()
            return False, status_report
    
    except Exception as e:
        error_log.append({
            "timestamp": status_report["timestamp"],
            "market": market,
            "timeframe": timeframe,
            "error": f"Unexpected error processing order holder prices: {str(e)}"
        })
        save_errors()
        log_and_print(f"Error processing order holder prices for {market} {timeframe}: {str(e)}", "ERROR")
        status_report["message"] = f"Unexpected error: {str(e)}"
        mt5.shutdown()
        return False, status_report


def PendingOrderUpdater(market: str, timeframe: str, json_dir: str) -> tuple[bool, dict]:
    """Update pending orders in pricecandle.json based on calculatedprices.json, handling duplicates."""
    log_and_print(f"Updating pending orders for market={market}, timeframe={timeframe}", "INFO")
    
    # Initialize status report
    status_report = {
        "market": market,
        "timeframe": timeframe,
        "status": "failed",
        "message": "",
        "orders_updated": 0,
        "warnings": [],
        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
        "verified_order_count": 0
    }
    
    # Define file paths
    pricecandle_json_path = os.path.join(json_dir, "pricecandle.json")
    calculatedprices_json_path = os.path.join(json_dir, "calculatedprices.json")
    
    # Check if required JSON files exist
    if not os.path.exists(pricecandle_json_path):
        log_and_print(f"pricecandle.json not found at {pricecandle_json_path} for {market} {timeframe}", "ERROR")
        status_report["message"] = f"pricecandle.json not found at {pricecandle_json_path}"
        return False, status_report
    if not os.path.exists(calculatedprices_json_path):
        log_and_print(f"calculatedprices.json not found at {calculatedprices_json_path} for {market} {timeframe}", "ERROR")
        status_report["message"] = f"calculatedprices.json not found at {calculatedprices_json_path}"
        return False, status_report
    
    try:
        # Load pricecandle.json
        with open(pricecandle_json_path, 'r') as f:
            pricecandle_data = json.load(f)
        
        # Load calculatedprices.json
        with open(calculatedprices_json_path, 'r') as f:
            calculatedprices_data = json.load(f)
        
        # Prepare updated pricecandle data
        updated_pricecandle_data = []
        duplicates_removed = 0
        
        # Process each trendline in pricecandle
        for pricecandle_trendline in pricecandle_data:
            order_type = pricecandle_trendline.get("receiver", {}).get("order_type", "").lower()
            order_holder = pricecandle_trendline.get("order_holder", {})
            actual_price = order_holder.get("High" if order_type == "long" else "Low", 0)
            
            # Find matching calculatedprices entry
            matching_calculated = None
            if order_type == "long":
                for calc_entry in calculatedprices_data:
                    if (calc_entry.get("order_type") == "buy_limit" and 
                        abs(calc_entry.get("entry_price") - actual_price) < 1e-5):
                        matching_calculated = calc_entry
                        break
            elif order_type == "short":
                for calc_entry in calculatedprices_data:
                    if (calc_entry.get("order_type") == "sell_limit" and 
                        abs(calc_entry.get("entry_price") - actual_price) < 1e-5):
                        matching_calculated = calc_entry
                        break
            
            # Update pending order
            if matching_calculated:
                pricecandle_trendline["pending order"] = {
                    "status": f"{matching_calculated.get('order_type', 'unknown')} {matching_calculated.get('entry_price', 'N/A')}"
                }
                updated_pricecandle_data.append(pricecandle_trendline)
                status_report["orders_updated"] += 1
            else:
                log_and_print(
                    f"No matching calculated price for trendline {pricecandle_trendline.get('type')} in {market} {timeframe}. "
                    f"Expected: {actual_price}. Skipping.",
                    "INFO"
                )
                status_report["warnings"].append(f"No matching calculated price for trendline {pricecandle_trendline.get('type')}")
                continue
        
        # Check for duplicates based on entry_price and keep the oldest
        final_pricecandle_data = []
        seen_entry_prices = {}
        
        for trendline in updated_pricecandle_data:
            order_type = trendline.get("receiver", {}).get("order_type", "").lower()
            order_holder = trendline.get("order_holder", {})
            entry_price = order_holder.get("High" if order_type == "long" else "Low", 0)
            order_time = order_holder.get("Time", "")
            
            if entry_price and order_time:
                if entry_price in seen_entry_prices:
                    existing_time = seen_entry_prices[entry_price]["Time"]
                    if order_time < existing_time:
                        seen_entry_prices[entry_price] = {
                            "trendline": trendline,
                            "Time": order_time
                        }
                        log_and_print(
                            f"Duplicate pending order detected for trendline {trendline.get('type')} with entry_price {entry_price} in {market} {timeframe}. "
                            f"Keeping older entry at {order_time}.",
                            "INFO"
                        )
                        duplicates_removed += 1
                    else:
                        log_and_print(
                            f"Duplicate pending order detected for trendline {trendline.get('type')} with entry_price {entry_price} in {market} {timeframe}. "
                            f"Discarding newer entry at {order_time}.",
                            "INFO"
                        )
                        duplicates_removed += 1
                        continue
                else:
                    seen_entry_prices[entry_price] = {
                        "trendline": trendline,
                        "Time": order_time
                    }
            final_pricecandle_data.append(trendline)
        
        # Save updated pricecandle.json
        if os.path.exists(pricecandle_json_path):
            os.remove(pricecandle_json_path)
            log_and_print(f"Existing {pricecandle_json_path} deleted", "INFO")
        
        try:
            with open(pricecandle_json_path, 'w') as f:
                json.dump(final_pricecandle_data, f, indent=4)
            log_and_print(f"Updated pricecandle.json with {len(final_pricecandle_data)} entries for {market} {timeframe}", "SUCCESS")
            # Verify saved file
            with open(pricecandle_json_path, 'r') as f:
                saved_data = json.load(f)
            status_report["verified_order_count"] = len(saved_data)
            status_report["status"] = "success"
            status_report["message"] = f"Updated {len(final_pricecandle_data)} entries, removed {duplicates_removed} duplicates"
            return True, status_report
        except Exception as e:
            log_and_print(f"Error saving final pricecandle.json for {market} {timeframe}: {e}", "ERROR")
            status_report["message"] = f"Error saving final pricecandle.json: {str(e)}"
            return False, status_report
    
    except Exception as e:
        log_and_print(f"Error processing pending order updates for {market} {timeframe}: {e}", "ERROR")
        status_report["message"] = f"Unexpected error: {str(e)}"
        return False, status_report
def collect_all_pending_orders(market: str, timeframe: str, json_dir: str) -> tuple[bool, dict]:
    """Collect all pending orders from pricecandle.json and fetchedpendingorders.json for a specific market and timeframe,
    save to contractpendingorders.json, and aggregate across all markets and timeframes to temp_pendingorders.json."""
    log_and_print(f"Collecting pending orders for market={market}, timeframe={timeframe}", "INFO")
    
    # Initialize status report
    status_report = {
        "market": market,
        "timeframe": timeframe,
        "status": "failed",
        "message": "",
        "pending_orders_collected": 0,
        "warnings": [],
        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
        "verified_pending_count": 0,
        "total_collective_pending": 0
    }
    
    # Define file paths
    pricecandle_json_path = os.path.join(json_dir, "pricecandle.json")
    calculatedprices_json_path = os.path.join(json_dir, "calculatedprices.json")
    fetchedpendingorders_json_path = os.path.join(json_dir, "fetchedpendingorders.json")
    pending_orders_json_path = os.path.join(json_dir, "contractpendingorders.json")
    collective_pending_path = os.path.join(BASE_OUTPUT_FOLDER, "temp_pendingorders.json")
    
    # Check if required JSON files exist
    if not os.path.exists(pricecandle_json_path):
        log_and_print(f"pricecandle.json not found at {pricecandle_json_path} for {market} {timeframe}", "ERROR")
        status_report["message"] = f"pricecandle.json not found at {pricecandle_json_path}"
        return False, status_report
    if not os.path.exists(calculatedprices_json_path):
        log_and_print(f"calculatedprices.json not found at {calculatedprices_json_path} for {market} {timeframe}", "ERROR")
        status_report["message"] = f"calculatedprices.json not found at {calculatedprices_json_path}"
        return False, status_report
    
    try:
        # Load pricecandle.json
        with open(pricecandle_json_path, 'r') as f:
            pricecandle_data = json.load(f)
        
        # Load calculatedprices.json
        with open(calculatedprices_json_path, 'r') as f:
            calculatedprices_data = json.load(f)
        
        # Load fetchedpendingorders.json if it exists
        fetched_pending_orders = []
        if os.path.exists(fetchedpendingorders_json_path):
            try:
                with open(fetchedpendingorders_json_path, 'r') as f:
                    fetched_data = json.load(f)
                if isinstance(fetched_data, dict) and "orders" in fetched_data:
                    fetched_pending_orders = fetched_data["orders"]
                    log_and_print(f"Loaded {len(fetched_pending_orders)} orders from fetchedpendingorders.json for {market} {timeframe}", "INFO")
                else:
                    log_and_print(f"Invalid data format in fetchedpendingorders.json for {market} {timeframe}", "WARNING")
                    status_report["warnings"].append("Invalid data format in fetchedpendingorders.json")
            except Exception as e:
                log_and_print(f"Error reading fetchedpendingorders.json for {market} {timeframe}: {str(e)}", "WARNING")
                status_report["warnings"].append(f"Error reading fetchedpendingorders.json: {str(e)}")
        
        # Extract pending orders from pricecandle.json
        contract_pending_orders = []
        seen_entry_prices = {}
        skipped_reasons = {}
        
        # Process pricecandle.json orders
        for trendline in pricecandle_data:
            pending_order = trendline.get("pending order", {})
            trendline_type = trendline.get("type")
            order_holder = trendline.get("order_holder", {})
            order_holder_position = order_holder.get("position_number")
            order_holder_timestamp = order_holder.get("Time", "N/A")
            order_type = trendline.get("receiver", {}).get("order_type", "").lower()
            contract_status = trendline.get("contract status summary", {}).get("contract status", "")
            
            if contract_status in ["profit reached exit contract", "Exit contract at stoploss"]:
                skipped_reasons[trendline_type] = f"Skipped due to executed contract status: {contract_status}"
                log_and_print(
                    f"Skipping trendline {trendline_type} in {market} {timeframe} due to contract status: {contract_status}",
                    "INFO"
                )
                status_report["warnings"].append(f"Skipped trendline {trendline_type} due to contract status: {contract_status}")
                continue
            
            if not pending_order or "status" not in pending_order:
                skipped_reasons[trendline_type] = "No valid pending order or missing status"
                log_and_print(f"No valid pending order for trendline {trendline_type} in {market} {timeframe}", "INFO")
                status_report["warnings"].append(f"No valid pending order for trendline {trendline_type}")
                continue
            
            actual_price = order_holder.get("High" if order_type == "long" else "Low", 0)
            if actual_price == 0:
                skipped_reasons[trendline_type] = "Invalid order_holder price"
                log_and_print(f"Invalid order_holder price for trendline {trendline_type} in {market} {timeframe}", "WARNING")
                status_report["warnings"].append(f"Invalid order_holder price for trendline {trendline_type}")
                continue
            
            matching_calculated = None
            price_tolerance = 1e-3
            for calc_entry in calculatedprices_data:
                calc_order_type = calc_entry.get("order_type", "").lower()
                calc_entry_price = calc_entry.get("entry_price", 0)
                if (calc_order_type == ("buy_limit" if order_type == "long" else "sell_limit") and 
                    abs(calc_entry_price - actual_price) < price_tolerance):
                    matching_calculated = calc_entry
                    log_and_print(
                        f"Matched trendline {trendline_type}: pricecandle_entry={actual_price}, "
                        f"calc_entry={calc_entry_price}, price_diff={abs(calc_entry_price - actual_price)}, "
                        f"order_type={order_type}",
                        "DEBUG"
                    )
                    break
            
            if not matching_calculated:
                skipped_reasons[trendline_type] = f"No matching calculated prices for entry_price={actual_price}, order_type={order_type}"
                log_and_print(
                    f"No matching calculated prices found for trendline {trendline_type} with entry_price {actual_price} in {market} {timeframe}",
                    "WARNING"
                )
                status_report["warnings"].append(f"No matching calculated prices for trendline {trendline_type}")
                continue
            
            calc_order_type = matching_calculated.get("order_type").lower()
            calc_entry_price = matching_calculated.get("entry_price")
            if calc_order_type != ("buy_limit" if order_type == "long" else "sell_limit"):
                skipped_reasons[trendline_type] = f"Order type mismatch: pricecandle={order_type}, calculatedprices={calc_order_type}"
                log_and_print(
                    f"Order type mismatch for trendline {trendline_type} in {market} {timeframe}: "
                    f"pricecandle={order_type}, calculatedprices={calc_order_type}",
                    "WARNING"
                )
                status_report["warnings"].append(f"Order type mismatch for trendline {trendline_type}")
                continue
            if abs(calc_entry_price - actual_price) > price_tolerance:
                skipped_reasons[trendline_type] = f"Entry price mismatch: pricecandle={actual_price}, calculatedprices={calc_entry_price}"
                log_and_print(
                    f"Entry price mismatch for trendline {trendline_type} in {market} {timeframe}: "
                    f"pricecandle={actual_price}, calculatedprices={calc_entry_price}",
                    "WARNING"
                )
                status_report["warnings"].append(f"Entry price mismatch for trendline {trendline_type}")
                continue
            
            if actual_price in seen_entry_prices:
                existing_time = seen_entry_prices[actual_price]["order_holder_timestamp"]
                if order_holder_timestamp < existing_time:
                    seen_entry_prices[actual_price] = {
                        "trendline": trendline,
                        "matching_calculated": matching_calculated,
                        "order_holder_timestamp": order_holder_timestamp
                    }
                    log_and_print(
                        f"Duplicate pending order detected for trendline {trendline_type} with entry_price {actual_price} in {market} {timeframe}. "
                        f"Keeping older entry at {order_holder_timestamp}.",
                        "INFO"
                    )
                else:
                    skipped_reasons[trendline_type] = f"Duplicate entry_price {actual_price}, newer timestamp {order_holder_timestamp}"
                    log_and_print(
                        f"Duplicate pending order detected for trendline {trendline_type} with entry_price {actual_price} in {market} {timeframe}. "
                        f"Discarding newer entry at {order_holder_timestamp}.",
                        "INFO"
                    )
                    status_report["warnings"].append(f"Duplicate pending order for trendline {trendline_type}")
                    continue
            else:
                seen_entry_prices[actual_price] = {
                    "trendline": trendline,
                    "matching_calculated": matching_calculated,
                    "order_holder_timestamp": order_holder_timestamp
                }
        
        # Process fetchedpendingorders.json orders
        for fetched_order in fetched_pending_orders:
            order_type = fetched_order.get("order_type", "").lower()
            actual_price = fetched_order.get("entry_price", 0)
            order_holder_timestamp = fetched_order.get("created_at", "N/A")
            
            if actual_price == 0:
                skipped_reasons[f"fetched_order_{order_holder_timestamp}"] = "Invalid entry price"
                log_and_print(f"Invalid entry price for fetched order at {order_holder_timestamp} in {market} {timeframe}", "WARNING")
                status_report["warnings"].append(f"Invalid entry price for fetched order at {order_holder_timestamp}")
                continue
            
            matching_calculated = None
            price_tolerance = 1e-3
            for calc_entry in calculatedprices_data:
                calc_order_type = calc_entry.get("order_type", "").lower()
                calc_entry_price = calc_entry.get("entry_price", 0)
                if calc_order_type == order_type and abs(calc_entry_price - actual_price) < price_tolerance:
                    matching_calculated = calc_entry
                    log_and_print(
                        f"Matched fetched order: fetched_entry={actual_price}, "
                        f"calc_entry={calc_entry_price}, price_diff={abs(calc_entry_price - actual_price)}, "
                        f"order_type={order_type}",
                        "DEBUG"
                    )
                    break
            
            if not matching_calculated:
                skipped_reasons[f"fetched_order_{order_holder_timestamp}"] = f"No matching calculated prices for entry_price={actual_price}, order_type={order_type}"
                log_and_print(
                    f"No matching calculated prices found for fetched order at {order_holder_timestamp} with entry_price {actual_price} in {market} {timeframe}",
                    "WARNING"
                )
                status_report["warnings"].append(f"No matching calculated prices for fetched order at {order_holder_timestamp}")
                continue
            
            if matching_calculated.get("order_type").lower() != order_type:
                skipped_reasons[f"fetched_order_{order_holder_timestamp}"] = f"Order type mismatch: fetched={order_type}, calculatedprices={matching_calculated.get('order_type')}"
                log_and_print(
                    f"Order type mismatch for fetched order at {order_holder_timestamp} in {market} {timeframe}: "
                    f"fetched={order_type}, calculatedprices={matching_calculated.get('order_type')}",
                    "WARNING"
                )
                status_report["warnings"].append(f"Order type mismatch for fetched order at {order_holder_timestamp}")
                continue
            
            if abs(matching_calculated.get("entry_price") - actual_price) > price_tolerance:
                skipped_reasons[f"fetched_order_{order_holder_timestamp}"] = f"Entry price mismatch: fetched={actual_price}, calculatedprices={matching_calculated.get('entry_price')}"
                log_and_print(
                    f"Entry price mismatch for fetched order at {order_holder_timestamp} in {market} {timeframe}: "
                    f"fetched={actual_price}, calculatedprices={matching_calculated.get('entry_price')}",
                    "WARNING"
                )
                status_report["warnings"].append(f"Entry price mismatch for fetched order at {order_holder_timestamp}")
                continue
            
            if actual_price in seen_entry_prices:
                existing_time = seen_entry_prices[actual_price]["order_holder_timestamp"]
                if order_holder_timestamp < existing_time:
                    seen_entry_prices[actual_price] = {
                        "fetched_order": fetched_order,
                        "matching_calculated": matching_calculated,
                        "order_holder_timestamp": order_holder_timestamp
                    }
                    log_and_print(
                        f"Duplicate pending order detected for fetched order with entry_price {actual_price} in {market} {timeframe}. "
                        f"Keeping older entry at {order_holder_timestamp}.",
                        "INFO"
                    )
                else:
                    skipped_reasons[f"fetched_order_{order_holder_timestamp}"] = f"Duplicate entry_price {actual_price}, newer timestamp {order_holder_timestamp}"
                    log_and_print(
                        f"Duplicate pending order detected for fetched order with entry_price {actual_price} in {market} {timeframe}. "
                        f"Discarding newer entry at {order_holder_timestamp}.",
                        "INFO"
                    )
                    status_report["warnings"].append(f"Duplicate pending order for fetched order at {order_holder_timestamp}")
                    continue
            else:
                seen_entry_prices[actual_price] = {
                    "fetched_order": fetched_order,
                    "matching_calculated": matching_calculated,
                    "order_holder_timestamp": order_holder_timestamp
                }
        
        # Process all unique orders (from pricecandle and fetchedpendingorders)
        for entry in seen_entry_prices.values():
            trendline = entry.get("trendline")
            fetched_order = entry.get("fetched_order")
            matching_calculated = entry["matching_calculated"]
            order_holder_timestamp = entry["order_holder_timestamp"]
            
            if trendline:
                trendline_type = trendline.get("type")
                order_holder = trendline.get("order_holder", {})
                order_holder_position = order_holder.get("position_number")
                order_type = trendline.get("receiver", {}).get("order_type", "").lower()
                trendline_type = "ph-to-ph" if order_type == "long" else "pl-to-pl"
                contract_entry = {
                    "market": market,
                    "pair": matching_calculated.get("pair", market),
                    "timeframe": timeframe,
                    "order_type": matching_calculated.get("order_type"),
                    "entry_price": matching_calculated.get("entry_price", 0.0),
                    "exit_price": matching_calculated.get("exit_price", 0.0),
                    "1:0.5_price": matching_calculated.get("1:0.5_price", 0.0),
                    "1:1_price": matching_calculated.get("1:1_price", 0.0),
                    "1:2_price": matching_calculated.get("1:2_price", 0.0),
                    "profit_price": matching_calculated.get("profit_price", 0.0),
                    "lot_size": matching_calculated.get("lot_size", 0.0),
                    "trendline_type": trendline_type,
                    "order_holder_position": order_holder_position,
                    "order_holder_timestamp": order_holder_timestamp
                }
            else:
                order_type = fetched_order.get("order_type", "").lower()
                trendline_type = "ph-to-ph" if order_type == "buy_limit" else "pl-to-pl"
                contract_entry = {
                    "market": fetched_order.get("market", market),
                    "pair": fetched_order.get("pair", market),
                    "timeframe": fetched_order.get("timeframe", timeframe),
                    "order_type": fetched_order.get("order_type"),
                    "entry_price": fetched_order.get("entry_price", 0.0),
                    "exit_price": fetched_order.get("exit_price", 0.0),
                    "1:0.5_price": fetched_order.get("1:0.5_price", 0.0),
                    "1:1_price": fetched_order.get("1:1_price", 0.0),
                    "1:2_price": fetched_order.get("1:2_price", 0.0),
                    "profit_price": fetched_order.get("profit_price", 0.0),
                    "lot_size": matching_calculated.get("lot_size", 0.0),
                    "trendline_type": trendline_type,
                    "order_holder_position": None,
                    "order_holder_timestamp": order_holder_timestamp
                }
            
            if any(price <= 0 for price in [
                contract_entry["entry_price"],
                contract_entry["exit_price"],
                contract_entry["1:0.5_price"],
                contract_entry["1:1_price"],
                contract_entry["1:2_price"],
                contract_entry["profit_price"]
            ]):
                key = trendline_type if trendline else f"fetched_order_{order_holder_timestamp}"
                skipped_reasons[key] = f"Invalid price values: {contract_entry}"
                log_and_print(f"Invalid price values for {key} in {market} {timeframe}: {contract_entry}", "WARNING")
                status_report["warnings"].append(f"Invalid price values for {key}")
                continue
            
            if contract_entry["lot_size"] <= 0:
                key = trendline_type if trendline else f"fetched_order_{order_holder_timestamp}"
                skipped_reasons[key] = f"Invalid lot_size {contract_entry['lot_size']}"
                log_and_print(f"Invalid lot_size {contract_entry['lot_size']} for {key} in {market} {timeframe}", "WARNING")
                status_report["warnings"].append(f"Invalid lot_size for {key}")
                continue
            
            contract_pending_orders.append(contract_entry)
            status_report["pending_orders_collected"] += 1
            log_and_print(
                f"Added pending order for {trendline_type if trendline else 'fetched_order'}: order_type={contract_entry['order_type']}, "
                f"entry_price={contract_entry['entry_price']}, exit_price={contract_entry['exit_price']}, "
                f"lot_size={contract_entry['lot_size']} in {market} {timeframe}",
                "DEBUG"
            )
        
        if skipped_reasons:
            log_and_print(f"Skipped entries in {market} {timeframe}: {skipped_reasons}", "INFO")
            status_report["warnings"].extend([f"Skipped {k}: {v}" for k, v in skipped_reasons.items()])
        if not contract_pending_orders:
            log_and_print(f"No pending orders collected for {market} {timeframe}. Skipped reasons: {skipped_reasons}", "WARNING")
            status_report["message"] = "No pending orders collected"
        
        try:
            with open(pending_orders_json_path, 'w') as f:
                json.dump(contract_pending_orders, f, indent=4)
            log_and_print(
                f"Saved {len(contract_pending_orders)} pending orders to {pending_orders_json_path} for {market} {timeframe}",
                "SUCCESS"
            )
            status_report["verified_pending_count"] = len(contract_pending_orders)
        except Exception as e:
            log_and_print(f"Error saving contractpendingorders.json for {market} {timeframe}: {str(e)}", "ERROR")
            status_report["message"] = f"Error saving contractpendingorders.json: {str(e)}"
            return False, status_report
        
        all_pending_orders = []
        timeframe_counts_pending = {
            "5minutes": 0,
            "15minutes": 0,
            "30minutes": 0,
            "1Hour": 0,
            "4Hour": 0
        }
        
        for mkt in MARKETS:
            formatted_market = mkt.replace(" ", "_")
            for tf in TIMEFRAMES:
                tf_dir = os.path.join(BASE_OUTPUT_FOLDER, formatted_market, tf.lower())
                pending_path = os.path.join(tf_dir, "contractpendingorders.json")
                db_tf = DB_TIMEFRAME_MAPPING.get(tf, tf)
                
                if os.path.exists(pending_path):
                    try:
                        with open(pending_path, 'r') as f:
                            pending_data = json.load(f)
                        if isinstance(pending_data, list):
                            all_pending_orders.extend(pending_data)
                            timeframe_counts_pending[db_tf] += len(pending_data)
                        else:
                            log_and_print(f"Invalid data format in {pending_path}: Expected list, got {type(pending_data)}", "WARNING")
                            status_report["warnings"].append(f"Invalid data format in {pending_path}")
                    except Exception as e:
                        log_and_print(f"Error reading {pending_path}: {str(e)}", "WARNING")
                        status_report["warnings"].append(f"Error reading {pending_path}: {str(e)}")
        
        pending_output = {
            "temp_pendingorders": len(all_pending_orders),
            "5minutes pending orders": timeframe_counts_pending["5minutes"],
            "15minutes pending orders": timeframe_counts_pending["15minutes"],
            "30minutes pending orders": timeframe_counts_pending["30minutes"],
            "1Hour pending orders": timeframe_counts_pending["1Hour"],
            "4Hours pending orders": timeframe_counts_pending["4Hour"],
            "orders": all_pending_orders
        }
        
        try:
            with open(collective_pending_path, 'w') as f:
                json.dump(pending_output, f, indent=4)
            log_and_print(
                f"Saved {len(all_pending_orders)} pending orders to {collective_pending_path} "
                f"(5m: {timeframe_counts_pending['5minutes']}, 15m: {timeframe_counts_pending['15minutes']}, "
                f"30m: {timeframe_counts_pending['30minutes']}, 1H: {timeframe_counts_pending['1Hour']}, "
                f"4H: {timeframe_counts_pending['4Hour']})",
                "SUCCESS"
            )
            status_report["total_collective_pending"] = len(all_pending_orders)
            status_report["status"] = "success"
            status_report["message"] = f"Saved {len(contract_pending_orders)} pending orders for {market} {timeframe}"
            return True, status_report
        except Exception as e:
            log_and_print(f"Error saving temp_pendingorders.json: {str(e)}", "ERROR")
            status_report["message"] = f"Error saving temp_pendingorders.json: {str(e)}"
            return False, status_report
    
    except Exception as e:
        log_and_print(f"Error collecting pending orders for {market} {timeframe}: {str(e)}", "ERROR")
        status_report["message"] = f"Unexpected error: {str(e)}"
        return False, status_report

def move_fetchedpendingordersto_temppendingorders():
    # File paths
    pending_orders_path = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\orders\fetchedpendingorders.json"
    lotsizes_path = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\orders\lotsizes.json"
    temp_pendingorders_path = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\orders\temp_pendingorders.json"
    
    try:
        # Read pending orders
        with open(pending_orders_path, 'r') as f:
            pending_orders_data = json.load(f)
        
        # Read lotsizes
        with open(lotsizes_path, 'r') as f:
            lotsizes_data = json.load(f)
        
        # Read temp_pendingorders (or initialize if it doesn't exist)
        try:
            with open(temp_pendingorders_path, 'r') as f:
                temp_pendingorders_data = json.load(f)
        except FileNotFoundError:
            temp_pendingorders_data = {
                "temp_pendingorders": 0,
                "5minutes pending orders": 0,
                "15minutes pending orders": 0,
                "30minutes pending orders": 0,
                "1Hour pending orders": 0,
                "4Hours pending orders": 0,
                "orders": []
            }
        
        # Create a mapping of pair-timeframe to lot_size
        lotsize_map = {}
        for lotsize in lotsizes_data:
            # Normalize timeframe format (e.g., "5minutes" to "M5")
            timeframe = lotsize['timeframe']
            if timeframe == "5minutes":
                timeframe = "M5"
            elif timeframe == "15minutes":
                timeframe = "M15"
            elif timeframe == "30minutes":
                timeframe = "M30"
            elif timeframe == "1hour":
                timeframe = "H1"
            elif timeframe == "4hours":
                timeframe = "H4"
            key = (lotsize['pair'], timeframe)
            lotsize_map[key] = lotsize['lot_size']
        
        # Process orders
        new_orders = []
        for order in pending_orders_data['orders']:
            # Get the pair and timeframe
            pair = order['pair']
            timeframe = order['timeframe']
            
            # Find matching lot size
            lot_size = lotsize_map.get((pair, timeframe), 0.01)  # Default to 0.01 if not found
            
            # Determine trendline_type based on order_type
            trendline_type = 'pl-to-pl' if order['order_type'] == 'sell_limit' else 'ph-to-ph' if order['order_type'] == 'buy_limit' else ''
            
            # Create new order dictionary with fields in desired order
            new_order = {
                'market': order['market'],
                'pair': order['pair'],
                'timeframe': order['timeframe'],
                'order_type': order['order_type'],
                'entry_price': order['entry_price'],
                'exit_price': order['exit_price'],
                '1:0.5_price': order['1:0.5_price'],
                '1:1_price': order['1:1_price'],
                '1:2_price': order['1:2_price'],
                'profit_price': order['profit_price'],
                'lot_size': lot_size,
                'trendline_type': trendline_type,
                'order_holder_timestamp': order['created_at']
            }
            new_orders.append(new_order)
        
        # Append new orders to temp_pendingorders
        temp_pendingorders_data['orders'].extend(new_orders)
        
        # Update summary counts
        temp_pendingorders_data['temp_pendingorders'] += pending_orders_data['summary']['total_valid_orders']
        temp_pendingorders_data['5minutes pending orders'] += pending_orders_data['summary']['5m_valid_orders']
        temp_pendingorders_data['15minutes pending orders'] += pending_orders_data['summary']['15m_valid_orders']
        temp_pendingorders_data['30minutes pending orders'] += pending_orders_data['summary']['30m_valid_orders']
        temp_pendingorders_data['1Hour pending orders'] += pending_orders_data['summary']['1h_valid_orders']
        temp_pendingorders_data['4Hours pending orders'] += pending_orders_data['summary']['4h_valid_orders']
        
        # Save to temp_pendingorders file
        os.makedirs(os.path.dirname(temp_pendingorders_path), exist_ok=True)
        with open(temp_pendingorders_path, 'w') as f:
            json.dump(temp_pendingorders_data, f, indent=4)
        
        print(f"Successfully appended orders to {temp_pendingorders_path}")
        
    except FileNotFoundError as e:
        print(f"Error: File not found - {e}")
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format - {e}")
    except Exception as e:
        print(f"Error: {e}")


def validatesignals():
    """Initialize MT5, fetch available symbols, validate signals from temp_pendingorders.json, place limit orders for valid signals, and categorize as valid or invalid price."""
    log_and_print("===== Verifying Signals with Server =====", "TITLE")
    
    # Define MT5 credentials and terminal path
    TERMINAL_PATH = r"C:\Program Files\MetaTrader 5\terminal64.exe"
    LOGIN_ID = "101347351"
    PASSWORD = "@Techknowdge12#"
    SERVER = "DerivSVG-Server-02"
    
    # Define paths
    BASE_OUTPUT_FOLDER = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\orders"
    BASE_ERROR_FOLDER = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\orders\debugs"
    signals_json_path = os.path.join(BASE_OUTPUT_FOLDER, "temp_pendingorders.json")
    valid_json_path = os.path.join(BASE_OUTPUT_FOLDER, "validpendingorders.json")
    valid_pairs_path = os.path.join(BASE_OUTPUT_FOLDER, "validpendingpairs.json")
    invalid_json_path = os.path.join(BASE_OUTPUT_FOLDER, "invalidexecutedorders.json")
    
    # Initialize error log list
    error_log = []
    error_json_path = os.path.join(BASE_ERROR_FOLDER, "validatesignalserror.json")
    
    # Helper function to save errors to JSON
    def save_errors():
        try:
            os.makedirs(BASE_ERROR_FOLDER, exist_ok=True)
            with open(error_json_path, 'w') as f:
                json.dump(error_log, f, indent=4)
            log_and_print(f"Errors saved to {error_json_path}", "INFO")
        except Exception as e:
            log_and_print(f"Failed to save errors to {error_json_path}: {str(e)}", "ERROR")
    
    # Helper function to normalize timeframe
    def normalize_timeframe(timeframe):
        timeframe = timeframe.lower().replace('minutes', 'm').replace('hour', 'h').replace('hours', 'h')
        timeframe_map = {
            'm5': '5m',
            'm15': '15m',
            'm30': '30m',
            'h1': '1h',
            'h4': '4h'
        }
        return timeframe_map.get(timeframe, timeframe)
    
    # Verify terminal executable exists
    if not os.path.exists(TERMINAL_PATH):
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "error": f"MT5 terminal executable not found at {TERMINAL_PATH}"
        })
        save_errors()
        log_and_print(f"MT5 terminal executable not found at {TERMINAL_PATH}", "ERROR")
        return 0, 0, 0, 0, 0, 0, 0
    
    # Read temp_pendingorders.json
    signals_data = {}
    try:
        if os.path.exists(signals_json_path):
            with open(signals_json_path, 'r') as f:
                signals_data = json.load(f)
            log_and_print(f"Successfully loaded signals from {signals_json_path}", "INFO")
        else:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"temp_pendingorders.json not found at {signals_json_path}"
            })
            save_errors()
            log_and_print(f"temp_pendingorders.json not found at {signals_json_path}", "ERROR")
            return 0, 0, 0, 0, 0, 0, 0
    except Exception as e:
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "error": f"Error reading {signals_json_path}: {str(e)}"
        })
        save_errors()
        log_and_print(f"Error reading {signals_json_path}: {str(e)}", "ERROR")
        return 0, 0, 0, 0, 0, 0, 0
    
    # Validate signals_data structure
    if not isinstance(signals_data, dict) or 'orders' not in signals_data or not isinstance(signals_data['orders'], list):
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "error": f"Invalid structure in temp_pendingorders.json: Expected dict with 'orders' list"
        })
        save_errors()
        log_and_print(f"Invalid structure in temp_pendingorders.json: Expected dict with 'orders' list", "ERROR")
        return 0, 0, 0, 0, 0, 0, 0
    
    # Extract unique pairs from signals (case-insensitive)
    signal_pairs = set()
    for order in signals_data['orders']:
        pair = order.get('pair', '').replace(' ', '').lower()
        if pair:
            signal_pairs.add(pair)
    log_and_print(f"Found {len(signal_pairs)} unique pairs in temp_pendingorders.json", "INFO")
    
    # Initialize MT5
    try:
        if not mt5.initialize(
            path=TERMINAL_PATH,
            login=int(LOGIN_ID),
            server=SERVER,
            password=PASSWORD,
            timeout=30000
        ):
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Failed to initialize MT5: {mt5.last_error()}"
            })
            save_errors()
            log_and_print(f"Failed to initialize MT5: {mt5.last_error()}", "ERROR")
            return 0, 0, 0, 0, 0, 0, 0
        
        # Verify login
        if not mt5.login(
            login=int(LOGIN_ID),
            server=SERVER,
            password=PASSWORD
        ):
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Failed to login to MT5: {mt5.last_error()}"
            })
            save_errors()
            log_and_print(f"Failed to login to MT5: {mt5.last_error()}", "ERROR")
            mt5.shutdown()
            return 0, 0, 0, 0, 0, 0, 0
        
        log_and_print(f"Successfully initialized and logged into MT5 (loginid={LOGIN_ID}, server={SERVER})", "SUCCESS")
        
        # Fetch available symbols
        symbols = mt5.symbols_get()
        if not symbols:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Failed to retrieve symbols: {mt5.last_error()}"
            })
            save_errors()
            log_and_print(f"Failed to retrieve symbols: {mt5.last_error()}", "ERROR")
            mt5.shutdown()
            return 0, 0, 0, 0, 0, 0, 0
        
        total_symbols = len(symbols)
        log_and_print(f"Retrieved {total_symbols} symbols from the broker", "INFO")
        
        # Initialize counters and lists
        matched_pairs = set()
        unmatched_pairs = set()
        selected_pairs = set()
        unable_to_select_pairs = set()
        valid_orders = []
        invalid_executed_orders = []
        orders_placed = 0
        timeframe_counts = {
            "4h": 0,
            "1h": 0,
            "30m": 0,
            "15m": 0,
            "5m": 0
        }
        
        # Create a dictionary of broker symbols for matching (case-insensitive)
        broker_symbols_dict = {symbol.name.replace(' ', '').lower(): symbol.name for symbol in symbols}
        
        # Process each order, validate, and place limit orders only for valid signals
        log_and_print("===== Validation and filtering =====", "TITLE")
        total_orders = len(signals_data['orders'])
        for i, order in enumerate(signals_data['orders'], 1):
            log_and_print(f"Validating orders {i}/{total_orders}", "INFO")
            pair = order.get('pair', '').replace(' ', '').lower()
            original_symbol = broker_symbols_dict.get(pair)
            order_type = order.get('order_type', '').lower()
            entry_price = order.get('entry_price')
            exit_price = order.get('exit_price')  # Stop-loss
            lot_size = order.get('lot_size')
            timeframe = normalize_timeframe(order.get('timeframe', '').lower())
            
            # Validate order fields
            if not all([original_symbol, order_type in ['buy_limit', 'sell_limit'], isinstance(entry_price, (int, float)), isinstance(lot_size, (int, float))]):
                unmatched_pairs.add(pair)
                order_copy = order.copy()
                order_copy['reason'] = "Missing or invalid order fields (symbol, order_type, entry_price, or lot_size)"
                invalid_executed_orders.append(order_copy)
                continue
            
            # Select the symbol in Market Watch
            try:
                if mt5.symbol_select(original_symbol, True):
                    selected_pairs.add(pair)
                else:
                    unable_to_select_pairs.add(pair)
                    unmatched_pairs.add(pair)
                    order_copy = order.copy()
                    order_copy['reason'] = f"Failed to select symbol: {mt5.last_error()}"
                    invalid_executed_orders.append(order_copy)
                    continue
            except Exception as e:
                unable_to_select_pairs.add(pair)
                log_and_print(f"Error selecting symbol {original_symbol} for pair {pair}: {str(e)}", "ERROR")
                unmatched_pairs.add(pair)
                order_copy = order.copy()
                order_copy['reason'] = f"Error selecting symbol: {str(e)}"
                invalid_executed_orders.append(order_copy)
                continue
            
            # Get current market price (using last tick)
            tick = mt5.symbol_info_tick(original_symbol)
            if not tick:
                unmatched_pairs.add(pair)
                order_copy = order.copy()
                order_copy['reason'] = f"Failed to retrieve tick data: {mt5.last_error()}"
                invalid_executed_orders.append(order_copy)
                continue
            
            current_price = (tick.bid + tick.ask) / 2  # Use midpoint of bid/ask for comparison
            matched_pairs.add(pair)
            
            # Validate entry price
            is_valid_entry = True
            reason = ""
            if order_type == 'buy_limit' and current_price <= entry_price:
                is_valid_entry = False
                reason = "Current price <= Entry price for buy_limit"
            elif order_type == 'sell_limit' and current_price >= entry_price:
                is_valid_entry = False
                reason = "Current price >= Entry price for sell_limit"
            
            # Validate stop-loss (exit_price)
            if is_valid_entry and isinstance(exit_price, (int, float)):
                if order_type == 'buy_limit' and exit_price >= entry_price:
                    is_valid_entry = False
                    reason = "Stop-loss >= Entry price for buy_limit"
                elif order_type == 'sell_limit' and exit_price <= entry_price:
                    is_valid_entry = False
                    reason = "Stop-loss <= Entry price for sell_limit"
            
            # Categorize order before placing
            if not is_valid_entry:
                order_copy = order.copy()
                order_copy['reason'] = reason
                invalid_executed_orders.append(order_copy)
                continue
            
            # If entry is valid, proceed to place the order
            mt5_order_type = mt5.ORDER_TYPE_BUY_LIMIT if order_type == 'buy_limit' else mt5.ORDER_TYPE_SELL_LIMIT
            
            # Prepare order request
            request = {
                "action": mt5.TRADE_ACTION_PENDING,
                "symbol": original_symbol,
                "volume": float(lot_size),
                "type": mt5_order_type,
                "price": float(entry_price),
                "sl": float(exit_price) if isinstance(exit_price, (int, float)) else 0.0,  # Include stop-loss if provided
                "type_time": mt5.ORDER_TIME_GTC,  # Good Till Cancel
                "type_filling": mt5.ORDER_FILLING_IOC,  # Immediate or Cancel
            }
            
            # Place limit order
            result = mt5.order_send(request)
            if result.retcode == mt5.TRADE_RETCODE_DONE:
                orders_placed += 1
                valid_orders.append(order)
                # Increment timeframe count for normalized timeframe
                if timeframe in timeframe_counts:
                    timeframe_counts[timeframe] += 1
            else:
                unmatched_pairs.add(pair)
                order_copy = order.copy()
                order_copy['reason'] = f"Failed to place order: {result.comment}"
                invalid_executed_orders.append(order_copy)
                continue
        
        # Print newline to move to next line after progress updates
        print()
        
        # Create directories for outputs
        for dir_path in [BASE_OUTPUT_FOLDER, BASE_ERROR_FOLDER]:
            try:
                os.makedirs(dir_path, exist_ok=True)
                log_and_print(f"Created directory: {dir_path}", "INFO")
            except Exception as e:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Error creating directory {dir_path}: {str(e)}"
                })
                save_errors()
                log_and_print(f"Error creating directory {dir_path}: {str(e)}", "ERROR")
                mt5.shutdown()
                return total_symbols, len(matched_pairs), len(unmatched_pairs), len(selected_pairs), len(unable_to_select_pairs), len(valid_orders), len(invalid_executed_orders)
        
        # Save valid orders with updated summary
        try:
            valid_data = {
                "summary": {
                    "total_valid_orders": len(valid_orders),
                    "4h_valid_orders": timeframe_counts['4h'],
                    "1h_valid_orders": timeframe_counts['1h'],
                    "30m_valid_orders": timeframe_counts['30m'],
                    "15m_valid_orders": timeframe_counts['15m'],
                    "5m_valid_orders": timeframe_counts['5m']
                },
                "orders": valid_orders
            }
            with open(valid_json_path, 'w') as f:
                json.dump(valid_data, f, indent=4)
            log_and_print(f"Valid orders saved to {valid_json_path}", "SUCCESS")
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Error saving {valid_json_path}: {str(e)}"
            })
            save_errors()
            log_and_print(f"Error saving {valid_json_path}: {str(e)}", "ERROR")
        
        # Save valid pairs to separate file
        try:
            valid_pairs_data = {
                "valid_pairs": list(matched_pairs - unmatched_pairs)
            }
            with open(valid_pairs_path, 'w') as f:
                json.dump(valid_pairs_data, f, indent=4)
            log_and_print(f"Valid pairs saved to {valid_pairs_path}", "SUCCESS")
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Error saving {valid_pairs_path}: {str(e)}"
            })
            save_errors()
            log_and_print(f"Error saving {valid_pairs_path}: {str(e)}", "ERROR")
        
        # Save invalid executed orders
        try:
            invalid_data = {
                "summary": {
                    "total_invalid_executed_orders": len(invalid_executed_orders),
                    "invalid_pairs": list(unmatched_pairs)
                },
                "orders": invalid_executed_orders
            }
            with open(invalid_json_path, 'w') as f:
                json.dump(invalid_data, f, indent=4)
            log_and_print(f"Invalid executed orders saved to {invalid_json_path}", "SUCCESS")
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Error saving {invalid_json_path}: {str(e)}"
            })
            save_errors()
            log_and_print(f"Error saving {invalid_json_path}: {str(e)}", "ERROR")
        
        # Log summary
        log_and_print(f"Found {len(matched_pairs)} signal pairs matching broker symbols", "INFO")
        log_and_print(f"Found {len(unmatched_pairs)} signal pairs not matching broker symbols", "INFO")
        log_and_print(f"Selected {len(selected_pairs)} signal pairs out of {total_symbols} server symbols", "INFO")
        log_and_print(f"Unable to select {len(unable_to_select_pairs)} signal pairs out of {total_symbols} server symbols", "INFO")
        log_and_print(f"Total valid orders: {len(valid_orders)}", "INFO")
        log_and_print(f"Valid 4h orders: {timeframe_counts['4h']}", "INFO")
        log_and_print(f"Valid 1h orders: {timeframe_counts['1h']}", "INFO")
        log_and_print(f"Valid 30m orders: {timeframe_counts['30m']}", "INFO")
        log_and_print(f"Valid 15m orders: {timeframe_counts['15m']}", "INFO")
        log_and_print(f"Valid 5m orders: {timeframe_counts['5m']}", "INFO")
        log_and_print(f"Total invalid executed orders: {len(invalid_executed_orders)}", "INFO")
        log_and_print(f"Total orders placed: {orders_placed}", "INFO")
        
        # Shutdown MT5
        mt5.shutdown()
        return total_symbols, len(matched_pairs), len(unmatched_pairs), len(selected_pairs), len(unable_to_select_pairs), len(valid_orders), len(invalid_executed_orders)
    
    except Exception as e:
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "error": f"Unexpected error in validatesignals: {str(e)}"
        })
        save_errors()
        log_and_print(f"Unexpected error in validatesignals: {str(e)}", "ERROR")
        mt5.shutdown()
        return 0, 0, 0, 0, 0, 0, 0

def cancel_limitorders():
    """Delete all pending orders in the MT5 account."""
    
    # Define MT5 credentials and terminal path
    TERMINAL_PATH = r"C:\Program Files\MetaTrader 5\terminal64.exe"
    LOGIN_ID = "101347351"
    PASSWORD = "@Techknowdge12#"
    SERVER = "DerivSVG-Server-02"
    
    # Initialize error log list
    error_log = []
    error_json_path = os.path.join(BASE_ERROR_FOLDER, "deletependingorderserror.json")
    
    # Helper function to save errors to JSON
    def save_errors():
        try:
            with open(error_json_path, 'w') as f:
                json.dump(error_log, f, indent=4)
            #log_and_print(f"Errors saved to {error_json_path}", "INFO")
            log_and_print(f"ALL CLEANED", "INFO")
        except Exception as e:
            log_and_print(f"Failed to save errors to {error_json_path}: {str(e)}", "ERROR")
    
    # Verify terminal executable exists
    if not os.path.exists(TERMINAL_PATH):
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "error": f"MT5 terminal executable not found at {TERMINAL_PATH}"
        })
        save_errors()
        log_and_print(f"MT5 terminal executable not found at {TERMINAL_PATH}", "ERROR")
        return 0
    
    # Initialize MT5
    try:
        if not mt5.initialize(
            path=TERMINAL_PATH,
            login=int(LOGIN_ID),
            server=SERVER,
            password=PASSWORD,
            timeout=30000
        ):
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Failed to initialize MT5: {mt5.last_error()}"
            })
            save_errors()
            log_and_print(f"Failed to initialize MT5: {mt5.last_error()}", "ERROR")
            return 0
        
        # Verify login
        if not mt5.login(
            login=int(LOGIN_ID),
            server=SERVER,
            password=PASSWORD
        ):
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Failed to login to MT5: {mt5.last_error()}"
            })
            save_errors()
            log_and_print(f"Failed to login to MT5: {mt5.last_error()}", "ERROR")
            mt5.shutdown()
            return 0
        
        #log_and_print(f"Successfully initialized and logged into MT5 (loginid={LOGIN_ID}, server={SERVER})", "SUCCESS")
        
        # Fetch all pending orders
        orders = mt5.orders_get()
        if orders is None:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Failed to retrieve pending orders: {mt5.last_error()}"
            })
            save_errors()
            #log_and_print(f"Failed to retrieve pending orders: {mt5.last_error()}", "ERROR")
            mt5.shutdown()
            return 0
        
        total_orders = len(orders)
        #log_and_print(f"Found {total_orders} pending orders", "INFO")
        log_and_print(f"Cleaning up...", "INFO")
        
        # Initialize counter for deleted orders
        orders_deleted = 0
        
        # Delete each pending order
        for order in orders:
            request = {
                "action": mt5.TRADE_ACTION_REMOVE,
                "order": order.ticket
            }
            result = mt5.order_send(request)
            if result.retcode == mt5.TRADE_RETCODE_DONE:
                orders_deleted += 1
                #log_and_print(f"Successfully deleted pending order (ticket={order.ticket}, symbol={order.symbol})", "SUCCESS")
            else:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Failed to delete order (ticket={order.ticket}, symbol={order.symbol}): {result.comment}"
                })
                #log_and_print(f"Failed to delete order (ticket={order.ticket}, symbol={order.symbol}): {result.comment}", "ERROR")
        
        # Save any errors
        if error_log:
            save_errors()
        
        # Log summary
        #log_and_print(f"Total pending orders deleted: {orders_deleted}", "INFO")
        #log_and_print(f"Total pending orders failed to delete: {total_orders - orders_deleted}", "INFO")
        
        # Shutdown MT5
        mt5.shutdown()
        return orders_deleted
    
    except Exception as e:
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "error": f"Unexpected error in deletependingorders: {str(e)}"
        })
        save_errors()
        #log_and_print(f"Unexpected error in deletependingorders: {str(e)}", "ERROR")
        mt5.shutdown()
        return 0

def marketsliststatus() -> tuple[bool, dict]:
    """Generate a status report for all markets and timeframes, summarizing invalid pending orders."""
    log_and_print("Generating markets list status", "INFO")
    
    # Initialize status report
    status_report = {
        "status": "failed",
        "message": "",
        "markets_processed": 0,
        "warnings": [],
        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
        "market_timeframe_status": {},
        "total_invalid_pending_orders": 0
    }
    
    # Define file paths
    BASE_OUTPUT_FOLDER = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\orders"
    output_json_path = os.path.join(BASE_OUTPUT_FOLDER, "marketsliststatus.json")
    valid_pending_path = os.path.join(BASE_OUTPUT_FOLDER, "validpendingorders.json")
    
    try:
        # Initialize market status
        market_status = {}
        total_invalid_pending = 0
        
        # Load valid pending orders
        valid_pending_orders = []
        if os.path.exists(valid_pending_path):
            try:
                with open(valid_pending_path, 'r') as f:
                    pending_data = json.load(f)
                valid_pending_orders = pending_data.get("orders", [])
                log_and_print(f"Loaded {len(valid_pending_orders)} orders from {valid_pending_path}", "INFO")
            except Exception as e:
                log_and_print(f"Error reading validpendingorders.json: {str(e)}", "WARNING")
                status_report["warnings"].append(f"Error reading validpendingorders.json: {str(e)}")
        
        # Process each market and timeframe
        for market in MARKETS:
            formatted_market = market.replace(" ", "_")
            market_status[market] = {}
            for timeframe in TIMEFRAMES:
                # Initialize count
                invalid_pending_count = 0
                
                # Count invalid pending orders for this market and timeframe
                invalid_pending_count = sum(1 for order in valid_pending_orders 
                                          if order["market"] == market 
                                          and order["timeframe"] == timeframe 
                                          and order.get("is_valid") == False)
                
                market_status[market][timeframe] = {
                    "invalid_pending_orders": invalid_pending_count
                }
                status_report["market_timeframe_status"][f"{market}_{timeframe}"] = {
                    "invalid_pending_orders": invalid_pending_count
                }
                status_report["markets_processed"] += 1
                total_invalid_pending += invalid_pending_count
                log_and_print(
                    f"Market: {market}, Timeframe: {timeframe}, Invalid Pending: {invalid_pending_count}",
                    "DEBUG"
                )
        
        # Prepare output data
        output_data = {
            "timestamp": status_report["timestamp"],
            "total_invalid_pending_orders": total_invalid_pending,
            "markets": market_status
        }
        
        # Save to marketsliststatus.json
        try:
            with open(output_json_path, 'w') as f:
                json.dump(output_data, f, indent=4)
            log_and_print(f"Saved markets list status to {output_json_path}", "SUCCESS")
            status_report["status"] = "success"
            status_report["message"] = f"Processed {status_report['markets_processed']} market-timeframe combinations"
            status_report["total_invalid_pending_orders"] = total_invalid_pending
            return True, status_report
        except Exception as e:
            log_and_print(f"Error saving marketsliststatus.json: {str(e)}", "ERROR")
            status_report["message"] = f"Error saving marketsliststatus.json: {str(e)}"
            return False, status_report
    
    except Exception as e:
        log_and_print(f"Error generating markets list status: {str(e)}", "ERROR")
        status_report["message"] = f"Unexpected error: {str(e)}"
        return False, status_report

def insertinvalidexecutedorderstodb(json_path: str = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\orders\invalidexecutedorders.json") -> bool:
    """Insert all invalid executed orders from invalidexecutedorders.json into cipher_processed_bouncestreamsignals table after validation, 
    removing only duplicate orders."""
    log_and_print("Inserting all invalid executed orders into cipher_processed_bouncestreamsignals table", "INFO")
    # Initialize error log list
    error_log = []
    
    # Define paths
    error_json_path = os.path.join(BASE_PROCESSING_FOLDER, "insertinvalidexecutedorderserror.json")
    
    # Helper function to save errors to JSON
    def save_errors():
        try:
            with open(error_json_path, 'w') as f:
                json.dump(error_log, f, indent=4)
            log_and_print(f"Errors saved to {error_json_path}", "INFO")
        except Exception as e:
            log_and_print(f"Failed to save errors to {error_json_path}: {str(e)}", "ERROR")
    
    # Load invalid executed orders from JSON
    try:
        if not os.path.exists(json_path):
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Invalid executed orders JSON file not found at: {json_path}"
            })
            save_errors()
            log_and_print(f"Invalid executed orders JSON file not found at: {json_path}", "ERROR")
            return False
        
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        orders = data.get('orders', [])
        if not orders:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": "No orders found in invalidexecutedorders.json or 'orders' key is missing"
            })
            save_errors()
            log_and_print("No orders found in invalidexecutedorders.json or 'orders' key is missing", "INFO")
            return True  # No orders to process, but not an error
        
        log_and_print(f"Loaded {len(orders)} invalid executed orders from {json_path}", "INFO")
        
    except Exception as e:
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "error": f"Error loading invalidexecutedorders.json: {str(e)}"
        })
        save_errors()
        log_and_print(f"Error loading invalidexecutedorders.json: {str(e)}", "ERROR")
        return False
    
    # Fetch existing signals from the database
    fetch_query = """
        SELECT pair, timeframe, order_type, entry_price, created_at
        FROM cipher_processed_bouncestreamsignals
    """
    try:
        result = db.execute_query(fetch_query)
        log_and_print(f"Raw query result for fetching signals: {json.dumps(result, indent=2)}", "DEBUG")
        
        existing_signals = []
        if isinstance(result, dict):
            if result.get('status') != 'success':
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Query failed: {result.get('message', 'No message provided')}"
                })
                save_errors()
                log_and_print(f"Query failed: {result.get('message', 'No message provided')}", "ERROR")
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
            log_and_print(f"Invalid result format: Expected dict or list, got {type(result)}", "ERROR")
            return False
        
    except Exception as e:
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "error": f"Error fetching existing signals: {str(e)}"
        })
        save_errors()
        log_and_print(f"Error fetching existing signals: {str(e)}", "ERROR")
        return False
    
    # Process JSON orders and validate
    json_order_keys = set()
    valid_orders = []
    
    # Define maximum allowed value for numeric fields
    MAX_NUMERIC_VALUE = 9999999999.99
    MIN_NUMERIC_VALUE = -9999999999.99
    
    for order in orders:
        try:
            pair = order.get('pair', 'N/A')
            timeframe = DB_TIMEFRAME_MAPPING.get(order.get('timeframe', 'N/A'), order.get('timeframe', 'N/A'))
            order_type = order.get('order_type', 'N/A')
            entry_price = float(order.get('entry_price', 0.0))
            exit_price = float(order.get('exit_price', 0.0))
            ratio_0_5_price = float(order.get('1:0.5_price', 0.0))
            ratio_1_price = float(order.get('1:1_price', 0.0))
            ratio_2_price = float(order.get('1:2_price', 0.0))
            profit_price = float(order.get('profit_price', 0.0))
            message = order.get('reason', None)  # Map 'reason' to 'message' column
            created_at = order.get('order_holder_timestamp', 'N/A')  # Map order_holder_timestamp to created_at
            
            if created_at == 'N/A':
                raise ValueError("Missing order_holder_timestamp")
            
            # Validate numeric fields
            for field_name, value in [
                ('entry_price', entry_price),
                ('exit_price', exit_price),
                ('1:0.5_price', ratio_0_5_price),
                ('1:1_price', ratio_1_price),
                ('1:2_price', ratio_2_price),
                ('profit_price', profit_price)
            ]:
                if not (MIN_NUMERIC_VALUE <= value <= MAX_NUMERIC_VALUE):
                    raise ValueError(f"{field_name} out of range: {value}")
            
            # Validate message (can be NULL, so no strict validation needed)
            if message is not None and not isinstance(message, str):
                raise ValueError(f"Invalid message format: {message}")
            
            order_key = (pair, timeframe, order_type, entry_price)
            if order_key in json_order_keys:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Duplicate order in JSON: {pair}, {timeframe}, {order_type}, {entry_price}"
                })
                log_and_print(f"Duplicate order in JSON: {pair}, {timeframe}, {order_type}, {entry_price}", "WARNING")
                continue
            json_order_keys.add(order_key)
            valid_orders.append({
                'pair': pair,
                'timeframe': timeframe,
                'order_type': order_type,
                'entry_price': entry_price,
                'exit_price': exit_price,
                '1:0.5_price': ratio_0_5_price,
                '1:1_price': ratio_1_price,
                '1:2_price': ratio_2_price,
                'profit_price': profit_price,
                'message': message,
                'created_at': created_at
            })
        except (ValueError, TypeError) as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Invalid data format in order {order.get('pair', 'unknown')} {order.get('timeframe', 'unknown')}: {str(e)}"
            })
            log_and_print(f"Invalid data format in order {order.get('pair', 'unknown')} {order.get('timeframe', 'unknown')}: {str(e)}", "ERROR")
            continue
    
    # Identify duplicates in DB
    db_order_keys = {}
    duplicates_to_remove = []
    
    for signal in existing_signals:
        try:
            pair = signal.get('pair', 'N/A')
            timeframe = signal.get('timeframe', 'N/A')
            order_type = signal.get('order_type', 'N/A')
            entry_price = float(signal.get('entry_price', 0.0))
            created_at = signal.get('created_at', '')
            
            signal_key = (pair, timeframe, order_type, entry_price)
            
            if signal_key in db_order_keys:
                if created_at < db_order_keys[signal_key]['created_at']:
                    duplicates_to_remove.append(db_order_keys[signal_key])
                    db_order_keys[signal_key] = signal
                else:
                    duplicates_to_remove.append(signal)
            else:
                db_order_keys[signal_key] = signal
        except (ValueError, TypeError) as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Invalid data in existing signal {signal.get('pair', 'unknown')}: {str(e)}"
            })
            log_and_print(f"Invalid data in existing signal {signal.get('pair', 'unknown')}: {str(e)}", "ERROR")
            continue
    
    # Batch delete duplicates
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
                log_and_print(f"Successfully removed {affected_rows} duplicate orders in batch {batch_number}", "SUCCESS")
            except Exception as e:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Failed to batch delete duplicates in batch {batch_number}: {str(e)}"
                })
                save_errors()
                log_and_print(f"Failed to batch delete duplicates in batch {batch_number}: {str(e)}", "ERROR")
                return False
    
    # Prepare batch INSERT query for new valid orders (in chunks of 80)
    BATCH_SIZE = 80
    sql_query_base = """
        INSERT INTO cipher_processed_bouncestreamsignals (
            pair, timeframe, order_type, entry_price, exit_price,
            ratio_0_5_price, ratio_1_price, ratio_2_price, 
            profit_price, message, created_at
        ) VALUES 
    """
    value_strings = []
    
    for order in valid_orders:
        try:
            pair = order.get('pair', 'N/A')
            timeframe = order.get('timeframe', 'N/A')
            order_type = order.get('order_type', 'N/A')
            entry_price = float(order.get('entry_price', 0.0))
            exit_price = float(order.get('exit_price', 0.0))
            ratio_0_5_price = float(order.get('1:0.5_price', 0.0))
            ratio_1_price = float(order.get('1:1_price', 0.0))
            ratio_2_price = float(order.get('1:2_price', 0.0))
            profit_price = float(order.get('profit_price', 0.0))
            message = order.get('message', None)
            created_at = order.get('created_at', 'N/A')
            
            # Re-validate numeric fields
            for field_name, value in [
                ('entry_price', entry_price),
                ('exit_price', exit_price),
                ('1:0.5_price', ratio_0_5_price),
                ('1:1_price', ratio_1_price),
                ('1:2_price', ratio_2_price),
                ('profit_price', profit_price)
            ]:
                if not (MIN_NUMERIC_VALUE <= value <= MAX_NUMERIC_VALUE):
                    raise ValueError(f"{field_name} out of range: {value}")
            
            if created_at == 'N/A':
                raise ValueError("Missing created_at")
            
            # Validate message
            if message is not None and not isinstance(message, str):
                raise ValueError(f"Invalid message format: {message}")
            
            order_key = (pair, timeframe, order_type, entry_price)
            
            if order_key not in db_order_keys:
                pair_escaped = pair.replace("'", "''")
                timeframe_escaped = timeframe.replace("'", "''")
                order_type_escaped = order_type.replace("'", "''")
                created_at_escaped = created_at.replace("'", "''")
                message_escaped = "'" + message.replace("'", "''") + "'" if message is not None else 'NULL'
                value_string = (
                    f"('{pair_escaped}', '{timeframe_escaped}', '{order_type_escaped}', {entry_price}, {exit_price}, "
                    f"{ratio_0_5_price}, {ratio_1_price}, {ratio_2_price}, {profit_price}, {message_escaped}, '{created_at_escaped}')"
                )
                value_strings.append(value_string)
        except (ValueError, TypeError) as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Invalid data format in order {order.get('pair', 'unknown')} {order.get('timeframe', 'unknown')}: {str(e)}"
            })
            log_and_print(f"Invalid data format in order {order.get('pair', 'unknown')} {order.get('timeframe', 'unknown')}: {str(e)}", "ERROR")
            continue
    
    if not value_strings:
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "error": "No new valid invalid executed orders to insert after processing"
        })
        save_errors()
        log_and_print(f"No new valid invalid executed orders to insert after processing", "INFO")
        return True
    
    # Execute batch INSERT in chunks of BATCH_SIZE with retries
    success = True
    insert_batch_counts = []
    for i in range(0, len(value_strings), BATCH_SIZE):
        batch = value_strings[i:i + BATCH_SIZE]
        batch_number = i // BATCH_SIZE + 1
        sql_query = sql_query_base + ", ".join(batch)
        
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = db.execute_query(sql_query)
                log_and_print(f"Raw query result for inserting batch {batch_number}: {json.dumps(result, indent=2)}", "DEBUG")
                
                if not isinstance(result, dict):
                    error_log.append({
                        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                        "error": f"Invalid result format on attempt {attempt} for batch {batch_number}: Expected dict, got {type(result)}"
                    })
                    save_errors()
                    log_and_print(f"Invalid result format on attempt {attempt} for batch {batch_number}: Expected dict, got {type(result)}", "ERROR")
                    success = False
                    continue
                
                if result.get('status') != 'success':
                    error_log.append({
                        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                        "error": f"Query failed on attempt {attempt} for batch {batch_number}: {result.get('message', 'No message provided')}"
                    })
                    save_errors()
                    log_and_print(f"Query failed on attempt {attempt} for batch {batch_number}: {result.get('message', 'No message provided')}", "ERROR")
                    success = False
                    continue
                
                affected_rows = result.get('results', {}).get('affected_rows', 0)
                insert_batch_counts.append((batch_number, affected_rows))  # Track insert count
                log_and_print(f"Successfully inserted {affected_rows} invalid executed orders in batch {batch_number}", "SUCCESS")
                break  # Exit retry loop on success
                
            except Exception as e:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Exception on attempt {attempt} for batch {batch_number}: {str(e)}"
                })
                save_errors()
                log_and_print(f"Exception on attempt {attempt} for batch {batch_number}: {str(e)}", "ERROR")
                
                if attempt < MAX_RETRIES:
                    delay = RETRY_DELAY * (2 ** (attempt - 1))
                    log_and_print(f"Retrying batch {batch_number} after {delay} seconds...", "INFO")
                    time.sleep(delay)
                else:
                    error_log.append({
                        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                        "error": f"Max retries reached for batch {batch_number}"
                    })
                    save_errors()
                    log_and_print(f"Max retries reached for batch {batch_number}", "ERROR")
                    success = False
    
    # Log batch processing counts
    for batch_number, count in insert_batch_counts:
        log_and_print(f"Insert batch {batch_number} processed: {count}", "INFO")
    
    if not success:
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "error": "Function failed due to errors in one or more batches"
        })
        save_errors()
        log_and_print(f"Function failed due to errors in one or more batches", "ERROR")
        return False
    
    log_and_print(f"All batches processed successfully", "SUCCESS")
    return True

def executeinsertinvalidexecutedorderstodb():
    """Execute the insertion of invalid executed orders into the database."""
    log_and_print("===== Execute Insert Invalid Executed Orders to Database =====", "TITLE")
    if not insertinvalidexecutedorderstodb():
        log_and_print("Failed to insert invalid executed orders into database. Exiting.", "ERROR")
        return
    log_and_print("===== Insert Invalid Executed Orders to Database Completed =====", "TITLE")


def insertpendingorderstodb(json_path: str = os.path.join(BASE_OUTPUT_FOLDER, "validpendingorders.json")) -> bool:
    """Insert all pending orders from validpendingorders.json into cipherbouncestream_signals table after validation, 
    removing only duplicate orders."""
    log_and_print("Inserting all pending orders into cipherbouncestream_signals table", "INFO")
    # Initialize error log list
    error_log = []
    
    # Define paths
    error_json_path = os.path.join(BASE_PROCESSING_FOLDER, "insertpendingorderserror.json")
    
    # Helper function to save errors to JSON
    def save_errors():
        try:
            with open(error_json_path, 'w') as f:
                json.dump(error_log, f, indent=4)
            log_and_print(f"Errors saved to {error_json_path}", "INFO")
        except Exception as e:
            log_and_print(f"Failed to save errors to {error_json_path}: {str(e)}", "ERROR")
    
    # Load pending orders from JSON
    try:
        if not os.path.exists(json_path):
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Pending orders JSON file not found at: {json_path}"
            })
            save_errors()
            log_and_print(f"Pending orders JSON file not found at: {json_path}", "ERROR")
            return False
        
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        orders = data.get('orders', [])
        if not orders:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": "No orders found in validpendingorders.json or 'orders' key is missing"
            })
            save_errors()
            log_and_print("No orders found in validpendingorders.json or 'orders' key is missing", "INFO")
            return True  # No orders to process, but not an error
        
        log_and_print(f"Loaded {len(orders)} pending orders from {json_path}", "INFO")
        
    except Exception as e:
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "error": f"Error loading validpendingorders.json: {str(e)}"
        })
        save_errors()
        log_and_print(f"Error loading validpendingorders.json: {str(e)}", "ERROR")
        return False
    
    # Fetch existing signals from the database
    fetch_query = """
        SELECT pair, timeframe, order_type, entry_price, created_at
        FROM cipherbouncestream_signals
    """
    try:
        result = db.execute_query(fetch_query)
        log_and_print(f"Raw query result for fetching signals: {json.dumps(result, indent=2)}", "DEBUG")
        
        existing_signals = []
        if isinstance(result, dict):
            if result.get('status') != 'success':
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Query failed: {result.get('message', 'No message provided')}"
                })
                save_errors()
                log_and_print(f"Query failed: {result.get('message', 'No message provided')}", "ERROR")
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
            log_and_print(f"Invalid result format: Expected dict or list, got {type(result)}", "ERROR")
            return False
        
    except Exception as e:
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "error": f"Error fetching existing signals: {str(e)}"
        })
        save_errors()
        log_and_print(f"Error fetching existing signals: {str(e)}", "ERROR")
        return False
    
    # Process JSON orders and validate
    json_order_keys = set()
    valid_orders = []
    
    # Define maximum allowed value for numeric fields
    MAX_NUMERIC_VALUE = 9999999999.99
    MIN_NUMERIC_VALUE = -9999999999.99
    
    for order in orders:
        try:
            pair = order.get('pair', 'N/A')
            timeframe = DB_TIMEFRAME_MAPPING.get(order.get('timeframe', 'N/A'), order.get('timeframe', 'N/A'))
            order_type = order.get('order_type', 'N/A')
            entry_price = float(order.get('entry_price', 0.0))
            exit_price = float(order.get('exit_price', 0.0))
            ratio_0_5_price = float(order.get('1:0.5_price', 0.0))
            ratio_1_price = float(order.get('1:1_price', 0.0))
            ratio_2_price = float(order.get('1:2_price', 0.0))
            profit_price = float(order.get('profit_price', 0.0))
            created_at = order.get('order_holder_timestamp', 'N/A')  # Map order_holder_timestamp to created_at
            
            if created_at == 'N/A':
                raise ValueError("Missing order_holder_timestamp")
            
            # Validate numeric fields
            for field_name, value in [
                ('entry_price', entry_price),
                ('exit_price', exit_price),
                ('1:0.5_price', ratio_0_5_price),
                ('1:1_price', ratio_1_price),
                ('1:2_price', ratio_2_price),
                ('profit_price', profit_price)
            ]:
                if not (MIN_NUMERIC_VALUE <= value <= MAX_NUMERIC_VALUE):
                    raise ValueError(f"{field_name} out of range: {value}")
            
            order_key = (pair, timeframe, order_type, entry_price)
            if order_key in json_order_keys:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Duplicate order in JSON: {pair}, {timeframe}, {order_type}, {entry_price}"
                })
                log_and_print(f"Duplicate order in JSON: {pair}, {timeframe}, {order_type}, {entry_price}", "WARNING")
                continue
            json_order_keys.add(order_key)
            valid_orders.append({
                'pair': pair,
                'timeframe': timeframe,
                'order_type': order_type,
                'entry_price': entry_price,
                'exit_price': exit_price,
                '1:0.5_price': ratio_0_5_price,
                '1:1_price': ratio_1_price,
                '1:2_price': ratio_2_price,
                'profit_price': profit_price,
                'created_at': created_at
            })
        except (ValueError, TypeError) as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Invalid data format in order {order.get('pair', 'unknown')} {order.get('timeframe', 'unknown')}: {str(e)}"
            })
            log_and_print(f"Invalid data format in order {order.get('pair', 'unknown')} {order.get('timeframe', 'unknown')}: {str(e)}", "ERROR")
            continue
    
    # Identify duplicates in DB
    db_order_keys = {}
    duplicates_to_remove = []
    
    for signal in existing_signals:
        try:
            pair = signal.get('pair', 'N/A')
            timeframe = signal.get('timeframe', 'N/A')
            order_type = signal.get('order_type', 'N/A')
            entry_price = float(signal.get('entry_price', 0.0))
            created_at = signal.get('created_at', '')
            
            signal_key = (pair, timeframe, order_type, entry_price)
            
            if signal_key in db_order_keys:
                if created_at < db_order_keys[signal_key]['created_at']:
                    duplicates_to_remove.append(db_order_keys[signal_key])
                    db_order_keys[signal_key] = signal
                else:
                    duplicates_to_remove.append(signal)
            else:
                db_order_keys[signal_key] = signal
        except (ValueError, TypeError) as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Invalid data in existing signal {signal.get('pair', 'unknown')}: {str(e)}"
            })
            log_and_print(f"Invalid data in existing signal {signal.get('pair', 'unknown')}: {str(e)}", "ERROR")
            continue
    
    # Batch delete duplicates
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
                
                delete_query = f"DELETE FROM cipherbouncestream_signals WHERE {' OR '.join(delete_conditions)}"
                result = db.execute_query(delete_query)
                affected_rows = result.get('results', {}).get('affected_rows', 0) if isinstance(result, dict) else 0
                log_and_print(f"Successfully removed {affected_rows} duplicate orders in batch {batch_number}", "SUCCESS")
            except Exception as e:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Failed to batch delete duplicates in batch {batch_number}: {str(e)}"
                })
                save_errors()
                log_and_print(f"Failed to batch delete duplicates in batch {batch_number}: {str(e)}", "ERROR")
                return False
    
    # Prepare batch INSERT query for new valid orders (in chunks of 80)
    BATCH_SIZE = 80
    sql_query_base = """
        INSERT INTO cipherbouncestream_signals (
            pair, timeframe, order_type, entry_price, exit_price,
            ratio_0_5_price, ratio_1_price, ratio_2_price, 
            profit_price, created_at
        ) VALUES 
    """
    value_strings = []
    
    for order in valid_orders:
        try:
            pair = order.get('pair', 'N/A')
            timeframe = order.get('timeframe', 'N/A')
            order_type = order.get('order_type', 'N/A')
            entry_price = float(order.get('entry_price', 0.0))
            exit_price = float(order.get('exit_price', 0.0))
            ratio_0_5_price = float(order.get('1:0.5_price', 0.0))
            ratio_1_price = float(order.get('1:1_price', 0.0))
            ratio_2_price = float(order.get('1:2_price', 0.0))
            profit_price = float(order.get('profit_price', 0.0))
            created_at = order.get('created_at', 'N/A')
            
            # Re-validate numeric fields
            for field_name, value in [
                ('entry_price', entry_price),
                ('exit_price', exit_price),
                ('1:0.5_price', ratio_0_5_price),
                ('1:1_price', ratio_1_price),
                ('1:2_price', ratio_2_price),
                ('profit_price', profit_price)
            ]:
                if not (MIN_NUMERIC_VALUE <= value <= MAX_NUMERIC_VALUE):
                    raise ValueError(f"{field_name} out of range: {value}")
            
            if created_at == 'N/A':
                raise ValueError("Missing created_at")
            
            order_key = (pair, timeframe, order_type, entry_price)
            
            if order_key not in db_order_keys:
                pair_escaped = pair.replace("'", "''")
                timeframe_escaped = timeframe.replace("'", "''")
                order_type_escaped = order_type.replace("'", "''")
                created_at_escaped = created_at.replace("'", "''")
                value_string = (
                    f"('{pair_escaped}', '{timeframe_escaped}', '{order_type_escaped}', {entry_price}, {exit_price}, "
                    f"{ratio_0_5_price}, {ratio_1_price}, {ratio_2_price}, {profit_price}, '{created_at_escaped}')"
                )
                value_strings.append(value_string)
        except (ValueError, TypeError) as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Invalid data format in order {order.get('pair', 'unknown')} {order.get('timeframe', 'unknown')}: {str(e)}"
            })
            log_and_print(f"Invalid data format in order {order.get('pair', 'unknown')} {order.get('timeframe', 'unknown')}: {str(e)}", "ERROR")
            continue
    
    if not value_strings:
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "error": "No new valid pending orders to insert after processing"
        })
        save_errors()
        log_and_print(f"No new valid pending orders to insert after processing", "INFO")
        return True
    
    # Execute batch INSERT in chunks of BATCH_SIZE with retries
    success = True
    insert_batch_counts = []
    for i in range(0, len(value_strings), BATCH_SIZE):
        batch = value_strings[i:i + BATCH_SIZE]
        batch_number = i // BATCH_SIZE + 1
        sql_query = sql_query_base + ", ".join(batch)
        
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = db.execute_query(sql_query)
                log_and_print(f"Raw query result for inserting batch {batch_number}: {json.dumps(result, indent=2)}", "DEBUG")
                
                if not isinstance(result, dict):
                    error_log.append({
                        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                        "error": f"Invalid result format on attempt {attempt} for batch {batch_number}: Expected dict, got {type(result)}"
                    })
                    save_errors()
                    log_and_print(f"Invalid result format on attempt {attempt} for batch {batch_number}: Expected dict, got {type(result)}", "ERROR")
                    success = False
                    continue
                
                if result.get('status') != 'success':
                    error_log.append({
                        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                        "error": f"Query failed on attempt {attempt} for batch {batch_number}: {result.get('message', 'No message provided')}"
                    })
                    save_errors()
                    log_and_print(f"Query failed on attempt {attempt} for batch {batch_number}: {result.get('message', 'No message provided')}", "ERROR")
                    success = False
                    continue
                
                affected_rows = result.get('results', {}).get('affected_rows', 0)
                insert_batch_counts.append((batch_number, affected_rows))  # Track insert count
                log_and_print(f"Successfully inserted {affected_rows} pending orders in batch {batch_number}", "SUCCESS")
                break  # Exit retry loop on success
                
            except Exception as e:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"Exception on attempt {attempt} for batch {batch_number}: {str(e)}"
                })
                save_errors()
                log_and_print(f"Exception on attempt {attempt} for batch {batch_number}: {str(e)}", "ERROR")
                
                if attempt < MAX_RETRIES:
                    delay = RETRY_DELAY * (2 ** (attempt - 1))
                    log_and_print(f"Retrying batch {batch_number} after {delay} seconds...", "INFO")
                    time.sleep(delay)
                else:
                    error_log.append({
                        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                        "error": f"Max retries reached for batch {batch_number}"
                    })
                    save_errors()
                    log_and_print(f"Max retries reached for batch {batch_number}", "ERROR")
                    success = False
    
    # Log batch processing counts
    for batch_number, count in insert_batch_counts:
        log_and_print(f"Insert batch {batch_number} processed: {count}", "INFO")
    
    if not success:
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "error": "Function failed due to errors in one or more batches"
        })
        save_errors()
        log_and_print(f"Function failed due to errors in one or more batches", "ERROR")
        return False
    
    log_and_print(f"All batches processed successfully", "SUCCESS")
    return True

def executeinsertpendingorderstodb():
    """Execute the insertion of pending orders into the database."""
    log_and_print("===== Execute Insert Pending Orders to Database =====", "TITLE")
    if not insertpendingorderstodb():
        log_and_print("Failed to insert pending orders into database. Exiting.", "ERROR")
        return
    executeinsertinvalidexecutedorderstodb()
    log_and_print("===== Insert Pending Orders to Database Completed =====", "TITLE")

def check_verification_json(market: str) -> bool:
    """Check if verification.json for a market has all required timeframes set to 'chart_identified' and 'all_timeframes' set to 'verified'."""
    try:
        market_folder_name = market.replace(" ", "_")
        verification_file = os.path.join(FETCHCHART_DESTINATION_PATH, market_folder_name, "verification.json")
        
        if not os.path.exists(verification_file):
            log_and_print(f"Verification file not found for {market}: {verification_file}", "WARNING")
            return False
        
        with open(verification_file, 'r') as f:
            verification_data = json.load(f)
        
        required_timeframes = ["m5", "m15", "m30", "h1", "h4"]
        all_timeframes_verified = all(
            verification_data.get(tf) in ["chart_identified", "active"] for tf in required_timeframes
        ) and verification_data.get("all_timeframes") == "verified"
        if all_timeframes_verified:
            log_and_print(f"All timeframes in verification.json for {market} are 'chart_identified' and 'all_timeframes' is 'verified'", "INFO")
            return True
        else:
            log_and_print(f"Not all timeframes in verification.json for {market} are 'chart_identified' or 'all_timeframes' is not 'verified'", "INFO")
            return False
    
    except Exception as e:
        log_and_print(f"Error reading verification.json for {market}: {e}", "ERROR")
        return False
              
def process_5minutes_timeframe():
    """Process all markets for the 5-minute (M5) timeframe if verification.json has all timeframes 'chart_identified' and 'all_timeframes' verified, using markets from batchbybatch.json, returning a summary of processing results."""
    try:
        log_and_print("===== Fetch and Process M5 Candle Data =====", "TITLE")
        
        # Verify that credentials were loaded
        if not all([LOGIN_ID, PASSWORD, SERVER, TERMINAL_PATH]):
            error_message = "Credentials not properly loaded from base.json. Exiting."
            log_and_print(error_message, "ERROR")
            return {
                "status": "failed",
                "message": error_message,
                "markets_processed": 0,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "process_messages": {}
            }
        
        # Load markets from batchbybatch.json
        batch_json_path = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\batches\batchbybatch.json"
        if not os.path.exists(batch_json_path):
            error_message = f"batchbybatch.json not found at {batch_json_path}. Exiting."
            log_and_print(error_message, "ERROR")
            return {
                "status": "failed",
                "message": error_message,
                "markets_processed": 0,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "process_messages": {}
            }
        
        try:
            with open(batch_json_path, 'r') as f:
                batch_data = json.load(f)
            batch_markets = batch_data.get("markets", [])
            if not batch_markets:
                error_message = "No markets found in batchbybatch.json. Exiting."
                log_and_print(error_message, "ERROR")
                return {
                    "status": "failed",
                    "message": error_message,
                    "markets_processed": 0,
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "process_messages": {}
                }
        except json.JSONDecodeError as e:
            error_message = f"Error decoding batchbybatch.json: {str(e)}. Exiting."
            log_and_print(error_message, "ERROR")
            return {
                "status": "failed",
                "message": error_message,
                "markets_processed": 0,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "process_messages": {}
            }
        
        log_and_print(f"Loaded {len(batch_markets)} markets from batchbybatch.json", "INFO")
        
        # Filter markets that are in both batchbybatch.json and MARKETS
        valid_markets = [market for market in batch_markets if market in MARKETS]
        if not valid_markets:
            error_message = "No valid markets found in both batchbybatch.json and MARKETS list. Exiting."
            log_and_print(error_message, "ERROR")
            return {
                "status": "failed",
                "message": error_message,
                "markets_processed": 0,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "process_messages": {}
            }
        
        # Check M5 candle time left globally using the first valid market
        default_market = valid_markets[0]
        timeframe = "M5"
        log_and_print(f"Checking M5 candle time left using market: {default_market}", "INFO")
        time_left, next_close_time = candletimeleft_5minutes(default_market, None, min_time_left=0.5)
        
        if time_left is None or next_close_time is None:
            error_message = f"Failed to retrieve candle time for {default_market} (M5). Exiting."
            log_and_print(error_message, "ERROR")
            return {
                "status": "failed",
                "message": error_message,
                "markets_processed": 0,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "process_messages": {}
            }
        
        log_and_print(f"M5 candle time left: {time_left:.2f} minutes. Proceeding with execution.", "INFO")

        # Create tasks for markets with valid verification.json
        tasks = []
        process_messages = {}
        markets_with_all_timeframes_verified = []
        for market in valid_markets:
            if check_verification_json(market):
                tasks.append((market, timeframe))
                markets_with_all_timeframes_verified.append(market)
            else:
                log_and_print(f"Skipping market {market}: verification.json not valid or missing 'chart_identified' or 'all_timeframes' verified", "WARNING")
        
        allfound = len(tasks)
        if not tasks:
            message = "No markets with valid verification.json found for M5 timeframe. Exiting."
            log_and_print(message, "WARNING")
            return {
                "status": "no_markets",
                "message": message,
                "markets_processed": 0,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "process_messages": {}
            }
        
        log_and_print(f"Processing {allfound} markets for M5 timeframe", "INFO")
        success_data = []
        no_pending_data = []
        failed_data = []
        fetch_statuses = []
        match_statuses = []
        save_mostrecent_statuses = []
        match_mostrecent_statuses = []
        calc_candles_inbetween_statuses = []
        candles_after_breakout_statuses = []
        order_holder_prices_statuses = []
        pending_order_updater_statuses = []
        collect_pending_orders_statuses = []
        lock_pending_orders_statuses = []
        markets_list_statuses = []

        with multiprocessing.Pool(processes=4) as pool:
            results = pool.starmap(process_market_timeframe, tasks)

        # Collect status for each market
        markets_processed = 0
        for (market, _), (success, error_message, status, market_process_messages) in zip(tasks, results):
            markets_processed += 1
            process_messages[market] = market_process_messages
            
            # Collect fetch_candle_data status
            fetch_status = market_process_messages.get("fetch_candle_data", {})
            fetch_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "fetch_status": fetch_status.get("status", "unknown"),
                "fetch_message": fetch_status.get("message", "No fetch data"),
                "candle_count": fetch_status.get("candle_count", 0),
                "timestamp": fetch_status.get("timestamp", time.strftime("%Y-%m-%d %H:%M:%S"))
            })
            
            # Collect match_trendline_with_candle_data status
            match_status = market_process_messages.get("match_trendline_with_candle_data", {})
            match_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "match_status": match_status.get("status", "unknown"),
                "match_message": match_status.get("message", "No match data"),
                "trendline_count": match_status.get("trendline_count", 0),
                "timestamp": match_status.get("timestamp", time.strftime("%Y-%m-%d %H:%M:%S")),
                "warnings": match_status.get("warnings", [])
            })
            
            # Collect save_new_mostrecent_completed_candle status
            save_status = market_process_messages.get("save_new_mostrecent_completed_candle", {})
            save_mostrecent_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "save_status": save_status.get("status", "unknown"),
                "save_message": save_status.get("message", "No save data"),
                "candles_fetched": save_status.get("candles_fetched", 0),
                "timestamp": save_status.get("timestamp", time.strftime("%Y-%m-%d %H:%M:%S")),
                "warnings": save_status.get("warnings", [])
            })
            
            # Collect match_mostrecent_candle status
            match_mostrecent_status = market_process_messages.get("match_mostrecent_candle", {})
            match_mostrecent_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "match_mostrecent_status": match_mostrecent_status.get("status", "unknown"),
                "match_mostrecent_message": match_mostrecent_status.get("message", "No match most recent data"),
                "candles_matched": match_mostrecent_status.get("candles_matched", 0),
                "timestamp": match_mostrecent_status.get("timestamp", time.strftime("%Y-%m-%d %H:%M:%S")),
                "warnings": match_mostrecent_status.get("warnings", []),
                "match_result_status": match_mostrecent_status.get("match_result_status", "none"),
                "candles_inbetween": match_mostrecent_status.get("candles_inbetween", 0)
            })
            
            # Collect calculate_candles_inbetween status
            calc_status = market_process_messages.get("calculate_candles_inbetween", {})
            calc_candles_inbetween_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "calc_status": calc_status.get("status", "unknown"),
                "calc_message": calc_status.get("message", "No calculate candles in between data"),
                "candles_inbetween": calc_status.get("candles_inbetween", 0),
                "plus_newmostrecent": calc_status.get("plus_newmostrecent", 0),
                "timestamp": calc_status.get("timestamp", time.strftime("%Y-%m-%d %H:%M:%S")),
                "warnings": calc_status.get("warnings", [])
            })
            
            # Collect candleafterbreakoutparent_to_currentprice status
            cabp_status = market_process_messages.get("candleafterbreakoutparent_to_currentprice", {})
            candles_after_breakout_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "cabp_status": cabp_status.get("status", "unknown"),
                "cabp_message": cabp_status.get("message", "No candles after breakout data"),
                "trendlines_processed": cabp_status.get("trendlines_processed", 0),
                "total_candles_fetched": cabp_status.get("total_candles_fetched", 0),
                "timestamp": cabp_status.get("timestamp", time.strftime("%Y-%m-%d %H:%M:%S")),
                "warnings": cabp_status.get("warnings", [])
            })
            
            # Collect getorderholderpriceswithlotsizeandrisk status
            order_holder_prices_status = market_process_messages.get("getorderholderpriceswithlotsizeandrisk", {})
            order_holder_prices_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "order_holder_prices_status": "success" if isinstance(order_holder_prices_status, str) and "Calculated prices" in order_holder_prices_status else "failed",
                "order_holder_prices_message": order_holder_prices_status if isinstance(order_holder_prices_status, str) else "No order holder prices data",
                "orders_processed": 0,
                "verified_order_count": 0,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "warnings": []
            })
            if isinstance(order_holder_prices_status, str) and "Calculated prices" in order_holder_prices_status:
                try:
                    output_json_path = os.path.join(BASE_OUTPUT_FOLDER, market.replace(" ", "_"), timeframe.lower(), 'calculatedprices.json')
                    if os.path.exists(output_json_path) and os.path.getsize(output_json_path) > 0:
                        with open(output_json_path, 'r') as f:
                            try:
                                calculated_data = json.load(f)
                                order_holder_prices_statuses[-1]["orders_processed"] = len(calculated_data)
                                order_holder_prices_statuses[-1]["verified_order_count"] = len(calculated_data)
                            except json.JSONDecodeError as e:
                                order_holder_prices_statuses[-1]["warnings"].append(f"Error decoding calculatedprices.json: {str(e)}")
                    else:
                        order_holder_prices_statuses[-1]["warnings"].append(f"calculatedprices.json is missing or empty for {market} {timeframe}")
                except Exception as e:
                    order_holder_prices_statuses[-1]["warnings"].append(f"Error reading calculatedprices.json: {str(e)}")
            
            # Collect PendingOrderUpdater status
            pending_order_updater_status = market_process_messages.get("PendingOrderUpdater", {})
            pending_order_updater_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "pending_order_updater_status": "success" if isinstance(pending_order_updater_status, str) and "Tracked breakeven" in pending_order_updater_status else "failed",
                "pending_order_updater_message": pending_order_updater_status if isinstance(pending_order_updater_status, str) else "No pending order updater data",
                "orders_updated": 0,
                "verified_order_count": 0,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "warnings": []
            })
            if isinstance(pending_order_updater_status, str) and "Tracked breakeven" in pending_order_updater_status:
                try:
                    pricecandle_json_path = os.path.join(BASE_OUTPUT_FOLDER, market.replace(" ", "_"), timeframe.lower(), 'pricecandle.json')
                    if os.path.exists(pricecandle_json_path) and os.path.getsize(pricecandle_json_path) > 0:
                        with open(pricecandle_json_path, 'r') as f:
                            try:
                                pricecandle_data = json.load(f)
                                pending_order_updater_statuses[-1]["orders_updated"] = len(pricecandle_data)
                                pending_order_updater_statuses[-1]["verified_order_count"] = len(pricecandle_data)
                            except json.JSONDecodeError as e:
                                pending_order_updater_statuses[-1]["warnings"].append(f"Error decoding pricecandle.json: {str(e)}")
                    else:
                        pending_order_updater_statuses[-1]["warnings"].append(f"pricecandle.json is missing or empty for {market} {timeframe}")
                except Exception as e:
                    pending_order_updater_statuses[-1]["warnings"].append(f"Error reading pricecandle.json: {str(e)}")
            
            # Collect collect_all_pending_orders status
            collect_pending_orders_status = market_process_messages.get("collect_all_pending_orders", {})
            collect_pending_orders_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "collect_pending_status": "success" if isinstance(collect_pending_orders_status, str) and "Collected" in collect_pending_orders_status else "failed",
                "collect_pending_message": collect_pending_orders_status if isinstance(collect_pending_orders_status, str) else "No collect pending orders data",
                "pending_orders_collected": 0,
                "verified_pending_count": 0,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "warnings": []
            })
            if isinstance(collect_pending_orders_status, str) and "Collected" in collect_pending_orders_status:
                try:
                    pending_json_path = os.path.join(BASE_OUTPUT_FOLDER, market.replace(" ", "_"), timeframe.lower(), 'contractpendingorders.json')
                    if os.path.exists(pending_json_path) and os.path.getsize(pending_json_path) > 0:
                        with open(pending_json_path, 'r') as f:
                            try:
                                pending_data = json.load(f)
                                collect_pending_orders_statuses[-1]["pending_orders_collected"] = len(pending_data)
                                collect_pending_orders_statuses[-1]["verified_pending_count"] = len(pending_data)
                            except json.JSONDecodeError as e:
                                collect_pending_orders_statuses[-1]["warnings"].append(f"Error decoding contractpendingorders.json: {str(e)}")
                    else:
                        collect_pending_orders_statuses[-1]["warnings"].append(f"contractpendingorders.json is missing or empty for {market} {timeframe}")
                except Exception as e:
                    collect_pending_orders_statuses[-1]["warnings"].append(f"Error reading contractpendingorders.json: {str(e)}")
            
            

            # Collect lockpendingorders status
            lock_pending_status = market_process_messages.get("lockpendingorders", {})
            lock_pending_orders_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "lock_pending_status": "success" if isinstance(lock_pending_status, str) and "Saved" in lock_pending_status else "failed",
                "lock_pending_message": lock_pending_status if isinstance(lock_pending_status, str) else "No lock pending orders data",
                "orders_locked": 0,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "warnings": []
            })
            if isinstance(lock_pending_status, str) and "Saved" in lock_pending_status:
                try:
                    locked_pending_path = os.path.join(BASE_OUTPUT_FOLDER, 'lockedpendingorders.json')
                    if os.path.exists(locked_pending_path) and os.path.getsize(locked_pending_path) > 0:
                        with open(locked_pending_path, 'r') as f:
                            try:
                                locked_data = json.load(f)
                                lock_pending_orders_statuses[-1]["orders_locked"] = len(locked_data.get("temp_pendingorders", []))
                            except json.JSONDecodeError as e:
                                lock_pending_orders_statuses[-1]["warnings"].append(f"Error decoding lockedpendingorders.json: {str(e)}")
                    else:
                        lock_pending_orders_statuses[-1]["warnings"].append(f"lockedpendingorders.json is missing or empty")
                except Exception as e:
                    lock_pending_orders_statuses[-1]["warnings"].append(f"Error reading lockedpendingorders.json: {str(e)}")
            
            # Collect marketsliststatus status
            markets_list_status = market_process_messages.get("marketsliststatus", {})
            markets_list_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "markets_list_status": "success" if isinstance(markets_list_status, str) and "Generated" in markets_list_status else "failed",
                "markets_list_message": markets_list_status if isinstance(markets_list_status, str) else "No markets list status data",
                "markets_processed": 0,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "warnings": []
            })
            if isinstance(markets_list_status, str) and "Generated" in markets_list_status:
                try:
                    markets_order_list_path = os.path.join(BASE_OUTPUT_FOLDER, "marketsorderlist.json")
                    if os.path.exists(markets_order_list_path) and os.path.getsize(markets_order_list_path) > 0:
                        with open(markets_order_list_path, 'r') as f:
                            try:
                                markets_data = json.load(f)
                                markets_list_statuses[-1]["markets_processed"] = len(markets_data.get("markets_pending", {})) + sum(
                                    len(markets_data.get("order_free_markets", {}).get(f"market_{tf.lower()}", [])) for tf in TIMEFRAMES
                                )
                            except json.JSONDecodeError as e:
                                markets_list_statuses[-1]["warnings"].append(f"Error decoding marketsorderlist.json: {str(e)}")
                    else:
                        markets_list_statuses[-1]["warnings"].append(f"marketsorderlist.json is missing or empty")
                except Exception as e:
                    markets_list_statuses[-1]["warnings"].append(f"Error reading marketsorderlist.json: {str(e)}")
            
            # Categorize result
            if success or status == "no_pending_orders":
                if status == "no_pending_orders":
                    no_pending_data.append({
                        "market": market,
                        "timeframe": timeframe,
                        "message": market_process_messages.get("match_trendline_with_candle_data", {}).get("message", "No pending orders found"),
                        "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "process_messages": market_process_messages
                    })
                else:
                    success_data.append({
                        "market": market,
                        "timeframe": timeframe,
                        "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "process_messages": market_process_messages
                    })
            else:
                failed_data.append({
                    "market": market,
                    "timeframe": timeframe,
                    "error_message": error_message,
                    "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "process_messages": market_process_messages
                })
            
            # Log detailed status for each step
            for step, details in market_process_messages.items():
                if isinstance(details, dict):
                    log_and_print(
                        f"{market} {timeframe} - {step}: {details['status']} - {details['message']}",
                        "SUCCESS" if details['status'] in ["success", "no_pending_orders"] else "ERROR"
                    )
                    if details.get("warnings"):
                        for warning in details["warnings"]:
                            log_and_print(f"{market} {timeframe} - {step} Warning: {warning}", "WARNING")
        
        # Summarize results
        success_count = len(success_data)
        no_pending_count = len(no_pending_data)
        failed_count = len(failed_data)
        fetch_success_count = sum(1 for fs in fetch_statuses if fs["fetch_status"] == "success")
        fetch_failed_count = sum(1 for fs in fetch_statuses if fs["fetch_status"] == "failed")
        total_candles_fetched = sum(fs["candle_count"] for fs in fetch_statuses)
        match_success_count = sum(1 for ms in match_statuses if ms["match_status"] == "success")
        match_failed_count = sum(1 for ms in match_statuses if ms["match_status"] == "failed")
        match_no_pending_count = sum(1 for ms in match_statuses if ms["match_status"] == "no_pending_orders")
        total_trendlines_matched = sum(ms["trendline_count"] for ms in match_statuses)
        total_warnings = sum(len(ms["warnings"]) for ms in match_statuses)
        save_mostrecent_success_count = sum(1 for ss in save_mostrecent_statuses if ss["save_status"] == "success")
        save_mostrecent_failed_count = sum(1 for ss in save_mostrecent_statuses if ss["save_status"] == "failed")
        total_mostrecent_candles_fetched = sum(ss["candles_fetched"] for ss in save_mostrecent_statuses)
        match_mostrecent_success_count = sum(1 for mms in match_mostrecent_statuses if mms["match_mostrecent_status"] == "success")
        match_mostrecent_failed_count = sum(1 for mms in match_mostrecent_statuses if mms["match_mostrecent_status"] == "failed")
        total_mostrecent_candles_matched = sum(mms["candles_matched"] for mms in match_mostrecent_statuses)
        calc_candles_success_count = sum(1 for cs in calc_candles_inbetween_statuses if cs["calc_status"] == "success")
        calc_candles_failed_count = sum(1 for cs in calc_candles_inbetween_statuses if cs["calc_status"] == "failed")
        total_candles_inbetween = sum(cs["candles_inbetween"] for cs in calc_candles_inbetween_statuses)
        cabp_success_count = sum(1 for cabp in candles_after_breakout_statuses if cabp["cabp_status"] == "success")
        cabp_failed_count = sum(1 for cabp in candles_after_breakout_statuses if cabp["cabp_status"] == "failed")
        total_cabp_trendlines_processed = sum(cabp["trendlines_processed"] for cabp in candles_after_breakout_statuses)
        total_cabp_candles_fetched = sum(cabp["total_candles_fetched"] for cabp in candles_after_breakout_statuses)
        order_holder_prices_success_count = sum(1 for ohp in order_holder_prices_statuses if ohp["order_holder_prices_status"] == "success")
        order_holder_prices_failed_count = sum(1 for ohp in order_holder_prices_statuses if ohp["order_holder_prices_status"] == "failed")
        total_orders_processed = sum(ohp["orders_processed"] for ohp in order_holder_prices_statuses)
        pending_order_updater_success_count = sum(1 for pou in pending_order_updater_statuses if pou["pending_order_updater_status"] == "success")
        pending_order_updater_failed_count = sum(1 for pou in pending_order_updater_statuses if pou["pending_order_updater_status"] == "failed")
        total_orders_updated = sum(pou["orders_updated"] for pou in pending_order_updater_statuses)
        collect_pending_success_count = sum(1 for cpo in collect_pending_orders_statuses if cpo["collect_pending_status"] == "success")
        collect_pending_failed_count = sum(1 for cpo in collect_pending_orders_statuses if cpo["collect_pending_status"] == "failed")
        total_pending_orders_collected = sum(cpo["pending_orders_collected"] for cpo in collect_pending_orders_statuses)
        lock_pending_success_count = sum(1 for lpo in lock_pending_orders_statuses if lpo["lock_pending_status"] == "success")
        lock_pending_failed_count = sum(1 for lpo in lock_pending_orders_statuses if lpo["lock_pending_status"] == "failed")
        total_orders_locked = sum(lpo["orders_locked"] for lpo in lock_pending_orders_statuses)
        markets_list_success_count = sum(1 for mls in markets_list_statuses if mls["markets_list_status"] == "success")
        markets_list_failed_count = sum(1 for mls in markets_list_statuses if mls["markets_list_status"] == "failed")
        total_markets_processed = sum(mls["markets_processed"] for mls in markets_list_statuses)

        log_and_print("===== SUMMARY =====", "TITLE")
        log_and_print(f"Processing completed: {success_count}/{allfound} successful, {no_pending_count} with no pending orders, {failed_count} failed", "INFO")
        log_and_print(f"Fetch candle data summary: {fetch_success_count}/{allfound} successful, {fetch_failed_count} failed, total candles fetched: {total_candles_fetched}", "INFO")
        log_and_print(f"Save most recent candle summary: {save_mostrecent_success_count}/{allfound} successful, {save_mostrecent_failed_count} failed, total candles fetched: {total_mostrecent_candles_fetched}", "INFO")
        log_and_print(f"Match most recent candle summary: {match_mostrecent_success_count}/{allfound} successful, {match_mostrecent_failed_count} failed, total candles matched: {total_mostrecent_candles_matched}", "INFO")
        log_and_print(f"Calculate candles in between summary: {calc_candles_success_count}/{allfound} successful, {calc_candles_failed_count} failed, total candles in between: {total_candles_inbetween}", "INFO")
        log_and_print(f"Candles after breakout parent summary: {cabp_success_count}/{allfound} successful, {cabp_failed_count} failed, total trendlines processed: {total_cabp_trendlines_processed}, total candles fetched: {total_cabp_candles_fetched}", "INFO")
        log_and_print(f"Order holder prices summary: {order_holder_prices_success_count}/{allfound} successful, {order_holder_prices_failed_count} failed, total orders processed: {total_orders_processed}", "INFO")
        log_and_print(f"Pending order updater summary: {pending_order_updater_success_count}/{allfound} successful, {pending_order_updater_failed_count} failed, total orders updated: {total_orders_updated}", "INFO")
        log_and_print(f"Collect pending orders summary: {collect_pending_success_count}/{allfound} successful, {collect_pending_failed_count} failed, total pending orders collected: {total_pending_orders_collected}", "INFO")
        log_and_print(f"Lock pending orders summary: {lock_pending_success_count}/{allfound} successful, {lock_pending_failed_count} failed, total orders locked: {total_orders_locked}", "INFO")
        log_and_print(f"Markets list status summary: {markets_list_success_count}/{allfound} successful, {markets_list_failed_count} failed, total markets processed: {total_markets_processed}", "INFO")
        log_and_print(f"Match trendline summary: {match_success_count}/{allfound} successful, {match_failed_count} failed, {match_no_pending_count} with no pending orders, total trendlines matched: {total_trendlines_matched}, total warnings: {total_warnings}", "INFO")
        log_and_print(f"Sum success / allfound: {success_count}/{allfound}", "SUCCESS")
        log_and_print(f"Markets with verification.json and all timeframes 'verified': {len(markets_with_all_timeframes_verified)}", "INFO")
        log_and_print(f"Result: {success_count}/{failed_count}", "INFO")
        
        summary = {
            "status": "success" if success_data or no_pending_data else "failed",
            "message": f"Processed {allfound} markets: {success_count} successful, {no_pending_count} with no pending orders, {failed_count} failed",
            "markets_processed": allfound,
            "successful_markets": [d["market"] for d in success_data],
            "no_pending_markets": [d["market"] for d in no_pending_data],
            "failed_markets": [d["market"] for d in failed_data],
            "failed_details": [{"market": d["market"], "error": d["error_message"]} for d in failed_data],
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "process_messages": process_messages,
            "markets_with_all_timeframes_verified": markets_with_all_timeframes_verified
        }
        
        log_and_print(
            f"M5 Processing Summary: {success_count} markets processed successfully, "
            f"{no_pending_count} with no pending orders, {failed_count} failed",
            "INFO"
        )
        marketsliststatus()
        
        if failed_data:
            for d in failed_data:
                log_and_print(f"Failed market {d['market']}: {d['error_message']}", "ERROR")
        
        return summary

    except Exception as e:
        error_message = f"Unexpected error in process_5minutes_timeframe: {str(e)}"
        log_and_print(error_message, "ERROR")
        return {
            "status": "failed",
            "message": error_message,
            "markets_processed": 0,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "process_messages": {}
        }
    finally:
        try:
            mt5.shutdown()
        except Exception as e:
            log_and_print(f"Error shutting down MT5: {str(e)}", "WARNING")

def process_market_timeframe(market: str, timeframe: str) -> Tuple[bool, Optional[str], str, Dict]:
    """Process a single market and timeframe combination, returning success status, error message, status, and process messages."""
    error_message = None
    status = "failed"
    process_messages = {}
    try:
        log_and_print(f"Processing market: {market}, timeframe: {timeframe}", "INFO")
        
        # Fetch candle data
        candle_data, json_dir, fetch_status = fetch_candle_data(market, timeframe)
        process_messages["fetch_candle_data"] = {
            "status": fetch_status["status"],
            "message": fetch_status["message"],
            "candle_count": fetch_status["candle_count"],
            "timestamp": fetch_status["timestamp"]
        }
        if candle_data is None or json_dir is None:
            error_message = f"Failed to fetch candle data for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            return False, error_message, "failed", process_messages
        
        # Verify candle_data.json
        candle_data_path = os.path.join(json_dir, 'candle_data.json')
        if not os.path.exists(candle_data_path) or os.path.getsize(candle_data_path) == 0:
            error_message = f"candle_data.json is missing or empty for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["fetch_candle_data"]["warnings"] = process_messages["fetch_candle_data"].get("warnings", []) + [error_message]
            return False, error_message, "failed", process_messages
        
        try:
            with open(candle_data_path, 'r') as f:
                candle_data_content = json.load(f)
            candle_count = len(candle_data_content)
            process_messages["fetch_candle_data"]["verified_candle_count"] = candle_count
        except json.JSONDecodeError as e:
            error_message = f"Error decoding candle_data.json for {market} {timeframe}: {str(e)}"
            log_and_print(error_message, "ERROR")
            process_messages["fetch_candle_data"]["warnings"] = process_messages["fetch_candle_data"].get("warnings", []) + [error_message]
            return False, error_message, "failed", process_messages

        # Save the most recent completed candle
        success, error_msg, save_status, save_status_report = save_new_mostrecent_completed_candle(market, timeframe, json_dir)
        process_messages["save_new_mostrecent_completed_candle"] = {
            "status": save_status_report["status"],
            "message": save_status_report["message"],
            "candles_fetched": save_status_report["candles_fetched"],
            "timestamp": save_status_report["timestamp"],
            "warnings": save_status_report["warnings"],
            "candle_time": save_status_report.get("candle_time", "unknown")
        }
        if not success:
            error_message = error_msg or f"Failed to save most recent completed candle for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["save_new_mostrecent_completed_candle"]["message"] = error_message
        else:
            process_messages["save_new_mostrecent_completed_candle"]["verified_candle_time"] = save_status_report["candle_time"]

        # Match most recent completed candle with candle data
        success, error_msg, match_status, match_status_report = match_mostrecent_candle(market, timeframe, json_dir)
        process_messages["match_mostrecent_candle"] = {
            "status": match_status_report["status"],
            "message": match_status_report["message"],
            "candles_matched": match_status_report["candles_matched"],
            "timestamp": match_status_report["timestamp"],
            "warnings": match_status_report["warnings"],
            "match_result_status": match_status_report.get("match_result_status", "none"),
            "candles_inbetween": match_status_report.get("candles_inbetween", 0)
        }
        if not success:
            error_message = error_msg or f"Failed to match most recent completed candle for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["match_mostrecent_candle"]["message"] = error_message
        else:
            matched_candles_path = os.path.join(json_dir, 'matchedcandles.json')
            if os.path.exists(matched_candles_path) and os.path.getsize(matched_candles_path) > 0:
                try:
                    with open(matched_candles_path, 'r') as f:
                        matched_data = json.load(f)
                    process_messages["match_mostrecent_candle"]["verified_match_result_status"] = matched_data.get('match_result_status', 'unknown')
                    process_messages["match_mostrecent_candle"]["verified_candles_inbetween"] = matched_data.get('candles_inbetween', 0)
                except json.JSONDecodeError as e:
                    process_messages["match_mostrecent_candle"]["warnings"] = process_messages["match_mostrecent_candle"].get("warnings", []) + [f"Error decoding matchedcandles.json: {str(e)}"]
            else:
                process_messages["match_mostrecent_candle"]["warnings"] = process_messages["match_mostrecent_candle"].get("warnings", []) + [f"matchedcandles.json is missing or empty for {market} {timeframe}"]

        # Calculate candles in between
        success, error_msg, calc_status, calc_status_report = calculate_candles_inbetween(market, timeframe, json_dir)
        process_messages["calculate_candles_inbetween"] = {
            "status": calc_status_report["status"],
            "message": calc_status_report["message"],
            "candles_inbetween": calc_status_report["candles_inbetween"],
            "plus_newmostrecent": calc_status_report["plus_newmostrecent"],
            "timestamp": calc_status_report["timestamp"],
            "warnings": calc_status_report["warnings"]
        }
        if not success:
            error_message = error_msg or f"Failed to calculate candles in between for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["calculate_candles_inbetween"]["message"] = error_message
        else:
            inbetween_path = os.path.join(json_dir, 'candlesamountinbetween.json')
            if os.path.exists(inbetween_path) and os.path.getsize(inbetween_path) > 0:
                try:
                    with open(inbetween_path, 'r') as f:
                        inbetween_data = json.load(f)
                    process_messages["calculate_candles_inbetween"]["verified_candles_inbetween"] = inbetween_data.get('candles in between', 0)
                    process_messages["calculate_candles_inbetween"]["verified_plus_newmostrecent"] = inbetween_data.get('plus_newmostrecent', 0)
                except json.JSONDecodeError as e:
                    process_messages["calculate_candles_inbetween"]["warnings"] = process_messages["calculate_candles_inbetween"].get("warnings", []) + [f"Error decoding candlesamountinbetween.json: {str(e)}"]
            else:
                process_messages["calculate_candles_inbetween"]["warnings"] = process_messages["calculate_candles_inbetween"].get("warnings", []) + [f"candlesamountinbetween.json is missing or empty for {market} {timeframe}"]

        # Match trendline with candle data
        success, error_msg, trendline_status, match_status = match_trendline_with_candle_data(candle_data, json_dir, market, timeframe)
        process_messages["match_trendline_with_candle_data"] = {
            "status": match_status["status"],
            "message": match_status["message"],
            "trendline_count": match_status["trendline_count"],
            "timestamp": match_status["timestamp"],
            "warnings": match_status.get("warnings", [])
        }
        if not success:
            log_and_print(error_msg or f"Failed to process pending orders for {market} {timeframe}", "INFO" if trendline_status == "no_pending_orders" else "ERROR")
            if trendline_status == "no_pending_orders":
                pending_json_path = os.path.join(BASE_PROCESSING_FOLDER, market.replace(" ", "_"), timeframe.lower(), "pendingorder.json")
                if not os.path.exists(pending_json_path):
                    process_messages["match_trendline_with_candle_data"]["message"] = f"No pending orders found because pendingorder.json does not exist for {market} {timeframe}"
                else:
                    try:
                        with open(pending_json_path, 'r') as f:
                            pending_data = json.load(f)
                        if not pending_data:
                            process_messages["match_trendline_with_candle_data"]["message"] = f"No pending orders found because pendingorder.json is empty for {market} {timeframe}"
                        else:
                            process_messages["match_trendline_with_candle_data"]["message"] = f"No pending orders found for {market} {timeframe}"
                    except json.JSONDecodeError as e:
                        process_messages["match_trendline_with_candle_data"]["warnings"] = process_messages["match_trendline_with_candle_data"].get("warnings", []) + [f"Error decoding pendingorder.json: {str(e)}"]
                        error_message = f"Failed to process pending orders due to invalid pendingorder.json for {market} {timeframe}"
                        return False, error_message, "failed", process_messages
            else:
                process_messages["match_trendline_with_candle_data"]["message"] = f"Failed to match pending orders: {error_msg}"
                return False, error_msg, trendline_status, process_messages
        else:
            pricecandle_path = os.path.join(json_dir, 'pricecandle.json')
            if os.path.exists(pricecandle_path) and os.path.getsize(pricecandle_path) > 0:
                try:
                    with open(pricecandle_path, 'r') as f:
                        pricecandle_data = json.load(f)
                    process_messages["match_trendline_with_candle_data"]["verified_trendline_count"] = len(pricecandle_data)
                except json.JSONDecodeError as e:
                    process_messages["match_trendline_with_candle_data"]["warnings"] = process_messages["match_trendline_with_candle_data"].get("warnings", []) + [f"Error decoding pricecandle.json: {str(e)}"]
            else:
                process_messages["match_trendline_with_candle_data"]["warnings"] = process_messages["match_trendline_with_candle_data"].get("warnings", []) + [f"pricecandle.json is missing or empty for {market} {timeframe}"]

        # Fetch candles from after Breakout_parent to current price
        success, error_msg, cabp_status, cabp_status_report = candleafterbreakoutparent_to_currentprice(market, timeframe, json_dir)
        process_messages["candleafterbreakoutparent_to_currentprice"] = {
            "status": cabp_status_report["status"],
            "message": cabp_status_report["message"],
            "trendlines_processed": cabp_status_report["trendlines_processed"],
            "total_candles_fetched": cabp_status_report["total_candles_fetched"],
            "timestamp": cabp_status_report["timestamp"],
            "warnings": cabp_status_report["warnings"]
        }
        if not success:
            error_message = error_msg or f"Failed to fetch candles after Breakout_parent for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["candleafterbreakoutparent_to_currentprice"]["message"] = error_message
        else:
            cabp_path = os.path.join(json_dir, 'candlesafterbreakoutparent.json')
            if os.path.exists(cabp_path) and os.path.getsize(cabp_path) > 0:
                try:
                    with open(cabp_path, 'r') as f:
                        cabp_data = json.load(f)
                    trendline_count = len(cabp_data)
                    total_candles = sum(len(trendline.get('candles', [])) for trendline in cabp_data)
                    process_messages["candleafterbreakoutparent_to_currentprice"]["verified_trendline_count"] = trendline_count
                    process_messages["candleafterbreakoutparent_to_currentprice"]["verified_total_candles"] = total_candles
                except json.JSONDecodeError as e:
                    process_messages["candleafterbreakoutparent_to_currentprice"]["warnings"] = process_messages["candleafterbreakoutparent_to_currentprice"].get("warnings", []) + [f"Error decoding candlesafterbreakoutparent.json: {str(e)}"]
            else:
                process_messages["candleafterbreakoutparent_to_currentprice"]["warnings"] = process_messages["candleafterbreakoutparent_to_currentprice"].get("warnings", []) + [f"candlesafterbreakoutparent.json is missing or empty for {market} {timeframe}"]

        # Calculate order holder prices with lot size and risk
        if not getorderholderpriceswithlotsizeandrisk(market, timeframe, json_dir):
            error_message = f"Failed to calculate order holder prices with lot size and risk for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["getorderholderpriceswithlotsizeandrisk"] = error_message
        else:
            output_json_path = os.path.join(json_dir, 'calculatedprices.json')
            if os.path.exists(output_json_path) and os.path.getsize(output_json_path) > 0:
                try:
                    with open(output_json_path, 'r') as f:
                        calculated_data = json.load(f)
                    process_messages["getorderholderpriceswithlotsizeandrisk"] = (
                        f"Calculated prices for {len(calculated_data)} trendlines for {market} {timeframe}"
                    )
                except json.JSONDecodeError as e:
                    process_messages["getorderholderpriceswithlotsizeandrisk"] = f"Error decoding calculatedprices.json: {str(e)}"
                    process_messages["getorderholderpriceswithlotsizeandrisk_warnings"] = [f"Error decoding calculatedprices.json: {str(e)}"]
            else:
                process_messages["getorderholderpriceswithlotsizeandrisk"] = (
                    f"No calculated prices saved for {market} {timeframe}"
                )
                process_messages["getorderholderpriceswithlotsizeandrisk_warnings"] = [f"calculatedprices.json is missing or empty for {market} {timeframe}"]

        # Track breakeven, stoploss, and profit
        if not PendingOrderUpdater(market, timeframe, json_dir):
            error_message = f"Failed to track breakeven, stoploss, and profit for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["PendingOrderUpdater"] = error_message
        else:
            pricecandle_path = os.path.join(json_dir, 'pricecandle.json')
            if os.path.exists(pricecandle_path) and os.path.getsize(pricecandle_path) > 0:
                try:
                    with open(pricecandle_path, 'r') as f:
                        pricecandle_data = json.load(f)
                    contract_statuses = [t.get('contract status summary', {}).get('contract status', 'unknown') for t in pricecandle_data]
                    process_messages["PendingOrderUpdater"] = (
                        f"Tracked breakeven, stoploss, and profit for {len(pricecandle_data)} trendlines: {', '.join(set(contract_statuses))} for {market} {timeframe}"
                    )
                except json.JSONDecodeError as e:
                    process_messages["PendingOrderUpdater"] = f"Error decoding pricecandle.json: {str(e)}"
                    process_messages["PendingOrderUpdater_warnings"] = [f"Error decoding pricecandle.json: {str(e)}"]
            else:
                process_messages["PendingOrderUpdater"] = f"No pricecandle data saved for {market} {timeframe}"
                process_messages["PendingOrderUpdater_warnings"] = [f"pricecandle.json is missing or empty for {market} {timeframe}"]

        # Collect pending orders
        if not collect_all_pending_orders(market, timeframe, json_dir):
            error_message = f"Failed to collect pending orders for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["collect_all_pending_orders"] = error_message
        else:
            output_json_path = os.path.join(json_dir, 'contractpendingorders.json')
            collective_pending_path = os.path.join(BASE_OUTPUT_FOLDER, 'temp_pendingorders.json')
            pending_count = 0
            all_pending_count = 0
            if os.path.exists(output_json_path) and os.path.getsize(output_json_path) > 0:
                try:
                    with open(output_json_path, 'r') as f:
                        contract_data = json.load(f)
                    pending_count = len(contract_data)
                except json.JSONDecodeError as e:
                    process_messages["collect_all_pending_orders_warnings"] = [f"Error decoding contractpendingorders.json: {str(e)}"]
            if os.path.exists(collective_pending_path) and os.path.getsize(collective_pending_path) > 0:
                try:
                    with open(collective_pending_path, 'r') as f:
                        all_pending_data = json.load(f)
                    all_pending_count = len(all_pending_data.get('orders', []))
                except json.JSONDecodeError as e:
                    process_messages["collect_all_pending_orders_warnings"] = process_messages.get("collect_all_pending_orders_warnings", []) + [f"Error decoding temp_pendingorders.json: {str(e)}"]
            process_messages["collect_all_pending_orders"] = (
                f"Collected {pending_count} pending orders for {market} {timeframe}, total {all_pending_count} in collective"
            )

        # Generate markets order list status
        if not marketsliststatus():
            error_message = f"Failed to generate markets order list status for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["marketsliststatus"] = error_message
        else:
            markets_order_list_path = os.path.join(BASE_OUTPUT_FOLDER, "marketsorderlist.json")
            if os.path.exists(markets_order_list_path) and os.path.getsize(markets_order_list_path) > 0:
                try:
                    with open(markets_order_list_path, 'r') as f:
                        markets_data = json.load(f)
                    pending_markets = markets_data.get("markets_pending", {})
                    order_free = markets_data.get("order_free_markets", {})
                    process_messages["marketsliststatus"] = (
                        f"Generated markets order list: {len(pending_markets)} markets with pending orders, "
                        f"order-free markets - M5: {len(order_free.get('market_m5', []))}, "
                        f"M15: {len(order_free.get('market_m15', []))}, "
                        f"M30: {len(order_free.get('market_m30', []))}, "
                        f"H1: {len(order_free.get('market_1H', []))}, "
                        f"H4: {len(order_free.get('market_4H', []))}"
                    )
                except json.JSONDecodeError as e:
                    process_messages["marketsliststatus"] = f"Error decoding marketsorderlist.json: {str(e)}"
                    process_messages["marketsliststatus_warnings"] = [f"Error decoding marketsorderlist.json: {str(e)}"]
            else:
                process_messages["marketsliststatus"] = (
                    f"No markets order list generated for {market} {timeframe}"
                )
                process_messages["marketsliststatus_warnings"] = [f"marketsorderlist.json is missing or empty for {market} {timeframe}"]

        log_and_print(f"Completed processing market: {market}, timeframe: {timeframe}", "SUCCESS")
        return True, None, "success", process_messages

    except Exception as e:
        error_message = f"Unexpected error processing market {market} timeframe {timeframe}: {str(e)}"
        log_and_print(error_message, "ERROR")
        process_messages["error"] = error_message
        return False, error_message, "failed", process_messages
         
def main():
    """Main function to process markets for all timeframes with valid verification.json, using markets from batchbybatch.json, saving all status to marketsstatus.json."""
    try:
        log_and_print("===== Fetch and Process Candle Data =====", "TITLE")
        
        # Verify that credentials were loaded
        if not all([LOGIN_ID, PASSWORD, SERVER, TERMINAL_PATH]):
            log_and_print("Credentials not properly loaded from base.json. Exiting.", "ERROR")
            return
        
        # Load markets from batchbybatch.json
        batch_json_path = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\batches\batchbybatch.json"
        if not os.path.exists(batch_json_path):
            log_and_print(f"batchbybatch.json not found at {batch_json_path}. Exiting.", "ERROR")
            return
        
        try:
            with open(batch_json_path, 'r') as f:
                batch_data = json.load(f)
            batch_markets = batch_data.get("markets", [])
            if not batch_markets:
                log_and_print("No markets found in batchbybatch.json. Exiting.", "ERROR")
                return
        except json.JSONDecodeError as e:
            log_and_print(f"Error decoding batchbybatch.json: {str(e)}. Exiting.", "ERROR")
            return
        
        log_and_print(f"Loaded {len(batch_markets)} markets from batchbybatch.json", "INFO")
        
        # Filter markets that are in both batchbybatch.json and MARKETS
        valid_markets = [market for market in batch_markets if market in MARKETS]
        if not valid_markets:
            log_and_print("No valid markets found in both batchbybatch.json and MARKETS list. Exiting.", "ERROR")
            return
        
        # Check M15 candle time left globally using the first valid market
        default_market = valid_markets[0]
        timeframe = "M15"
        log_and_print(f"Checking M15 candle time left using market: {default_market}", "INFO")
        time_left, next_close_time = candletimeleft(default_market, timeframe, None, min_time_left=0.3)
        
        if time_left is None or next_close_time is None:
            log_and_print(f"Failed to retrieve candle time for {default_market} (M15). Exiting.", "ERROR")
            return
        
        log_and_print(f"M15 candle time left: {time_left:.2f} minutes. Proceeding with execution.", "INFO")

        # Create tasks for market-timeframe combinations with valid verification.json
        tasks = []
        markets_with_all_timeframes_verified = []
        for market in valid_markets:
            if check_verification_json(market):
                markets_with_all_timeframes_verified.append(market)
                for timeframe in TIMEFRAMES:
                    tasks.append((market, timeframe))
            else:
                log_and_print(f"Skipping market {market}: verification.json not valid or missing 'chart_identified' or 'all_timeframes' verified", "WARNING")
        
        allfound = len(tasks)
        if not tasks:
            log_and_print("No markets with valid verification.json found for processing. Exiting.", "WARNING")
            return
        
        log_and_print(f"Processing {allfound} market-timeframe combinations with valid verification.json", "INFO")
        success_data = []
        no_pending_data = []
        failed_data = []
        fetch_statuses = []
        match_statuses = []
        save_mostrecent_statuses = []
        match_mostrecent_statuses = []
        calc_candles_inbetween_statuses = []
        candles_after_breakout_statuses = []
        executioner_candle_statuses = []
        order_holder_prices_statuses = []
        pending_order_updater_statuses = []
        collect_pending_orders_statuses = []
        lock_pending_orders_statuses = []
        collect_executioner_orders_statuses = []
        markets_list_statuses = []

        with multiprocessing.Pool(processes=4) as pool:
            results = pool.starmap(process_market_timeframe, tasks)

        # Collect status for each market-timeframe combination
        for (market, timeframe), (success, error_message, status, process_messages) in zip(tasks, results):
            # Collect fetch_candle_data status
            fetch_status = process_messages.get("fetch_candle_data", {})
            fetch_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "fetch_status": fetch_status.get("status", "unknown"),
                "fetch_message": fetch_status.get("message", "No fetch data"),
                "candle_count": fetch_status.get("candle_count", 0),
                "timestamp": fetch_status.get("timestamp", time.strftime("%Y-%m-%d %H:%M:%S"))
            })
            # Collect match_trendline_with_candle_data status
            match_status = process_messages.get("match_trendline_with_candle_data", {})
            match_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "match_status": match_status.get("status", "unknown"),
                "match_message": match_status.get("message", "No match data"),
                "trendline_count": match_status.get("trendline_count", 0),
                "timestamp": match_status.get("timestamp", time.strftime("%Y-%m-%d %H:%M:%S")),
                "warnings": match_status.get("warnings", [])
            })
            # Collect save_new_mostrecent_completed_candle status
            save_status = process_messages.get("save_new_mostrecent_completed_candle", {})
            save_mostrecent_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "save_status": save_status.get("status", "unknown"),
                "save_message": save_status.get("message", "No save data"),
                "candles_fetched": save_status.get("candles_fetched", 0),
                "timestamp": save_status.get("timestamp", time.strftime("%Y-%m-%d %H:%M:%S")),
                "warnings": save_status.get("warnings", [])
            })
            # Collect match_mostrecent_candle status
            match_mostrecent_status = process_messages.get("match_mostrecent_candle", {})
            match_mostrecent_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "match_mostrecent_status": match_mostrecent_status.get("status", "unknown"),
                "match_mostrecent_message": match_mostrecent_status.get("message", "No match most recent data"),
                "candles_matched": match_mostrecent_status.get("candles_matched", 0),
                "timestamp": match_mostrecent_status.get("timestamp", time.strftime("%Y-%m-%d %H:%M:%S")),
                "warnings": match_mostrecent_status.get("warnings", []),
                "match_result_status": match_mostrecent_status.get("match_result_status", "none"),
                "candles_inbetween": match_mostrecent_status.get("candles_inbetween", 0)
            })
            # Collect calculate_candles_inbetween status
            calc_status = process_messages.get("calculate_candles_inbetween", {})
            calc_candles_inbetween_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "calc_status": calc_status.get("status", "unknown"),
                "calc_message": calc_status.get("message", "No calculate candles in between data"),
                "candles_inbetween": calc_status.get("candles_inbetween", 0),
                "plus_newmostrecent": calc_status.get("plus_newmostrecent", 0),
                "timestamp": calc_status.get("timestamp", time.strftime("%Y-%m-%d %H:%M:%S")),
                "warnings": calc_status.get("warnings", [])
            })
            # Collect candleafterbreakoutparent_to_currentprice status
            cabp_status = process_messages.get("candleafterbreakoutparent_to_currentprice", {})
            candles_after_breakout_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "cabp_status": cabp_status.get("status", "unknown"),
                "cabp_message": cabp_status.get("message", "No candles after breakout data"),
                "trendlines_processed": cabp_status.get("trendlines_processed", 0),
                "total_candles_fetched": cabp_status.get("total_candles_fetched", 0),
                "timestamp": cabp_status.get("timestamp", time.strftime("%Y-%m-%d %H:%M:%S")),
                "warnings": cabp_status.get("warnings", [])
            })
            # Collect getorderholderpriceswithlotsizeandrisk status
            order_holder_prices_status = process_messages.get("getorderholderpriceswithlotsizeandrisk", {})
            order_holder_prices_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "order_holder_prices_status": "success" if isinstance(order_holder_prices_status, str) and "Calculated prices" in order_holder_prices_status else "failed",
                "order_holder_prices_message": order_holder_prices_status if isinstance(order_holder_prices_status, str) else "No order holder prices data",
                "orders_processed": 0,
                "verified_order_count": 0,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "warnings": []
            })
            if isinstance(order_holder_prices_status, str) and "Calculated prices" in order_holder_prices_status:
                try:
                    output_json_path = os.path.join(BASE_OUTPUT_FOLDER, market.replace(" ", "_"), timeframe.lower(), 'calculatedprices.json')
                    if os.path.exists(output_json_path):
                        with open(output_json_path, 'r') as f:
                            calculated_data = json.load(f)
                        order_holder_prices_statuses[-1]["orders_processed"] = len(calculated_data)
                        order_holder_prices_statuses[-1]["verified_order_count"] = len(calculated_data)
                except Exception as e:
                    order_holder_prices_statuses[-1]["warnings"].append(f"Error reading calculatedprices.json: {str(e)}")
            # Collect PendingOrderUpdater status
            pending_order_updater_status = process_messages.get("PendingOrderUpdater", {})
            pending_order_updater_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "pending_order_updater_status": "success" if isinstance(pending_order_updater_status, str) and "Tracked breakeven" in pending_order_updater_status else "failed",
                "pending_order_updater_message": pending_order_updater_status if isinstance(pending_order_updater_status, str) else "No pending order updater data",
                "orders_updated": 0,
                "verified_order_count": 0,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "warnings": []
            })
            if isinstance(pending_order_updater_status, str) and "Tracked breakeven" in pending_order_updater_status:
                try:
                    pricecandle_json_path = os.path.join(BASE_OUTPUT_FOLDER, market.replace(" ", "_"), timeframe.lower(), 'pricecandle.json')
                    if os.path.exists(pricecandle_json_path):
                        with open(pricecandle_json_path, 'r') as f:
                            pricecandle_data = json.load(f)
                        pending_order_updater_statuses[-1]["orders_updated"] = len(pricecandle_data)
                        pending_order_updater_statuses[-1]["verified_order_count"] = len(pricecandle_data)
                except Exception as e:
                    pending_order_updater_statuses[-1]["warnings"].append(f"Error reading pricecandle.json: {str(e)}")


            # Collect collect_all_pending_orders status
            collect_pending_orders_status = process_messages.get("collect_all_pending_orders", {})
            collect_pending_orders_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "collect_pending_status": "success" if isinstance(collect_pending_orders_status, str) and "Collected" in collect_pending_orders_status else "failed",
                "collect_pending_message": collect_pending_orders_status if isinstance(collect_pending_orders_status, str) else "No collect pending orders data",
                "pending_orders_collected": 0,
                "verified_pending_count": 0,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "warnings": []
            })
            if isinstance(collect_pending_orders_status, str) and "Collected" in collect_pending_orders_status:
                try:
                    pending_json_path = os.path.join(BASE_OUTPUT_FOLDER, market.replace(" ", "_"), timeframe.lower(), 'contractpendingorders.json')
                    if os.path.exists(pending_json_path):
                        with open(pending_json_path, 'r') as f:
                            pending_data = json.load(f)
                        collect_pending_orders_statuses[-1]["pending_orders_collected"] = len(pending_data)
                        collect_pending_orders_statuses[-1]["verified_pending_count"] = len(pending_data)
                except Exception as e:
                    collect_pending_orders_statuses[-1]["warnings"].append(f"Error reading contractpendingorders.json: {str(e)}")
            # Collect lockpendingorders status
            lock_pending_status = process_messages.get("lockpendingorders", {})
            lock_pending_orders_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "lock_pending_status": "success" if isinstance(lock_pending_status, str) and "Saved" in lock_pending_status else "failed",
                "lock_pending_message": lock_pending_status if isinstance(lock_pending_status, str) else "No lock pending orders data",
                "orders_locked": 0,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "warnings": []
            })
            if isinstance(lock_pending_status, str) and "Saved" in lock_pending_status:
                try:
                    locked_pending_path = os.path.join(BASE_OUTPUT_FOLDER, 'lockedpendingorders.json')
                    if os.path.exists(locked_pending_path):
                        with open(locked_pending_path, 'r') as f:
                            locked_data = json.load(f)
                        lock_pending_orders_statuses[-1]["orders_locked"] = locked_data.get("temp_pendingorders", 0)
                except Exception as e:
                    lock_pending_orders_statuses[-1]["warnings"].append(f"Error reading lockedpendingorders.json: {str(e)}")
            # Collect marketsliststatus status
            markets_list_status = process_messages.get("marketsliststatus", {})
            markets_list_statuses.append({
                "market": market,
                "timeframe": timeframe,
                "markets_list_status": "success" if isinstance(markets_list_status, str) and "Generated" in markets_list_status else "failed",
                "markets_list_message": markets_list_status if isinstance(markets_list_status, str) else "No markets list status data",
                "markets_processed": 0,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "warnings": []
            })
            if isinstance(markets_list_status, str) and "Generated" in markets_list_status:
                try:
                    markets_order_list_path = os.path.join(BASE_OUTPUT_FOLDER, "marketsorderlist.json")
                    if os.path.exists(markets_order_list_path):
                        with open(markets_order_list_path, 'r') as f:
                            markets_data = json.load(f)
                        markets_list_statuses[-1]["markets_processed"] = len(markets_data.get("markets_pending", {})) + sum(len(markets_data.get("order_free_markets", {}).get(f"market_{tf.lower()}", [])) for tf in TIMEFRAMES)
                except Exception as e:
                    markets_list_statuses[-1]["warnings"].append(f"Error reading marketsorderlist.json: {str(e)}")
            
            if status == "success":
                success_data.append({
                    "market": market,
                    "timeframe": timeframe,
                    "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "process_messages": process_messages
                })
            elif status == "no_pending_orders":
                no_pending_data.append({
                    "market": market,
                    "timeframe": timeframe,
                    "message": process_messages.get("match_trendline_with_candle_data", {}).get("message", "No pending orders found"),
                    "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "process_messages": process_messages
                })
            else:
                failed_data.append({
                    "market": market,
                    "timeframe": timeframe,
                    "error_message": error_message,
                    "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "process_messages": process_messages
                })
        marketsliststatus()

        log_and_print("===== SUMMARY =====", "TITLE")
        success_count = len(success_data)
        no_pending_count = len(no_pending_data)
        failed_count = len(failed_data)
        fetch_success_count = sum(1 for fs in fetch_statuses if fs["fetch_status"] == "success")
        fetch_failed_count = sum(1 for fs in fetch_statuses if fs["fetch_status"] == "failed")
        total_candles_fetched = sum(fs["candle_count"] for fs in fetch_statuses)
        match_success_count = sum(1 for ms in match_statuses if ms["match_status"] == "success")
        match_failed_count = sum(1 for ms in match_statuses if ms["match_status"] == "failed")
        match_no_pending_count = sum(1 for ms in match_statuses if ms["match_status"] == "no_pending_orders")
        total_trendlines_matched = sum(ms["trendline_count"] for ms in match_statuses)
        total_warnings = sum(len(ms["warnings"]) for ms in match_statuses)
        save_mostrecent_success_count = sum(1 for ss in save_mostrecent_statuses if ss["save_status"] == "success")
        save_mostrecent_failed_count = sum(1 for ss in save_mostrecent_statuses if ss["save_status"] == "failed")
        total_mostrecent_candles_fetched = sum(ss["candles_fetched"] for ss in save_mostrecent_statuses)
        match_mostrecent_success_count = sum(1 for mms in match_mostrecent_statuses if mms["match_mostrecent_status"] == "success")
        match_mostrecent_failed_count = sum(1 for mms in match_mostrecent_statuses if mms["match_mostrecent_status"] == "failed")
        total_mostrecent_candles_matched = sum(mms["candles_matched"] for mms in match_mostrecent_statuses)
        calc_candles_success_count = sum(1 for cs in calc_candles_inbetween_statuses if cs["calc_status"] == "success")
        calc_candles_failed_count = sum(1 for cs in calc_candles_inbetween_statuses if cs["calc_status"] == "failed")
        total_candles_inbetween = sum(cs["candles_inbetween"] for cs in calc_candles_inbetween_statuses)
        cabp_success_count = sum(1 for cabp in candles_after_breakout_statuses if cabp["cabp_status"] == "success")
        cabp_failed_count = sum(1 for cabp in candles_after_breakout_statuses if cabp["cabp_status"] == "failed")
        total_cabp_trendlines_processed = sum(cabp["trendlines_processed"] for cabp in candles_after_breakout_statuses)
        total_cabp_candles_fetched = sum(cabp["total_candles_fetched"] for cabp in candles_after_breakout_statuses)
        exec_success_count = sum(1 for es in executioner_candle_statuses if es["exec_status"] == "success")
        exec_failed_count = sum(1 for es in executioner_candle_statuses if es["exec_status"] == "failed")
        total_exec_trendlines_processed = sum(es["trendlines_processed"] for es in executioner_candle_statuses)
        total_executioner_candles_found = sum(es["executioner_candles_found"] for es in executioner_candle_statuses)
        total_executioner_candles_not_found = sum(es["executioner_candles_not_found"] for es in executioner_candle_statuses)
        order_holder_prices_success_count = sum(1 for ohp in order_holder_prices_statuses if ohp["order_holder_prices_status"] == "success")
        order_holder_prices_failed_count = sum(1 for ohp in order_holder_prices_statuses if ohp["order_holder_prices_status"] == "failed")
        total_orders_processed = sum(ohp["orders_processed"] for ohp in order_holder_prices_statuses)
        pending_order_updater_success_count = sum(1 for pou in pending_order_updater_statuses if pou["pending_order_updater_status"] == "success")
        pending_order_updater_failed_count = sum(1 for pou in pending_order_updater_statuses if pou["pending_order_updater_status"] == "failed")
        total_orders_updated = sum(pou["orders_updated"] for pou in pending_order_updater_statuses)
        collect_pending_success_count = sum(1 for cpo in collect_pending_orders_statuses if cpo["collect_pending_status"] == "success")
        collect_pending_failed_count = sum(1 for cpo in collect_pending_orders_statuses if cpo["collect_pending_status"] == "failed")
        total_pending_orders_collected = sum(cpo["pending_orders_collected"] for cpo in collect_pending_orders_statuses)
        lock_pending_success_count = sum(1 for lpo in lock_pending_orders_statuses if lpo["lock_pending_status"] == "success")
        lock_pending_failed_count = sum(1 for lpo in lock_pending_orders_statuses if lpo["lock_pending_status"] == "failed")
        total_orders_locked = sum(lpo["orders_locked"] for lpo in lock_pending_orders_statuses)
        collect_executioner_success_count = sum(1 for ceo in collect_executioner_orders_statuses if ceo["collect_executioner_status"] == "success")
        collect_executioner_failed_count = sum(1 for ceo in collect_executioner_orders_statuses if ceo["collect_executioner_status"] == "failed")
        total_executioner_orders_collected = sum(ceo["executioner_orders_collected"] for ceo in collect_executioner_orders_statuses)
        markets_list_success_count = sum(1 for mls in markets_list_statuses if mls["markets_list_status"] == "success")
        markets_list_failed_count = sum(1 for mls in markets_list_statuses if mls["markets_list_status"] == "failed")
        total_markets_processed = sum(mls["markets_processed"] for mls in markets_list_statuses)
     
        log_and_print(f"Processing completed: {success_count}/{allfound}", "INFO")
        log_and_print(f"{no_pending_count} with no pending orders, {failed_count} failed", "INFO")
        log_and_print(f"Fetch candle data summary: {fetch_success_count}/{allfound} successful, {fetch_failed_count} failed, total candles fetched: {total_candles_fetched}", "INFO")
        log_and_print(f"Save most recent candle summary: {save_mostrecent_success_count}/{allfound} successful, {save_mostrecent_failed_count} failed, total candles fetched: {total_mostrecent_candles_fetched}", "INFO")
        log_and_print(f"Match most recent candle summary: {match_mostrecent_success_count}/{allfound} successful, {match_mostrecent_failed_count} failed, total candles matched: {total_mostrecent_candles_matched}", "INFO")
        log_and_print(f"Calculate candles in between summary: {calc_candles_success_count}/{allfound} successful, {calc_candles_failed_count} failed, total candles in between: {total_candles_inbetween}", "INFO")
        log_and_print(f"Candles after breakout parent summary: {cabp_success_count}/{allfound} successful, {cabp_failed_count} failed, total trendlines processed: {total_cabp_trendlines_processed}, total candles fetched: {total_cabp_candles_fetched}", "INFO")
        log_and_print(f"Executioner candle summary: {exec_success_count}/{allfound} successful, {exec_failed_count} failed, total trendlines processed: {total_exec_trendlines_processed}, executioner candles found: {total_executioner_candles_found}, not found: {total_executioner_candles_not_found}", "INFO")
        log_and_print(f"Order holder prices summary: {order_holder_prices_success_count}/{allfound} successful, {order_holder_prices_failed_count} failed, total orders processed: {total_orders_processed}", "INFO")
        log_and_print(f"Pending order updater summary: {pending_order_updater_success_count}/{allfound} successful, {pending_order_updater_failed_count} failed, total orders updated: {total_orders_updated}", "INFO")
        log_and_print(f"Collect pending orders summary: {collect_pending_success_count}/{allfound} successful, {collect_pending_failed_count} failed, total pending orders collected: {total_pending_orders_collected}", "INFO")
        log_and_print(f"Lock pending orders summary: {lock_pending_success_count}/{allfound} successful, {lock_pending_failed_count} failed, total orders locked: {total_orders_locked}", "INFO")
        log_and_print(f"Collect executioner orders summary: {collect_executioner_success_count}/{allfound} successful, {collect_executioner_failed_count} failed, total executioner orders collected: {total_executioner_orders_collected}", "INFO")
        log_and_print(f"Markets list status summary: {markets_list_success_count}/{allfound} successful, {markets_list_failed_count} failed, total markets processed: {total_markets_processed}", "INFO")
        log_and_print(f"Match trendline summary: {match_success_count}/{allfound} successful, {match_failed_count} failed, {match_no_pending_count} with no pending orders, total trendlines matched: {total_trendlines_matched}, total warnings: {total_warnings}", "INFO")
        log_and_print(f"Sum success / allfound: {success_count}/{allfound}", "SUCCESS")
        log_and_print(f"Markets with verification.json and all timeframes 'verified': {len(markets_with_all_timeframes_verified)}", "INFO")
        log_and_print(f"Result: {success_count}/{failed_count}", "INFO")
        
    except Exception as e:
        log_and_print(f"Error in main processing: {str(e)}", "ERROR")
    finally:
        cancel_limitorders()
        log_and_print("===== Fetch and Process Candle Data Completed =====", "TITLE")

if __name__ == "__main__":
    main()
    executeinsertpendingorderstodb()
    
    

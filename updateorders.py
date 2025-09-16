import json
import os
import multiprocessing
import time
from datetime import datetime, timezone,  timedelta
from typing import Dict, Optional, List, Tuple
import pandas as pd
import MetaTrader5 as mt5
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

def fetch_candle_data(market: str, timeframe: str) -> tuple[Optional[Dict], Optional[str]]:
    """Fetch candle data from MT5 for a specific market and timeframe."""
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
        log_and_print(f"Failed to initialize MT5 terminal for {market} {timeframe} after {MAX_RETRIES} attempts", "ERROR")
        return None, None

    # Wait for terminal to be fully ready
    for _ in range(5):
        if mt5.terminal_info() is not None:
            log_and_print(f"MT5 terminal fully initialized for {market} {timeframe}", "DEBUG")
            break
        log_and_print(f"Waiting for MT5 terminal to fully initialize for {market} {timeframe}...", "INFO")
        time.sleep(2)
    else:
        log_and_print(f"MT5 terminal not ready for {market} {timeframe}", "ERROR")
        mt5.shutdown()
        return None, None

    # Attempt login with retries
    for attempt in range(MAX_RETRIES):
        if mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=60000):
            log_and_print(f"Successfully logged in to MT5 for {market} {timeframe}", "SUCCESS")
            break
        error_code, error_message = mt5.last_error()
        log_and_print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to log in to MT5 for {market} {timeframe}. Error code: {error_code}, Message: {error_message}", "ERROR")
        time.sleep(RETRY_DELAY)
    else:
        log_and_print(f"Failed to log in to MT5 for {market} {timeframe} after {MAX_RETRIES} attempts", "ERROR")
        mt5.shutdown()
        return None, None

    # Select market symbol
    if not mt5.symbol_select(market, True):
        log_and_print(f"Failed to select market: {market}, error: {mt5.last_error()}", "ERROR")
        mt5.shutdown()
        return None, None

    # Get timeframe
    mt5_timeframe = TIMEFRAME_MAPPING.get(timeframe)
    if not mt5_timeframe:
        log_and_print(f"Invalid timeframe {timeframe} for {market}", "ERROR")
        mt5.shutdown()
        return None, None

    # Fetch candle data
    candles = mt5.copy_rates_from_pos(market, mt5_timeframe, 1, 500)
    if candles is None or len(candles) < 500:
        log_and_print(f"Failed to fetch candle data for {market} {timeframe}, error: {mt5.last_error()}", "ERROR")
        mt5.shutdown()
        return None, None

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
        #log_and_print(f"stored candle for position {position}: Time={candle_details['Time']}, Open={candle_details['Open']}, High={candle_details['High']}, Low={candle_details['Low']}, Close={candle_details['Close']}", "DEBUG")

    # Verify the first few candles for correct indexing
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
    except Exception as e:
        log_and_print(f"Error saving candle data for {market} {timeframe}: {e}", "ERROR")
        mt5.shutdown()
        return None, None

    mt5.shutdown()
    return candle_data, json_dir

def match_trendline_with_candle_data(candle_data: Dict, json_dir: str, market: str, timeframe: str) -> Tuple[bool, Optional[str], str]:
    """Match pending order data with candle data and save to pricecandle.json."""
    # Normalize timeframe
    normalized_timeframe = normalize_timeframe(timeframe)
    if normalized_timeframe is None:
        error_message = f"Invalid timeframe {timeframe} for {market}"
        log_and_print(error_message, "ERROR")
        return False, error_message, "failed"
    
    formatted_market_name = market.replace(" ", "_")
    pending_json_path = os.path.join(BASE_PROCESSING_FOLDER, formatted_market_name, normalized_timeframe.lower(), "pendingorder.json")

    # Check if pendingorder.json exists
    if not os.path.exists(pending_json_path):
        error_message = f"Pending order JSON file not found at {pending_json_path} for {market} {normalized_timeframe}"
        log_and_print(error_message, "ERROR")
        return False, error_message, "failed"

    # Read pendingorder.json
    try:
        with open(pending_json_path, 'r') as f:
            pending_data = json.load(f)
    except Exception as e:
        error_message = f"Error reading pending order JSON file at {pending_json_path} for {market} {normalized_timeframe}: {str(e)}"
        log_and_print(error_message, "ERROR")
        return False, error_message, "failed"

    # Check if pending_data is empty
    if not pending_data:
        error_message = f"No pending orders in pendingorder.json for {market} {normalized_timeframe}"
        log_and_print(error_message, "INFO")
        pricecandle_json_path = os.path.join(json_dir, "pricecandle.json")
        try:
            if os.path.exists(pricecandle_json_path):
                os.remove(pricecandle_json_path)
                log_and_print(f"Existing {pricecandle_json_path} deleted", "INFO")
            with open(pricecandle_json_path, 'w') as f:
                json.dump([], f, indent=4)
            log_and_print(f"Empty pricecandle.json saved for {market} {normalized_timeframe}", "INFO")
        except Exception as e:
            error_message = f"Error saving empty pricecandle.json for {market} {normalized_timeframe}: {str(e)}"
            log_and_print(error_message, "ERROR")
            return False, error_message, "failed"
        return False, error_message, "no_pending_orders"

    matched_data = []

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
            log_and_print(f"No candle data found for sender position {sender_pos} in {market} {normalized_timeframe}", "WARNING")

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
            log_and_print(f"No candle data found for receiver position {receiver_pos} in {market} {normalized_timeframe}", "WARNING")

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
                log_and_print(f"No candle data found for order holder position {order_holder_pos} in {market} {normalized_timeframe}", "WARNING")
                matched_entry["order_holder"] = {
                    "label": order_holder_label,
                    "position_number": order_holder_pos
                }
        else:
            log_and_print(f"No order holder found for trendline in {market} {normalized_timeframe}", "WARNING")
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
                log_and_print(f"No candle data found for Breakout_parent position {breakout_parent_pos} in {market} {normalized_timeframe}", "WARNING")

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
                log_and_print(f"No candle data found for position {next_candle_pos} (right after Breakout_parent) in {market} {normalized_timeframe}", "WARNING")
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
            log_and_print(f"No Breakout_parent found or invalid for trendline in {market} {normalized_timeframe}", "WARNING")
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

    pricecandle_json_path = os.path.join(json_dir, "pricecandle.json")
    if os.path.exists(pricecandle_json_path):
        os.remove(pricecandle_json_path)
        log_and_print(f"Existing {pricecandle_json_path} deleted", "INFO")

    try:
        with open(pricecandle_json_path, 'w') as f:
            json.dump(matched_data, f, indent=4)
        log_and_print(f"Matched pending order and candle data saved to {pricecandle_json_path} for {market} {normalized_timeframe}", "SUCCESS")
    except Exception as e:
        error_message = f"Error saving pricecandle.json for {market} {normalized_timeframe}: {str(e)}"
        log_and_print(error_message, "ERROR")
        return False, error_message, "failed"

    return True, None, "success"

def match_mostrecent_candle(market: str, timeframe: str, json_dir: str) -> bool:
    """Match the most recent completed candle with candle data and save to matchedcandles.json."""
    log_and_print(f"Matching most recent completed candle for market={market}, timeframe={timeframe}", "INFO")
    
    # Define file paths
    formatted_market_name = market.replace(" ", "_")
    mostrecent_json_path = os.path.join(FETCHCHART_DESTINATION_PATH, formatted_market_name, timeframe.lower(), "mostrecent_completedcandle.json")
    candle_data_json_path = os.path.join(json_dir, "candle_data.json")
    matched_candles_json_path = os.path.join(json_dir, "matchedcandles.json")
    
    # Check if both JSON files exist
    if not os.path.exists(mostrecent_json_path):
        log_and_print(f"mostrecent_completedcandle.json not found at {mostrecent_json_path} for {market} {timeframe}", "ERROR")
        return False
    if not os.path.exists(candle_data_json_path):
        log_and_print(f"candle_data.json not found at {candle_data_json_path} for {market} {timeframe}", "ERROR")
        return False
    
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
            log_and_print(f"No timestamp found in mostrecent_completedcandle.json for {market} {timeframe}", "ERROR")
            return False
        
        # Parse timestamp (ISO format: "2025-08-28T13:15:00+00:00")
        try:
            mostrecent_timestamp = datetime.fromisoformat(mostrecent_timestamp_str.replace('Z', '+00:00'))
        except ValueError as e:
            log_and_print(f"Invalid timestamp format in mostrecent_completedcandle.json: {mostrecent_timestamp_str}, error: {e}", "ERROR")
            return False
        
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
                log_and_print(f"No timestamp found for {candle_key} in candle_data.json for {market} {timeframe}", "WARNING")
                continue
            
            try:
                # Parse candle timestamp and make it offset-aware (assume UTC)
                candle_timestamp = datetime.strptime(candle_timestamp_str, "%Y-%m-%d %H:%M:%S")
                candle_timestamp = candle_timestamp.replace(tzinfo=timezone.utc)
            except ValueError as e:
                log_and_print(f"Invalid timestamp format for {candle_key} in candle_data.json: {candle_timestamp_str}, error: {e}", "WARNING")
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
                # Determine if the matched candle is behind or ahead
                match_result_status = "nearestahead" if time_diff < 0 else "nearestbehind"
                # Estimate candles in between based on timeframe duration
                timeframe_minutes = {
                    "M5": 5, "M15": 15, "M30": 30, "H1": 60, "H4": 240
                }.get(timeframe, 5)
                candles_inbetween = int(abs_time_diff / (timeframe_minutes * 60))
        
        if not matched_candle:
            log_and_print(f"No matching or nearby candle found for {market} {timeframe}", "ERROR")
            return False
        
        # Prepare matched candles data with updated keys
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
            return True
        except Exception as e:
            log_and_print(f"Error saving matchedcandles.json for {market} {timeframe}: {e}", "ERROR")
            return False
    
    except Exception as e:
        log_and_print(f"Error matching most recent candle for {market} {timeframe}: {e}", "ERROR")
        return False

def save_new_mostrecent_completed_candle(market: str, timeframe: str, json_dir: str) -> bool:
    """Fetch and save the most recent completed candle for a market and timeframe."""
    log_and_print(f"Fetching most recent completed candle for market={market}, timeframe={timeframe}", "INFO")

    # Ensure no existing MT5 connections interfere
    mt5.shutdown()

    # Initialize MT5 terminal with explicit path and timeout
    for attempt in range(MAX_RETRIES):
        if mt5.initialize(path=TERMINAL_PATH, timeout=60000):
            log_and_print(f"Successfully initialized MT5 terminal for most recent candle {market} {timeframe}", "SUCCESS")
            break
        error_code, error_message = mt5.last_error()
        log_and_print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to initialize MT5 for most recent candle {market} {timeframe}. Error: {error_code}, {error_message}", "ERROR")
        time.sleep(RETRY_DELAY)
    else:
        log_and_print(f"Failed to initialize MT5 terminal for most recent candle {market} {timeframe} after {MAX_RETRIES} attempts", "ERROR")
        return False

    # Wait for terminal to be fully ready
    for _ in range(5):
        if mt5.terminal_info() is not None:
            log_and_print(f"MT5 terminal fully initialized for most recent candle {market} {timeframe}", "DEBUG")
            break
        log_and_print(f"Waiting for MT5 terminal to fully initialize for most recent candle {market} {timeframe}...", "INFO")
        time.sleep(2)
    else:
        log_and_print(f"MT5 terminal not ready for most recent candle {market} {timeframe}", "ERROR")
        mt5.shutdown()
        return False

    # Attempt login with retries
    for attempt in range(MAX_RETRIES):
        if mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=60000):
            log_and_print(f"Successfully logged in to MT5 for most recent candle {market} {timeframe}", "SUCCESS")
            break
        error_code, error_message = mt5.last_error()
        log_and_print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to log in to MT5 for most recent candle {market} {timeframe}. Error code: {error_code}, Message: {error_message}", "ERROR")
        time.sleep(RETRY_DELAY)
    else:
        log_and_print(f"Failed to log in to MT5 for most recent candle {market} {timeframe} after {MAX_RETRIES} attempts", "ERROR")
        mt5.shutdown()
        return False

    # Select market symbol
    if not mt5.symbol_select(market, True):
        log_and_print(f"Failed to select market for most recent candle: {market}, error: {mt5.last_error()}", "ERROR")
        mt5.shutdown()
        return False

    # Get timeframe
    mt5_timeframe = TIMEFRAME_MAPPING.get(timeframe)
    if not mt5_timeframe:
        log_and_print(f"Invalid timeframe {timeframe} for most recent candle {market}", "ERROR")
        mt5.shutdown()
        return False

    # Fetch the most recent completed candle (position 1)
    new_mostrecent_candle = mt5.copy_rates_from_pos(market, mt5_timeframe, 1, 1)
    if new_mostrecent_candle is None or len(new_mostrecent_candle) == 0:
        log_and_print(f"Failed to fetch most recent completed candle for {market} {timeframe}, error: {mt5.last_error()}", "ERROR")
        mt5.shutdown()
        return False

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

    # Save to JSON
    json_file_path = os.path.join(json_dir, "newmostrecent_completedcandle.json")
    if os.path.exists(json_file_path):
        os.remove(json_file_path)
        log_and_print(f"Existing {json_file_path} deleted", "INFO")

    try:
        with open(json_file_path, "w") as json_file:
            json.dump(new_mostrecent_candle_data, json_file, indent=4)
        log_and_print(f"Most recent completed candle saved to {json_file_path}", "SUCCESS")
        mt5.shutdown()
        return True
    except Exception as e:
        log_and_print(f"Error saving most recent completed candle for {market} {timeframe}: {e}", "ERROR")
        mt5.shutdown()
        return False

def calculate_candles_inbetween(market: str, timeframe: str, json_dir: str) -> bool:
    """Calculate the number of candles between newmostrecent_completedcandle.json and matchedcandles.json 'with candledata'."""
    log_and_print(f"Calculating candles in between for market={market}, timeframe={timeframe}", "INFO")
    
    # Define file paths
    formatted_market_name = market.replace(" ", "_")
    newmostrecent_json_path = os.path.join(json_dir, "newmostrecent_completedcandle.json")
    matched_candles_json_path = os.path.join(json_dir, "matchedcandles.json")
    output_json_path = os.path.join(json_dir, "candlesamountinbetween.json")
    
    # Check if both JSON files exist
    if not os.path.exists(newmostrecent_json_path):
        log_and_print(f"newmostrecent_completedcandle.json not found at {newmostrecent_json_path} for {market} {timeframe}", "ERROR")
        return False
    if not os.path.exists(matched_candles_json_path):
        log_and_print(f"matchedcandles.json not found at {matched_candles_json_path} for {market} {timeframe}", "ERROR")
        return False
    
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
            log_and_print(f"No timestamp found in newmostrecent_completedcandle.json for {market} {timeframe}", "ERROR")
            return False
        
        # Extract timestamp from matched candles "with candledata"
        matched_candle = matched_candles_data.get('with candledata', {})
        matched_timestamp_str = matched_candle.get('timestamp')
        if not matched_timestamp_str:
            log_and_print(f"No timestamp found in matchedcandles.json 'with candledata' for {market} {timeframe}", "ERROR")
            return False
        
        # Parse timestamps
        try:
            newmostrecent_timestamp = datetime.strptime(newmostrecent_timestamp_str, "%Y-%m-%d %H:%M:%S")
            newmostrecent_timestamp = newmostrecent_timestamp.replace(tzinfo=timezone.utc)
            
            matched_timestamp = datetime.strptime(matched_timestamp_str, "%Y-%m-%d %H:%M:%S")
            matched_timestamp = matched_timestamp.replace(tzinfo=timezone.utc)
        except ValueError as e:
            log_and_print(f"Invalid timestamp format in JSON files for {market} {timeframe}: {e}", "ERROR")
            return False
        
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
        
        # Add 1 to include the new most recent candle itself
        plusmostrecent = candles_inbetween + 1
        
        # Prepare output data with new structure
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
            return True
        except Exception as e:
            log_and_print(f"Error saving candlesamountinbetween.json for {market} {timeframe}: {e}", "ERROR")
            return False
    
    except Exception as e:
        log_and_print(f"Error calculating candles in between for {market} {timeframe}: {e}", "ERROR")
        return False

def candleafterbreakoutparent_to_currentprice(market: str, timeframe: str, json_dir: str) -> bool:
    """Fetch candles from the candle after Breakout_parent to the current price candle and save to JSON."""
    log_and_print(f"Fetching candles from after Breakout_parent to current price for market={market}, timeframe={timeframe}", "INFO")
    
    # Define file paths
    pricecandle_json_path = os.path.join(json_dir, "pricecandle.json")
    output_json_path = os.path.join(json_dir, "candlesafterbreakoutparent.json")
    
    # Check if pricecandle.json exists
    if not os.path.exists(pricecandle_json_path):
        log_and_print(f"pricecandle.json not found at {pricecandle_json_path} for {market} {timeframe}", "ERROR")
        return False
    
    try:
        # Load pricecandle.json to get Breakout_parent and order details
        with open(pricecandle_json_path, 'r') as f:
            pricecandle_data = json.load(f)
        
        # Initialize MT5
        mt5.shutdown()  # Ensure no existing connections
        for attempt in range(MAX_RETRIES):
            if mt5.initialize(path=TERMINAL_PATH, timeout=60000):
                log_and_print(f"Successfully initialized MT5 terminal for candles after Breakout_parent {market} {timeframe}", "SUCCESS")
                break
            error_code, error_message = mt5.last_error()
            log_and_print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to initialize MT5 for candles after Breakout_parent {market} {timeframe}. Error: {error_code}, {error_message}", "ERROR")
            time.sleep(RETRY_DELAY)
        else:
            log_and_print(f"Failed to initialize MT5 terminal for candles after Breakout_parent {market} {timeframe} after {MAX_RETRIES} attempts", "ERROR")
            return False

        # Wait for terminal to be fully ready
        for _ in range(5):
            if mt5.terminal_info() is not None:
                log_and_print(f"MT5 terminal fully initialized for candles after Breakout_parent {market} {timeframe}", "DEBUG")
                break
            log_and_print(f"Waiting for MT5 terminal to fully initialize for candles after Breakout_parent {market} {timeframe}...", "INFO")
            time.sleep(2)
        else:
            log_and_print(f"MT5 terminal not ready for candles after Breakout_parent {market} {timeframe}", "ERROR")
            mt5.shutdown()
            return False

        # Attempt login
        for attempt in range(MAX_RETRIES):
            if mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=60000):
                log_and_print(f"Successfully logged in to MT5 for candles after Breakout_parent {market} {timeframe}", "SUCCESS")
                break
            error_code, error_message = mt5.last_error()
            log_and_print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to log in to MT5 for candles after Breakout_parent {market} {timeframe}. Error: {error_code}, {error_message}", "ERROR")
            time.sleep(RETRY_DELAY)
        else:
            log_and_print(f"Failed to log in to MT5 for candles after Breakout_parent {market} {timeframe} after {MAX_RETRIES} attempts", "ERROR")
            mt5.shutdown()
            return False

        # Select market symbol
        if not mt5.symbol_select(market, True):
            log_and_print(f"Failed to select market for candles after Breakout_parent: {market}, error: {mt5.last_error()}", "ERROR")
            mt5.shutdown()
            return False

        # Get timeframe
        mt5_timeframe = TIMEFRAME_MAPPING.get(timeframe)
        if not mt5_timeframe:
            log_and_print(f"Invalid timeframe {timeframe} for candles after Breakout_parent {market}", "ERROR")
            mt5.shutdown()
            return False

        # Initialize output data
        candles_data = []
        
        # Process each trendline in pricecandle.json
        for trendline in pricecandle_data:
            breakout_parent = trendline.get("Breakout_parent", {})
            breakout_parent_pos = breakout_parent.get("position_number")
            breakout_candle = breakout_parent.get("candle_rightafter_Breakoutparent", {})
            order_type = trendline.get("receiver", {}).get("order_type", "").lower()
            order_holder = trendline.get("order_holder", {})
            
            if breakout_parent_pos is None or breakout_candle.get("position_number") is None:
                log_and_print(f"No valid Breakout_parent or candle_rightafter_Breakoutparent for trendline in {market} {timeframe}", "WARNING")
                continue

            # Get the position number of the candle right after Breakout_parent
            start_pos = breakout_candle.get("position_number")
            if start_pos is None:
                log_and_print(f"Invalid position number for candle_rightafter_Breakoutparent in {market} {timeframe}", "WARNING")
                continue

            # Validate order_type
            if order_type not in ["long", "short"]:
                log_and_print(f"Invalid order_type {order_type} for trendline in {market} {timeframe}", "WARNING")
                order_type = None
                order_holder_entry = None
            else:
                # Determine Order_holder_entry based on order_type
                order_holder_entry = float(order_holder.get("Low", 0)) if order_type == "short" else float(order_holder.get("High", 0)) if order_type == "long" else None
                if order_holder_entry == 0 or order_holder.get("position_number") is None:
                    log_and_print(f"No valid order holder data for trendline in {market} {timeframe}", "WARNING")
                    order_holder_entry = None

            # Fetch candles from start_pos to position 0 (current incomplete candle)
            try:
                # Fetch candles from start_pos to the latest completed candle (position 1)
                num_candles = start_pos - 1
                if num_candles > 0:
                    candles = mt5.copy_rates_from_pos(market, mt5_timeframe, 1, num_candles)
                    if candles is None or len(candles) == 0:
                        log_and_print(f"Failed to fetch candles from position {start_pos} to 1 for {market} {timeframe}, error: {mt5.last_error()}", "ERROR")
                        continue
                else:
                    candles = []

                # Prepare candle data
                trendline_candles = []
                for i, candle in enumerate(candles):
                    position = start_pos - i
                    trendline_candles.append({
                        "position_number": position,
                        "Time": str(pd.to_datetime(candle['time'], unit='s')),
                        "Open": float(candle['open']),
                        "High": float(candle['high']),
                        "Low": float(candle['low']),
                        "Close": float(candle['close'])
                    })

                # Fetch current (incomplete) candle (position 0)
                current_candle = mt5.copy_rates_from_pos(market, mt5_timeframe, 0, 1)
                if current_candle is None or len(current_candle) == 0:
                    log_and_print(f"Failed to fetch current candle for {market} {timeframe}, error: {mt5.last_error()}", "WARNING")
                    current_candle_data = {
                        "position_number": 0,
                        "Time": None,
                        "Open": None
                    }
                else:
                    current_candle_data = {
                        "position_number": 0,
                        "Time": str(pd.to_datetime(current_candle[0]['time'], unit='s')),
                        "Open": float(current_candle[0]['open'])
                    }

                trendline_candles.append(current_candle_data)
                
                # Add to output data with updated trendline structure
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

            except Exception as e:
                log_and_print(f"Error fetching candles for trendline in {market} {timeframe}: {e}", "ERROR")
                continue

        # Save to candlesafterbreakoutparent.json
        if os.path.exists(output_json_path):
            os.remove(output_json_path)
            log_and_print(f"Existing {output_json_path} deleted", "INFO")

        if not candles_data:
            log_and_print(f"No candles data to save for {market} {timeframe}. Saving empty candlesafterbreakoutparent.json", "WARNING")
            try:
                with open(output_json_path, 'w') as f:
                    json.dump(candles_data, f, indent=4)
                log_and_print(f"Empty candlesafterbreakoutparent.json saved to {output_json_path} for {market} {timeframe}", "INFO")
                mt5.shutdown()
                return True
            except Exception as e:
                log_and_print(f"Error saving empty candlesafterbreakoutparent.json for {market} {timeframe}: {e}", "ERROR")
                mt5.shutdown()
                return False

        try:
            with open(output_json_path, 'w') as f:
                json.dump(candles_data, f, indent=4)
            log_and_print(f"Candles after Breakout_parent saved to {output_json_path} for {market} {timeframe}", "SUCCESS")
            mt5.shutdown()
            return True
        except Exception as e:
            log_and_print(f"Error saving candlesafterbreakoutparent.json for {market} {timeframe}: {e}", "ERROR")
            mt5.shutdown()
            return False

    except Exception as e:
        log_and_print(f"Error processing candles after Breakout_parent for {market} {timeframe}: {e}", "ERROR")
        mt5.shutdown()
        return False

def executioncandle_after_breakoutparent(market: str, timeframe: str, json_dir: str) -> bool:
    """Search candlesafterbreakoutparent.json for a candle matching the order_holder_entry price and update pricecandle.json."""
    log_and_print(f"Searching for executioner candle for market={market}, timeframe={timeframe}", "INFO")
    
    # Define file paths
    pricecandle_json_path = os.path.join(json_dir, "pricecandle.json")
    candlesafterbreakoutparent_json_path = os.path.join(json_dir, "candlesafterbreakoutparent.json")
    
    # Check if required JSON files exist
    if not os.path.exists(pricecandle_json_path):
        log_and_print(f"pricecandle.json not found at {pricecandle_json_path} for {market} {timeframe}", "ERROR")
        return False
    if not os.path.exists(candlesafterbreakoutparent_json_path):
        log_and_print(f"candlesafterbreakoutparent.json not found at {candlesafterbreakoutparent_json_path} for {market} {timeframe}", "ERROR")
        return False
    
    try:
        # Load pricecandle.json
        with open(pricecandle_json_path, 'r') as f:
            pricecandle_data = json.load(f)
        
        # Load candlesafterbreakoutparent.json
        with open(candlesafterbreakoutparent_json_path, 'r') as f:
            candlesafterbreakoutparent_data = json.load(f)
        
        # Prepare updated pricecandle data
        updated_pricecandle_data = []
        
        # Process each trendline in pricecandle.json
        for pricecandle_trendline in pricecandle_data:
            # Find the corresponding trendline in candlesafterbreakoutparent.json
            trendline_type = pricecandle_trendline.get("type")
            breakout_parent_pos = pricecandle_trendline.get("Breakout_parent", {}).get("position_number")
            matching_cabp_trendline = None
            
            for cabp_trendline in candlesafterbreakoutparent_data:
                cabp_trendline_info = cabp_trendline.get("trendline", {})
                if (cabp_trendline_info.get("type") == trendline_type and 
                    cabp_trendline_info.get("Breakout_parent_position") == breakout_parent_pos):
                    matching_cabp_trendline = cabp_trendline
                    break
            
            if not matching_cabp_trendline:
                log_and_print(
                    f"No matching trendline found in candlesafterbreakoutparent.json for {trendline_type} "
                    f"with Breakout_parent_position {breakout_parent_pos} in {market} {timeframe}", "WARNING"
                )
                pricecandle_trendline["executioner candle"] = {
                    "status": "No executioner candle"
                }
                pricecandle_trendline.setdefault("contract status summary", {})["contract status"] = "pending"
                updated_pricecandle_data.append(pricecandle_trendline)
                continue
            
            # Get order_type and Order_holder_entry from candlesafterbreakoutparent.json
            order_type = matching_cabp_trendline.get("trendline", {}).get("order_type", "").lower()
            order_holder_entry = matching_cabp_trendline.get("trendline", {}).get("Order_holder_entry")
            
            # Validate order_type and order_holder_entry
            if order_type not in ["long", "short"] or order_holder_entry is None:
                log_and_print(
                    f"Invalid order_type {order_type} or missing Order_holder_entry for trendline {trendline_type} "
                    f"in {market} {timeframe}", "WARNING"
                )
                pricecandle_trendline["executioner candle"] = {
                    "status": "No executioner candle"
                }
                pricecandle_trendline.setdefault("contract status summary", {})["contract status"] = "pending"
                updated_pricecandle_data.append(pricecandle_trendline)
                continue
            
            # Validate order_holder_entry against pricecandle.json
            order_holder = pricecandle_trendline.get("order_holder", {})
            expected_entry = float(order_holder.get("High" if order_type == "long" else "Low", 0))
            price_tolerance = 1e-3  # Match tolerance with executedordersupdater
            if expected_entry == 0 or abs(float(order_holder_entry) - expected_entry) > price_tolerance:
                log_and_print(
                    f"Mismatch in Order_holder_entry for trendline {trendline_type} in {market} {timeframe}. "
                    f"Expected: {expected_entry}, Got: {order_holder_entry}, Diff: {abs(float(order_holder_entry) - expected_entry)}",
                    "INFO"
                )
                pricecandle_trendline["executioner candle"] = {
                    "status": "No executioner candle due to Order_holder_entry mismatch"
                }
                pricecandle_trendline.setdefault("contract status summary", {})["contract status"] = "pending"
                updated_pricecandle_data.append(pricecandle_trendline)
                continue
            
            # Search for a matching candle
            matching_candle = None
            for candle in matching_cabp_trendline.get("candles", []):
                high_price = candle.get("High")
                low_price = candle.get("Low")
                close_price = candle.get("Close")
                
                # Check for a match based on order_type with close confirmation
                if order_type == "long" and low_price is not None and close_price is not None and order_holder_entry is not None:
                    if low_price <= order_holder_entry + price_tolerance and close_price <= order_holder_entry:
                        log_and_print(
                            f"Buy_limit (long) order execution confirmed: low_price={low_price}, close_price={close_price}, "
                            f"order_holder_entry={order_holder_entry} for position {candle.get('position_number')} "
                            f"in {market} {timeframe}", "DEBUG"
                        )
                        matching_candle = {
                            "status": "Candle found at order holder entry level",
                            "position_number": candle.get("position_number"),
                            "Time": candle.get("Time"),
                            "Open": candle.get("Open"),
                            "High": high_price,
                            "Low": low_price,
                            "Close": close_price
                        }
                        break
                elif order_type == "short" and high_price is not None and close_price is not None and order_holder_entry is not None:
                    if high_price >= order_holder_entry - price_tolerance and close_price >= order_holder_entry:
                        log_and_print(
                            f"Sell_limit (short) order execution confirmed: high_price={high_price}, close_price={close_price}, "
                            f"order_holder_entry={order_holder_entry} for position {candle.get('position_number')} "
                            f"in {market} {timeframe}", "DEBUG"
                        )
                        matching_candle = {
                            "status": "Candle found at order holder entry level",
                            "position_number": candle.get("position_number"),
                            "Time": candle.get("Time"),
                            "Open": candle.get("Open"),
                            "High": high_price,
                            "Low": low_price,
                            "Close": close_price
                        }
                        break
            
            # Handle the current price candle (position 0) separately
            if not matching_candle:
                current_candle = next((c for c in matching_cabp_trendline.get("candles", []) if c.get("position_number") == 0), None)
                if current_candle:
                    high_price = current_candle.get("High")
                    low_price = current_candle.get("Low")
                    close_price = current_candle.get("Close")  # May be None for current candle
                    if order_type == "long" and low_price is not None and order_holder_entry is not None:
                        if low_price <= order_holder_entry + price_tolerance and (close_price is None or close_price <= order_holder_entry):
                            log_and_print(
                                f"Buy_limit (long) order execution confirmed (current candle): low_price={low_price}, close_price={close_price}, "
                                f"order_holder_entry={order_holder_entry} for position 0 in {market} {timeframe}", "DEBUG"
                            )
                            matching_candle = {
                                "status": "Candle found at order holder entry level",
                                "position_number": 0,
                                "Time": current_candle.get("Time"),
                                "Open": current_candle.get("Open"),
                                "High": high_price,
                                "Low": low_price,
                                "Close": close_price if close_price is not None else None
                            }
                    elif order_type == "short" and high_price is not None and order_holder_entry is not None:
                        if high_price >= order_holder_entry - price_tolerance and (close_price is None or close_price >= order_holder_entry):
                            log_and_print(
                                f"Sell_limit (short) order execution confirmed (current candle): high_price={high_price}, close_price={close_price}, "
                                f"order_holder_entry={order_holder_entry} for position 0 in {market} {timeframe}", "DEBUG"
                            )
                            matching_candle = {
                                "status": "Candle found at order holder entry level",
                                "position_number": 0,
                                "Time": current_candle.get("Time"),
                                "Open": current_candle.get("Open"),
                                "High": high_price,
                                "Low": low_price,
                                "Close": close_price if close_price is not None else None
                            }
            
            # Update pricecandle_trendline with the result
            if matching_candle:
                pricecandle_trendline["executioner candle"] = matching_candle
                pricecandle_trendline.setdefault("contract status summary", {})["contract status"] = "executed"
                log_and_print(
                    f"Executioner candle found for trendline {trendline_type}: status={matching_candle['status']}, "
                    f"position={matching_candle['position_number']}, time={matching_candle['Time']} in {market} {timeframe}",
                    "INFO"
                )
            else:
                pricecandle_trendline["executioner candle"] = {
                    "status": "No executioner candle"
                }
                pricecandle_trendline.setdefault("contract status summary", {})["contract status"] = "pending"
                log_and_print(
                    f"No executioner candle found for trendline {trendline_type} in {market} {timeframe}. "
                    f"Order_type: {order_type}, Order_holder_entry: {order_holder_entry}", "INFO"
                )
            
            updated_pricecandle_data.append(pricecandle_trendline)
        
        # Save updated pricecandle.json
        if os.path.exists(pricecandle_json_path):
            os.remove(pricecandle_json_path)
            log_and_print(f"Existing {pricecandle_json_path} deleted", "INFO")
        
        try:
            with open(pricecandle_json_path, 'w') as f:
                json.dump(updated_pricecandle_data, f, indent=4)
            #log_and_print(f"updated pricecandle.json with executioner candle for {market} {timeframe}", "SUCCESS")
            return True
        except Exception as e:
            log_and_print(f"Error saving updated pricecandle.json for {market} {timeframe}: {e}", "ERROR")
            return False
    
    except Exception as e:
        log_and_print(f"Error processing executioner candle for {market} {timeframe}: {e}", "ERROR")
        return False
    
def fetchlotsizeandriskallowed(json_dir: str = BASE_OUTPUT_FOLDER) -> bool:
    """Fetch all lot size and allowed risk data from ciphercontracts_lotsizeandrisk table and save to lotsizeandrisk.json."""
    log_and_print("Fetching all lot size and allowed risk data", "INFO")
    
    # Initialize error log list
    error_log = []
    
    # Define error log file path
    error_json_path = os.path.join(BASE_PROCESSING_FOLDER, "fetchlotsizeandriskerror.json")
    
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
            output_json_path = os.path.join(json_dir, "lotsizeandrisk.json")
            
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
    # Fetch lot size and allowed risk data once
        if not fetchlotsizeandriskallowed():
            log_and_print("Failed to fetch lot size and allowed risk data. Exiting.", "ERROR")
            return

def getorderholderpriceswithlotsizeandrisk(market: str, timeframe: str, json_dir: str) -> bool:
    """Fetch order holder prices, calculate exit and profit prices using lot size and allowed risk from centralized lotsizeandrisk.json, and save to calculatedprices.json."""
    log_and_print(f"Calculating order holder prices with lot size and risk for market={market}, timeframe={timeframe}", "INFO")
    
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
    
    # Check if pricecandle.json exists
    if not os.path.exists(pricecandle_json_path):
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "market": market,
            "timeframe": timeframe,
            "error": f"pricecandle.json not found at {pricecandle_json_path}"
        })
        save_errors()
        log_and_print(f"pricecandle.json not found at {pricecandle_json_path} for {market} {timeframe}", "ERROR")
        return False
    
    # Check if lotsizeandrisk.json exists
    if not os.path.exists(lotsizeandrisk_json_path):
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "market": market,
            "timeframe": timeframe,
            "error": f"lotsizeandrisk.json not found at {lotsizeandrisk_json_path}"
        })
        save_errors()
        log_and_print(f"lotsizeandrisk.json not found at {lotsizeandrisk_json_path} for {market} {timeframe}", "ERROR")
        return False
    
    try:
        # Initialize MT5 to fetch market-specific data
        mt5.shutdown()
        for attempt in range(MAX_RETRIES):
            if mt5.initialize(path=TERMINAL_PATH, timeout=60000):
                log_and_print(f"Successfully initialized MT5 for {market} {timeframe}", "SUCCESS")
                break
            error_code, error_message = mt5.last_error()
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "market": market,
                "timeframe": timeframe,
                "error": f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to initialize MT5. Error: {error_code}, {error_message}"
            })
            save_errors()
            log_and_print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to initialize MT5 for {market} {timeframe}. Error: {error_code}, {error_message}", "ERROR")
            time.sleep(RETRY_DELAY)
        else:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "market": market,
                "timeframe": timeframe,
                "error": f"Failed to initialize MT5 for {market} {timeframe} after {MAX_RETRIES} attempts"
            })
            save_errors()
            log_and_print(f"Failed to initialize MT5 for {market} {timeframe} after {MAX_RETRIES} attempts", "ERROR")
            return False

        # Login to MT5
        for attempt in range(MAX_RETRIES):
            if mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=60000):
                log_and_print(f"Successfully logged in to MT5 for {market} {timeframe}", "SUCCESS")
                break
            error_code, error_message = mt5.last_error()
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "market": market,
                "timeframe": timeframe,
                "error": f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to log in to MT5. Error: {error_code}, {error_message}"
            })
            save_errors()
            log_and_print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to log in to MT5 for {market} {timeframe}. Error: {error_code}, {error_message}", "ERROR")
            time.sleep(RETRY_DELAY)
        else:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "market": market,
                "timeframe": timeframe,
                "error": f"Failed to log in to MT5 for {market} {timeframe} after {MAX_RETRIES} attempts"
            })
            save_errors()
            log_and_print(f"Failed to log in to MT5 for {market} {timeframe} after {MAX_RETRIES} attempts", "ERROR")
            mt5.shutdown()
            return False

        # Select market symbol
        if not mt5.symbol_select(market, True):
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "market": market,
                "timeframe": timeframe,
                "error": f"Failed to select market: {market}, error: {mt5.last_error()}"
            })
            save_errors()
            log_and_print(f"Failed to select market: {market}, error: {mt5.last_error()}", "ERROR")
            mt5.shutdown()
            return False

        # Fetch symbol info for pip size and contract size
        symbol_info = mt5.symbol_info(market)
        if not symbol_info:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "market": market,
                "timeframe": timeframe,
                "error": f"Failed to fetch symbol info for {market}"
            })
            save_errors()
            log_and_print(f"Failed to fetch symbol info for {market}", "ERROR")
            mt5.shutdown()
            return False

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
        #log_and_print(f"Available pairs and timeframes in lotsizeandrisk.json: {available_pairs_timeframes}", "DEBUG")
        
        # Normalize input timeframe
        normalized_timeframe = normalize_timeframe(timeframe)
        if normalized_timeframe is None:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "market": market,
                "timeframe": timeframe,
                "error": f"Invalid timeframe: {timeframe}"
            })
            save_errors()
            log_and_print(f"Invalid timeframe: {timeframe}", "ERROR")
            mt5.shutdown()
            return False
        
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
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "market": market,
                "timeframe": timeframe,
                "error": f"No matching lot size and risk data found for pair={market}, timeframe={db_timeframe} (normalized from {timeframe})"
            })
            save_errors()
            log_and_print(f"No matching lot size and risk data found for {market} {timeframe} (normalized to pair={market.lower()}, timeframe={db_timeframe})", "WARNING")
            mt5.shutdown()
            return False
        
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
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "market": market,
                    "timeframe": timeframe,
                    "trendline_type": trendline_type,
                    "error": f"No valid order holder: label={order_holder.get('label', 'none')}, position_number={order_holder_position}"
                })
                save_errors()
                log_and_print(f"No valid order holder for trendline {trendline_type} in {market} {timeframe}", "INFO")
                continue
            
            # Validate order_type
            if order_type not in ["long", "short"]:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "market": market,
                    "timeframe": timeframe,
                    "trendline_type": trendline_type,
                    "error": f"Invalid order_type {order_type}"
                })
                save_errors()
                log_and_print(f"Invalid order_type {order_type} for trendline {trendline_type} in {market} {timeframe}", "WARNING")
                continue
            
            # Get entry price based on order type
            entry_price = float(order_holder.get("Low", 0)) if order_type == "short" else float(order_holder.get("High", 0)) if order_type == "long" else None
            if entry_price == 0:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "market": market,
                    "timeframe": timeframe,
                    "trendline_type": trendline_type,
                    "error": f"No valid entry price for order holder: Low={order_holder.get('Low', 'N/A')}, High={order_holder.get('High', 'N/A')}"
                })
                save_errors()
                log_and_print(f"No valid entry price for order holder in trendline {trendline_type} in {market} {timeframe}", "WARNING")
                continue
            
            # Extract lot size and allowed risk
            lot_size = float(matching_lot_size.get("lot_size", 0))
            allowed_risk = float(matching_lot_size.get("allowed_risk", 0))
            if lot_size <= 0 or allowed_risk <= 0:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "market": market,
                    "timeframe": timeframe,
                    "trendline_type": trendline_type,
                    "error": f"Invalid lot_size {lot_size} or allowed_risk {allowed_risk}"
                })
                save_errors()
                log_and_print(f"Invalid lot_size {lot_size} or allowed_risk {allowed_risk} for trendline {trendline_type} in {market} {timeframe}", "WARNING")
                continue
            
            # Calculate pip value
            pip_value = lot_size * contract_size * pip_size
            if market.endswith("JPY"):
                current_price = mt5.symbol_info_tick(market).bid
                if current_price > 0:
                    pip_value = pip_value / current_price
                else:
                    error_log.append({
                        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                        "market": market,
                        "timeframe": timeframe,
                        "trendline_type": trendline_type,
                        "error": f"Failed to fetch current price for {market} to adjust pip value"
                    })
                    save_errors()
                    log_and_print(f"Failed to fetch current price for {market} to adjust pip value", "WARNING")
                    pip_value = lot_size * 10  # Fallback
            
            # Calculate risk in pips
            risk_in_pips = allowed_risk / pip_value if pip_value != 0 else 0
            if risk_in_pips <= 0:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "market": market,
                    "timeframe": timeframe,
                    "trendline_type": trendline_type,
                    "error": f"Invalid risk_in_pips {risk_in_pips}. pip_value={pip_value}, allowed_risk={allowed_risk}"
                })
                save_errors()
                log_and_print(f"Invalid risk_in_pips {risk_in_pips} for trendline {trendline_type} in {market} {timeframe}", "WARNING")
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
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "market": market,
                    "timeframe": timeframe,
                    "trendline_type": trendline_type,
                    "error": f"Invalid prices: entry={entry_price}, exit={exit_price}, 1:0.5={price_1_0_5}, 1:1={price_1_1}, 1:2={price_1_2}, profit={profit_price}"
                })
                save_errors()
                log_and_print(f"Invalid prices: exit={exit_price}, 1:0.5={price_1_0_5}, 1:1={price_1_1}, 1:2={price_1_2}, profit={profit_price} for trendline {trendline_type} in {market} {timeframe}", "ERROR")
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
            log_and_print(
                f"Calculated prices for trendline {trendline_type}: entry={entry_price}, exit={exit_price}, "
                f"1:0.5={price_1_0_5}, 1:1={price_1_1}, 1:2={price_1_2}, profit={profit_price}, "
                f"lot_size={lot_size}, order_type={calculated_entry['order_type']}, "
                f"risk_in_pips={risk_in_pips}, pip_value={pip_value} in {market} {timeframe}",
                "DEBUG"
            )
        
        # Log the number of calculated prices
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
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
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "market": market,
                    "timeframe": timeframe,
                    "error": f"Error deleting existing {output_json_path}: {str(e)}"
                })
                save_errors()
                log_and_print(f"Error deleting existing {output_json_path}: {str(e)}", "ERROR")
                mt5.shutdown()
                return False
        
        if not calculated_prices:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
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
                mt5.shutdown()
                return True
            except Exception as e:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "market": market,
                    "timeframe": timeframe,
                    "error": f"Error saving empty calculatedprices.json: {str(e)}"
                })
                save_errors()
                log_and_print(f"Error saving empty calculatedprices.json for {market} {timeframe}: {str(e)}", "ERROR")
                mt5.shutdown()
                return False
        
        try:
            with open(output_json_path, 'w') as f:
                json.dump(calculated_prices, f, indent=4)
            log_and_print(
                f"Saved {len(calculated_prices)} calculated price entries to {output_json_path} for {market} {timeframe}",
                "SUCCESS"
            )
            mt5.shutdown()
            return True
        except Exception as e:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "market": market,
                "timeframe": timeframe,
                "error": f"Error saving calculatedprices.json: {str(e)}"
            })
            save_errors()
            log_and_print(f"Error saving calculatedprices.json for {market} {timeframe}: {str(e)}", "ERROR")
            mt5.shutdown()
            return False
    
    except Exception as e:
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "market": market,
            "timeframe": timeframe,
            "error": f"Unexpected error processing order holder prices: {str(e)}"
        })
        save_errors()
        log_and_print(f"Error processing order holder prices for {market} {timeframe}: {str(e)}", "ERROR")
        mt5.shutdown()
        return False

def PendingOrderUpdater(market: str, timeframe: str, json_dir: str) -> bool:
    #log_and_print(f"updating pending orders for market={market}, timeframe={timeframe}", "INFO")
    
    # Define file paths
    pricecandle_json_path = os.path.join(json_dir, "pricecandle.json")
    calculatedprices_json_path = os.path.join(json_dir, "calculatedprices.json")
    candle_data_json_path = os.path.join(json_dir, "candle_data.json")
    
    # Check if required JSON files exist
    if not os.path.exists(pricecandle_json_path):
        log_and_print(f"pricecandle.json not found at {pricecandle_json_path} for {market} {timeframe}", "ERROR")
        return False
    if not os.path.exists(calculatedprices_json_path):
        log_and_print(f"calculatedprices.json not found at {calculatedprices_json_path} for {market} {timeframe}", "ERROR")
        return False
    if not os.path.exists(candle_data_json_path):
        log_and_print(f"candle_data.json not found at {candle_data_json_path} for {market} {timeframe}", "ERROR")
        return False
    
    try:
        # Load pricecandle.json
        with open(pricecandle_json_path, 'r') as f:
            pricecandle_data = json.load(f)
        
        # Load calculatedprices.json
        with open(calculatedprices_json_path, 'r') as f:
            calculatedprices_data = json.load(f)
        
        # Load candle_data.json
        with open(candle_data_json_path, 'r') as f:
            candle_data = json.load(f)
        
        # Prepare updated pricecandle data
        updated_pricecandle_data = []
        
        # Process each trendline in pricecandle
        for pricecandle_trendline in pricecandle_data:
            order_type = pricecandle_trendline.get("receiver", {}).get("order_type", "").lower()
            order_holder = pricecandle_trendline.get("order_holder", {})
            executioner_candle = pricecandle_trendline.get("executioner candle", {})
            
            # Find matching calculatedprices entry
            matching_calculated = None
            actual_price = order_holder.get("High" if order_type == "long" else "Low", 0)
            if order_type == "long":
                # For long (buy_limit), match order_holder High with entry_price
                for calc_entry in calculatedprices_data:
                    if (calc_entry.get("order_type") == "buy_limit" and 
                        abs(calc_entry.get("entry_price") - actual_price) < 1e-5):
                        matching_calculated = calc_entry
                        break
            elif order_type == "short":
                # For short (sell_limit), match order_holder Low with entry_price
                for calc_entry in calculatedprices_data:
                    if (calc_entry.get("order_type") == "sell_limit" and 
                        abs(calc_entry.get("entry_price") - actual_price) < 1e-5):
                        matching_calculated = calc_entry
                        break
            
            # Handle case with no valid executioner candle
            if executioner_candle.get("status") != "Candle found at order holder entry level":
                log_and_print(f"No valid executioner candle for trendline {pricecandle_trendline.get('type')} in {market} {timeframe}", "INFO")
                # Skip processing if no matching calculatedprices entry
                if not matching_calculated:
                    log_and_print(
                        f"Mismatch in Order_holder_entry for trendline {pricecandle_trendline.get('type')} in {market} {timeframe}. "
                        f"Expected: {calc_entry.get('entry_price', 'N/A') if calc_entry else 'N/A'}, Got: {actual_price}. Skipping.",
                        "INFO"
                    )
                    continue
                # Update pending order with order_type and entry_price from calculatedprices.json
                pricecandle_trendline["pending order"] = {
                    "status": f"{matching_calculated.get('order_type', 'unknown')} {matching_calculated.get('entry_price', 'N/A')}"
                }
                # Remove other fields if they exist
                for key in ["executioner candle", "stoploss", "1:0.5 candle", "1:1 candle", "1:2 candle", "profit candle", "contract status summary"]:
                    pricecandle_trendline.pop(key, None)
                updated_pricecandle_data.append(pricecandle_trendline)
                continue
            
            # If executioner candle exists, clear pending order and retain executioner candle
            pricecandle_trendline.pop("pending order", None)
            updated_pricecandle_data.append(pricecandle_trendline)
        
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
                    # Compare times to keep the oldest
                    existing_time = seen_entry_prices[entry_price]["Time"]
                    if order_time < existing_time:
                        # Replace with older entry
                        seen_entry_prices[entry_price] = {
                            "trendline": trendline,
                            "Time": order_time
                        }
                        log_and_print(
                            f"Duplicate pending order detected for trendline {trendline.get('type')} with entry_price {entry_price} in {market} {timeframe}. "
                            f"Keeping older entry at {order_time}.",
                            "INFO"
                        )
                    else:
                        log_and_print(
                            f"Duplicate pending order detected for trendline {trendline.get('type')} with entry_price {entry_price} in {market} {timeframe}. "
                            f"Discarding newer entry at {order_time}.",
                            "INFO"
                        )
                        continue
                else:
                    seen_entry_prices[entry_price] = {
                        "trendline": trendline,
                        "Time": order_time
                    }
            final_pricecandle_data.append(trendline)
        
        # Extract final trendlines from seen_entry_prices
        final_pricecandle_data = [entry["trendline"] for entry in seen_entry_prices.values()]
        
        # Save final pricecandle.json
        if os.path.exists(pricecandle_json_path):
            os.remove(pricecandle_json_path)
            log_and_print(f"Existing {pricecandle_json_path} deleted", "INFO")
        
        try:
            with open(pricecandle_json_path, 'w') as f:
                json.dump(final_pricecandle_data, f, indent=4)
            #log_and_print(f"updated pricecandle.json with pending order updates and duplicates removed for {market} {timeframe}", "SUCCESS")
            return True
        except Exception as e:
            log_and_print(f"Error saving final pricecandle.json for {market} {timeframe}: {e}", "ERROR")
            return False
    
    except Exception as e:
        log_and_print(f"Error processing pending order updates for {market} {timeframe}: {e}", "ERROR")
        return False
def collect_all_pending_orders(market: str, timeframe: str, json_dir: str) -> bool:
    """Collect all pending orders from pricecandle.json for a specific market and timeframe,
    save to contractpendingorders.json, and aggregate across all markets and timeframes to allpendingorders.json."""
    log_and_print(f"Collecting pending orders for market={market}, timeframe={timeframe}", "INFO")
    
    # Define file paths
    pricecandle_json_path = os.path.join(json_dir, "pricecandle.json")
    calculatedprices_json_path = os.path.join(json_dir, "calculatedprices.json")
    pending_orders_json_path = os.path.join(json_dir, "contractpendingorders.json")
    collective_pending_path = os.path.join(BASE_OUTPUT_FOLDER, "allpendingorders.json")
    
    # Check if required JSON files exist
    if not os.path.exists(pricecandle_json_path):
        log_and_print(f"pricecandle.json not found at {pricecandle_json_path} for {market} {timeframe}", "ERROR")
        return False
    if not os.path.exists(calculatedprices_json_path):
        log_and_print(f"calculatedprices.json not found at {calculatedprices_json_path} for {market} {timeframe}", "ERROR")
        return False
    
    try:
        # Load pricecandle.json
        with open(pricecandle_json_path, 'r') as f:
            pricecandle_data = json.load(f)
        
        # Load calculatedprices.json
        with open(calculatedprices_json_path, 'r') as f:
            calculatedprices_data = json.load(f)
        
        # Extract pending orders from pricecandle.json
        contract_pending_orders = []
        seen_entry_prices = {}  # Track unique entry prices to keep oldest order
        skipped_reasons = {}  # Track why trendlines are skipped
        
        for trendline in pricecandle_data:
            pending_order = trendline.get("pending order", {})
            trendline_type = trendline.get("type")
            order_holder = trendline.get("order_holder", {})
            order_holder_position = order_holder.get("position_number")
            order_holder_timestamp = order_holder.get("Time", "N/A")
            order_type = trendline.get("receiver", {}).get("order_type", "").lower()
            contract_status = trendline.get("contract status summary", {}).get("contract status", "")
            
            # Skip if the order is already executed
            if contract_status in ["profit reached exit contract", "Exit contract at stoploss"]:
                skipped_reasons[trendline_type] = f"Skipped due to executed contract status: {contract_status}"
                log_and_print(
                    f"Skipping trendline {trendline_type} in {market} {timeframe} due to contract status: {contract_status}",
                    "INFO"
                )
                continue
            
            # Check if there is a valid pending order
            if not pending_order or "status" not in pending_order:
                skipped_reasons[trendline_type] = "No valid pending order or missing status"
                log_and_print(f"No valid pending order for trendline {trendline_type} in {market} {timeframe}", "INFO")
                continue
            
            # Extract order_type and entry_price from order_holder
            actual_price = order_holder.get("High" if order_type == "long" else "Low", 0)
            if actual_price == 0:
                skipped_reasons[trendline_type] = "Invalid order_holder price"
                log_and_print(f"Invalid order_holder price for trendline {trendline_type} in {market} {timeframe}", "WARNING")
                continue
            
            # Find matching calculatedprices entry
            matching_calculated = None
            price_tolerance = 1e-3  # Match tolerance with executedordersupdater
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
                continue
            
            # Validate order_type and entry_price consistency
            calc_order_type = matching_calculated.get("order_type").lower()
            calc_entry_price = matching_calculated.get("entry_price")
            if calc_order_type != ("buy_limit" if order_type == "long" else "sell_limit"):
                skipped_reasons[trendline_type] = f"Order type mismatch: pricecandle={order_type}, calculatedprices={calc_order_type}"
                log_and_print(
                    f"Order type mismatch for trendline {trendline_type} in {market} {timeframe}: "
                    f"pricecandle={order_type}, calculatedprices={calc_order_type}",
                    "WARNING"
                )
                continue
            if abs(calc_entry_price - actual_price) > price_tolerance:
                skipped_reasons[trendline_type] = f"Entry price mismatch: pricecandle={actual_price}, calculatedprices={calc_entry_price}"
                log_and_print(
                    f"Entry price mismatch for trendline {trendline_type} in {market} {timeframe}: "
                    f"pricecandle={actual_price}, calculatedprices={calc_entry_price}",
                    "WARNING"
                )
                continue
            
            # Deduplicate based on entry_price, keep oldest
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
                    continue
            else:
                seen_entry_prices[actual_price] = {
                    "trendline": trendline,
                    "matching_calculated": matching_calculated,
                    "order_holder_timestamp": order_holder_timestamp
                }
        
        # Process deduplicated orders
        for entry in seen_entry_prices.values():
            trendline = entry["trendline"]
            matching_calculated = entry["matching_calculated"]
            trendline_type = trendline.get("type")
            order_holder = trendline.get("order_holder", {})
            order_holder_position = order_holder.get("position_number")
            order_holder_timestamp = order_holder.get("Time", "N/A")
            order_type = trendline.get("receiver", {}).get("order_type", "").lower()
            
            # Create contract pending order entry
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
            
            # Validate prices
            if any(price <= 0 for price in [
                contract_entry["entry_price"],
                contract_entry["exit_price"],
                contract_entry["1:0.5_price"],
                contract_entry["1:1_price"],
                contract_entry["1:2_price"],
                contract_entry["profit_price"]
            ]):
                skipped_reasons[trendline_type] = f"Invalid price values: {contract_entry}"
                log_and_print(f"Invalid price values for trendline {trendline_type} in {market} {timeframe}: {contract_entry}", "WARNING")
                continue
            
            if contract_entry["lot_size"] <= 0:
                skipped_reasons[trendline_type] = f"Invalid lot_size {contract_entry['lot_size']}"
                log_and_print(f"Invalid lot_size {contract_entry['lot_size']} for trendline {trendline_type} in {market} {timeframe}", "WARNING")
                continue
            
            contract_pending_orders.append(contract_entry)
            log_and_print(
                f"Added pending order for trendline {trendline_type}: order_type={contract_entry['order_type']}, "
                f"entry_price={contract_entry['entry_price']}, exit_price={contract_entry['exit_price']}, "
                f"1:0.5_price={contract_entry['1:0.5_price']}, 1:1_price={contract_entry['1:1_price']}, "
                f"1:2_price={contract_entry['1:2_price']}, profit_price={contract_entry['profit_price']}, "
                f"lot_size={contract_entry['lot_size']}, order_holder_position={order_holder_position}, "
                f"order_holder_timestamp={order_holder_timestamp} in {market} {timeframe}",
                "DEBUG"
            )
        
        # Log skipped reasons
        if skipped_reasons:
            log_and_print(f"Skipped trendlines in {market} {timeframe}: {skipped_reasons}", "INFO")
        if not contract_pending_orders:
            log_and_print(f"No pending orders collected for {market} {timeframe}. Skipped reasons: {skipped_reasons}", "WARNING")
        
        # Save pending orders to contractpendingorders.json
        try:
            with open(pending_orders_json_path, 'w') as f:
                json.dump(contract_pending_orders, f, indent=4)
            log_and_print(
                f"Saved {len(contract_pending_orders)} pending orders to {pending_orders_json_path} for {market} {timeframe}",
                "SUCCESS"
            )
        except Exception as e:
            log_and_print(f"Error saving contractpendingorders.json for {market} {timeframe}: {str(e)}", "ERROR")
            return False
        
        # Aggregate all pending orders across markets and timeframes
        all_pending_orders = []
        timeframe_counts_pending = {
            "5minutes": 0,
            "15minutes": 0,
            "30minutes": 0,
            "1Hour": 0,
            "4Hour": 0
        }
        
        # Iterate through all markets and timeframes
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
                    except Exception as e:
                        log_and_print(f"Error reading {pending_path}: {str(e)}", "WARNING")
        
        # Prepare and save collective pending orders JSON
        pending_output = {
            "allpendingorders": len(all_pending_orders),
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
        except Exception as e:
            log_and_print(f"Error saving allpendingorders.json: {str(e)}", "ERROR")
            return False
        
        return True
    
    except Exception as e:
        log_and_print(f"Error collecting pending orders for {market} {timeframe}: {str(e)}", "ERROR")
        return False

def collect_all_executionercandle_orders(market: str, timeframe: str, json_dir: str) -> bool:
    """Collect executioner candle orders from pricecandle.json for a specific market and timeframe,
    move to executedorders.json, remove from pricecandle.json, and aggregate across all markets and timeframes to allexecutedorders.json."""
    log_and_print(f"Collecting executioner candle orders for market={market}, timeframe={timeframe}", "INFO")
    
    # Define file paths
    pricecandle_json_path = os.path.join(json_dir, "pricecandle.json")
    executed_orders_json_path = os.path.join(json_dir, "executedorders.json")
    collective_executed_path = os.path.join(BASE_OUTPUT_FOLDER, "allexecutedorders.json")
    
    # Check if pricecandle.json exists
    if not os.path.exists(pricecandle_json_path):
        log_and_print(f"pricecandle.json not found at {pricecandle_json_path} for {market} {timeframe}", "ERROR")
        return False
    
    try:
        # Load pricecandle.json
        with open(pricecandle_json_path, 'r') as f:
            pricecandle_data = json.load(f)
        
        # Extract executioner candle orders from pricecandle.json
        executed_orders = []
        seen_entry_prices = {}  # Track unique entry prices to keep oldest order
        skipped_reasons = {}  # Track why trendlines are skipped
        trendlines_to_keep = []  # Track trendlines to retain in pricecandle.json
        
        for trendline in pricecandle_data:
            executioner_candle = trendline.get("executioner candle", {})
            trendline_type = trendline.get("type")
            order_holder = trendline.get("order_holder", {})
            order_holder_position = order_holder.get("position_number")
            order_holder_timestamp = order_holder.get("Time", "N/A")
            order_type = trendline.get("receiver", {}).get("order_type", "").lower()
            contract_status = trendline.get("contract status summary", {}).get("contract status", "")
            
            # Skip if there is no valid executioner candle or it wasn't triggered
            if not executioner_candle or executioner_candle.get("status") != "Candle found at order holder entry level":
                skipped_reasons[trendline_type] = f"No valid executioner candle or status not triggered: {executioner_candle.get('status', 'N/A')}"
                log_and_print(f"No valid executioner candle for trendline {trendline_type} in {market} {timeframe}", "INFO")
                trendlines_to_keep.append(trendline)  # Keep non-executed trendline
                continue
            
            # Extract entry_price from order_holder
            actual_price = order_holder.get("High" if order_type == "long" else "Low", 0)
            if actual_price == 0:
                skipped_reasons[trendline_type] = "Invalid order_holder price"
                log_and_print(f"Invalid order_holder price for trendline {trendline_type} in {market} {timeframe}", "WARNING")
                trendlines_to_keep.append(trendline)  # Keep trendline with invalid price
                continue
            
            # Deduplicate based on entry_price, keep oldest
            if actual_price in seen_entry_prices:
                existing_time = seen_entry_prices[actual_price]["order_holder_timestamp"]
                if order_holder_timestamp < existing_time:
                    seen_entry_prices[actual_price] = {
                        "trendline": trendline,
                        "order_holder_timestamp": order_holder_timestamp
                    }
                    log_and_print(
                        f"Duplicate executioner order detected for trendline {trendline_type} with entry_price {actual_price} in {market} {timeframe}. "
                        f"Keeping older entry at {order_holder_timestamp}.",
                        "INFO"
                    )
                else:
                    skipped_reasons[trendline_type] = f"Duplicate entry_price {actual_price}, newer timestamp {order_holder_timestamp}"
                    log_and_print(
                        f"Duplicate executioner order detected for trendline {trendline_type} with entry_price {actual_price} in {market} {timeframe}. "
                        f"Discarding newer entry at {order_holder_timestamp}.",
                        "INFO"
                    )
                    trendlines_to_keep.append(trendline)  # Keep newer duplicate
                    continue
            else:
                seen_entry_prices[actual_price] = {
                    "trendline": trendline,
                    "order_holder_timestamp": order_holder_timestamp
                }
        
        # Process deduplicated executioner orders
        for entry in seen_entry_prices.values():
            trendline = entry["trendline"]
            trendline_type = trendline.get("type")
            order_holder = trendline.get("order_holder", {})
            order_holder_position = order_holder.get("position_number")
            order_holder_timestamp = order_holder.get("Time", "N/A")
            order_type = trendline.get("receiver", {}).get("order_type", "").lower()
            executioner_candle = trendline.get("executioner candle", {})
            sender = trendline.get("sender", {})
            receiver = trendline.get("receiver", {})
            breakout_parent = trendline.get("Breakout_parent", {})
            
            # Create executed order entry using the pricecandle.json format
            executed_order = {
                "market": market,
                "timeframe": timeframe,
                "type": trendline_type,
                "sender": {
                    "candle_color": sender.get("candle_color", ""),
                    "position_number": sender.get("position_number", 0),
                    "sender_arrow_number": sender.get("sender_arrow_number", 0),
                    "Time": sender.get("Time", "N/A"),
                    "Open": sender.get("Open", 0.0),
                    "High": sender.get("High", 0.0),
                    "Low": sender.get("Low", 0.0),
                    "Close": sender.get("Close", 0.0)
                },
                "receiver": {
                    "candle_color": receiver.get("candle_color", ""),
                    "position_number": receiver.get("position_number", 0),
                    "order_type": receiver.get("order_type", ""),
                    "order_status": receiver.get("order_status", ""),
                    "Breakout_parent": receiver.get("Breakout_parent", ""),
                    "order_parent": receiver.get("order_parent", ""),
                    "actual_orderparent": receiver.get("actual_orderparent", ""),
                    "reassigned_orderparent": receiver.get("reassigned_orderparent", ""),
                    "receiver_contractcandle_arrownumber": receiver.get("receiver_contractcandle_arrownumber", 0),
                    "Time": receiver.get("Time", "N/A"),
                    "Open": receiver.get("Open", 0.0),
                    "High": receiver.get("High", 0.0),
                    "Low": receiver.get("Low", 0.0),
                    "Close": receiver.get("Close", 0.0)
                },
                "order_holder": {
                    "label": order_holder.get("label", ""),
                    "position_number": order_holder_position,
                    "Time": order_holder_timestamp,
                    "Open": order_holder.get("Open", 0.0),
                    "High": order_holder.get("High", 0.0),
                    "Low": order_holder.get("Low", 0.0),
                    "Close": order_holder.get("Close", 0.0)
                },
                "Breakout_parent": {
                    "label": breakout_parent.get("label", ""),
                    "position_number": breakout_parent.get("position_number", 0),
                    "Time": breakout_parent.get("Time", "N/A"),
                    "Open": breakout_parent.get("Open", 0.0),
                    "High": breakout_parent.get("High", 0.0),
                    "Low": breakout_parent.get("Low", 0.0),
                    "Close": breakout_parent.get("Close", 0.0),
                    "candle_rightafter_Breakoutparent": breakout_parent.get("candle_rightafter_Breakoutparent", {})
                },
                "executioner candle": {
                    "status": executioner_candle.get("status", "N/A"),
                    "position_number": executioner_candle.get("position_number", 0),
                    "Time": executioner_candle.get("Time", "N/A"),
                    "Open": executioner_candle.get("Open", 0.0),
                    "High": executioner_candle.get("High", 0.0),
                    "Low": executioner_candle.get("Low", 0.0),
                    "Close": executioner_candle.get("Close", 0.0)
                }
            }
            
            # Validate prices
            if any(price <= 0 for price in [
                executed_order["sender"]["Open"],
                executed_order["sender"]["High"],
                executed_order["sender"]["Low"],
                executed_order["sender"]["Close"],
                executed_order["receiver"]["Open"],
                executed_order["receiver"]["High"],
                executed_order["receiver"]["Low"],
                executed_order["receiver"]["Close"],
                executed_order["order_holder"]["Open"],
                executed_order["order_holder"]["High"],
                executed_order["order_holder"]["Low"],
                executed_order["order_holder"]["Close"],
                executed_order["Breakout_parent"]["Open"],
                executed_order["Breakout_parent"]["High"],
                executed_order["Breakout_parent"]["Low"],
                executed_order["Breakout_parent"]["Close"],
                executed_order["executioner candle"]["Open"],
                executed_order["executioner candle"]["High"],
                executed_order["executioner candle"]["Low"],
                executed_order["executioner candle"]["Close"]
            ]):
                skipped_reasons[trendline_type] = f"Invalid price values: {executed_order}"
                log_and_print(f"Invalid price values for trendline {trendline_type} in {market} {timeframe}: {executed_order}", "WARNING")
                trendlines_to_keep.append(trendline)  # Keep trendline with invalid prices
                continue
            
            executed_orders.append(executed_order)
            log_and_print(
                f"Moving executioner order for trendline {trendline_type}: "
                f"order_type={executed_order['receiver']['order_type']}, "
                f"order_holder_position={order_holder_position}, "
                f"order_holder_timestamp={order_holder_timestamp}, "
                f"executioner_candle_timestamp={executed_order['executioner candle']['Time']}",
                "DEBUG"
            )
        
        # Log skipped reasons
        if skipped_reasons:
            log_and_print(f"Skipped trendlines in {market} {timeframe}: {skipped_reasons}", "INFO")
        if not executed_orders:
            log_and_print(f"No executioner orders moved for {market} {timeframe}. Skipped reasons: {skipped_reasons}", "WARNING")
        
        # Save updated pricecandle.json without executed orders
        try:
            with open(pricecandle_json_path, 'w') as f:
                json.dump(trendlines_to_keep, f, indent=4)
        except Exception as e:
            log_and_print(f"Error updating pricecandle.json for {market} {timeframe}: {str(e)}", "ERROR")
            return False
        
        # Save executioner orders to executedorders.json
        try:
            with open(executed_orders_json_path, 'w') as f:
                json.dump(executed_orders, f, indent=4)
            log_and_print(
                f"Saved {len(executed_orders)} executioner orders to {executed_orders_json_path} for {market} {timeframe}",
                "SUCCESS"
            )
        except Exception as e:
            log_and_print(f"Error saving executedorders.json for {market} {timeframe}: {str(e)}", "ERROR")
            return False
        
        # Aggregate all executed orders across markets and timeframes
        all_executed_orders = []
        timeframe_counts_executed = {
            "5minutes": 0,
            "15minutes": 0,
            "30minutes": 0,
            "1Hour": 0,
            "4Hour": 0
        }
        
        # Iterate through all markets and timeframes
        for mkt in MARKETS:
            formatted_market = mkt.replace(" ", "_")
            for tf in TIMEFRAMES:
                tf_dir = os.path.join(BASE_OUTPUT_FOLDER, formatted_market, tf.lower())
                executed_path = os.path.join(tf_dir, "executedorders.json")
                db_tf = DB_TIMEFRAME_MAPPING.get(tf, tf)
                
                if os.path.exists(executed_path):
                    try:
                        with open(executed_path, 'r') as f:
                            executed_data = json.load(f)
                        if isinstance(executed_data, list):
                            all_executed_orders.extend(executed_data)
                            timeframe_counts_executed[db_tf] += len(executed_data)
                        else:
                            log_and_print(f"Invalid data format in {executed_path}: Expected list, got {type(executed_data)}", "WARNING")
                    except Exception as e:
                        log_and_print(f"Error reading {executed_path}: {str(e)}", "WARNING")
        
        # Prepare and save collective executed orders JSON
        executed_output = {
            "allexecutedorders": len(all_executed_orders),
            "5minutes executed orders": timeframe_counts_executed["5minutes"],
            "15minutes executed orders": timeframe_counts_executed["15minutes"],
            "30minutes executed orders": timeframe_counts_executed["30minutes"],
            "1Hour executed orders": timeframe_counts_executed["1Hour"],
            "4Hours executed orders": timeframe_counts_executed["4Hour"],
            "orders": all_executed_orders
        }
        
        try:
            with open(collective_executed_path, 'w') as f:
                json.dump(executed_output, f, indent=4)
            log_and_print(
                f"Saved {len(all_executed_orders)} executed orders to {collective_executed_path} "
                f"(5m: {timeframe_counts_executed['5minutes']}, 15m: {timeframe_counts_executed['15minutes']}, "
                f"30m: {timeframe_counts_executed['30minutes']}, 1H: {timeframe_counts_executed['1Hour']}, "
                f"4H: {timeframe_counts_executed['4Hour']})",
                "SUCCESS"
            )
        except Exception as e:
            log_and_print(f"Error saving allexecutedorders.json: {str(e)}", "ERROR")
            return False
        
        return True
    
    except Exception as e:
        log_and_print(f"Error moving executioner orders for {market} {timeframe}: {str(e)}", "ERROR")
        return False

def marketsliststatus() -> bool:
    """Generate marketsorderlist.json with pending orders, order-free markets, and new number position for matched candle data by timeframe.
    Updates status.json for order-free market-timeframe pairs to 'order_free' and pending order pairs to 'chart_identified' based on new rules.
    Appends daily date in DD-MM-YYYY format to bouncestreamdates.json if not already present."""
    log_and_print("Generating markets order list status", "INFO")
    
    markets_order_list_path = os.path.join(BASE_OUTPUT_FOLDER, "marketsorderlist.json")
    collective_pending_path = os.path.join(BASE_OUTPUT_FOLDER, "allpendingorders.json")
    bouncestream_dates_path = os.path.join(BASE_OUTPUT_FOLDER, "bouncestreamdates.json")
    status_json_base_path = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\fetched"
    
    # Initialize the output structure
    markets_pending = {
        "candlesamountinbetween": {
            "m5": "0",
            "m15": "0",
            "m30": "0",
            "1H": "0",
            "4H": "0"
        }
    }
    order_free_markets = {
        "m5": [],
        "m15": [],
        "m30": [],
        "1H": [],
        "4H": []
    }
    
    # Map timeframes to the required format
    timeframe_mapping = {
        "M5": "m5",
        "M15": "m15",
        "M30": "m30",
        "H1": "1H",
        "H4": "4H"
    }
    
    try:
        # Load allpendingorders.json
        if not os.path.exists(collective_pending_path):
            log_and_print(f"allpendingorders.json not found at {collective_pending_path}", "ERROR")
            return False
        
        with open(collective_pending_path, 'r') as f:
            pending_data = json.load(f)
        
        if not isinstance(pending_data, dict) or "orders" not in pending_data:
            log_and_print(f"Invalid data format in {collective_pending_path}: Expected dict with 'orders'", "ERROR")
            return False
        
        pending_orders = pending_data.get("orders", [])
        log_and_print(f"Loaded {len(pending_orders)} pending orders from {collective_pending_path}", "DEBUG")
        
        # Initialize counts for all markets and timeframes
        for market in MARKETS:
            formatted_market = market.replace(" ", "_")
            markets_pending[formatted_market] = {}
            for tf in TIMEFRAMES:
                tf_key = timeframe_mapping.get(tf, tf.lower())
                markets_pending[formatted_market][tf_key] = {
                    "buy_limit": 0,
                    "sell_limit": 0
                }
        
        # Count pending orders by market, timeframe, and order type
        for order in pending_orders:
            market = order.get("market", "").replace(" ", "_")
            timeframe = order.get("timeframe", "")
            order_type = order.get("order_type", "").lower()
            
            tf_key = timeframe_mapping.get(timeframe, timeframe.lower())
            
            if market in markets_pending and tf_key in markets_pending[market]:
                if order_type == "buy_limit":
                    markets_pending[market][tf_key]["buy_limit"] += 1
                elif order_type == "sell_limit":
                    markets_pending[market][tf_key]["sell_limit"] += 1
        
        # Collect new number position for matched candle data for each timeframe
        for tf in TIMEFRAMES:
            tf_key = timeframe_mapping.get(tf, tf.lower())
            new_number_position = None
            for market in MARKETS:
                formatted_market = market.replace(" ", "_")
                json_dir = os.path.join(BASE_OUTPUT_FOLDER, formatted_market, tf.lower())
                candles_inbetween_path = os.path.join(json_dir, "candlesamountinbetween.json")
                
                if os.path.exists(candles_inbetween_path):
                    try:
                        with open(candles_inbetween_path, 'r') as f:
                            candles_data = json.load(f)
                        new_number_position_value = candles_data.get("new number position for matched candle data")
                        if new_number_position_value is not None:
                            new_number_position = str(new_number_position_value)
                            # Take the first valid value
                            break
                    except Exception as e:
                        log_and_print(
                            f"Error reading candlesamountinbetween.json for {market} {tf}: {str(e)}",
                            "WARNING"
                        )
                        continue
                else:
                    log_and_print(
                        f"candlesamountinbetween.json not found for {market} {tf} at {candles_inbetween_path}",
                        "WARNING"
                    )
            
            # Assign the new number position to the timeframe, default to "0" if not found
            markets_pending["candlesamountinbetween"][tf_key] = new_number_position if new_number_position is not None else "0"
        
        # Handle bouncestreamdates.json
        current_date = datetime.now(pytz.timezone('Africa/Lagos')).strftime('%d-%m-%Y')
        dates_data = {"dates": []}
        
        # Load existing dates if file exists
        if os.path.exists(bouncestream_dates_path):
            try:
                with open(bouncestream_dates_path, 'r') as f:
                    dates_data = json.load(f)
                if not isinstance(dates_data, dict) or "dates" not in dates_data:
                    log_and_print(f"Invalid format in {bouncestream_dates_path}: Expected dict with 'dates'", "WARNING")
                    dates_data = {"dates": []}
            except Exception as e:
                log_and_print(f"Error reading bouncestreamdates.json: {str(e)}", "WARNING")
                dates_data = {"dates": []}
        
        # Append today's date if not already present
        if current_date not in dates_data["dates"]:
            dates_data["dates"].append(current_date)
            try:
                with open(bouncestream_dates_path, 'w') as f:
                    json.dump(dates_data, f, indent=4)
                log_and_print(f"Appended date {current_date} to {bouncestream_dates_path}", "DEBUG")
            except Exception as e:
                log_and_print(f"Error writing to bouncestreamdates.json: {str(e)}", "ERROR")
        else:
            log_and_print(f"Date {current_date} already exists in {bouncestream_dates_path}, skipping append", "DEBUG")
        
        # Determine order-free markets and update status.json based on new rules
        for market in MARKETS:
            formatted_market = market.replace(" ", "_")
            for tf in TIMEFRAMES:
                tf_key = timeframe_mapping.get(tf, tf.lower())
                status_file = os.path.join(status_json_base_path, formatted_market, tf.lower(), "status.json")
                
                # Get current time in WAT (Africa/Lagos, UTC+1)
                current_time = datetime.now(pytz.timezone('Africa/Lagos'))
                am_pm = "am" if current_time.hour < 12 else "pm"
                hour_12 = current_time.hour % 12
                if hour_12 == 0:
                    hour_12 = 12  # Convert 0 to 12 for 12 AM/PM
                timestamp = (
                    f"{current_time.strftime('%Y-%m-%d T %I:%M:%S')} {am_pm} "
                    f".{current_time.microsecond:06d}+01:00"
                )
                
                # Prepare status data
                status_data = {
                    "market": market,
                    "timeframe": tf,
                    "timestamp": timestamp
                }
                
                # Get order counts and candles amount
                buy_limit_count = markets_pending.get(formatted_market, {}).get(tf_key, {}).get("buy_limit", 0)
                sell_limit_count = markets_pending.get(formatted_market, {}).get(tf_key, {}).get("sell_limit", 0)
                candles_count = int(markets_pending["candlesamountinbetween"].get(tf_key, "0"))
                
                # Apply new rules to determine order-free status
                is_order_free = False
                
                if buy_limit_count == 0 and sell_limit_count == 0:
                    # No orders
                    if tf_key == "m5" and candles_count >= 100:
                        is_order_free = True
                    elif tf_key == "m15" and candles_count >= 100:
                        is_order_free = True
                    elif tf_key == "m30" and candles_count >= 50:
                        is_order_free = True
                    elif tf_key == "1H" and candles_count >= 20:
                        is_order_free = True
                    elif tf_key == "4H" and candles_count >= 10:
                        is_order_free = True
                elif (buy_limit_count == 0 and sell_limit_count > 0) or (buy_limit_count > 0 and sell_limit_count == 0):
                    # One order type exists
                    if tf_key == "m5" and candles_count >= 200:
                        is_order_free = True
                    elif tf_key == "m15" and candles_count >= 100:
                        is_order_free = True
                    elif tf_key == "m30" and candles_count >= 50:
                        is_order_free = True
                    elif tf_key == "1H" and candles_count >= 20:
                        is_order_free = True
                    elif tf_key == "4H" and candles_count >= 20:
                        is_order_free = True
                elif buy_limit_count > 0 and sell_limit_count > 0:
                    # Both order types exist
                    if tf_key == "m5" and candles_count >= 500:
                        is_order_free = True
                    elif tf_key == "m15" and candles_count >= 300:
                        is_order_free = True
                    elif tf_key == "m30" and candles_count >= 200:
                        is_order_free = True
                    elif tf_key == "1H" and candles_count >= 100:
                        is_order_free = True
                    elif tf_key == "4H" and candles_count >= 50:
                        is_order_free = True
                
                # Set status based on rules
                if is_order_free:
                    order_free_markets[tf_key].append(market)
                    status_data["status"] = "order_free"
                    status_data["elligible_status"] = "order_free"
                else:
                    status_data["status"] = "chart_identified"
                    status_data["elligible_status"] = "chart_identified"
                
                # Update status.json
                try:
                    os.makedirs(os.path.dirname(status_file), exist_ok=True)
                    with open(status_file, 'w') as f:
                        json.dump(status_data, f, indent=4)
                except Exception as e:
                    log_and_print(f"Error updating status.json for {market} ({tf}) at {status_file}: {str(e)}", "ERROR")
        
        # Prepare output structure
        output = {
            "markets_pending": markets_pending,
            "order_free_markets": order_free_markets
        }
        
        # Save to marketsorderlist.json
        try:
            with open(markets_order_list_path, 'w') as f:
                json.dump(output, f, indent=4)
            log_and_print(
                f"Saved markets order list to {markets_order_list_path}: "
                f"{len(markets_pending) - 1} markets with pending orders, "  # Subtract 1 for candlesamountinbetween
                f"order-free markets - M5: {len(order_free_markets['m5'])}, "
                f"M15: {len(order_free_markets['m15'])}, "
                f"M30: {len(order_free_markets['m30'])}, "
                f"1H: {len(order_free_markets['1H'])}, "
                f"4H: {len(order_free_markets['4H'])}, "
                f"new number position - M5: {markets_pending['candlesamountinbetween']['m5']}, "
                f"M15: {markets_pending['candlesamountinbetween']['m15']}, "
                f"M30: {markets_pending['candlesamountinbetween']['m30']}, "
                f"1H: {markets_pending['candlesamountinbetween']['1H']}, "
                f"4H: {markets_pending['candlesamountinbetween']['4H']}",
                "SUCCESS"
            )
        except Exception as e:
            log_and_print(f"Error saving marketsorderlist.json: {str(e)}", "ERROR")
            return False
        
        return True
    
    except Exception as e:
        log_and_print(f"Error processing marketsliststatus: {str(e)}", "ERROR")
        return False

def insertpendingorderstodb(json_path: str = os.path.join(BASE_OUTPUT_FOLDER, "allpendingorders.json")) -> bool:
    """Insert all pending orders from allpendingorders.json into cipherbouncestream_signals table."""
    log_and_print("Inserting all pending orders into cipherbouncestream_signals table", "INFO")
    
    # Initialize error log list
    error_log = []
    
    # Define error log file path
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
        
        # Extract orders list
        orders = data.get('orders', [])
        if not orders:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": "No orders found in allpendingorders.json or 'orders' key is missing"
            })
            save_errors()
            log_and_print("No orders found in allpendingorders.json or 'orders' key is missing", "ERROR")
            return False
        
        log_and_print(f"Loaded {len(orders)} pending orders from {json_path}", "INFO")
        
    except Exception as e:
        error_log.append({
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
            "error": f"Error loading allpendingorders.json: {str(e)}"
        })
        save_errors()
        log_and_print(f"Error loading allpendingorders.json: {str(e)}", "ERROR")
        return False
    
    # Prepare SQL INSERT query with formatted values
    sql_query = """
        INSERT INTO cipherbouncestream_signals (
            pair, timeframe, order_type, entry_price, 
            ratio_0_5_price, ratio_1_price, ratio_2_price, 
            profit_price
        ) VALUES 
    """
    value_strings = []
    
    for order in orders:
        try:
            pair = order.get('pair', 'N/A')
            timeframe = DB_TIMEFRAME_MAPPING.get(order.get('timeframe', 'N/A'), order.get('timeframe', 'N/A'))
            order_type = order.get('order_type', 'N/A')
            entry_price = float(order.get('entry_price', 0.0))
            ratio0_5_price = float(order.get('1:0.5_price', 0.0))
            ratio_1_price = float(order.get('1:1_price', 0.0))
            ratio_2_price = float(order.get('1:2_price', 0.0))
            profit_price = float(order.get('profit_price', 0.0))
            
            # Format values into a single string, ensuring strings are quoted
            value_string = (
                f"('{pair}', '{timeframe}', '{order_type}', {entry_price}, "
                f"{ratio0_5_price}, {ratio_1_price}, {ratio_2_price}, {profit_price})"
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
            "error": "No valid orders to insert after processing"
        })
        save_errors()
        log_and_print("No valid orders to insert after processing", "ERROR")
        return False
    
    # Combine value strings into SQL query
    sql_query += ", ".join(value_strings)
    
    # Execute query with retries
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = db.execute_query(sql_query)
            log_and_print(f"Raw query result for inserting pending orders: {json.dumps(result, indent=2)}", "DEBUG")
            
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
                
            # Check affected rows
            affected_rows = result.get('results', {}).get('affected_rows', 0)
            if affected_rows > 0:
                log_and_print(f"Successfully inserted {affected_rows} pending orders into cipherbouncestream_signals", "SUCCESS")
                return True
            else:
                error_log.append({
                    "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                    "error": f"No rows affected on attempt {attempt}"
                })
                save_errors()
                log_and_print(f"No rows affected on attempt {attempt}", "ERROR")
                continue
                
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
                "error": "Max retries reached for inserting pending orders"
            })
            save_errors()
            log_and_print("Max retries reached for inserting pending orders", "ERROR")
            return False
    
    error_log.append({
        "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
        "error": "Function exited without success"
    })
    save_errors()
    return False
def executeinsertpendingorderstodb():
    """Execute the insertion of pending orders into the database."""
    log_and_print("===== Execute Insert Pending Orders to Database =====", "TITLE")
    if not insertpendingorderstodb():
        log_and_print("Failed to insert pending orders into database. Exiting.", "ERROR")
        return
    log_and_print("===== Insert Pending Orders to Database Completed =====", "TITLE")

def process_5minutes_timeframe():
    """Process all markets for the 5-minute (M5) timeframe, checking candle time left."""
    try:
        log_and_print("===== Fetch and Process M5 Candle Data =====", "TITLE")
        
        # Verify that credentials were loaded
        if not all([LOGIN_ID, PASSWORD, SERVER, TERMINAL_PATH]):
            log_and_print("Credentials not properly loaded from base.json. Exiting.", "ERROR")
            return
        
        # Check M5 candle time left globally
        if not MARKETS:
            log_and_print("No markets defined in MARKETS list. Exiting.", "ERROR")
            return
        default_market = MARKETS[0]
        timeframe = "M5"
        log_and_print(f"Checking M5 candle time left using market: {default_market}", "INFO")
        time_left, next_close_time = candletimeleft_5minutes(default_market, None, min_time_left=1)
        
        if time_left is None or next_close_time is None:
            log_and_print(f"Failed to retrieve candle time for {default_market} (M5). Exiting.", "ERROR")
            return
        
        log_and_print(f"M5 candle time left: {time_left:.2f} minutes. Proceeding with execution.", "INFO")

        # Create tasks for all markets with M5 timeframe
        tasks = [(market, timeframe) for market in MARKETS]
        success_data = []
        no_pending_data = []
        failed_data = []

        with multiprocessing.Pool(processes=4) as pool:
            results = pool.starmap(process_market_timeframe, tasks)

        # Collect status for each market
        for (market, _), (success, error_message, status, process_messages) in zip(tasks, results):
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
                    "message": process_messages.get("match_trendline_with_candle_data", "No pending orders found"),
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

        # Log summary
        success_count = len(success_data)
        no_pending_count = len(no_pending_data)
        failed_count = len(failed_data)
        log_and_print(f"M5 processing completed: {success_count}/{len(tasks)} markets processed successfully, "
                      f"{no_pending_count} with no pending orders, {failed_count} failed", "INFO")
        
    except Exception as e:
        log_and_print(f"Error in M5 processing: {str(e)}", "ERROR")
    finally:
        log_and_print("===== Fetch and Process M5 Candle Data Completed =====", "TITLE")

def process_market_timeframe(market: str, timeframe: str) -> Tuple[bool, Optional[str], str, Dict]:
    """Process a single market and timeframe combination, returning success status, error message, status, and process messages."""
    error_message = None
    status = "failed"
    process_messages = {}
    try:
        log_and_print(f"Processing market: {market}, timeframe: {timeframe}", "INFO")
        
        # Fetch candle data
        candle_data, json_dir = fetch_candle_data(market, timeframe)
        if candle_data is None or json_dir is None:
            error_message = f"Failed to fetch candle data for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["fetch_candle_data"] = error_message
            return False, error_message, "failed", process_messages
        with open(os.path.join(json_dir, 'candle_data.json'), 'r') as f:
            candle_data_content = json.load(f)
        candle_count = len(candle_data_content)
        process_messages["fetch_candle_data"] = f"Fetched {candle_count} candles for {market} {timeframe}"

        # Save the most recent completed candle
        if not save_new_mostrecent_completed_candle(market, timeframe, json_dir):
            error_message = f"Failed to save most recent completed candle for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["save_new_mostrecent_completed_candle"] = error_message
        else:
            with open(os.path.join(json_dir, 'newmostrecent_completedcandle.json'), 'r') as f:
                candle_content = json.load(f)
            candle_time = candle_content.get('time', 'unknown')
            process_messages["save_new_mostrecent_completed_candle"] = f"Saved most recent completed candle at {candle_time} for {market} {timeframe}"

        # Match most recent completed candle with candle data
        if not match_mostrecent_candle(market, timeframe, json_dir):
            error_message = f"Failed to match most recent completed candle for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["match_mostrecent_candle"] = error_message
        else:
            with open(os.path.join(json_dir, 'matchedcandles.json'), 'r') as f:
                matched_data = json.load(f)
            match_status = matched_data.get('match_result_status', 'unknown')
            candles_inbetween = matched_data.get('candles_inbetween', '0')
            process_messages["match_mostrecent_candle"] = f"Matched most recent candle with status '{match_status}' and {candles_inbetween} candles in between for {market} {timeframe}"

        # Calculate candles in between
        if not calculate_candles_inbetween(market, timeframe, json_dir):
            error_message = f"Failed to calculate candles in between for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["calculate_candles_inbetween"] = error_message
        else:
            with open(os.path.join(json_dir, 'candlesamountinbetween.json'), 'r') as f:
                inbetween_data = json.load(f)
            candles_count = inbetween_data.get('candles in between', '0')
            plus_newmostrecent = inbetween_data.get('plus_newmostrecent', '0')
            process_messages["calculate_candles_inbetween"] = f"Calculated {candles_count} candles in between, plus {plus_newmostrecent} including new most recent for {market} {timeframe}"

        # Match trendline with candle data
        success, error_message, status = match_trendline_with_candle_data(candle_data, json_dir, market, timeframe)
        if not success:
            log_and_print(error_message or f"Failed to process pending orders for {market} {timeframe}", "INFO" if status == "no_pending_orders" else "ERROR")
            if status == "no_pending_orders":
                pending_json_path = os.path.join(BASE_PROCESSING_FOLDER, market.replace(" ", "_"), timeframe.lower(), "pendingorder.json")
                if not os.path.exists(pending_json_path):
                    process_messages["match_trendline_with_candle_data"] = f"No pending orders found because pendingorder.json does not exist for {market} {timeframe}"
                else:
                    with open(pending_json_path, 'r') as f:
                        pending_data = json.load(f)
                    if not pending_data:
                        process_messages["match_trendline_with_candle_data"] = f"No pending orders found because pendingorder.json is empty for {market} {timeframe}"
                    else:
                        process_messages["match_trendline_with_candle_data"] = f"No pending orders found for {market} {timeframe}"
            else:
                process_messages["match_trendline_with_candle_data"] = f"Failed to match pending orders: {error_message}"
            return False, error_message, status, process_messages
        else:
            with open(os.path.join(json_dir, 'pricecandle.json'), 'r') as f:
                pricecandle_data = json.load(f)
            trendline_count = len(pricecandle_data)
            process_messages["match_trendline_with_candle_data"] = f"Matched {trendline_count} trendlines with candle data for {market} {timeframe}"

        # Fetch candles from after Breakout_parent to current price
        if not candleafterbreakoutparent_to_currentprice(market, timeframe, json_dir):
            error_message = f"Failed to fetch candles after Breakout_parent for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["candleafterbreakoutparent_to_currentprice"] = error_message
        else:
            with open(os.path.join(json_dir, 'candlesafterbreakoutparent.json'), 'r') as f:
                cabp_data = json.load(f)
            trendline_count = len(cabp_data)
            total_candles = sum(len(trendline.get('candles', [])) for trendline in cabp_data)
            process_messages["candleafterbreakoutparent_to_currentprice"] = f"Fetched {total_candles} candles for {trendline_count} trendlines after Breakout_parent for {market} {timeframe}"

        # Search for executioner candle and update pricecandle.json
        if not executioncandle_after_breakoutparent(market, timeframe, json_dir):
            error_message = f"Failed to process executioner candle for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["executioncandle_after_breakoutparent"] = error_message
        else:
            with open(os.path.join(json_dir, 'pricecandle.json'), 'r') as f:
                pricecandle_data = json.load(f)
            executioner_found = 0
            executioner_not_found = 0
            for trendline in pricecandle_data:
                exec_candle = trendline.get('executioner candle', {})
                if exec_candle.get('status') == 'Candle found at order holder entry level':
                    executioner_found += 1
                else:
                    executioner_not_found += 1
            process_messages["executioncandle_after_breakoutparent"] = (
                f"Processed {executioner_found} trendlines with executioner candle found, "
                f"{executioner_not_found} with no executioner candle for {market} {timeframe}"
            )

        # Calculate order holder prices with lot size and risk
        if not getorderholderpriceswithlotsizeandrisk(market, timeframe, json_dir):
            error_message = f"Failed to calculate order holder prices with lot size and risk for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["getorderholderpriceswithlotsizeandrisk"] = error_message
        else:
            output_json_path = os.path.join(json_dir, 'calculatedprices.json')
            if os.path.exists(output_json_path):
                with open(output_json_path, 'r') as f:
                    calculated_data = json.load(f)
                process_messages["getorderholderpriceswithlotsizeandrisk"] = (
                    f"Calculated prices for {len(calculated_data)} trendlines for {market} {timeframe}"
                )
            else:
                process_messages["getorderholderpriceswithlotsizeandrisk"] = (
                    f"No calculated prices saved for {market} {timeframe}"
                )

        # Track breakeven, stoploss, and profit
        if not PendingOrderUpdater(market, timeframe, json_dir):
            error_message = f"Failed to track breakeven, stoploss, and profit for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["PendingOrderUpdater"] = error_message
        else:
            with open(os.path.join(json_dir, 'pricecandle.json'), 'r') as f:
                pricecandle_data = json.load(f)
            contract_statuses = [t.get('contract status summary', {}).get('contract status', 'unknown') for t in pricecandle_data]
            process_messages["PendingOrderUpdater"] = (
                f"Tracked breakeven, stoploss, and profit for {len(pricecandle_data)} trendlines: {', '.join(set(contract_statuses))} for {market} {timeframe}"
            )

        # Collect pending orders
        if not collect_all_pending_orders(market, timeframe, json_dir):
            error_message = f"Failed to collect pending orders for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["collect_all_pending_orders"] = error_message
        else:
            output_json_path = os.path.join(json_dir, 'contractpendingorders.json')
            collective_pending_path = os.path.join(BASE_OUTPUT_FOLDER, 'allpendingorders.json')
            pending_count = 0
            all_pending_count = 0
            if os.path.exists(output_json_path):
                with open(output_json_path, 'r') as f:
                    contract_data = json.load(f)
                pending_count = len(contract_data)
            if os.path.exists(collective_pending_path):
                with open(collective_pending_path, 'r') as f:
                    all_pending_data = json.load(f)
                all_pending_count = len(all_pending_data.get('orders', []))
            process_messages["collect_all_pending_orders"] = (
            )

        # Collect executioner candle orders
        if not collect_all_executionercandle_orders(market, timeframe, json_dir):
            error_message = f"Failed to collect executioner candle orders for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["collect_all_executionercandle_orders"] = error_message
        else:
            executed_orders_json_path = os.path.join(json_dir, 'executedorders.json')
            executed_count = 0
            if os.path.exists(executed_orders_json_path):
                with open(executed_orders_json_path, 'r') as f:
                    executed_data = json.load(f)
                executed_count = len(executed_data)
            process_messages["collect_all_executionercandle_orders"] = (
            )

        # Generate markets order list status
        if not marketsliststatus():
            error_message = f"Failed to generate markets order list status for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["marketsliststatus"] = error_message
        else:
            markets_order_list_path = os.path.join(BASE_OUTPUT_FOLDER, "marketsorderlist.json")
            if os.path.exists(markets_order_list_path):
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
            else:
                process_messages["marketsliststatus"] = (
                    f"No markets order list generated for {market} {timeframe}"
                )

        log_and_print(f"Completed processing market: {market}, timeframe: {timeframe}", "SUCCESS")
        return True, None, "success", process_messages

    except Exception as e:
        error_message = f"Unexpected error processing market {market} timeframe {timeframe}: {str(e)}"
        log_and_print(error_message, "ERROR")
        process_messages["error"] = error_message
        return False, error_message, "failed", process_messages
    finally:
        mt5.shutdown()

def main():
    """Main function to process all markets and timeframes, saving all status to marketsstatus.json."""
    try:
        log_and_print("===== Fetch and Process Candle Data =====", "TITLE")
        
        # Verify that credentials were loaded
        if not all([LOGIN_ID, PASSWORD, SERVER, TERMINAL_PATH]):
            log_and_print("Credentials not properly loaded from base.json. Exiting.", "ERROR")
            return
        
        # Check M15 candle time left globally
        if not MARKETS:
            log_and_print("No markets defined in MARKETS list. Exiting.", "ERROR")
            return
        default_market = MARKETS[0]
        timeframe = "M15"  # Changed from "M5" to "M15"
        log_and_print(f"Checking M15 candle time left using market: {default_market}", "INFO")
        time_left, next_close_time = candletimeleft(default_market, timeframe, None, min_time_left=0.3)
        
        if time_left is None or next_close_time is None:
            log_and_print(f"Failed to retrieve candle time for {default_market} (M15). Exiting.", "ERROR")
            return
        
        log_and_print(f"M15 candle time left: {time_left:.2f} minutes. Proceeding with execution.", "INFO")

        # Create tasks for all market-timeframe combinations
        tasks = [(market, timeframe) for market in MARKETS for timeframe in TIMEFRAMES]
        success_data = []
        no_pending_data = []
        failed_data = []

        with multiprocessing.Pool(processes=4) as pool:
            results = pool.starmap(process_market_timeframe, tasks)

        # Collect status for each market-timeframe combination
        for (market, timeframe), (success, error_message, status, process_messages) in zip(tasks, results):
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
                    "message": process_messages.get("match_trendline_with_candle_data", "No pending orders found"),
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

        # Log summary
        success_count = len(success_data)
        no_pending_count = len(no_pending_data)
        failed_count = len(failed_data)
        log_and_print(f"Processing completed: {success_count}/{len(tasks)} market-timeframe combinations processed successfully, "
                      f"{no_pending_count} with no pending orders, {failed_count} failed, all recorded in marketsstatus.json", "INFO")
        

    except Exception as e:
        log_and_print(f"Error in main processing: {str(e)}", "ERROR")
    finally:
        log_and_print("===== Fetch and Process Candle Data Completed =====", "TITLE")

if __name__ == "__main__":
    executefetchlotsizeandrisk()
    main()
    executeinsertpendingorderstodb()
    

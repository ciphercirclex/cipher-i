import json
import os
import multiprocessing
import time
from datetime import datetime, timezone  # Added timezone import
from typing import Dict, Optional, List, Tuple
import pandas as pd
import MetaTrader5 as mt5
from colorama import Fore, Style, init
import logging
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
LOGIN_ID = "101347351"
PASSWORD = "@Techknowdge12#"
SERVER = "DerivSVG-Server-02"
TERMINAL_PATH = r"C:\Program Files\MetaTrader 5\terminal64.exe"  # Update with your MT5 terminal path
MAX_RETRIES = 5
RETRY_DELAY = 3

# Market names and timeframes
MARKETS = [
    "AUDUSD", "Volatility 75 Index", "Step Index", "Drift Switch Index 30",
    "Drift Switch Index 20", "Drift Switch Index 10", "Volatility 25 Index",
    "XAUUSD", "US Tech 100", "Wall Street 30", "GBPUSD", "EURUSD", "USDJPY",
    "USDCAD", "USDCHF", "NZDUSD"
]
TIMEFRAMES = ["M5", "M15", "M30", "H1", "H4"]

# Base paths
BASE_PROCESSING_FOLDER = r"C:\xampp\htdocs\CIPHER\cipher i\bouncestream\chart\processing"
BASE_OUTPUT_FOLDER = r"C:\xampp\htdocs\CIPHER\cipher i\bouncestream\chart\orders"
FETCHCHART_DESTINATION_PATH = r"C:\xampp\htdocs\CIPHER\cipher i\bouncestream\chart\fetched"

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
    "H1": "1hour",
    "H4": "4hour"
}


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
    candles = mt5.copy_rates_from_pos(market, mt5_timeframe, 1, 300)
    if candles is None or len(candles) < 300:
        log_and_print(f"Failed to fetch candle data for {market} {timeframe}, error: {mt5.last_error()}", "ERROR")
        mt5.shutdown()
        return None, None

    df = pd.DataFrame(candles)
    candle_data = {}
    for i in range(300):
        candle = df.iloc[i]
        candle_details = {
            "Time": str(pd.to_datetime(candle['time'], unit='s')),
            "Open": float(candle['open']),
            "High": float(candle['high']),
            "Low": float(candle['low']),
            "Close": float(candle['close'])
        }
        candle_data[f"Candle_{300-i}"] = candle_details

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
        log_and_print(f"Candle details saved to {json_file_path}", "SUCCESS")
    except Exception as e:
        log_and_print(f"Error saving candle data for {market} {timeframe}: {e}", "ERROR")
        mt5.shutdown()
        return None, None

    mt5.shutdown()
    return candle_data, json_dir

def match_trendline_with_candle_data(candle_data: Dict, json_dir: str, market: str, timeframe: str) -> Tuple[bool, Optional[str], str]:
    """Match pending order data with candle data and save to pricecandle.json."""
    formatted_market_name = market.replace(" ", "_")
    pending_json_path = os.path.join(BASE_PROCESSING_FOLDER, formatted_market_name, timeframe.lower(), "pendingorder.json")

    # Check if pendingorder.json exists
    if not os.path.exists(pending_json_path):
        error_message = f"Pending order JSON file not found at {pending_json_path} for {market} {timeframe}"
        log_and_print(error_message, "ERROR")
        return False, error_message, "failed"

    # Read pendingorder.json
    try:
        with open(pending_json_path, 'r') as f:
            pending_data = json.load(f)
    except Exception as e:
        error_message = f"Error reading pending order JSON file at {pending_json_path} for {market} {timeframe}: {str(e)}"
        log_and_print(error_message, "ERROR")
        return False, error_message, "failed"

    # Check if pending_data is empty
    if not pending_data:
        error_message = f"No pending orders in pendingorder.json for {market} {timeframe}"
        log_and_print(error_message, "INFO")
        pricecandle_json_path = os.path.join(json_dir, "pricecandle.json")
        try:
            if os.path.exists(pricecandle_json_path):
                os.remove(pricecandle_json_path)
                log_and_print(f"Existing {pricecandle_json_path} deleted", "INFO")
            with open(pricecandle_json_path, 'w') as f:
                json.dump([], f, indent=4)
            log_and_print(f"Empty pricecandle.json saved for {market} {timeframe}", "INFO")
        except Exception as e:
            error_message = f"Error saving empty pricecandle.json for {market} {timeframe}: {str(e)}"
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
            log_and_print(f"No candle data found for sender position {sender_pos} in {market} {timeframe}", "WARNING")

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
            log_and_print(f"No candle data found for receiver position {receiver_pos} in {market} {timeframe}", "WARNING")

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
                log_and_print(f"No candle data found for order holder position {order_holder_pos} in {market} {timeframe}", "WARNING")
                matched_entry["order_holder"] = {
                    "label": order_holder_label,
                    "position_number": order_holder_pos
                }
        else:
            log_and_print(f"No order holder found for trendline in {market} {timeframe}", "WARNING")
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
                log_and_print(f"No candle data found for Breakout_parent position {breakout_parent_pos} in {market} {timeframe}", "WARNING")

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
                log_and_print(f"No candle data found for position {next_candle_pos} (right after Breakout_parent) in {market} {timeframe}", "WARNING")
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
            log_and_print(f"No Breakout_parent found or invalid for trendline in {market} {timeframe}", "WARNING")
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
        log_and_print(f"Matched pending order and candle data saved to {pricecandle_json_path} for {market} {timeframe}", "SUCCESS")
    except Exception as e:
        error_message = f"Error saving pricecandle.json for {market} {timeframe}: {str(e)}"
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

def candleatorderentry_from_candlesafterbreakoutparent(market: str, timeframe: str, json_dir: str) -> bool:
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
                log_and_print(f"No matching trendline found in candlesafterbreakoutparent.json for {trendline_type} with Breakout_parent_position {breakout_parent_pos} in {market} {timeframe}", "WARNING")
                pricecandle_trendline["executioner candle"] = {
                    "status": "No executioner candle"
                }
                updated_pricecandle_data.append(pricecandle_trendline)
                continue
            
            # Get order_type and Order_holder_entry from candlesafterbreakoutparent.json
            order_type = matching_cabp_trendline.get("trendline", {}).get("order_type")
            order_holder_entry = matching_cabp_trendline.get("trendline", {}).get("Order_holder_entry")
            
            # Validate order_type and order_holder_entry
            if order_type not in ["long", "short"] or order_holder_entry is None:
                log_and_print(f"Invalid order_type {order_type} or missing Order_holder_entry for trendline {trendline_type} in {market} {timeframe}", "WARNING")
                pricecandle_trendline["executioner candle"] = {
                    "status": "No executioner candle"
                }
                updated_pricecandle_data.append(pricecandle_trendline)
                continue
            
            # Validate order_holder_entry against pricecandle.json
            order_holder = pricecandle_trendline.get("order_holder", {})
            expected_entry = float(order_holder.get("Low", 0)) if order_type == "short" else float(order_holder.get("High", 0)) if order_type == "long" else None
            if expected_entry == 0 or abs(float(order_holder_entry) - expected_entry) > 0.0001:  # Small tolerance for floating-point comparison
                log_and_print(f"Mismatch in Order_holder_entry for trendline {trendline_type} in {market} {timeframe}. Expected: {expected_entry}, Got: {order_holder_entry}", "ERROR")
                pricecandle_trendline["executioner candle"] = {
                    "status": "No executioner candle due to Order_holder_entry mismatch"
                }
                updated_pricecandle_data.append(pricecandle_trendline)
                continue
            
            # Search for a matching candle
            matching_candle = None
            for candle in matching_cabp_trendline.get("candles", []):
                high_price = candle.get("High")
                low_price = candle.get("Low")
                
                # Check for a match based on order_type with strict validation
                if order_type == "short" and high_price is not None and order_holder_entry is not None:
                    if high_price >= order_holder_entry - 0.0001:  # Tolerance for floating-point precision
                        log_and_print(f"Short order match check: high_price={high_price}, order_holder_entry={order_holder_entry} for position {candle.get('position_number')} in {market} {timeframe}", "DEBUG")
                        matching_candle = {
                            "status": "Candle found at order holder entry level",
                            "position_number": candle.get("position_number"),
                            "Time": candle.get("Time"),
                            "Open": candle.get("Open"),
                            "High": high_price,
                            "Low": low_price,
                            "Close": candle.get("Close")
                        }
                        break
                elif order_type == "long" and low_price is not None and order_holder_entry is not None:
                    if low_price <= order_holder_entry + 0.0001:  # Tolerance for floating-point precision
                        log_and_print(f"Long order match check: low_price={low_price}, order_holder_entry={order_holder_entry} for position {candle.get('position_number')} in {market} {timeframe}", "DEBUG")
                        matching_candle = {
                            "status": "Candle found at order holder entry level",
                            "position_number": candle.get("position_number"),
                            "Time": candle.get("Time"),
                            "Open": candle.get("Open"),
                            "High": high_price,
                            "Low": low_price,
                            "Close": candle.get("Close")
                        }
                        break
            
            # Handle the current price candle (position 0) separately
            if not matching_candle:
                current_candle = next((c for c in matching_cabp_trendline.get("candles", []) if c.get("position_number") == 0), None)
                if current_candle:
                    high_price = current_candle.get("High")
                    low_price = current_candle.get("Low")
                    if order_type == "short" and high_price is not None and order_holder_entry is not None:
                        if high_price >= order_holder_entry - 0.0001:  # Tolerance for floating-point precision
                            log_and_print(f"Short order match check (current candle): high_price={high_price}, order_holder_entry={order_holder_entry} for position 0 in {market} {timeframe}", "DEBUG")
                            matching_candle = {
                                "status": "Candle found at order holder entry level",
                                "position_number": 0,
                                "Time": current_candle.get("Time"),
                                "Open": current_candle.get("Open"),
                                "High": high_price,
                                "Low": low_price,
                                "Close": None
                            }
                    elif order_type == "long" and low_price is not None and order_holder_entry is not None:
                        if low_price <= order_holder_entry + 0.0001:  # Tolerance for floating-point precision
                            log_and_print(f"Long order match check (current candle): low_price={low_price}, order_holder_entry={order_holder_entry} for position 0 in {market} {timeframe}", "DEBUG")
                            matching_candle = {
                                "status": "Candle found at order holder entry level",
                                "position_number": 0,
                                "Time": current_candle.get("Time"),
                                "Open": current_candle.get("Open"),
                                "High": high_price,
                                "Low": low_price,
                                "Close": None
                            }
            
            # Update pricecandle_trendline with the result
            if matching_candle:
                pricecandle_trendline["executioner candle"] = matching_candle
            else:
                pricecandle_trendline["executioner candle"] = {
                    "status": "No executioner candle"
                }
                log_and_print(f"No executioner candle found for trendline {trendline_type} in {market} {timeframe}. Order_type: {order_type}, Order_holder_entry: {order_holder_entry}", "INFO")
            
            updated_pricecandle_data.append(pricecandle_trendline)
        
        # Save updated pricecandle.json
        if os.path.exists(pricecandle_json_path):
            os.remove(pricecandle_json_path)
            log_and_print(f"Existing {pricecandle_json_path} deleted", "INFO")
        
        try:
            with open(pricecandle_json_path, 'w') as f:
                json.dump(updated_pricecandle_data, f, indent=4)
            log_and_print(f"Updated pricecandle.json with executioner candle for {market} {timeframe}", "SUCCESS")
            return True
        except Exception as e:
            log_and_print(f"Error saving updated pricecandle.json for {market} {timeframe}: {e}", "ERROR")
            return False
    
    except Exception as e:
        log_and_print(f"Error processing executioner candle for {market} {timeframe}: {e}", "ERROR")
        return False

def fetchlotsizeandriskallowed(market: str, timeframe: str, json_dir: str) -> bool:
    """Fetch lot size and allowed risk data from ciphercontracts_lotsizeandrisk table and save to lotsizeandrisk.json."""
    log_and_print(f"Fetching lot size and allowed risk data for market={market}, timeframe={timeframe}", "INFO")
    
    # Normalize market name for database query
    formatted_market_name = market.replace("'", "''")  # Escape single quotes for SQL
    
    # Map script timeframe to database timeframe
    db_timeframe = DB_TIMEFRAME_MAPPING.get(timeframe, timeframe)
    formatted_db_timeframe = db_timeframe.replace("'", "''")  # Escape single quotes for SQL
    
    # SQL query with embedded values (escaped to prevent SQL injection)
    sql_query = f"""
        SELECT id, market, pair, timeframe, lot_size, allowed_risk, created_at
        FROM ciphercontracts_lotsizeandrisk
        WHERE market = '{formatted_market_name}' AND timeframe = '{formatted_db_timeframe}'
    """
    
    # Define output JSON path
    output_json_path = os.path.join(json_dir, "lotsizeandrisk.json")
    
    # Execute query with retries
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = db.execute_query(sql_query)  # No params argument
            log_and_print(f"Raw query result for lot size and risk: {json.dumps(result, indent=2)}", "DEBUG")
            
            if not isinstance(result, dict):
                log_and_print(f"Invalid result format on attempt {attempt}: Expected dict, got {type(result)}", "ERROR")
                continue
                
            if result.get('status') != 'success':
                error_message = result.get('message', 'No message provided')
                log_and_print(f"Query failed on attempt {attempt} for {market} {timeframe}: {error_message}", "ERROR")
                continue
                
            # Handle both 'data' and 'results' keys
            rows = None
            if 'data' in result and 'rows' in result['data'] and isinstance(result['data']['rows'], list):
                rows = result['data']['rows']
            elif 'results' in result and isinstance(result['results'], list):
                rows = result['results']
            else:
                log_and_print(f"Invalid or missing rows in result on attempt {attempt} for {market} {timeframe}: {json.dumps(result, indent=2)}", "ERROR")
                continue
            
            # Normalize data and ensure proper types
            lot_size_risk_data = []
            for row in rows:
                lot_size_risk_data.append({
                    'id': int(row.get('id', 0)),
                    'market': row.get('market', 'N/A'),
                    'pair': row.get('pair', 'N/A'),
                    'timeframe': row.get('timeframe', 'N/A'),  # Keep database timeframe format in output
                    'lot_size': float(row.get('lot_size', 0.0)) if row.get('lot_size') is not None else None,
                    'allowed_risk': float(row.get('allowed_risk', 0.0)) if row.get('allowed_risk') is not None else None,
                    'created_at': row.get('created_at', 'N/A')
                })
            
            # Save to JSON
            if os.path.exists(output_json_path):
                os.remove(output_json_path)
                log_and_print(f"Existing {output_json_path} deleted", "INFO")
                
            try:
                with open(output_json_path, 'w') as f:
                    json.dump(lot_size_risk_data, f, indent=4)
                log_and_print(f"Lot size and allowed risk data saved to {output_json_path} for {market} {timeframe}", "SUCCESS")
                return True
            except Exception as e:
                log_and_print(f"Error saving lotsizeandrisk.json for {market} {timeframe}: {str(e)}", "ERROR")
                return False
                
        except Exception as e:
            log_and_print(f"Exception on attempt {attempt} for {market} {timeframe}: {str(e)}", "ERROR")
            
        if attempt < MAX_RETRIES:
            delay = RETRY_DELAY * (2 ** (attempt - 1))
            log_and_print(f"Retrying after {delay} seconds...", "INFO")
            time.sleep(delay)
        else:
            log_and_print(f"Max retries reached for fetching lot size and risk data for {market} {timeframe}", "ERROR")
            return False
    
    return False

def getorderholderpriceswithlotsizeandrisk(market: str, timeframe: str, json_dir: str) -> bool:
    """Fetch order holder prices, calculate exit and profit prices using lot size and allowed risk, and save to calculatedprices.json."""
    log_and_print(f"Calculating order holder prices with lot size and risk for market={market}, timeframe={timeframe}", "INFO")
    
    # Define file paths
    pricecandle_json_path = os.path.join(json_dir, "pricecandle.json")
    lotsizeandrisk_json_path = os.path.join(json_dir, "lotsizeandrisk.json")
    output_json_path = os.path.join(json_dir, "calculatedprices.json")
    
    # Check if pricecandle.json exists
    if not os.path.exists(pricecandle_json_path):
        log_and_print(f"pricecandle.json not found at {pricecandle_json_path} for {market} {timeframe}", "ERROR")
        return False
    
    # Check if lotsizeandrisk.json exists
    if not os.path.exists(lotsizeandrisk_json_path):
        log_and_print(f"lotsizeandrisk.json not found at {lotsizeandrisk_json_path} for {market} {timeframe}", "ERROR")
        return False
    
    try:
        # Load pricecandle.json
        with open(pricecandle_json_path, 'r') as f:
            pricecandle_data = json.load(f)
        
        # Load lotsizeandrisk.json
        with open(lotsizeandrisk_json_path, 'r') as f:
            lotsizeandrisk_data = json.load(f)
        
        # Initialize output data and deduplication set
        calculated_prices = []
        seen_order_holder_positions = set()  # Track unique order_holder_position to prevent duplicates
        
        # Process each trendline in pricecandle.json
        for trendline in pricecandle_data:
            order_holder = trendline.get("order_holder", {})
            order_type = trendline.get("receiver", {}).get("order_type", "").lower()
            trendline_type = trendline.get("type", "unknown")
            
            # Check if order holder is valid
            order_holder_position = order_holder.get("position_number")
            if order_holder.get("label", "none") == "none" or order_holder_position is None:
                log_and_print(f"No valid order holder for trendline {trendline_type} in {market} {timeframe}", "INFO")
                continue
            
            # Check for duplicate order_holder_position
            if order_holder_position in seen_order_holder_positions:
                log_and_print(
                    f"Duplicate order_holder_position {order_holder_position} detected for trendline {trendline_type} in {market} {timeframe}. Skipping.",
                    "WARNING"
                )
                continue
            seen_order_holder_positions.add(order_holder_position)
            
            # Validate order_type
            if order_type not in ["long", "short"]:
                log_and_print(f"Invalid order_type {order_type} for trendline {trendline_type} in {market} {timeframe}", "WARNING")
                continue
            
            # Get entry price based on order type
            entry_price = float(order_holder.get("Low", 0)) if order_type == "short" else float(order_holder.get("High", 0)) if order_type == "long" else None
            if entry_price == 0:
                log_and_print(f"No valid entry price for order holder in trendline {trendline_type} in {market} {timeframe}", "WARNING")
                continue
            
            # Find corresponding lot size and risk data
            matching_lot_size = None
            for lot_entry in lotsizeandrisk_data:
                if (lot_entry.get("market") == market and 
                    lot_entry.get("timeframe") == DB_TIMEFRAME_MAPPING.get(timeframe, timeframe)):
                    matching_lot_size = lot_entry
                    break
            
            if not matching_lot_size:
                log_and_print(f"No matching lot size and risk data found for {market} {timeframe} in trendline {trendline_type}", "WARNING")
                continue
            
            # Extract lot size and allowed risk
            lot_size = float(matching_lot_size.get("lot_size", 0))
            allowed_risk = float(matching_lot_size.get("allowed_risk", 0))
            if lot_size == 0 or allowed_risk == 0:
                log_and_print(f"Invalid lot_size {lot_size} or allowed_risk {allowed_risk} for trendline {trendline_type} in {market} {timeframe}", "WARNING")
                continue
            
            # Calculate pip value (assumes forex-like instruments; adjust if needed for indices)
            pip_value = 0.0001
            if "Index" in market:
                pip_value = 1.0  # Adjust for indices like Volatility 75 Index
            
            # Calculate price movement for allowed risk
            risk_in_pips = allowed_risk / (lot_size * pip_value)
            
            # Calculate exit_price and profit_price
            if order_type == "short":
                exit_price = entry_price + risk_in_pips * pip_value  # Stop loss above entry for short
                profit_price = entry_price - (3 * risk_in_pips * pip_value)  # 3:1 reward-to-risk ratio
            else:  # order_type == "long"
                exit_price = entry_price - risk_in_pips * pip_value  # Stop loss below entry for long
                profit_price = entry_price + (3 * risk_in_pips * pip_value)  # 3:1 reward-to-risk ratio
            
            # Round prices to appropriate decimal places
            decimal_places = 2 if "Index" in market else 5
            entry_price = round(entry_price, decimal_places)
            exit_price = round(exit_price, decimal_places)
            profit_price = round(profit_price, decimal_places)
            
            # Prepare calculated price entry
            calculated_entry = {
                "id": matching_lot_size.get("id"),
                "market": market,
                "pair": matching_lot_size.get("pair"),
                "timeframe": timeframe,  # Use script timeframe, not DB format
                "entry_price": entry_price,
                "exit_price": exit_price,
                "profit_price": profit_price,
                "order_type": "sell_limit" if order_type == "short" else "buy_limit",
                "trendline_type": trendline_type,
                "order_holder_position": order_holder_position
            }
            
            calculated_prices.append(calculated_entry)
            log_and_print(
                f"Calculated prices for trendline {trendline_type}: entry={entry_price}, exit={exit_price}, "
                f"profit={profit_price}, order_type={calculated_entry['order_type']} in {market} {timeframe}",
                "DEBUG"
            )
        
        # Save to calculatedprices.json
        if os.path.exists(output_json_path):
            os.remove(output_json_path)
            log_and_print(f"Existing {output_json_path} deleted", "INFO")
        
        if not calculated_prices:
            log_and_print(f"No valid calculated prices to save for {market} {timeframe}. Saving empty file.", "INFO")
            try:
                with open(output_json_path, 'w') as f:
                    json.dump([], f, indent=4)
                log_and_print(f"Empty calculatedprices.json saved to {output_json_path} for {market} {timeframe}", "SUCCESS")
                return True
            except Exception as e:
                log_and_print(f"Error saving empty calculatedprices.json for {market} {timeframe}: {str(e)}", "ERROR")
                return False
        
        try:
            with open(output_json_path, 'w') as f:
                json.dump(calculated_prices, f, indent=4)
            log_and_print(
                f"Saved {len(calculated_prices)} calculated price entries to {output_json_path} for {market} {timeframe}",
                "SUCCESS"
            )
            return True
        except Exception as e:
            log_and_print(f"Error saving calculatedprices.json for {market} {timeframe}: {str(e)}", "ERROR")
            return False
    
    except Exception as e:
        log_and_print(f"Error processing order holder prices for {market} {timeframe}: {str(e)}", "ERROR")
        return False

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

        # Fetch lot size and allowed risk data
        if not fetchlotsizeandriskallowed(market, timeframe, json_dir):
            error_message = f"Failed to fetch lot size and allowed risk data for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["fetchlotsizeandriskallowed"] = error_message
        else:
            output_json_path = os.path.join(json_dir, 'lotsizeandrisk.json')
            if os.path.exists(output_json_path):
                with open(output_json_path, 'r') as f:
                    lotsize_data = json.load(f)
                process_messages["fetchlotsizeandriskallowed"] = f"Fetched {len(lotsize_data)} lot size and risk records for {market} {timeframe}"
            else:
                process_messages["fetchlotsizeandriskallowed"] = f"No lot size and risk data saved for {market} {timeframe}"

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
        if not candleatorderentry_from_candlesafterbreakoutparent(market, timeframe, json_dir):
            error_message = f"Failed to process executioner candle for {market} {timeframe}"
            log_and_print(error_message, "ERROR")
            process_messages["candleatorderentry_from_candlesafterbreakoutparent"] = error_message
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
            process_messages["candleatorderentry_from_candlesafterbreakoutparent"] = (
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

        log_and_print(f"Completed processing market: {market}, timeframe: {timeframe}", "SUCCESS")
        return True, None, "success", process_messages

    except Exception as e:
        error_message = f"Unexpected error processing market {market} timeframe {timeframe}: {str(e)}"
        log_and_print(error_message, "ERROR")
        process_messages["error"] = error_message
        return False, error_message, "failed", process_messages
    finally:
        mt5.shutdown()

def save_status_json(success_data: List[Dict], no_pending_data: List[Dict], failed_data: List[Dict]) -> None:
    """Save all status data (success, no pending, failed) to marketsstatus.json with detailed process status messages."""
    successmarkets_path = os.path.join(BASE_OUTPUT_FOLDER, "marketsstatus.json")

    # Ensure the output directory exists
    os.makedirs(BASE_OUTPUT_FOLDER, exist_ok=True)

    # Calculate counts for summary
    total_processed = len(success_data) + len(no_pending_data) + len(failed_data)
    pending_order_missing = len(no_pending_data)

    # Prepare the summary section
    summary = {
        "processed_markets": total_processed,
        "pending_order_missing": pending_order_missing
    }

    # Combine all data into a single list for marketsstatus.json
    combined_data = []

    # Process successful entries
    for item in success_data:
        process_messages = item.get("process_messages", {})
        status_dict = {
            "fetch_candle_data": process_messages.get("fetch_candle_data", "No message"),
            "save_new_mostrecent_completed_candle": process_messages.get("save_new_mostrecent_completed_candle", "No message"),
            "match_mostrecent_candle": process_messages.get("match_mostrecent_candle", "No message"),
            "calculate_candles_inbetween": process_messages.get("calculate_candles_inbetween", "No message"),
            "fetchlotsizeandriskallowed": process_messages.get("fetchlotsizeandriskallowed", "No message"),
            "match_trendline_with_candle_data": process_messages.get("match_trendline_with_candle_data", "No message"),
            "candleafterbreakoutparent_to_currentprice": process_messages.get("candleafterbreakoutparent_to_currentprice", "No message"),
            "candleatorderentry_from_candlesafterbreakoutparent": process_messages.get("candleatorderentry_from_candlesafterbreakoutparent", "No message"),
            "getorderholderpriceswithlotsizeandrisk": process_messages.get("getorderholderpriceswithlotsizeandrisk", "No message")
        }
        combined_data.append({
            "market": item["market"],
            "timeframe": item["timeframe"],
            "processed_at": item["processed_at"],
            "status": status_dict,
            "overall_status": "success"
        })

    # Process no_pending_orders entries
    for item in no_pending_data:
        process_messages = item.get("process_messages", {})
        status_dict = {
            "fetch_candle_data": process_messages.get("fetch_candle_data", "No message"),
            "save_new_mostrecent_completed_candle": process_messages.get("save_new_mostrecent_completed_candle", "No message"),
            "match_mostrecent_candle": process_messages.get("match_mostrecent_candle", "No message"),
            "calculate_candles_inbetween": process_messages.get("calculate_candles_inbetween", "No message"),
            "fetchlotsizeandriskallowed": process_messages.get("fetchlotsizeandriskallowed", "No message"),
            "match_trendline_with_candle_data": item.get("message", "No pending orders found"),
            "candleafterbreakoutparent_to_currentprice": "Skipped due to no pending orders",
            "candleatorderentry_from_candlesafterbreakoutparent": "Skipped due to no pending orders",
            "getorderholderpriceswithlotsizeandrisk": "Skipped due to no pending orders"
        }
        combined_data.append({
            "market": item["market"],
            "timeframe": item["timeframe"],
            "processed_at": item["processed_at"],
            "status": status_dict,
            "overall_status": "no_pending_orders"
        })

    # Process failed entries
    for item in failed_data:
        process_messages = item.get("process_messages", {})
        status_dict = {
            "fetch_candle_data": process_messages.get("fetch_candle_data", "No message"),
            "save_new_mostrecent_completed_candle": process_messages.get("save_new_mostrecent_completed_candle", "No message"),
            "match_mostrecent_candle": process_messages.get("match_mostrecent_candle", "No message"),
            "calculate_candles_inbetween": process_messages.get("calculate_candles_inbetween", "No message"),
            "fetchlotsizeandriskallowed": process_messages.get("fetchlotsizeandriskallowed", "No message"),
            "match_trendline_with_candle_data": process_messages.get("match_trendline_with_candle_data", "No message"),
            "candleafterbreakoutparent_to_currentprice": process_messages.get("candleafterbreakoutparent_to_currentprice", "No message"),
            "candleatorderentry_from_candlesafterbreakoutparent": process_messages.get("candleatorderentry_from_candlesafterbreakoutparent", "No message"),
            "getorderholderpriceswithlotsizeandrisk": process_messages.get("getorderholderpriceswithlotsizeandrisk", "No message"),
            "error": item.get("error_message", "Unknown error")
        }
        combined_data.append({
            "market": item["market"],
            "timeframe": item["timeframe"],
            "processed_at": item["processed_at"],
            "status": status_dict,
            "overall_status": "failed"
        })

    # Combine summary and market data
    output_data = {
        "summary": summary,
        "markets": combined_data
    }

    # Save combined data to marketsstatus.json
    try:
        if os.path.exists(successmarkets_path):
            os.remove(successmarkets_path)
            log_and_print(f"Existing {successmarkets_path} deleted", "INFO")
        with open(successmarkets_path, 'w') as f:
            json.dump(output_data, f, indent=4)
        log_and_print(f"All markets saved to {successmarkets_path} ({len(combined_data)} entries)", "SUCCESS")
    except Exception as e:
        log_and_print(f"Error saving marketsstatus.json: {str(e)}", "ERROR")

def main():
    """Main function to process all markets and timeframes, saving all status to marketsstatus.json."""
    try:
        log_and_print("===== Fetch and Process Candle Data =====", "TITLE")
        
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
        
        # Save all status to marketsstatus.json
        save_status_json(success_data, no_pending_data, failed_data)

    except Exception as e:
        log_and_print(f"Error in main processing: {str(e)}", "ERROR")
    finally:
        log_and_print("===== Fetch and Process Candle Data Completed =====", "TITLE")

if __name__ == "__main__":
    main()
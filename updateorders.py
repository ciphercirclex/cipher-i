import json
import os
import multiprocessing
import time
from datetime import datetime
from typing import Dict, Optional, List, Tuple
import pandas as pd
import MetaTrader5 as mt5
import connectwithinfinitydb as db
from colorama import Fore, Style, init
import logging
from datetime import datetime, timezone

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
BASE_PROCESSING_FOLDER = r"C:\xampp\htdocs\CIPHER\cipher i\bouncestream\chart\processing\main"
BASE_OUTPUT_FOLDER = r"C:\xampp\htdocs\CIPHER\cipher i\bouncestream\chart\orders\main"
FETCHCHART_DESTINATION_PATH = r"C:\xampp\htdocs\CIPHER\cipher i\bouncestream\chart\fetched"

# Timeframe mapping
TIMEFRAME_MAPPING = {
    "M5": mt5.TIMEFRAME_M5,
    "M15": mt5.TIMEFRAME_M15,
    "M30": mt5.TIMEFRAME_M30,
    "H1": mt5.TIMEFRAME_H1,
    "H4": mt5.TIMEFRAME_H4
}

# Market classification for Deriv and Forex
DERIV_MARKETS = [
    "Volatility 75 Index", "Step Index", "Drift Switch Index 30",
    "Drift Switch Index 20", "Drift Switch Index 10", "Volatility 25 Index"
]
FOREX_MARKETS = [
    "AUDUSD", "XAUUSD", "US Tech 100", "Wall Street 30",
    "GBPUSD", "EURUSD", "USDJPY", "USDCAD", "USDCHF", "NZDUSD"
]

# Timeframe conversion to database format
TIMEFRAME_DB_MAPPING = {
    "M5": "5minutes",
    "M15": "15minutes",
    "M30": "30minutes",
    "H1": "1hour",
    "H4": "4hour"
}

# Order type mapping for database
ORDER_TYPE_DB_MAPPING = {
    "short": "sell_limit",
    "long": "buy_limit"
}

def clear_contracts_table():
    """Clear all existing data in cipherprogrammes_contracts table for relevant columns."""
    log_and_print("Clearing cipherprogrammes_contracts table", "INFO")
    
    # List all relevant columns to clear
    columns_to_clear = ['deriv_bouncestreamcontracts', 'forex_bouncestreamcontracts']
    clear_query = f"""
        UPDATE cipherprogrammes_contracts
        SET {', '.join(f'{col} = NULL' for col in columns_to_clear)}, timeframe = NULL, order_type = NULL
    """
    
    for attempt in range(1, MAX_RETRIES + 1):
        log_and_print(f"Executing clear query (attempt {attempt}/{MAX_RETRIES}): {clear_query}", "DEBUG")
        try:
            result = db.execute_query(clear_query)
            if result.get('status') == 'success':
                log_and_print("Successfully cleared cipherprogrammes_contracts table", "SUCCESS")
                return True
            else:
                log_and_print(f"Failed to clear cipherprogrammes_contracts table: {result.get('message', 'Unknown error')}", "ERROR")
        except Exception as e:
            log_and_print(f"Error executing clear query: {str(e)}", "ERROR")
        
        if attempt < MAX_RETRIES:
            delay = RETRY_DELAY * (2 ** (attempt - 1))
            log_and_print(f"Retrying clear query after {delay} seconds...", "INFO")
            time.sleep(delay)
        else:
            log_and_print("Max retries reached for clearing cipherprogrammes_contracts table. Aborting.", "ERROR")
            return False
    
    return False

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

def insert_order_to_database(market: str, timeframe: str, order_data: Dict) -> bool:
    """Insert order holder data into cipherprogrammes_contracts table in JavaScript-compatible format."""
    log_and_print(f"Inserting order data for market={market}, timeframe={timeframe}", "INFO")
    
    # Determine market type (Deriv or Forex)
    if market in DERIV_MARKETS:
        column = 'deriv_bouncestreamcontracts'
        broker = 'deriv'
    elif market in FOREX_MARKETS:
        column = 'forex_bouncestreamcontracts'
        broker = 'forex'
    else:
        log_and_print(f"Market {market} not recognized as Deriv or Forex", "ERROR")
        return False

    # Convert timeframe to database format
    db_timeframe = TIMEFRAME_DB_MAPPING.get(timeframe)
    if not db_timeframe:
        log_and_print(f"Invalid timeframe {timeframe} for database insertion", "ERROR")
        return False

    # Extract order details
    order_type = order_data.get('receiver', {}).get('order_type', '').lower()
    if order_type not in ['long', 'short']:
        log_and_print(f"Invalid order type {order_type} for market={market}, timeframe={timeframe}", "ERROR")
        return False

    # Map order type to database format
    db_order_type = ORDER_TYPE_DB_MAPPING.get(order_type)
    if not db_order_type:
        log_and_print(f"Invalid order type mapping for {order_type} for market={market}, timeframe={timeframe}", "ERROR")
        return False

    # Extract order details
    candle = order_data.get('order_holder', {})
    if not candle:
        log_and_print(f"No order holder candle data for market={market}, timeframe={timeframe}", "ERROR")
        return False

    entry_price = float(candle.get('Low', 0)) if order_type == 'short' else float(candle.get('High', 0))
    exit_price = float(candle.get('High', 0)) if order_type == 'short' else float(candle.get('Low', 0))
    
    # Set exit limit price (example: 1% above/below exit price)
    exit_limit_price = exit_price * 1.01 if order_type == 'long' else exit_price * 0.99

    # Format contract data to match JavaScript's formatContractData
    contract_type = 'bouncestream'  # Since this script handles bouncestream contracts
    contract_data = f"contract type: {contract_type}, market name: {market}, timeframe: {db_timeframe}, order type: {db_order_type}, entry price: {entry_price:.4f}, exit price: {exit_price:.4f}, exit-limit price: {exit_limit_price:.4f}"
    
    # Prepare SQL query for insertion
    contract_data_escaped = contract_data.replace("'", "''")  # Escape single quotes for SQL
    db_order_type_escaped = db_order_type.replace("'", "''")  # Escape single quotes for SQL
    sql_query = f"""
        INSERT INTO cipherprogrammes_contracts ({column}, timeframe, order_type, created_at)
        VALUES ('{contract_data_escaped}', '{db_timeframe}', '{db_order_type_escaped}', NOW())
    """

    # Execute insert query with retries
    for attempt in range(1, MAX_RETRIES + 1):
        log_and_print(f"Executing insert query (attempt {attempt}/{MAX_RETRIES}) for market={market}, timeframe={timeframe}: {sql_query}", "DEBUG")
        try:
            result = db.execute_query(sql_query)
            if result.get('status') == 'success':
                if isinstance(result.get('results'), dict) and result.get('results', {}).get('affected_rows', 0) > 0:
                    log_and_print(f"Successfully inserted order for market={market}, timeframe={timeframe}: {contract_data}", "SUCCESS")
                    return True
                elif isinstance(result.get('results'), list) and len(result.get('results')) > 0:
                    log_and_print(f"Unexpected results format for INSERT query for market={market}, timeframe={timeframe}: {result}", "WARNING")
                    return True  # Treat as success if query executed without errors
                else:
                    log_and_print(f"Query executed but no rows affected for market={market}, timeframe={timeframe}: {result.get('message', 'No message')}", "WARNING")
            else:
                log_and_print(f"Failed to insert order for {market}, timeframe={timeframe}: {result.get('message', 'Unknown error')}", "ERROR")
        except Exception as e:
            log_and_print(f"Error executing insert query for {market}, timeframe={timeframe}: {str(e)}", "ERROR")
        
        if attempt < MAX_RETRIES:
            delay = RETRY_DELAY * (2 ** (attempt - 1))
            log_and_print(f"Retrying insert query after {delay} seconds...", "INFO")
            time.sleep(delay)
        else:
            log_and_print(f"Max retries reached for inserting data in {market} {timeframe}. Insertion failed.", "ERROR")
            return False

    return False

def match_trendline_with_candle_data(candle_data: Dict, json_dir: str, market: str, timeframe: str) -> bool:
    """Match pending order data with candle data, save to pricecandle.json, and insert order holder to database."""
    formatted_market_name = market.replace(" ", "_")
    pending_json_path = os.path.join(BASE_PROCESSING_FOLDER, formatted_market_name, timeframe.lower(), "pendingorder.json")

    if not os.path.exists(pending_json_path):
        log_and_print(f"Pending order JSON file not found: {pending_json_path} for {market} {timeframe}", "WARNING")
        return False

    try:
        with open(pending_json_path, 'r') as f:
            pending_data = json.load(f)
    except Exception as e:
        log_and_print(f"Error reading pending order JSON file for {market} {timeframe}: {e}", "ERROR")
        return False

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

            # Fetch the candle right after Breakout_parent (position_number - 1)
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

    if not matched_data:
        log_and_print(f"pricecandle.json is empty for {market} {timeframe}. Skipping database insertion.", "WARNING")
        try:
            with open(pricecandle_json_path, 'w') as f:
                json.dump(matched_data, f, indent=4)
            log_and_print(f"Empty pricecandle.json saved for {market} {timeframe}", "INFO")
        except Exception as e:
            log_and_print(f"Error saving empty pricecandle.json for {market} {timeframe}: {e}", "ERROR")
            return False
        return False

    try:
        with open(pricecandle_json_path, 'w') as f:
            json.dump(matched_data, f, indent=4)
        log_and_print(f"Matched pending order and candle data saved to {pricecandle_json_path} for {market} {timeframe}", "SUCCESS")
    except Exception as e:
        log_and_print(f"Error saving pricecandle.json for {market} {timeframe}: {e}", "ERROR")
        return False

    # Insert orders to database only if matched_data is not empty
    for matched_entry in matched_data:
        if matched_entry.get("order_holder", {}).get("position_number") is not None:
            success = insert_order_to_database(market, timeframe, matched_entry)
            if not success:
                log_and_print(f"Failed to insert order for {market} {timeframe}", "ERROR")
        else:
            log_and_print(f"Skipping database insertion for {market} {timeframe} due to missing order holder", "WARNING")

    return True

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
                order_type = None  # Set to None to indicate invalid order type
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
                num_candles = start_pos - 1  # From start_pos to position 1
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
            
            if order_type not in ["long", "short"] or order_holder_entry is None:
                log_and_print(f"Invalid order_type {order_type} or missing Order_holder_entry for trendline {trendline_type} in {market} {timeframe}", "WARNING")
                pricecandle_trendline["executioner candle"] = {
                    "status": "No executioner candle"
                }
                updated_pricecandle_data.append(pricecandle_trendline)
                continue
            
            # Search for a matching candle
            matching_candle = None
            for candle in matching_cabp_trendline.get("candles", []):
                high_price = candle.get("High")
                low_price = candle.get("Low")
                
                # Check for a match based on order_type
                if order_type == "short" and high_price is not None and order_holder_entry is not None:
                    if high_price >= order_holder_entry:
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
                    if low_price <= order_holder_entry:
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
                        if high_price >= order_holder_entry:
                            matching_candle = {
                                "status": "Candle found at order holder entry level",
                                "position_number": 0,
                                "Time": current_candle.get("Time"),
                                "Open": current_candle.get("Open"),
                                "High": high_price,
                                "Low": low_price,
                                "Close": None  # Current candle may not have a close price
                            }
                    elif order_type == "long" and low_price is not None and order_holder_entry is not None:
                        if low_price <= order_holder_entry:
                            matching_candle = {
                                "status": "Candle found at order holder entry level",
                                "position_number": 0,
                                "Time": current_candle.get("Time"),
                                "Open": current_candle.get("Open"),
                                "High": high_price,
                                "Low": low_price,
                                "Close": None  # Current candle may not have a close price
                            }
            
            # Update pricecandle_trendline with the result
            if matching_candle:
                pricecandle_trendline["executioner candle"] = matching_candle
            else:
                pricecandle_trendline["executioner candle"] = {
                    "status": "No executioner candle"
                }
            
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

def fetch_lot_size_and_risk(market: str, timeframe: str) -> tuple[Optional[float], Optional[float]]:
    """Fetch lot size and allowed risk from ciphercontracts_lotsizeandrisk table."""
    log_and_print(f"Fetching lot size and allowed risk for market={market}, timeframe={timeframe}", "INFO")
    
    # Convert timeframe to database format
    db_timeframe = TIMEFRAME_DB_MAPPING.get(timeframe, timeframe)
    
    # Prepare SQL query
    sql_query = f"""
        SELECT lot_size, allowed_risk
        FROM ciphercontracts_lotsizeandrisk
        WHERE market = '{market}' AND timeframe = '{db_timeframe}'
    """
    
    try:
        # Execute query using connectwithinfinitydb
        result = db.execute_query(sql_query)
        if result.get('status') == 'success' and result.get('results'):
            # Assuming the query returns a list of dictionaries
            record = result['results'][0]
            lot_size = float(record.get('lot_size')) if record.get('lot_size') is not None else None
            allowed_risk = float(record.get('allowed_risk')) if record.get('allowed_risk') is not None else None
            
            if lot_size is None or allowed_risk is None:
                log_and_print(f"No lot_size or allowed_risk found for {market} {timeframe}", "ERROR")
                return None, None
            
            log_and_print(f"Retrieved lot_size={lot_size}, allowed_risk={allowed_risk} for {market} {timeframe}", "SUCCESS")
            return lot_size, allowed_risk
        else:
            log_and_print(f"No records found for {market} {timeframe} in ciphercontracts_lotsizeandrisk table", "ERROR")
            return None, None
    except Exception as e:
        log_and_print(f"Error fetching lot size and risk for {market} {timeframe}: {str(e)}", "ERROR")
        return None, None

def calculatemarkettimeframescontractspricewiththeirlotsizes(market: str, timeframe: str, json_dir: str) -> bool:
    """Calculate contract prices with lot sizes from database and save to contractsorders.json."""
    log_and_print(f"Calculating contract prices with lot sizes for market={market}, timeframe={timeframe}", "INFO")
    
    # Define file paths
    candlesafterbreakoutparent_json_path = os.path.join(json_dir, "candlesafterbreakoutparent.json")
    pricecandle_json_path = os.path.join(json_dir, "pricecandle.json")
    contractsorders_json_path = os.path.join(json_dir, "contractsorders.json")
    
    # Check if required JSON files exist
    if not os.path.exists(candlesafterbreakoutparent_json_path):
        log_and_print(f"candlesafterbreakoutparent.json not found at {candlesafterbreakoutparent_json_path} for {market} {timeframe}", "ERROR")
        return False
    if not os.path.exists(pricecandle_json_path):
        log_and_print(f"pricecandle.json not found at {pricecandle_json_path} for {market} {timeframe}", "ERROR")
        return False
    
    try:
        # Load candlesafterbreakoutparent.json
        with open(candlesafterbreakoutparent_json_path, 'r') as f:
            cabp_data = json.load(f)
        
        # Load pricecandle.json to check for executioner candle
        with open(pricecandle_json_path, 'r') as f:
            pricecandle_data = json.load(f)
        
        # Fetch lot size and allowed risk from database
        lot_size, allowed_risk = fetch_lot_size_and_risk(market, timeframe)
        if lot_size is None or allowed_risk is None:
            log_and_print(f"Failed to retrieve lot size or allowed risk for {market} {timeframe}", "ERROR")
            return False
        
        # Prepare output data
        contracts_orders = []
        
        # Process each trendline in candlesafterbreakoutparent.json
        for cabp_trendline, pricecandle_trendline in zip(cabp_data, pricecandle_data):
            trendline_info = cabp_trendline.get("trendline", {})
            order_type = trendline_info.get("order_type")
            order_holder_entry = trendline_info.get("Order_holder_entry")
            breakout_parent_pos = trendline_info.get("Breakout_parent_position")
            
            if order_type not in ["long", "short"] or order_holder_entry is None:
                log_and_print(f"Invalid order_type {order_type} or missing Order_holder_entry for trendline {trendline_info.get('type')} in {market} {timeframe}", "WARNING")
                continue
            
            # Get executioner candle status from pricecandle.json
            executioner_candle = pricecandle_trendline.get("executioner candle", {})
            contract_status = "pending order" if executioner_candle.get("status") == "No executioner candle" else "executed"
            
            # Calculate new exit price
            # Risk = lot_size * |entry_price - exit_price| = allowed_risk
            # For short: exit_price = entry_price + (allowed_risk / lot_size)
            # For long: exit_price = entry_price - (allowed_risk / lot_size)
            entry_price = float(order_holder_entry)
            if order_type == "short":
                exit_price = entry_price + (allowed_risk / lot_size)
            else:  # long
                exit_price = entry_price - (allowed_risk / lot_size)
            
            # Get order holder data from pricecandle.json for timestamp and position
            order_holder = pricecandle_trendline.get("order_holder", {})
            position_number = order_holder.get("position_number")
            timestamp = order_holder.get("Time")
            
            # Structure the contract data
            contract = {
                "order_holder": {
                    "timeframe": timeframe,
                    "position_number": position_number,
                    "timestamp": timestamp,
                    "order_type": order_type,
                    "entry_price": round(entry_price, 4),
                    "lotsize": lot_size,
                    "exit_price": round(exit_price, 4),
                    "contract_status": contract_status
                }
            }
            contracts_orders.append(contract)
        
        # Save to contractsorders.json
        if os.path.exists(contractsorders_json_path):
            os.remove(contractsorders_json_path)
            log_and_print(f"Existing {contractsorders_json_path} deleted", "INFO")
        
        if not contracts_orders:
            log_and_print(f"No contracts data to save for {market} {timeframe}. Saving empty contractsorders.json", "WARNING")
            try:
                with open(contractsorders_json_path, 'w') as f:
                    json.dump(contracts_orders, f, indent=4)
                log_and_print(f"Empty contractsorders.json saved to {contractsorders_json_path} for {market} {timeframe}", "INFO")
                return True
            except Exception as e:
                log_and_print(f"Error saving empty contractsorders.json for {market} {timeframe}: {e}", "ERROR")
                return False
        
        try:
            with open(contractsorders_json_path, 'w') as f:
                json.dump(contracts_orders, f, indent=4)
            log_and_print(f"Contracts orders saved to {contractsorders_json_path} for {market} {timeframe}", "SUCCESS")
            return True
        except Exception as e:
            log_and_print(f"Error saving contractsorders.json for {market} {timeframe}: {e}", "ERROR")
            return False
    
    except Exception as e:
        log_and_print(f"Error processing contracts orders for {market} {timeframe}: {e}", "ERROR")
        return False

def process_market_timeframe(market: str, timeframe: str) -> bool:
    """Process a single market and timeframe combination."""
    try:
        log_and_print(f"Processing market: {market}, timeframe: {timeframe}", "INFO")
        candle_data, json_dir = fetch_candle_data(market, timeframe)
        if candle_data is None or json_dir is None:
            log_and_print(f"Skipping {market} {timeframe} due to failure in fetching candle data", "ERROR")
            return False

        # Save the most recent completed candle
        if not save_new_mostrecent_completed_candle(market, timeframe, json_dir):
            log_and_print(f"Failed to save most recent completed candle for {market} {timeframe}", "ERROR")
            # Continue processing even if this fails, as it may not be critical

        # Match most recent completed candle with candle data
        if not match_mostrecent_candle(market, timeframe, json_dir):
            log_and_print(f"Failed to match most recent completed candle for {market} {timeframe}", "ERROR")
            # Continue processing even if matching fails, as it may not be critical

        # Calculate candles in between
        if not calculate_candles_inbetween(market, timeframe, json_dir):
            log_and_print(f"Failed to calculate candles in between for {market} {timeframe}", "ERROR")
            # Continue processing even if this fails, as it may not be critical

        # Match trendline with candle data
        success = match_trendline_with_candle_data(candle_data, json_dir, market, timeframe)
        if not success:
            log_and_print(f"Failed to process pending orders for {market} {timeframe}", "ERROR")
            return False

        # Fetch candles from after Breakout_parent to current price
        if not candleafterbreakoutparent_to_currentprice(market, timeframe, json_dir):
            log_and_print(f"Failed to fetch candles after Breakout_parent for {market} {timeframe}", "ERROR")
            # Continue processing even if this fails, as it may not be critical

        # Search for executioner candle and update pricecandle.json
        if not candleatorderentry_from_candlesafterbreakoutparent(market, timeframe, json_dir):
            log_and_print(f"Failed to process executioner candle for {market} {timeframe}", "ERROR")
            # Continue processing even if this fails, as it may not be critical

        # Calculate contract prices with lot sizes using database
        if not calculatemarkettimeframescontractspricewiththeirlotsizes(market, timeframe, json_dir):
            log_and_print(f"Failed to calculate contract prices with lot sizes for {market} {timeframe}", "ERROR")
            # Continue processing even if this fails, as it may not be critical

        log_and_print(f"Completed processing market: {market}, timeframe: {timeframe}", "SUCCESS")
        return True

    except Exception as e:
        log_and_print(f"Error processing market {market} timeframe {timeframe}: {str(e)}", "ERROR")
        return False
    finally:
        mt5.shutdown()


def main():
    """Main function to process all markets and timeframes."""
    try:
        log_and_print("===== Fetch and Insert Orders Process =====", "TITLE")
        
        # Clear the contracts table before processing signals
        if not clear_contracts_table():
            log_and_print("Aborting signal processing due to failure in clearing contracts table", "ERROR")
            return
        
        tasks = [(market, timeframe) for market in MARKETS for timeframe in TIMEFRAMES]
        with multiprocessing.Pool(processes=4) as pool:  # Limit to 4 processes to avoid resource contention
            results = pool.starmap(process_market_timeframe, tasks)
        success_count = sum(1 for result in results if result)
        log_and_print(f"Processing completed: {success_count}/{len(tasks)} market-timeframe combinations processed successfully", "INFO")
    except Exception as e:
        log_and_print(f"Error in main processing: {str(e)}", "ERROR")
    finally:
        db.shutdown()  # Ensure browser and session cleanup
        log_and_print("===== Fetch and Insert Orders Process Completed =====", "TITLE")

if __name__ == "__main__":
    main()
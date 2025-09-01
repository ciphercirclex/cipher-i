import json
import time
import os
import multiprocessing
from typing import Dict, Optional
import MetaTrader5 as mt5
import pandas as pd
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
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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

# Base path for pricecandle.json files
BASE_OUTPUT_FOLDER = r"C:\xampp\htdocs\CIPHER\cipher i\bouncestream\chart\orders\main"

# Timeframe mapping
TIMEFRAME_MAPPING = {
    "M5": mt5.TIMEFRAME_M5,
    "M15": mt5.TIMEFRAME_M15,
    "M30": mt5.TIMEFRAME_M30,
    "H1": mt5.TIMEFRAME_H1,
    "H4": mt5.TIMEFRAME_H4
}

def initialize_mt5():
    """Initialize MT5 terminal and login."""
    log_and_print("Initializing MT5 terminal", "INFO")
    
    # Ensure no existing MT5 connections interfere
    mt5.shutdown()

    # Initialize MT5 terminal with explicit path and timeout
    for attempt in range(MAX_RETRIES):
        if mt5.initialize(path=TERMINAL_PATH, timeout=60000):
            log_and_print("Successfully initialized MT5 terminal", "SUCCESS")
            break
        error_code, error_message = mt5.last_error()
        log_and_print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to initialize MT5 terminal. Error: {error_code}, {error_message}", "ERROR")
        time.sleep(RETRY_DELAY)
    else:
        log_and_print(f"Failed to initialize MT5 terminal after {MAX_RETRIES} attempts", "ERROR")
        return False

    # Wait for terminal to be fully ready
    for _ in range(5):
        if mt5.terminal_info() is not None:
            log_and_print("MT5 terminal fully initialized", "DEBUG")
            break
        log_and_print("Waiting for MT5 terminal to fully initialize...", "INFO")
        time.sleep(2)
    else:
        log_and_print("MT5 terminal not ready", "ERROR")
        mt5.shutdown()
        return False

    # Attempt login with retries
    for attempt in range(MAX_RETRIES):
        if mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=60000):
            log_and_print("Successfully logged in to MT5", "SUCCESS")
            return True
        error_code, error_message = mt5.last_error()
        log_and_print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to log in to MT5. Error code: {error_code}, Message: {error_message}", "ERROR")
        time.sleep(RETRY_DELAY)
    else:
        log_and_print(f"Failed to log in to MT5 after {MAX_RETRIES} attempts", "ERROR")
        mt5.shutdown()
        return False

def fetch_candles_after_breakout(market: str, timeframe: str, breakout_time: str, num_candles: int = 1000) -> Optional[pd.DataFrame]:
    """Fetch candles after the Breakout_parent candle's time."""
    log_and_print(f"Fetching candles for {market} {timeframe} after {breakout_time}", "INFO")

    # Select market symbol
    if not mt5.symbol_select(market, True):
        log_and_print(f"Failed to select market: {market}, error: {mt5.last_error()}", "ERROR")
        return None

    # Get timeframe
    mt5_timeframe = TIMEFRAME_MAPPING.get(timeframe)
    if not mt5_timeframe:
        log_and_print(f"Invalid timeframe {timeframe} for {market}", "ERROR")
        return None

    # Parse breakout time
    try:
        breakout_datetime = datetime.strptime(breakout_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError as e:
        log_and_print(f"Invalid breakout time format for {market} {timeframe}: {breakout_time}, error: {e}", "ERROR")
        return None

    # Fetch candles starting from breakout time
    candles = mt5.copy_rates_from(market, mt5_timeframe, breakout_datetime, num_candles)
    if candles is None or len(candles) == 0:
        log_and_print(f"Failed to fetch candles for {market} {timeframe} after {breakout_time}, error: {mt5.last_error()}", "ERROR")
        return None

    df = pd.DataFrame(candles)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    return df


def process_pricecandle_json(market: str, timeframe: str) -> bool:
    """Process pricecandle.json for a given market and timeframe."""
    try:
        formatted_market_name = market.replace(" ", "_")
        json_path = os.path.join(BASE_OUTPUT_FOLDER, formatted_market_name, timeframe.lower(), "pricecandle.json")
        
        log_and_print(f"Processing pricecandle.json for {market} {timeframe} at {json_path}", "INFO")
        
        if not os.path.exists(json_path):
            log_and_print(f"pricecandle.json not found for {market} {timeframe} at {json_path}", "WARNING")
            return False

        # Read pricecandle.json
        try:
            with open(json_path, 'r') as f:
                pricecandle_data = json.load(f)
        except Exception as e:
            log_and_print(f"Error reading pricecandle.json for {market} {timeframe}: {e}", "ERROR")
            return False

        if not pricecandle_data:
            log_and_print(f"pricecandle.json is empty for {market} {timeframe}", "WARNING")
            return False

        # Initialize MT5 for this market
        if not initialize_mt5():
            log_and_print(f"Failed to initialize MT5 for {market} {timeframe}", "ERROR")
            return False

        try:
            modified = False
            for contract in pricecandle_data:
                # Check if Executioner_candle already exists
                if "Executioner_candle" in contract:
                    log_and_print(f"Executioner_candle already exists in contract for {market} {timeframe}, skipping", "INFO")
                    continue

                # Get order holder and breakout parent details
                order_holder = contract.get("order_holder", {})
                breakout_parent = contract.get("Breakout_parent", {})
                order_type = contract.get("receiver", {}).get("order_type", "").lower()

                if not order_holder.get("High") or not order_holder.get("Low") or not breakout_parent.get("Time"):
                    log_and_print(f"Missing order_holder or breakout_parent data for {market} {timeframe}", "WARNING")
                    contract["Executioner_candle"] = {"Executioner": "no execution yet"}
                    modified = True
                    continue

                order_holder_high = float(order_holder.get("High"))
                order_holder_low = float(order_holder.get("Low"))
                breakout_time = breakout_parent.get("Time")

                # Fetch candles after breakout time
                candles_df = fetch_candles_after_breakout(market, timeframe, breakout_time)
                if candles_df is None or candles_df.empty:
                    log_and_print(f"No candles fetched after breakout for {market} {timeframe}", "WARNING")
                    contract["Executioner_candle"] = {"Executioner": "no execution yet"}
                    modified = True
                    continue

                # Search for executioner candle
                executioner_candle = None
                for index, candle in candles_df.iterrows():
                    candle_time = candle['time'].strftime("%Y-%m-%d %H:%M:%S")
                    # Skip the breakout candle itself (if it's the first candle)
                    if candle_time == breakout_time:
                        continue

                    candle_high = float(candle['high'])
                    candle_low = float(candle['low'])

                    # Check execution condition based on order type
                    if order_type == "long" and candle_low <= order_holder_high:
                        executioner_candle = {
                            "Executioner": "executed order holder level",
                            "position_number": None,  # Will be updated later
                            "Time": candle_time,
                            "Open": float(candle['open']),
                            "High": candle_high,
                            "Low": candle_low,
                            "Close": float(candle['close'])
                        }
                        break
                    elif order_type == "short" and candle_high >= order_holder_low:
                        executioner_candle = {
                            "Executioner": "executed order holder level",
                            "position_number": None,  # Will be updated later
                            "Time": candle_time,
                            "Open": float(candle['open']),
                            "High": candle_high,
                            "Low": candle_low,
                            "Close": float(candle['close'])
                        }
                        break

                if executioner_candle:
                    # Determine position number by fetching recent candles and finding the matching candle
                    recent_candles = mt5.copy_rates_from_pos(market, TIMEFRAME_MAPPING[timeframe], 1, 300)
                    if recent_candles is None or len(recent_candles) == 0:
                        log_and_print(f"Failed to fetch recent candles to determine position for {market} {timeframe}", "ERROR")
                        contract["Executioner_candle"] = {"Executioner": "no execution yet"}
                        modified = True
                        continue

                    recent_candles_df = pd.DataFrame(recent_candles)
                    recent_candles_df['time'] = pd.to_datetime(recent_candles_df['time'], unit='s')
                    for idx, recent_candle in recent_candles_df.iterrows():
                        recent_candle_time = recent_candle['time'].strftime("%Y-%m-%d %H:%M:%S")
                        if recent_candle_time == executioner_candle["Time"]:
                            executioner_candle["position_number"] = 300 - idx
                            break
                    else:
                        log_and_print(f"Could not determine position number for executioner candle in {market} {timeframe}", "WARNING")
                        executioner_candle["position_number"] = None

                    contract["Executioner_candle"] = executioner_candle
                    log_and_print(f"Executioner candle found for {market} {timeframe} at {executioner_candle['Time']}", "SUCCESS")
                    modified = True
                else:
                    contract["Executioner_candle"] = {"Executioner": "no execution yet"}
                    log_and_print(f"No executioner candle found for {market} {timeframe}", "INFO")
                    modified = True

            # Save updated pricecandle.json if modified
            if modified:
                try:
                    with open(json_path, 'w') as f:
                        json.dump(pricecandle_data, f, indent=4)
                    log_and_print(f"Updated pricecandle.json with Executioner_candle for {market} {timeframe}", "SUCCESS")
                except Exception as e:
                    log_and_print(f"Error saving updated pricecandle.json for {market} {timeframe}: {e}", "ERROR")
                    return False

            return True

        finally:
            mt5.shutdown()

    except Exception as e:
        log_and_print(f"Error processing pricecandle.json for {market} {timeframe}: {str(e)}", "ERROR")
        return False

def main():
    """Main function to process all markets and timeframes."""
    try:
        log_and_print("===== Orders Records Process =====", "TITLE")
        
        tasks = [(market, timeframe) for market in MARKETS for timeframe in TIMEFRAMES]
        with multiprocessing.Pool(processes=4) as pool:  # Limit to 4 processes to avoid resource contention
            results = pool.starmap(process_pricecandle_json, tasks)
        success_count = sum(1 for result in results if result)
        log_and_print(f"Processing completed: {success_count}/{len(tasks)} market-timeframe combinations processed successfully", "INFO")
    
    except Exception as e:
        log_and_print(f"Error in main processing: {str(e)}", "ERROR")
    
    finally:
        log_and_print("===== Orders Records Process Completed =====", "TITLE")

if __name__ == "__main__":
    main()
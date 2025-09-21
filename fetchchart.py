from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import logging
import time
import os
import shutil
import cv2
import numpy as np
import json
import multiprocessing
import MetaTrader5 as mt5
from datetime import datetime, timedelta
import pytz

# Configure Logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] [%(processName)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Suppress WebDriver and other logs
for name in ['webdriver_manager', 'selenium', 'urllib3', 'selenium.webdriver', 'tensorflow']:
    logging.getLogger(name).setLevel(logging.WARNING)

# Configuration
MARKETS_JSON_PATH = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\base.json"

# Function to load markets, timeframes, and credentials from JSON
def load_markets_and_credentials(json_path):
    """Load markets, timeframes, credentials, and additional subjects from base.json file."""
    try:
        if not os.path.exists(json_path):
            raise FileNotFoundError(f"Markets JSON file not found at: {json_path}")
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        # Load all subjects from JSON
        markets = data.get("MARKETS", [])
        forex_markets = data.get("FOREX_MARKETS", [])
        forex_majors = data.get("FOREX_MAJORS", [])
        forex_minors = data.get("FOREX_MINORS", [])
        synthetic_indices = data.get("SYNTHETIC_INDICES", [])
        drift_switching_indices = data.get("DRIFT_SWITCHING_INDICES", [])
        multi_step_indices = data.get("MULTI_STEP_INDICES", [])
        skewed_step_indices = data.get("SKEWED_STEP_INDICES", [])
        trek_indices = data.get("TREK_INDICES", [])
        tactical_indices = data.get("TACTICAL_INDICES", [])
        basket_indices = data.get("BASKET_INDICES", [])
        crypto = data.get("CRYPTO", [])
        index_markets = data.get("INDEX_MARKETS", [])
        commodities = data.get("COMMODITIES", [])
        timeframes = data.get("TIMEFRAMES", [])
        credentials = data.get("CREDENTIALS", {})
        
        # Extract credentials
        login_id = credentials.get("LOGIN_ID", "")
        password = credentials.get("PASSWORD", "")
        server = credentials.get("SERVER", "")
        base_url = credentials.get("BASE_URL", "")
        terminal_path = credentials.get("TERMINAL_PATH", "")
        
        # Validate required fields
        if not all([markets, timeframes, login_id, password, server, base_url, terminal_path]):
            raise ValueError("MARKETS, TIMEFRAMES, or CREDENTIALS not found in base.json or are empty")
        
        # Log loaded data (excluding sensitive credentials)
        logger.debug(f"Loaded MARKETS: {markets}")
        logger.debug(f"Loaded FOREX_MARKETS: {forex_markets}")
        logger.debug(f"Loaded FOREX_MAJORS: {forex_majors}")
        logger.debug(f"Loaded FOREX_MINORS: {forex_minors}")
        logger.debug(f"Loaded SYNTHETIC_INDICES: {synthetic_indices}")
        logger.debug(f"Loaded DRIFT_SWITCHING_INDICES: {drift_switching_indices}")
        logger.debug(f"Loaded MULTI_STEP_INDICES: {multi_step_indices}")
        logger.debug(f"Loaded SKEWED_STEP_INDICES: {skewed_step_indices}")
        logger.debug(f"Loaded TREK_INDICES: {trek_indices}")
        logger.debug(f"Loaded TACTICAL_INDICES: {tactical_indices}")
        logger.debug(f"Loaded BASKET_INDICES: {basket_indices}")
        logger.debug(f"Loaded CRYPTO: {crypto}")
        logger.debug(f"Loaded INDEX_MARKETS: {index_markets}")
        logger.debug(f"Loaded COMMODITIES: {commodities}")
        logger.debug(f"Loaded TIMEFRAMES: {timeframes}")
        logger.debug("Loaded CREDENTIALS: [Sensitive data not logged]")
        
        # Return all loaded data
        return (markets, forex_markets, forex_majors, forex_minors, synthetic_indices,
                drift_switching_indices, multi_step_indices, skewed_step_indices,
                trek_indices, tactical_indices, basket_indices, crypto, index_markets,
                commodities, timeframes, login_id, password, server, base_url, terminal_path)
    
    except Exception as e:
        logger.error(f"Error loading base.json: {e}")
        # Return empty/default values for all fields in case of error
        return [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], "", "", "", "", ""

# Load all subjects and credentials from JSON
(MARKETS, FOREX_MARKETS, FOREX_MAJORS, FOREX_MINORS, SYNTHETIC_INDICES,
 DRIFT_SWITCHING_INDICES, MULTI_STEP_INDICES, SKEWED_STEP_INDICES, TREK_INDICES,
 TACTICAL_INDICES, BASKET_INDICES, CRYPTO, INDEX_MARKETS, COMMODITIES, TIMEFRAMES,
 LOGIN_ID, PASSWORD, SERVER, BASE_URL, TERMINAL_PATH) = load_markets_and_credentials(MARKETS_JSON_PATH)

MT5_TIMEFRAMES = {
    "M5": mt5.TIMEFRAME_M5,
    "M15": mt5.TIMEFRAME_M15,
    "M30": mt5.TIMEFRAME_M30,
    "H1": mt5.TIMEFRAME_H1,
    "H4": mt5.TIMEFRAME_H4
}
DESTINATION_PATH = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart\fetched"

# Shared multiprocessing variables
network_issue_event = multiprocessing.Event()
network_resolution_lock = multiprocessing.Lock()


def normalize_timeframe(timeframe):
    """Normalize timeframe strings to a consistent format."""
    timeframe = timeframe.lower().strip()
    timeframe_map = {
        '5m': 'm5',
        '5minutes': 'm5',
        '5 minutes': 'm5',
        '15m': 'm15',
        '15minutes': 'm15',
        '15 minutes': 'm15',
        '30m': 'm30',
        '30minutes': 'm30',
        '30 minutes': 'm30',
        '1minute': 'm1',
        '1 minute': 'm1',
        '1m': 'm1',
        'h1': 'h1',
        '1h': 'h1',
        '1hour': 'h1',
        '1 hour': 'h1',
        'h4': 'h4',
        '4h': 'h4',
        '4hours': 'h4',
        '4 hours': 'h4'
    }
    normalized = timeframe_map.get(timeframe, timeframe)
    #logger.debug(f"Normalized timeframe '{timeframe}' to '{normalized}'")
    return normalized

def get_eligible_market_timeframes():
    """Retrieve market-timeframe pairs with elligible_status 'order_free'."""
    eligible_pairs = []
    for market in MARKETS:
        market_folder = os.path.join(DESTINATION_PATH, market.replace(" ", "_"))
        for tf in TIMEFRAMES:
            normalized_tf = normalize_timeframe(tf)  # Normalize timeframe for folder path
            status_file = os.path.join(market_folder, normalized_tf, "status.json")
            try:
                if os.path.exists(status_file):
                    with open(status_file, 'r') as f:
                        status_data = json.load(f)
                        elligible_status = status_data.get("elligible_status", "")
                        if elligible_status == "order_free":
                            eligible_pairs.append((market, tf))  # Use original timeframe in pair
                else:
                    # If status.json doesn't exist, assume eligible to allow processing
                    eligible_pairs.append((market, tf))
            except Exception as e:
                logger.warning(f"Error reading status.json for {market} ({tf}): {e}")
                # If status.json is unreadable, assume eligible to allow processing
                eligible_pairs.append((market, tf))
    logger.debug(f"Eligible market-timeframe pairs: {eligible_pairs}")
    return eligible_pairs

def clear_all_market_files():
    """Clear market-related PNG files in Downloads and destination folders for markets with elligible_status 'order_free'."""
    logger.debug("Clearing market-related files for eligible markets in Downloads and destination folders")
    downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")

    # Clear market-related files from Downloads folder for eligible markets
    try:
        for market in MARKETS:
            market_folder = os.path.join(DESTINATION_PATH, market.replace(" ", "_"))
            for tf in TIMEFRAMES:
                status_file = os.path.join(market_folder, tf.lower(), "status.json")
                should_clear = False
                if os.path.exists(status_file):
                    try:
                        with open(status_file, 'r') as f:
                            status_data = json.load(f)
                            elligible_status = status_data.get("elligible_status", "")
                            if elligible_status == "order_free":
                                should_clear = True
                    except Exception as e:
                        logger.warning(f"Error reading status.json for {market} ({tf}): {e}")
                        should_clear = True  # Clear if status.json is unreadable to ensure processing
                else:
                    should_clear = True  # Clear if no status.json exists to allow processing

                if should_clear:
                    market_files = [f for f in os.listdir(downloads_path)
                                    if os.path.isfile(os.path.join(downloads_path, f)) and market in f and tf.lower() in f.lower()]
                    for file in market_files:
                        file_path = os.path.join(downloads_path, file)
                        try:
                            os.remove(file_path)
                            logger.debug(f"Deleted file in Downloads: {file_path}")
                        except Exception as e:
                            logger.error(f"Error deleting file {file_path}: {e}")
                    timeframe_folder = os.path.join(market_folder, tf.lower())
                    try:
                        if os.path.exists(timeframe_folder):
                            files = [f for f in os.listdir(timeframe_folder)
                                     if os.path.isfile(os.path.join(timeframe_folder, f)) and f.endswith('.png')]
                            for file in files:
                                file_path = os.path.join(timeframe_folder, file)
                                try:
                                    os.remove(file_path)
                                    logger.debug(f"Deleted file in {timeframe_folder}: {file_path}")
                                except Exception as e:
                                    logger.error(f"Error deleting file {file_path}: {e}")
                    except Exception as e:
                        logger.error(f"Error clearing timeframe folder {timeframe_folder}: {e}")
        logger.debug("Completed clearing eligible market-related files")
    except Exception as e:
        logger.error(f"Error clearing Downloads folder: {e}")

def tradinghoursordays(market):
    """Check if the market is within its trading hours or days."""
    current_time = datetime.now(pytz.UTC)
    current_day = current_time.weekday()  # 0=Monday, 6=Sunday
    current_hour = current_time.hour
    current_minute = current_time.minute
    logger.debug(f"[Process-{market}] Checking trading hours for {market}. Current UTC time: {current_time}, Day: {current_day}, Hour: {current_hour}:{current_minute:02d}")

    # Markets that trade 24/7 (including weekends)
    twenty_four_seven_markets = (
        SYNTHETIC_INDICES + DRIFT_SWITCHING_INDICES + MULTI_STEP_INDICES +
        SKEWED_STEP_INDICES + TREK_INDICES +
        ["DEX 900 DOWN Index", "DEX 900 UP Index", "DEX 600 UP Index",
         "DEX 600 DOWN Index", "DEX 1500 UP Index", "DEX 1500 DOWN Index",
         "VolSwitch Low Vol Index", "VolSwitch Medium Vol Index", "VolSwitch High Vol Index"]
    )
    if market in twenty_four_seven_markets:
        logger.debug(f"[Process-{market}] {market} is a 24/7 market")
        return True

    # Forex markets: Sunday 22:00 UTC to Friday 22:00 UTC
    if market in FOREX_MARKETS:
        if current_day == 6:  # Sunday
            if current_time < datetime.now(pytz.UTC).replace(hour=22, minute=0, second=0, microsecond=0):
                logger.debug(f"[Process-{market}] Forex market {market} closed on Sunday before 22:00 UTC")
                return False
            return True
        elif current_day == 4:  # Friday
            if current_time >= datetime.now(pytz.UTC).replace(hour=22, minute=0, second=0, microsecond=0):
                logger.debug(f"[Process-{market}] Forex market {market} closed on Friday after 22:00 UTC")
                return False
            return True
        elif current_day == 5:  # Saturday
            logger.debug(f"[Process-{market}] Forex market {market} closed on Saturday")
            return False
        logger.debug(f"[Process-{market}] Forex market {market} is open")
        return True

    # Index markets and Tactical Indices: Monday–Friday, 1:00 AM–10:00 PM UTC
    if market in INDEX_MARKETS or market in TACTICAL_INDICES:
        if current_day in [5, 6]:  # Closed on Saturday and Sunday
            logger.debug(f"[Process-{market}] {market} (Index or Tactical Index) closed on weekend")
            return False
        if current_hour < 1 or current_hour >= 22:  # 1:00 AM–10:00 PM UTC
            logger.debug(f"[Process-{market}] {market} (Index or Tactical Index) closed outside 01:00–22:00 UTC")
            return False
        logger.debug(f"[Process-{market}] {market} (Index or Tactical Index) is open")
        return True

    # Commodities: Specific hours, Monday–Friday
    if market in COMMODITIES:
        if current_day in [5, 6]:  # Closed on Saturday and Sunday
            logger.debug(f"[Process-{market}] Commodity {market} closed on weekend")
            return False
        if market in ["CoffeeRobu", "CoffeeArab"]:  # ~4:00 AM–12:30 PM UTC
            if current_hour < 4 or (current_hour >= 12 and current_minute > 30) or current_hour >= 13:
                logger.debug(f"[Process-{market}] Coffee {market} closed outside 04:00–12:30 UTC")
                return False
        elif market in ["Cocoa"]:  # ~4:45 AM–1:20 PM UTC
            if current_hour < 4 or (current_hour == 4 and current_minute < 45) or \
               (current_hour >= 13 and current_minute > 20) or current_hour >= 14:
                logger.debug(f"[Process-{market}] Cocoa closed outside 04:45–13:20 UTC")
                return False
        elif market in ["Sugar"]:  # ~3:30 AM–1:00 PM UTC
            if current_hour < 3 or (current_hour == 3 and current_minute < 30) or \
               current_hour >= 13:
                logger.debug(f"[Process-{market}] Sugar closed outside 03:30–13:00 UTC")
                return False
        elif market in ["Cotton"]:  # ~2:00 AM–2:20 PM UTC
            if current_hour < 2 or (current_hour >= 14 and current_minute > 20) or current_hour >= 15:
                logger.debug(f"[Process-{market}] Cotton closed outside 02:00–14:20 UTC")
                return False
        elif market in ["NGAS", "UK Brent Oil", "US Oil"]:  # ~1:00 AM–10:00 PM UTC
            if current_hour < 1 or current_hour >= 22:
                logger.debug(f"[Process-{market}] Energy {market} closed outside 01:00–22:00 UTC")
                return False
        elif market == "XAUUSD":  # Gold: ~1:00 AM–10:00 PM UTC
            if current_hour < 1 or current_hour >= 22:
                logger.debug(f"[Process-{market}] Gold closed outside 01:00–22:00 UTC")
                return False
        logger.debug(f"[Process-{market}] Commodity {market} is open")
        return True

    # Crypto: Typically closed on weekends (broker-specific)
    if market in CRYPTO:
        if current_day in [5, 6]:  # Saturday and Sunday
            logger.debug(f"[Process-{market}] Crypto {market} closed on weekend (broker-specific)")
            return False
        logger.debug(f"[Process-{market}] Crypto {market} is open")
        return True

    # Basket Indices: Monday–Friday, 1:00 AM–10:00 PM UTC
    if market in BASKET_INDICES:
        if current_day in [5, 6]:  # Closed on Saturday and Sunday
            logger.debug(f"[Process-{market}] Basket index {market} closed on weekend")
            return False
        if current_hour < 1 or current_hour >= 22:  # 1:00 AM–10:00 PM UTC
            logger.debug(f"[Process-{market}] Basket index {market} closed outside 01:00–22:00 UTC")
            return False
        logger.debug(f"[Process-{market}] Basket index {market} is open")
        return True

    # Unknown markets: Conservative default to closed
    logger.warning(f"[Process-{market}] Unknown market type for {market}, assuming closed")
    return False

def categorize_markets_by_trading_status(markets, destination_path, timeframes):
    """
    Categorize markets into open and closed based on trading hours, and update status.json for closed markets.
    
    Args:
        markets (list): List of market symbols.
        destination_path (str): Path to store status files.
        timeframes (list): List of timeframes to update status for closed markets.
    
    Returns:
        tuple: (open_markets, closed_markets)
            - open_markets (list): Markets that are open or trade 24/7.
            - closed_markets (list): Markets that are currently closed.
    """
    logger.debug("Categorizing markets by trading status")
    open_markets = []
    closed_markets = []
    current_day = datetime.now(pytz.UTC).weekday()  # 0=Monday, 6=Sunday

    # On weekends (Saturday/Sunday), categorize markets directly
    if current_day in [5, 6]:
        logger.debug("Weekend detected (Saturday/Sunday), categorizing markets accordingly")
        # 24/7 markets (open on weekends)
        twenty_four_seven_markets = (
            SYNTHETIC_INDICES + DRIFT_SWITCHING_INDICES + MULTI_STEP_INDICES +
            SKEWED_STEP_INDICES + TREK_INDICES +
            ["DEX 900 DOWN Index", "DEX 900 UP Index", "DEX 600 UP Index",
             "DEX 600 DOWN Index", "DEX 1500 UP Index", "DEX 1500 DOWN Index",
             "VolSwitch Low Vol Index", "VolSwitch Medium Vol Index", "VolSwitch High Vol Index"]
        )
        # Non-24/7 markets (closed on weekends)
        non_twenty_four_seven_markets = (
            FOREX_MARKETS + INDEX_MARKETS + COMMODITIES + CRYPTO +
            BASKET_INDICES + TACTICAL_INDICES
        )

        for market in markets:
            try:
                if market in twenty_four_seven_markets:
                    open_markets.append(market)
                    logger.debug(f"Market {market} is open (24/7 market)")
                elif market in non_twenty_four_seven_markets:
                    closed_markets.append(market)
                    logger.debug(f"Market {market} is closed (non-24/7 market on weekend)")
                    # Update status.json for all timeframes
                    for tf in timeframes:
                        try:
                            save_status(market, tf, destination_path, "market_closed")
                        except Exception as e:
                            logger.error(f"[Process-{market}] Error updating status.json for {market} ({tf}): {e}")
                else:
                    logger.warning(f"[Process-{market}] Unknown market {market}, assuming closed")
                    closed_markets.append(market)
                    for tf in timeframes:
                        try:
                            save_status(market, tf, destination_path, "market_closed_unknown")
                        except Exception as e:
                            logger.error(f"[Process-{market}] Error updating status.json for {market} ({tf}): {e}")
            except Exception as e:
                logger.error(f"[Process-{market}] Error processing {market}: {e}")
                closed_markets.append(market)
                for tf in timeframes:
                    try:
                        save_status(market, tf, destination_path, "market_closed_error")
                    except Exception as e:
                        logger.error(f"[Process-{market}] Error updating status.json for {market} ({tf}): {e}")
    else:
        # On weekdays, use tradinghoursordays to check specific hours
        for market in markets:
            try:
                if tradinghoursordays(market):
                    open_markets.append(market)
                    logger.debug(f"Market {market} is open")
                else:
                    closed_markets.append(market)
                    logger.debug(f"Market {market} is closed")
                    # Update status.json for all timeframes
                    for tf in timeframes:
                        try:
                            save_status(market, tf, destination_path, "market_closed")
                        except Exception as e:
                            logger.error(f"[Process-{market}] Error updating status.json for {market} ({tf}): {e}")
            except Exception as e:
                logger.error(f"[Process-{market}] Error checking trading status for {market}: {e}")
                closed_markets.append(market)
                for tf in timeframes:
                    try:
                        save_status(market, tf, destination_path, "market_closed_error")
                    except Exception as e:
                        logger.error(f"[Process-{market}] Error updating status.json for {market} ({tf}): {e}")

    logger.debug(f"Open markets: {open_markets}")
    logger.debug(f"Closed markets: {closed_markets}")
    return open_markets, closed_markets

def initialize_mt5(market):
    """Initialize MT5 connection with retries and symbol verification."""
    logger.debug(f"[Process-{market}] Initializing MT5 for {market}")
    for attempt in range(3):
        if mt5.initialize(path=TERMINAL_PATH, timeout=60000):
            break
        logger.error(f"[Process-{market}] Attempt {attempt + 1}/3: Failed to initialize MT5 terminal. Error: {mt5.last_error()}")
        time.sleep(5)
    else:
        logger.error(f"[Process-{market}] Failed to initialize MT5 terminal after 3 attempts")
        return False
    
    # Verify symbol availability
    if not mt5.symbol_select(market, True):
        logger.error(f"[Process-{market}] Symbol {market} not available, error: {mt5.last_error()}")
        return False
    symbol_info = mt5.symbol_info(market)
    if symbol_info is None or not symbol_info.visible:
        logger.error(f"[Process-{market}] Symbol {market} not visible or invalid")
        return False
    
    for _ in range(5):
        if mt5.terminal_info() is not None:
            break
        logger.debug(f"[Process-{market}] Waiting for MT5 terminal to fully initialize...")
        time.sleep(2)
    else:
        logger.error(f"[Process-{market}] MT5 terminal not ready")
        return False
    for attempt in range(3):
        if mt5.login(login=int(LOGIN_ID), password=PASSWORD, server=SERVER, timeout=60000):
            logger.debug(f"[Process-{market}] Successfully logged in to MT5")
            return True
        error_code, error_message = mt5.last_error()
        logger.error(f"[Process-{market}] Attempt {attempt + 1}/3: Failed to log in to MT5. Error code: {error_code}, Message: {error_message}")
        time.sleep(5)
    logger.error(f"[Process-{market}] Failed to log in to MT5 after 3 attempts")
    return False

def get_time_until_candle_close(market, timeframe, candle_time):
    """Calculate time left until the candle closes."""
    candle_datetime = datetime.fromtimestamp(candle_time, tz=pytz.UTC)
    timeframe_minutes = {"M5": 5, "M15": 15, "M30": 30, "H1": 60, "H4": 240}
    minutes_per_candle = timeframe_minutes.get(timeframe.upper(), 5)
    
    total_minutes = (candle_datetime.hour * 60 + candle_datetime.minute)
    remainder = total_minutes % minutes_per_candle
    last_candle_start = candle_datetime - timedelta(minutes=remainder, seconds=candle_datetime.second, microseconds=candle_datetime.microsecond)
    next_close_time = last_candle_start + timedelta(minutes=minutes_per_candle)
    
    current_time = datetime.now(pytz.UTC)
    time_left = (next_close_time - current_time).total_seconds() / 60.0
    
    if time_left <= 0:
        next_close_time += timedelta(minutes=minutes_per_candle)
        time_left = (next_close_time - current_time).total_seconds() / 60.0
    
    logger.debug(f"[Process-{market}] Candle time: {candle_datetime}, Next close: {next_close_time}, Time left: {time_left:.2f} minutes")
    
    return time_left, next_close_time

def fetch_current_price_candle(market, timeframe):
    """Fetch the latest candle for a given market and timeframe."""
    if not mt5.symbol_select(market, True):
        logger.error(f"[Process-{market}] Failed to select market: {market}, error: {mt5.last_error()}")
        return None
    timeframe_minutes = {"M5": 6, "M15": 16, "M30": 31, "H1": 61, "H4": 241}
    max_age = timeframe_minutes.get(timeframe.upper(), 5) * 60
    for attempt in range(5):
        start_time = time.time()
        candles = mt5.copy_rates_from_pos(market, MT5_TIMEFRAMES[timeframe.upper()], 0, 1)
        logger.debug(f"[Process-{market}] Fetch attempt {attempt + 1}/5 took {time.time() - start_time:.2f} seconds")
        if candles is None or len(candles) == 0:
            logger.error(f"[Process-{market}] Attempt {attempt + 1}/5: No candle data, error: {mt5.last_error()}")
            time.sleep(3)
            continue
        current_time = datetime.now(pytz.UTC)
        candle_time = datetime.fromtimestamp(candles[0]['time'], tz=pytz.UTC)
        if (current_time - candle_time).total_seconds() > max_age:
            logger.warning(f"[Process-{market}] Attempt {attempt + 1}/5: Candle time {candle_time} too old for {market} ({timeframe})")
            if market in COMMODITIES or market in CRYPTO:
                logger.debug(f"[Process-{market}] Allowing older candle for {market} ({timeframe})")
                return candles[0]  # Allow older candles for commodities/crypto
            time.sleep(5)
            continue
        logger.debug(f"[Process-{market}] Fetched candle: time={candle_time}, open={candles[0]['open']}")
        return candles[0]
    logger.error(f"[Process-{market}] Failed to fetch recent candle for {market} ({timeframe}) after 5 attempts")
    return None

def operate(mode="headless"):
    """Initialize WebDriver."""
    CHROME_BINARY_PATH = r"C:\xampp\htdocs\CIPHER\googlechrome\Google\Chrome\Application\chrome.exe"
    options = Options()
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-autofill")
    options.add_argument("--log-level=3")
    options.binary_location = CHROME_BINARY_PATH
    if mode.lower() in ["headless", "head"]:
        logger.debug(f"Initializing WebDriver in {mode} mode")
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")
    else:
        logger.debug(f"Initializing WebDriver in {mode} mode")
        options.add_argument("--start-maximized")
    try:
        if not os.path.exists(CHROME_BINARY_PATH):
            logger.error(f"Chrome binary not found at {CHROME_BINARY_PATH}")
            raise WebDriverException(f"Chrome binary not found at {CHROME_BINARY_PATH}")
        # Use ChromeDriverManager with the version parameter
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager(driver_version="139.0.7258.128").install()),
            options=options
        )
        driver.set_page_load_timeout(180)
        logger.debug(f"WebDriver initialized successfully in {mode} mode")
        return driver
    except WebDriverException as e:
        logger.error(f"Error initializing WebDriver: {e}")
        raise

def handle_network_issue():
    """Handle network issues collectively."""
    logger.debug("Network issue detected, resolving")
    network_issue_event.set()
    with network_resolution_lock:
        if network_issue_event.is_set():
            logger.debug("Resolving network issue")
            # Placeholder for check_network_condition() implementation
            network_issue_event.clear()
            logger.debug("Network issue resolved")
            time.sleep(2)

def wait_for_page_load(driver, timeout=30):
    """Wait for page to fully load."""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        logger.debug("Page fully loaded")
        return True
    except TimeoutException:
        logger.error("Timeout waiting for page load")
        return False

def login(driver, login_id, password, server, market):
    """Attempt login."""
    try:
        logger.debug(f"[Process-{market}] Logging in for {market}")
        driver.get(BASE_URL)
        if not wait_for_page_load(driver, timeout=30):
            logger.error(f"[Process-{market}] Page load failed for login")
            return False
        try:
            login_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Login') or contains(text(), 'Log in') or @type='submit']"))
            )
            login_field = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//input[contains(@id, 'login') or contains(@name, 'login') or @type='text']"))
            )
            password_field = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//input[contains(@id, 'password') or contains(@name, 'password') or @type='password']"))
            )
            logger.debug(f"[Process-{market}] Entering credentials")
            driver.execute_script("arguments[0].setAttribute('autocomplete', 'off')", login_field)
            driver.execute_script("arguments[0].setAttribute('autocomplete', 'off')", password_field)
            login_field.clear()
            login_field.send_keys(login_id)
            password_field.clear()
            password_field.send_keys(password)
            try:
                server_field = WebDriverWait(driver, 2).until(
                    EC.element_to_be_clickable((By.XPATH, "//input[contains(@id, 'server') or contains(@name, 'server')]"))
                )
                server_field.clear()
                server_field.send_keys(server)
            except (TimeoutException, NoSuchElementException):
                logger.debug(f"[Process-{market}] Server input not found, checking dropdown")
                try:
                    server_dropdown = Select(WebDriverWait(driver, 2).until(
                        EC.presence_of_element_located((By.XPATH, "//select[contains(@id, 'server') or contains(@name, 'server')]"))
                    ))
                    server_dropdown.select_by_visible_text(server)
                except (TimeoutException, NoSuchElementException):
                    logger.debug(f"[Process-{market}] Server dropdown not found; assuming pre-filled")
            logger.debug(f"[Process-{market}] Submitting login")
            login_button.click()
            try:
                error_message = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'Invalid') or contains(text(), 'invalid')]"))
                )
                logger.error(f"[Process-{market}] Login error: {error_message.text}")
                return False
            except TimeoutException:
                logger.debug(f"[Process-{market}] No login error detected")
            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.TAG_NAME, "canvas"))
            )
            logger.debug(f"[Process-{market}] Login successful")
            wait_for_page_load(driver, 10)
            return True
        except TimeoutException:
            logger.debug(f"[Process-{market}] No login form; assuming auto-authenticated")
            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.TAG_NAME, "canvas"))
            )
            logger.debug(f"[Process-{market}] Chart detected")
            wait_for_page_load(driver, 10)
            return True
    except Exception as e:
        logger.error(f"[Process-{market}] Login error for {market}: {e}")
        return False

def tradewindow(driver, action, market):
    """Toggle trade window."""
    try:
        logger.debug(f"[Process-{market}] Toggling trade window ({action})")
        trade_icon_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'left-panel')]//div[@title='trade' and contains(@class, 'icon-button')] | //div[@title='trade' or @title='Trade']"))
        )
        trade_icon_button.click()
        if action == 'open':
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, "//*[contains(@class, 'trade-panel') and contains(@class, 'visible')]"))
            )
        else:
            WebDriverWait(driver, 5).until(
                EC.invisibility_of_element_located((By.XPATH, "//*[contains(@class, 'trade-panel') and contains(@class, 'visible')]"))
            )
        logger.debug(f"[Process-{market}] Trade window {action} successful")
        return True
    except Exception as e:
        logger.error(f"[Process-{market}] Error toggling trade window ({action}): {e}")
        return False

def timeframe(driver, tf, market):
    """Select timeframe for a market."""
    timeframe_map = {
        'M1': '1 Minute', 'M5': '5 Minutes', 'M15': '15 Minutes', 'M30': '30 Minutes',
        'H1': '1 Hour', 'H4': '4 Hours', 'D1': 'Daily', 'W1': 'Weekly', 'MN': 'Monthly'
    }
    tf_full = timeframe_map.get(tf.upper())
    if not tf_full:
        logger.error(f"[Process-{market}] Invalid timeframe '{tf}' for {market}")
        return False
    try:
        logger.debug(f"[Process-{market}] Selecting timeframe '{tf}' for {market}")
        timeframe_button = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((
                By.XPATH,
                f"//div[contains(@class, 'icon-button') and contains(@class, 'svelte-1iwf8ix') and @title='{tf_full}' and not(contains(@class, 'withCorner'))]"
            ))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", timeframe_button)
        timeframe_button.click()
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((
                By.XPATH,
                f"//div[contains(@class, 'icon-button') and contains(@class, 'svelte-1iwf8ix') and @title='{tf_full}' and contains(@class, 'checked')]"
            ))
        )
        logger.debug(f"[Process-{market}] Timeframe '{tf}' selected for {market}")
        wait_for_page_load(driver, 10)
        time.sleep(5)
        return True
    except Exception as e:
        logger.error(f"[Process-{market}] Error selecting timeframe '{tf}' for {market}: {e}")
        return False

def search(driver, market):
    """Search for a market, retry with a random market on failure, then retry the original market."""
    try:
        logger.debug(f"[Process-{market}] Checking if market '{market}' is already displayed")

        # Attempt to verify if the market is already displayed
        try:
            market_display = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    f"//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{market.lower()}') and "
                    f"(contains(@class, 'symbol') or contains(@class, 'title') or contains(@class, 'chart-header'))]"
                ))
            )
            logger.debug(f"[Process-{market}] Market '{market}' is already displayed")
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "canvas"))
            )
            logger.debug(f"[Process-{market}] Chart canvas detected for already displayed market '{market}'")
            wait_for_page_load(driver, 10)
            return True
        except TimeoutException:
            logger.debug(f"[Process-{market}] Market '{market}' not detected as currently displayed, proceeding with search")

        # Proceed with searching for the market
        logger.debug(f"[Process-{market}] Searching for market '{market}'")
        search_bar = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//input[contains(@id, 'search') or contains(@class, 'search') or contains(@placeholder, 'Search')]"))
        )
        search_bar.clear()
        search_bar.send_keys(market)
        try:
            search_bar.send_keys(Keys.RETURN)
        except Exception as e:
            logger.debug(f"[Process-{market}] Enter key failed: {e}")
            try:
                search_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'search') or contains(text(), 'Search')]"))
                )
                search_button.click()
            except (TimeoutException, NoSuchElementException):
                logger.debug(f"[Process-{market}] Search button not found; assuming Enter worked")

        # Wait for and click the search result
        try:
            search_result = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, f"//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{market.lower()}')]"))
            )
            search_result.click()
            time.sleep(1)  # Brief pause to allow UI to update

            # Confirm selection by checking for chart canvas presence
            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.TAG_NAME, "canvas"))
            )
            logger.debug(f"[Process-{market}] Chart canvas detected, assuming market '{market}' selected successfully")
            wait_for_page_load(driver, 10)
            return True
        except (TimeoutException, NoSuchElementException):
            logger.warning(f"[Process-{market}] Failed to select market '{market}', attempting random market")
            import random
            other_markets = [m for m in MARKETS if m != market]
            if not other_markets:
                logger.error(f"[Process-{market}] No other markets available to try")
                return False
            random_market = random.choice(other_markets)
            logger.debug(f"[Process-{market}] Attempting to select random market '{random_market}'")
            search_bar.clear()
            search_bar.send_keys(random_market)
            try:
                search_bar.send_keys(Keys.RETURN)
            except Exception as e:
                logger.debug(f"[Process-{market}] Enter key failed for random market: {e}")
                try:
                    search_button = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'search') or contains(text(), 'Search')]"))
                    )
                    search_button.click()
                except (TimeoutException, NoSuchElementException):
                    logger.debug(f"[Process-{market}] Search button not found for random market; assuming Enter worked")
            try:
                random_result = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, f"//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{random_market.lower()}')]"))
                )
                random_result.click()
                time.sleep(1)
                WebDriverWait(driver, 60).until(
                    EC.presence_of_element_located((By.TAG_NAME, "canvas"))
                )
                logger.debug(f"[Process-{market}] Successfully selected random market '{random_market}'")
            except (TimeoutException, NoSuchElementException):
                logger.error(f"[Process-{market}] Failed to select random market '{random_market}'")
                return False

            # Retry the original market
            logger.debug(f"[Process-{market}] Retrying search for original market '{market}'")
            search_bar.clear()
            search_bar.send_keys(market)
            try:
                search_bar.send_keys(Keys.RETURN)
            except Exception as e:
                logger.debug(f"[Process-{market}] Enter key failed on retry: {e}")
                try:
                    search_button = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'search') or contains(text(), 'Search')]"))
                    )
                    search_button.click()
                except (TimeoutException, NoSuchElementException):
                    logger.debug(f"[Process-{market}] Search button not found on retry; assuming Enter worked")
            try:
                search_result = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, f"//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{market.lower()}')]"))
                )
                search_result.click()
                time.sleep(1)
                WebDriverWait(driver, 60).until(
                    EC.presence_of_element_located((By.TAG_NAME, "canvas"))
                )
                logger.debug(f"[Process-{market}] Chart canvas detected after retry, assuming market '{market}' selected successfully")
                wait_for_page_load(driver, 10)
                return True
            except (TimeoutException, NoSuchElementException):
                logger.error(f"[Process-{market}] Failed to select market '{market}' after retry")
                return False

    except Exception as e:
        logger.error(f"[Process-{market}] Unexpected error searching for '{market}': {e}")
        return False
       
def watchlist(driver, action, market):
    """Toggle watchlist."""
    try:
        logger.debug(f"[Process-{market}] Toggling watchlist ({action})")
        group_elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "group"))
        )
        if len(group_elements) >= 9:
            watchlist_button = group_elements[8]
            watchlist_button.click()
            if action == 'open':
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, "//*[contains(@class, 'watchlist') and contains(@class, 'visible')]"))
                )
            else:
                WebDriverWait(driver, 5).until(
                    EC.invisibility_of_element_located((By.XPATH, "//*[contains(@class, 'watchlist') and contains(@class, 'visible')]"))
                )
            logger.debug(f"[Process-{market}] Watchlist {action} successful")
            return True
        else:
            logger.error(f"[Process-{market}] Fewer than 9 group elements found: {len(group_elements)}")
            return False
    except Exception as e:
        logger.error(f"[Process-{market}] Error toggling watchlist ({action}): {e}")
        return False

def wait_for_download(downloads_path, market, timeframe, max_wait=30):
    """Wait for a chart file to download."""
    start_time = time.time()
    while time.time() - start_time < max_wait:
        files = [f for f in os.listdir(downloads_path) 
                 if os.path.isfile(os.path.join(downloads_path, f)) 
                 and market in f and timeframe.lower() in f.lower() and not f.endswith('.crdownload')]
        if files:
            latest_file = max(
                [os.path.join(downloads_path, f) for f in files],
                key=os.path.getmtime
            )
            initial_size = os.path.getsize(latest_file)
            time.sleep(1)
            final_size = os.path.getsize(latest_file)
            if initial_size == final_size:
                logger.debug(f"[Process-{market}] Download completed: {latest_file}")
                return latest_file
        time.sleep(1)
    logger.error(f"[Process-{market}] Timeout waiting for download: {market} ({timeframe})")
    return None

def save_chart(driver, timeout, market):
    """Save chart as image."""
    try:
        logger.debug(f"[Process-{market}] Saving chart for {market}")
        save_chart_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//*[@title='Save Chart as Image (Ctrl + S)'] | //*[contains(@class, 'save-chart') or contains(@class, 'camera')]"))
        )
        time.sleep(timeout)
        save_chart_button.click()
        logger.debug(f"[Process-{market}] Chart save initiated")
        time.sleep(0.5)
        return True
    except Exception as e:
        logger.error(f"[Process-{market}] Error saving chart for {market}: {e}")
        return False

def check_page_load_status(driver, market, timeout=10):
    """Check if chart canvas is loaded."""
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.TAG_NAME, "canvas"))
        )
        logger.debug(f"[Process-{market}] Chart canvas detected")
        return True
    except TimeoutException:
        logger.error(f"[Process-{market}] Chart canvas not detected")
        return False

def copy_chart_to_destination(driver, market, timeframe, destination_path):
    """Copy chart to destination folder with filename format market_timeframe.png."""
    try:
        logger.debug(f"[Process-{market}] Copying chart for {market} ({timeframe})")
        downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
        normalized_tf = normalize_timeframe(timeframe)  # Normalize timeframe for folder path
        market_folder = os.path.join(destination_path, market.replace(" ", "_"), normalized_tf)
        os.makedirs(market_folder, exist_ok=True)
        latest_file = wait_for_download(downloads_path, market, timeframe)
        if not latest_file:
            logger.error(f"[Process-{market}] No chart file found for {market} ({timeframe})")
            return False
        # Create new filename in the format market_timeframe.png
        new_filename = f"{market.replace(' ', '_')}_{normalized_tf}.png"
        destination_file = os.path.join(market_folder, new_filename)
        shutil.copy2(latest_file, destination_file)
        logger.debug(f"[Process-{market}] Copied chart to {destination_file}")
        return destination_file
    except Exception as e:
        logger.error(f"[Process-{market}] Error copying chart for {market} ({timeframe}): {e}")
        return False

def verify_candlestick_contours(image_path, market, timeframe):
    """Verify candlesticks in the chart image."""
    logger.debug(f"[Process-{market}] Verifying candlesticks in {image_path}")
    normalized_tf = normalize_timeframe(timeframe)  # Normalize timeframe for folder path
    market_folder = os.path.join(DESTINATION_PATH, market.replace(" ", "_"), normalized_tf)
    os.makedirs(market_folder, exist_ok=True)

    try:
        img = cv2.imread(image_path)
        if img is None:
            logger.error(f"[Process-{market}] Failed to load image: {image_path}")
            return False, False
        img_hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
        lower_red = np.array([0, 100, 100])
        upper_red = np.array([10, 255, 255])
        lower_green = np.array([40, 100, 100])
        upper_green = np.array([80, 255, 255])
        mask_red = cv2.inRange(img_hsv, lower_red, upper_red)
        mask_green = cv2.inRange(img_hsv, lower_green, upper_green)
        height, width = img.shape[:2]
        mid_x = width // 2

        img_left = img[:, :mid_x].copy()
        mask_red_left = mask_red[:, :mid_x]
        mask_green_left = mask_green[:, :mid_x]
        
        img_right = img[:, mid_x:].copy()
        mask_red_right = mask_red[:, mid_x:]
        mask_green_right = mask_green[:, mid_x:]

        grid_count = 5
        grid_width_left = mid_x // grid_count
        grid_width_right = (width - mid_x) // grid_count

        left_grid_contours = [0] * grid_count
        right_grid_contours = [0] * grid_count
        min_contour_area = 0.01

        for i in range(grid_count):
            start_x = i * grid_width_left
            end_x = (i + 1) * grid_width_left if i < grid_count - 1 else mid_x
            grid_mask_red = mask_red_left[:, start_x:end_x]
            grid_mask_green = mask_green_left[:, start_x:end_x]
            contours_red, _ = cv2.findContours(grid_mask_red, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            contours_green, _ = cv2.findContours(grid_mask_green, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            left_grid_contours[i] = len([c for c in contours_red if cv2.contourArea(c) >= min_contour_area]) + \
                                    len([c for c in contours_green if cv2.contourArea(c) >= min_contour_area])
            cv2.drawContours(img_left[:, start_x:end_x], contours_red, -1, (0, 0, 255), 1)
            cv2.drawContours(img_left[:, start_x:end_x], contours_green, -1, (0, 255, 0), 1)
            if left_grid_contours[i] > 0:
                text_x = start_x + 10
                text_y = 30
                cv2.putText(img_left, "D", (text_x, text_y), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 0), 2, cv2.LINE_AA)

        for i in range(grid_count):
            start_x = i * grid_width_right
            end_x = (i + 1) * grid_width_right if i < grid_count - 1 else (width - mid_x)
            grid_mask_red = mask_red_right[:, start_x:end_x]
            grid_mask_green = mask_green_right[:, start_x:end_x]
            contours_red, _ = cv2.findContours(grid_mask_red, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            contours_green, _ = cv2.findContours(grid_mask_green, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            right_grid_contours[i] = len([c for c in contours_red if cv2.contourArea(c) >= min_contour_area]) + \
                                     len([c for c in contours_green if cv2.contourArea(c) >= min_contour_area])
            cv2.drawContours(img_right[:, start_x:end_x], contours_red, -1, (0, 0, 255), 1)
            cv2.drawContours(img_right[:, start_x:end_x], contours_green, -1, (0, 255, 0), 1)
            if right_grid_contours[i] > 0:
                text_x = start_x + 10
                text_y = 30
                cv2.putText(img_right, "D", (text_x, text_y), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 0), 2, cv2.LINE_AA)

        for i in range(1, grid_count):
            x_left = i * grid_width_left
            cv2.line(img_left, (x_left, 0), (x_left, height), (0, 0, 0), 1)
            x_right = i * grid_width_right
            cv2.line(img_right, (x_right, 0), (x_right, height), (0, 0, 0), 1)

        left_output_path = os.path.join(market_folder, "chartverification_left.png")
        right_output_path = os.path.join(market_folder, "chartverification_right.png")
        cv2.imwrite(left_output_path, img_left)
        cv2.imwrite(right_output_path, img_right)
        logger.debug(f"[Process-{market}] Saved left half: {left_output_path}, contours: {left_grid_contours}")
        logger.debug(f"[Process-{market}] Saved right half: {right_output_path}, contours: {right_grid_contours}")

        left_valid = all(count > 0 for count in left_grid_contours)
        right_valid = all(count > 0 for count in right_grid_contours[:3])
        total_left_contours = sum(left_grid_contours)
        total_right_contours = sum(right_grid_contours)

        if left_valid and right_valid:
            logger.debug(f"[Process-{market}] Candlesticks verified for {image_path}")
            return True, False
        else:
            logger.warning(f"[Process-{market}] Insufficient candlesticks in {image_path}")
            needs_reload = (total_left_contours > 0 and total_right_contours == 0) or \
                           (total_left_contours == 0 and total_right_contours > 0) or \
                           (any(count > 0 for count in left_grid_contours) and not left_valid) or \
                           (any(count > 0 for count in right_grid_contours[:3]) and not right_valid)
            return False, needs_reload
    except Exception as e:
        logger.error(f"[Process-{market}] Error verifying candlesticks in {image_path}: {e}")
        return False, False

def save_status(market, timeframe, destination_path, status):
    """Save the status of the process for a market and timeframe to a JSON file."""
    try:
        mapped_timeframe = normalize_timeframe(timeframe)
        market_folder = os.path.join(destination_path, market.replace(" ", "_"), mapped_timeframe)
        os.makedirs(market_folder, exist_ok=True)
        status_file = os.path.join(market_folder, "status.json")
        current_time = datetime.now(pytz.timezone('Africa/Lagos'))
        am_pm = "am" if current_time.hour < 12 else "pm"
        hour_12 = current_time.hour % 12 or 12
        timestamp = f"{current_time.strftime('%Y-%m-%d T %I:%M:%S')} {am_pm} .{current_time.microsecond:06d}+01:00"
        
        status_data = {
            "market": market,
            "timeframe": timeframe,
            "normalized_timeframe": mapped_timeframe,
            "timestamp": timestamp,
            "status": status,
            "elligible_status": "chart_identified" if status == "chart_identified" else "order_free"
        }
        
        if os.path.exists(status_file):
            try:
                with open(status_file, 'r') as f:
                    existing_data = json.load(f)
                existing_data.update(status_data)
                status_data = existing_data
            except Exception as e:
                logger.warning(f"[Process-{market}] Error reading existing status.json: {e}")
        
        with open(status_file, 'w') as f:
            json.dump(status_data, f, indent=4)
        logger.debug(f"[Process-{market}] Saved status '{status}' with elligible_status '{status_data['elligible_status']}' to {status_file}")
    except Exception as e:
        logger.error(f"[Process-{market}] Error saving status for {market} ({timeframe}): {e}")

def create_verification_json(market, destination_path):
    """Create verification.json for a market by collecting statuses from each timeframe's status.json, creating status.json if missing."""
    try:
        market_folder = os.path.join(destination_path, market.replace(" ", "_"))
        verification_data = {}
        all_identified = True

        for tf in TIMEFRAMES:
            normalized_tf = normalize_timeframe(tf)  # Normalize timeframe for folder path
            status_file = os.path.join(market_folder, normalized_tf, "status.json")
            if os.path.exists(status_file):
                try:
                    with open(status_file, 'r') as f:
                        status_data = json.load(f)
                        status = status_data.get("status", "unknown")
                        verification_data[tf.lower()] = status
                        if status != "chart_identified":
                            all_identified = False
                except Exception as e:
                    logger.error(f"[Process-{market}] Error reading status.json for {market} ({tf}): {e}")
                    verification_data[tf.lower()] = "error_reading_status"
                    all_identified = False
            else:
                # Create status.json with "incomplete" status
                logger.warning(f"[Process-{market}] status.json not found for {market} ({tf}), creating with 'incomplete' status")
                os.makedirs(os.path.dirname(status_file), exist_ok=True)
                current_time = datetime.now(pytz.timezone('Africa/Lagos'))
                am_pm = "am" if current_time.hour < 12 else "pm"
                hour_12 = current_time.hour % 12 or 12
                timestamp = f"{current_time.strftime('%Y-%m-%d T %I:%M:%S')} {am_pm} .{current_time.microsecond:06d}+01:00"
                status_data = {
                    "market": market,
                    "timeframe": tf,
                    "normalized_timeframe": normalized_tf,
                    "timestamp": timestamp,
                    "status": "incomplete",
                    "elligible_status": "order_free"
                }
                try:
                    with open(status_file, 'w') as f:
                        json.dump(status_data, f, indent=4)
                    logger.debug(f"[Process-{market}] Created status.json for {market} ({tf}) with status 'incomplete'")
                    verification_data[tf.lower()] = "incomplete"
                    all_identified = False
                except Exception as e:
                    logger.error(f"[Process-{market}] Error creating status.json for {market} ({tf}): {e}")
                    verification_data[tf.lower()] = "error_creating_status"
                    all_identified = False

        verification_data["all_timeframes"] = "verified" if all_identified else "incomplete_verification"
        verification_file = os.path.join(market_folder, "verification.json")
        os.makedirs(market_folder, exist_ok=True)
        with open(verification_file, 'w') as f:
            json.dump(verification_data, f, indent=4)
        logger.debug(f"[Process-{market}] Saved verification.json to {verification_file}")
        return True
    except Exception as e:
        logger.error(f"[Process-{market}] Error creating verification.json for {market}: {e}")
        return False
     
def download_and_verify_chart(driver, market, timeframe, destination_path, max_timeout=30):
    """Download and verify chart, retrying up to 2 times within session, and save candle data and status."""
    normalized_tf = normalize_timeframe(timeframe)  # Normalize timeframe for folder path
    market_folder = os.path.join(destination_path, market.replace(" ", "_"), normalized_tf)
    os.makedirs(market_folder, exist_ok=True)

    # Initialize status
    save_status(market, timeframe, destination_path, "initializing")

    is_trading_active = tradinghoursordays(market)
    candle_data = None
    mostrecent_completedcandle_data = None
    candle = None
    mostrecent_completedcandle = None
    if is_trading_active:
        if not initialize_mt5(market):
            logger.error(f"[Process-{market}] Failed to initialize MT5 for {market} ({timeframe})")
            save_status(market, timeframe, destination_path, "mt5_initialization_failed")
            return False
        try:
            candle = fetch_current_price_candle(market, timeframe)
            if candle is None:
                logger.error(f"[Process-{market}] Failed to fetch current candle for {market} ({timeframe})")
                save_status(market, timeframe, destination_path, "candle_fetch_failed")
                return False
            time_left, next_close_time = get_time_until_candle_close(market, timeframe, candle['time'])
            if time_left is None:
                logger.error(f"[Process-{market}] Failed to calculate candle close time for {market} ({timeframe})")
                save_status(market, timeframe, destination_path, "candle_time_calculation_failed")
                return False
            logger.debug(f"[Process-{market}] Time left until candle close for {market} ({timeframe}): {time_left:.2f} minutes")
            if time_left < 3:
                logger.debug(f"[Process-{market}] Waiting for next candle for {market} ({timeframe})")
                wait_time = (next_close_time - datetime.now(pytz.UTC)).total_seconds() + 10
                logger.debug(f"[Process-{market}] Waiting {wait_time:.2f} seconds for next candle")
                time.sleep(max(wait_time, 0))
                candle = fetch_current_price_candle(market, timeframe)
                if candle is None:
                    logger.error(f"[Process-{market}] Failed to fetch current candle after waiting for {market} ({timeframe})")
                    save_status(market, timeframe, destination_path, "candle_fetch_failed")
                    return False
            mostrecent_completedcandle = mt5.copy_rates_from_pos(market, MT5_TIMEFRAMES[timeframe.upper()], 1, 1)
            if mostrecent_completedcandle is None or len(mostrecent_completedcandle) == 0:
                logger.error(f"[Process-{market}] Failed to fetch previous candle for {market} ({timeframe})")
                save_status(market, timeframe, destination_path, "previous_candle_fetch_failed")
                return False
        finally:
            mt5.shutdown()
    else:
        logger.debug(f"[Process-{market}] Market {market} is outside trading hours, skipping MT5 candle fetch")
        save_status(market, timeframe, destination_path, "market_closed")

    max_retries = 3
    timeout = 1
    reload_attempted = False
    for attempt in range(max_retries):
        try:
            logger.debug(f"[Process-{market}] Processing chart for {market} ({timeframe}, attempt {attempt + 1}/{max_retries})")
            if not check_page_load_status(driver, market, timeout=15):
                logger.error(f"[Process-{market}] Chart canvas not detected")
                save_status(market, timeframe, destination_path, "chart_canvas_not_detected")
                return False
            if not save_chart(driver, timeout, market):
                logger.error(f"[Process-{market}] Failed to save chart for {market} ({timeframe})")
                save_status(market, timeframe, destination_path, "chart_save_failed")
                return False
            downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
            latest_file = wait_for_download(downloads_path, market, timeframe)
            if not latest_file:
                logger.error(f"[Process-{market}] No chart file found in downloads for {market} ({timeframe}), retrying save")
                save_status(market, timeframe, destination_path, "download_failed")
                time.sleep(3)
                if not save_chart(driver, timeout, market):
                    logger.error(f"[Process-{market}] Failed to save chart after retry for {market} ({timeframe})")
                    save_status(market, timeframe, destination_path, "chart_save_failed")
                    return False
                latest_file = wait_for_download(downloads_path, market, timeframe)
                if not latest_file:
                    logger.error(f"[Process-{market}] Still no chart file found for {market} ({timeframe})")
                    save_status(market, timeframe, destination_path, "download_failed")
                    return False
            verified, needs_reload = verify_candlestick_contours(latest_file, market, timeframe)
            if verified:
                destination_file = copy_chart_to_destination(driver, market, timeframe, destination_path)
                if not destination_file:
                    logger.error(f"[Process-{market}] Failed to copy chart for {market} ({timeframe}), retrying save")
                    save_status(market, timeframe, destination_path, "chart_copy_failed")
                    time.sleep(3)
                    if not save_chart(driver, timeout, market):
                        logger.error(f"[Process-{market}] Failed to save chart after retry for {market} ({timeframe})")
                        save_status(market, timeframe, destination_path, "chart_save_failed")
                        return False
                    latest_file = wait_for_download(downloads_path, market, timeframe)
                    if not latest_file:
                        logger.error(f"[Process-{market}] Still no chart file found for {market} ({timeframe})")
                        save_status(market, timeframe, destination_path, "download_failed")
                        return False
                    destination_file = copy_chart_to_destination(driver, market, timeframe, destination_path)
                    if not destination_file:
                        logger.error(f"[Process-{market}] Failed to copy chart after retry for {market} ({timeframe})")
                        save_status(market, timeframe, destination_path, "chart_copy_failed")
                        return False
                if candle is not None:
                    candle_data = {
                        "market": market,
                        "timeframe": timeframe,
                        "open_price": float(candle['open']),
                        "time": datetime.fromtimestamp(candle['time'], tz=pytz.UTC).isoformat()
                    }
                    candle_json_path = os.path.join(market_folder, "currentpricecandle.json")
                    try:
                        with open(candle_json_path, 'w') as f:
                            json.dump(candle_data, f, indent=4)
                        logger.debug(f"[Process-{market}] Saved current candle data to {candle_json_path}")
                    except Exception as e:
                        logger.error(f"[Process-{market}] Error saving current candle data to {candle_json_path}: {e}")
                        save_status(market, timeframe, destination_path, "candle_data_save_failed")
                if mostrecent_completedcandle is not None:
                    open_price = float(mostrecent_completedcandle[0]['open'])
                    close_price = float(mostrecent_completedcandle[0]['close'])
                    candle_color = "green" if close_price > open_price else "red" if close_price < open_price else "neutral"
                    mostrecent_completedcandle_data = {
                        "market": market,
                        "timeframe": timeframe,
                        "open_price": open_price,
                        "close_price": close_price,
                        "high_price": float(mostrecent_completedcandle[0]['high']),
                        "low_price": float(mostrecent_completedcandle[0]['low']),
                        "time": datetime.fromtimestamp(mostrecent_completedcandle[0]['time'], tz=pytz.UTC).isoformat(),
                        "color": candle_color
                    }
                    mostrecent_completedcandle_json_path = os.path.join(market_folder, "mostrecent_completedcandle.json")
                    try:
                        with open(mostrecent_completedcandle_json_path, 'w') as f:
                            json.dump(mostrecent_completedcandle_data, f, indent=4)
                        logger.debug(f"[Process-{market}] Saved previous candle data to {mostrecent_completedcandle_json_path}")
                    except Exception as e:
                        logger.error(f"[Process-{market}] Error saving previous candle data to {mostrecent_completedcandle_json_path}: {e}")
                        save_status(market, timeframe, destination_path, "previous_candle_data_save_failed")
                save_status(market, timeframe, destination_path, "chart_identified")
                return True
            else:
                os.remove(latest_file)
                logger.warning(f"[Process-{market}] Verification failed for {market} ({timeframe})")
                save_status(market, timeframe, destination_path, "chart_verification_failed")
                if attempt < max_retries - 1:
                    logger.debug(f"[Process-{market}] Waiting 3 seconds before retrying save for {market} ({timeframe})")
                    time.sleep(3)
                    continue
                if needs_reload:
                    logger.debug(f"[Process-{market}] Reloading page for {market} ({timeframe})")
                    driver.refresh()
                    wait_for_page_load(driver, timeout=30)
                    if not check_page_load_status(driver, market, timeout=15):
                        logger.error(f"[Process-{market}] Chart canvas not detected after reload")
                        save_status(market, timeframe, destination_path, "chart_canvas_not_detected")
                        return False
                    if not timeframe(driver, timeframe, market):
                        logger.error(f"[Process-{market}] Failed to re-select timeframe {timeframe} after reload")
                        save_status(market, timeframe, destination_path, "timeframe_selection_failed")
                        return False
                    time.sleep(5)
                    if not save_chart(driver, timeout, market):
                        logger.error(f"[Process-{market}] Failed to save chart after reload for {market} ({timeframe})")
                        save_status(market, timeframe, destination_path, "chart_save_failed")
                        return False
                    latest_file = wait_for_download(downloads_path, market, timeframe)
                    if not latest_file:
                        logger.error(f"[Process-{market}] No chart file found after reload for {market} ({timeframe})")
                        save_status(market, timeframe, destination_path, "download_failed")
                        return False
                    verified, _ = verify_candlestick_contours(latest_file, market, timeframe)
                    if verified:
                        destination_file = copy_chart_to_destination(driver, market, timeframe, destination_path)
                        if not destination_file:
                            logger.error(f"[Process-{market}] Failed to copy chart after reload for {market} ({timeframe})")
                            save_status(market, timeframe, destination_path, "chart_copy_failed")
                            return False
                        if candle is not None:
                            candle_data = {
                                "market": market,
                                "timeframe": timeframe,
                                "open_price": float(candle['open']),
                                "time": datetime.fromtimestamp(candle['time'], tz=pytz.UTC).isoformat()
                            }
                            candle_json_path = os.path.join(market_folder, "currentpricecandle.json")
                            try:
                                with open(candle_json_path, 'w') as f:
                                    json.dump(candle_data, f, indent=4)
                                logger.debug(f"[Process-{market}] Saved current candle data to {candle_json_path}")
                            except Exception as e:
                                logger.error(f"[Process-{market}] Error saving current candle data to {candle_json_path}: {e}")
                                save_status(market, timeframe, destination_path, "candle_data_save_failed")
                        if mostrecent_completedcandle is not None:
                            open_price = float(mostrecent_completedcandle[0]['open'])
                            close_price = float(mostrecent_completedcandle[0]['close'])
                            candle_color = "green" if close_price > open_price else "red" if close_price < open_price else "neutral"
                            mostrecent_completedcandle_data = {
                                "market": market,
                                "timeframe": timeframe,
                                "open_price": open_price,
                                "close_price": close_price,
                                "high_price": float(mostrecent_completedcandle[0]['high']),
                                "low_price": float(mostrecent_completedcandle[0]['low']),
                                "time": datetime.fromtimestamp(mostrecent_completedcandle[0]['time'], tz=pytz.UTC).isoformat(),
                                "color": candle_color
                            }
                            mostrecent_completedcandle_json_path = os.path.join(market_folder, "mostrecent_completedcandle.json")
                            try:
                                with open(mostrecent_completedcandle_json_path, 'w') as f:
                                    json.dump(mostrecent_completedcandle_data, f, indent=4)
                                logger.debug(f"[Process-{market}] Saved previous candle data to {mostrecent_completedcandle_json_path}")
                            except Exception as e:
                                logger.error(f"[Process-{market}] Error saving previous candle data to {mostrecent_completedcandle_json_path}: {e}")
                                save_status(market, timeframe, destination_path, "previous_candle_data_save_failed")
                        save_status(market, timeframe, destination_path, "chart_identified")
                        return True
                    else:
                        os.remove(latest_file)
                        logger.warning(f"[Process-{market}] Verification failed after reload for {market} ({timeframe})")
                        save_status(market, timeframe, destination_path, "chart_verification_failed")
                        return False
                return False
        except Exception as e:
            logger.error(f"[Process-{market}] Error in download_and_verify_chart for {market} ({timeframe}): {e}")
            save_status(market, timeframe, destination_path, "unexpected_error")
            return False
    return False

def marketsstatus(destination_path, markets, timeframes):
    """Generate a status report for all markets based on their verification.json files."""
    logger.debug("Generating market status report")
    try:
        chart_identified_markets = {}
        incomplete_markets = {}

        for market in markets:
            verification_file = os.path.join(destination_path, market.replace(" ", "_"), "verification.json")
            if not os.path.exists(verification_file):
                logger.warning(f"No verification.json found for {market}, marking all timeframes as incomplete")
                incomplete_markets[market] = {
                    "timeframes": timeframes,
                    "reason": "missing_verification_file"
                }
                continue

            try:
                with open(verification_file, 'r') as f:
                    verification_data = json.load(f)
                identified_timeframes = []
                incomplete_timeframes = []
                reasons = []

                for tf in timeframes:
                    normalized_tf = normalize_timeframe(tf)
                    status = verification_data.get(tf.lower(), "missing_status")
                    status_file = os.path.join(destination_path, market.replace(" ", "_"), normalized_tf, "status.json")
                    
                    if status == "chart_identified":
                        identified_timeframes.append(tf)
                    else:
                        incomplete_timeframes.append(tf)
                        # Determine reason based on status or status.json
                        if status == "missing_status":
                            if os.path.exists(status_file):
                                try:
                                    with open(status_file, 'r') as f_status:
                                        status_data = json.load(f_status)
                                        status_reason = status_data.get("status", "incomplete")
                                        reasons.append(f"{tf}: {status_reason}")
                                except Exception as e:
                                    logger.error(f"Error reading status.json for {market} ({tf}): {e}")
                                    reasons.append(f"{tf}: error_reading_status")
                            else:
                                reasons.append(f"{tf}: missing_status_file")
                        else:
                            reasons.append(f"{tf}: {status}")

                if identified_timeframes:
                    chart_identified_markets[market] = identified_timeframes
                if incomplete_timeframes:
                    # Combine reasons into a single string or structure as needed
                    reason_summary = "; ".join(set(reasons))  # Use set to avoid duplicates
                    incomplete_markets[market] = {
                        "timeframes": incomplete_timeframes,
                        "reason": reason_summary
                    }

            except Exception as e:
                logger.error(f"Error reading verification.json for {market}: {e}")
                incomplete_markets[market] = {
                    "timeframes": timeframes,
                    "reason": "error_reading_verification"
                }

        status_data = {
            "chart_identified_markets": chart_identified_markets,
            "incomplete_markets": incomplete_markets,
            "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime(
                "%Y-%m-%d T %I:%M:%S %p .%f+01:00"
            )
        }

        output_file = os.path.join(destination_path, "fetchmarketstatus.json")
        os.makedirs(destination_path, exist_ok=True)
        with open(output_file, 'w') as f:
            json.dump(status_data, f, indent=4)
        logger.debug(f"Saved market status report to {output_file}")
        return True

    except Exception as e:
        logger.error(f"Error generating market status report: {e}")
        return False
    
def timeframeselligibilityupdater(*, timeframe, elligible_status):
    """
    Configure eligible timeframes for processing by setting elligible_status and status in status.json.
    This is a settings function called once at script start to specify which timeframes to process.
    
    Args:
        timeframe (str): Comma-separated list of timeframes (e.g., "m5,m15,m30,1hour,4hours").
        elligible_status (str): Status to set ("order_free" or "chart_identified").
    
    Returns:
        bool: True if successful, False otherwise.
    """
    logger.debug(f"Configuring timeframes with timeframe: {timeframe}, elligible_status: {elligible_status}")
    try:
        # Validate inputs
        if not timeframe:
            logger.error("Timeframe parameter is empty")
            return False
        
        if not elligible_status:
            logger.error("Elligible_status parameter is empty")
            return False
        
        # Validate status
        if elligible_status not in ["order_free", "chart_identified"]:
            logger.error(f"Invalid elligible_status: {elligible_status}. Must be 'order_free' or 'chart_identified'")
            return False
        
        # Parse timeframes and normalize
        input_timeframes = [tf.strip() for tf in timeframe.split(",")]
        valid_timeframes = []
        for tf in input_timeframes:
            normalized_tf = normalize_timeframe(tf)
            if normalized_tf in [normalize_timeframe(t) for t in TIMEFRAMES]:
                valid_timeframes.append(normalized_tf)
            else:
                logger.warning(f"Ignoring invalid timeframe: {tf}")
        
        if not valid_timeframes:
            logger.error("No valid timeframes provided")
            return False
        
        logger.debug(f"Configuring valid timeframes: {valid_timeframes}, elligible_status: {elligible_status}")
        
        # Update or create status.json for each market and timeframe
        for market in MARKETS:
            for tf in TIMEFRAMES:
                normalized_tf = normalize_timeframe(tf)
                # Only process specified timeframes
                if normalized_tf in valid_timeframes:
                    try:
                        market_folder = os.path.join(DESTINATION_PATH, market.replace(" ", "_"), normalized_tf)
                        status_file = os.path.join(market_folder, "status.json")
                        os.makedirs(market_folder, exist_ok=True)
                        
                        # Get current time in WAT (Africa/Lagos, UTC+1)
                        current_time = datetime.now(pytz.timezone('Africa/Lagos'))
                        am_pm = "am" if current_time.hour < 12 else "pm"
                        hour_12 = current_time.hour % 12 or 12
                        timestamp = (
                            f"{current_time.strftime('%Y-%m-%d T %I:%M:%S')} {am_pm} "
                            f".{current_time.microsecond:06d}+01:00"
                        )
                        
                        # Load existing status.json or create new
                        status_data = {
                            "market": market,
                            "timeframe": tf,
                            "normalized_timeframe": normalized_tf,
                            "timestamp": timestamp,
                            "elligible_status": elligible_status,
                            "status": elligible_status  # Set status to the same value as elligible_status
                        }
                        
                        if os.path.exists(status_file):
                            try:
                                with open(status_file, 'r') as f:
                                    existing_data = json.load(f)
                                # Update only the specified fields, preserving others
                                existing_data.update({
                                    "elligible_status": elligible_status,
                                    "status": elligible_status,  # Update status to match elligible_status
                                    "timestamp": timestamp
                                })
                                status_data = existing_data
                            except Exception as e:
                                logger.warning(f"[Process-{market}] Error reading existing status.json for {market} ({tf}): {e}")
                        
                        # Write updated status.json
                        with open(status_file, 'w') as f:
                            json.dump(status_data, f, indent=4)
                        logger.debug(f"[Process-{market}] Configured status.json for {market} ({tf}) with elligible_status: {elligible_status}, status: {status_data['status']}")
                    except Exception as e:
                        logger.error(f"[Process-{market}] Error configuring status.json for {market} ({tf}): {e}")
                else:
                    # For non-specified timeframes, set elligible_status to a non-processable state if needed
                    try:
                        market_folder = os.path.join(DESTINATION_PATH, market.replace(" ", "_"), normalized_tf)
                        status_file = os.path.join(market_folder, "status.json")
                        if os.path.exists(status_file):
                            with open(status_file, 'r') as f:
                                existing_data = json.load(f)
                            if existing_data.get("elligible_status") != "chart_identified":
                                existing_data["elligible_status"] = "inactive"
                                existing_data["timestamp"] = datetime.now(pytz.timezone('Africa/Lagos')).strftime(
                                    "%Y-%m-%d T %I:%M:%S %p .%f+01:00"
                                )
                                with open(status_file, 'w') as f:
                                    json.dump(existing_data, f, indent=4)
                                logger.debug(f"[Process-{market}] Set elligible_status to 'inactive' for non-specified timeframe {tf} of {market}")
                    except Exception as e:
                        logger.error(f"[Process-{market}] Error updating non-specified timeframe {tf} for {market}: {e}")
        
        logger.debug("Completed configuring eligible timeframes")
        return True
    
    except Exception as e:
        logger.error(f"Error in timeframeselligibilityupdater: {e}")
        return False
        
def run_script_for_market(market, eligible_pairs, processed_pairs):
    """Process a single market for eligible timeframes with elligible_status 'order_free'."""
    driver = None
    try:
        # Get eligible timeframes for this market
        eligible_timeframes = [tf for m, tf in eligible_pairs if m == market]
        if not eligible_timeframes:
            logger.debug(f"[Process-{market}] No eligible timeframes for {market}, skipping")
            create_verification_json(market, DESTINATION_PATH)
            return True

        while True:
            logger.debug(f"[Process-{market}] Processing market: {market} with timeframes: {eligible_timeframes}")
            driver = operate("headed")
            # Save initial status for eligible timeframes
            for tf in eligible_timeframes:
                save_status(market, tf, DESTINATION_PATH, "starting")
            if not login(driver, LOGIN_ID, PASSWORD, SERVER, market):
                logger.error(f"[Process-{market}] Login failed for {market}, restarting")
                for tf in eligible_timeframes:
                    save_status(market, tf, DESTINATION_PATH, "login_failed")
                driver.quit()
                create_verification_json(market, DESTINATION_PATH)
                time.sleep(10)
                continue
            if not tradewindow(driver, 'close', market):
                logger.warning(f"[Process-{market}] Failed to close trade window for {market}, proceeding")
            if not search(driver, market):
                logger.error(f"[Process-{market}] Failed to select market '{market}', restarting")
                for tf in eligible_timeframes:
                    save_status(market, tf, DESTINATION_PATH, "market_selection_failed")
                driver.quit()
                create_verification_json(market, DESTINATION_PATH)
                time.sleep(10)
                continue
            # Verify market selection
            try:
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        f"//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{market.lower()}') and "
                        f"(contains(@class, 'symbol') or contains(@class, 'title') or contains(@class, 'chart-header'))]"
                    ))
                )
                logger.debug(f"[Process-{market}] Confirmed market '{market}' is selected")
            except TimeoutException:
                logger.error(f"[Process-{market}] Market '{market}' not confirmed as selected, restarting")
                for tf in eligible_timeframes:
                    save_status(market, tf, DESTINATION_PATH, "market_confirmation_failed")
                driver.quit()
                create_verification_json(market, DESTINATION_PATH)
                time.sleep(10)
                continue
            if not watchlist(driver, 'close', market):
                logger.warning(f"[Process-{market}] Failed to close watchlist for {market}, proceeding")

            success = True
            for tf in eligible_timeframes:
                logger.debug(f"[Process-{market}] Processing timeframe {tf} for {market}")
                if not timeframe(driver, tf, market):
                    logger.error(f"[Process-{market}] Failed to select timeframe {tf} for {market}")
                    save_status(market, tf, DESTINATION_PATH, "timeframe_selection_failed")
                    success = False
                    break
                result = download_and_verify_chart(driver, market, tf, DESTINATION_PATH)
                if not result:
                    logger.error(f"[Process-{market}] Failed to process chart for {market} ({tf})")
                    success = False
                    break
                else:
                    processed_pairs.append((market, tf))  # Track successful market-timeframe pair
            driver.quit()
            driver = None
            create_verification_json(market, DESTINATION_PATH)
            if success:
                logger.debug(f"[Process-{market}] All eligible timeframes processed successfully for {market}")
                return True
            logger.error(f"[Process-{market}] Failed to process some timeframes for {market}, restarting")
            time.sleep(10)
    except KeyboardInterrupt:
        logger.error(f"[Process-{market}] Interrupted by user for {market}")
        for tf in eligible_timeframes:
            save_status(market, tf, DESTINATION_PATH, "interrupted")
        if driver:
            driver.quit()
        create_verification_json(market, DESTINATION_PATH)
        return False
    except Exception as e:
        logger.error(f"[Process-{market}] Unexpected error for {market}: {e}")
        for tf in eligible_timeframes:
            save_status(market, tf, DESTINATION_PATH, "unexpected_error")
        if driver:
            driver.quit()
        create_verification_json(market, DESTINATION_PATH)
        time.sleep(10)
        return False
    
def test_all_symbols():
    """Test availability of all markets in MT5."""
    if not mt5.initialize(path=TERMINAL_PATH, timeout=60000):
        logger.error("Failed to initialize MT5 for symbol check")
        return False
    unavailable_symbols = []
    for market in MARKETS:
        if not mt5.symbol_select(market, True):
            logger.error(f"Symbol {market} not available: {mt5.last_error()}")
            unavailable_symbols.append(market)
        else:
            symbol_info = mt5.symbol_info(market)
            if symbol_info is None or not symbol_info.visible:
                logger.error(f"Symbol {market} not visible in MT5")
                unavailable_symbols.append(market)
    mt5.shutdown()
    if unavailable_symbols:
        logger.error(f"Unavailable symbols: {unavailable_symbols}")
        return False
    return True

def is_pair_completed(market, timeframe):
    """Check if a market-timeframe pair is completed (chart_identified or market_closed)."""
    status_file = os.path.join(DESTINATION_PATH, market.replace(" ", "_"), normalize_timeframe(timeframe), "status.json")
    if os.path.exists(status_file):
        try:
            with open(status_file, 'r') as f:
                status_data = json.load(f)
                status = status_data.get("status")
                return status in ["chart_identified", "market_closed"]
        except Exception as e:
            logger.error(f"[Process-{market}] Error checking status for {market} ({timeframe}): {e}")
            return False
    return False

def main():
    """Main loop with market categorization, symbol pre-check, and verification.json creation."""
    logger.debug("Starting main loop")
    clear_all_market_files()
    
    # Check all symbols for availability in MT5
    if not test_all_symbols():
        logger.error("Symbol availability check failed. Creating verification.json for all markets.")
        for market in MARKETS:
            create_verification_json(market, DESTINATION_PATH)
        marketsstatus(DESTINATION_PATH, MARKETS, TIMEFRAMES)
        return False
    
    # Validate required data
    if not MARKETS or not TIMEFRAMES or not FOREX_MARKETS or not SYNTHETIC_INDICES or \
       not INDEX_MARKETS or not all([LOGIN_ID, PASSWORD, SERVER, BASE_URL, TERMINAL_PATH]):
        logger.error("Required lists or credentials missing. Exiting.")
        for market in MARKETS:
            create_verification_json(market, DESTINATION_PATH)
        marketsstatus(DESTINATION_PATH, MARKETS, TIMEFRAMES)
        return False
    
    # Categorize markets by trading status
    open_markets, closed_markets = categorize_markets_by_trading_status(MARKETS, DESTINATION_PATH, TIMEFRAMES)
    
    # Get eligible market-timeframe pairs for open markets only
    eligible_pairs = get_eligible_market_timeframes()
    eligible_pairs = [(market, tf) for market, tf in eligible_pairs if market in open_markets]
    markets_to_process = list(set(pair[0] for pair in eligible_pairs))
    processed_pairs = []
    
    if not markets_to_process:
        logger.debug("No open markets with elligible_status 'order_free' found")
        for market in MARKETS:
            create_verification_json(market, DESTINATION_PATH)
        marketsstatus(DESTINATION_PATH, MARKETS, TIMEFRAMES)
        return True
    
    batch_size = 10
    batch_attempts = 0
    
    while markets_to_process:
        batch_attempts += 1
        logger.debug(f"Batch attempt {batch_attempts} for markets: {markets_to_process}")
        current_batch = markets_to_process[:batch_size]
        failed_markets = []
        processes = []
        try:
            logger.debug(f"Processing batch: {current_batch}")
            for market in current_batch:
                process = multiprocessing.Process(target=run_script_for_market, args=(market, eligible_pairs, processed_pairs))
                processes.append((market, process))
                process.start()
            for market, process in processes:
                process.join()
                if process.exitcode != 0:
                    logger.error(f"[Process-{market}] Process failed")
                    failed_markets.append(market)
            if len(failed_markets) >= 5:
                handle_network_issue()
            
            # Refresh eligible pairs for open markets
            eligible_pairs = get_eligible_market_timeframes()
            eligible_pairs = [(market, tf) for market, tf in eligible_pairs if market in open_markets]
            markets_to_process = list(set(
                pair[0] for pair in eligible_pairs
                if pair not in processed_pairs and not is_pair_completed(pair[0], pair[1])
            ))
            
            logger.debug(f"Remaining markets to process: {markets_to_process}")
            logger.debug(f"Processed pairs: {processed_pairs}")
            
            if not markets_to_process:
                logger.debug("All eligible market-timeframe pairs for open markets are chart_identified or market_closed")
                break
                
        except Exception as e:
            logger.error(f"Main loop error: {e}")
            for _, process in processes:
                if process.is_alive():
                    process.terminate()
            if len(markets_to_process) >= 5:
                handle_network_issue()
            eligible_pairs = get_eligible_market_timeframes()
            eligible_pairs = [(market, tf) for market, tf in eligible_pairs if market in open_markets]
            markets_to_process = list(set(
                pair[0] for pair in eligible_pairs
                if pair not in processed_pairs and not is_pair_completed(pair[0], pair[1])
            ))
            time.sleep(10)
    
    # Ensure verification.json is created for all markets (open and closed)
    for market in MARKETS:
        create_verification_json(market, DESTINATION_PATH)
    
    # Generate final status report for all markets
    marketsstatus(DESTINATION_PATH, MARKETS, TIMEFRAMES)
    
    # Log and print final processed pairs
    formatted_pairs = [f"{market}({tf})" for market, tf in processed_pairs]
    logger.debug(f"Eligible processed markets: {formatted_pairs}")
    print(f'Eligible processed markets[{", ".join(formatted_pairs)}]')
    
    # Final verification
    all_completed = True
    for market, tf in eligible_pairs:
        if not is_pair_completed(market, tf):
            all_completed = False
            status_file = os.path.join(DESTINATION_PATH, market.replace(" ", "_"), normalize_timeframe(tf), "status.json")
            if os.path.exists(status_file):
                try:
                    with open(status_file, 'r') as f:
                        status_data = json.load(f)
                        logger.warning(f"[Process-{market}] {market} ({tf}) not completed: status={status_data.get('status')}, elligible_status={status_data.get('elligible_status')}")
                except Exception as e:
                    logger.error(f"[Process-{market}] Error reading status.json for {market} ({tf}): {e}")
            else:
                logger.warning(f"[Process-{market}] status.json missing for {market} ({tf})")
    
    if all_completed:
        logger.debug("Main loop completed: all eligible market-timeframe pairs for open markets are chart_identified")
        return True
    else:
        logger.error("Main loop completed but not all eligible market-timeframe pairs for open markets are chart_identified")
        return False

if __name__ == "__main__":
    try:
        clear_all_market_files()
        main()
    except Exception as e:
        logger.error(f"Main loop failed: {e}")


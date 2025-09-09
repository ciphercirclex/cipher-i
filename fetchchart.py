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
MARKETS_JSON_PATH = r"C:\xampp\htdocs\CIPHER\cipher i\bouncestream\chart\base.json"

# Function to load markets, timeframes, and credentials from JSON
def load_markets_and_credentials(json_path):
    """Load MARKETS, FOREX_MARKETS, SYNTHETIC_INDICES, INDEX_MARKETS, TIMEFRAMES, and CREDENTIALS from base.json file."""
    try:
        if not os.path.exists(json_path):
            raise FileNotFoundError(f"Markets JSON file not found at: {json_path}")
        with open(json_path, 'r') as f:
            data = json.load(f)
        markets = data.get("MARKETS", [])
        forex_markets = data.get("FOREX_MARKETS", [])
        synthetic_indices = data.get("SYNTHETIC_INDICES", [])
        index_markets = data.get("INDEX_MARKETS", [])
        timeframes = data.get("TIMEFRAMES", [])
        credentials = data.get("CREDENTIALS", {})
        login_id = credentials.get("LOGIN_ID", "")
        password = credentials.get("PASSWORD", "")
        server = credentials.get("SERVER", "")
        base_url = credentials.get("BASE_URL", "")
        terminal_path = credentials.get("TERMINAL_PATH", "")
        if not markets or not timeframes or not all([login_id, password, server, base_url, terminal_path]):
            raise ValueError("MARKETS, TIMEFRAMES, or CREDENTIALS not found in base.json or are empty")
        logger.debug(f"Loaded MARKETS: {markets}")
        logger.debug(f"Loaded FOREX_MARKETS: {forex_markets}")
        logger.debug(f"Loaded SYNTHETIC_INDICES: {synthetic_indices}")
        logger.debug(f"Loaded INDEX_MARKETS: {index_markets}")
        logger.debug(f"Loaded TIMEFRAMES: {timeframes}")
        logger.debug("Loaded CREDENTIALS: [Sensitive data not logged]")
        return markets, forex_markets, synthetic_indices, index_markets, timeframes, login_id, password, server, base_url, terminal_path
    except Exception as e:
        logger.error(f"Error loading base.json: {e}")
        return [], [], [], [], [], "", "", "", "", ""

# Load MARKETS, FOREX_MARKETS, SYNTHETIC_INDICES, INDEX_MARKETS, TIMEFRAMES, and credentials from JSON
MARKETS, FOREX_MARKETS, SYNTHETIC_INDICES, INDEX_MARKETS, TIMEFRAMES, LOGIN_ID, PASSWORD, SERVER, BASE_URL, TERMINAL_PATH = load_markets_and_credentials(MARKETS_JSON_PATH)

MT5_TIMEFRAMES = {
    "M5": mt5.TIMEFRAME_M5,
    "M15": mt5.TIMEFRAME_M15,
    "M30": mt5.TIMEFRAME_M30,
    "H1": mt5.TIMEFRAME_H1,
    "H4": mt5.TIMEFRAME_H4
}
DESTINATION_PATH = r"C:\xampp\htdocs\CIPHER\cipher i\bouncestream\chart\fetched"

# Shared multiprocessing variables
network_issue_event = multiprocessing.Event()
network_resolution_lock = multiprocessing.Lock()

def clear_all_market_files():
    """Clear all market-related PNG files in Downloads and destination folders once at script start."""
    logger.debug("Clearing all market-related files in Downloads and destination folders")
    downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
    
    # Clear market-related files from Downloads folder
    try:
        for market in MARKETS:
            market_files = [f for f in os.listdir(downloads_path) 
                           if os.path.isfile(os.path.join(downloads_path, f)) and market in f]
            for file in market_files:
                file_path = os.path.join(downloads_path, file)
                try:
                    os.remove(file_path)
                    logger.debug(f"Deleted file in Downloads: {file_path}")
                except Exception as e:
                    logger.error(f"Error deleting file {file_path}: {e}")
        logger.debug(f"Cleared {len(market_files)} files from Downloads folder")
    except Exception as e:
        logger.error(f"Error clearing Downloads folder: {e}")

    # Clear market-related files from destination folders
    for market in MARKETS:
        market_folder = os.path.join(DESTINATION_PATH, market.replace(" ", "_"))
        for tf in TIMEFRAMES:
            timeframe_folder = os.path.join(market_folder, tf.lower())
            try:
                if os.path.exists(timeframe_folder):
                    files = [f for f in os.listdir(timeframe_folder) 
                             if os.path.isfile(os.path.join(timeframe_folder, f))]
                    for file in files:
                        file_path = os.path.join(timeframe_folder, file)
                        try:
                            os.remove(file_path)
                            logger.debug(f"Deleted file in {timeframe_folder}: {file_path}")
                        except Exception as e:
                            logger.error(f"Error deleting file {file_path}: {e}")
            except Exception as e:
                logger.error(f"Error clearing timeframe folder {timeframe_folder}: {e}")
    logger.debug("Completed clearing all market-related files")

def tradinghoursordays(market):
    """Check if the market is within its trading hours or days."""
    current_time = datetime.now(pytz.UTC)
    current_day = current_time.weekday()
    current_hour = current_time.hour
    logger.debug(f"[Process-{market}] Checking trading hours for {market}. Current UTC time: {current_time}, Day: {current_day}, Hour: {current_hour}")

    if market in SYNTHETIC_INDICES:
        return True
    elif market in FOREX_MARKETS:
        start_time = datetime.now(pytz.UTC).replace(hour=22, minute=0, second=0, microsecond=0)
        if current_day == 6:
            if current_time < start_time:
                return False
            return True
        elif current_day == 4:
            if current_time >= start_time:
                return False
            return True
        elif current_day == 5:
            return False
        else:
            return True
    elif market in INDEX_MARKETS:
        if current_day in [5, 6]:
            return False
        elif current_hour < 1 or current_hour >= 22:
            return False
        return True
    else:
        logger.warning(f"[Process-{market}] Unknown market type for {market}, assuming 24/7 trading")
        return True

def initialize_mt5(market):
    """Initialize MT5 connection with retries."""
    logger.debug(f"[Process-{market}] Initializing MT5 for {market}")
    for attempt in range(3):
        if mt5.initialize(path=TERMINAL_PATH, timeout=60000):
            break
        logger.error(f"[Process-{market}] Attempt {attempt + 1}/3: Failed to initialize MT5 terminal. Error: {mt5.last_error()}")
        time.sleep(5)
    else:
        logger.error(f"[Process-{market}] Failed to initialize MT5 terminal after 3 attempts")
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
    for attempt in range(3):
        candles = mt5.copy_rates_from_pos(market, MT5_TIMEFRAMES[timeframe.upper()], 0, 1)
        if candles is None or len(candles) == 0:
            logger.error(f"[Process-{market}] Attempt {attempt + 1}/3: Failed to fetch candle data for {market} ({timeframe}), error: {mt5.last_error()}")
            time.sleep(2)
            continue
        current_time = datetime.now(pytz.UTC)
        candle_time = datetime.fromtimestamp(candles[0]['time'], tz=pytz.UTC)
        timeframe_minutes = {"M5": 6, "M15": 16, "M30": 31, "H1": 61, "H4": 241}
        if (current_time - candle_time).total_seconds() > timeframe_minutes[timeframe.upper()] * 60:
            logger.error(f"[Process-{market}] Attempt {attempt + 1}/3: Candle for {market} ({timeframe}) is too old (time: {candle_time})")
            time.sleep(2)
            continue
        return candles[0]
    logger.error(f"[Process-{market}] Failed to fetch recent candle data for {market} ({timeframe}) after 3 attempts")
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
    """Search for a market and confirm selection by chart canvas presence."""
    try:
        logger.debug(f"[Process-{market}] Searching for market '{market}'")
        # Locate and interact with the search bar
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

    except TimeoutException:
        logger.error(f"[Process-{market}] Timeout waiting for search results or chart canvas for '{market}'")
        return False
    except Exception as e:
        logger.error(f"[Process-{market}] Error searching for '{market}': {e}")
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
        market_folder = os.path.join(destination_path, market.replace(" ", "_"), timeframe.lower())
        os.makedirs(market_folder, exist_ok=True)
        latest_file = wait_for_download(downloads_path, market, timeframe)
        if not latest_file:
            logger.error(f"[Process-{market}] No chart file found for {market} ({timeframe})")
            return False
        # Create new filename in the format market_timeframe.png
        new_filename = f"{market.replace(' ', '_')}_{timeframe.lower()}.png"
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
    market_folder = os.path.join(DESTINATION_PATH, market.replace(" ", "_"), timeframe.lower())
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
        market_folder = os.path.join(destination_path, market.replace(" ", "_"), timeframe.lower())
        os.makedirs(market_folder, exist_ok=True)
        status_file = os.path.join(market_folder, "status.json")
        # Get current time in WAT (Africa/Lagos, UTC+1)
        current_time = datetime.now(pytz.timezone('Africa/Lagos'))
        # Format timestamp as "YYYY-MM-DD T HH:MM:SS am/pm .microseconds+HH:MM"
        am_pm = "am" if current_time.hour < 12 else "pm"
        hour_12 = current_time.hour % 12
        if hour_12 == 0:
            hour_12 = 12  # Convert 0 to 12 for 12 AM/PM
        timestamp = (
            f"{current_time.strftime('%Y-%m-%d T %I:%M:%S')} {am_pm} "
            f".{current_time.microsecond:06d}+01:00"
        )
        status_data = {
            "market": market,
            "timeframe": timeframe,
            "status": status,
            "timestamp": timestamp
        }
        with open(status_file, 'w') as f:
            json.dump(status_data, f, indent=4)
        logger.debug(f"[Process-{market}] Saved status '{status}' to {status_file}")
    except Exception as e:
        logger.error(f"[Process-{market}] Error saving status for {market} ({timeframe}): {e}")

def create_verification_json(market, destination_path):
    """Create verification.json for a market by collecting statuses from each timeframe's status.json."""
    try:
        market_folder = os.path.join(destination_path, market.replace(" ", "_"))
        verification_data = {}
        all_identified = True

        for tf in TIMEFRAMES:
            status_file = os.path.join(market_folder, tf.lower(), "status.json")
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
                logger.warning(f"[Process-{market}] status.json not found for {market} ({tf})")
                verification_data[tf.lower()] = "missing_status"
                all_identified = False

        verification_data["all_timeframes"] = "verified" if all_identified else "incomplete_verification"
        verification_file = os.path.join(market_folder, "verification.json")
        os.makedirs(market_folder, exist_ok=True)
        with open(verification_file, 'w') as f:
            json.dump(verification_data, f, indent=4)
        logger.debug(f"[Process-{market}] Saved verification.json to {verification_file}")
    except Exception as e:
        logger.error(f"[Process-{market}] Error creating verification.json for {market}: {e}")

def download_and_verify_chart(driver, market, timeframe, destination_path, max_timeout=30):
    """Download and verify chart, retrying up to 2 times within session, and save candle data and status."""
    market_folder = os.path.join(destination_path, market.replace(" ", "_"), timeframe.lower())
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
                wait_time = (next_close_time - datetime.now(pytz.UTC)).total_seconds() + 10  # Wait until after candle closes
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

    max_retries = 3  # Initial attempt + 2 retries
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
                incomplete_markets[market] = timeframes
                continue

            try:
                with open(verification_file, 'r') as f:
                    verification_data = json.load(f)
                identified_timeframes = []
                incomplete_timeframes = []

                for tf in timeframes:
                    status = verification_data.get(tf.lower(), "missing_status")
                    if status == "chart_identified":
                        identified_timeframes.append(tf)
                    else:
                        incomplete_timeframes.append(tf)

                if identified_timeframes:
                    chart_identified_markets[market] = identified_timeframes
                if incomplete_timeframes:
                    incomplete_markets[market] = incomplete_timeframes

            except Exception as e:
                logger.error(f"Error reading verification.json for {market}: {e}")
                incomplete_markets[market] = timeframes

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

def run_script_for_market(market):
    """Process a single market for all timeframes."""
    driver = None
    try:
        while True:
            logger.debug(f"[Process-{market}] Processing market: {market}")
            driver = operate("headless")
            # Save initial status for all timeframes
            for tf in TIMEFRAMES:
                save_status(market, tf, DESTINATION_PATH, "starting")
            if not login(driver, LOGIN_ID, PASSWORD, SERVER, market):
                logger.error(f"[Process-{market}] Login failed for {market}, restarting")
                for tf in TIMEFRAMES:
                    save_status(market, tf, DESTINATION_PATH, "login_failed")
                driver.quit()
                create_verification_json(market, DESTINATION_PATH)  # Create verification.json on failure
                time.sleep(10)
                continue
            if not tradewindow(driver, 'close', market):
                logger.warning(f"[Process-{market}] Failed to close trade window for {market}, proceeding")
            if not search(driver, market):
                logger.error(f"[Process-{market}] Failed to select market '{market}', restarting")
                for tf in TIMEFRAMES:
                    save_status(market, tf, DESTINATION_PATH, "market_selection_failed")
                driver.quit()
                create_verification_json(market, DESTINATION_PATH)  # Create verification.json on failure
                time.sleep(10)
                continue
            if not watchlist(driver, 'close', market):
                logger.warning(f"[Process-{market}] Failed to close watchlist for {market}, proceeding")

            success = True
            for tf in TIMEFRAMES:
                logger.debug(f"[Process-{market}] Processing timeframe {tf} for {market}")
                if not timeframe(driver, tf, market):
                    logger.error(f"[Process-{market}] Failed to select timeframe {tf} for {market}")
                    save_status(market, tf, DESTINATION_PATH, "timeframe_selection_failed")
                    success = False
                    break
                result = download_and_verify_chart(driver, market, tf, DESTINATION_PATH)
                if not result:
                    logger.error(f"[Process-{market}] Failed to process chart for {market} ({tf})")
                    # Status is already set in download_and_verify_chart
                    success = False
                    break
            driver.quit()
            driver = None
            create_verification_json(market, DESTINATION_PATH)  # Create verification.json after processing
            if success:
                logger.debug(f"[Process-{market}] All timeframes processed successfully for {market}")
                return True
            logger.error(f"[Process-{market}] Failed to process some timeframes for {market}, restarting")
            time.sleep(10)
    except KeyboardInterrupt:
        logger.error(f"[Process-{market}] Interrupted by user for {market}")
        for tf in TIMEFRAMES:
            save_status(market, tf, DESTINATION_PATH, "interrupted")
        if driver:
            driver.quit()
        create_verification_json(market, DESTINATION_PATH)  # Create verification.json on interruption
        return False
    except Exception as e:
        logger.error(f"[Process-{market}] Unexpected error for {market}: {e}")
        for tf in TIMEFRAMES:
            save_status(market, tf, DESTINATION_PATH, "unexpected_error")
        if driver:
            driver.quit()
        create_verification_json(market, DESTINATION_PATH)  # Create verification.json on error
        time.sleep(10)
        return False

def main():
    """Main loop to process all markets until all are verified."""
    logger.debug("Starting main loop")
    
    # Check if required lists and credentials are loaded
    if not MARKETS or not TIMEFRAMES or not FOREX_MARKETS or not SYNTHETIC_INDICES or not INDEX_MARKETS or not all([LOGIN_ID, PASSWORD, SERVER, BASE_URL, TERMINAL_PATH]):
        logger.error("One or more required lists (MARKETS, FOREX_MARKETS, SYNTHETIC_INDICES, INDEX_MARKETS, TIMEFRAMES) or credentials (LOGIN_ID, PASSWORD, SERVER, BASE_URL, TERMINAL_PATH) are empty. Check base.json file. Exiting.")
        return False
    
    markets_to_process = MARKETS.copy()
    while markets_to_process:
        failed_markets = []
        processes = []
        try:
            logger.debug(f"Processing markets: {markets_to_process}")
            for market in markets_to_process:
                logger.debug(f"Starting process for {market}")
                process = multiprocessing.Process(target=run_script_for_market, args=(market,))
                processes.append((market, process))
                process.start()
            for market, process in processes:
                process.join()
                if process.exitcode != 0:
                    logger.error(f"[Process-{market}] Process failed for {market}")
                    failed_markets.append(market)
                else:
                    logger.debug(f"[Process-{market}] Market {market} processed successfully")
            if len(failed_markets) >= 5:
                logger.debug(f"Detected {len(failed_markets)} market failures, calling handle_network_issue")
                handle_network_issue()
            markets_to_process = failed_markets
            if not markets_to_process:
                logger.debug("All markets processed successfully")
                # Generate market status report after successful processing
                if not marketsstatus(DESTINATION_PATH, MARKETS, TIMEFRAMES):
                    logger.error("Failed to generate market status report")
                return True
            logger.debug(f"Markets failed: {failed_markets}. Retrying...")
            time.sleep(10)
        except Exception as e:
            logger.error(f"Main loop error: {e}")
            for _, process in processes:
                if process.is_alive():
                    process.terminate()
                    logger.debug(f"Terminated process: {process.name}")
            if len(markets_to_process) >= 5:
                logger.debug(f"Main loop error with {len(markets_to_process)} markets, calling handle_network_issue")
                handle_network_issue()
            markets_to_process = failed_markets if failed_markets else MARKETS.copy()
            time.sleep(10)
    logger.debug("Main loop completed successfully")
    # Generate market status report in case loop exits unexpectedly
    if not marketsstatus(DESTINATION_PATH, MARKETS, TIMEFRAMES):
        logger.error("Failed to generate market status report")
    return True

if __name__ == "__main__":
    try:
        clear_all_market_files()
        main()
    except Exception as e:
        logger.error(f"Main loop failed: {e}")


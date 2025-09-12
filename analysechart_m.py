import os
import cv2
import numpy as np
import MetaTrader5 as mt5
import time
from datetime import datetime, timedelta
import pytz
import json
import multiprocessing

# Path configuration
BASE_INPUT_FOLDER = r"C:\xampp\htdocs\CIPHER\cipher i\bouncestream\chart\fetched"
BASE_OUTPUT_FOLDER = r"C:\xampp\htdocs\CIPHER\cipher i\bouncestream\chart\processing"
MARKETS_JSON_PATH = r"C:\xampp\htdocs\CIPHER\cipher i\bouncestream\chart\base.json"

# Initialize global credentials as None
LOGIN_ID = None
PASSWORD = None
SERVER = None
TERMINAL_PATH = None
MARKETS = []
TIMEFRAMES = []

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
    print(f"Normalized timeframe '{timeframe}' to '{normalized}'")
    return normalized

# Function to load markets, timeframes, and credentials from JSON
def load_markets_and_timeframes(json_path):
    """Load MARKETS, TIMEFRAMES, and CREDENTIALS from base.json file."""
    global LOGIN_ID, PASSWORD, SERVER, TERMINAL_PATH, MARKETS, TIMEFRAMES
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
                print(f"[Process-{market}] Time left ({time_left:.2f} minutes) is <= {min_time_left} minutes, waiting for next candle")
                time_to_wait = (next_close_time - current_time).total_seconds() + 5  # Wait until next candle starts
                time.sleep(time_to_wait)
                continue
            
    finally:
        mt5.shutdown()

def clear_image_and_json_files():
    """Clear all .png, .jpg, .jpeg, and .json files in all market/timeframe subfolders within BASE_OUTPUT_FOLDER."""
    try:
        if not os.path.exists(BASE_OUTPUT_FOLDER):
            print(f"Output folder does not exist: {BASE_OUTPUT_FOLDER}")
            return

        # Define the file extensions to delete
        file_extensions = ('.png', '.jpg', '.jpeg', '.json')

        # Iterate through each market folder
        for market in os.listdir(BASE_OUTPUT_FOLDER):
            market_path = os.path.join(BASE_OUTPUT_FOLDER, market)
            if os.path.isdir(market_path):
                # Iterate through each timeframe folder within the market folder
                for timeframe in os.listdir(market_path):
                    timeframe_path = os.path.join(market_path, timeframe)
                    if os.path.isdir(timeframe_path):
                        # Iterate through files in the timeframe folder
                        for item in os.listdir(timeframe_path):
                            item_path = os.path.join(timeframe_path, item)
                            # Check if the item is a file and has a target extension
                            if os.path.isfile(item_path) and item.lower().endswith(file_extensions):
                                try:
                                    os.remove(item_path)
                                    print(f"Deleted file: {item_path}")
                                except Exception as e:
                                    print(f"Error deleting file {item_path}: {e}")
                            else:
                                print(f"Skipped non-target file or directory: {item_path}")
                    else:
                        print(f"Skipped non-directory: {timeframe_path}")
            else:
                print(f"Skipped non-directory: {market_path}")

    except Exception as e:
        print(f"Error clearing output folder {BASE_OUTPUT_FOLDER}: {e}")

def check_market_verification(market):
    """Check if all timeframes for a market are verified in the market's verification.json file."""
    try:
        market_folder_name = market.replace(" ", "_")
        verification_file = os.path.join(BASE_INPUT_FOLDER, market_folder_name, "verification.json")
        if not os.path.exists(verification_file):
            print(f"Verification file not found for {market}: {verification_file}")
            return False
        try:
            with open(verification_file, 'r') as f:
                verification_data = json.load(f)
            # Access the "verification" key (Note: Adjusting for actual structure in verification.json)
            for timeframe in TIMEFRAMES:
                normalized_tf = normalize_timeframe(timeframe).lower()  # Normalize timeframe for key
                timeframe_key = timeframe.lower()  # Use original timeframe for verification data key
                if timeframe_key not in verification_data:
                    print(f"Timeframe {timeframe} not found in verification data for {market}")
                    return False
                if verification_data[timeframe_key] != "chart_identified":
                    print(f"Verification failed for {market} timeframe {timeframe}: value is '{verification_data[timeframe_key]}', expected 'chart_identified'")
                    return False
            print(f"All timeframes verified for market: {market}")
            return True
        except Exception as e:
            print(f"Error reading verification file for {market}: {e}")
            return False
    except Exception as e:
        print(f"Error checking verification for market {market}: {e}")
        return False

def load_latest_chart(input_folder, market_name, timeframe):
    """Find and load the latest chart image with filename format market_timeframe.png."""
    if not os.path.exists(input_folder):
        print(f"Input folder does not exist: {input_folder}")
        return None, None
    
    # Normalize timeframe for folder and filename
    normalized_tf = normalize_timeframe(timeframe)
    
    # Construct the exact filename: market_timeframe.png
    expected_filename = f"{market_name.replace(' ', '_')}_{normalized_tf}.png"
    chart_path = os.path.join(input_folder, expected_filename)
    
    if os.path.isfile(chart_path):
        print(f"Chart file found: {chart_path}")
        img = cv2.imread(chart_path)
        if img is None:
            raise ValueError(f"Failed to load image: {chart_path}")
        base_name = os.path.splitext(os.path.basename(chart_path))[0]
        return img, base_name
    
    # Fallback: Search for files containing market_timeframe
    search_pattern = f"{market_name.replace(' ', '_')}_{normalized_tf}"
    files = [
        os.path.join(input_folder, f) for f in os.listdir(input_folder)
        if os.path.isfile(os.path.join(input_folder, f)) and 
           search_pattern in f.replace(' ', '_') and
           f.lower().endswith('.png')
    ]
    
    if not files:
        print(f"No files found containing '{search_pattern}' in {input_folder}")
        return None, None
    
    chart_path = max(files, key=os.path.getmtime)
    print(f"Latest chart file found: {chart_path}")
    
    img = cv2.imread(chart_path)
    if img is None:
        raise ValueError(f"Failed to load image: {chart_path}")
    
    base_name = os.path.splitext(os.path.basename(chart_path))[0]
    return img, base_name

def crop_image(img, height, width):
    """Crop the image: 200px from left, 30px from bottom, 150px from right."""
    if height < 20 or width < 350:
        raise ValueError(f"Image too small to crop (height: {height}, width: {width})")
    return img[0:height-20, 0:width-100]

def enhance_colors(img):
    """Convert to HSV and enhance saturation and brightness for red and green pixels."""
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
    red_lower1 = np.array([0, 20, 20])
    red_upper1 = np.array([15, 255, 255])
    red_lower2 = np.array([165, 20, 20])
    red_upper2 = np.array([180, 255, 255])
    green_lower = np.array([30, 20, 20])
    green_upper = np.array([100, 255, 255])
    
    mask_red1 = cv2.inRange(hsv, red_lower1, red_upper1)
    mask_red2 = cv2.inRange(hsv, red_lower2, red_upper2)
    mask_red = cv2.bitwise_or(mask_red1, mask_red2)
    mask_green = cv2.inRange(hsv, green_lower, green_upper)
    mask = cv2.bitwise_or(mask_red, mask_green)
    
    hsv = hsv.astype(np.float32)
    h, s, v = cv2.split(hsv)
    s[mask > 0] = np.clip(s[mask > 0] * 2.0, 0, 255)
    v[mask > 0] = np.clip(v[mask > 0] * 1.5, 0, 255)
    hsv_enhanced = cv2.merge([h, s, v]).astype(np.uint8)
    
    img_enhanced = cv2.cvtColor(hsv_enhanced, cv2.COLOR_HSV2BGR)
    return img_enhanced, mask_red, mask_green, mask

def replace_near_black_wicks(img_enhanced, mask_red, mask_green):
    """Replace near-black wick pixels with bold red/green."""
    near_black_mask = cv2.inRange(img_enhanced, (0, 0, 0), (50, 50, 50))
    red_proximity = cv2.dilate(mask_red, np.ones((3, 3), np.uint8), iterations=1)
    img_enhanced[np.logical_and(near_black_mask > 0, red_proximity > 0)] = [0, 0, 255]
    green_proximity = cv2.dilate(mask_green, np.ones((3, 3), np.uint8), iterations=1)
    img_enhanced[np.logical_and(near_black_mask > 0, green_proximity > 0)] = [0, 255, 0]
    return img_enhanced

def sharpen_image(img_enhanced):
    """Apply sharpening to make candlesticks and wicks bold."""
    kernel = np.array([[-1, -1, -1],
                       [-1, 10, -1],
                       [-1, -1, -1]])
    return cv2.filter2D(img_enhanced, -1, kernel)

def set_background_black(img_enhanced, mask):
    """Set the background to pure black."""
    background_mask = cv2.bitwise_not(mask)
    img_enhanced[background_mask > 0] = [0, 0, 0]
    return img_enhanced

def save_enhanced_image(img_enhanced, base_name, output_folder):
    """Save the enhanced image."""
    normalized_tf = normalize_timeframe(base_name.split('_')[-1])  # Extract timeframe from base_name
    market_name = '_'.join(base_name.split('_')[:-1])  # Extract market name
    output_folder = os.path.join(BASE_OUTPUT_FOLDER, market_name, normalized_tf)
    os.makedirs(output_folder, exist_ok=True)
    debug_image_path = os.path.join(output_folder, f"{base_name}_enhanced.png")
    cv2.imwrite(debug_image_path, img_enhanced)
    print(f"Debug enhanced image saved to: {debug_image_path}")
    return debug_image_path

def remove_horizontal_lines(img_enhanced, mask_red, mask_green, width):
    """Remove horizontal lines from the image."""
    gray = cv2.cvtColor(img_enhanced, cv2.COLOR_BGR2GRAY)
    binary = cv2.threshold(gray, 1, 255, cv2.THRESH_BINARY)[1]
    horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (int(width * 0.2), 1))
    horizontal_lines = cv2.morphologyEx(binary, cv2.MORPH_OPEN, horizontal_kernel, iterations=2)
    
    contours, _ = cv2.findContours(horizontal_lines, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    for contour in contours:
        x, y, w, h = cv2.boundingRect(contour)
        aspect_ratio = w / float(h)
        if w > width * 0.1 and aspect_ratio > 10 and h < 5:
            cv2.drawContours(img_enhanced, [contour], -1, (0, 0, 0), -1)
            cv2.drawContours(mask_red, [contour], -1, 0, -1)
            cv2.drawContours(mask_green, [contour], -1, 0, -1)
    
    mask = cv2.bitwise_or(mask_red, mask_green)
    return img_enhanced, mask_red, mask_green, mask

def load_candlesamountinbetween(market, timeframe):
    """Load the 'new number position for matched candle data' value from candlesamountinbetween.json and save it to loadednumber.json with source (including raw value) and any errors."""
    errors = []  # List to store any errors or issues
    start_number = 1  # Default value if file is missing or invalid
    raw_value = "not found"  # Default raw value if file or field is missing
    normalized_tf = normalize_timeframe(timeframe)  # Normalize timeframe for folder path
    source = f"{market.replace(' ', '_')}_{normalized_tf} candlesamountinbetween (new number position for matched candle data: {raw_value})"
    
    try:
        market_folder_name = market.replace(" ", "_")
        json_path = os.path.join(
            r"C:\xampp\htdocs\CIPHER\cipher i\bouncestream\chart\orders",
            market_folder_name,
            normalized_tf,
            "candlesamountinbetween.json"
        )
        output_folder = os.path.join(BASE_OUTPUT_FOLDER, market_folder_name, normalized_tf)
        os.makedirs(output_folder, exist_ok=True)  # Ensure output folder exists
        
        if not os.path.exists(json_path):
            errors.append(f"candlesamountinbetween.json not found for {market} timeframe {timeframe}: {json_path}")
            print(f"candlesamountinbetween.json not found for {market} timeframe {timeframe}: {json_path}")
        else:
            try:
                with open(json_path, 'r') as f:
                    data = json.load(f)
                raw_value = data.get("new number position for matched candle data", "not found")
                source = f"{market.replace(' ', '_')}_{normalized_tf} candlesamountinbetween (new number position for matched candle data: {raw_value})"
                try:
                    start_number = int(raw_value)
                    if start_number < 1:
                        errors.append(f"Invalid 'new number position for matched candle data' value {raw_value} for {market} timeframe {timeframe}, defaulting to 1")
                        print(f"Invalid 'new number position for matched candle data' value {raw_value} for {market} timeframe {timeframe}, defaulting to 1")
                        start_number = 1
                    else:
                        print(f"Starting number for {market} timeframe {timeframe}: {start_number}")
                except ValueError:
                    errors.append(f"Invalid 'new number position for matched candle data' value {raw_value} for {market} timeframe {timeframe}, defaulting to 1")
                    print(f"Invalid 'new number position for matched candle data' value {raw_value} for {market} timeframe {timeframe}, defaulting to 1")
                    start_number = 1
            except Exception as e:
                errors.append(f"Error reading candlesamountinbetween.json for {market} timeframe {timeframe}: {str(e)}")
                print(f"Error reading candlesamountinbetween.json for {market} timeframe {timeframe}: {str(e)}")
                start_number = 1

        # Save loadednumber.json with start number, source, and errors
        loadednumber_data = {
            "start_number": start_number,
            "from": source,
            "errors": errors if errors else ["No errors encountered"]
        }
        loadednumber_json_path = os.path.join(output_folder, "loadednumber.json")
        try:
            with open(loadednumber_json_path, 'w') as f:
                json.dump(loadednumber_data, f, indent=4)
            print(f"Loaded number data saved to: {loadednumber_json_path}")
        except Exception as e:
            errors.append(f"Error saving loadednumber.json: {str(e)}")
            print(f"Error saving loadednumber.json: {str(e)}")

        return start_number

    except Exception as e:
        errors.append(f"Critical error in load_candlesamountinbetween: {str(e)}")
        print(f"Critical error in load_candlesamountinbetween: {str(e)}")
        # Save loadednumber.json even if a critical error occurs
        loadednumber_data = {
            "start_number": start_number,
            "from": source,
            "errors": errors if errors else ["Critical error occurred"]
        }
        loadednumber_json_path = os.path.join(output_folder, "loadednumber.json")
        try:
            with open(loadednumber_json_path, 'w') as f:
                json.dump(loadednumber_data, f, indent=4)
            print(f"Loaded number data saved to: {loadednumber_json_path}")
        except Exception as save_e:
            print(f"Error saving loadednumber.json after critical error: {str(save_e)}")
        return start_number

def detect_candlestick_contours(img_enhanced, mask_red, mask_green, start_number):
    """Detect and draw contours for red and green candlesticks, draw one white arrow per unique candlestick position pointing downward to the top with a vertical line to the image top, and collect arrow data for JSON output. Save labelstart.json with start number and any errors."""
    errors = []  # List to store any errors or issues
    try:
        img_contours = img_enhanced.copy()
        height, width = img_contours.shape[:2]
        contours_red, _ = cv2.findContours(mask_red, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        red_count = 0
        red_positions = []
        all_candlestick_positions = []

        for contour in contours_red:
            area = cv2.contourArea(contour)
            if area < 0.01:
                errors.append(f"Skipped red contour with area {area} (below threshold 0.01)")
                continue
            red_count += 1
            x, y, w, h = cv2.boundingRect(contour)
            center_x = x + w // 2
            top_y = y
            bottom_y = y + h
            red_positions.append((center_x, top_y, bottom_y))
            all_candlestick_positions.append((center_x, top_y, bottom_y, 'red', contour))

        contours_green, _ = cv2.findContours(mask_green, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        green_count = 0
        green_positions = []

        for contour in contours_green:
            area = cv2.contourArea(contour)
            if area < 0.01:
                errors.append(f"Skipped green contour with area {area} (below threshold 0.01)")
                continue
            green_count += 1
            x, y, w, h = cv2.boundingRect(contour)
            center_x = x + w // 2
            top_y = y
            bottom_y = y + h
            green_positions.append((center_x, top_y, bottom_y))
            all_candlestick_positions.append((center_x, top_y, bottom_y, 'green', contour))

        # Check if no contours were detected
        if red_count == 0 and green_count == 0:
            errors.append("No valid red or green candlestick contours detected")

        unique_positions = []
        seen_x = {}
        for pos in sorted(all_candlestick_positions, key=lambda x: x[0]):
            center_x, top_y, bottom_y, color, contour = pos
            if center_x not in seen_x:
                seen_x[center_x] = pos
                unique_positions.append(pos)
            else:
                existing_pos = seen_x[center_x]
                existing_contour = existing_pos[4]
                if cv2.contourArea(contour) > cv2.contourArea(existing_contour):
                    unique_positions[unique_positions.index(existing_pos)] = pos
                    seen_x[center_x] = pos

        for center_x, top_y, bottom_y, color, contour in unique_positions:
            contour_color = (255, 0, 0) if color == 'red' else (255, 255, 255)
            try:
                cv2.drawContours(img_contours, [contour], -1, contour_color, 1)
                arrow_start = (center_x, max(0, top_y - 30))
                arrow_end = (center_x, top_y)
                cv2.arrowedLine(img_contours, arrow_start, arrow_end, (255, 255, 255), 1, tipLength=0.3)
                cv2.line(img_contours, arrow_end, (center_x, 0), (255, 255, 255), 1)
            except Exception as e:
                errors.append(f"Error drawing contour or arrow at x={center_x}, y={top_y}: {str(e)}")

        arrow_data = []
        arrow_count = 0
        for i, (center_x, top_y, bottom_y, color, _) in enumerate(reversed(unique_positions[:-1]), start=start_number):
            arrow_count += 1
            arrow_data.append({
                "arrow_number": i,
                "pointing_on_candle_color": color,
                "x": center_x
            })

        print(f"Red candlesticks detected: {red_count}")
        print(f"Green candlesticks detected: {green_count}")
        print(f"Total candlesticks detected: {red_count + green_count}")
        print(f"Unique candlestick positions: {len(unique_positions)}")
        print(f"Total arrows: {arrow_count}, starting from number {start_number}")

        # Save labelstart.json with start number and errors
        labelstart_data = {
            "start_number": start_number,
            "errors": errors if errors else ["No errors encountered"]
        }
        labelstart_json_path = os.path.join(OUTPUT_FOLDER, "labelstart.json")
        try:
            with open(labelstart_json_path, 'w') as f:
                json.dump(labelstart_data, f, indent=4)
            print(f"Label start data saved to: {labelstart_json_path}")
        except Exception as e:
            errors.append(f"Error saving labelstart.json: {str(e)}")
            print(f"Error saving labelstart.json: {str(e)}")

        return img_contours, red_positions, green_positions, arrow_data

    except Exception as e:
        errors.append(f"Critical error in detect_candlestick_contours: {str(e)}")
        print(f"Critical error in detect_candlestick_contours: {str(e)}")
        # Save labelstart.json even if a critical error occurs
        labelstart_data = {
            "start_number": start_number,
            "errors": errors if errors else ["Critical error occurred"]
        }
        labelstart_json_path = os.path.join(OUTPUT_FOLDER, "labelstart.json")
        try:
            with open(labelstart_json_path, 'w') as f:
                json.dump(labelstart_data, f, indent=4)
            print(f"Label start data saved to: {labelstart_json_path}")
        except Exception as save_e:
            print(f"Error saving labelstart.json after critical error: {str(save_e)}")
        return img_contours, [], [], []  # Return empty data to allow processing to continue

def save_arrow_data_to_json(arrow_data, output_folder):
    """Save the arrow data to a JSON file named after the market in the OUTPUT_FOLDER."""
    normalized_tf = normalize_timeframe(output_folder.split(os.sep)[-1])  # Extract timeframe from output_folder
    market_name = output_folder.split(os.sep)[-2]  # Extract market name
    output_folder = os.path.join(BASE_OUTPUT_FOLDER, market_name, normalized_tf)
    os.makedirs(output_folder, exist_ok=True)
    json_path = os.path.join(output_folder, "arrows.json")
    try:
        with open(json_path, 'w') as f:
            json.dump(arrow_data, f, indent=4)
        print(f"Arrow data saved to: {json_path}")
    except Exception as e:
        print(f"Error saving arrow data to JSON: {e}")
    return json_path

def save_contour_image(img_contours, base_name, output_folder):
    """Save the contour image."""
    normalized_tf = normalize_timeframe(base_name.split('_')[-1])  # Extract timeframe from base_name
    market_name = '_'.join(base_name.split('_')[:-1])  # Extract market name
    output_folder = os.path.join(BASE_OUTPUT_FOLDER, market_name, normalized_tf)
    os.makedirs(output_folder, exist_ok=True)
    contour_image_path = os.path.join(output_folder, f"{base_name}_contours.png")
    cv2.imwrite(contour_image_path, img_contours)
    print(f"Original contour image saved to: {contour_image_path}")
    return contour_image_path

def connect_contours(img_contours, red_positions, green_positions):
    """Connect candlestick contours with lines based on high/low points."""
    img_connected_contours = img_contours.copy()
    all_positions = [(pos, 'red') for pos in red_positions] + [(pos, 'green') for pos in green_positions]
    all_positions.sort(key=lambda x: x[0][0])
    
    connection_points = []
    for i, (pos, color) in enumerate(all_positions):
        x, top_y, bottom_y = pos
        is_low = False
        is_high = False
        
        if i > 0 and i < len(all_positions) - 1:
            prev_bottom_y = all_positions[i-1][0][2]
            next_bottom_y = all_positions[i+1][0][2]
            if bottom_y > prev_bottom_y and bottom_y > next_bottom_y:
                is_low = True
            prev_top_y = all_positions[i-1][0][1]
            next_top_y = all_positions[i+1][0][1]
            if top_y < prev_top_y and top_y < next_top_y:
                is_high = True
        elif i == 0 and len(all_positions) > 1:
            next_bottom_y = all_positions[i+1][0][2]
            next_top_y = all_positions[i+1][0][1]
            if bottom_y > next_bottom_y:
                is_low = True
            if top_y < next_top_y:
                is_high = True
        elif i == len(all_positions) - 1 and len(all_positions) > 1:
            prev_bottom_y = all_positions[i-1][0][2]
            prev_top_y = all_positions[i-1][0][1]
            if bottom_y > prev_bottom_y:
                is_low = True
            if top_y < prev_top_y:
                is_high = True
        
        if is_low or is_high:
            connection_points.append((x, top_y, bottom_y, 'low' if is_low else 'high', color))
    
    connection_points.sort(key=lambda x: x[0])
    
    for i in range(len(connection_points) - 1):
        x1, top_y1, bottom_y1, type1, color1 = connection_points[i]
        x2, top_y2, bottom_y2, type2, color2 = connection_points[i + 1]
        
        y1 = top_y1 if type1 == 'high' else bottom_y1
        y2 = top_y2 if type2 == 'high' else bottom_y2
        cv2.line(img_connected_contours, (x1, y1), (x2, y2), (255, 255, 255), 1)
    
    return img_connected_contours, all_positions

def save_connected_contour_image(img_connected_contours, base_name, output_folder):
    """Save the connected contour image."""
    normalized_tf = normalize_timeframe(base_name.split('_')[-1])  # Extract timeframe from base_name
    market_name = '_'.join(base_name.split('_')[:-1])  # Extract market name
    output_folder = os.path.join(BASE_OUTPUT_FOLDER, market_name, normalized_tf)
    os.makedirs(output_folder, exist_ok=True)
    connected_contour_image_path = os.path.join(output_folder, f"{base_name}_connected_contours.png")
    cv2.imwrite(connected_contour_image_path, img_connected_contours)
    print(f"Connected contour image saved to: {connected_contour_image_path}")
    return connected_contour_image_path

def identify_parent_highs_and_lows(img_enhanced, all_positions, base_name, left_required, right_required, arrow_data, output_folder):
    """Identify and label Parent Highs (PH) and Parent Lows (PL) on the enhanced image using arrow numbers."""
    normalized_tf = normalize_timeframe(base_name.split('_')[-1])  # Extract timeframe from base_name
    market_name = '_'.join(base_name.split('_')[:-1])  # Extract market name
    output_folder = os.path.join(BASE_OUTPUT_FOLDER, market_name, normalized_tf)
    os.makedirs(output_folder, exist_ok=True)
    
    img_parent_labeled = img_enhanced.copy()
    low_points = []
    high_points = []
    total_candles = len(all_positions)
    arrow_map = {item['x']: item['arrow_number'] for item in arrow_data}
    
    for i, (pos, color) in enumerate(reversed(all_positions[:-1]), 1):
        x, top_y, bottom_y = pos
        orig_index = total_candles - 1 - i
        
        is_low = False
        is_high = False
        
        # Initialize neighbor comparison flags
        has_lower_bottom = False
        has_higher_top = False
        
        # Check immediate neighbors for PL and PH conditions
        if orig_index > 0 and orig_index < total_candles - 1:
            prev_pos, _ = all_positions[orig_index + 1]  # Left neighbor
            next_pos, _ = all_positions[orig_index - 1]  # Right neighbor
            prev_top_y, prev_bottom_y = prev_pos[1], prev_pos[2]
            next_top_y, next_bottom_y = next_pos[1], next_pos[2]
            
            # For PL: bottom_y must be strictly lower (larger y) than both neighbors' bottoms
            if bottom_y > prev_bottom_y and bottom_y > next_bottom_y:
                has_lower_bottom = True
                
            # For PH: top_y must be strictly higher (smaller y) than both neighbors' tops
            if top_y < prev_top_y and top_y < next_top_y:
                has_higher_top = True
                
        elif orig_index == 0 and total_candles > 2:
            next_pos, _ = all_positions[orig_index + 1]  # Right neighbor
            next_top_y, next_bottom_y = next_pos[1], next_pos[2]
            
            # For PL: bottom_y must be lower than the right neighbor's bottom
            if bottom_y > next_bottom_y:
                has_lower_bottom = True
                
            # For PH: top_y must be higher than the right neighbor's top
            if top_y < next_top_y:
                has_higher_top = True
                
        elif orig_index == total_candles - 2 and total_candles > 2:
            prev_pos, _ = all_positions[orig_index - 1]  # Left neighbor
            prev_top_y, prev_bottom_y = prev_pos[1], prev_pos[2]
            
            # For PL: bottom_y must be lower than the left neighbor's bottom
            if bottom_y > prev_bottom_y:
                has_lower_bottom = True
                
            # For PH: top_y must be higher than the left neighbor's top
            if top_y < prev_top_y:
                has_higher_top = True
        
        # Only proceed with PL/PH checks if neighbor conditions are met
        if has_lower_bottom:
            left_count = 0
            right_count = 0
            for j in range(orig_index + 1, min(orig_index + left_required + 1, total_candles)):
                if all_positions[j][0][2] < bottom_y:
                    left_count += 1
            for j in range(max(orig_index - right_required, -1), orig_index):
                if all_positions[j][0][2] < bottom_y:
                    right_count += 1
            if left_count >= left_required and right_count >= right_required:
                is_low = True
                
        if has_higher_top:
            left_count = 0
            right_count = 0
            for j in range(orig_index + 1, min(orig_index + left_required + 1, total_candles)):
                if all_positions[j][0][1] > top_y:
                    left_count += 1
            for j in range(max(orig_index - right_required, -1), orig_index):
                if all_positions[j][0][1] > top_y:
                    right_count += 1
            if left_count >= left_required and right_count >= right_required:
                is_high = True
        
        if is_low:
            low_points.append((orig_index, x, bottom_y, i))
        if is_high:
            high_points.append((orig_index, x, top_y, i))
    
    low_points.sort(key=lambda x: x[1])
    high_points.sort(key=lambda x: x[1])
    
    pl_count = 0
    pl_labels = []
    for i, (orig_index, x, bottom_y, number) in enumerate(low_points):
        left_count = 0
        right_count = 0
        is_lowest = True
        
        for j in range(i - 1, -1, -1):
            if low_points[j][2] < bottom_y:
                left_count += 1
                if left_count >= left_required:
                    break
            else:
                is_lowest = False
        
        for j in range(i + 1, len(low_points)):
            if low_points[j][2] < bottom_y:
                right_count += 1
                if right_count >= right_required:
                    break
            else:
                is_lowest = False
        
        if left_count >= left_required and right_count >= right_required and is_lowest:
            pl_count += 1
            arrow_number = arrow_map.get(x, None)
            if arrow_number is not None:
                label = f"PL{arrow_number}"
                text_position = (x - 20, bottom_y + 20)
                cv2.putText(img_parent_labeled, label, text_position, cv2.FONT_HERSHEY_SIMPLEX,
                            0.5, (255, 255, 255), 1, cv2.LINE_AA)
                pl_labels.append((x, bottom_y, label, arrow_number))
    
    ph_count = 0
    ph_labels = []
    for i, (orig_index, x, top_y, number) in enumerate(high_points):
        left_count = 0
        right_count = 0
        is_highest = True
        
        for j in range(i - 1, -1, -1):
            if high_points[j][2] > top_y:
                left_count += 1
                if left_count >= left_required:
                    break
            else:
                is_highest = False
        
        for j in range(i + 1, len(high_points)):
            if high_points[j][2] > top_y:
                right_count += 1
                if right_count >= right_required:
                    break
            else:
                is_highest = False
        
        if left_count >= left_required and right_count >= right_required and is_highest:
            ph_count += 1
            arrow_number = arrow_map.get(x, None)
            if arrow_number is not None:
                label = f"PH{arrow_number}"
                text_position = (x - 20, top_y - 10)
                cv2.putText(img_parent_labeled, label, text_position, cv2.FONT_HERSHEY_SIMPLEX,
                            0.5, (255, 255, 255), 1, cv2.LINE_AA)
                ph_labels.append((x, top_y, label, arrow_number))
    
    print(f"Identified {pl_count} Parent Lows (PL) with {left_required} left and {right_required} right lows required")
    print(f"Identified {ph_count} Parent Highs (PH) with {left_required} left and {right_required} right highs required")
    
    parent_labeled_image_path = os.path.join(output_folder, f"{base_name}_parent_highs_lows.png")
    cv2.imwrite(parent_labeled_image_path, img_parent_labeled)
    print(f"Parent highs and lows labeled image saved to: {parent_labeled_image_path}")
    
    return parent_labeled_image_path, pl_labels, ph_labels

def controlleftandrighthighsandlows(left, right):
    """Control the number of highs/lows required to the left and right for PH/PL identification."""
    try:
        left_count_required = int(left)
        right_count_required = int(right)
        if left_count_required < 0 or right_count_required < 0:
            raise ValueError("Left and right counts must be non-negative.")
        return left_count_required, right_count_required
    except ValueError as e:
        raise ValueError(f"Invalid input for left or right: {e}")
    
#TRENDLINE AND DRAWING
def draw_parent_main_trendlines(img_parent_labeled, all_positions, base_name, left_required, right_required, 
                                main_trendline_position, distance_threshold, num_contracts, allow_latest_main_trendline,
                                pl_labels, ph_labels):
    img_main_trendlines = img_parent_labeled.copy()
    total_candles = len(all_positions)
    
    # Get image width for extending trendlines and boxes to the right edge
    img_width = img_main_trendlines.shape[1]
    
    # Font settings for labels and position numbers
    font = cv2.FONT_HERSHEY_SIMPLEX
    font_scale = 0.5
    text_color = (255, 255, 255)  # White text
    thickness = 1
    line_type = cv2.LINE_AA
    
    # Draw PH and PL labels and position numbers
    for x, bottom_y, label, arrow_number in pl_labels:
        text_position_pl = (x - 20, bottom_y + 20)
        cv2.putText(img_main_trendlines, label, text_position_pl, font,
                    font_scale, text_color, thickness, line_type)
        text_position_num = (x - 20, bottom_y + 35)
        cv2.putText(img_main_trendlines, str(arrow_number), text_position_num, font,
                    font_scale, text_color, thickness, line_type)
    
    for x, top_y, label, arrow_number in ph_labels:
        text_position_ph = (x - 20, top_y - 10)
        cv2.putText(img_main_trendlines, label, text_position_ph, font,
                    font_scale, text_color, thickness, line_type)
        text_position_num = (x - 20, top_y - 25)
        cv2.putText(img_main_trendlines, str(arrow_number), text_position_num, font,
                    font_scale, text_color, thickness, line_type)
    
    # Sort all_positions by x-coordinate
    sorted_positions = sorted(all_positions, key=lambda x: x[0][0])
    
    # Initialize lists to store trendline and contracts data
    main_trendline_data = []
    contracts_data = []
    
    # Set to track unique trendlines
    unique_trendlines = set()
    
    # Initialize list to store parent distances for JSON
    parent_distances = []
    
    # Calculate PH-to-PH vertical distances
    ph_labels_sorted = sorted(ph_labels, key=lambda x: x[0])
    for i in range(len(ph_labels_sorted) - 1):
        x1, top_y1, label1, arrow_number1 = ph_labels_sorted[i]
        x2, top_y2, label2, arrow_number2 = ph_labels_sorted[i + 1]
        distance = abs(top_y1 - top_y2)
        parent_distances.append({
            "type": "PH-to-PH",
            "from_label": label1,
            "to_label": label2,
            "vertical_distance_px": distance
        })
        print(f"PH-to-PH distance from {label1} (x={x1}, y={top_y1}) to {label2} (x={x2}, y={top_y2}): {distance}px")
    
    # Calculate PL-to-PL vertical distances
    pl_labels_sorted = sorted(pl_labels, key=lambda x: x[0])
    for i in range(len(pl_labels_sorted) - 1):
        x1, bottom_y1, label1, arrow_number1 = pl_labels_sorted[i]
        x2, bottom_y2, label2, arrow_number2 = pl_labels_sorted[i + 1]
        distance = abs(bottom_y1 - bottom_y2)
        parent_distances.append({
            "type": "PL-to-PL",
            "from_label": label1,
            "to_label": label2,
            "vertical_distance_px": distance
        })
        print(f"PL-to-PL distance from {label1} (x={x1}, y={bottom_y1}) to {label2} (x={x2}, y={bottom_y2}): {distance}px")
    
    # Save parent distances to JSON
    parent_distances_json_path = os.path.join(OUTPUT_FOLDER, f"{base_name}_parent_distances.json")
    try:
        with open(parent_distances_json_path, 'w') as f:
            json.dump(parent_distances, f, indent=4)
        print(f"Parent distances saved to: {parent_distances_json_path}")
    except Exception as e:
        print(f"Error saving parent distances to JSON: {e}")
    
    # Helper function to find candlestick at specified position
    def get_candle_at_position(receiver_x, position):
        try:
            pos = int(position)
        except ValueError:
            pos = 1
        count = 0
        for pos_data, color in sorted_positions:
            if pos_data[0] > receiver_x:
                count += 1
                if count == pos:
                    return pos_data, color
        print(f"No candlestick found at position {position} to the right of x={receiver_x}")
        return None, None
    
    # Helper function to check if a trendline crosses another PH or PL
    def crosses_other_parent(start_x, end_x, start_y, end_y, points_to_check, exclude_labels=None):
        if exclude_labels is None:
            exclude_labels = set()
        for x, y, label, _ in points_to_check:
            if label in exclude_labels:
                continue
            if start_x < x < end_x:
                if start_x != end_x:
                    t = (x - start_x) / (end_x - start_x)
                    line_y = start_y + t * (end_y - start_y)
                    if abs(line_y - y) < 20:
                        return True, label
        return False, None
    
    # Helper function to extend line to the right edge of the image
    def extend_line_to_right_edge(start_x, start_y, target_x, target_y, img_width):
        if target_x == start_x:
            return img_width, start_y
        m = (target_y - start_y) / (target_x - start_x)
        c = start_y - m * start_x
        end_y = m * img_width + c
        return img_width, int(end_y)
    
    # Helper function to count parents ahead
    def count_parents_ahead(x, parent_points):
        return sum(1 for p in parent_points if p[0] > x)
    
    # Helper function to extract position number from label
    def get_position_number_from_label(label):
        try:
            return int(label[2:])
        except ValueError:
            return None
    
    # Helper function to get position number for a candlestick
    def get_position_number(x, labels, all_positions):
        for px, _, label, _ in labels:
            if px == x:
                pos_number = get_position_number_from_label(label)
                if pos_number is not None:
                    return pos_number
        for i, (pos, _) in enumerate(reversed(all_positions[:-1]), 1):
            if pos[0] == x:
                return i
        return None
    
    # Helper function to find the candlestick for a parent
    def get_candle_for_parent(parent_x, all_positions):
        for pos, color in all_positions:
            if pos[0] == parent_x:
                return pos, color
        return None, None
    
    # Helper function to get y-coordinates for PLOP/PHOP
    def get_parent_y_coordinates(parent_label, parent_type, all_positions, pl_labels, ph_labels):
        parent_candle, _ = get_candle_for_parent(
            next((x for x, _, label, _ in (pl_labels if parent_type == 'PL' else ph_labels) if label == parent_label), None),
            all_positions
        )
        if parent_candle is None:
            return None, None
        if parent_type == 'PL':
            return parent_candle[1], parent_candle[2]  # Top and bottom for PL
        else:
            return parent_candle[2], parent_candle[1]  # Bottom and top for PH
    
    # Helper function to find the first candlestick after BO that crosses the Order Parent level
    def find_crossing_candle(breakout_x, order_parent_label, trendline_type, all_positions, pl_labels, ph_labels):
        if order_parent_label == "invalid":
            return None
        parent_type = 'PL' if trendline_type == "PH-to-PH" else 'PH'
        top_y, bottom_y = get_parent_y_coordinates(order_parent_label, parent_type, all_positions, pl_labels, ph_labels)
        if top_y is None or bottom_y is None:
            return None
        order_y = top_y if trendline_type == "PH-to-PH" else bottom_y  # Top for PLOP, bottom for PHOP
        for pos, _ in sorted(all_positions, key=lambda x: x[0][0]):
            x, top_y, bottom_y = pos
            if x > breakout_x:
                # Check if the candlestick body crosses the order_y level
                if top_y <= order_y <= bottom_y:
                    return x
        return None
    
    # Modified helper function to find Breakout Parent and Order Parent with new reassignment logic
    def find_breakout_and_order_parent(receiver_x, receiver_y, trendline_type, all_parents, pl_labels, ph_labels, all_positions):
        breakout_label = "invalid"
        order_parent_label = "invalid"
        actual_order_parent_label = "invalid"
        order_parent_y = None
        order_parent_x = None
        actual_order_parent_y = None
        actual_order_parent_x = None
        breakout_x = None
        breakout_y = None
        reassigned_op = False
        reassigned_op_label = "none"
        reassigned_op_x = None
        reassigned_op_top_y = None
        reassigned_op_bottom_y = None
        
        if trendline_type == "PH-to-PH":
            # Find PHBO: First PH to the right with top_y < receiver_y (higher on image)
            for x, parent_type, y, label, _ in sorted(all_parents, key=lambda p: p[0]):
                if x > receiver_x and parent_type == "PH" and y < receiver_y:
                    breakout_label = label
                    breakout_x = x
                    breakout_y = y
                    break
            if breakout_label != "invalid":
                # Find initial PLOP: Parent immediately before PHBO must be PL
                sorted_parents = sorted(all_parents, key=lambda p: p[0])
                for i, (x, parent_type, y, label, _) in enumerate(sorted_parents):
                    if label == breakout_label and i > 0:
                        prev_x, prev_type, prev_y, prev_label, _ = sorted_parents[i - 1]
                        if prev_type == "PL" and receiver_x < prev_x < breakout_x:
                            order_parent_label = prev_label
                            actual_order_parent_label = prev_label
                            order_parent_y = prev_y
                            order_parent_x = prev_x
                            actual_order_parent_y = prev_y
                            actual_order_parent_x = prev_x
                        break
                # Check the parent immediately after the actual PLOP
                if actual_order_parent_label != "invalid":
                    actual_pl_candle = get_candle_for_parent(actual_order_parent_x, all_positions)[0]
                    if actual_pl_candle:
                        actual_bottom_y = actual_pl_candle[2]
                        # Find the next parent after actual PLOP
                        for i, (x, parent_type, y, label, _) in enumerate(sorted_parents):
                            if x == actual_order_parent_x and i + 1 < len(sorted_parents):
                                next_x, next_type, next_y, next_label, _ = sorted_parents[i + 1]
                                if next_type == "PL" and next_x < breakout_x:
                                    next_pl_candle = get_candle_for_parent(next_x, all_positions)[0]
                                    if next_pl_candle and next_pl_candle[2] > actual_bottom_y and receiver_x < next_x < breakout_x:
                                        print(f"Reassigning PLOP from {order_parent_label} to {next_label} at x={next_x} "
                                            f"because next PL bottom (y={next_pl_candle[2]}) > actual PLOP bottom (y={actual_bottom_y})")
                                        order_parent_label = next_label
                                        order_parent_x = next_x
                                        order_parent_y = next_y
                                        reassigned_op = True
                                        reassigned_op_label = next_label
                                        reassigned_op_x = next_x
                                        reassigned_op_top_y = next_pl_candle[1]
                                        reassigned_op_bottom_y = next_pl_candle[2]
                                        break
                # Find the lowest PL (highest bottom_y) before PHBO
                candidate_pls = [(x, y, label, *get_parent_y_coordinates(label, 'PL', all_positions, pl_labels, ph_labels))
                                for x, y, label, _ in pl_labels if x < breakout_x]
                candidate_pls.sort(key=lambda x: x[4], reverse=True)  # Sort by bottom_y descending
                for pl_x, pl_y, pl_label, pl_top_y, pl_bottom_y in candidate_pls:
                    if pl_top_y is None or pl_bottom_y is None:
                        continue
                    # Only reassign if this PL is lower than the current OP (if valid) and within bounds
                    if order_parent_label != "invalid":
                        current_pl_candle = get_candle_for_parent(order_parent_x, all_positions)[0]
                        if current_pl_candle and pl_bottom_y > current_pl_candle[2] and receiver_x < pl_x < breakout_x:
                            print(f"Reassigning PLOP from {order_parent_label} to {pl_label} at x={pl_x} "
                                f"because new PL bottom (y={pl_bottom_y}) > current PLOP bottom (y={current_pl_candle[2]})")
                            order_parent_label = pl_label
                            order_parent_x = pl_x
                            order_parent_y = pl_y
                            reassigned_op = True
                            reassigned_op_label = pl_label
                            reassigned_op_x = pl_x
                            reassigned_op_top_y = pl_top_y
                            reassigned_op_bottom_y = pl_bottom_y
                            break
                    elif receiver_x < pl_x < breakout_x:
                        print(f"Assigning PLOP to {pl_label} at x={pl_x} as initial PLOP is invalid")
                        order_parent_label = pl_label
                        order_parent_x = pl_x
                        order_parent_y = pl_y
                        reassigned_op = True
                        reassigned_op_label = pl_label
                        reassigned_op_x = pl_x
                        reassigned_op_top_y = pl_top_y
                        reassigned_op_bottom_y = pl_bottom_y
                        break
        else:  # PL-to-PL
            # Find PLBO: First PL to the right with bottom_y > receiver_y (lower on image)
            for x, parent_type, y, label, _ in sorted(all_parents, key=lambda p: p[0]):
                if x > receiver_x and parent_type == "PL" and y > receiver_y:
                    breakout_label = label
                    breakout_x = x
                    breakout_y = y
                    break
            if breakout_label != "invalid":
                # Find initial PHOP: Parent immediately before PLBO must be PH
                sorted_parents = sorted(all_parents, key=lambda p: p[0])
                for i, (x, parent_type, y, label, _) in enumerate(sorted_parents):
                    if label == breakout_label and i > 0:
                        prev_x, prev_type, prev_y, prev_label, _ = sorted_parents[i - 1]
                        if prev_type == "PH" and receiver_x < prev_x < breakout_x:
                            order_parent_label = prev_label
                            actual_order_parent_label = prev_label
                            order_parent_y = prev_y
                            order_parent_x = prev_x
                            actual_order_parent_y = prev_y
                            actual_order_parent_x = prev_x
                        break
                # Check the parent immediately after the actual PHOP
                if actual_order_parent_label != "invalid":
                    actual_ph_candle = get_candle_for_parent(actual_order_parent_x, all_positions)[0]
                    if actual_ph_candle:
                        actual_top_y = actual_ph_candle[1]
                        # Find the next parent after actual PHOP
                        for i, (x, parent_type, y, label, _) in enumerate(sorted_parents):
                            if x == actual_order_parent_x and i + 1 < len(sorted_parents):
                                next_x, next_type, next_y, next_label, _ = sorted_parents[i + 1]
                                if next_type == "PH" and next_x < breakout_x:
                                    next_ph_candle = get_candle_for_parent(next_x, all_positions)[0]
                                    if next_ph_candle and next_ph_candle[1] < actual_top_y and receiver_x < next_x < breakout_x:
                                        print(f"Reassigning PHOP from {order_parent_label} to {next_label} at x={next_x} "
                                            f"because next PH top (y={next_ph_candle[1]}) < actual PHOP top (y={actual_top_y})")
                                        order_parent_label = next_label
                                        order_parent_x = next_x
                                        order_parent_y = next_y
                                        reassigned_op = True
                                        reassigned_op_label = next_label
                                        reassigned_op_x = next_x
                                        reassigned_op_top_y = next_ph_candle[1]
                                        reassigned_op_bottom_y = next_ph_candle[2]
                                        break
                # Find the highest PH (lowest top_y) before PLBO
                candidate_phs = [(x, y, label, *get_parent_y_coordinates(label, 'PH', all_positions, pl_labels, ph_labels))
                                for x, y, label, _ in ph_labels if x < breakout_x]
                candidate_phs.sort(key=lambda x: x[4])  # Sort by top_y ascending
                for ph_x, ph_y, ph_label, ph_top_y, ph_bottom_y in candidate_phs:
                    if ph_top_y is None or ph_bottom_y is None:
                        continue
                    # Only reassign if this PH is higher than the current OP (if valid) and within bounds
                    if order_parent_label != "invalid":
                        current_ph_candle = get_candle_for_parent(order_parent_x, all_positions)[0]
                        if current_ph_candle and ph_top_y < current_ph_candle[1] and receiver_x < ph_x < breakout_x:
                            print(f"Reassigning PHOP from {order_parent_label} to {ph_label} at x={ph_x} "
                                f"because new PH top (y={ph_top_y}) < current PHOP top (y={current_ph_candle[1]})")
                            order_parent_label = ph_label
                            order_parent_x = ph_x
                            order_parent_y = ph_y
                            reassigned_op = True
                            reassigned_op_label = ph_label
                            reassigned_op_x = ph_x
                            reassigned_op_top_y = ph_top_y
                            reassigned_op_bottom_y = ph_bottom_y
                            break
                    elif receiver_x < ph_x < breakout_x:
                        print(f"Assigning PHOP to {ph_label} at x={ph_x} as initial PHOP is invalid")
                        order_parent_label = ph_label
                        order_parent_x = ph_x
                        order_parent_y = ph_y
                        reassigned_op = True
                        reassigned_op_label = ph_label
                        reassigned_op_x = ph_x
                        reassigned_op_top_y = ph_top_y
                        reassigned_op_bottom_y = ph_bottom_y
                        break
        
        return (breakout_label, order_parent_label, order_parent_y, order_parent_x, breakout_x, breakout_y,
                reassigned_op, reassigned_op_label, reassigned_op_x, reassigned_op_top_y, reassigned_op_bottom_y,
                actual_order_parent_label, actual_order_parent_x, actual_order_parent_y)

    # All parents for finding PHBO/PLOP or PLBO/PHOP
    all_parents = [(x, 'PL', y, label, arrow_number) for x, y, label, arrow_number in pl_labels] + \
                  [(x, 'PH', y, label, arrow_number) for x, y, label, arrow_number in ph_labels]
    
    # PH-to-PH (top to top, green)
    ph_labels_sorted = sorted(ph_labels, key=lambda x: x[0])
    used_points_ph_to_ph = set()
    i = 0
    while i < len(ph_labels_sorted):
        x_ms, top_y_ms, label_ms, arrow_number_ms = ph_labels_sorted[i]  # MainSender (MSPH)
        
        # Find the next PH as ReceiverAndSender (RASPH)
        next_ph = None
        for j, (x_ras, top_y_ras, label_ras, arrow_number_ras) in enumerate(ph_labels_sorted[i+1:], start=i+1):
            next_ph = (x_ras, top_y_ras, label_ras, arrow_number_ras)
            break
        
        if next_ph is None:
            print(f"No PH found to the right of MSPH {label_ms}, skipping connection")
            i += 1
            continue
        
        x_ras, top_y_ras, label_ras, arrow_number_ras = next_ph  # ReceiverAndSender (RASPH)
        
        # Get MainSender candlestick
        ms_candle, ms_color = get_candle_for_parent(x_ms, all_positions)
        if ms_candle is None:
            print(f"No candlestick found for MSPH {label_ms} at x={x_ms}, skipping connection")
            i += 1
            continue
        ms_top_y = ms_candle[1]
        
        # Check if MainSender top is higher than ReceiverAndSender top
        ras_valid = True
        if ms_top_y >= top_y_ras:
            print(f"Skipping MSPH-to-RASPH trendline from {label_ms} to {label_ras} because MSPH top (y={ms_top_y}) "
                  f"is not higher than RASPH top (y={top_y_ras})")
            ras_valid = False
        
        if ras_valid and not allow_latest_main_trendline:
            ph_ahead_count = count_parents_ahead(x_ras, ph_labels)
            if ph_ahead_count == 0:
                print(f"No PH found ahead of RASPH {label_ras}, skipping MSPH-to-RASPH trendline from {label_ms}")
                ras_valid = False
        
        # Get receiver candlestick at specified position
        receiver_candle, receiver_color = get_candle_at_position(x_ras, main_trendline_position)
        if receiver_candle is None:
            print(f"No candlestick found at position {main_trendline_position} to the right of RASPH {label_ras}, skipping connection")
            ras_valid = False
        
        if ras_valid:
            vertical_distance = abs(top_y_ms - top_y_ras)
            if vertical_distance < distance_threshold:
                print(f"Skipping MSPH-to-RASPH trendline from {label_ms} to {label_ras} "
                      f"due to vertical distance {vertical_distance} < threshold {distance_threshold}")
                ras_valid = False
        
        if ras_valid:
            crosses, crossed_label = crosses_other_parent(
                x_ms, x_ras, top_y_ms, top_y_ras,
                pl_labels + ph_labels,
                exclude_labels={label_ms, label_ras}
            )
            if crosses:
                print(f"Skipping MSPH-to-RASPH trendline from {label_ms} to {label_ras} "
                      f"due to crossing {crossed_label}")
                ras_valid = False
        
        if ras_valid:
            end_x, end_y = extend_line_to_right_edge(x_ms, top_y_ms, x_ras, top_y_ras, img_width)
            crosses, crossed_label = crosses_other_parent(
                x_ras, end_x, top_y_ras, end_y,
                pl_labels + ph_labels,
                exclude_labels={label_ms, label_ras}
            )
            if crosses:
                print(f"Skipping MSPH-to-RASPH trendline extension from {label_ms} to {label_ras} "
                      f"due to crossing {crossed_label} in extension")
                ras_valid = False
        
        if ras_valid:
            # Find PHBO and PLOP for RASPH
            (breakout_label, order_parent_label, order_parent_y, order_parent_x, breakout_x, breakout_y,
             reassigned_op, reassigned_op_label, reassigned_op_x, reassigned_op_top_y, reassigned_op_bottom_y,
             actual_order_parent_label, actual_order_parent_x, actual_order_parent_y) = \
                find_breakout_and_order_parent(x_ras, top_y_ras, "PH-to-PH", all_parents, pl_labels, ph_labels, all_positions)
            # Determine order_status based on whether the box touches a candle
            order_status = "pending order"
            if breakout_label != "invalid" and order_parent_label != "invalid":
                # Check crossing with the Order Parent used for the box (reassigned or actual)
                crossing_label = order_parent_label if reassigned_op and reassigned_op_label == order_parent_label else actual_order_parent_label
                crossing_x = find_crossing_candle(breakout_x, crossing_label, "PH-to-PH", all_positions, pl_labels, ph_labels)
                if crossing_x is not None:
                    order_status = "executed"
            
            sender_pos_number = get_position_number_from_label(label_ms)
            sender_arrow_number = arrow_number_ms
            receiver_pos_number = get_position_number(x_ras, pl_labels + ph_labels, all_positions)
            
            # Determine order holder and append " order holder" to the appropriate parent
            order_parent_value = order_parent_label if order_parent_label and order_parent_label.startswith("PL") else "invalid"
            actual_order_parent_value = actual_order_parent_label if actual_order_parent_label and actual_order_parent_label.startswith("PL") else "invalid"
            reassigned_op_value = reassigned_op_label if reassigned_op_label else "none"
            if reassigned_op and reassigned_op_label == order_parent_label:
                order_parent_value = f"{order_parent_value} order holder"
            else:
                actual_order_parent_value = f"{actual_order_parent_value} order holder"
            
            # Create unique identifier for the trendline
            trendline_id = f"PH-to-PH_{sender_pos_number}_{receiver_pos_number}"
            if trendline_id not in unique_trendlines:
                main_trendline_entry = {
                    "type": "PH-to-PH",
                    "sender": {
                        "candle_color": ms_color,
                        "position_number": sender_pos_number,
                        "sender_arrow_number": sender_arrow_number
                    },
                    "receiver": {
                        "candle_color": receiver_color,
                        "position_number": receiver_pos_number,
                        "order_type": "long",
                        "order_status": order_status,
                        "Breakout_parent": breakout_label if breakout_label and breakout_label.startswith("PH") else "invalid",
                        "order_parent": order_parent_value,
                        "actual_orderparent": actual_order_parent_value,
                        "reassigned_orderparent": reassigned_op_value,
                        "receiver_contractcandle_arrownumber": arrow_number_ras
                    }
                }
                main_trendline_data.append(main_trendline_entry)
                contracts_data.append(main_trendline_entry)
                unique_trendlines.add(trendline_id)
                used_points_ph_to_ph.add(label_ms)
        
        # Try RASPH-to-RASRPH
        rasr_valid = False
        next_ph_rasr = None
        for j, (x_rasr, top_y_rasr, label_rasr, arrow_number_rasr) in enumerate(ph_labels_sorted[i+2:], start=i+2):
            next_ph_rasr = (x_rasr, top_y_rasr, label_rasr, arrow_number_rasr)
            break
        
        if next_ph_rasr is not None:
            x_rasr, top_y_rasr, label_rasr, arrow_number_rasr = next_ph_rasr
            ras_candle = get_candle_for_parent(x_ras, all_positions)[0]
            if ras_candle is None:
                print(f"No candlestick found for RASPH {label_ras} at x={x_ras}, skipping RASPH-to-RASRPH")
            else:
                ras_top_y = ras_candle[1]
                rasr_valid = True
                if ras_top_y >= top_y_rasr:
                    print(f"Skipping RASPH-to-RASRPH trendline from {label_ras} to {label_rasr} because RASPH top (y={ras_top_y}) "
                          f"is not higher than RASRPH top (y={top_y_rasr})")
                    rasr_valid = False
                
                if rasr_valid and not allow_latest_main_trendline:
                    ph_ahead_count = count_parents_ahead(x_rasr, ph_labels)
                    if ph_ahead_count == 0:
                        print(f"No PH found ahead of RASRPH {label_rasr}, skipping RASPH-to-RASRPH trendline from {label_ras}")
                        rasr_valid = False
                
                if rasr_valid:
                    receiver_candle_rasr, receiver_color_rasr = get_candle_at_position(x_rasr, main_trendline_position)
                    if receiver_candle_rasr is None:
                        print(f"No candlestick found at position {main_trendline_position} to the right of RASRPH {label_rasr}, skipping connection")
                        rasr_valid = False
                
                if rasr_valid:
                    vertical_distance = abs(top_y_ras - top_y_rasr)
                    if vertical_distance < distance_threshold:
                        print(f"Skipping RASPH-to-RASRPH trendline from {label_ras} to {label_rasr} "
                              f"due to vertical distance {vertical_distance} < threshold {distance_threshold}")
                        rasr_valid = False
                
                if rasr_valid:
                    crosses, crossed_label = crosses_other_parent(
                        x_ras, x_rasr, top_y_ras, top_y_rasr,
                        pl_labels + ph_labels,
                        exclude_labels={label_ras, label_rasr}
                    )
                    if crosses:
                        print(f"Skipping RASPH-to-RASRPH trendline from {label_ras} to {label_rasr} "
                              f"due to crossing {crossed_label}")
                        rasr_valid = False
                
                if rasr_valid:
                    end_x, end_y = extend_line_to_right_edge(x_ras, top_y_ras, x_rasr, top_y_rasr, img_width)
                    crosses, crossed_label = crosses_other_parent(
                        x_rasr, end_x, top_y_rasr, end_y,
                        pl_labels + ph_labels,
                        exclude_labels={label_ras, label_rasr}
                    )
                    if crosses:
                        print(f"Skipping RASPH-to-RASRPH trendline extension from {label_ras} to {label_rasr} "
                              f"due to crossing {crossed_label} in extension")
                        rasr_valid = False
                
                if rasr_valid:
                    # Find PHBO and PLOP for RASRPH
                    (breakout_label, order_parent_label, order_parent_y, order_parent_x, breakout_x, breakout_y,
                     reassigned_op, reassigned_op_label, reassigned_op_x, reassigned_op_top_y, reassigned_op_bottom_y,
                     actual_order_parent_label, actual_order_parent_x, actual_order_parent_y) = \
                        find_breakout_and_order_parent(x_rasr, top_y_rasr, "PH-to-PH", all_parents, pl_labels, ph_labels, all_positions)
                    # Determine order_status based on whether the box touches a candle
                    order_status = "pending order"
                    if breakout_label != "invalid" and order_parent_label != "invalid":
                        # Check crossing with the Order Parent used for the box (reassigned or actual)
                        crossing_label = order_parent_label if reassigned_op and reassigned_op_label == order_parent_label else actual_order_parent_label
                        crossing_x = find_crossing_candle(breakout_x, crossing_label, "PH-to-PH", all_positions, pl_labels, ph_labels)
                        if crossing_x is not None:
                            order_status = "executed"
                    
                    sender_pos_number = get_position_number_from_label(label_ras)
                    sender_arrow_number = arrow_number_ras
                    receiver_pos_number = get_position_number(x_rasr, pl_labels + ph_labels, all_positions)
                    
                    # Determine order holder and append " order holder" to the appropriate parent
                    order_parent_value = order_parent_label if order_parent_label and order_parent_label.startswith("PL") else "invalid"
                    actual_order_parent_value = actual_order_parent_label if actual_order_parent_label and actual_order_parent_label.startswith("PL") else "invalid"
                    reassigned_op_value = reassigned_op_label if reassigned_op_label else "none"
                    if reassigned_op and reassigned_op_label == order_parent_label:
                        order_parent_value = f"{order_parent_value} order holder"
                    else:
                        actual_order_parent_value = f"{actual_order_parent_value} order holder"
                    
                    # Create unique identifier for the trendline
                    trendline_id = f"PH-to-PH_{sender_pos_number}_{receiver_pos_number}"
                    if trendline_id not in unique_trendlines:
                        main_trendline_entry = {
                            "type": "PH-to-PH",
                            "sender": {
                                "candle_color": get_candle_for_parent(x_ras, all_positions)[1],
                                "position_number": sender_pos_number,
                                "sender_arrow_number": sender_arrow_number
                            },
                            "receiver": {
                                "candle_color": receiver_color_rasr,
                                "position_number": receiver_pos_number,
                                "order_type": "long",
                                "order_status": order_status,
                                "Breakout_parent": breakout_label if breakout_label and breakout_label.startswith("PH") else "invalid",
                                "order_parent": order_parent_value,
                                "actual_orderparent": actual_order_parent_value,
                                "reassigned_orderparent": reassigned_op_value,
                                "receiver_contractcandle_arrownumber": arrow_number_rasr
                            }
                        }
                        main_trendline_data.append(main_trendline_entry)
                        contracts_data.append(main_trendline_entry)
                        unique_trendlines.add(trendline_id)
                        used_points_ph_to_ph.add(label_ras)
        
        # Try MSPH-to-RASRPH
        if not rasr_valid and next_ph_rasr is not None:
            x_rasr, top_y_rasr, label_rasr, arrow_number_rasr = next_ph_rasr
            ras_rasr_distance = abs(top_y_ras - top_y_rasr)
            if ras_rasr_distance <= 40:
                print(f"Skipping MSPH-to-RASRPH trendline from {label_ms} to {label_rasr} "
                      f"because RASPH-RASRPH vertical distance {ras_rasr_distance}px <= 40px")
                i = next((idx for idx, lbl in enumerate(ph_labels_sorted) if lbl[2] == label_ras), i + 1) if ras_valid else i + 1
                continue
            if ras_rasr_distance <= 60:
                print(f"Skipping MSPH-to-RASRPH trendline from {label_ms} to {label_rasr} "
                      f"because RASPH-RASRPH vertical distance {ras_rasr_distance}px is between 40px and 60px")
                i = next((idx for idx, lbl in enumerate(ph_labels_sorted) if lbl[2] == label_ras), i + 1) if ras_valid else i + 1
                continue
            
            ms_rasr_valid = True
            if ms_top_y >= top_y_rasr:
                print(f"Skipping MSPH-to-RASRPH trendline from {label_ms} to {label_rasr} because MSPH top (y={ms_top_y}) "
                      f"is not higher than RASRPH top (y={top_y_rasr})")
                ms_rasr_valid = False
            
            if ms_rasr_valid and not allow_latest_main_trendline:
                ph_ahead_count = count_parents_ahead(x_rasr, ph_labels)
                if ph_ahead_count == 0:
                    print(f"No PH found ahead of RASRPH {label_rasr}, skipping MSPH-to-RASRPH trendline from {label_ms}")
                    ms_rasr_valid = False
            
            if ms_rasr_valid:
                receiver_candle_rasr, receiver_color_rasr = get_candle_at_position(x_rasr, main_trendline_position)
                if receiver_candle_rasr is None:
                    print(f"No candlestick found at position {main_trendline_position} to the right of RASRPH {label_rasr}, skipping MSPH-to-RASRPH")
                    ms_rasr_valid = False
            
            if ms_rasr_valid:
                vertical_distance = abs(top_y_ms - top_y_rasr)
                if vertical_distance < distance_threshold:
                    print(f"Skipping MSPH-to-RASRPH trendline from {label_ms} to {label_rasr} "
                          f"due to vertical distance {vertical_distance} < threshold {distance_threshold}")
                    ms_rasr_valid = False
            
            if ms_rasr_valid:
                crosses, crossed_label = crosses_other_parent(
                    x_ms, x_rasr, top_y_ms, top_y_rasr,
                    pl_labels + ph_labels,
                    exclude_labels={label_ms, label_rasr}
                )
                if crosses:
                    print(f"Skipping MSPH-to-RASRPH trendline from {label_ms} to {label_rasr} "
                          f"due to crossing {crossed_label}")
                    ms_rasr_valid = False
            
            if ms_rasr_valid:
                end_x, end_y = extend_line_to_right_edge(x_ms, top_y_ms, x_rasr, top_y_rasr, img_width)
                crosses, crossed_label = crosses_other_parent(
                    x_rasr, end_x, top_y_rasr, end_y,
                    pl_labels + ph_labels,
                    exclude_labels={label_ms, label_rasr}
                )
                if crosses:
                    print(f"Skipping MSPH-to-RASRPH trendline extension from {label_ms} to {label_rasr} "
                          f"due to crossing {crossed_label} in extension")
                    ms_rasr_valid = False
            
            if ms_rasr_valid:
                # Find PHBO and PLOP for RASRPH
                (breakout_label, order_parent_label, order_parent_y, order_parent_x, breakout_x, breakout_y,
                 reassigned_op, reassigned_op_label, reassigned_op_x, reassigned_op_top_y, reassigned_op_bottom_y,
                 actual_order_parent_label, actual_order_parent_x, actual_order_parent_y) = \
                    find_breakout_and_order_parent(x_rasr, top_y_rasr, "PH-to-PH", all_parents, pl_labels, ph_labels, all_positions)
                # Determine order_status based on whether the box touches a candle
                order_status = "pending order"
                if breakout_label != "invalid" and order_parent_label != "invalid":
                    # Check crossing with the Order Parent used for the box (reassigned or actual)
                    crossing_label = order_parent_label if reassigned_op and reassigned_op_label == order_parent_label else actual_order_parent_label
                    crossing_x = find_crossing_candle(breakout_x, crossing_label, "PH-to-PH", all_positions, pl_labels, ph_labels)
                    if crossing_x is not None:
                        order_status = "executed"
                
                sender_pos_number = get_position_number_from_label(label_ms)
                sender_arrow_number = arrow_number_ms
                receiver_pos_number = get_position_number(x_rasr, pl_labels + ph_labels, all_positions)
                
                # Determine order holder and append " order holder" to the appropriate parent
                order_parent_value = order_parent_label if order_parent_label and order_parent_label.startswith("PL") else "invalid"
                actual_order_parent_value = actual_order_parent_label if actual_order_parent_label and actual_order_parent_label.startswith("PL") else "invalid"
                reassigned_op_value = reassigned_op_label if reassigned_op_label else "none"
                if reassigned_op and reassigned_op_label == order_parent_label:
                    order_parent_value = f"{order_parent_value} order holder"
                else:
                    actual_order_parent_value = f"{actual_order_parent_value} order holder"
                
                # Create unique identifier for the trendline
                trendline_id = f"PH-to-PH_{sender_pos_number}_{receiver_pos_number}"
                if trendline_id not in unique_trendlines:
                    main_trendline_entry = {
                        "type": "PH-to-PH",
                        "sender": {
                            "candle_color": ms_color,
                            "position_number": sender_pos_number,
                            "sender_arrow_number": sender_arrow_number
                        },
                        "receiver": {
                            "candle_color": receiver_color_rasr,
                            "position_number": receiver_pos_number,
                            "order_type": "long",
                            "order_status": order_status,
                            "Breakout_parent": breakout_label if breakout_label and breakout_label.startswith("PH") else "invalid",
                            "order_parent": order_parent_value,
                            "actual_orderparent": actual_order_parent_value,
                            "reassigned_orderparent": reassigned_op_value,
                            "receiver_contractcandle_arrownumber": arrow_number_rasr
                        }
                    }
                    main_trendline_data.append(main_trendline_entry)
                    contracts_data.append(main_trendline_entry)
                    unique_trendlines.add(trendline_id)
                    used_points_ph_to_ph.add(label_ms)
        
        i = next((idx for idx, lbl in enumerate(ph_labels_sorted) if lbl[2] == label_ras), i + 1) if ras_valid else i + 1
    
    # PL-to-PL (bottom to bottom, yellow)
    pl_labels_sorted = sorted(pl_labels, key=lambda x: x[0])
    used_points_pl_to_pl = set()
    i = 0
    while i < len(pl_labels_sorted):
        x_ms, bottom_y_ms, label_ms, arrow_number_ms = pl_labels_sorted[i]  # MainSender (MSPL)
        
        # Find the next PL as ReceiverAndSender (RASPL)
        next_pl = None
        for j, (x_ras, bottom_y_ras, label_ras, arrow_number_ras) in enumerate(pl_labels_sorted[i+1:], start=i+1):
            next_pl = (x_ras, bottom_y_ras, label_ras, arrow_number_ras)
            break
        
        if next_pl is None:
            print(f"No PL found to the right of MSPL {label_ms}, skipping connection")
            i += 1
            continue
        
        x_ras, bottom_y_ras, label_ras, arrow_number_ras = next_pl  # ReceiverAndSender (RASPL)
        
        # Get MainSender candlestick
        ms_candle, ms_color = get_candle_for_parent(x_ms, all_positions)
        if ms_candle is None:
            print(f"No candlestick found for MSPL {label_ms} at x={x_ms}, skipping connection")
            i += 1
            continue
        ms_bottom_y = ms_candle[2]
        
        # Check if MainSender bottom is lower than ReceiverAndSender bottom
        ras_valid = True
        if ms_bottom_y <= bottom_y_ras:
            print(f"Skipping MSPL-to-RASPL trendline from {label_ms} to {label_ras} because MSPL bottom (y={ms_bottom_y}) "
                  f"is not lower than RASPL bottom (y={bottom_y_ras})")
            ras_valid = False
        
        if ras_valid and not allow_latest_main_trendline:
            pl_ahead_count = count_parents_ahead(x_ras, pl_labels)
            if pl_ahead_count == 0:
                print(f"No PL found ahead of RASPL {label_ras}, skipping MSPL-to-RASPL trendline from {label_ms}")
                ras_valid = False
        
        # Get receiver candlestick at specified position
        receiver_candle, receiver_color = get_candle_at_position(x_ras, main_trendline_position)
        if receiver_candle is None:
            print(f"No candlestick found at position {main_trendline_position} to the right of RASPL {label_ras}, skipping connection")
            ras_valid = False
        
        if ras_valid:
            vertical_distance = abs(bottom_y_ms - bottom_y_ras)
            if vertical_distance < distance_threshold:
                print(f"Skipping MSPL-to-RASPL trendline from {label_ms} to {label_ras} "
                      f"due to vertical distance {vertical_distance} < threshold {distance_threshold}")
                ras_valid = False
        
        if ras_valid:
            crosses, crossed_label = crosses_other_parent(
                x_ms, x_ras, bottom_y_ms, bottom_y_ras,
                pl_labels + ph_labels,
                exclude_labels={label_ms, label_ras}
            )
            if crosses:
                print(f"Skipping MSPL-to-RASPL trendline from {label_ms} to {label_ras} "
                      f"due to crossing {crossed_label}")
                ras_valid = False
        
        if ras_valid:
            end_x, end_y = extend_line_to_right_edge(x_ms, bottom_y_ms, x_ras, bottom_y_ras, img_width)
            crosses, crossed_label = crosses_other_parent(
                x_ras, end_x, bottom_y_ras, end_y,
                pl_labels + ph_labels,
                exclude_labels={label_ms, label_ras}
            )
            if crosses:
                print(f"Skipping MSPL-to-RASPL trendline extension from {label_ms} to {label_ras} "
                      f"due to crossing {crossed_label} in extension")
                ras_valid = False
        
        if ras_valid:
            # Find PLBO and PHOP for RASPL
            (breakout_label, order_parent_label, order_parent_y, order_parent_x, breakout_x, breakout_y,
             reassigned_op, reassigned_op_label, reassigned_op_x, reassigned_op_top_y, reassigned_op_bottom_y,
             actual_order_parent_label, actual_order_parent_x, actual_order_parent_y) = \
                find_breakout_and_order_parent(x_ras, bottom_y_ras, "PL-to-PL", all_parents, pl_labels, ph_labels, all_positions)
            # Determine order_status based on whether the box touches a candle
            order_status = "pending order"
            if breakout_label != "invalid" and order_parent_label != "invalid":
                # Check crossing with the Order Parent used for the box (reassigned or actual)
                crossing_label = order_parent_label if reassigned_op and reassigned_op_label == order_parent_label else actual_order_parent_label
                crossing_x = find_crossing_candle(breakout_x, crossing_label, "PL-to-PL", all_positions, pl_labels, ph_labels)
                if crossing_x is not None:
                    order_status = "executed"
            
            sender_pos_number = get_position_number_from_label(label_ms)
            sender_arrow_number = arrow_number_ms
            receiver_pos_number = get_position_number(x_ras, pl_labels + ph_labels, all_positions)
            
            # Determine order holder and append " order holder" to the appropriate parent
            order_parent_value = order_parent_label if order_parent_label and order_parent_label.startswith("PH") else "invalid"
            actual_order_parent_value = actual_order_parent_label if actual_order_parent_label and actual_order_parent_label.startswith("PH") else "invalid"
            reassigned_op_value = reassigned_op_label if reassigned_op_label else "none"
            if reassigned_op and reassigned_op_label == order_parent_label:
                order_parent_value = f"{order_parent_value} order holder"
            else:
                actual_order_parent_value = f"{actual_order_parent_value} order holder"
            
            # Create unique identifier for the trendline
            trendline_id = f"PL-to-PL_{sender_pos_number}_{receiver_pos_number}"
            if trendline_id not in unique_trendlines:
                main_trendline_entry = {
                    "type": "PL-to-PL",
                    "sender": {
                        "candle_color": ms_color,
                        "position_number": sender_pos_number,
                        "sender_arrow_number": arrow_number_ms
                    },
                    "receiver": {
                        "candle_color": receiver_color,
                        "position_number": receiver_pos_number,
                        "order_type": "short",
                        "order_status": order_status,
                        "Breakout_parent": breakout_label if breakout_label and breakout_label.startswith("PL") else "invalid",
                        "order_parent": order_parent_value,
                        "actual_orderparent": actual_order_parent_value,
                        "reassigned_orderparent": reassigned_op_value,
                        "receiver_contractcandle_arrownumber": arrow_number_ras
                    }
                }
                main_trendline_data.append(main_trendline_entry)
                contracts_data.append(main_trendline_entry)
                unique_trendlines.add(trendline_id)
                used_points_pl_to_pl.add(label_ms)
        
        # Try RASPL-to-RASRPL
        rasr_valid = False
        next_pl_rasr = None
        for j, (x_rasr, bottom_y_rasr, label_rasr, arrow_number_rasr) in enumerate(pl_labels_sorted[i+2:], start=i+2):
            next_pl_rasr = (x_rasr, bottom_y_rasr, label_rasr, arrow_number_rasr)
            break
        
        if next_pl_rasr is not None:
            x_rasr, bottom_y_rasr, label_rasr, arrow_number_rasr = next_pl_rasr
            ras_candle = get_candle_for_parent(x_ras, all_positions)[0]
            if ras_candle is None:
                print(f"No candlestick found for RASPL {label_ras} at x={x_ras}, skipping RASPL-to-RASRPL")
            else:
                ras_bottom_y = ras_candle[2]
                rasr_valid = True
                if ras_bottom_y <= bottom_y_rasr:
                    print(f"Skipping RASPL-to-RASRPL trendline from {label_ras} to {label_rasr} because RASPL bottom (y={ras_bottom_y}) "
                          f"is not lower than RASRPL bottom (y={bottom_y_rasr})")
                    rasr_valid = False
                
                if rasr_valid and not allow_latest_main_trendline:
                    pl_ahead_count = count_parents_ahead(x_rasr, pl_labels)
                    if pl_ahead_count == 0:
                        print(f"No PL found ahead of RASRPL {label_rasr}, skipping RASPL-to-RASRPL trendline from {label_ras}")
                        rasr_valid = False
                
                if rasr_valid:
                    receiver_candle_rasr, receiver_color_rasr = get_candle_at_position(x_rasr, main_trendline_position)
                    if receiver_candle_rasr is None:
                        print(f"No candlestick found at position {main_trendline_position} to the right of RASRPL {label_rasr}, skipping connection")
                        rasr_valid = False
                
                if rasr_valid:
                    vertical_distance = abs(bottom_y_ras - bottom_y_rasr)
                    if vertical_distance < distance_threshold:
                        print(f"Skipping RASPL-to-RASRPL trendline from {label_ras} to {label_rasr} "
                              f"due to vertical distance {vertical_distance} < threshold {distance_threshold}")
                        rasr_valid = False
                
                if rasr_valid:
                    crosses, crossed_label = crosses_other_parent(
                        x_ras, x_rasr, bottom_y_ras, bottom_y_rasr,
                        pl_labels + ph_labels,
                        exclude_labels={label_ras, label_rasr}
                    )
                    if crosses:
                        print(f"Skipping RASPL-to-RASRPL trendline from {label_ras} to {label_rasr} "
                              f"due to crossing {crossed_label}")
                        rasr_valid = False
                
                if rasr_valid:
                    end_x, end_y = extend_line_to_right_edge(x_ras, bottom_y_ras, x_rasr, bottom_y_rasr, img_width)
                    crosses, crossed_label = crosses_other_parent(
                        x_rasr, end_x, bottom_y_rasr, end_y,
                        pl_labels + ph_labels,
                        exclude_labels={label_ras, label_rasr}
                    )
                    if crosses:
                        print(f"Skipping RASPL-to-RASRPL trendline extension from {label_ras} to {label_rasr} "
                              f"due to crossing {crossed_label} in extension")
                        rasr_valid = False
                
                if rasr_valid:
                    # Find PLBO and PHOP for RASRPL
                    (breakout_label, order_parent_label, order_parent_y, order_parent_x, breakout_x, breakout_y,
                     reassigned_op, reassigned_op_label, reassigned_op_x, reassigned_op_top_y, reassigned_op_bottom_y,
                     actual_order_parent_label, actual_order_parent_x, actual_order_parent_y) = \
                        find_breakout_and_order_parent(x_rasr, bottom_y_rasr, "PL-to-PL", all_parents, pl_labels, ph_labels, all_positions)
                    # Determine order_status based on whether the box touches a candle
                    order_status = "pending order"
                    if breakout_label != "invalid" and order_parent_label != "invalid":
                        # Check crossing with the Order Parent used for the box (reassigned or actual)
                        crossing_label = order_parent_label if reassigned_op and reassigned_op_label == order_parent_label else actual_order_parent_label
                        crossing_x = find_crossing_candle(breakout_x, crossing_label, "PL-to-PL", all_positions, pl_labels, ph_labels)
                        if crossing_x is not None:
                            order_status = "executed"
                    
                    sender_pos_number = get_position_number_from_label(label_ras)
                    sender_arrow_number = arrow_number_ras
                    receiver_pos_number = get_position_number(x_rasr, pl_labels + ph_labels, all_positions)
                    
                    # Determine order holder and append " order holder" to the appropriate parent
                    order_parent_value = order_parent_label if order_parent_label and order_parent_label.startswith("PH") else "invalid"
                    actual_order_parent_value = actual_order_parent_label if actual_order_parent_label and actual_order_parent_label.startswith("PH") else "invalid"
                    reassigned_op_value = reassigned_op_label if reassigned_op_label else "none"
                    if reassigned_op and reassigned_op_label == order_parent_label:
                        order_parent_value = f"{order_parent_value} order holder"
                    else:
                        actual_order_parent_value = f"{actual_order_parent_value} order holder"
                    
                    # Create unique identifier for the trendline
                    trendline_id = f"PL-to-PL_{sender_pos_number}_{receiver_pos_number}"
                    if trendline_id not in unique_trendlines:
                        main_trendline_entry = {
                            "type": "PL-to-PL",
                            "sender": {
                                "candle_color": get_candle_for_parent(x_ras, all_positions)[1],
                                "position_number": sender_pos_number,
                                "sender_arrow_number": sender_arrow_number
                            },
                            "receiver": {
                                "candle_color": receiver_color_rasr,
                                "position_number": receiver_pos_number,
                                "order_type": "short",
                                "order_status": order_status,
                                "Breakout_parent": breakout_label if breakout_label and breakout_label.startswith("PL") else "invalid",
                                "order_parent": order_parent_value,
                                "actual_orderparent": actual_order_parent_value,
                                "reassigned_orderparent": reassigned_op_value,
                                "receiver_contractcandle_arrownumber": arrow_number_rasr
                            }
                        }
                        main_trendline_data.append(main_trendline_entry)
                        contracts_data.append(main_trendline_entry)
                        unique_trendlines.add(trendline_id)
                        used_points_pl_to_pl.add(label_ras)
        
        # Try MSPL-to-RASRPL
        if not rasr_valid and next_pl_rasr is not None:
            x_rasr, bottom_y_rasr, label_rasr, arrow_number_rasr = next_pl_rasr
            ras_rasr_distance = abs(bottom_y_ras - bottom_y_rasr)
            if ras_rasr_distance <= 40:
                print(f"Skipping MSPL-to-RASRPL trendline from {label_ms} to {label_rasr} "
                      f"because RASPL-RASRPL vertical distance {ras_rasr_distance}px <= 40px")
                i = next((idx for idx, lbl in enumerate(pl_labels_sorted) if lbl[2] == label_ras), i + 1) if ras_valid else i + 1
                continue
            if ras_rasr_distance <= 60:
                print(f"Skipping MSPL-to-RASRPL trendline from {label_ms} to {label_rasr} "
                      f"because RASPL-RASRPL vertical distance {ras_rasr_distance}px is between 40px and 60px")
                i = next((idx for idx, lbl in enumerate(pl_labels_sorted) if lbl[2] == label_ras), i + 1) if ras_valid else i + 1
                continue
            
            ms_rasr_valid = True
            if ms_bottom_y <= bottom_y_rasr:
                print(f"Skipping MSPL-to-RASRPL trendline from {label_ms} to {label_rasr} because MSPL bottom (y={ms_bottom_y}) "
                      f"is not lower than RASRPL bottom (y={bottom_y_rasr})")
                ms_rasr_valid = False
            
            if ms_rasr_valid and not allow_latest_main_trendline:
                pl_ahead_count = count_parents_ahead(x_rasr, pl_labels)
                if pl_ahead_count == 0:
                    print(f"No PL found ahead of RASRPL {label_rasr}, skipping MSPL-to-RASRPL trendline from {label_ms}")
                ms_rasr_valid = False
            
            if ms_rasr_valid:
                receiver_candle_rasr, receiver_color_rasr = get_candle_at_position(x_rasr, main_trendline_position)
                if receiver_candle_rasr is None:
                    print(f"No candlestick found at position {main_trendline_position} to the right of RASRPL {label_rasr}, skipping MSPL-to-RASRPL")
                    ms_rasr_valid = False
            
            if ms_rasr_valid:
                vertical_distance = abs(bottom_y_ms - bottom_y_rasr)
                if vertical_distance < distance_threshold:
                    print(f"Skipping MSPL-to-RASRPL trendline from {label_ms} to {label_rasr} "
                          f"due to vertical distance {vertical_distance} < threshold {distance_threshold}")
                    ms_rasr_valid = False
            
            if ms_rasr_valid:
                crosses, crossed_label = crosses_other_parent(
                    x_ms, x_rasr, bottom_y_ms, bottom_y_rasr,
                    pl_labels + ph_labels,
                    exclude_labels={label_ms, label_rasr}
                )
                if crosses:
                    print(f"Skipping MSPL-to-RASRPL trendline from {label_ms} to {label_rasr} "
                          f"due to crossing {crossed_label}")
                    ms_rasr_valid = False
            
            if ms_rasr_valid:
                end_x, end_y = extend_line_to_right_edge(x_ms, bottom_y_ms, x_rasr, bottom_y_rasr, img_width)
                crosses, crossed_label = crosses_other_parent(
                    x_rasr, end_x, bottom_y_rasr, end_y,
                    pl_labels + ph_labels,
                    exclude_labels={label_ms, label_rasr}
                )
                if crosses:
                    print(f"Skipping MSPL-to-RASRPL trendline extension from {label_ms} to {label_rasr} "
                          f"due to crossing {crossed_label} in extension")
                    ms_rasr_valid = False
            
            if ms_rasr_valid:
                # Find PLBO and PHOP for RASRPL
                (breakout_label, order_parent_label, order_parent_y, order_parent_x, breakout_x, breakout_y,
                 reassigned_op, reassigned_op_label, reassigned_op_x, reassigned_op_top_y, reassigned_op_bottom_y,
                 actual_order_parent_label, actual_order_parent_x, actual_order_parent_y) = \
                    find_breakout_and_order_parent(x_rasr, bottom_y_rasr, "PL-to-PL", all_parents, pl_labels, ph_labels, all_positions)
                # Determine order_status based on whether the box touches a candle
                order_status = "pending order"
                if breakout_label != "invalid" and order_parent_label != "invalid":
                    # Check crossing with the Order Parent used for the box (reassigned or actual)
                    crossing_label = order_parent_label if reassigned_op and reassigned_op_label == order_parent_label else actual_order_parent_label
                    crossing_x = find_crossing_candle(breakout_x, crossing_label, "PL-to-PL", all_positions, pl_labels, ph_labels)
                    if crossing_x is not None:
                        order_status = "executed"
                
                sender_pos_number = get_position_number_from_label(label_ms)
                sender_arrow_number = arrow_number_ms
                receiver_pos_number = get_position_number(x_rasr, pl_labels + ph_labels, all_positions)
                
                # Determine order holder and append " order holder" to the appropriate parent
                order_parent_value = order_parent_label if order_parent_label and order_parent_label.startswith("PH") else "invalid"
                actual_order_parent_value = actual_order_parent_label if actual_order_parent_label and actual_order_parent_label.startswith("PH") else "invalid"
                reassigned_op_value = reassigned_op_label if reassigned_op_label else "none"
                if reassigned_op and reassigned_op_label == order_parent_label:
                    order_parent_value = f"{order_parent_value} order holder"
                else:
                    actual_order_parent_value = f"{actual_order_parent_value} order holder"
                
                # Create unique identifier for the trendline
                trendline_id = f"PL-to-PL_{sender_pos_number}_{receiver_pos_number}"
                if trendline_id not in unique_trendlines:
                    main_trendline_entry = {
                        "type": "PL-to-PL",
                        "sender": {
                            "candle_color": ms_color,
                            "position_number": sender_pos_number,
                            "sender_arrow_number": arrow_number_ms
                        },
                        "receiver": {
                            "candle_color": receiver_color_rasr,
                            "position_number": receiver_pos_number,
                            "order_type": "short",
                            "order_status": order_status,
                            "Breakout_parent": breakout_label if breakout_label and breakout_label.startswith("PL") else "invalid",
                            "order_parent": order_parent_value,
                            "actual_orderparent": actual_order_parent_value,
                            "reassigned_orderparent": reassigned_op_value,
                            "receiver_contractcandle_arrownumber": arrow_number_rasr
                        }
                    }
                    main_trendline_data.append(main_trendline_entry)
                    contracts_data.append(main_trendline_entry)
                    unique_trendlines.add(trendline_id)
                    used_points_pl_to_pl.add(label_ms)
        
        i = next((idx for idx, lbl in enumerate(pl_labels_sorted) if lbl[2] == label_ras), i + 1) if ras_valid else i + 1
    
    # Sort trendlines by sender position number
    main_trendline_data.sort(key=lambda x: x['sender']['position_number'])
    contracts_data.sort(key=lambda x: x['sender']['position_number'])
    
    # Select the number of trendlines to draw (rightmost first)
    main_trendlines_to_draw = sorted(main_trendline_data, key=lambda x: x['receiver']['position_number'], reverse=True)[:num_contracts]
    print(f"Total valid trendlines: {len(main_trendline_data)}, drawing {min(num_contracts, len(main_trendline_data))} trendlines")
    
    if num_contracts == 0:
        print("Number of contracts set to 0, only PH/PL labels and position numbers drawn")
        main_trendline_image_path = os.path.join(OUTPUT_FOLDER, f"{base_name}_parent_main_trendlines.png")
        cv2.imwrite(main_trendline_image_path, img_main_trendlines)
        print(f"Parent trendlines image saved to: {main_trendline_image_path}")
        save_contracts_data_to_json(contracts_data)
        return main_trendline_image_path, main_trendline_data
    
    # Draw selected trendlines and Order Parent boxes
    for main_trendline in main_trendlines_to_draw:
        sender_label = f"{main_trendline['type'].split('-')[0]}{main_trendline['sender']['position_number']}"
        receiver_label = f"{main_trendline['type'].split('-')[0]}{main_trendline['receiver']['receiver_contractcandle_arrownumber']}"
        
        sender_x = next((x for x, _, label, _ in (ph_labels if main_trendline['type'] == 'PH-to-PH' else pl_labels) if label == sender_label), None)
        sender_y = next((y for x, y, label, _ in (ph_labels if main_trendline['type'] == 'PH-to-PH' else pl_labels) if label == sender_label), None)
        receiver_x = next((x for x, _, label, _ in (ph_labels if main_trendline['type'] == 'PH-to-PH' else pl_labels) if label == receiver_label), None)
        receiver_y = next((y for x, y, label, _ in (ph_labels if main_trendline['type'] == 'PH-to-PH' else pl_labels) if label == receiver_label), None)
        
        if sender_x is None or sender_y is None or receiver_x is None or receiver_y is None:
            print(f"Skipping trendline drawing for {main_trendline['type']} due to missing coordinates")
            continue
        
        # Draw main trendline
        start = (sender_x, sender_y)
        end = extend_line_to_right_edge(sender_x, sender_y, receiver_x, receiver_y, img_width)
        color = (0, 255, 0) if main_trendline['type'] == 'PH-to-PH' else (0, 255, 255)  # Green or Yellow
        cv2.line(img_main_trendlines, start, end, color, 2)
        
        # Find Order Parent and Breakout Parent for the box
        order_parent_label = main_trendline['receiver']['order_parent'].replace(" order holder", "")
        actual_order_parent_label = main_trendline['receiver']['actual_orderparent'].replace(" order holder", "")
        breakout_label = main_trendline['receiver']['Breakout_parent']
        order_parent_candle = None
        order_parent_x = None
        breakout_x = None
        top_y = None
        bottom_y = None
        
        # Check if OP was reassigned
        (breakout_label_check, order_parent_label_check, order_parent_y, order_parent_x, breakout_x_check, breakout_y,
         reassigned_op, reassigned_op_label, reassigned_op_x, reassigned_op_top_y, reassigned_op_bottom_y,
         actual_order_parent_label_check, actual_order_parent_x, actual_order_parent_y) = \
            find_breakout_and_order_parent(
                receiver_x, receiver_y, main_trendline['type'], all_parents, pl_labels, ph_labels, all_positions
            )
        
        # Only proceed with box drawing if order_parent_label is valid and matches the expected type
        if order_parent_label != "invalid" and \
           ((main_trendline['type'] == 'PH-to-PH' and order_parent_label.startswith("PL")) or \
            (main_trendline['type'] == 'PL-to-PL' and order_parent_label.startswith("PH"))):
            if reassigned_op and reassigned_op_label == order_parent_label:
                # Use reassigned OP coordinates for box dimensions
                order_parent_candle = (reassigned_op_x, reassigned_op_top_y, reassigned_op_bottom_y)
                order_parent_x = reassigned_op_x
                top_y = reassigned_op_top_y
                bottom_y = reassigned_op_bottom_y
                print(f"Using reassigned OP {order_parent_label} at x={order_parent_x} for box drawing")
            else:
                # Get Order Parent coordinates and candlestick
                for x, y, label, _ in (pl_labels if main_trendline['type'] == 'PH-to-PH' else ph_labels):
                    if label == order_parent_label:
                        order_parent_candle, _ = get_candle_for_parent(x, all_positions)
                        order_parent_x = x
                        break
            # Get Breakout Parent x-coordinate if available
            if breakout_label != "invalid":
                for x, y, label, _ in (ph_labels if main_trendline['type'] == 'PH-to-PH' else pl_labels):
                    if label == breakout_label:
                        breakout_x = x
                        break
        
        if order_parent_candle is None:
            print(f"No candlestick found for order_parent {order_parent_label} at x={order_parent_x}, skipping box")
            continue
        
        # Get y-coordinates for the box
        if top_y is None or bottom_y is None:
            if main_trendline['type'] == 'PH-to-PH':
                # For PH-to-PH: Box from top to bottom of PLOP
                top_y, bottom_y = get_parent_y_coordinates(order_parent_label, 'PL', all_positions, pl_labels, ph_labels)
            else:
                # For PL-to-PL: Box from bottom to top of PHOP
                bottom_y, top_y = get_parent_y_coordinates(order_parent_label, 'PH', all_positions, pl_labels, ph_labels)
        
        if top_y is None or bottom_y is None:
            print(f"Skipping box for {main_trendline['type']} from {order_parent_label} due to missing y-coordinates")
            continue
        
        # Determine box right edge and log
        box_right_x = img_width
        if breakout_x is not None:
            # Check crossing with the Order Parent used for the box (reassigned or actual)
            crossing_label = order_parent_label if reassigned_op and reassigned_op_label == order_parent_label else actual_order_parent_label
            crossing_x = find_crossing_candle(breakout_x, crossing_label, main_trendline['type'], all_positions, pl_labels, ph_labels)
            if crossing_x is not None:
                box_right_x = crossing_x
                print(f"Drew 1px white box for {main_trendline['type']} from {'reassigned' if reassigned_op and reassigned_op_label == order_parent_label else 'actual'} "
                      f"OP {order_parent_label} (x={order_parent_x}, top_y={top_y}, bottom_y={bottom_y}) to crossing candlestick (x={crossing_x})")
            else:
                print(f"Drew 1px white box for {main_trendline['type']} from {'reassigned' if reassigned_op and reassigned_op_label == order_parent_label else 'actual'} "
                      f"OP {order_parent_label} (x={order_parent_x}, top_y={top_y}, bottom_y={bottom_y}) to right edge (x={img_width})")
        else:
            print(f"Drew 1px white box for {main_trendline['type']} from {'reassigned' if reassigned_op and reassigned_op_label == order_parent_label else 'actual'} "
                  f"OP {order_parent_label} (x={order_parent_x}, top_y={top_y}, bottom_y={bottom_y}) to right edge (x={img_width}) due to no breakout parent")
        
        # Draw the 1px solid white box
        top_left = (order_parent_x, top_y)
        bottom_right = (box_right_x, bottom_y)
        cv2.rectangle(img_main_trendlines, top_left, bottom_right, (255, 255, 255), 1)  # White, 1px thickness
        
        print(f"Connected {main_trendline['type']} from sender (pos={main_trendline['sender']['position_number']}, "
              f"color={main_trendline['sender']['candle_color']}, arrow={main_trendline['sender']['sender_arrow_number']}) "
              f"to receiver (pos={main_trendline['receiver']['position_number']}, "
              f"color={main_trendline['receiver']['candle_color']}, arrow={main_trendline['receiver']['receiver_contractcandle_arrownumber']}) "
              f"with {'green' if main_trendline['type'] == 'PH-to-PH' else 'yellow'} trendline, "
              f"order_type={main_trendline['receiver']['order_type']}, order_status={main_trendline['receiver']['order_status']}, "
              f"Breakout_parent: {main_trendline['receiver']['Breakout_parent']}, "
              f"order_parent: {main_trendline['receiver']['order_parent']}, "
              f"actual_orderparent: {main_trendline['receiver']['actual_orderparent']}, "
              f"reassigned_orderparent: {main_trendline['receiver']['reassigned_orderparent']}")
    
    # Save the trendline image
    main_trendline_image_path = os.path.join(OUTPUT_FOLDER, f"{base_name}_parent_main_trendlines.png")
    cv2.imwrite(main_trendline_image_path, img_main_trendlines)
    print(f"Parent trendlines image saved to: {main_trendline_image_path}")
    
    # Save contracts data to JSON
    save_contracts_data_to_json(contracts_data)
    
    return main_trendline_image_path, main_trendline_data

def save_contracts_data_to_json(contracts_data):
    """
    Save the contracts data to a JSON file named contracts.json in the OUTPUT_FOLDER.
    Additionally, extract entries with order_status="pending order" and valid (non-"invalid") 
    Breakout_parent and order_parent, and save to pendingorder.json.
    
    Args:
        contracts_data (list): List of dictionaries containing contract details.
    
    Returns:
        str: Path to the saved contracts.json file.
    """
    # Extract market and timeframe from OUTPUT_FOLDER
    normalized_tf = normalize_timeframe(os.path.basename(OUTPUT_FOLDER))  # Get timeframe from OUTPUT_FOLDER
    market_name = os.path.basename(os.path.dirname(OUTPUT_FOLDER))  # Get market from parent directory
    output_folder = os.path.join(BASE_OUTPUT_FOLDER, market_name, normalized_tf)
    os.makedirs(output_folder, exist_ok=True)  # Ensure folder exists

    # Save all contracts data
    json_path = os.path.join(output_folder, "contracts.json")
    try:
        with open(json_path, 'w') as f:
            json.dump(contracts_data, f, indent=4)
        print(f"Contracts data saved to: {json_path}")
    except Exception as e:
        print(f"Error saving contracts data to JSON: {e}")
    
    # Save pending orders with valid Breakout_parent and order_parent
    pending_orders = [
        entry for entry in contracts_data
        if (entry['receiver']['order_status'] == "pending order" and
            entry['receiver']['Breakout_parent'] != "invalid" and
            entry['receiver']['order_parent'] != "invalid")
    ]
    pending_json_path = os.path.join(output_folder, "pendingorder.json")
    try:
        with open(pending_json_path, 'w') as f:
            json.dump(pending_orders, f, indent=4)
        print(f"Pending orders with valid Breakout_parent and order_parent saved to: {pending_json_path}")
    except Exception as e:
        print(f"Error saving pending orders to JSON: {e}")
    
    return json_path

def latestmain_trendline(switch):
    """
    Control whether to show or hide the last main_trendline when there is no parent ahead of the receiver.
    
    Args:
        switch (str): "show" to allow the last main_trendline, "hide" to restrict it.
    
    Returns:
        bool: True if the last main_trendline should be shown, False otherwise.
    """
    valid_options = ["show", "hide"]
    if switch.lower() not in valid_options:
        print(f"Invalid switch value '{switch}', defaulting to 'hide'")
        return False
    return switch.lower() == "show"

def main_trendlinecontracts(number):
    """
    Validate the number of contract points (CPs) and their associated main_trendlines to display.
    Starts from the rightmost (latest) main_trendline and selects sequentially leftward.
    
    Args:
        number (str): Number of contract points to display (e.g., "1", "2"). If "0", display none.
                     If greater than available main_trendlines, display all.
    
    Returns:
        int: Validated number of contract points to display, defaults to 0 if invalid.
    """
    try:
        num_contracts = int(number)
        if num_contracts < 0:
            print(f"Number of contracts {num_contracts} is negative, defaulting to 0")
            num_contracts = 0
        return num_contracts
    except ValueError:
        print(f"Invalid number of contracts {number}, defaulting to 0")
        return 0

def PHandPLmain_trendlinedistancecontrol(distance):
    """
    Validate the distance threshold for main_trendlines between sender (PH/PL) and the target candlestick.
    
    Args:
        distance (str): The minimum vertical distance in pixels (e.g., "0", "10", "20", "50", "100", "200").
    
    Returns:
        int: Validated distance threshold, defaults to 0 if invalid or not in allowed values.
    """
    allowed_distances = [0, 10, 20, 50, 100, 200]
    try:
        dist = int(distance)
        if dist not in allowed_distances:
            print(f"Distance {dist} is not in allowed values {allowed_distances}, defaulting to 0")
            dist = 0
    except ValueError:
        print(f"Invalid distance {distance}, defaulting to 0")
        dist = 0
    
    return dist

def main_trendlinetocandleposition(position):
    """
    Validate the candlestick position to connect main_trendlines to (1 to 5).
    
    Args:
        position (str): The position of the candlestick to the right (1 to 5).
    
    Returns:
        str: Validated position as a string (1 to 5), defaults to "1" if invalid or out of range.
    """
    try:
        pos = int(position)
        if pos > 5 or pos < 1:
            print(f"Position {pos} is out of range (1-5), defaulting to 1")
            pos = 1
    except ValueError:
        print(f"Invalid position {position}, defaulting to 1")
        pos = 1
    
    return str(pos)

def process_market_timeframe(market, timeframe):
    """Process a single market and timeframe combination."""
    try:
        # Normalize timeframe for folder paths
        normalized_tf = normalize_timeframe(timeframe)
        # Dynamically construct input and output folders
        market_folder_name = market.replace(" ", "_")
        input_folder = os.path.join(BASE_INPUT_FOLDER, market_folder_name, normalized_tf)
        output_folder = os.path.join(BASE_OUTPUT_FOLDER, market_folder_name, normalized_tf)
        
        # Set global OUTPUT_FOLDER for use in draw_parent_main_trendlines and JSON saving functions
        global OUTPUT_FOLDER
        OUTPUT_FOLDER = output_folder
        
        # Create output folder
        os.makedirs(output_folder, exist_ok=True)
        
        print(f"Processing market: {market}, timeframe: {timeframe}")
        
        # MECHANISM CONTROLS
        left_required, right_required = controlleftandrighthighsandlows("1", "1")
        main_trendline_position = main_trendlinetocandleposition("1")
        distance_threshold = PHandPLmain_trendlinedistancecontrol("10")
        num_contracts = main_trendlinecontracts("100")
        allow_latest_main_trendline = latestmain_trendline("show")
        
        # Load starting number from candlesamountinbetween.json
        start_number = load_candlesamountinbetween(market, timeframe)
        
        # Load latest chart
        img, base_name = load_latest_chart(input_folder, market, timeframe)
        if img is None:
            print(f"Skipping market {market} timeframe {timeframe} due to no chart found")
            return False
        
        # Get image dimensions
        height, width = img.shape[:2]
        
        # Crop image
        img = crop_image(img, height, width)
        
        # Enhance colors
        img_enhanced, mask_red, mask_green, mask = enhance_colors(img)
        
        # Replace near-black wicks
        img_enhanced = replace_near_black_wicks(img_enhanced, mask_red, mask_green)
        
        # Sharpen image
        img_enhanced = sharpen_image(img_enhanced)
        
        # Set background to black
        img_enhanced = set_background_black(img_enhanced, mask)
        
        # Save enhanced image
        save_enhanced_image(img_enhanced, base_name, output_folder)
        
        # Remove horizontal lines
        img_enhanced, mask_red, mask_green, mask = remove_horizontal_lines(img_enhanced, mask_red, mask_green, width)
        
        # Detect candlestick contours and collect arrow data with start_number
        img_contours, red_positions, green_positions, arrow_data = detect_candlestick_contours(img_enhanced, mask_red, mask_green, start_number)
        
        # Save arrow data to JSON
        save_arrow_data_to_json(arrow_data, output_folder)
        
        # Save contour image
        save_contour_image(img_contours, base_name, output_folder)
        
        # Connect contours
        img_connected_contours, all_positions = connect_contours(img_contours, red_positions, green_positions)
        
        # Save connected contour image
        save_connected_contour_image(img_connected_contours, base_name, output_folder)
        
        # Identify parent highs and lows
        parent_labeled_image_path, pl_labels, ph_labels = identify_parent_highs_and_lows(
            img_enhanced, all_positions, base_name, left_required, right_required, arrow_data, output_folder
        )
        
        # Draw main trendlines and collect main trendline data
        main_trendline_image_path, main_trendline_data = draw_parent_main_trendlines(
            img_enhanced,
            all_positions,
            base_name,
            left_required,
            right_required,
            main_trendline_position=main_trendline_position,
            distance_threshold=distance_threshold,
            num_contracts=num_contracts,
            allow_latest_main_trendline=allow_latest_main_trendline,
            pl_labels=pl_labels,
            ph_labels=ph_labels
        )
        
        print(f"Completed processing market: {market}, timeframe: {timeframe}")
        return True
    
    except Exception as e:
        print(f"Error processing market {market} timeframe {timeframe}: {e}")
        return False

def main1():
    def check_status_json(market, timeframe):
        """Check if the status.json file for a market and timeframe has 'chart identified' or 'chart_identified' status.
        Create a default status.json if it doesn't exist."""
        try:
            normalized_tf = normalize_timeframe(timeframe)  # Normalize timeframe for folder path
            market_folder_name = market.replace(" ", "_")
            status_file = os.path.join(BASE_INPUT_FOLDER, market_folder_name, normalized_tf, "status.json")
            # Create the directory if it doesn't exist
            os.makedirs(os.path.dirname(status_file), exist_ok=True)
            
            # Check if status file exists, if not create it with default values
            if not os.path.exists(status_file):
                print(f"Status file not found for {market} timeframe {timeframe}: {status_file}. Creating default status.json")
                default_status = {
                    "market": market,
                    "timeframe": timeframe,
                    "normalized_timeframe": normalized_tf,  # Include normalized timeframe
                    "timestamp": "",
                    "status": "order_free",
                    "elligible_status": "order_free"
                }
                with open(status_file, 'w') as f:
                    json.dump(default_status, f, indent=4)
            
            # Read the status file
            with open(status_file, 'r') as f:
                status_data = json.load(f)
            status = status_data.get("status", "")
            if status in ["chart identified", "chart_identified"]:
                print(f"Status '{status}' found for {market} timeframe {timeframe}")
                return True
            else:
                print(f"Status '{status}' for {market} timeframe {timeframe}, skipping processing")
                return False
        except Exception as e:
            print(f"Error reading or creating status file for {market} timeframe {timeframe}: {e}")
            return False

    def chart_identified_main():
        """Main function to process markets and timeframes with 'chart identified' or 'chart_identified' status."""
        try:
            # Load markets, timeframes, and credentials
            global MARKETS, TIMEFRAMES
            MARKETS, TIMEFRAMES = load_markets_and_timeframes(MARKETS_JSON_PATH)
            
            # Check M5 candle time left globally (using a default market, e.g., first in MARKETS or a specific one)
            if not MARKETS or not TIMEFRAMES:
                print("No markets defined in MARKETS list. Exiting.")
                return
            default_market = MARKETS[0]  # Use the first market for candle time check
            timeframe = "M5"  # Fixed to M5 as per candletimeleft logic
            print(f"Checking M5 candle time left using market: {default_market}")
            time_left, next_close_time = candletimeleft(default_market, timeframe, None, min_time_left=3)
            
            if time_left is None or next_close_time is None:
                print(f"Failed to retrieve candle time for {default_market} (M5). Exiting.")
                return
            
            print(f"M5 candle time left: {time_left:.2f} minutes. Proceeding with execution.")

            # Clear the base output folder before processing
            #clear_image_and_json_files() # Uncomment if you want to clear the output folder
            
            # Process markets with 'chart identified' or 'chart_identified' status
            print(f"Markets to check: {MARKETS}")
            
            # Create a list of market and timeframe combinations with valid status
            tasks = []
            for market in MARKETS:
                for timeframe in TIMEFRAMES:
                    if check_status_json(market, timeframe):
                        tasks.append((market, timeframe))
            
            if not tasks:
                print("No market-timeframe combinations with 'chart identified' or 'chart_identified' status found. Exiting.")
                return
            
            print(f"Processing {len(tasks)} market-timeframe combinations with 'chart identified' or 'chart_identified' status")
            
            # Use multiprocessing to process valid market-timeframe combinations in parallel
            with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
                results = pool.starmap(process_market_timeframe, tasks)
            
            # Print summary of processing
            success_count = sum(1 for result in results if result)
            print(f"Processing completed: {success_count}/{len(tasks)} market-timeframe combinations processed successfully")
            
        except Exception as e:
            print(f"Error in main processing: {e}")
    chart_identified_main()

def main2():
    def check_status_json(market, timeframe):
        """Check if the status.json file for a market and timeframe has 'order_free' status.
        Create a default status.json if it doesn't exist."""
        try:
            normalized_tf = normalize_timeframe(timeframe)  # Normalize timeframe for folder path
            market_folder_name = market.replace(" ", "_")
            status_file = os.path.join(BASE_INPUT_FOLDER, market_folder_name, normalized_tf, "status.json")
            # Create the directory if it doesn't exist
            os.makedirs(os.path.dirname(status_file), exist_ok=True)
            
            # Check if status file exists, if not create it with default values
            if not os.path.exists(status_file):
                print(f"Status file not found for {market} timeframe {timeframe}: {status_file}. Creating default status.json")
                default_status = {
                    "market": market,
                    "timeframe": timeframe,
                    "normalized_timeframe": normalized_tf,  # Include normalized timeframe
                    "timestamp": "",
                    "status": "order_free",
                    "elligible_status": "order_free"
                }
                with open(status_file, 'w') as f:
                    json.dump(default_status, f, indent=4)
            
            # Read the status file
            with open(status_file, 'r') as f:
                status_data = json.load(f)
            status = status_data.get("status", "")
            if status in ["order_free"]:
                print(f"Status '{status}' found for {market} timeframe {timeframe}")
                return True
            else:
                print(f"Status '{status}' for {market} timeframe {timeframe}, skipping processing")
                return False
        except Exception as e:
            print(f"Error reading or creating status file for {market} timeframe {timeframe}: {e}")
            return False

    def chart_identified_main():
        """Main function to process markets and timeframes with 'order_free' status."""
        try:
            # Load markets, timeframes, and credentials
            global MARKETS, TIMEFRAMES
            MARKETS, TIMEFRAMES = load_markets_and_timeframes(MARKETS_JSON_PATH)
            
            # Check M5 candle time left globally (using a default market, e.g., first in MARKETS or a specific one)
            if not MARKETS or not TIMEFRAMES:
                print("No markets defined in MARKETS list. Exiting.")
                return
            default_market = MARKETS[0]  # Use the first market for candle time check
            timeframe = "M5"  # Fixed to M5 as per candletimeleft logic
            print(f"Checking M5 candle time left using market: {default_market}")
            time_left, next_close_time = candletimeleft(default_market, timeframe, None, min_time_left=3)
            
            if time_left is None or next_close_time is None:
                print(f"Failed to retrieve candle time for {default_market} (M5). Exiting.")
                return
            
            print(f"M5 candle time left: {time_left:.2f} minutes. Proceeding with execution.")

            # Clear the base output folder before processing
            #clear_image_and_json_files() # Uncomment if you want to clear the output folder
            
            # Process markets with 'order_free' status
            print(f"Markets to check: {MARKETS}")
            
            # Create a list of market and timeframe combinations with valid status
            tasks = []
            for market in MARKETS:
                for timeframe in TIMEFRAMES:
                    if check_status_json(market, timeframe):
                        tasks.append((market, timeframe))
            
            if not tasks:
                print("No market-timeframe combinations with 'order_free' status found. Exiting.")
                return
            
            print(f"Processing {len(tasks)} market-timeframe combinations with 'order_free' status")
            
            # Use multiprocessing to process valid market-timeframe combinations in parallel
            with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
                results = pool.starmap(process_market_timeframe, tasks)
            
            # Print summary of processing
            success_count = sum(1 for result in results if result)
            print(f"Processing completed: {success_count}/{len(tasks)} market-timeframe combinations processed successfully")
            
        except Exception as e:
            print(f"Error in main processing: {e}")
    chart_identified_main()

if __name__ == "__main__":
    main2()
    main1()

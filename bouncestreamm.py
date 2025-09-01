import os
import cv2
import numpy as np
import shutil
import json

# Path configuration
BASE_INPUT_FOLDER = r"C:\xampp\htdocs\CIPHER\cipher i\bouncestream\chart\fetched"
BASE_OUTPUT_FOLDER = r"C:\xampp\htdocs\CIPHER\cipher i\bouncestream\chart\processing\main"
MARKET_NAME = "Drift Switch Index 30"

# Dynamically construct INPUT_FOLDER and OUTPUT_FOLDER with market name
MARKET_FOLDER_NAME = MARKET_NAME.replace(" ", "_")
INPUT_FOLDER = os.path.join(BASE_INPUT_FOLDER, MARKET_FOLDER_NAME)
OUTPUT_FOLDER = os.path.join(BASE_OUTPUT_FOLDER, MARKET_FOLDER_NAME)

def clear_output_folder():
    """Clear all files and subfolders in the BASE_OUTPUT_FOLDER."""
    try:
        if os.path.exists(BASE_OUTPUT_FOLDER):
            # Iterate through all items in the output folder
            for item in os.listdir(BASE_OUTPUT_FOLDER):
                item_path = os.path.join(BASE_OUTPUT_FOLDER, item)
                try:
                    if os.path.isfile(item_path):
                        os.remove(item_path)
                        print(f"Deleted file: {item_path}")
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                        print(f"Deleted folder: {item_path}")
                except Exception as e:
                    print(f"Error deleting {item_path}: {e}")
        else:
            print(f"Output folder does not exist: {BASE_OUTPUT_FOLDER}")
    except Exception as e:
        print(f"Error clearing output folder {BASE_OUTPUT_FOLDER}: {e}")

def load_latest_chart():
    """Find and load the latest chart image containing MARKET_NAME in the market-specific folder."""
    # Ensure the input folder exists
    if not os.path.exists(INPUT_FOLDER):
        print(f"Input folder does not exist: {INPUT_FOLDER}")
        return None, None
    
    # Search for files containing the market name (case-insensitive)
    files = [
        os.path.join(INPUT_FOLDER, f) for f in os.listdir(INPUT_FOLDER)
        if os.path.isfile(os.path.join(INPUT_FOLDER, f)) and 
           MARKET_NAME.replace(" ", "").lower() in f.replace(" ", "").lower()
    ]
    
    if not files:
        print(f"No files found containing '{MARKET_NAME}' in {INPUT_FOLDER}")
        return None, None
    
    # Find the latest file based on modification time
    chart_path = max(files, key=os.path.getmtime)
    print(f"Latest chart file found: {chart_path}")
    
    # Load the image
    img = cv2.imread(chart_path)
    if img is None:
        raise ValueError(f"Failed to load image: {chart_path}")
    
    # Get the base name of the file (without extension)
    base_name = os.path.splitext(os.path.basename(chart_path))[0]
    return img, base_name

def crop_image(img, height, width):
    """Crop the image: 200px from left, 30px from bottom, 150px from right."""
    if height < 20 or width < 350:  # 350 = 200 (left) + 150 (right)
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
    s[mask > 0] = np.clip(s[mask > 0] * 2.0, 0, 255)  # Boost saturation by 2x
    v[mask > 0] = np.clip(v[mask > 0] * 1.5, 0, 255)  # Boost brightness by 1.5x
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

def save_enhanced_image(img_enhanced, base_name):
    """Save the enhanced image."""
    debug_image_path = os.path.join(OUTPUT_FOLDER, f"{base_name}_enhanced.png")
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

def detect_candlestick_contours(img_enhanced, mask_red, mask_green):
    """Detect and draw contours for red and green candlesticks, draw one white arrow per unique candlestick position pointing downward to the top with a vertical line to the image top, and collect arrow data for JSON output."""
    img_contours = img_enhanced.copy()
    height, width = img_contours.shape[:2]  # Get image dimensions
    contours_red, _ = cv2.findContours(mask_red, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    red_count = 0
    red_positions = []
    all_candlestick_positions = []

    # Process red candlesticks
    for contour in contours_red:
        if cv2.contourArea(contour) >= 0.01:
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

    # Process green candlesticks
    for contour in contours_green:
        if cv2.contourArea(contour) >= 0.01:
            green_count += 1
            x, y, w, h = cv2.boundingRect(contour)
            center_x = x + w // 2
            top_y = y
            bottom_y = y + h
            green_positions.append((center_x, top_y, bottom_y))
            all_candlestick_positions.append((center_x, top_y, bottom_y, 'green', contour))

    # Filter out duplicate positions (same center_x)
    unique_positions = []
    seen_x = {}
    for pos in sorted(all_candlestick_positions, key=lambda x: x[0]):  # Sort by x-coordinate
        center_x, top_y, bottom_y, color, contour = pos
        if center_x not in seen_x:
            seen_x[center_x] = pos
            unique_positions.append(pos)
        else:
            # If duplicate x-coordinate, keep the contour with larger area
            existing_pos = seen_x[center_x]
            existing_contour = existing_pos[4]
            if cv2.contourArea(contour) > cv2.contourArea(existing_contour):
                # Replace with the new position if it has a larger contour area
                unique_positions[unique_positions.index(existing_pos)] = pos
                seen_x[center_x] = pos

    # Draw contours and arrows for unique positions
    for center_x, top_y, bottom_y, color, contour in unique_positions:
        # Draw contour based on color
        contour_color = (255, 0, 0) if color == 'red' else (255, 255, 255)
        cv2.drawContours(img_contours, [contour], -1, contour_color, 1)

        # Draw white arrow pointing downward to the top of the candlestick
        arrow_start = (center_x, max(0, top_y - 30))  # Start 30 pixels above the top
        arrow_end = (center_x, top_y)  # End at the top of the candlestick
        cv2.arrowedLine(img_contours, arrow_start, arrow_end, (255, 255, 255), 1, tipLength=0.3)

        # Draw vertical line from arrowhead to the top of the image
        cv2.line(img_contours, arrow_end, (center_x, 0), (255, 255, 255), 1)

    # Create arrow data for JSON, numbering from right to left, excluding the rightmost candlestick
    arrow_data = []
    arrow_count = 0
    for i, (center_x, top_y, bottom_y, color, _) in enumerate(reversed(unique_positions[:-1]), 1):  # Exclude rightmost
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
    print(f"Total arrows: {arrow_count}")

    return img_contours, red_positions, green_positions, arrow_data

def save_arrow_data_to_json(arrow_data):
    """
    Save the arrow data to a JSON file named after the market in the OUTPUT_FOLDER.
    
    Args:
        arrow_data (list): List of dictionaries containing arrow details.
    
    Returns:
        str: Path to the saved JSON file.
    """
    json_path = os.path.join(OUTPUT_FOLDER, f"{MARKET_FOLDER_NAME}_arrows.json")
    try:
        with open(json_path, 'w') as f:
            json.dump(arrow_data, f, indent=4)
        print(f"Arrow data saved to: {json_path}")
    except Exception as e:
        print(f"Error saving arrow data to JSON: {e}")
    return json_path

def save_contour_image(img_contours, base_name):
    """Save the contour image."""
    contour_image_path = os.path.join(OUTPUT_FOLDER, f"{base_name}_contours.png")
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

def save_connected_contour_image(img_connected_contours, base_name):
    """Save the connected contour image."""
    connected_contour_image_path = os.path.join(OUTPUT_FOLDER, f"{base_name}_connected_contours.png")
    cv2.imwrite(connected_contour_image_path, img_connected_contours)
    print(f"Connected contour image saved to: {connected_contour_image_path}")
    return connected_contour_image_path


def identify_parent_highs_and_lows(img_enhanced, all_positions, base_name, left_required, right_required, arrow_data):
    """
    Identify and label Parent Highs (PH) and Parent Lows (PL) on the enhanced image using arrow numbers.
    A Parent Low (PL) has the lowest bottom y-coordinate compared to at least 'left_required' low(s) to its left
    and at least 'right_required' low(s) to its right, where all these lows have higher bottoms than the candidate.
    A Parent High (PH) has the highest top y-coordinate compared to at least 'left_required' high(s) to its left
    and at least 'right_required' high(s) to its right, where all these highs have lower tops than the candidate.
    
    Args:
        img_enhanced (numpy.ndarray): Enhanced image without any numbering.
        all_positions (list): List of tuples containing (position, color) for each candlestick,
                             where position is (x, top_y, bottom_y).
        base_name (str): Base name of the input file for saving the output.
        left_required (int): Number of highs/lows required to the left.
        right_required (int): Number of highs/lows required to the right.
        arrow_data (list): List of dictionaries containing arrow details (arrow_number, x).
    
    Returns:
        str: Path to the saved image with PL and PH labels.
    """
    # Create a copy of the input image to draw labels on
    img_parent_labeled = img_enhanced.copy()
    
    # Initialize lists to store low and high points
    low_points = []
    high_points = []
    total_candles = len(all_positions)
    
    # Create a mapping of x-coordinate to arrow number
    arrow_map = {item['x']: item['arrow_number'] for item in arrow_data}
    
    # Identify low and high points
    for i, (pos, color) in enumerate(reversed(all_positions[:-1]), 1):  # Exclude rightmost candlestick
        x, top_y, bottom_y = pos
        orig_index = total_candles - 1 - i  # Original index in all_positions (left-to-right)
        
        is_low = False
        is_high = False
        
        if orig_index > 0 and orig_index < total_candles - 1:
            prev_bottom_y = all_positions[orig_index + 1][0][2]  # Next candlestick (left)
            next_bottom_y = all_positions[orig_index - 1][0][2]  # Previous candlestick (right)
            if bottom_y > prev_bottom_y and bottom_y > next_bottom_y:
                is_low = True
            prev_top_y = all_positions[orig_index + 1][0][1]
            next_top_y = all_positions[orig_index - 1][0][1]
            if top_y < prev_top_y and top_y < next_top_y:
                is_high = True
        elif orig_index == 0 and total_candles > 2:  # Second candlestick from right
            next_bottom_y = all_positions[orig_index + 1][0][2]
            next_top_y = all_positions[orig_index + 1][0][1]
            if bottom_y > next_bottom_y:
                is_low = True
            if top_y < next_top_y:
                is_high = True
        elif orig_index == total_candles - 2 and total_candles > 2:  # Leftmost numbered candlestick
            prev_bottom_y = all_positions[orig_index - 1][0][2]
            prev_top_y = all_positions[orig_index - 1][0][1]
            if bottom_y > prev_bottom_y:
                is_low = True
            if top_y < prev_top_y:
                is_high = True
        
        if is_low:
            low_points.append((orig_index, x, bottom_y, i))
        if is_high:
            high_points.append((orig_index, x, top_y, i))
    
    # Sort points by x-coordinate (left to right)
    low_points.sort(key=lambda x: x[1])
    high_points.sort(key=lambda x: x[1])
    
    # Identify Parent Lows (PL)
    pl_count = 0
    pl_labels = []
    for i, (orig_index, x, bottom_y, number) in enumerate(low_points):
        left_count = 0
        right_count = 0
        is_lowest = True
        
        # Check left lows
        for j in range(i - 1, -1, -1):
            if low_points[j][2] < bottom_y:  # Left low is higher
                left_count += 1
                if left_count >= left_required:
                    break
            else:
                is_lowest = False
        
        # Check right lows
        for j in range(i + 1, len(low_points)):
            if low_points[j][2] < bottom_y:  # Right low is higher
                right_count += 1
                if right_count >= right_required:
                    break
            else:
                is_lowest = False
        
        # Label as PL if it has the lowest bottom among the checked points
        if left_count >= left_required and right_count >= right_required and is_lowest:
            pl_count += 1
            arrow_number = arrow_map.get(x, None)
            if arrow_number is not None:
                label = f"PL{arrow_number}"
                text_position = (x - 20, bottom_y + 20)  # Below the candlestick
                cv2.putText(img_parent_labeled, label, text_position, cv2.FONT_HERSHEY_SIMPLEX,
                            0.5, (255, 255, 255), 1, cv2.LINE_AA)
                pl_labels.append((x, bottom_y, label, arrow_number))
    
    # Identify Parent Highs (PH)
    ph_count = 0
    ph_labels = []
    for i, (orig_index, x, top_y, number) in enumerate(high_points):
        left_count = 0
        right_count = 0
        is_highest = True
        
        # Check left highs
        for j in range(i - 1, -1, -1):
            if high_points[j][2] > top_y:  # Left high is lower
                left_count += 1
                if left_count >= left_required:
                    break
            else:
                is_highest = False
        
        # Check right highs
        for j in range(i + 1, len(high_points)):
            if high_points[j][2] > top_y:  # Right high is lower
                right_count += 1
                if right_count >= right_required:
                    break
            else:
                is_highest = False
        
        # Label as PH if it has the highest top among the checked points
        if left_count >= left_required and right_count >= right_required and is_highest:
            ph_count += 1
            arrow_number = arrow_map.get(x, None)
            if arrow_number is not None:
                label = f"PH{arrow_number}"
                text_position = (x - 20, top_y - 10)  # Above the candlestick
                cv2.putText(img_parent_labeled, label, text_position, cv2.FONT_HERSHEY_SIMPLEX,
                            0.5, (255, 255, 255), 1, cv2.LINE_AA)
                ph_labels.append((x, top_y, label, arrow_number))
    
    # Print the number of PL and PH identified
    print(f"Identified {pl_count} Parent Lows (PL) with {left_required} left and {right_required} right lows required")
    print(f"Identified {ph_count} Parent Highs (PH) with {left_required} left and {right_required} right highs required")
    
    # Save the labeled image
    parent_labeled_image_path = os.path.join(OUTPUT_FOLDER, f"{base_name}_parent_highs_lows.png")
    cv2.imwrite(parent_labeled_image_path, img_parent_labeled)
    print(f"Parent highs and lows labeled image saved to: {parent_labeled_image_path}")
    
    return parent_labeled_image_path, pl_labels, ph_labels

def controlleftandrighthighsandlows(left, right):
    """
    Control the number of highs/lows required to the left and right for PH/PL identification.
    
    Args:
        left (str): Number of highs/lows required to the left (as a string, e.g., "1").
        right (str): Number of highs/lows required to the right (as a string, e.g., "1").
    
    Returns:
        tuple: (left_count_required, right_count_required) as integers.
    """
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
    
    # Get image width for extending trendlines to the right edge
    img_width = img_main_trendlines.shape[1]
    
    # Font settings for position numbers
    font = cv2.FONT_HERSHEY_SIMPLEX
    font_scale = 0.5
    text_color = (255, 255, 255)  # White text
    thickness = 1
    line_type = cv2.LINE_AA
    
    # Draw position numbers for PL and PH
    for x, bottom_y, label, arrow_number in pl_labels:
        text_position = (x - 20, bottom_y + 35)  # 20px below PL label
        cv2.putText(img_main_trendlines, str(arrow_number), text_position, font,
                    font_scale, text_color, thickness, line_type)
    
    for x, top_y, label, arrow_number in ph_labels:
        text_position = (x - 20, top_y - 25)  # 10px above PH label
        cv2.putText(img_main_trendlines, str(arrow_number), text_position, font,
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
    
    # Helper function to find Breakout Parent (PHBO or PLBO) and Order Parent (PLOP or PHOP)
    def find_breakout_and_order_parent(receiver_x, receiver_y, trendline_type, all_parents, pl_labels, ph_labels):
        breakout_label = None
        order_parent_label = None
        order_parent_y = None
        order_parent_x = None
        breakout_x = None
        breakout_y = None
        
        if trendline_type == "PH-to-PH":
            # Find PHBO: First PH to the right with top_y < receiver_y (higher on image)
            for x, parent_type, y, label, _ in sorted(all_parents, key=lambda p: p[0]):
                if x > receiver_x and parent_type == "PH" and y < receiver_y:
                    breakout_label = label
                    breakout_x = x
                    breakout_y = y
                    break
            if breakout_label:
                # Find PLOP: Parent immediately before PHBO must be PL
                sorted_parents = sorted(all_parents, key=lambda p: p[0])
                for i, (x, parent_type, y, label, _) in enumerate(sorted_parents):
                    if label == breakout_label and i > 0:
                        prev_x, prev_type, prev_y, prev_label, _ = sorted_parents[i - 1]
                        if prev_type == "PL":
                            order_parent_label = prev_label
                            order_parent_y = prev_y
                            order_parent_x = prev_x
                        break
        else:  # PL-to-PL
            # Find PLBOSd: First PL to the right with bottom_y > receiver_y (lower on image)
            for x, parent_type, y, label, _ in sorted(all_parents, key=lambda p: p[0]):
                if x > receiver_x and parent_type == "PL" and y > receiver_y:
                    breakout_label = label
                    breakout_x = x
                    breakout_y = y
                    break
            if breakout_label:
                # Find PHOP: Parent immediately before PLBO must be PH
                sorted_parents = sorted(all_parents, key=lambda p: p[0])
                for i, (x, parent_type, y, label, _) in enumerate(sorted_parents):
                    if label == breakout_label and i > 0:
                        prev_x, prev_type, prev_y, prev_label, _ = sorted_parents[i - 1]
                        if prev_type == "PH":
                            order_parent_label = prev_label
                            order_parent_y = prev_y
                            order_parent_x = prev_x
                        break
        
        return breakout_label, order_parent_label, order_parent_y, order_parent_x, breakout_x, breakout_y
    
    # Helper function to get y-coordinates for PLOP/PHOP and PHBO/PLBO
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
    def find_crossing_candle(breakout_x, order_y, all_positions, trendline_type):
        for pos, _ in sorted(all_positions, key=lambda x: x[0][0]):
            x, top_y, bottom_y = pos
            if x > breakout_x:
                # Check if the candlestick body crosses the order_y level
                if trendline_type == "PH-to-PH":
                    # For PH-to-PH, order_y is top of PLOP
                    if top_y <= order_y <= bottom_y:
                        return x
                else:  # PL-to-PL
                    # For PL-to-PL, order_y is bottom of PHOP
                    if top_y <= order_y <= bottom_y:
                        return x
        return None
    
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
            breakout_label, order_parent_label, order_parent_y, order_parent_x, breakout_x, breakout_y = find_breakout_and_order_parent(
                x_ras, top_y_ras, "PH-to-PH", all_parents, pl_labels, ph_labels
            )
            # Determine order_status
            order_status = "pending order"
            if breakout_label == "invalid" or order_parent_label == "invalid":
                order_status = "invalid"
            elif breakout_label and order_parent_label:
                crossing_x = find_crossing_candle(breakout_x, order_parent_y, all_positions, "PH-to-PH")
                if crossing_x is not None:
                    order_status = "executed"
            
            sender_pos_number = get_position_number_from_label(label_ms)
            sender_arrow_number = arrow_number_ms
            receiver_pos_number = get_position_number(x_ras, pl_labels + ph_labels, all_positions)
            
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
                        "order_type": "short",
                        "order_status": order_status,
                        "Breakout_parent": breakout_label if breakout_label and breakout_label.startswith("PH") else "invalid",
                        "order_parent": order_parent_label if order_parent_label and order_parent_label.startswith("PL") else "invalid",
                        "receiver_contractcandle_arrownumber": arrow_number_ras
                    }
                }
                main_trendline_data.append(main_trendline_entry)
                contracts_data.append(main_trendline_entry)  # Only append main_trendline_entry
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
                    breakout_label, order_parent_label, order_parent_y, order_parent_x, breakout_x, breakout_y = find_breakout_and_order_parent(
                        x_rasr, top_y_rasr, "PH-to-PH", all_parents, pl_labels, ph_labels
                    )
                    # Determine order_status
                    order_status = "pending order"
                    if breakout_label == "invalid" or order_parent_label == "invalid":
                        order_status = "invalid"
                    elif breakout_label and order_parent_label:
                        crossing_x = find_crossing_candle(breakout_x, order_parent_y, all_positions, "PH-to-PH")
                        if crossing_x is not None:
                            order_status = "executed"
                    
                    sender_pos_number = get_position_number_from_label(label_ras)
                    sender_arrow_number = arrow_number_ras
                    receiver_pos_number = get_position_number(x_rasr, pl_labels + ph_labels, all_positions)
                    
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
                                "order_type": "short",
                                "order_status": order_status,
                                "Breakout_parent": breakout_label if breakout_label and breakout_label.startswith("PH") else "invalid",
                                "order_parent": order_parent_label if order_parent_label and order_parent_label.startswith("PL") else "invalid",
                                "receiver_contractcandle_arrownumber": arrow_number_rasr
                            }
                        }
                        main_trendline_data.append(main_trendline_entry)
                        contracts_data.append(main_trendline_entry)  # Only append main_trendline_entry
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
                breakout_label, order_parent_label, order_parent_y, order_parent_x, breakout_x, breakout_y = find_breakout_and_order_parent(
                    x_rasr, top_y_rasr, "PH-to-PH", all_parents, pl_labels, ph_labels
                )
                # Determine order_status
                order_status = "pending order"
                if breakout_label == "invalid" or order_parent_label == "invalid":
                    order_status = "invalid"
                elif breakout_label and order_parent_label:
                    crossing_x = find_crossing_candle(breakout_x, order_parent_y, all_positions, "PH-to-PH")
                    if crossing_x is not None:
                        order_status = "executed"
                
                sender_pos_number = get_position_number_from_label(label_ms)
                sender_arrow_number = arrow_number_ms
                receiver_pos_number = get_position_number(x_rasr, pl_labels + ph_labels, all_positions)
                
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
                            "order_type": "short",
                            "order_status": order_status,
                            "Breakout_parent": breakout_label if breakout_label and breakout_label.startswith("PH") else "invalid",
                            "order_parent": order_parent_label if order_parent_label and order_parent_label.startswith("PL") else "invalid",
                            "receiver_contractcandle_arrownumber": arrow_number_rasr
                        }
                    }
                    main_trendline_data.append(main_trendline_entry)
                    contracts_data.append(main_trendline_entry)  # Only append main_trendline_entry
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
            breakout_label, order_parent_label, order_parent_y, order_parent_x, breakout_x, breakout_y = find_breakout_and_order_parent(
                x_ras, bottom_y_ras, "PL-to-PL", all_parents, pl_labels, ph_labels
            )
            # Determine order_status
            order_status = "pending order"
            if breakout_label == "invalid" or order_parent_label == "invalid":
                order_status = "invalid"
            elif breakout_label and order_parent_label:
                crossing_x = find_crossing_candle(breakout_x, order_parent_y, all_positions, "PL-to-PL")
                if crossing_x is not None:
                    order_status = "executed"
            
            sender_pos_number = get_position_number_from_label(label_ms)
            sender_arrow_number = arrow_number_ms
            receiver_pos_number = get_position_number(x_ras, pl_labels + ph_labels, all_positions)
            
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
                        "order_type": "long",
                        "order_status": order_status,
                        "Breakout_parent": breakout_label if breakout_label and breakout_label.startswith("PL") else "invalid",
                        "order_parent": order_parent_label if order_parent_label and order_parent_label.startswith("PH") else "invalid",
                        "receiver_contractcandle_arrownumber": arrow_number_ras
                    }
                }
                main_trendline_data.append(main_trendline_entry)
                contracts_data.append(main_trendline_entry)  # Only append main_trendline_entry
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
                    breakout_label, order_parent_label, order_parent_y, order_parent_x, breakout_x, breakout_y = find_breakout_and_order_parent(
                        x_rasr, bottom_y_rasr, "PL-to-PL", all_parents, pl_labels, ph_labels
                    )
                    # Determine order_status
                    order_status = "pending order"
                    if breakout_label == "invalid" or order_parent_label == "invalid":
                        order_status = "invalid"
                    elif breakout_label and order_parent_label:
                        crossing_x = find_crossing_candle(breakout_x, order_parent_y, all_positions, "PL-to-PL")
                        if crossing_x is not None:
                            order_status = "executed"
                    
                    sender_pos_number = get_position_number_from_label(label_ras)
                    sender_arrow_number = arrow_number_ras
                    receiver_pos_number = get_position_number(x_rasr, pl_labels + ph_labels, all_positions)
                    
                    # Create unique identifier for the trendline
                    trendline_id = f"PL-to-PL_{sender_pos_number}_{receiver_pos_number}"
                    if trendline_id not in unique_trendlines:
                        main_trendline_entry = {
                            "type": "PL-to-PL",
                            "sender": {
                                "candle_color": get_candle_for_parent(x_ras, all_positions)[1],
                                "position_number": sender_pos_number,
                                "sender_arrow_number": arrow_number_ras
                            },
                            "receiver": {
                                "candle_color": receiver_color_rasr,
                                "position_number": receiver_pos_number,
                                "order_type": "long",
                                "order_status": order_status,
                                "Breakout_parent": breakout_label if breakout_label and breakout_label.startswith("PL") else "invalid",
                                "order_parent": order_parent_label if order_parent_label and order_parent_label.startswith("PH") else "invalid",
                                "receiver_contractcandle_arrownumber": arrow_number_rasr
                            }
                        }
                        main_trendline_data.append(main_trendline_entry)
                        contracts_data.append(main_trendline_entry)  # Only append main_trendline_entry
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
                breakout_label, order_parent_label, order_parent_y, order_parent_x, breakout_x, breakout_y = find_breakout_and_order_parent(
                    x_rasr, bottom_y_rasr, "PL-to-PL", all_parents, pl_labels, ph_labels
                )
                # Determine order_status
                order_status = "pending order"
                if breakout_label == "invalid" or order_parent_label == "invalid":
                    order_status = "invalid"
                elif breakout_label and order_parent_label:
                    crossing_x = find_crossing_candle(breakout_x, order_parent_y, all_positions, "PL-to-PL")
                    if crossing_x is not None:
                        order_status = "executed"
                
                sender_pos_number = get_position_number_from_label(label_ms)
                sender_arrow_number = arrow_number_ms
                receiver_pos_number = get_position_number(x_rasr, pl_labels + ph_labels, all_positions)
                
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
                            "order_type": "long",
                            "order_status": order_status,
                            "Breakout_parent": breakout_label if breakout_label and breakout_label.startswith("PL") else "invalid",
                            "order_parent": order_parent_label if order_parent_label and order_parent_label.startswith("PH") else "invalid",
                            "receiver_contractcandle_arrownumber": arrow_number_rasr
                        }
                    }
                    main_trendline_data.append(main_trendline_entry)
                    contracts_data.append(main_trendline_entry)  # Only append main_trendline_entry
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
        print("Number of contracts set to 0, only position numbers drawn")
        main_trendline_image_path = os.path.join(OUTPUT_FOLDER, f"{base_name}_parent_main_trendlines.png")
        cv2.imwrite(main_trendline_image_path, img_main_trendlines)
        print(f"Parent trendlines image saved to: {main_trendline_image_path}")
        save_contracts_data_to_json(contracts_data)
        return main_trendline_image_path, main_trendline_data
    
    # Draw selected trendlines
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
        order_parent_label = main_trendline['receiver']['order_parent']
        breakout_label = main_trendline['receiver']['Breakout_parent']
        order_parent_candle = None
        order_parent_x = None
        breakout_x = None
        
        if order_parent_label != "invalid" and breakout_label != "invalid":
            # Get Order Parent coordinates and candlestick
            for x, y, label, _ in (pl_labels if main_trendline['type'] == 'PH-to-PH' else ph_labels):
                if label == order_parent_label:
                    order_parent_candle, _ = get_candle_for_parent(x, all_positions)
                    order_parent_x = x
                    break
            # Get Breakout Parent x-coordinate
            for x, y, label, _ in (ph_labels if main_trendline['type'] == 'PH-to-PH' else pl_labels):
                if label == breakout_label:
                    breakout_x = x
                    break
        
        if order_parent_candle is None or breakout_x is None:
            print(f"No candlestick found for order_parent {order_parent_label} at x={order_parent_x} or breakout_parent {breakout_label} at x={breakout_x}, skipping box")
            continue
        
        # Get y-coordinates for the box (top and bottom of Order Parent candlestick)
        if main_trendline['type'] == 'PH-to-PH':
            # For PH-to-PH: Box from top to bottom of PLOP
            top_y, bottom_y = get_parent_y_coordinates(order_parent_label, 'PL', all_positions, pl_labels, ph_labels)
            order_y = top_y  # For crossing check (top of PLOP)
        else:
            # For PL-to-PL: Box from bottom to top of PHOP
            bottom_y, top_y = get_parent_y_coordinates(order_parent_label, 'PH', all_positions, pl_labels, ph_labels)
            order_y = bottom_y  # For crossing check (bottom of PHOP)
        
        if top_y is None or bottom_y is None:
            print(f"Skipping box for {main_trendline['type']} from {order_parent_label} due to missing y-coordinates")
            continue
        
        # Find the first candlestick after Breakout Parent that crosses the Order Parent level
        crossing_x = find_crossing_candle(breakout_x, order_y, all_positions, main_trendline['type'])
        if crossing_x is not None:
            box_right_x = crossing_x
            print(f"Drew 1px white box for {main_trendline['type']} from {order_parent_label} (x={order_parent_x}, top_y={top_y}, bottom_y={bottom_y}) "
                  f"to crossing candlestick (x={crossing_x})")
        else:
            box_right_x = img_width
            print(f"Drew 1px white box for {main_trendline['type']} from {order_parent_label} (x={order_parent_x}, top_y={top_y}, bottom_y={bottom_y}) "
                  f"to right edge (x={img_width})")
        
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
              f"order_parent: {main_trendline['receiver']['order_parent']}")
    
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
    # Save all contracts data
    json_path = os.path.join(OUTPUT_FOLDER, "contracts.json")
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
    pending_json_path = os.path.join(OUTPUT_FOLDER, "pendingorder.json")
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




def main():
    """Main function to orchestrate image processing pipeline."""
    # Clear the base output folder before processing
    clear_output_folder()
    
    # Create market-specific output folder
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    try:
        # MECHANISM CONTROLS
        left_required, right_required = controlleftandrighthighsandlows("1", "1")
        main_trendline_position = main_trendlinetocandleposition("2")
        distance_threshold = PHandPLmain_trendlinedistancecontrol("10")
        num_contracts = main_trendlinecontracts("100")
        allow_latest_main_trendline = latestmain_trendline("show")

        # Load latest chart
        img, base_name = load_latest_chart()
        if img is None:
            return
        
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
        save_enhanced_image(img_enhanced, base_name)
        
        # Remove horizontal lines
        img_enhanced, mask_red, mask_green, mask = remove_horizontal_lines(img_enhanced, mask_red, mask_green, width)
        
        # Detect candlestick contours and collect arrow data
        img_contours, red_positions, green_positions, arrow_data = detect_candlestick_contours(img_enhanced, mask_red, mask_green)
        
        # Save contour image
        save_contour_image(img_contours, base_name)
        
        # Save arrow data to JSON
        save_arrow_data_to_json(arrow_data)
        
        # Construct all_positions from red_positions and green_positions
        all_positions = [(pos, 'red') for pos in red_positions] + [(pos, 'green') for pos in green_positions]
        all_positions.sort(key=lambda x: x[0][0])
        
        # Identify and label Parent Highs and Lows using enhanced image
        parent_labeled_image_path, pl_labels, ph_labels = identify_parent_highs_and_lows(
            img_enhanced, all_positions, base_name, left_required, right_required, arrow_data
        )
        img_parent_labeled = cv2.imread(parent_labeled_image_path)
        
        # Draw main_trendlines between Parent Highs and Lows and collect main_trendline data
        main_trendline_image_path, main_trendline_data = draw_parent_main_trendlines(
            img_parent_labeled, all_positions, base_name, left_required, right_required,
            main_trendline_position, distance_threshold, num_contracts, allow_latest_main_trendline,
            pl_labels, ph_labels
        )
        
        # Note: User will remove save_main_trendline_data_to_json call
        # save_main_trendline_data_to_json(main_trendline_data)
        
        print("Candlestick contour processing, parent highs/lows labeling, main_trendline drawing, and JSON data saving complete.")
        
    except Exception as e:
        print(f"Error processing image: {e}")
if __name__ == "__main__":
    main()
    

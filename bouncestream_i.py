import os
import cv2
import numpy as np
import shutil
import json
import multiprocessing

# Path configuration
BASE_INPUT_FOLDER = r"C:\xampp\htdocs\CIPHER\cipher i\bouncestream\chart\fetched"
BASE_OUTPUT_FOLDER = r"C:\xampp\htdocs\CIPHER\cipher i\bouncestream\chart\processing\initial"

# Market names and timeframes from fetchmarket.py
MARKETS = [
    "Volatility 75 Index",
    "Step Index",
    "Drift Switch Index 30",
    "Drift Switch Index 20",
    "Drift Switch Index 10",
    "Volatility 25 Index",
    "XAUUSD",
    "US Tech 100",
    "Wall Street 30",
    "GBPUSD",
    "EURUSD",
    "USDJPY",
    "AUDUSD",
    "USDCAD",
    "USDCHF",
    "NZDUSD"
]
TIMEFRAMES = ["M5", "M15", "M30", "H1", "H4"]

def clear_output_folder():
    """Clear all files and subfolders in the BASE_OUTPUT_FOLDER."""
    try:
        if os.path.exists(BASE_OUTPUT_FOLDER):
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

def load_latest_chart(input_folder, market_name):
    """Find and load the latest chart image containing MARKET_NAME in the market-specific folder."""
    if not os.path.exists(input_folder):
        print(f"Input folder does not exist: {input_folder}")
        return None, None
    
    files = [
        os.path.join(input_folder, f) for f in os.listdir(input_folder)
        if os.path.isfile(os.path.join(input_folder, f)) and 
           market_name.replace(" ", "").lower() in f.replace(" ", "").lower()
    ]
    
    if not files:
        print(f"No files found containing '{market_name}' in {input_folder}")
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

def detect_candlestick_contours(img_enhanced, mask_red, mask_green):
    """Detect and draw contours for red and green candlesticks, draw one white arrow per unique candlestick position pointing downward to the top with a vertical line to the image top, and collect arrow data for JSON output."""
    img_contours = img_enhanced.copy()
    height, width = img_contours.shape[:2]
    contours_red, _ = cv2.findContours(mask_red, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    red_count = 0
    red_positions = []
    all_candlestick_positions = []

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

    for contour in contours_green:
        if cv2.contourArea(contour) >= 0.01:
            green_count += 1
            x, y, w, h = cv2.boundingRect(contour)
            center_x = x + w // 2
            top_y = y
            bottom_y = y + h
            green_positions.append((center_x, top_y, bottom_y))
            all_candlestick_positions.append((center_x, top_y, bottom_y, 'green', contour))

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
        cv2.drawContours(img_contours, [contour], -1, contour_color, 1)
        arrow_start = (center_x, max(0, top_y - 30))
        arrow_end = (center_x, top_y)
        cv2.arrowedLine(img_contours, arrow_start, arrow_end, (255, 255, 255), 1, tipLength=0.3)
        cv2.line(img_contours, arrow_end, (center_x, 0), (255, 255, 255), 1)

    arrow_data = []
    arrow_count = 0
    for i, (center_x, top_y, bottom_y, color, _) in enumerate(reversed(unique_positions[:-1]), 1):
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

def save_arrow_data_to_json(arrow_data, output_folder):
    """Save the arrow data to a JSON file named after the market in the OUTPUT_FOLDER."""
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
    connected_contour_image_path = os.path.join(output_folder, f"{base_name}_connected_contours.png")
    cv2.imwrite(connected_contour_image_path, img_connected_contours)
    print(f"Connected contour image saved to: {connected_contour_image_path}")
    return connected_contour_image_path

def identify_parent_highs_and_lows(img_enhanced, all_positions, base_name, left_required, right_required, arrow_data, output_folder):
    """Identify and label Parent Highs (PH) and Parent Lows (PL) on the enhanced image using arrow numbers."""
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
        
        if orig_index > 0 and orig_index < total_candles - 1:
            prev_bottom_y = all_positions[orig_index + 1][0][2]
            next_bottom_y = all_positions[orig_index - 1][0][2]
            if bottom_y > prev_bottom_y and bottom_y > next_bottom_y:
                is_low = True
            prev_top_y = all_positions[orig_index + 1][0][1]
            next_top_y = all_positions[orig_index - 1][0][1]
            if top_y < prev_top_y and top_y < next_top_y:
                is_high = True
        elif orig_index == 0 and total_candles > 2:
            next_bottom_y = all_positions[orig_index + 1][0][2]
            next_top_y = all_positions[orig_index + 1][0][1]
            if bottom_y > next_bottom_y:
                is_low = True
            if top_y < next_top_y:
                is_high = True
        elif orig_index == total_candles - 2 and total_candles > 2:
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

# New function definitions (unchanged from provided code)
def draw_parent_main_trendlines(img_parent_labeled, all_positions, base_name, left_required, right_required, 
                                main_trendline_position, distance_threshold, num_contracts, allow_latest_main_trendline,
                                pl_labels, ph_labels):
    """
    Draw main trendlines between Parent Highs (PH) and Parent Lows (PL), label position numbers, draw boxes for receivers,
    and collect main_trendline data for JSON output, including missed entry and order statuses.
    """
    img_main_trendlines = img_parent_labeled.copy()
    total_candles = len(all_positions)
    
    # Get image width for extending main_trendlines to the right edge
    img_width = img_main_trendlines.shape[1]
    
    # Font settings for position numbers and 's' labels
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
    
    # Track used points for PH-to-PL and PL-to-PH connections
    used_points_ph_to_pl = set()
    used_points_pl_to_ph = set()
    
    # Initialize lists to store main_trendline data for JSON
    main_trendline_data = []  # For missed entry data
    contracts_data = []       # For contracts data
    
    # Initialize list to store parent distances for JSON
    parent_distances = []
    
    # Calculate PH-to-PH vertical distances
    ph_labels_sorted = sorted(ph_labels, key=lambda x: x[0])  # Sort by x-coordinate
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
    pl_labels_sorted = sorted(pl_labels, key=lambda x: x[0])  # Sort by x-coordinate
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
    
    # Save parent distances to a separate JSON file
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
    
    # Helper function to check if a main_trendline crosses another PH or PL
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
    
    def count_parents_ahead(x, parent_points):
        return sum(1 for p in parent_points if p[0] > x)
    
    # Helper function to extract position number from label (e.g., PH3 -> 3, PL5 -> 5)
    def get_position_number_from_label(label):
        try:
            return int(label[2:])  # Extract number after "PH" or "PL"
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
    
    # Helper function to get the next two parents after the receiver
    def get_next_two_parents(receiver_x, pl_labels, ph_labels, main_trendline_type):
        all_parents = [(x, 'PL', y) for x, y, _, _ in pl_labels] + [(x, 'PH', y) for x, y, _, _ in ph_labels]
        parents_to_right = sorted([(x, parent_type, y) for x, parent_type, y in all_parents if x > receiver_x], key=lambda p: p[0])
        if len(parents_to_right) >= 1:
            first_parent = parents_to_right[0][1]
            if main_trendline_type == 'PH-to-PL' and first_parent != 'PH':
                first_parent = 'invalid'
            elif main_trendline_type == 'PL-to-PH' and first_parent != 'PL':
                first_parent = 'invalid'
        else:
            first_parent = 'invalid'
        if len(parents_to_right) >= 2:
            second_parent = parents_to_right[1][1]
            if main_trendline_type == 'PH-to-PL' and second_parent != 'PH':
                second_parent = 'invalid'
            elif main_trendline_type == 'PL-to-PH' and second_parent != 'PL':
                second_parent = 'invalid'
        else:
            second_parent = 'invalid'
        return first_parent, second_parent
    
    # Helper function to find the candlestick that contains a parent (PL or PH) by x-coordinate
    def get_candle_for_parent(parent_x, all_positions):
        for pos, color in all_positions:
            if pos[0] == parent_x:
                return pos, color
        return None, None
    
    # Helper function to find the next PL or PH to the right
    def get_next_parent_position(receiver_x, pl_labels, ph_labels, main_trendline_type):
        all_parents = [(x, 'PL', y) for x, y, _, _ in pl_labels] + [(x, 'PH', y) for x, y, _, _ in ph_labels]
        parents_to_right = [(x, parent_type, y) for x, parent_type, y in all_parents if x > receiver_x]
        if not parents_to_right:
            return None, None, None
        valid_parent_type = 'PH' if main_trendline_type == 'PH-to-PL' else 'PL'
        valid_parents = [p for p in parents_to_right if p[1] == valid_parent_type]
        if not valid_parents:
            return None, None, None
        closest_parent = min(valid_parents, key=lambda p: p[0])
        return closest_parent[0], closest_parent[1], closest_parent[2]
    
    # Helper function to find the next candlestick touched by a horizontal line at y_level
    def get_next_candle_touched(receiver_x, y_level, sorted_positions, check_below=False, check_above=False):
        for pos_data, _ in sorted_positions:
            if pos_data[0] > receiver_x:
                if check_below and pos_data[2] > y_level:
                    return pos_data[0]
                if check_above and pos_data[1] < y_level:
                    return pos_data[0]
                if not (check_below or check_above) and pos_data[1] <= y_level <= pos_data[2]:
                    return pos_data[0]
        return None
    
    # Helper function to find the latest candlestick
    def get_latest_candle(sorted_positions):
        if sorted_positions:
            return sorted_positions[-1][0]
        return None
    
    # Helper function to find the opposite parent after the next parent
    def get_opposite_parent(next_parent_x, receiver_x, pl_labels, ph_labels, main_trendline_type):
        all_parents = [(x, 'PL', y) for x, y, _, _ in pl_labels] + [(x, 'PH', y) for x, y, _, _ in ph_labels]
        parents_to_right = [(x, parent_type, y) for x, parent_type, y in all_parents if x > next_parent_x]
        if not parents_to_right:
            return None, None, None
        opposite_type = 'PL' if main_trendline_type == 'PH-to-PL' else 'PH'
        opposite_parents = [p for p in parents_to_right if p[1] == opposite_type]
        if not opposite_parents:
            return None, None, None
        closest_opposite = min(opposite_parents, key=lambda p: p[0])
        return closest_opposite[0], closest_opposite[1], closest_opposite[2]
    
    # Modified function to record orders
    def record_orders(main_trendline, order_status, receiver_x, parent_candle, pl_labels, ph_labels, sorted_positions):
        """
        Update the order_status for a main trendline based on 'executed' and 'pending order' conditions,
        checking the number of contracts ahead to determine if the pending order is expired.
        """
        if order_status == "executed":
            return order_status

        all_parents = [(x, 'PL', y, label, arrow_number) for x, y, label, arrow_number in pl_labels] + \
                      [(x, 'PH', y, label, arrow_number) for x, y, label, arrow_number in ph_labels]
        parents_to_right = sorted([p for p in all_parents if p[0] > receiver_x], key=lambda p: p[0])
        
        valid_parent_type = 'PH' if main_trendline['type'] == 'PH-to-PL' else 'PL'
        contracts_ahead = [p for p in parents_to_right if p[1] == valid_parent_type]
        num_contracts_ahead = len(contracts_ahead)
        
        first_parent, second_parent = get_next_two_parents(receiver_x, pl_labels, ph_labels, main_trendline['type'])
        valid_parent = first_parent != 'invalid' or second_parent != 'invalid'
        
        if valid_parent:
            latest_candle = get_latest_candle(sorted_positions)
            if latest_candle:
                should_be_pending = False
                if main_trendline['type'] == 'PH-to-PL':
                    if latest_candle[2] < parent_candle[1]:
                        should_be_pending = True
                else:
                    if latest_candle[1] > parent_candle[2]:
                        should_be_pending = True
                
                if should_be_pending:
                    if num_contracts_ahead >= 3:
                        order_status = "pending order (expired)"
                        print(f"Set order_status to 'pending order (expired)' for {main_trendline['type'].split('-')[2]}{main_trendline['receiver']['receiver_contractcandle_arrownumber']} "
                              f"as there are {num_contracts_ahead} contracts ahead")
                    else:
                        total_contracts = num_contracts_ahead + 1
                        if total_contracts == 3:
                            order_status = "pending order"
                            print(f"Set order_status to 'pending order' for {main_trendline['type'].split('-')[2]}{main_trendline['receiver']['receiver_contractcandle_arrownumber']} "
                                  f"as it is the 3rd contract with 2 contracts ahead")
                        else:
                            order_status = "pending order (expired)"
                            print(f"Set order_status to 'pending order (expired)' for {main_trendline['type'].split('-')[2]}{main_trendline['receiver']['receiver_contractcandle_arrownumber']} "
                                  f"as it is not the 3rd contract (total contracts: {total_contracts})")
        
        return order_status
    
    # Function to record orders and missed entries
    def record_ordersandmissedentry(main_trendline, receiver_x, parent_candle, main_trendline_position, pl_labels, ph_labels, sorted_positions):
        """
        Determine order and stoploss statuses, including missed entry statuses, for a main trendline.
        """
        order_status = "none"
        stoploss_status = "none"
        top_y = parent_candle[1]
        bottom_y = parent_candle[2]
        box_end_x = min(receiver_x + 100, img_width)

        first_parent, second_parent = get_next_two_parents(receiver_x, pl_labels, ph_labels, main_trendline['type'])
        valid_parent = False
        next_parent_x = None
        next_parent_type = None
        next_parent_y = None
        if first_parent == ('PH' if main_trendline['type'] == 'PH-to-PL' else 'PL'):
            valid_parent = True
            next_parent_x, next_parent_type, next_parent_y = get_next_parent_position(receiver_x, pl_labels, ph_labels, main_trendline['type'])
        elif second_parent == ('PH' if main_trendline['type'] == 'PH-to-PL' else 'PL'):
            valid_parent = True
            all_parents = [(x, parent_type, y) for x, parent_type, y in [(x, 'PL', y) for x, y, _, _ in pl_labels] + [(x, 'PH', y) for x, y, _, _ in ph_labels] if x > receiver_x]
            if len(all_parents) >= 2:
                second_parent_data = sorted(all_parents, key=lambda p: p[0])[1]
                next_parent_x, next_parent_type, next_parent_y = second_parent_data

        if valid_parent:
            y_level = top_y if main_trendline['type'] == 'PH-to-PL' else bottom_y
            check_below = main_trendline['type'] == 'PH-to-PL'
            check_above = main_trendline['type'] == 'PL-to-PH'
            found = False
            if next_parent_x is not None:
                next_parent_candle, _ = get_candle_for_parent(next_parent_x, all_positions)
                if next_parent_candle:
                    if (check_below and next_parent_candle[2] > y_level) or (check_above and next_parent_candle[1] < y_level):
                        box_end_x = next_parent_x
                        order_status = "executed"
                        found = True
                        print(f"Order executed: Box extends to {next_parent_type} at x={next_parent_x} for {main_trendline['type'].split('-')[2]}{main_trendline['receiver']['receiver_contractcandle_arrownumber']}")
                    else:
                        for pos_data, _ in sorted_positions:
                            if pos_data[0] > next_parent_x:
                                if (check_below and pos_data[2] > y_level) or (check_above and pos_data[1] < y_level):
                                    box_end_x = pos_data[0]
                                    order_status = "executed"
                                    found = True
                                    print(f"Order executed: Box extends to candlestick at x={pos_data[0]} for {main_trendline['type'].split('-')[2]}{main_trendline['receiver']['receiver_contractcandle_arrownumber']}")
                                    break
            if not found:
                opp_parent_x, opp_parent_type, oppvii_parent_y = get_opposite_parent(next_parent_x, receiver_x, pl_labels, ph_labels, main_trendline['type'])
                if opp_parent_x is not None:
                    opp_parent_candle, _ = get_candle_for_parent(opp_parent_x, all_positions)
                    if opp_parent_candle:
                        if main_trendline['type'] == 'PH-to-PL' and opp_parent_candle[2] < top_y:
                            target_candle = None
                            for pos_data, _ in sorted_positions:
                                if pos_data[0] > opp_parent_x and pos_data[1] < opp_parent_candle[1]:
                                    target_candle = pos_data
                                    break
                            latest_candle = get_latest_candle(sorted_positions)
                            if target_candle:
                                if target_candle[0] == latest_candle[0]:
                                    if target_candle[1] < opp_parent_candle[1]:
                                        order_status = "missed entry (instant buy)"
                                        print(f"Order status: 'missed entry (instant buy)' for PL{main_trendline['receiver']['receiver_contractcandle_arrownumber']}")
                                    else:
                                        order_status = "missed entry (pending instant buy)"
                                        print(f"Order status: 'missed entry (pending instant buy)' for PL{main_trendline['receiver']['receiver_contractcandle_arrownumber']}")
                                else:
                                    order_status = "missed entry (expired buy)"
                                    print(f"Order status: 'missed entry (expired buy)' for PL{main_trendline['receiver']['receiver_contractcandle_arrownumber']}")
                            else:
                                order_status = "pending order"
                                print(f"Order status: 'pending order' for PL{main_trendline['receiver']['receiver_contractcandle_arrownumber']}")
                        elif main_trendline['type'] == 'PL-to-PH' and opp_parent_candle[1] > bottom_y:
                            target_candle = None
                            for pos_data, _ in sorted_positions:
                                if pos_data[0] > opp_parent_x and pos_data[2] > opp_parent_candle[2]:
                                    target_candle = pos_data
                                    break
                            latest_candle = get_latest_candle(sorted_positions)
                            if target_candle:
                                if target_candle[0] == latest_candle[0]:
                                    if target_candle[2] > opp_parent_candle[2]:
                                        order_status = "missed entry (instant sell)"
                                        print(f"Order status: 'missed entry (instant sell)' for PH{main_trendline['receiver']['receiver_contractcandle_arrownumber']}")
                                    else:
                                        order_status = "missed entry (pending instant sell)"
                                        print(f"Order status: 'missed entry (pending instant sell)' for PH{main_trendline['receiver']['receiver_contractcandle_arrownumber']}")
                                else:
                                    order_status = "missed entry (expired sell)"
                                    print(f"Order status: 'missed entry (expired sell)' for PH{main_trendline['receiver']['receiver_contractcandle_arrownumber']}")
                            else:
                                order_status = "pending order"
                                print(f"Order status: 'pending order' for PH{main_trendline['receiver']['receiver_contractcandle_arrownumber']}")
                        else:
                            order_status = "pending order"
                            print(f"Order status: 'pending order' for {main_trendline['type'].split('-')[2]}{main_trendline['receiver']['receiver_contractcandle_arrownumber']}")
                    else:
                        order_status = "pending order"
                        print(f"Order status: 'pending order' for {main_trendline['type'].split('-')[2]}{main_trendline['receiver']['receiver_contractcandle_arrownumber']}")
                else:
                    order_status = "pending order"
                    print(f"Order status: 'pending order' for {main_trendline['type'].split('-')[2]}{main_trendline['receiver']['receiver_contractcandle_arrownumber']}")
        else:
            order_status = "none"
            print(f"No valid parent for {main_trendline['type'].split('-')[2]}{main_trendline['receiver']['receiver_contractcandle_arrownumber']}, order_status=none")

        if order_status == "executed":
            candle_height = bottom_y - top_y
            remaining_distance = 150 - candle_height
            if remaining_distance < 0:
                remaining_distance = 0
                print(f"Warning: Candlestick height {candle_height}px exceeds 150px for {main_trendline['type'].split('-')[2]}{main_trendline['receiver']['receiver_contractcandle_arrownumber']}")
            
            s_label_y = (bottom_y + remaining_distance) if main_trendline['type'] == 'PH-to-PL' else (top_y - remaining_distance)
            check_below = main_trendline['type'] == 'PH-to-PL'
            check_above = main_trendline['type'] == 'PL-to-PH'
            next_candle_x = get_next_candle_touched(receiver_x, s_label_y, sorted_positions, check_below=check_below, check_above=check_above)
            if next_candle_x is not None:
                stoploss_status = "hit"
                box_end_x = next_candle_x
                print(f"Stoploss hit at x={next_candle_x} for {main_trendline['type'].split('-')[2]}{main_trendline['receiver']['receiver_contractcandle_arrownumber']}")
            else:
                stoploss_status = "free"
                print(f"Stoploss free for {main_trendline['type'].split('-')[2]}{main_trendline['receiver']['receiver_contractcandle_arrownumber']}")

        return order_status, stoploss_status, box_end_x
    
    # PH to PL (top to top of the candlestick at specified position to the right of the nearest PL, green)
    for i, (x1, top_y1, label1, arrow_number1) in enumerate(ph_labels):
        if label1 in used_points_ph_to_pl:
            continue
        
        nearest_pl = None
        min_distance = float('inf')
        for j, (x2, bottom_y2, label2, arrow_number2) in enumerate(pl_labels):
            if x2 > x1 and label2 not in used_points_ph_to_pl:
                distance = x2 - x1
                if distance < min_distance:
                    min_distance = distance
                    nearest_pl = (x2, bottom_y2, label2, arrow_number2)
        
        if nearest_pl is None:
            print(f"No unused PL found to the right of {label1}, skipping connection")
            continue
        
        x2, bottom_y2, label2, arrow_number2 = nearest_pl
        
        if not allow_latest_main_trendline:
            ph_ahead_count = count_parents_ahead(x2, ph_labels)
            if ph_ahead_count == 0:
                print(f"No PH found ahead of {label2}, skipping PH-to-PL main_trendline from {label1}")
                continue
        
        next_candle, receiver_color = get_candle_at_position(x2, main_trendline_position)
        if next_candle is None:
            print(f"No candlestick found at position {main_trendline_position} to the right of {label2}, skipping connection")
            continue
        
        if next_candle[1] <= top_y1:
            print(f"Skipping PH-to-PL main_trendline from {label1} to candlestick at position {main_trendline_position} right of {label2} "
                  f"because receiver top (y={next_candle[1]}) is higher than or equal to sender top (y={top_y1})")
            continue
        
        vertical_distance = abs(top_y1 - next_candle[1])
        if vertical_distance < distance_threshold:
            print(f"Skipping PH-to-PL main_trendline from {label1} to candlestick at position {main_trendline_position} right of {label2} "
                  f"due to vertical distance {vertical_distance} < threshold {distance_threshold}")
            continue
        
        crosses, crossed_label = crosses_other_parent(
            x1, next_candle[0], top_y1, next_candle[1],
            pl_labels + ph_labels,
            exclude_labels={label1, label2}
        )
        if crosses:
            print(f"Skipping PH-to-PL main_trendline from {label1} to candlestick at position {main_trendline_position} right of {label2} "
                  f"due to crossing {crossed_label}")
            continue
        
        end_x, end_y = extend_line_to_right_edge(x1, top_y1, next_candle[0], next_candle[1], img_width)
        
        crosses, crossed_label = crosses_other_parent(
            next_candle[0], end_x, next_candle[1], end_y,
            pl_labels + ph_labels,
            exclude_labels={label1, label2}
        )
        if crosses:
            print(f"Skipping PH-to-PL main_trendline extension from {label1} to candlestick at position {main_trendline_position} right of {label2} "
                  f"due to crossing {crossed_label} in extension")
            continue
        
        sender_color = [color for pos, color in all_positions if pos[0] == x1][0]
        sender_pos_number = get_position_number_from_label(label1)
        sender_arrow_number = arrow_number1
        receiver_pos_number = get_position_number(next_candle[0], pl_labels + ph_labels, all_positions)
        
        first_parent, second_parent = get_next_two_parents(x2, pl_labels, ph_labels, 'PH-to-PL')
        next_parent_label = first_parent
        
        main_trendline_entry = {
            "type": "PH-to-PL",
            "sender": {
                "candle_color": sender_color,
                "position_number": sender_pos_number,
                "sender_arrow_number": sender_arrow_number
            },
            "receiver": {
                "candle_color": receiver_color,
                "position_number": receiver_pos_number,
                "order_type": "long",
                "order_status": "none",
                "stoploss_status": "none",
                "next_parent": next_parent_label,
                "receiver_contractcandle_arrownumber": arrow_number2
            }
        }
        main_trendline_data.append(main_trendline_entry)
        contracts_entry = {
            "type": "PH-to-PL",
            "sender": {
                "candle_color": sender_color,
                "position_number": sender_pos_number,
                "sender_arrow_number": sender_arrow_number
            },
            "receiver": {
                "candle_color": receiver_color,
                "position_number": receiver_pos_number,
                "order_type": "long",
                "order_status": "none",
                "stoploss_status": "none",
                "next_parent": next_parent_label,
                "receiver_contractcandle_arrownumber": arrow_number2
            }
        }
        contracts_data.append(contracts_entry)
        
        used_points_ph_to_pl.add(label1)
        used_points_ph_to_pl.add(label2)
    
    # PL to PH (bottom to bottom of the candlestick at specified position to the right of the nearest PH, yellow)
    for i, (x1, bottom_y1, label1, arrow_number1) in enumerate(pl_labels):
        if label1 in used_points_pl_to_ph:
            continue
        
        nearest_ph = None
        min_distance = float('inf')
        for j, (x2, top_y2, label2, arrow_number2) in enumerate(ph_labels):
            if x2 > x1 and label2 not in used_points_pl_to_ph:
                distance = x2 - x1
                if distance < min_distance:
                    min_distance = distance
                    nearest_ph = (x2, top_y2, label2, arrow_number2)
        
        if nearest_ph is None:
            print(f"No unused PH found to the right of {label1}, skipping connection")
            continue
        
        x2, top_y2, label2, arrow_number2 = nearest_ph
        
        if not allow_latest_main_trendline:
            pl_ahead_count = count_parents_ahead(x2, pl_labels)
            if pl_ahead_count == 0:
                print(f"No PL found ahead of {label2}, skipping PL-to-PH main_trendline from {label1}")
                continue
        
        next_candle, receiver_color = get_candle_at_position(x2, main_trendline_position)
        if next_candle is None:
            print(f"No candlestick at position {main_trendline_position} to the right of {label2}, skipping connection")
            continue
        
        if next_candle[2] >= bottom_y1:
            print(f"Skipping PL-to-PH main_trendline from {label1} to candlestick at position {main_trendline_position} right of {label2} "
                  f"because receiver bottom (y={next_candle[2]}) is lower than or equal to sender bottom (y={bottom_y1})")
            continue
        
        vertical_distance = abs(bottom_y1 - next_candle[2])
        if vertical_distance < distance_threshold:
            print(f"Skipping PL-to-PH main_trendline from {label1} to candlestick at position {main_trendline_position} right of {label2} "
                  f"due to vertical distance {vertical_distance} < threshold {distance_threshold}")
            continue
        
        crosses, crossed_label = crosses_other_parent(
            x1, next_candle[0], bottom_y1, next_candle[2],
            pl_labels + ph_labels,
            exclude_labels={label1, label2}
        )
        if crosses:
            print(f"Skipping PL-to-PH main_trendline from {label1} to candlestick at position {main_trendline_position} right of {label2} "
                  f"due to crossing {crossed_label}")
            continue
        
        end_x, end_y = extend_line_to_right_edge(x1, bottom_y1, next_candle[0], next_candle[2], img_width)
        
        crosses, crossed_label = crosses_other_parent(
            next_candle[0], end_x, next_candle[2], end_y,
            pl_labels + ph_labels,
            exclude_labels={label1, label2}
        )
        if crosses:
            print(f"Skipping PL-to-PH main_trendline extension from {label1} to candlestick at position {main_trendline_position} right of {label2} "
                  f"due to crossing {crossed_label} in extension")
            continue
        
        sender_color = [color for pos, color in all_positions if pos[0] == x1][0]
        sender_pos_number = get_position_number_from_label(label1)
        sender_arrow_number = arrow_number1
        receiver_pos_number = get_position_number(next_candle[0], pl_labels + ph_labels, all_positions)
        
        first_parent, second_parent = get_next_two_parents(x2, pl_labels, ph_labels, 'PL-to-PH')
        next_parent_label = first_parent
        
        main_trendline_entry = {
            "type": "PL-to-PH",
            "sender": {
                "candle_color": sender_color,
                "position_number": sender_pos_number,
                "sender_arrow_number": sender_arrow_number
            },
            "receiver": {
                "candle_color": receiver_color,
                "position_number": receiver_pos_number,
                "order_type": "short",
                "order_status": "none",
                "stoploss_status": "none",
                "next_parent": next_parent_label,
                "receiver_contractcandle_arrownumber": arrow_number2
            }
        }
        main_trendline_data.append(main_trendline_entry)
        contracts_entry = {
            "type": "PL-to-PH",
            "sender": {
                "candle_color": sender_color,
                "position_number": sender_pos_number,
                "sender_arrow_number": sender_arrow_number
            },
            "receiver": {
                "candle_color": receiver_color,
                "position_number": receiver_pos_number,
                "order_type": "short",
                "order_status": "none",
                "stoploss_status": "none",
                "next_parent": next_parent_label,
                "receiver_contractcandle_arrownumber": arrow_number2
            }
        }
        contracts_data.append(contracts_entry)
        
        used_points_pl_to_ph.add(label1)
        used_points_pl_to_ph.add(label2)
    
    main_trendline_data.sort(key=lambda x: x['sender']['position_number'])
    contracts_data.sort(key=lambda x: x['sender']['position_number'])
    
    main_trendlines_to_draw = sorted(main_trendline_data, key=lambda x: x['receiver']['position_number'], reverse=True)[:num_contracts]
    print(f"Total valid main_trendlines: {len(main_trendline_data)}, drawing {min(num_contracts, len(main_trendline_data))} main_trendlines")
    
    if num_contracts == 0:
        print("Number of contracts set to 0, only position numbers drawn")
        main_trendline_image_path = os.path.join(OUTPUT_FOLDER, f"{base_name}_parent_main_trendlines.png")
        cv2.imwrite(main_trendline_image_path, img_main_trendlines)
        print(f"Parent main_trendlines image saved to: {main_trendline_image_path}")
        save_main_trendline_data_to_json(main_trendline_data)
        save_contracts_data_to_json(contracts_data)
        return main_trendline_image_path, main_trendline_data
    
    for main_trendline in main_trendlines_to_draw:
        contracts_entry = next((entry for entry in contracts_data 
                               if entry['type'] == main_trendline['type'] and 
                               entry['sender']['position_number'] == main_trendline['sender']['position_number'] and 
                               entry['receiver']['receiver_contractcandle_arrownumber'] == main_trendline['receiver']['receiver_contractcandle_arrownumber']), None)
        
        if contracts_entry is None:
            print(f"No matching contracts entry found for {main_trendline['type']} from sender pos {main_trendline['sender']['position_number']}")
            continue
        
        sender_x = next((x for x, _, label, _ in (ph_labels if main_trendline['type'] == 'PH-to-PL' else pl_labels) if label == f"{main_trendline['type'].split('-')[0]}{main_trendline['sender']['position_number']}"), None)
        sender_y = next((y for x, y, label, _ in (ph_labels if main_trendline['type'] == 'PH-to-PL' else pl_labels) if label == f"{main_trendline['type'].split('-')[0]}{main_trendline['sender']['position_number']}"), None)
        receiver_x = next((x for x, _, label, _ in (pl_labels if main_trendline['type'] == 'PH-to-PL' else ph_labels) if label == f"{main_trendline['type'].split('-')[2]}{main_trendline['receiver']['receiver_contractcandle_arrownumber']}"), None)
        receiver_y = next((y for x, y, label, _ in (pl_labels if main_trendline['type'] == 'PH-to-PL' else ph_labels) if label == f"{main_trendline['type'].split('-')[2]}{main_trendline['receiver']['receiver_contractcandle_arrownumber']}"), None)
        
        if sender_x is None or sender_y is None or receiver_x is None or receiver_y is None:
            print(f"Skipping main_trendline drawing for {main_trendline['type']} due to missing coordinates")
            continue
        
        receiver_candle, _ = get_candle_at_position(receiver_x, main_trendline_position)
        if receiver_candle is None:
            print(f"No candlestick found at position {main_trendline_position} for {main_trendline['type']}, skipping")
            continue
        
        start = (sender_x, sender_y)
        end = extend_line_to_right_edge(sender_x, sender_y, receiver_candle[0], receiver_candle[1 if main_trendline['type'] == 'PH-to-PL' else 2], img_width)
        color = (0, 255, 0) if main_trendline['type'] == 'PH-to-PL' else (0, 255, 255)
        cv2.line(img_main_trendlines, start, end, color, 2)
        
        cp_y = receiver_candle[1 if main_trendline['type'] == 'PH-to-PL' else 2]
        text_position = (receiver_candle[0] - 40 if main_trendline['type'] == 'PH-to-PL' else receiver_candle[0] - 30, cp_y)
        cv2.putText(img_main_trendlines, 'CP', text_position, cv2.FONT_HERSHEY_SIMPLEX,
                    0.5, (255, 255, 255), 1, cv2.LINE_AA)
        
        type_label = 'PH&PL' if main_trendline['type'] == 'PH-to-PL' else 'PL&PH'
        type_position = (text_position[0], text_position[1] + 15)
        cv2.putText(img_main_trendlines, type_label, type_position, cv2.FONT_HERSHEY_SIMPLEX,
                    0.4, (255, 255, 255), 1, cv2.LINE_AA)
        
        parent_candle, parent_color = get_candle_for_parent(receiver_x, all_positions)
        if parent_candle is None:
            print(f"No candlestick found for parent {main_trendline['type'].split('-')[2]}{main_trendline['receiver']['receiver_contractcandle_arrownumber']} at x={receiver_x}, skipping box and 's' label")
            continue
        
        order_status, stoploss_status, box_end_x = record_ordersandmissedentry(
            main_trendline, receiver_x, parent_candle, main_trendline_position,
            pl_labels, ph_labels, sorted_positions
        )
        
        contracts_order_status = record_orders(
            contracts_entry, order_status, receiver_x, parent_candle,
            pl_labels, ph_labels, sorted_positions
        )
        
        if order_status != "none":
            top_y = parent_candle[1]
            bottom_y = parent_candle[2]
            cv2.rectangle(img_main_trendlines, (receiver_x, top_y), (box_end_x, bottom_y), (255, 255, 255), 1)
            cv2.arrowedLine(img_main_trendlines, (box_end_x - 10, top_y), (box_end_x, top_y), (255, 255, 255), 1, tipLength=0.3)
            cv2.arrowedLine(img_main_trendlines, (box_end_x - 10, bottom_y), (box_end_x, bottom_y), (255, 255, 255), 1, tipLength=0.3)
            print(f"Drew box from (x={receiver_x}, y={top_y}) to (x={box_end_x}, y={bottom_y}) for {main_trendline['type'].split('-')[2]}{main_trendline['receiver']['receiver_contractcandle_arrownumber']}")
            
            if order_status == "executed":
                candle_height = bottom_y - top_y
                remaining_distance = 150 - candle_height
                if remaining_distance < 0:
                    remaining_distance = 0
                s_label_y = (bottom_y + remaining_distance) if main_trendline['type'] == 'PH-to-PL' else (top_y - remaining_distance)
                s_label_position = (receiver_x - 20, s_label_y + 15 if main_trendline['type'] == 'PH-to-PL' else s_label_y - 5)
                if stoploss_status == "free":
                    cv2.rectangle(img_main_trendlines, 
                                 (receiver_x, top_y if main_trendline['type'] == 'PH-to-PL' else s_label_y), 
                                 (box_end_x, s_label_y if main_trendline['type'] == 'PH-to-PL' else bottom_y), 
                                 (255, 255, 255), 1)
                    s_label = f"s={int(remaining_distance)}"
                    cv2.putText(img_main_trendlines, s_label, s_label_position, cv2.FONT_HERSHEY_SIMPLEX,
                                0.5, (255, 255, 255), 1, cv2.LINE_AA)
                    arrow_end_x = box_end_x + 10
                    if arrow_end_x <= img_width:
                        cv2.line(img_main_trendlines, (box_end_x, s_label_y), (arrow_end_x, s_label_y), (255, 255, 255), 1)
                        cv2.arrowedLine(img_main_trendlines, (arrow_end_x - 10, s_label_y), (arrow_end_x, s_label_y), (255, 255, 255), 1, tipLength=0.3)
                    print(f"Drew stoploss box and label at y={s_label_y} for {main_trendline['type'].split('-')[2]}{main_trendline['receiver']['receiver_contractcandle_arrownumber']}")
                else:
                    cv2.rectangle(img_main_trendlines, 
                                 (receiver_x, top_y if main_trendline['type'] == 'PH-to-PL' else s_label_y), 
                                 (box_end_x, s_label_y if main_trendline['type'] == 'PH-to-PL' else bottom_y), 
                                 (255, 255, 255), 1)
                    print(f"Drew stoploss box for hit status at x={box_end_x} for {main_trendline['type'].split('-')[2]}{main_trendline['receiver']['receiver_contractcandle_arrownumber']}")
        
        main_trendline['receiver']['order_status'] = order_status
        main_trendline['receiver']['stoploss_status'] = stoploss_status
        
        contracts_entry['receiver']['order_status'] = contracts_order_status
        contracts_entry['receiver']['stoploss_status'] = stoploss_status
        
        if order_status not in ["executed", "missed entry (instant buy)", "missed entry (pending instant buy)", "missed entry (expired buy)", 
                               "missed entry (instant sell)", "missed entry (pending instant sell)", "missed entry (expired sell)"]:
            first_parent, second_parent = get_next_two_parents(receiver_x, pl_labels, ph_labels, main_trendline['type'])
            valid_parent = first_parent != 'invalid' or second_parent != 'invalid'
            if valid_parent:
                latest_candle = get_latest_candle(sorted_positions)
                if latest_candle:
                    if main_trendline['type'] == 'PH-to-PL':
                        if latest_candle[2] < parent_candle[1]:
                            order_status = "pending order"
                            main_trendline['receiver']['order_status'] = order_status
                            print(f"Set order_status to 'pending order' for PL{main_trendline['receiver']['receiver_contractcandle_arrownumber']}")
                    else:
                        if latest_candle[1] > parent_candle[2]:
                            order_status = "pending order"
                            main_trendline['receiver']['order_status'] = order_status
                            print(f"Set order_status to 'pending order' for PH{main_trendline['receiver']['receiver_contractcandle_arrownumber']}")
        
        print(f"Connected {main_trendline['type']} from sender (pos={main_trendline['sender']['position_number']}, "
              f"color={main_trendline['sender']['candle_color']}, arrow={main_trendline['sender']['sender_arrow_number']}) "
              f"to receiver (pos={main_trendline['receiver']['position_number']}, "
              f"color={main_trendline['receiver']['candle_color']}, arrow={main_trendline['receiver']['receiver_contractcandle_arrownumber']}) "
              f"with {'green' if main_trendline['type'] == 'PH-to-PL' else 'yellow'} main_trendline, "
              f"order_type={main_trendline['receiver']['order_type']}, order_status={main_trendline['receiver']['order_status']}, "
              f"stoploss_status={main_trendline['receiver']['stoploss_status']}, "
              f"next parent: {main_trendline['receiver'].get('next_parent', 'invalid')}")
        print(f"Contracts data - order_status={contracts_entry['receiver']['order_status']}, "
              f"stoploss_status={contracts_entry['receiver']['stoploss_status']}")
    
    main_trendline_image_path = os.path.join(OUTPUT_FOLDER, f"{base_name}_parent_main_trendlines.png")
    cv2.imwrite(main_trendline_image_path, img_main_trendlines)
    print(f"Parent main_trendlines image saved to: {main_trendline_image_path}")
    
    save_main_trendline_data_to_json(main_trendline_data)
    save_contracts_data_to_json(contracts_data)
    
    return main_trendline_image_path, main_trendline_data

def save_contracts_data_to_json(contracts_data):
    """
    Save the contracts data to a JSON file named contracts.json in the OUTPUT_FOLDER.
    Additionally, extract entries with order_status="pending order" or "pending order (expired)"
    and valid next_parent (PH or PL) into a separate JSON file named pendingorder.json.
    """
    json_path = os.path.join(OUTPUT_FOLDER, "contracts.json")
    try:
        with open(json_path, 'w') as f:
            json.dump(contracts_data, f, indent=4)
        print(f"Contracts data saved to: {json_path}")
    except Exception as e:
        print(f"Error saving contracts data to JSON: {e}")

    def valid_pending_order():
        """
        Extract contract entries with order_status="pending order" or "pending order (expired)"
        and valid next_parent (PH or PL) and save to pendingorder.json.
        """
        pending_orders = [
            entry for entry in contracts_data
            if entry['receiver']['order_status'] in ["pending order", "pending order (expired)"] and
               entry['receiver']['next_parent'] in ['PH', 'PL']
        ]
        pending_json_path = os.path.join(OUTPUT_FOLDER, "pendingorder.json")
        try:
            with open(pending_json_path, 'w') as f:
                json.dump(pending_orders, f, indent=4)
            print(f"Pending orders (including expired) with valid next_parent saved to: {pending_json_path}")
        except Exception as e:
            print(f"Error saving pending orders to JSON: {e}")

    valid_pending_order()
    return json_path

def save_main_trendline_data_to_json(main_trendline_data):
    """
    Save the main_trendline data to a JSON file named missedentry.json in the OUTPUT_FOLDER.
    Additionally, extract entries with order_status starting with "missed entry" and valid next_parent (PH or PL)
    into a separate JSON file named reversals.json.
    """
    json_path = os.path.join(OUTPUT_FOLDER, "missedentry.json")
    try:
        with open(json_path, 'w') as f:
            json.dump(main_trendline_data, f, indent=4)
        print(f"Main trendline data saved to: {json_path}")
    except Exception as e:
        print(f"Error saving main trendline data to JSON: {e}")

    def extract_missed_entries():
        """
        Extract main_trendline entries with order_status starting with "missed entry" and valid next_parent (PH or PL)
        and save to reversals.json.
        """
        missed_entries = [
            entry for entry in main_trendline_data
            if entry['receiver']['order_status'].startswith("missed entry") and
               entry['receiver']['next_parent'] in ['PH', 'PL']
        ]
        reversals_json_path = os.path.join(OUTPUT_FOLDER, "reversals.json")
        try:
            with open(reversals_json_path, 'w') as f:
                json.dump(missed_entries, f, indent=4)
            print(f"Missed entries with valid next_parent saved to: {reversals_json_path}")
        except Exception as e:
            print(f"Error saving missed entries to JSON: {e}")

    extract_missed_entries()
    return json_path

def process_market_timeframe(market, timeframe):
    """Process a single market and timeframe combination."""
    try:
        # Dynamically construct input and output folders
        market_folder_name = market.replace(" ", "_")
        input_folder = os.path.join(BASE_INPUT_FOLDER, market_folder_name, timeframe.lower())
        output_folder = os.path.join(BASE_OUTPUT_FOLDER, market_folder_name, timeframe.lower())
        
        # Set global OUTPUT_FOLDER for use in draw_parent_main_trendlines and JSON saving functions
        global OUTPUT_FOLDER
        OUTPUT_FOLDER = output_folder
        
        # Create output folder
        os.makedirs(output_folder, exist_ok=True)
        
        print(f"Processing market: {market}, timeframe: {timeframe}")
        
        # MECHANISM CONTROLS
        left_required, right_required = controlleftandrighthighsandlows("1", "1")
        
        # Load latest chart
        img, base_name = load_latest_chart(input_folder, market)
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
        
        # Detect candlestick contours and collect arrow data
        img_contours, red_positions, green_positions, arrow_data = detect_candlestick_contours(img_enhanced, mask_red, mask_green)
        
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
            img_enhanced,  # Use img_enhanced as the base image to preserve clarity
            all_positions,
            base_name,
            left_required,
            right_required,
            main_trendline_position=1,  # Default: connect to the first candlestick after the receiver parent
            distance_threshold=10,     # Default: minimum vertical distance of 10 pixels
            num_contracts=3,           # Default: draw up to 3 main trendlines
            allow_latest_main_trendline=True,  # Default: allow trendlines to the latest candlestick
            pl_labels=pl_labels,
            ph_labels=ph_labels
        )
        
        print(f"Completed processing market: {market}, timeframe: {timeframe}")
        return True
    
    except Exception as e:
        print(f"Error processing market {market} timeframe {timeframe}: {e}")
        return False

def main():
    """Main function to process all markets and timeframes."""
    try:
        # Clear the base output folder before processing
        clear_output_folder()
        
        # Create a list of all market and timeframe combinations
        tasks = [(market, timeframe) for market in MARKETS for timeframe in TIMEFRAMES]
        
        # Use multiprocessing to process markets and timeframes in parallel
        with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
            results = pool.starmap(process_market_timeframe, tasks)
        
        # Print summary of processing
        success_count = sum(1 for result in results if result)
        print(f"Processing completed: {success_count}/{len(tasks)} market-timeframe combinations processed successfully")
        
    except Exception as e:
        print(f"Error in main processing: {e}")

if __name__ == "__main__":
    main()
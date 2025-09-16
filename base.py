import json
import os

def create_base_json():
    # Define the target path and directory
    target_dir = r"C:\xampp\htdocs\CIPHER\cipher i\programmes\chart"
    target_file = os.path.join(target_dir, "base.json")

    # Create the directory if it doesn't exist
    os.makedirs(target_dir, exist_ok=True)

    # Define the JSON data structure
    base_data = {
        "MARKETS": [
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
        ],
        "FOREX_MARKETS": [
            "XAUUSD",
            "GBPUSD",
            "EURUSD",
            "USDJPY",
            "AUDUSD",
            "USDCAD",
            "USDCHF",
            "NZDUSD"
        ],
        "SYNTHETIC_INDICES": [
            "Volatility 75 Index",
            "Step Index",
            "Drift Switch Index 30",
            "Drift Switch Index 20",
            "Drift Switch Index 10",
            "Volatility 25 Index"
        ],
        "INDEX_MARKETS": [
            "US Tech 100",
            "Wall Street 30"
        ],
        "TIMEFRAMES": [
            "M5",
            "M15",
            "M30",
            "H1",
            "H4"
        ],
        "CREDENTIALS": {
            "LOGIN_ID": "101347351",
            "PASSWORD": "@Techknowdge12#",
            "SERVER": "DerivSVG-Server-02",
            "BASE_URL": "https://mt5-real02-web-svg.deriv.com/terminal?login=101347351&server=DerivSVG-Server-02",
            "TERMINAL_PATH": "C:\\Program Files\\MetaTrader 5\\terminal64.exe"
        }
    }

    # Check if base.json exists
    if not os.path.exists(target_file):
        # Create base.json if it doesn't exist
        with open(target_file, 'w') as json_file:
            json.dump(base_data, json_file, indent=2)
        print(f"base.json created successfully at {target_file}")
    else:
        print(f"base.json already exists at {target_file}")

if __name__ == "__main__":
    create_base_json()
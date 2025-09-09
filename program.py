import analysechart_m
import updateorders
import MetaTrader5 as mt5
import time
from datetime import datetime, timedelta
import pytz

MARKETS = [
    "AUDUSD", "Volatility 75 Index", "Step Index", "Drift Switch Index 30",
    "Drift Switch Index 20", "Drift Switch Index 10", "Volatility 25 Index",
    "XAUUSD", "US Tech 100", "Wall Street 30", "GBPUSD", "EURUSD", "USDJPY",
    "USDCAD", "USDCHF", "NZDUSD"
]
TIMEFRAMES = ["M5", "M15", "M30", "H1", "H4"]

def candletimeleft(market, timeframe, candle_time):
    # Initialize MT5
    print(f"[Process-{market}] Initializing MT5 for {market}")
    for attempt in range(3):
        if mt5.initialize(path=r"C:\Program Files\MetaTrader 5\terminal64.exe", timeout=60000):
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
        if mt5.login(login=101347351, password="@Techknowdge12#", server="DerivSVG-Server-02", timeout=60000):
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
            
            if time_left > 4:
                return time_left, next_close_time
            else:
                print(f"[Process-{market}] Time left ({time_left:.2f} minutes) is <= 4 minutes, restarting sequence")
                return None, None  # Return None to trigger restart in run_sequential
            
    finally:
        mt5.shutdown()

def run_analysechart_m():
    """Run the analysechart_m script."""
    try:
        analysechart_m.main()
        print("analysechart_m completed.")
    except Exception as e:
        print(f"Error in analysechart_m: {e}")

def run_updateorders():
    """Run the updateorders script."""
    try:
        updateorders.main()
        print("updateorders completed.")
    except Exception as e:
        print(f"Error in updateorders: {e}")

def execute(mode="loop"):
    """Execute the scripts sequentially with the specified mode: 'loop' or 'once'."""
    if mode not in ["loop", "once"]:
        raise ValueError("Invalid mode. Use 'loop' or 'once'.")

    def run_sequential():
        """Helper function to run analysechart_m and updateorders sequentially with candle time checks."""
        default_market = MARKETS[0]  # Use first market from MARKETS list
        timeframe = "M5"
        
        # First updateorders call
        time_left, next_close_time = candletimeleft(default_market, timeframe, None)
        if time_left is None or next_close_time is None:
            print(f"[Process-{default_market}] Failed to retrieve candle time for {default_market} (M5). Restarting sequence.")
            return
        print(f"[Process-{default_market}] Time left for M5 candle: {time_left:.2f} minutes. Running updateorders.")
        run_updateorders()  # Run updateorders first

        # analysechart_m call
        time_left, next_close_time = candletimeleft(default_market, timeframe, None)
        if time_left is None or next_close_time is None:
            print(f"[Process-{default_market}] Failed to retrieve candle time for {default_market} (M5). Restarting sequence.")
            return
        print(f"[Process-{default_market}] Time left for M5 candle: {time_left:.2f} minutes. Running analysechart_m.")
        run_analysechart_m()  # Run analysechart_m second

        # Second updateorders call
        time_left, next_close_time = candletimeleft(default_market, timeframe, None)
        if time_left is None or next_close_time is None:
            print(f"[Process-{default_market}] Failed to retrieve candle time for {default_market} (M5). Restarting sequence.")
            return
        print(f"[Process-{default_market}] Time left for M5 candle: {time_left:.2f} minutes. Running updateorders.")
        run_updateorders()  # Run updateorders third

        print("Both scripts completed.")

    try:
        if mode == "loop":
            while True:
                run_sequential()
                print("Restarting...")
        else:  # mode == "once"
            run_sequential()
            print("Execution completed (once mode).")

    except Exception as e:
        print(f"Error in main loop: {e}")

if __name__ == "__main__":
    # Example: Change to "once" or "loop" as needed
    execute(mode="once")
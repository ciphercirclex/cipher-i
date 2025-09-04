from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
import time
import signal
import sys
import os
import colorama
from colorama import Fore, Style
import json
from tqdm import tqdm
from datetime import datetime
import re

# Initialize colorama for colored output
colorama.init()

# Configuration
admin_email = 'ciphercirclex12@gmail.com'
admin_password = '@ciphercircleadminauthenticator#'
temp_download_dir = r'C:\xampp\htdocs\CIPHER\temp_downloads'
BATCH_SIZE = 1000  # Number of rows per INSERT query
MAX_RETRIES = 3  # Number of retries for network failures
REQUEST_TIMEOUT = 30  # Timeout for HTTP requests
SYNC_STATE_FILE = os.path.join(temp_download_dir, 'sync_state.json')
MIN_SYNC_INTERVAL = 1800  # 30 minutes in seconds

# Global driver and session dictionaries
drivers = {'server2': None, 'backuper': None}
sessions = {'server2': None, 'backuper': None}
backuper_used = False

# Server URL configurations
def get_server2_urls():
    """Return URLs for Server 2."""
    return {
        'query_page': 'https://connectwithinfinitydb.wuaze.com/phpmyadmintemplate.php',
        'fetch': 'https://connectwithinfinitydb.wuaze.com/phpmyadmin_tablesfetch.php'
    }

def get_backuper_urls():
    """Return URLs for Backuper."""
    return {
        'query_page': 'https://xevhtoaljedpik.infy.uk/phpmyadmintemplate.php',
        'fetch': 'https://xevhtoaljedpik.infy.uk/phpmyadmin_tablesfetch.php'
    }

def log_and_print(message, level="INFO"):
    """Print formatted messages with color coding."""
    indent = "    "
    color = {
        "INFO": Fore.CYAN,
        "SUCCESS": Fore.GREEN,
        "WARNING": Fore.YELLOW,
        "ERROR": Fore.RED,
        "TITLE": Fore.MAGENTA
    }.get(level, Fore.WHITE)
    formatted_message = f"{level:7} | {indent}{message}"
    print(f"{color}{formatted_message}{Style.RESET_ALL}")

def signal_handler(sig, frame):
    """Handle script interruption (Ctrl+C)."""
    log_and_print("Script interrupted by user. Initiating cleanup...", "WARNING")
    cleanup()
    sys.exit(0)

def cleanup():
    """Clean up resources before exiting."""
    log_and_print("--- Cleanup Operations ---", "TITLE")
    log_and_print("Starting cleanup process", "INFO")
    
    for server, driver in drivers.items():
        if driver:
            log_and_print(f"Closing {server} browser", "INFO")
            try:
                driver.quit()
                log_and_print(f"{server} browser closed successfully", "SUCCESS")
            except Exception as e:
                log_and_print(f"Failed to close {server} browser: {str(e)}", "ERROR")
            drivers[server] = None

    for server, session in sessions.items():
        if session:
            session.close()
            sessions[server] = None
            log_and_print(f"Closed {server} HTTP session", "SUCCESS")

    if os.path.exists(temp_download_dir):
        log_and_print(f"Cleaning temporary download directory: {temp_download_dir}", "INFO")
        for temp_file in os.listdir(temp_download_dir):
            if temp_file == 'sync_state.json':  # Preserve sync state
                continue
            file_path = os.path.join(temp_download_dir, temp_file)
            try:
                os.remove(file_path)
                log_and_print(f"Removed temporary file: {file_path}", "SUCCESS")
            except Exception as e:
                log_and_print(f"Failed to remove {file_path}: {str(e)}", "ERROR")

def load_sync_state():
    """Load last synced state from file."""
    if os.path.exists(SYNC_STATE_FILE):
        with open(SYNC_STATE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_sync_state(state):
    """Save sync state to file."""
    os.makedirs(temp_download_dir, exist_ok=True)
    with open(SYNC_STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)
    log_and_print(f"Saved sync state to {SYNC_STATE_FILE}", "SUCCESS")

def check_backuper_used_flag():
    """Check if backuper has been used."""
    global backuper_used
    flag_path = os.path.join(temp_download_dir, 'backuper_used.txt')
    if os.path.exists(flag_path):
        backuper_used = True
        return True
    return False

def set_backuper_used_flag():
    """Set backuper used flag."""
    global backuper_used
    flag_path = os.path.join(temp_download_dir, 'backuper_used.txt')
    os.makedirs(temp_download_dir, exist_ok=True)
    with open(flag_path, 'w') as f:
        f.write('True')
    backuper_used = True
    log_and_print("Backuper used flag set", "SUCCESS")

def initialize_browser(server, query_page_url):
    """Initialize Chrome browser and authenticate."""
    global drivers, sessions
    server_key = server.lower()
    
    for attempt in range(MAX_RETRIES):
        if drivers[server_key] is not None:
            log_and_print(f"{server} browser already initialized, reusing session", "INFO")
            try:
                drivers[server_key].get(query_page_url)
                WebDriverWait(drivers[server_key], 10).until(
                    EC.invisibility_of_element_located((By.ID, "authOverlay"))
                )
                log_and_print(f"{server} page refreshed, session still valid", "SUCCESS")
                cookies = drivers[server_key].get_cookies()
                sessions[server_key] = requests.Session()
                for cookie in cookies:
                    sessions[server_key].cookies.set(cookie['name'], cookie['value'])
                log_and_print(f"Updated {server} HTTP session cookies", "SUCCESS")
                return True
            except Exception as e:
                log_and_print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to refresh {server} page: {str(e)}, reinitializing browser", "WARNING")
                drivers[server_key].quit()
                drivers[server_key] = None

        log_and_print(f"--- Step 1: Setting Up {server} Chrome Browser ---", "TITLE")
        chrome_options = Options()
        chrome_options.add_experimental_option('prefs', {
            'download.default_directory': temp_download_dir,
            'download.prompt_for_download': False,
            'download.directory_upgrade': True,
            'safebrowsing.enabled': True
        })
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--blink-settings=imagesEnabled=false")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        chrome_options.add_argument("--log-level=3")

        os.makedirs(temp_download_dir, exist_ok=True)
        log_and_print(f"Created temporary download directory: {temp_download_dir}", "SUCCESS")

        log_and_print(f"--- Step 2: Initializing {server} ChromeDriver ---", "TITLE")
        try:
            drivers[server_key] = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
            log_and_print(f"{server} ChromeDriver initialized successfully", "SUCCESS")
        except Exception as e:
            log_and_print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to initialize {server} ChromeDriver: {str(e)}", "ERROR")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
                continue
            return False

        log_and_print(f"--- Step 3: Authenticate and Access {server} Query Page ---", "TITLE")
        try:
            drivers[server_key].get(query_page_url)
            drivers[server_key].execute_script(f"localStorage.setItem('admin_email', '{admin_email}');")
            drivers[server_key].execute_script(f"localStorage.setItem('admin_password', '{admin_password}');")
            log_and_print(f"Set {server} localStorage credentials", "SUCCESS")
            drivers[server_key].get(query_page_url)
            log_and_print(f"Loaded {server} page: {drivers[server_key].current_url}", "SUCCESS")
            log_and_print(f"{server} page title: {drivers[server_key].title}", "INFO")

            WebDriverWait(drivers[server_key], 10).until(
                EC.invisibility_of_element_located((By.ID, "authOverlay"))
            )
            log_and_print(f"{server} authentication successful - access granted", "SUCCESS")
            
            sessions[server_key] = requests.Session()
            cookies = drivers[server_key].get_cookies()
            for cookie in cookies:
                sessions[server_key].cookies.set(cookie['name'], cookie['value'])
            log_and_print(f"Initialized {server} HTTP session with cookies", "SUCCESS")
            return True
        except Exception as e:
            error_msg = f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to load {server} query page or bypass authentication: {str(e)}"
            log_and_print(error_msg, "ERROR")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
                continue
            return False

def execute_query(sql_query, server="Backuper"):
    """Execute an SQL query on the specified server."""
    global drivers, sessions
    server_key = server.lower()
    urls = get_server2_urls() if server == "Server2" else get_backuper_urls()
    
    try:
        signal.signal(signal.SIGINT, signal_handler)
        log_and_print(f"===== {server} Database Query Execution =====", "TITLE")

        if not initialize_browser(server, urls['query_page']):
            return {'status': 'error', 'message': f'Failed to initialize {server} browser', 'results': []}

        for attempt in range(MAX_RETRIES):
            log_and_print(f"--- Step 4: Attempting Direct POST Request on {server} ---", "TITLE")
            log_and_print(f"Executing query via POST on {server}: {sql_query}", "INFO")
            try:
                headers = {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                data = {'sql_query': sql_query}
                response = sessions[server_key].post(urls['fetch'], headers=headers, data=data, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                response_data = response.json()

                log_and_print(f"{server} server response: {json.dumps(response_data, indent=2)}", "DEBUG")
                
                if response_data.get('status') == 'success':
                    results = []
                    if 'rows' in response_data.get('data', {}):
                        for row in response_data['data']['rows']:
                            results.append({key: str(value) for key, value in row.items()})
                        log_and_print(f"Fetched {len(results)} rows from {server} direct POST", "SUCCESS")
                    elif 'affectedRows' in response_data.get('data', {}):
                        results = {'affected_rows': response_data['data']['affectedRows']}
                        log_and_print(f"Non-SELECT query affected {results['affected_rows']} rows on {server}", "SUCCESS")
                    return {
                        'status': 'success',
                        'message': response_data.get('message', 'Query executed successfully'),
                        'results': results
                    }
                else:
                    log_and_print(f"Direct POST failed on {server}: {response_data.get('message', 'Unknown error')}", "ERROR")
                    debug_path = r"C:\xampp\htdocs\CIPHER\cipher server 2\__pycache__\debugs"
                    os.makedirs(debug_path, exist_ok=True)
                    with open(os.path.join(debug_path, f"{server_key}_direct_post_error.json"), "w", encoding="utf-8") as f:
                        json.dump(response_data, f, indent=2)
                    log_and_print(f"Saved {server} direct POST error response to {debug_path}\{server_key}_direct_post_error.json", "INFO")
                    return {'status': 'error', 'message': response_data.get('message', 'Unknown error'), 'results': []}
            except Exception as e:
                log_and_print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Direct POST request failed on {server}: {str(e)}", "WARNING")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2)
                    continue
                return {'status': 'error', 'message': f"Direct POST failed after {MAX_RETRIES} attempts: {str(e)}", 'results': []}
    except Exception as e:
        log_and_print(f"Critical Error on {server}: {str(e)}", "ERROR")
        return {'status': 'error', 'message': str(e), 'results': []}

def get_table_dependencies(tables):
    """Determine table creation order based on foreign key dependencies."""
    dependencies = {}
    ordered_tables = []
    no_fk_tables = []

    for table in tables:
        create_query = f"SHOW CREATE TABLE {table}"
        result = execute_query(create_query, server="Backuper")
        if result['status'] != 'success' or not result['results']:
            log_and_print(f"Failed to fetch structure for table {table}", "ERROR")
            continue

        create_sql = result['results'][0].get('Create Table', '')
        # Suppress linter warning for complex regex
        # pylint: disable=anomalous-backslash-in-string
        fk_matches = re.findall(r'(?:CONSTRAINT `[^`]+` )?FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s*`([^`]+)`\s*\(([^)]+)\)', create_sql, re.IGNORECASE)
        log_and_print(f"Foreign keys for {table}: {fk_matches}", "DEBUG")
        dependencies[table] = set(match[1] for match in fk_matches)  # Referenced tables

        if not fk_matches:
            no_fk_tables.append(table)

    # Start with tables that have no foreign keys
    ordered_tables.extend(no_fk_tables)
    remaining_tables = set(tables) - set(no_fk_tables)

    # Add dependent tables in a valid order
    while remaining_tables:
        added = False
        for table in list(remaining_tables):
            if all(dep in ordered_tables for dep in dependencies.get(table, set())):
                ordered_tables.append(table)
                remaining_tables.remove(table)
                added = True
        if not added:
            log_and_print(f"Could not resolve dependencies for tables: {remaining_tables}, adding as-is", "WARNING")
            ordered_tables.extend(remaining_tables)
            break

    return ordered_tables

def sync_backuper_to_server2():
    """Sync all data from Backuper to Server2 with 30-minute interval check."""
    global backuper_used
    if check_backuper_used_flag():
        log_and_print("Backuper already used, skipping sync", "WARNING")
        return {'status': 'error', 'message': 'Backuper already used', 'results': []}

    log_and_print("===== Backuper Sync to Server2 =====", "TITLE")
    start_time = time.time()

    # Fetch table structure and data from Backuper
    log_and_print("Fetching table structures from Backuper", "INFO")
    show_tables_query = "SHOW TABLES"
    backuper_result = execute_query(show_tables_query, server="Backuper")
    if backuper_result['status'] != 'success':
        log_and_print(f"Failed to fetch tables from Backuper: {backuper_result['message']}", "ERROR")
        return backuper_result

    tables = [list(row.values())[0] for row in backuper_result['results']]
    log_and_print(f"Found tables: {tables}", "SUCCESS")

    # Get table creation order based on dependencies
    tables = get_table_dependencies(tables)
    log_and_print(f"Table creation order: {tables}", "INFO")

    # Check existing tables on Server2
    server2_tables_result = execute_query("SHOW TABLES", server="Server2")
    server2_tables = [list(row.values())[0] for row in server2_tables_result['results']] if server2_tables_result['status'] == 'success' else []

    # Disable foreign key checks
    log_and_print("Disabling foreign key checks on Server2", "INFO")
    fk_disable_result = execute_query("SET FOREIGN_KEY_CHECKS=0", server="Server2")
    if fk_disable_result['status'] != 'success':
        log_and_print(f"Failed to disable foreign key checks on Server2: {fk_disable_result['message']}", "ERROR")

    # Load sync state
    sync_state = load_sync_state()
    current_time = datetime.now()

    for table in tables:
        # Initialize sync state for this table
        if table not in sync_state:
            sync_state[table] = {'last_id': 0, 'last_created_at': None, 'last_sync_time': None}

        # Check last sync time
        last_sync_time_str = sync_state[table].get('last_sync_time')
        if last_sync_time_str:
            try:
                last_sync_time = datetime.strptime(last_sync_time_str, '%Y-%m-%d %H:%M:%S')
                time_since_last_sync = (current_time - last_sync_time).total_seconds()
                if time_since_last_sync < MIN_SYNC_INTERVAL:
                    log_and_print(f"Skipping sync for table {table}: Last synced {int(time_since_last_sync)} seconds ago (less than 30 minutes)", "INFO")
                    continue
            except ValueError:
                log_and_print(f"Invalid last_sync_time format for table {table}: {last_sync_time_str}, proceeding with sync", "WARNING")

        # Get table structure
        create_table_query = f"SHOW CREATE TABLE {table}"
        create_result = execute_query(create_table_query, server="Backuper")
        if create_result['status'] != 'success' or not create_result['results']:
            log_and_print(f"Failed to fetch structure for table {table}: {create_result['message']}", "ERROR")
            continue
        
        create_table_sql = create_result['results'][0].get('Create Table', '')
        log_and_print(f"Fetched structure for table {table}", "SUCCESS")
        
        # Create table on Server2 if it doesn't exist
        if table not in server2_tables:
            for attempt in range(MAX_RETRIES):
                server2_result = execute_query(create_table_sql, server="Server2")
                if server2_result['status'] == 'success':
                    log_and_print(f"Created table {table} on Server2", "SUCCESS")
                    server2_tables.append(table)
                    break
                else:
                    log_and_print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to create table {table} on Server2: {server2_result['message']}", "ERROR")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(2)
                        continue
                    log_and_print(f"Failed to create table {table} after {MAX_RETRIES} attempts, skipping", "ERROR")
                    continue
        else:
            log_and_print(f"Table {table} already exists on Server2, skipping creation", "INFO")

        # Check for incremental sync column
        columns_result = execute_query(f"SHOW COLUMNS FROM {table}", server="Backuper")
        if columns_result['status'] != 'success':
            log_and_print(f"Failed to fetch columns for table {table}: {columns_result['message']}", "ERROR")
            continue
        columns = [row['Field'] for row in columns_result['results']]
        incremental_column = 'created_at' if 'created_at' in columns else 'id' if 'id' in columns else None

        # Fetch data from Backuper with incremental sync
        if incremental_column:
            last_value = sync_state[table].get('last_' + incremental_column, 0 if incremental_column == 'id' else None)
            if incremental_column == 'created_at' and last_value:
                select_query = f"SELECT * FROM {table} WHERE {incremental_column} > '{last_value}'"
            elif incremental_column == 'id' and last_value:
                select_query = f"SELECT * FROM {table} WHERE {incremental_column} > {last_value}"
            else:
                select_query = f"SELECT * FROM {table}"
        else:
            select_query = f"SELECT * FROM {table}"

        data_result = execute_query(select_query, server="Backuper")
        if data_result['status'] != 'success' or not data_result['results']:
            log_and_print(f"No data fetched for table {table} or query failed: {data_result['message']}", "WARNING")
            continue

        rows = data_result['results']
        total_rows = len(rows)
        log_and_print(f"Fetched {total_rows} rows for table {table}", "SUCCESS")

        if total_rows == 0:
            log_and_print(f"No new data to insert for table {table}", "INFO")
            continue

        # Check existing IDs on Server2 to avoid duplicates
        id_column = 'id' if 'id' in columns else None
        existing_ids = set()
        if id_column:
            id_query = f"SELECT {id_column} FROM {table}"
            id_result = execute_query(id_query, server="Server2")
            if id_result['status'] == 'success':
                existing_ids = {row[id_column] for row in id_result['results']}
                log_and_print(f"Found {len(existing_ids)} existing IDs in {table} on Server2", "INFO")

        # Batch INSERT queries
        columns = ', '.join(rows[0].keys())
        batch = []
        new_last_id = sync_state[table].get('last_id', 0)
        new_last_created_at = sync_state[table].get('last_created_at', None)

        for i, row in tqdm(enumerate(rows), total=total_rows, desc=f"Processing {table}"):
            if id_column and row[id_column] in existing_ids:
                log_and_print(f"Skipping row with id {row[id_column]} in {table} (already exists)", "INFO")
                continue

            escaped_values = [str(v).replace("'", "''") if isinstance(v, str) else str(v) for v in row.values()]
            values = ', '.join([f"'{v}'" if v != 'NULL' else 'NULL' for v in escaped_values])
            batch.append(f"({values})")

            if incremental_column:
                if incremental_column == 'id':
                    new_last_id = max(new_last_id, int(row[incremental_column]))
                elif incremental_column == 'created_at' and row[incremental_column]:
                    try:
                        current_time = datetime.strptime(row[incremental_column], '%Y-%m-%d %H:%M:%S')
                        if not new_last_created_at or current_time > datetime.strptime(new_last_created_at, '%Y-%m-%d %H:%M:%S'):
                            new_last_created_at = row[incremental_column]
                    except ValueError:
                        pass

            if len(batch) >= BATCH_SIZE or i == total_rows - 1:
                if batch:
                    insert_query = f"INSERT IGNORE INTO {table} ({columns}) VALUES {','.join(batch)}"
                    for attempt in range(MAX_RETRIES):
                        insert_result = execute_query(insert_query, server="Server2")
                        if insert_result['status'] == 'success':
                            log_and_print(f"Inserted batch of {len(batch)} rows into {table} on Server2", "SUCCESS")
                            break
                        else:
                            log_and_print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Failed to insert batch into {table} on Server2: {insert_result['message']}", "ERROR")
                            if attempt < MAX_RETRIES - 1:
                                time.sleep(2)
                                continue
                            log_and_print(f"Failed to insert batch into {table} after {MAX_RETRIES} attempts, skipping batch", "ERROR")
                            break
                    batch = []

        # Update sync state with new values and sync time
        if incremental_column == 'id':
            sync_state[table]['last_id'] = new_last_id
        elif incremental_column == 'created_at' and new_last_created_at:
            sync_state[table]['last_created_at'] = new_last_created_at
        sync_state[table]['last_sync_time'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
        save_sync_state(sync_state)

    # Re-enable foreign key checks
    log_and_print("Re-enabling foreign key checks on Server2", "INFO")
    fk_enable_result = execute_query("SET FOREIGN_KEY_CHECKS=1", server="Server2")
    if fk_enable_result['status'] != 'success':
        log_and_print(f"Failed to re-enable foreign key checks on Server2: {fk_enable_result['message']}", "ERROR")

    set_backuper_used_flag()
    log_and_print(f"Backuper sync completed in {time.time() - start_time:.2f} seconds", "SUCCESS")
    return {'status': 'success', 'message': 'Backuper sync to Server2 completed', 'results': []}

if __name__ == "__main__":
    log_and_print("Server1 is unavailable, skipping initial query", "WARNING")
    print({'status': 'error', 'message': 'Server1 is unavailable', 'results': []})

    sync_result = sync_backuper_to_server2()
    print(sync_result)

    cleanup()
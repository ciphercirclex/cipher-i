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
from bs4 import BeautifulSoup
import re
import json
from datetime import datetime

# Initialize colorama for colored output
colorama.init()

# Configuration
primary_servers = {
    'query_page': 'https://adminpanelc.infy.uk/phpmyadmintemplate.php',
    'fetch': 'https://adminpanelc.infy.uk/phpmyadmin_tablesfetch.php'
}
backup_servers = {
    'query_page': 'https://xevhtoaljedpik.infy.uk/phpmyadmintemplate.php',
    'fetch': 'https://xevhtoaljedpik.infy.uk/phpmyadmin_tablesfetch.php'
}
server3 = {
    'query_page': 'https://connectwithinfinitydb.wuaze.com/phpmyadmintemplate.php',
    'fetch': 'https://connectwithinfinitydb.wuaze.com/phpmyadmin_tablesfetch.php'
}
admin_email = 'ciphercirclex12@gmail.com'
admin_password = '@ciphercircleadminauthenticator#'
temp_download_dir = r'C:\xampp\htdocs\CIPHER\temp_downloads'
json_log_path = r'C:\xampp\htdocs\CIPHER\cipher trader\market\dbserver\connectwithdb.json'

# Global driver and session
driver = None
session = None
current_servers = primary_servers  # Start with primary servers

def log_and_print(message, level="INFO"):
    """Helper function to print formatted messages with color coding and spacing."""
    indent = "    "
    if level == "INFO":
        color = Fore.CYAN
    elif level == "SUCCESS":
        color = Fore.GREEN
    elif level == "WARNING":
        color = Fore.YELLOW
    elif level == "ERROR":
        color = Fore.YELLOW
    elif level == "TITLE":
        color = Fore.MAGENTA
    else:
        color = Fore.WHITE
    formatted_message = f"{level:7} | {indent}{message}"
    print(f"{color}{formatted_message}{Style.RESET_ALL}")

def append_to_json_log(server_type, server_url):
    """Append the server used to the JSON log file if the URL is different from the last recorded URL."""
    log_entry = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'server_type': server_type,
        'server_url': server_url,
        'status': 'success'
    }
    log_data = []

    # Check if JSON file exists and load existing data
    try:
        if os.path.exists(json_log_path):
            with open(json_log_path, 'r', encoding='utf-8') as f:
                log_data = json.load(f)
                if not isinstance(log_data, list):
                    log_data = []
    except Exception as e:
        log_and_print(f"Error reading JSON log file: {str(e)}, starting with empty log", "WARNING")
        log_data = []

    # Check if the last entry has the same URL
    if log_data and log_data[-1].get('server_url') == server_url:
        log_and_print(f"Skipping log append: Same server URL ({server_url}) as last entry", "INFO")
        return

    # Append new entry
    log_data.append(log_entry)

    # Write back to JSON file
    try:
        os.makedirs(os.path.dirname(json_log_path), exist_ok=True)
        with open(json_log_path, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, indent=2)
        log_and_print(f"Logged server usage ({server_type}: {server_url}) to {json_log_path}", "SUCCESS")
    except Exception as e:
        log_and_print(f"Failed to write to JSON log file: {str(e)}", "ERROR")

def signal_handler(sig, frame):
    """Handle script interruption (Ctrl+C)."""
    log_and_print("Script interrupted by user. Initiating cleanup...", "WARNING")
    cleanup()
    sys.exit(0)

def cleanup():
    """Clean up resources before exiting."""
    global driver, session
    log_and_print("--- Cleanup Operations ---", "TITLE")
    log_and_print("Starting cleanup process", "INFO")
    
    if driver:
        log_and_print("Clearing browser localStorage", "INFO")
        try:
            if "data:" not in driver.current_url:  # Avoid localStorage access on data: URLs
                driver.execute_script("localStorage.clear();")
                log_and_print("LocalStorage cleared successfully", "SUCCESS")
        except Exception as e:
            log_and_print(f"Failed to clear localStorage: {str(e)}", "ERROR")
        log_and_print("Closing browser", "INFO")
        driver.quit()
        driver = None
        log_and_print("Browser closed successfully", "SUCCESS")

    if session:
        session.close()
        session = None
        log_and_print("Closed HTTP session", "SUCCESS")

    if os.path.exists(temp_download_dir):
        log_and_print(f"Cleaning temporary download directory: {temp_download_dir}", "INFO")
        max_attempts = 3
        attempt = 1
        while attempt <= max_attempts:
            try:
                for temp_file in os.listdir(temp_download_dir):
                    file_path = os.path.join(temp_download_dir, temp_file)
                    os.remove(file_path)
                    log_and_print(f"Removed temporary file: {file_path}", "SUCCESS")
                os.rmdir(temp_download_dir)
                log_and_print(f"Successfully removed temporary directory: {temp_download_dir}", "SUCCESS")
                break
            except Exception as e:
                log_and_print(f"Attempt {attempt}/{max_attempts}: Failed to clean temporary directory: {str(e)}", "ERROR")
                if attempt == max_attempts:
                    log_and_print(f"Failed to remove temporary directory after {max_attempts} attempts", "ERROR")
                time.sleep(2)
                attempt += 1

def check_server_availability(url):
    """Check if a server is available by sending a HEAD request with browser-like headers."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive'
        }
        response = requests.head(url, headers=headers, timeout=10, verify=True)
        log_and_print(f"Server check response for {url}: Status {response.status_code}", "INFO")
        return response.status_code == 200
    except requests.RequestException as e:
        log_and_print(f"Server availability check failed for {url}: {str(e)}", "INFO")
        return False

def initialize_browser():
    """Initialize Chrome browser and authenticate."""
    global driver, session, current_servers
    if driver is not None:
        log_and_print("Browser already initialized, reusing session", "INFO")
        try:
            driver.get(current_servers['query_page'])
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "sql-query"))
            )
            log_and_print("Page refreshed, session still valid", "SUCCESS")
            cookies = driver.get_cookies()
            session = requests.Session()
            for cookie in cookies:
                session.cookies.set(cookie['name'], cookie['value'])
            log_and_print("Updated HTTP session cookies", "SUCCESS")
            append_to_json_log("Current", current_servers['query_page'])
            return True
        except Exception as e:
            log_and_print(f"Failed to refresh page: {str(e)}, reinitializing browser", "INFO")
            driver.quit()
            driver = None
            return initialize_browser()

    log_and_print("--- Step 1: Setting Up Chrome Browser ---", "TITLE")
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

    if not os.path.exists(temp_download_dir):
        os.makedirs(temp_download_dir)
        log_and_print(f"Created temporary download directory: {temp_download_dir}", "SUCCESS")

    log_and_print("--- Step 2: Initializing ChromeDriver ---", "TITLE")
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        log_and_print("ChromeDriver initialized successfully", "SUCCESS")
    except Exception as e:
        log_and_print(f"Failed to initialize ChromeDriver: {str(e)}", "ERROR")
        return False

    log_and_print("--- Step 3: Authenticating and Accessing Query Page ---", "TITLE")
    server_attempts = [
        (primary_servers, "Primary"),
        (backup_servers, "Backup"),
        (server3, "Server3")
    ]
    for servers, server_type in server_attempts:
        current_servers = servers
        log_and_print(f"Attempting to connect to {server_type} server: {servers['query_page']}", "INFO")
        
        # Skip availability check for backup and server3 due to confirmed manual access
        if server_type == "Primary" and not check_server_availability(servers['query_page']):
            log_and_print(f"{server_type} server is not available, trying next server", "INFO")
            continue
        elif server_type in ["Backup", "Server3"]:
            log_and_print(f"Skipping availability check for {server_type} server due to confirmed manual access", "INFO")

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                driver.get(servers['query_page'])
                page_title = driver.title
                if "suspended" in page_title.lower() or "error" in page_title.lower():
                    log_and_print(f"{server_type} server redirected to an invalid page: {page_title}", "INFO")
                    if server_type == "Server3":
                        log_and_print("All servers (Primary, Backup, Server3) are unavailable", "WARNING")
                        return False
                    break

                driver.execute_script(f"localStorage.setItem('admin_email', '{admin_email}');")
                driver.execute_script(f"localStorage.setItem('admin_password', '{admin_password}');")
                log_and_print("Set localStorage credentials", "SUCCESS")
                driver.get(servers['query_page'])
                log_and_print(f"Loaded page: {driver.current_url}", "SUCCESS")
                log_and_print(f"Page title: {driver.title}", "INFO")

                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.ID, "sql-query"))
                )
                log_and_print(f"Authentication successful on {server_type} server", "SUCCESS")
                append_to_json_log(server_type, servers['query_page'])
                
                session = requests.Session()
                cookies = driver.get_cookies()
                for cookie in cookies:
                    session.cookies.set(cookie['name'], cookie['value'])
                log_and_print("Initialized HTTP session with cookies", "SUCCESS")
                return True
            except Exception as e:
                log_and_print(f"Attempt {attempt}/{max_retries} failed for {server_type} server: {str(e)}", "INFO")
                if attempt == max_retries and server_type == "Server3":
                    log_and_print("All servers (Primary, Backup, Server3) failed after maximum attempts", "WARNING")
                    return False
                time.sleep(2)
                break  # Move to next server

def execute_query(sql_query):
    """
    Execute an SQL query via the PHP web interface using direct POST request or Selenium.
    Args:
        sql_query (str): The SQL query to execute.
    Returns:
        dict: Contains 'status', 'message', and 'results' (list of dictionaries or affected rows).
    """
    global driver, session, current_servers
    try:
        signal.signal(signal.SIGINT, signal_handler)
        log_and_print("===== Database Query Execution =====", "TITLE")

        if not initialize_browser():
            return {'status': 'error', 'message': 'Failed to initialize browser, all servers unavailable', 'results': []}

        log_and_print("--- Step 4: Attempting Direct POST Request ---", "TITLE")
        server_attempts = [
            (current_servers, "Current"),
            (backup_servers if current_servers != backup_servers else primary_servers, "Backup"),
            (server3, "Server3")
        ]
        for servers, server_type in server_attempts:
            log_and_print(f"Executing query via POST on {server_type} server: {sql_query}", "INFO")
            try:
                headers = {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Connection': 'keep-alive'
                }
                data = {'sql_query': sql_query}
                response = session.post(servers['fetch'], headers=headers, data=data, timeout=10, verify=True)
                response.raise_for_status()
                try:
                    response_data = response.json()
                except ValueError as e:
                    log_and_print(f"Invalid JSON response from {server_type} server: {str(e)}", "INFO")
                    debug_path = r"C:\xampp\htdocs\CIPHER\cipher trader\__pycache__\debugs"
                    os.makedirs(debug_path, exist_ok=True)
                    with open(os.path.join(debug_path, f"direct_post_error_{server_type.lower()}.html"), "w", encoding="utf-8") as f:
                        f.write(response.text)
                    log_and_print(f"Saved direct POST error response to {debug_path}\\direct_post_error_{server_type.lower()}.html", "INFO")
                    if server_type == "Server3":
                        log_and_print("All servers (Primary, Backup, Server3) failed POST, falling back to Selenium", "WARNING")
                    continue

                log_and_print(f"Server response: {json.dumps(response_data, indent=2)}", "DEBUG")
                
                if response_data.get('status') == 'success':
                    results = []
                    if 'rows' in response_data['data']:
                        for row in response_data['data']['rows']:
                            results.append({key: str(value) for key, value in row.items()})
                        log_and_print(f"Fetched {len(results)} rows from direct POST on {server_type} server", "SUCCESS")
                    elif 'affectedRows' in response_data['data']:
                        results = {'affected_rows': response_data['data']['affectedRows']}
                        log_and_print(f"Non-SELECT query affected {results['affected_rows']} rows on {server_type} server", "SUCCESS")
                    else:
                        log_and_print("Query executed successfully, but no results returned", "INFO")
                    append_to_json_log(server_type, servers['fetch'])
                    return {
                        'status': 'success',
                        'message': response_data.get('message', 'Query executed successfully'),
                        'results': results
                    }
                else:
                    log_and_print(f"Direct POST failed on {server_type} server: {response_data.get('message', 'Unknown error')}", "INFO")
                    debug_path = r"C:\xampp\htdocs\CIPHER\cipher trader\__pycache__\debugs"
                    os.makedirs(debug_path, exist_ok=True)
                    with open(os.path.join(debug_path, f"direct_post_error_{server_type.lower()}.json"), "w", encoding="utf-8") as f:
                        f.write(json.dumps(response_data, indent=2))
                    log_and_print(f"Saved direct POST error response to {debug_path}\\direct_post_error_{server_type.lower()}.json", "INFO")
                    if server_type == "Server3":
                        log_and_print("All servers (Primary, Backup, Server3) failed POST, falling back to Selenium", "WARNING")
                    continue
            except Exception as e:
                log_and_print(f"Direct POST request failed on {server_type} server: {str(e)}", "INFO")
                if server_type == "Server3":
                    log_and_print("All servers (Primary, Backup, Server3) failed POST, falling back to Selenium", "WARNING")
                continue

        log_and_print("--- Step 4: Executing SQL Query via Selenium ---", "TITLE")
        log_and_print(f"Executing query: {sql_query}", "INFO")
        try:
            query_textarea = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "sql-query"))
            )
            query_textarea.clear()
            time.sleep(1)
            query_textarea.send_keys(sql_query)
            entered_query = query_textarea.get_attribute("value")
            if not entered_query.strip():
                log_and_print("Failed to enter query: textarea is empty", "ERROR")
                return {'status': 'error', 'message': 'Failed to enter query: textarea is empty', 'results': []}
            log_and_print("Entered SQL query into textarea", "SUCCESS")

            execute_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[text()='Execute Query']"))
            )
            time.sleep(1)
            execute_button.click()
            log_and_print("Clicked execute query button", "SUCCESS")
            append_to_json_log("Selenium", current_servers['query_page'])
        except Exception as e:
            error_msg = f"Failed to enter or execute query: {str(e)}"
            log_and_print(error_msg, "ERROR")
            return {'status': 'error', 'message': error_msg, 'results': []}

        log_and_print("--- Step 5: Fetching Query Results ---", "TITLE")
        try:
            max_attempts = 5
            attempt = 1
            results = []
            status = 'success'
            message = 'Query executed successfully'

            while attempt <= max_attempts:
                log_and_print(f"Attempt {attempt}/{max_attempts}: Waiting for results...", "INFO")
                try:
                    wait_time = 30 if sql_query.strip().upper().startswith('UPDATE') else 20
                    WebDriverWait(driver, wait_time).until(
                        EC.any_of(
                            EC.text_to_be_present_in_element((By.ID, "query-result"), "Query Results"),
                            EC.text_to_be_present_in_element((By.ID, "column-data"), "Data for Column"),
                            EC.presence_of_element_located((By.ID, "message"))
                        )
                    )

                    message_div = driver.find_element(By.ID, "message")
                    message_text = message_div.text
                    message_class = message_div.get_attribute("class")
                    if 'error' in message_class:
                        log_and_print(f"Server reported error: {message_text}", "ERROR")
                        debug_path = r"C:\xampp\htdocs\CIPHER\cipher trader\__pycache__\debugs"
                        os.makedirs(debug_path, exist_ok=True)
                        with open(os.path.join(debug_path, f"error_page_attempt_{attempt}.html"), "w", encoding="utf-8") as f:
                            f.write(driver.page_source)
                        log_and_print(f"Saved error page source to {debug_path}\\error_page_attempt_{attempt}.html", "INFO")
                        return {'status': 'error', 'message': message_text, 'results': []}

                    query_result_div = driver.find_element(By.ID, "query-result")
                    if query_result_div.text.strip() and "Query Results" in query_result_div.text:
                        soup = BeautifulSoup(query_result_div.get_attribute('outerHTML'), 'html.parser')
                        table = soup.find('table')
                        if table:
                            headers = [th.text.strip() for th in table.find_all('th')]
                            for row in table.find_all('tr')[1:]:
                                row_data = {headers[i]: td.text.strip() for i, td in enumerate(row.find_all('td'))}
                                results.append(row_data)
                            log_and_print(f"Fetched {len(results)} rows from query-result", "SUCCESS")
                            break
                        else:
                            log_and_print("No table found in query-result div", "WARNING")
                    else:
                        log_and_print("No query results found in query-result div", "INFO")

                    column_data_div = driver.find_element(By.ID, "column-data")
                    if column_data_div.text.strip() and "Data for Column" in column_data_div.text:
                        soup = BeautifulSoup(column_data_div.get_attribute('outerHTML'), 'html.parser')
                        table = soup.find('table')
                        if table:
                            header = table.find('th').text.strip()
                            for row in table.find_all('tr')[1:]:
                                value = row.find('td').text.strip()
                                results.append({header: value})
                            log_and_print(f"Fetched {len(results)} rows from column-data", "SUCCESS")
                            break
                        else:
                            log_and_print("No table found in column-data div", "WARNING")
                    else:
                        log_and_print("No column data found in column-data div", "INFO")

                    if not results and query_result_div.text.strip():
                        match = re.search(r"Affected rows: (\d+)", query_result_div.text)
                        if match:
                            affected_rows = int(match.group(1))
                            results = {'affected_rows': affected_rows}
                            log_and_print(f"Non-SELECT query affected {affected_rows} rows", "SUCCESS")
                            break
                        else:
                            log_and_print("No affected rows information found in query-result div", "WARNING")

                    if not results:
                        log_and_print("No results found yet, retrying...", "INFO")
                        time.sleep(3)
                        attempt += 1
                    else:
                        break

                except Exception as e:
                    log_and_print(f"Attempt {attempt}/{max_attempts}: Failed to fetch results: {str(e)}", "WARNING")
                    attempt += 1
                    time.sleep(3)

            if not results and attempt > max_attempts:
                log_and_print("Failed to fetch results after maximum attempts", "ERROR")
                message = "Query executed successfully, but no results returned"
                status = 'success'

            return {'status': status, 'message': message, 'results': results}

        except Exception as e:
            error_msg = f"Failed to fetch query results: {str(e)}"
            log_and_print(error_msg, "ERROR")
            return {'status': 'error', 'message': error_msg, 'results': []}

    except Exception as e:
        log_and_print(f"Critical Error: {str(e)}", "ERROR")
        return {'status': 'error', 'message': str(e), 'results': []}

def shutdown():
    """Explicitly shut down the browser and cleanup."""
    cleanup()

if __name__ == "__main__":
    # For testing standalone
    sql_query = "SELECT id FROM user_programmes WHERE id = '2'"
    result = execute_query(sql_query)
    print(result)
    shutdown()
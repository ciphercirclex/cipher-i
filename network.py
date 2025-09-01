import time
import os
import subprocess
import re
from customalert import show_alert

# Global variables
last_network_state = None
good_secured_connection = False
no_internetconnection = False
poor_internetconnection = False
network_connected = False
currently_connected = False

def check_network_condition():
    global last_network_state, good_secured_connection, no_internetconnection, poor_internetconnection
    
    # Host to ping (Google's DNS server, reliable and widely available)
    host = "8.8.8.8"
    
    # Reduce number of pings for faster response
    ping_count = 2
    
    # Platform-specific ping command
    ping_command = ["ping", "-n" if os.name == "nt" else "-c", str(ping_count), host]
    
    try:
        # Execute the ping command with a shorter timeout
        output = subprocess.run(ping_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=5)
        
        # Check if the ping was successful (return code 0 means success)
        if output.returncode == 0:
            # Extract response times from the output
            lines = output.stdout.splitlines()
            response_times = []
            
            for line in lines:
                if "time=" in line:  # Windows format: "time=XXms"
                    try:
                        time_str = line.split("time=")[1].split("ms")[0].strip()
                        response_times.append(float(time_str))
                    except (IndexError, ValueError):
                        continue
                elif "time " in line:  # Unix-like format: "time XX.XX ms"
                    try:
                        time_str = line.split("time ")[1].split(" ms")[0].strip()
                        response_times.append(float(time_str))
                    except (IndexError, ValueError):
                        continue
            
            if response_times:
                # Calculate average response time
                avg_response_time = sum(response_times) / len(response_times)
                
                # Define thresholds (in milliseconds)
                poor_threshold = 100
                
                if avg_response_time < poor_threshold:
                    current_state = "Connected to the internet"
                    good_secured_connection = True
                    poor_internetconnection = False
                    no_internetconnection = False
                else:
                    current_state = "Poor network condition, change your connection"
                    poor_internetconnection = True
                    no_internetconnection = False
                    good_secured_connection = False
                    show_alert("Poor Internet Connection")
                    time.sleep(5)
            else:
                current_state = "Connected to the internet (unable to measure quality)"
                poor_internetconnection = True
                no_internetconnection = False
                good_secured_connection = False
                show_alert("Poor Internet Connection")
                time.sleep(5)
        else:
            current_state = "No internet, connect wifi"
            show_alert("No Internet, connect wifi")
            no_internetconnection = True
            good_secured_connection = False
            poor_internetconnection = False
            check_and_connect_network()
            
    except subprocess.TimeoutExpired:
        current_state = "No internet, connect wifi"
        show_alert("No Internet Connection")
        no_internetconnection = True
        good_secured_connection = False
        poor_internetconnection = False
        check_and_connect_network()
    except Exception as e:
        current_state = "No internet, connect wifi"
        show_alert("No Internet Connection")
        no_internetconnection = True
        good_secured_connection = False
        poor_internetconnection = False
        check_and_connect_network()
    
    # Only print if the state has changed
    if current_state != last_network_state:
        print(current_state)
        last_network_state = current_state

def check_and_connect_network():
    global network_connected, currently_connected, last_network_state, good_secured_connection, no_internetconnection, poor_internetconnection
    show_alert("Searching for a network...")
    try:
        # Step 2: List available networks with mode=bssid
        result = subprocess.run(
            ["netsh", "wlan", "show", "networks", "mode=bssid"],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Get and print the output
        output = result.stdout
        print("\nAvailable Wi-Fi Networks:")
        print(output)

        # Parse SSIDs and signal strengths
        lines = output.splitlines()
        networks = {}
        current_ssid = None
        ssid_pattern = re.compile(r"SSID \d+ : (.+)")
        signal_pattern = re.compile(r"Signal\s+:\s+(\d+)%")

        for line in lines:
            ssid_match = ssid_pattern.search(line)
            if ssid_match:
                current_ssid = ssid_match.group(1).strip()
                if current_ssid not in networks:
                    networks[current_ssid] = 0  # Default signal if none found
            
            signal_match = signal_pattern.search(line)
            if signal_match and current_ssid:
                signal_strength = int(signal_match.group(1))
                networks[current_ssid] = signal_strength

        # Check if any networks were found
        if not networks:
            print("No Wi-Fi networks found.")
            show_alert("No Wi-Fi networks found.")
            network_connected = False
            check_and_connect_network()
            return

        # Print network conditions
        print("\nNetwork Conditions:")
        for ssid, signal in networks.items():
            if signal > 0:
                condition = "Good" if signal >= 65 else "Poor"
                print(f"{ssid}: Signal = {signal}%, Condition = {condition}")
            else:
                print(f"{ssid}: Signal strength not available")

        # Define and print the target keywords being checked
        target_names = ["taiwo", "kenny", "techknowdge"]
        print(f"\nChecking for networks containing: {', '.join(target_names)}")

        # Filter networks with "taiwo", "kenny", or "techknowdge" (case-insensitive)
        matching_networks = {
            ssid: signal for ssid, signal in networks.items()
            if any(name.lower() in ssid.lower() for name in target_names)
        }

        if not matching_networks:
            print("No networks found matching the specified criteria.")
            show_alert("Taiwothrone and iamkennyking's wifis are not available")
            network_connected = False
            check_and_connect_network()
            return

        # Sort matching networks by signal strength (highest first)
        sorted_networks = sorted(matching_networks.items(), key=lambda x: x[1], reverse=True)
        good_threshold = 65  # Threshold as per your request

        # Print matching networks and their conditions
        print("\nMatching Networks:")
        for ssid, signal in sorted_networks:
            if signal > 0:
                condition = "Good" if signal >= 75 else "Poor"
                print(f"{ssid}: Signal = {signal}%, Condition = {condition}")
            else:
                print(f"{ssid}: Signal strength not available")

        # Check if any meet the 75% threshold
        good_networks = [net for net in sorted_networks if net[1] >= good_threshold]
        if not good_networks:
            print("Network conditions not met (all below 65%).")
            network_connected = False
            check_and_connect_network()
            return

        # Try connecting to good networks in order
        for best_network, best_signal in good_networks:
            print(f"\nAttempting to connect to '{best_network}' (Signal = {best_signal}%)")
            show_alert(f"Connecting to {best_network}'s wifi")

            # Connect to the chosen network
            connect_command = ["netsh", "wlan", "connect", f"name={best_network}"]
            connect_result = subprocess.run(
                connect_command,
                capture_output=True,
                text=True
            )
            
            if connect_result.returncode == 0:
                print(f"Connection request to '{best_network}' sent.")
                time.sleep(2)  # Wait for connection to establish
                verify_result = subprocess.run(
                    ["netsh", "wlan", "show", "interfaces"],
                    capture_output=True,
                    text=True,
                    check=True
                )
                verify_output = verify_result.stdout
                verified_network = None
                for line in verify_output.splitlines():
                    if "SSID" in line and "BSSID" not in line:
                        verified_network = line.split(":")[1].strip()
                        break
                
                if verified_network == best_network:
                    print(f"Successfully connected to '{best_network}'")
                    show_alert(f"Successfully connected to {best_network}")
                    currently_connected = False
                    network_connected = True
                    good_secured_connection = True
                    print("good and secured network")
                    return  # Exit after successful connection
                else:
                    print(f"connection to '{best_network} failed'. Current network: {verified_network}")
                    show_alert(f"Connection to {best_network} failed. Trying next network...")
                    network_connected = False
                    check_and_connect_network()
            else:
                print(f"Connection to '{best_network}' failed: {connect_result.steerr}")
                show_alert(f"Connection to {best_network} failed. Trying next network...")
                network_connected = False
                check_and_connect_network()

        # If all attempts fail
        print("All connection attempts failed.")
        network_connected = False
        check_and_connect_network()

    except subprocess.CalledProcessError as e:
        print(f"Error running netsh: {e}")
        show_alert(f"Error running netsh: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
        show_alert(f"Unexpected error: {e}")
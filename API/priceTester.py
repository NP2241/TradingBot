import subprocess
import sys
import os
import time
from datetime import datetime, timedelta
import pytz

def get_script_dir():
    return os.path.dirname(os.path.realpath(__file__))

def is_market_open():
    now = datetime.now(pytz.timezone('US/Eastern'))
    # US market hours (9:30 AM to 4:00 PM EST)
    market_open_time = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close_time = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open_time <= now <= market_close_time

def run_setup_database_single_day(print_all=False):
    # Full path to the Python interpreter
    python_path = sys.executable  # Use the current Python executable

    # Define the path to setup_database.py
    script_dir = get_script_dir()
    setup_database_path = os.path.join(script_dir, "setupDatabase.py")

    # Define the command and arguments for a single day with hourly interval
    command = [python_path, setup_database_path, "AAPL", "yes", "2024-06-24", "1h"]

    # Run the setupDatabase.py script with the specified arguments and capture the output
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Split the output into lines
    output_lines = result.stdout.split('\n')

    print("Testing setupDatabase.py for single day (2024-06-24) with hourly data:")
    for line in output_lines:
        if "Database saved at:" in line:
            print(line)

def run_setup_database_date_range(print_all=False):
    # Full path to the Python interpreter
    python_path = sys.executable  # Use the current Python executable

    # Define the path to setup_database.py
    script_dir = get_script_dir()
    setup_database_path = os.path.join(script_dir, "setupDatabase.py")

    # Define the command and arguments for a date range with hourly interval
    command = [python_path, setup_database_path, "AAPL", "yes", "2024-06-21", "2024-06-24", "1h"]

    # Run the setupDatabase.py script with the specified arguments and capture the output
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Split the output into lines
    output_lines = result.stdout.split('\n')

    print("\nTesting setupDatabase.py for date range (2024-06-21 to 2024-06-24) with hourly data:")
    for line in output_lines:
        if "Database saved at:" in line:
            print(line)

def run_real_time_test(symbol="AAPL", interval="1m", duration_minutes=3):
    if not is_market_open():
        print("Cannot run the real-time test since the market is closed.")
        return

    # Full path to the Python interpreter
    python_path = sys.executable  # Use the current Python executable

    # Define the path to setup_database.py
    script_dir = get_script_dir()
    setup_database_path = os.path.join(script_dir, "setupDatabase.py")

    # Define the command and arguments for real-time data collection
    command = [python_path, setup_database_path, symbol, "no", datetime.now().strftime('%Y-%m-%d'), interval]

    print("Testing setupDatabase.py for real-time data collection with 1-minute intervals:")

    # Run the setupDatabase.py script in real-time and capture the output
    start_time = datetime.now()
    end_time = start_time + timedelta(minutes=duration_minutes)
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    try:
        while datetime.now() < end_time:
            remaining_time = end_time - datetime.now()
            remaining_minutes, remaining_seconds = divmod(remaining_time.seconds, 60)
            print(f"\rTime remaining: {remaining_minutes:02}:{remaining_seconds:02}", end='')
            time.sleep(1)  # Sleep for 1 second intervals to update the countdown timer

        # Terminate the process after the duration
        proc.terminate()
        print("\nReal-time test completed.")
    except KeyboardInterrupt:
        proc.terminate()
        print("\nReal-time test interrupted.")

    # Print the final output from the subprocess
    stdout, stderr = proc.communicate()
    print(f"\nOutput from setupDatabase.py:\n{stdout}")
    if stderr:
        print(f"\nErrors:\n{stderr}")

if __name__ == "__main__":
    # Check for command line arguments
    if len(sys.argv) > 1 and sys.argv[1].lower() == 'all':
        run_setup_database_single_day(print_all=True)
        run_setup_database_date_range(print_all=True)
        run_real_time_test()
    else:
        run_setup_database_single_day()
        run_setup_database_date_range()
        run_real_time_test()

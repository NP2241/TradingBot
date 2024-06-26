import subprocess
import sys
import os
import time
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv

def load_env():
    env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'paths.env'))
    if not os.path.exists(env_path):
        raise FileNotFoundError(f".env file not found at {env_path}")
    load_dotenv(dotenv_path=env_path)

def get_script_dir():
    return os.path.dirname(os.path.realpath(__file__))

def get_root_dir():
    root_dir = os.getenv('ROOT_DIR')
    if not root_dir:
        raise ValueError("ROOT_DIR environment variable not set.")
    return root_dir

def is_market_open():
    now = datetime.now(pytz.timezone('US/Eastern'))
    # US market hours (9:30 AM to 4:00 PM EST)
    market_open_time = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close_time = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open_time <= now <= market_close_time

def delete_existing_db_files():
    root_dir = get_root_dir()
    db_files = [
        os.path.join(root_dir, "data/AAPL_2024.06.24_1h.db"),
        os.path.join(root_dir, "data/AAPL_2024.06.21_2024.06.24_1h.db")
    ]
    for db_file in db_files:
        if os.path.exists(db_file):
            print(f"\nDeleting existing database file: {db_file}")
            os.remove(db_file)

def run_setup_database_single_day(print_all=False):
    python_path = sys.executable  # Use the current Python executable
    root_dir = get_root_dir()
    setup_database_path = os.path.join(root_dir, "API/setupDatabase.py")

    command = [python_path, setup_database_path, "AAPL", "yes", "2024-06-24", "1h"]

    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    output_lines = result.stdout.split('\n')

    print("\nTesting setupDatabase.py for single day (2024-06-24) with hourly data:")
    for line in output_lines:
        if "Database saved at:" in line:
            print(line)

def run_setup_database_date_range(print_all=False):
    python_path = sys.executable  # Use the current Python executable
    root_dir = get_root_dir()
    setup_database_path = os.path.join(root_dir, "API/setupDatabase.py")

    command = [python_path, setup_database_path, "AAPL", "yes", "2024-06-21", "2024-06-24", "1h"]

    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    output_lines = result.stdout.split('\n')

    print("\nTesting setupDatabase.py for date range (2024-06-21 to 2024-06-24) with hourly data:")
    for line in output_lines:
        if "Database saved at:" in line:
            print(line)

def run_real_time_test(symbol="AAPL", interval="1m", duration_minutes=3):
    if not is_market_open():
        print("\nCannot run the real-time test since the market is closed.\n")
        return

    python_path = sys.executable  # Use the current Python executable
    root_dir = get_root_dir()
    setup_database_path = os.path.join(root_dir, "API/setupDatabase.py")

    command = [python_path, setup_database_path, symbol, "no", datetime.now().strftime('%Y-%m-%d'), interval]

    print("\nTesting setupDatabase.py for real-time data collection with 1-minute intervals:")

    start_time = datetime.now()
    end_time = start_time + timedelta(minutes=duration_minutes)
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    try:
        while datetime.now() < end_time:
            remaining_time = end_time - datetime.now()
            remaining_minutes, remaining_seconds = divmod(remaining_time.seconds, 60)
            print(f"\rTime remaining: {remaining_minutes:02}:{remaining_seconds:02}", end='')
            time.sleep(1)

        proc.terminate()
        print("\nReal-time test completed.")
    except KeyboardInterrupt:
        proc.terminate()
        print("\nReal-time test interrupted.")

    stdout, stderr = proc.communicate()
    print(f"\nOutput from setupDatabase.py:\n{stdout}")
    if stderr:
        print(f"\nErrors:\n{stderr}")

if __name__ == "__main__":
    load_env()
    delete_existing_db_files()
    if len(sys.argv) > 1 and sys.argv[1].lower() == 'all':
        run_setup_database_single_day(print_all=True)
        run_setup_database_date_range(print_all=True)
        run_real_time_test()
    else:
        run_setup_database_single_day()
        run_setup_database_date_range()
        run_real_time_test()

import os
import subprocess
import sys
from datetime import datetime, timedelta

def get_script_dir():
    return os.path.dirname(os.path.realpath(__file__))

def generate_db_filename(symbol, start_date, end_date, interval):
    interval_str = interval.replace(' ', '').replace(':', '').replace('-', '')
    return f"{symbol}_{start_date.replace('-', '.')}_{end_date.replace('-', '.')}_{interval_str}.db"

def main():
    if len(sys.argv) != 5:
        print("Usage: python stockAnalysis.py <symbol> <start_date> <end_date> <interval>")
        sys.exit(1)

    symbol = sys.argv[1].upper()
    start_date = sys.argv[2].strip()
    end_date = sys.argv[3].strip()
    interval = sys.argv[4].strip()

    db_filename = generate_db_filename(symbol, start_date, end_date, interval)
    db_path = os.path.join(get_script_dir(), '..', 'data', db_filename)
    relative_db_path = os.path.join('TradingBot', 'data', db_filename)

    if os.path.exists(db_path):
        print(f"Database already exists at: {relative_db_path}")
    else:
        setup_database_path = os.path.join(get_script_dir(), '..', 'DatabaseSetup', 'setupDatabase.py')
        command = [sys.executable, setup_database_path, symbol, "yes", start_date, end_date, interval]
        subprocess.run(command)
        print(f"Database created at: {relative_db_path}")

if __name__ == "__main__":
    main()

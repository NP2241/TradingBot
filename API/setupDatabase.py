import sqlite3
import os
import subprocess
import sys
from datetime import datetime
import pytz

def create_database(db_path):
    # Create the directory for the database if it doesn't exist
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Create table
    c.execute('''CREATE TABLE IF NOT EXISTS stock_prices
                 (stock_name TEXT, stock_price REAL, price_time TEXT, price_date TEXT)''')

    # Save (commit) the changes and close the connection
    conn.commit()
    conn.close()

def populate_database(db_path, symbol, historical, start_date, end_date=None, interval='1h'):
    # Call priceTracker.py and capture its output
    command = [sys.executable, "priceTracker.py", symbol, "yes" if historical else "no", start_date]
    if end_date:
        command.append(end_date)
    command.append(interval)

    print(f"Running command: {' '.join(command)}")

    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    output_lines = result.stdout.strip().split('\n')

    print(f"Output from priceTracker.py:\n{result.stdout}")

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Insert data into the database
    for line in output_lines:
        print(f"Processing line: {line}")
        if line.startswith('(') and line.endswith(')'):
            try:
                record = eval(line)
                print(f"Inserting record: {record}")
                c.execute("INSERT INTO stock_prices (stock_name, stock_price, price_time, price_date) VALUES (?, ?, ?, ?)",
                          record)
                conn.commit()
                print(f"Added entry to the database: {record}")
            except Exception as e:
                print(f"Error inserting record {line}: {e}")

    conn.close()

def generate_db_filename(symbol, start_date, end_date=None, interval='1h'):
    interval_str = interval.replace(' ', '').replace(':', '').replace('-', '')
    if end_date:
        return f"{symbol}_{start_date.replace('-', '.')}_{end_date.replace('-', '.')}_{interval_str}.db"
    else:
        return f"{symbol}_{start_date.replace('-', '.')}_{interval_str}.db"

def populate_real_time_database(db_path, symbol, interval='1m'):
    # Call priceTracker.py and capture its output in real-time
    command = [sys.executable, "priceTracker.py", symbol, "no", datetime.now().strftime('%Y-%m-%d'), interval]

    print(f"Running real-time command: {' '.join(command)}")

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    result = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    for line in result.stdout:
        line = line.strip()
        print(f"Output from priceTracker: {line}")
        if line.startswith('(') and line.endswith(')'):
            try:
                record = eval(line)
                print(f"Inserting real-time record: {record}")
                c.execute("INSERT INTO stock_prices (stock_name, stock_price, price_time, price_date) VALUES (?, ?, ?, ?)",
                          record)
                conn.commit()
                print(f"Added entry to the database: {record}")
            except Exception as e:
                print(f"Error inserting real-time record {line}: {e}")

    conn.close()

def is_market_open():
    now = datetime.now(pytz.timezone('US/Eastern'))
    # US market hours (9:30 AM to 4:00 PM EST)
    market_open_time = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close_time = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open_time <= now <= market_close_time

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: python setupDatabase.py <symbol> <yes|no> <start_date> <interval> [end_date]")
        sys.exit(1)

    symbol = sys.argv[1].upper()
    historical = sys.argv[2].strip().lower() == 'yes'
    start_date = sys.argv[3].strip()
    interval = sys.argv[4].strip()
    end_date = sys.argv[5].strip() if len(sys.argv) > 5 else None

    db_filename = generate_db_filename(symbol, start_date, end_date, interval)
    db_path = os.path.abspath(f'../data/{db_filename}')

    create_database(db_path)

    if not historical:
        if not is_market_open():
            print("The market is currently closed.")
            sys.exit(1)
        populate_real_time_database(db_path, symbol, interval)
    else:
        populate_database(db_path, symbol, historical, start_date, end_date, interval)

    print(f"Database saved at: {db_path}")

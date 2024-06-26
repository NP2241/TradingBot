import sqlite3
import os
import subprocess
import sys

def create_database(db_path):
    print(f"Creating database directory: {os.path.dirname(db_path)}")
    # Create the directory for the database if it doesn't exist
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    print(f"Connecting to database at: {db_path}")
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Create table
    print("Creating table stock_prices")
    c.execute('''CREATE TABLE IF NOT EXISTS stock_prices
                 (stock_name TEXT, stock_price REAL, price_time TEXT, price_date TEXT)''')

    # Save (commit) the changes and close the connection
    conn.commit()
    conn.close()
    print(f"Database created at: {db_path}")

def populate_database(db_path, symbol, historical, start_date, end_date=None, interval='1h'):
    # Call priceTracker.py and capture its output
    command = [sys.executable, "priceTracker.py", symbol, "yes" if historical else "no", start_date]
    if end_date:
        command.append(end_date)
    command.append(interval)

    print(f"Running command: {' '.join(command)}")
    result = subprocess.run(command, stdout=subprocess.PIPE, text=True)
    output_lines = result.stdout.strip().split('\n')

    print(f"Connecting to database at: {db_path}")
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Insert data into the database
    for line in output_lines:
        if line.startswith('(') and line.endswith(')'):
            record = eval(line)
            c.execute("INSERT INTO stock_prices (stock_name, stock_price, price_time, price_date) VALUES (?, ?, ?, ?)",
                      record)

    conn.commit()
    conn.close()
    print(f"Database populated at: {db_path}")

def generate_db_filename(symbol, start_date, end_date=None):
    if end_date:
        return f"{symbol}_{start_date.replace('-', '.')}_{end_date.replace('-', '.')}.db"
    else:
        return f"{symbol}_{start_date.replace('-', '.')}.db"

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python setup_database.py <symbol> <yes|no> <start_date> [end_date]")
        sys.exit(1)

    symbol = sys.argv[1].upper()
    historical = sys.argv[2].strip().lower() == 'yes'
    start_date = sys.argv[3].strip()
    end_date = sys.argv[4].strip() if len(sys.argv) == 5 else None
    interval = '1h'  # Use hourly interval for testing

    db_filename = generate_db_filename(symbol, start_date, end_date)
    db_path = f'../data/{db_filename}'

    print(f"Creating database at: {db_path}")
    create_database(db_path)
    print(f"Populating database at: {db_path}")
    populate_database(db_path, symbol, historical, start_date, end_date, interval)
    print(f"Database saved at: {db_path}")

import os
import sqlite3
import subprocess
import sys
import time
import statistics

def database_exists(db_path):
    return os.path.exists(db_path)

def create_database(symbol, start_date, end_date, interval):
    script_path = os.path.join(os.path.dirname(__file__), '../DatabaseSetup/setupDatabase.py')
    command = [sys.executable, script_path, symbol, "yes", start_date, interval, end_date]
    subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

def get_db_path(symbol, start_date, end_date, interval):
    db_filename = f"{symbol}_{start_date.replace('-', '.')}_{end_date.replace('-', '.')}_{interval.replace(' ', '').replace(':', '').replace('-', '')}.db"
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data', db_filename))
    return db_path

def check_db_populated(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM stock_prices")
    count = c.fetchone()[0]
    conn.close()
    return count > 0

def calculate_stock_analysis(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT stock_price, volume FROM stock_prices")
    data = c.fetchall()
    prices = [row[0] for row in data]
    volumes = [row[1] for row in data]
    conn.close()

    if prices:
        median_price = statistics.median(prices)
        std_dev = statistics.stdev(prices)
        volatility_index = (std_dev / median_price) * 100
        print(f"Volatility Index: {volatility_index}")
    else:
        print("No data available to calculate the stock analysis.")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python stockAnalysis.py <symbol> <start_date> <end_date> <interval>")
        sys.exit(1)

    symbol = sys.argv[1].upper()
    start_date = sys.argv[2].strip()
    end_date = sys.argv[3].strip()
    interval = sys.argv[4].strip()

    db_path = get_db_path(symbol, start_date, end_date, interval)

    if not database_exists(db_path):
        print("Database does not exist. Creating database...")
        create_database(symbol, start_date, end_date, interval)

        # Wait until the database is created and populated
        while not (database_exists(db_path) and check_db_populated(db_path)):
            time.sleep(0.1)

    calculate_stock_analysis(db_path)

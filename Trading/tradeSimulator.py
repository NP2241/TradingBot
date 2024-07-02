import os
import sys
import sqlite3
import time
import subprocess

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../DataAnalysis')))

from stockAnalysis import calculate_buy_index, get_db_path, database_exists, create_database, check_db_populated, calculate_stock_analysis
from stockMetrics import calculate_bollinger_bands

def create_single_day_database(symbol, date, interval):
    script_path = os.path.join(os.path.dirname(__file__), '../DatabaseSetup/setupDatabase.py')
    command = [sys.executable, script_path, symbol, "yes", date, interval]
    subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

def check_table_exists(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stock_prices'")
    result = c.fetchone()
    conn.close()
    return result is not None

def simulate_trading(symbol, start_date, end_date, interval, simulate_date):
    # Ensure the database for the range exists
    db_path = get_db_path(symbol, start_date, end_date, interval)
    if not database_exists(db_path):
        print(f"Database for {symbol} does not exist. Creating database...")
        create_database(symbol, start_date, end_date, interval)

        while not (database_exists(db_path) and check_db_populated(db_path)):
            time.sleep(0.1)

    volatility_index, metrics = calculate_stock_analysis(db_path)
    if volatility_index is not None and metrics is not None:
        current_price = metrics['moving_average_value']  # Assuming the current price is the latest moving average
        buy_index = calculate_buy_index(volatility_index, metrics, current_price)
        print(f"Volatility Index: {volatility_index}")
        print(f"Buy Index: {buy_index}")
    else:
        print("No data available to calculate the stock analysis.")
        return

    lower_band = metrics['lower_band']
    upper_band = metrics['upper_band']

    # Create the single day database if it doesn't exist
    single_day_db_path = get_db_path(symbol, simulate_date, interval=interval)
    if not database_exists(single_day_db_path):
        print(f"Creating single-day database for {symbol} on {simulate_date}...")
        create_single_day_database(symbol, simulate_date, interval)

    while not (database_exists(single_day_db_path) and check_table_exists(single_day_db_path)):
        time.sleep(0.1)

    # Read the data from the single day database
    conn = sqlite3.connect(single_day_db_path)
    c = conn.cursor()
    c.execute("SELECT stock_price, volume, price_date, price_time FROM stock_prices WHERE price_date = ?", (simulate_date,))
    stock_data = c.fetchall()
    conn.close()

    if not stock_data:
        print(f"No data available for {symbol} on {simulate_date} even after creation.")
        return

    cash = 10000  # Starting cash in USD
    shares = 0

    for price, volume, date, time_ in stock_data:
        if price <= lower_band:  # Buy condition
            if cash >= price:
                shares += 1
                cash -= price
                print(f"Bought 1 share of {symbol} at {price} on {date} {time_}")
        elif price >= upper_band and shares > 0:  # Sell condition
            shares -= 1
            cash += price
            print(f"Sold 1 share of {symbol} at {price} on {date} {time_}")

    print(f"Ending cash: {cash}")
    print(f"Ending shares of {symbol}: {shares}")
    print(f"Lower Band: {lower_band}")
    print(f"Upper Band: {upper_band}")

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: python tradeSimulator.py <symbol> <start_date> <end_date> <interval> <simulate_date>")
        sys.exit(1)

    symbol = sys.argv[1].upper()
    start_date = sys.argv[2].strip()
    end_date = sys.argv[3].strip()
    interval = sys.argv[4].strip()
    simulate_date = sys.argv[5].strip()

    simulate_trading(symbol, start_date, end_date, interval, simulate_date)

import requests
import sqlite3
import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
import pytz
import holidays

# Load environment variables from paths.env file
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../paths.env'))
load_dotenv(dotenv_path)
POLYGON_API_KEYS = os.getenv('POLYGON_API_KEYS').split(',')

MARKET_OPEN_TIME = 9 * 60 + 30  # 9:30 AM in minutes
MARKET_CLOSE_TIME = 16 * 60     # 4:00 PM in minutes

def create_database(db_path):
    """
    Create the stock prices database if it doesn't exist.
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS stock_prices
                 (stock_name TEXT, stock_price REAL, volume INTEGER, price_time TEXT, price_date TEXT)''')
    conn.commit()
    conn.close()

def fetch_historical_data(symbol, date, api_key):
    """
    Fetch historical data for a given symbol on a single day.
    """
    start_date = date.strftime('%Y-%m-%d')
    end_date = (date + timedelta(days=1)).strftime('%Y-%m-%d')
    url = f'https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{start_date}/{end_date}'
    params = {'apiKey': api_key}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json().get('results', [])

def filter_market_hours(data):
    """
    Filter data to include only market hours.
    """
    market_data = []
    for entry in data:
        ts = entry['t'] // 1000
        dt = datetime.fromtimestamp(ts)
        total_minutes = dt.hour * 60 + dt.minute
        if MARKET_OPEN_TIME <= total_minutes <= MARKET_CLOSE_TIME:
            market_data.append(entry)
    return market_data

def convert_to_12hr_format(time_str):
    """
    Convert a time string to 12-hour format.
    """
    dt = datetime.strptime(time_str, '%H:%M:%S')
    return dt.strftime('%I:%M:%S %p')

def record_exists(c, symbol, price, volume, time, date):
    """
    Check if a record already exists in the database to prevent duplicates.
    """
    query = """SELECT 1 FROM stock_prices WHERE 
               stock_name = ? AND stock_price = ? AND volume = ? AND price_time = ? AND price_date = ?"""
    c.execute(query, (symbol, price, volume, time, date))
    return c.fetchone() is not None

def populate_database(db_path, symbol, start_date=None, end_date=None, interval='1m'):
    """
    Populate the database with historical stock prices.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    if not start_date:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=2*365)  # Default to 2 years of data if no start date is provided
    else:
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d') if end_date else datetime.now()

    pst = pytz.timezone('America/Los_Angeles')
    total_api_calls = 0
    start_time = time.time()
    key_index = 0  # Start with the first key
    api_switch_printed = False  # Flag to track when the "Switching API key" message is printed

    # Iterate through each day individually
    current_date = start_date
    while current_date <= end_date:
        # Skip weekends and holidays
        if current_date.weekday() >= 5 or current_date in holidays.US():
            current_date += timedelta(days=1)
            continue

        try:
            api_key = POLYGON_API_KEYS[key_index]
            data = fetch_historical_data(symbol, current_date, api_key)
            total_api_calls += 1

            # Reset the switch flag after a successful API call
            api_switch_printed = False

            # Filter data to include only market hours
            market_data = filter_market_hours(data)

            # Log if no data was fetched for a trading day
            if len(market_data) == 0:
                print(f"No data fetched for {symbol} on {current_date.strftime('%Y-%m-%d')}. This could indicate a problem with the data or API.")

            # Insert the data into the database, checking for duplicates first
            for entry in market_data:
                ts = entry['t'] // 1000
                dt = datetime.fromtimestamp(ts)
                price_time = convert_to_12hr_format(dt.strftime('%H:%M:%S'))
                price_date = dt.strftime('%Y-%m-%d')

                # Check if the entry already exists to prevent duplicates
                if not record_exists(c, symbol, entry['c'], entry['v'], price_time, price_date):
                    try:
                        c.execute("INSERT INTO stock_prices (stock_name, stock_price, volume, price_time, price_date) VALUES (?, ?, ?, ?, ?)",
                                  (symbol, entry['c'], entry['v'], price_time, price_date))
                    except sqlite3.Error as e:
                        print(f"Failed to insert data for {symbol} on {price_date}: {e}")

            conn.commit()

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Too Many Requests
                if not api_switch_printed:
                    print(f"\nRate limit exceeded. Switching API key... (Date: {current_date.strftime('%Y-%m-%d')})")
                    api_switch_printed = True  # Set the flag to True to avoid repeated prints
                key_index = (key_index + 1) % len(POLYGON_API_KEYS)
                continue  # Retry the current date with the new API key
            else:
                print(f"HTTP Error for {current_date.strftime('%Y-%m-%d')}: {e}")
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {current_date.strftime('%Y-%m-%d')}: {e}")

        # Log progress
        progress = ((current_date - start_date).days / (end_date - start_date).days) * 100
        elapsed_time = time.time() - start_time
        print(f"\rProgress: {progress:.2f}% - Total API Calls: {total_api_calls} - Time Elapsed: {elapsed_time:.2f}s", end='')

        # Move to the next day
        current_date += timedelta(days=1)

    conn.close()
    print("\nDatabase population complete.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python historicalDatabase.py <symbol>")
        sys.exit(1)

    symbol = sys.argv[1].upper()

    db_filename = f"{symbol}_historical_data.db"
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data', db_filename))

    create_database(db_path)
    populate_database(db_path, symbol)
    print(f"\nDatabase saved at: {db_filename}")

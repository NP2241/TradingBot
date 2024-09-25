import os
import sqlite3
import requests
import sys
from datetime import datetime, timedelta
import pytz
import holidays
from dotenv import load_dotenv

# Add custom path to stockAnalysis
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../DataAnalysis')))
from stockAnalysis import get_db_path

# Load environment variables from paths.env file
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../paths.env'))
load_dotenv(dotenv_path)

# Get multiple Tiingo API keys from the .env file (comma-separated)
TIINGO_API_KEYS = os.getenv('TIINGO_API_KEYS').split(',')

# Base URL for Tiingo REST API
BASE_URL = "https://api.tiingo.com/iex"

# Set timezone for New York
ny_tz = pytz.timezone('America/New_York')

# US Trading holidays
us_holidays = holidays.US()

# Rotate between API keys
def get_next_api_key():
    while True:
        for key in TIINGO_API_KEYS:
            yield key

api_key_gen = get_next_api_key()

# Handle rate limits by switching keys
def handle_rate_limit():
    print("Rate limit exceeded. Switching to next API key...")
    next_key = next(api_key_gen)
    return next_key

# Create the SQLite database
def create_database(db_path):
    if os.path.exists(db_path):
        os.remove(db_path)

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.execute('''
        CREATE TABLE IF NOT EXISTS stock_prices (
            stock_name TEXT,
            stock_price REAL,
            volume INTEGER,
            price_time TEXT,
            price_day TEXT
        )
    ''')

    conn.commit()
    conn.close()

# Fetch intraday data for a specified date range
def fetch_intraday_data_for_date_range(symbol, start_date, end_date):
    url = f"{BASE_URL}/{symbol}/prices?startDate={start_date}&endDate={end_date}&resampleFreq=1min&columns=open,high,low,close,volume"

    headers = {'Content-Type': 'application/json'}
    current_api_key = next(api_key_gen)

    while True:
        try:
            response = requests.get(f"{url}&token={current_api_key}", headers=headers)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                current_api_key = handle_rate_limit()
                continue
            else:
                print(f"Error fetching data from {start_date} to {end_date}: {response.status_code} - {response.text}")
                return []
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return []

# Insert data into the database
def insert_data_by_day(db_path, symbol, data):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    data_by_day = {}

    for entry in data:
        stock_name = symbol
        stock_price = entry.get('close')
        volume = entry.get('volume')
        date = entry.get('date')

        # Check for empty data and skip invalid entries
        if not stock_price or not volume or not date:
            continue

        ny_time = convert_to_ny_time(date)
        price_time = ny_time.strftime('%I:%M %p')
        price_day = ny_time.strftime('%Y-%m-%d')

        if price_day not in data_by_day:
            data_by_day[price_day] = []
        data_by_day[price_day].append((stock_name, stock_price, volume, price_time, price_day))

    # Insert data day by day into the database
    for day in sorted(data_by_day.keys()):
        print(f"\rInserting data for {day}", end="")
        entries = data_by_day[day]
        for entry in entries:
            try:
                c.execute("INSERT INTO stock_prices (stock_name, stock_price, volume, price_time, price_day) VALUES (?, ?, ?, ?, ?)", entry)
            except Exception as e:
                print(f"Error inserting data: {e}")

    conn.commit()
    conn.close()

# Convert UTC timestamp to New York time
def convert_to_ny_time(utc_time_str):
    utc_time = datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    utc_time = utc_time.replace(tzinfo=pytz.utc)
    ny_time = utc_time.astimezone(ny_tz)
    return ny_time

# Fetch historical data with progressive intervals (year → month → week → day)
def fetch_historical_data(symbol, start_date=None, end_date=None):
    # Default to last 7 years minus one day if no date range is provided
    if not start_date or not end_date:
        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date - timedelta(days=365 * 7)

    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    # Use the same database path for both historical data and simulation
    db_path = get_db_path(symbol, start_date_str, end_date_str, interval='1m')

    create_database(db_path)

    current_date = start_date
    while current_date <= end_date:
        # Calculate the remaining time between current date and end date
        remaining_time = (end_date - current_date).days

        # If more than 1 year remains, fetch data in 1-year intervals
        if remaining_time > 365:
            interval_days = 365
        # If between 6 months and 1 year remains, fetch 6 months of data
        elif remaining_time > 182:
            interval_days = 182
        # If between 3 months and 6 months remains, fetch 3 months of data
        elif remaining_time > 90:
            interval_days = 90
        # If between 1 month and 3 months remains, fetch 1 month of data
        elif remaining_time > 30:
            interval_days = 30
        # If between 1 week and 1 month remains, fetch 1 week of data
        elif remaining_time > 7:
            interval_days = 7
        # If 1 week or less remains, fetch 1 day of data at a time
        else:
            interval_days = 1

        next_date = current_date + timedelta(days=interval_days)
        if next_date > end_date:
            next_date = end_date

        current_date_str = current_date.strftime('%Y-%m-%d')
        next_date_str = next_date.strftime('%Y-%m-%d')

        print(f"Attempting to fetch data from {current_date_str} to {next_date_str} (Interval: {interval_days} days)")
        data = fetch_intraday_data_for_date_range(symbol, current_date_str, next_date_str)

        if data:
            insert_data_by_day(db_path, symbol, data)
        else:
            print(f"\nNo data found for {current_date_str}. Trying a smaller range...")

        # Move to the next valid trading day
        current_date = next_date + timedelta(days=1)

    print(f"\nData inserted for the range: {start_date_str} to {end_date_str}")
    print(f"\nData saved in database: {db_path}")

if __name__ == "__main__":
    if len(sys.argv) == 2:
        symbol = sys.argv[1].upper()
        fetch_historical_data(symbol)
    elif len(sys.argv) == 4:
        symbol = sys.argv[1].upper()
        start_date = datetime.strptime(sys.argv[2], '%Y-%m-%d')
        end_date = datetime.strptime(sys.argv[3], '%Y-%m-%d')
        fetch_historical_data(symbol, start_date, end_date)
    else:
        print("Usage: python historicalDatabase.py <symbol> [<start_date> <end_date>]")

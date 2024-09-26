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
                data = response.json()

                if data:
                    return data
                else:
                    return []
            elif response.status_code == 429:
                current_api_key = handle_rate_limit()
                continue
            else:
                return []
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return []

# Insert data into the database with validation to avoid empty entries
def insert_data_by_day(db_path, symbol, data):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    data_by_day = {}

    for entry in data:
        stock_name = symbol
        stock_price = entry.get('close')
        volume = entry.get('volume')
        date = entry.get('date')

        # Validate the data before inserting
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
        entries = data_by_day[day]
        for entry in entries:
            if all(entry):
                try:
                    c.execute("INSERT INTO stock_prices (stock_name, stock_price, volume, price_time, price_day) VALUES (?, ?, ?, ?, ?)", entry)
                except Exception as e:
                    pass

    conn.commit()
    conn.close()

# Convert UTC timestamp to New York time
def convert_to_ny_time(utc_time_str):
    utc_time = datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    utc_time = utc_time.replace(tzinfo=pytz.utc)
    ny_time = utc_time.astimezone(ny_tz)
    return ny_time

# Check if a date is a trading day (not a weekend or holiday)
def is_trading_day(date):
    if date.weekday() >= 5 or date in us_holidays:
        return False
    return True

def fetch_historical_data(symbol):
    # Adjust start date to the actual first available date based on data
    adjusted_start_date = adjust_start_date(symbol)

    # Convert adjusted_start_date to a datetime object for consistency in comparisons
    current_date = datetime.combine(adjusted_start_date, datetime.min.time())

    # Use the same database path for both historical data and simulation
    end_date = datetime.now()  # Ensure both are datetime objects
    db_path = get_db_path(symbol, adjusted_start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), interval='1m')

    create_database(db_path)

    # Ensure the first entry is fetched and inserted
    print(f"Fetching first data point starting from {current_date}")
    first_data = fetch_intraday_data_for_date_range(symbol, current_date.strftime('%Y-%m-%d'), current_date.strftime('%Y-%m-%d'))

    if first_data:
        insert_data_by_day(db_path, symbol, first_data)
        print(f"First data point inserted: {first_data[0]}")

    # Start with yearly intervals and adjust the interval based on remaining time to end_date
    while current_date <= end_date:
        remaining_time = (end_date - current_date).days

        # Adjust intervals based on remaining time to the end date
        if remaining_time > 365:
            interval_days = 365  # 1 year
        elif remaining_time > 182:  # Between 6 months and 1 year
            interval_days = 182  # 6 months
            print("Switching to 6-month intervals.")
        elif remaining_time > 90:  # Between 3 months and 6 months
            interval_days = 90  # 3 months
            print("Switching to 3-month intervals.")
        elif remaining_time > 30:  # Between 1 month and 3 months
            interval_days = 30  # 1 month
            print("Switching to 1-month intervals.")
        elif remaining_time > 7:  # Between 1 week and 1 month
            interval_days = 7  # 1 week
            print("Switching to 1-week intervals.")
        else:
            interval_days = 1  # 1 day
            print("Switching to 1-day intervals.")

        next_date = current_date + timedelta(days=interval_days)

        if next_date > end_date:
            next_date = end_date

        current_date_str = current_date.strftime('%Y-%m-%d')
        next_date_str = next_date.strftime('%Y-%m-%d')

        print(f"Fetching data from {current_date_str} to {next_date_str} (Interval: {interval_days} days)")
        data = fetch_intraday_data_for_date_range(symbol, current_date_str, next_date_str)

        if data:
            insert_data_by_day(db_path, symbol, data)
            print(f"Data inserted for {current_date_str} to {next_date_str}.")
        else:
            break

        current_date = next_date + timedelta(days=1)  # Move to the next valid day

    print(f"\nAll available data inserted for {symbol}.")
    print(f"\nData saved in database: {db_path}")

# Adjust start date to the first available date
def adjust_start_date(symbol):
    start_date = datetime(2000, 1, 1)

    while True:
        end_date = start_date + timedelta(days=365)  # 1 year interval
        print(f"Checking for data from {start_date.year}")
        data = fetch_intraday_data_for_date_range(symbol, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

        if data:
            first_entry = data[0]
            actual_start_date = datetime.strptime(first_entry['date'], "%Y-%m-%dT%H:%M:%S.%fZ").date()
            print(f"First available data found starting from {actual_start_date}. First entry: {first_entry}")
            return actual_start_date  # Return as a date object
        else:
            start_date = end_date  # Move to the next year



if __name__ == "__main__":
    if len(sys.argv) == 2:
        symbol = sys.argv[1].upper()
        fetch_historical_data(symbol)
    else:
        print("Usage: python historicalDatabase.py <symbol>")

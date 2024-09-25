import sqlite3
import os
import sys
from datetime import datetime, timedelta
import time
import requests
import pytz
import holidays  # Import for U.S. market holidays
from dotenv import load_dotenv

# Load environment variables from paths.env file
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../paths.env'))
load_dotenv(dotenv_path)

# Get Tiingo API key from the .env file
TIINGO_API_KEY = os.getenv('TIINGO_API_KEY')

# Base URL for Tiingo REST API
BASE_URL = "https://api.tiingo.com/iex"

# Set the timezone for New York
ny_tz = pytz.timezone('America/New_York')

# Get U.S. market holidays
us_holidays = holidays.US(years=range(1990, datetime.now().year + 1))

# Function to create the database
def create_database(db_path):
    if os.path.exists(db_path):
        os.remove(db_path)  # Remove the existing database to avoid conflicts

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Create table with correct data types
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

# Function to fetch historical intraday data from Tiingo for a specific date range
def fetch_intraday_data_for_date_range(symbol, start_date, end_date):
    # API endpoint for historical intraday prices with 1-minute intervals for the given date range
    url = f"{BASE_URL}/{symbol}/prices?startDate={start_date}&endDate={end_date}&resampleFreq=1min&columns=open,high,low,close,volume&token={TIINGO_API_KEY}"

    # Set up request headers (no token here as it's in the URL)
    headers = {
        'Content-Type': 'application/json'
    }

    print(f"Fetching data for {symbol} from {start_date} to {end_date}...")

    try:
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            if data:
                print(f"Fetched {len(data)} records from {start_date} to {end_date}")
                return data
            else:
                print(f"No data found for {start_date} to {end_date}")
                return []
        else:
            print(f"Error fetching data from {start_date} to {end_date}: {response.status_code} - {response.text}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the request: {e}")
        return []

# Convert UTC timestamp to New York time
def convert_to_ny_time(utc_time_str):
    """Convert UTC time string to New York local time (EST/EDT)."""
    utc_time = datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    utc_time = utc_time.replace(tzinfo=pytz.utc)  # Set timezone to UTC
    ny_time = utc_time.astimezone(ny_tz)  # Convert to New York time
    return ny_time

# Insert fetched data into the database
def insert_into_database(db_path, symbol, data):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Insert fetched data into the database
    for entry in data:
        try:
            stock_name = symbol
            stock_price = entry.get('close', None)
            volume = entry.get('volume', None)
            date = entry.get('date', None)

            if not stock_price or not volume or not date:
                print(f"Skipping incomplete record: {entry}")
                continue

            ny_time = convert_to_ny_time(date)  # Convert UTC to New York time
            price_time = ny_time.strftime('%I:%M %p')  # Format time to '09:31 AM'
            price_day = ny_time.strftime('%Y-%m-%d')  # Extract just the date (YYYY-MM-DD)

            # Debugging print statement to confirm data to be inserted
            print(f"Attempting to insert: {stock_name}, {stock_price}, {volume}, {price_time}, {price_day}")

            # Insert data into the database
            c.execute("INSERT INTO stock_prices (stock_name, stock_price, volume, price_time, price_day) VALUES (?, ?, ?, ?, ?)",
                      (stock_name, stock_price, volume, price_time, price_day))

        except Exception as e:
            print(f"Error inserting data: {e}")

    conn.commit()  # Ensure data is committed to the database
    conn.close()

# Fetch intraday data starting from today and going backward until no more data is available
def fetch_historical_data_until_exhausted(symbol):
    # Start at the current date
    end_date = datetime.now()

    # Set up the database
    db_filename = f"{symbol}_full_history_1min.db"
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data', db_filename))
    create_database(db_path)

    # Keep fetching data day by day until no more data is returned
    while True:
        start_date = end_date - timedelta(days=1)
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')

        # Skip weekends and U.S. holidays
        if start_date.weekday() >= 5:  # Skip Saturdays (5) and Sundays (6)
            print(f"Skipping weekend: {start_str}")
            end_date = start_date
            continue
        if start_str in us_holidays:
            print(f"Skipping U.S. holiday: {start_str}")
            end_date = start_date
            continue

        # Fetch data for the day
        daily_data = fetch_intraday_data_for_date_range(symbol, start_str, end_str)

        if not daily_data:
            print(f"Stopping data fetch. No more data available for {start_str} to {end_str}.")
            break

        # Insert the data into the database
        insert_into_database(db_path, symbol, daily_data)

        # Move the end_date back by 1 day to continue fetching earlier data
        end_date = start_date
        time.sleep(1)  # Pause between API calls to avoid rate limits

    print(f"\nData has been saved in the database: {db_filename}")

# Main function
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python historicalDatabase.py <symbol>")
        sys.exit(1)

    symbol = sys.argv[1].upper()

    # Fetch historical data going back in time until no more data is available
    fetch_historical_data_until_exhausted(symbol)

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

    try:
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            return data if data else []
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

# Insert fetched data into the database (day by day)
def insert_data_by_day(db_path, symbol, data):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Dictionary to store data by day
    data_by_day = {}

    # Organize the data by day
    for entry in data:
        try:
            stock_name = symbol
            stock_price = entry.get('close', None)
            volume = entry.get('volume', None)
            date = entry.get('date', None)

            if not stock_price or not volume or not date:
                continue

            # Convert the timestamp to New York time
            ny_time = convert_to_ny_time(date)
            price_time = ny_time.strftime('%I:%M %p')  # Time in '09:31 AM' format
            price_day = ny_time.strftime('%Y-%m-%d')  # Date in 'YYYY-MM-DD' format

            # Group data by day
            if price_day not in data_by_day:
                data_by_day[price_day] = []
            data_by_day[price_day].append((stock_name, stock_price, volume, price_time, price_day))

        except Exception as e:
            print(f"Error processing data: {e}")

    # Insert the grouped data into the database, day by day
    for day in sorted(data_by_day.keys()):  # Insert data in chronological order (oldest to newest)
        print(f"\rInserting data for {day}", end="")
        entries = data_by_day[day]
        for entry in entries:
            try:
                c.execute("INSERT INTO stock_prices (stock_name, stock_price, volume, price_time, price_day) VALUES (?, ?, ?, ?, ?)", entry)
            except Exception as e:
                print(f"Error inserting data: {e}")

    conn.commit()  # Ensure data is committed to the database
    conn.close()

# Fetch intraday data for the last 7 years minus one day
def fetch_historical_data_last_7_years(symbol):
    # Set the end_date as the day before today
    end_date = datetime.now() - timedelta(days=1)
    end_date_str = end_date.strftime('%Y-%m-%d')

    # Set the start_date as 7 years ago plus one day
    start_date = end_date - timedelta(days=365 * 7 - 1)  # 7 years ago minus 1 day
    start_date_str = start_date.strftime('%Y-%m-%d')

    # Set up the database
    db_filename = f"{symbol}_history_last_7_years.db"
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data', db_filename))
    create_database(db_path)

    current_date = start_date
    while current_date <= end_date:
        next_date = current_date + timedelta(days=365)  # Fetch data in yearly chunks
        next_date_str = next_date.strftime('%Y-%m-%d')
        current_date_str = current_date.strftime('%Y-%m-%d')

        # Skip weekends and U.S. holidays
        if current_date.weekday() >= 5 or current_date_str in us_holidays:  # Skip weekends (Saturday, Sunday) and holidays
            current_date += timedelta(days=1)
            continue

        # Ensure we don't go beyond the end_date
        if next_date > end_date:
            next_date = end_date

        # Fetch data for the range
        yearly_data = fetch_intraday_data_for_date_range(symbol, current_date_str, next_date_str)

        if not yearly_data:
            print(f"\nNo more data available from {current_date_str} to {next_date_str}.")
            break

        # Insert the data into the database day by day
        insert_data_by_day(db_path, symbol, yearly_data)

        # Move the current date forward by 1 year (or less if near the end)
        current_date = next_date
        time.sleep(1)  # Pause between API calls to avoid rate limits

    # Print overall date range when finished
    print(f"\nData inserted for the range: {start_date_str} to {end_date_str}")
    print(f"\nData has been saved in the database: {db_filename}")

# Main function (no arguments needed now)
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python historicalDatabase.py <symbol>")
        sys.exit(1)

    symbol = sys.argv[1].upper()

    # Fetch historical data for the last 7 years minus one day
    fetch_historical_data_last_7_years(symbol)

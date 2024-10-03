import os
import requests
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from paths.env file
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../paths.env'))
load_dotenv(dotenv_path)

# Get multiple Tiingo API keys from the .env file (comma-separated)
TIINGO_API_KEYS = os.getenv('TIINGO_API_KEYS').split(',')

# Base URL for Tiingo REST API
BASE_URL = "https://api.tiingo.com/iex"

# Rotate between API keys
def get_next_api_key():
    while True:
        for key in TIINGO_API_KEYS:
            yield key

api_key_gen = get_next_api_key()

# Fetch data from Tiingo API for a specific date range
def fetch_data(symbol, start_date, end_date):
    url = f"{BASE_URL}/{symbol}/prices?startDate={start_date}&endDate={end_date}&resampleFreq=1min&columns=open,high,low,close,volume"

    headers = {'Content-Type': 'application/json'}
    current_api_key = next(api_key_gen)

    print(f"Fetching data from {start_date} to {end_date} using API key...")
    while True:
        try:
            response = requests.get(f"{url}&token={current_api_key}", headers=headers)
            if response.status_code == 200:
                data = response.json()
                if data:
                    first_entry = data[0].get('date')
                    last_entry = data[-1].get('date')
                    print(f"Data found! First entry: {first_entry}, Last entry: {last_entry}")
                else:
                    print(f"No data returned for {symbol} from {start_date} to {end_date}.")
                break
            elif response.status_code == 429:  # Rate limit exceeded
                print("Rate limit exceeded. Switching to next API key...")
                current_api_key = next(api_key_gen)
                continue
            else:
                print(f"Error: {response.status_code} - {response.text}")
                break
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            break

# Main function to test the missing ranges
def check_missing_ranges():
    # Set the stock symbol and date ranges you want to test
    symbol = "SPY"
    missing_ranges = [
        ("2016-12-12", "2018-11-08"),
        ("2018-12-04", "2018-12-06"),
        ("2018-12-13", "2019-11-08"),
        ("2019-12-13", "2020-11-09"),
        # Add other ranges as identified by validate_database.py
    ]

    for start_date, end_date in missing_ranges:
        fetch_data(symbol, start_date, end_date)

if __name__ == "__main__":
    check_missing_ranges()

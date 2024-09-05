import time
import requests
from datetime import datetime, timedelta
import pytz
from APIFetching import get_current_price_and_volume, is_market_open
from dotenv import load_dotenv
import os

# Load environment variables from paths.env file
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../paths.env'))
load_dotenv(dotenv_path)
POLYGON_API_KEYS = os.getenv('POLYGON_API_KEYS').split(',')

def fetch_historical_data(symbol, date_str, interval, api_key):
    start_date = date_str
    end_date = (datetime.strptime(date_str, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    url = f'https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/{interval}/{start_date}/{end_date}'
    params = {
        'apiKey': api_key,
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json().get('results', [])

def track_price(symbol, interval='1m'):
    data = []
    eastern = pytz.timezone('US/Eastern')
    sleep_time = 3600 if interval == '1h' else 60  # Adjust sleep time based on interval

    if is_market_open():
        current_time = datetime.now(eastern)
        current_price, current_volume = get_current_price_and_volume(symbol)
        if current_price is not None and current_volume is not None:
            record = (symbol, current_price, current_volume, current_time.strftime('%H:%M:%S'), current_time.strftime('%Y-%m-%d'))
            data.append(record)
        time.sleep(sleep_time)  # Wait for the specified interval
    return data

def track_historical_prices(symbol, start_date, end_date=None, interval='1m'):
    data = []
    eastern = pytz.timezone('US/Eastern')
    start_date_dt = eastern.localize(datetime.strptime(start_date, '%Y-%m-%d'))

    if end_date:
        end_date_dt = eastern.localize(datetime.strptime(end_date, '%Y-%m-%d'))
    else:
        end_date_dt = start_date_dt

    current_date = start_date_dt
    key_index = 0  # Start with the first key

    while current_date <= end_date_dt:
        if current_date.weekday() >= 5:  # Skip weekends (Saturday and Sunday)
            current_date += timedelta(days=1)
            continue

        date_str = current_date.strftime('%Y-%m-%d')
        while True:
            try:
                api_key = POLYGON_API_KEYS[key_index]
                prices = fetch_historical_data(symbol, date_str, interval, api_key)
                break
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:  # Too Many Requests
                    print("\nRate limit exceeded. Switching API key...")
                    key_index = (key_index + 1) % len(POLYGON_API_KEYS)
                else:
                    raise

        if prices is not None:
            last_price = None
            for entry in prices:
                ts = entry['t'] // 1000
                dt = datetime.fromtimestamp(ts)
                price_time = dt.strftime('%H:%M:%S')
                price_date = dt.strftime('%Y-%m-%d')
                price = entry['c']
                volume = entry['v']

                if price != last_price:
                    last_price = price
                    record = (symbol, price, volume, price_time, price_date)
                    data.append(record)

        current_date += timedelta(days=1)
    return data

if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 6:
        symbol = sys.argv[1].upper()
        historical = sys.argv[2].strip().lower() == 'yes'
        start_date = sys.argv[3].strip()
        end_date = sys.argv[4].strip() if len(sys.argv) == 6 else None
        interval = sys.argv[5].strip()
    elif len(sys.argv) == 5:
        symbol = sys.argv[1].upper()
        historical = sys.argv[2].strip().lower() == 'yes'
        start_date = sys.argv[3].strip()
        end_date = None
        interval = sys.argv[4].strip()
    else:
        symbol = input("Enter a stock symbol: ").upper()
        historical = input("Do you want to check historical data? (yes/no): ").strip().lower() == 'yes'

        if historical:
            start_date = input("Enter the start date to check (YYYY-MM-DD): ").strip()
            end_date = input("Enter the end date to check (YYYY-MM-DD) or leave empty for a single day: ").strip()
            end_date = end_date if end_date else None
            interval = input("Enter the interval (e.g., 1m, 1h): ").strip()  # Ask for interval
        else:
            start_date = None
            end_date = None
            interval = input("Enter the interval (e.g., 1m, 1h): ").strip()  # Ask for interval

    if historical:
        data = track_historical_prices(symbol, start_date, end_date, interval)
    else:
        if is_market_open():
            data = track_price(symbol, interval)
        else:
            data = []

    for record in data:
        print(record)

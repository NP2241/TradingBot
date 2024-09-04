import requests
import sqlite3
import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
import pytz

# Load environment variables from paths.env file
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../paths.env'))
load_dotenv(dotenv_path)
POLYGON_API_KEYS = os.getenv('POLYGON_API_KEYS').split(',')

MARKET_OPEN_TIME = 9 * 60 + 30  # 9:30 AM in minutes
MARKET_CLOSE_TIME = 16 * 60     # 4:00 PM in minutes

def create_database(db_path):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS stock_prices
                 (stock_name TEXT, stock_price REAL, volume INTEGER, price_time TEXT, price_date TEXT)''')
    conn.commit()
    conn.close()

def fetch_historical_data(symbol, date, api_key):
    start_date = date.strftime('%Y-%m-%d')
    end_date = (date + timedelta(days=1)).strftime('%Y-%m-%d')
    url = f'https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{start_date}/{end_date}'
    params = {
        'apiKey': api_key,
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json().get('results', [])

def filter_market_hours(data):
    market_data = []
    for entry in data:
        ts = entry['t'] // 1000
        dt = datetime.fromtimestamp(ts)
        total_minutes = dt.hour * 60 + dt.minute
        if MARKET_OPEN_TIME <= total_minutes <= MARKET_CLOSE_TIME:
            market_data.append(entry)
    return market_data

def convert_to_12hr_format(time_str):
    dt = datetime.strptime(time_str, '%H:%M:%S')
    return dt.strftime('%I:%M:%S %p')

def populate_database(db_path, symbol):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    end_date = datetime.now()
    start_date = end_date - timedelta(days=2*365)

    total_days = (end_date - start_date).days
    pst = pytz.timezone('America/Los_Angeles')

    total_api_calls = 0
    start_time = time.time()
    key_index = 0  # Start with the first key

    for i in range(total_days):
        current_date = start_date + timedelta(days=i)

        while True:
            try:
                api_key = POLYGON_API_KEYS[key_index]
                data = fetch_historical_data(symbol, current_date, api_key)
                total_api_calls += 1
                break
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:  # Too Many Requests
                    print("\nRate limit exceeded. Switching API key...")
                    key_index = (key_index + 1) % len(POLYGON_API_KEYS)
                else:
                    raise

        market_data = filter_market_hours(data)

        for j, entry in enumerate(market_data):
            ts = entry['t'] // 1000
            dt = datetime.fromtimestamp(ts)
            price_time = convert_to_12hr_format(dt.strftime('%H:%M:%S'))
            price_date = dt.strftime('%Y-%m-%d')
            c.execute("INSERT INTO stock_prices (stock_name, stock_price, volume, price_time, price_date) VALUES (?, ?, ?, ?, ?)",
                      (symbol, entry['c'], entry['v'], price_time, price_date))

            progress = ((i + 1) / total_days) * 100
            elapsed_time = time.time() - start_time
            remaining_days = total_days - (i + 1)
            estimated_total_time = elapsed_time * total_days / (i + 1)
            estimated_completion_time = start_time + estimated_total_time
            estimated_completion_time_pst = datetime.fromtimestamp(estimated_completion_time, pst).strftime('%Y-%m-%d %I:%M:%S %p')
            print(f"\rProgress: {progress:.2f}% - Estimated completion time (PST): {estimated_completion_time_pst}", end='')

        conn.commit()

        # Sleep to respect API rate limits (5 calls/minute)
        time.sleep(12)  # Sleep for 12 seconds between each API call

    conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python historicalDatabase.py <symbol>")
        sys.exit(1)

    symbol = sys.argv[1].upper()

    db_filename = f"{symbol}_2years_1m.db"
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data', db_filename))

    create_database(db_path)
    populate_database(db_path, symbol)

    print(f"\nDatabase saved at: {db_filename}")

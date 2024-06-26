import time
from datetime import datetime, timedelta
import pytz
from APIFetching import get_current_price, get_historical_prices, is_market_open

def track_price(symbol, interval='1m'):
    data = []
    last_price = None
    eastern = pytz.timezone('US/Eastern')
    sleep_time = 3600 if interval == '1h' else 60  # Adjust sleep time based on interval
    while is_market_open():
        current_time = datetime.now(eastern)
        current_price = get_current_price(symbol)
        if current_price is not None and current_price != last_price:
            last_price = current_price
            record = (symbol, current_price, current_time.strftime('%H:%M:%S'), current_time.strftime('%Y-%m-%d'))
            data.append(record)
            print(record)
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
    while current_date <= end_date_dt:
        if current_date.weekday() >= 5:  # Skip weekends (Saturday and Sunday)
            current_date += timedelta(days=1)
            continue

        date_str = current_date.strftime('%Y-%m-%d')
        print(f"Fetching historical prices for {symbol} on {date_str} with interval {interval}")  # Debugging statement
        prices = get_historical_prices(symbol, date_str, interval=interval)
        if prices is not None:
            last_price = None
            for time_stamp, price in prices.items():
                if price != last_price:
                    last_price = price
                    record = (symbol, price, time_stamp.strftime('%H:%M:%S'), time_stamp.strftime('%Y-%m-%d'))
                    data.append(record)
                    print(record)
        current_date += timedelta(days=1)
    return data

if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 5:
        symbol = sys.argv[1].upper()
        historical = sys.argv[2].strip().lower() == 'yes'
        start_date = sys.argv[3].strip()
        interval = sys.argv[4].strip()
        end_date = sys.argv[5].strip() if len(sys.argv) == 6 else None
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
            print("The market is currently closed.")
            data = []

    for record in data:
        print(record)

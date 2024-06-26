import time
from datetime import datetime, timedelta
import pytz
from APIFetching import get_current_price, get_historical_prices, is_market_open

def track_price(symbol):
    last_price = None
    eastern = pytz.timezone('US/Eastern')
    while is_market_open(datetime.now(eastern)):
        current_time = datetime.now(eastern)
        current_price = get_current_price(symbol)
        if current_price is not None and current_price != last_price:
            print(f"At {current_time}, the price of {symbol} is: ${current_price:.2f}")
            last_price = current_price
        time.sleep(1)  # Wait for 1 second before checking again

def track_historical_prices(symbol, start_date, end_date=None):
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
        prices = get_historical_prices(symbol, date_str)
        if prices is not None:
            last_price = None
            for time_stamp, price in prices.items():
                if price != last_price:
                    print(f"At {time_stamp}, the price of {symbol} was: ${price:.2f}")
                    last_price = price
        else:
            print(f"No historical data available for {symbol} on {date_str} from market open to close.")

        current_date += timedelta(days=1)
        time.sleep(1)  # Wait for 1 second before fetching the next day's data

if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 4:
        symbol = sys.argv[1].upper()
        historical = sys.argv[2].strip().lower() == 'yes'
        start_date = sys.argv[3].strip()
        end_date = sys.argv[4].strip() if len(sys.argv) == 5 else None
    else:
        symbol = input("Enter a stock symbol: ").upper()
        historical = input("Do you want to check historical data? (yes/no): ").strip().lower() == 'yes'

        if historical:
            start_date = input("Enter the start date to check (YYYY-MM-DD): ").strip()
            end_date = input("Enter the end date to check (YYYY-MM-DD) or leave empty for a single day: ").strip()
            end_date = end_date if end_date else None
        else:
            start_date = None
            end_date = None

    if historical:
        track_historical_prices(symbol, start_date, end_date)
    else:
        if is_market_open(datetime.now(pytz.timezone('US/Eastern'))):
            track_price(symbol)
        else:
            print("The market is currently closed.")

import time
from datetime import datetime
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

def track_historical_prices(symbol, date):
    prices = get_historical_prices(symbol, date)
    if prices is not None:
        last_price = None
        for time_stamp, price in prices.items():
            if price != last_price:
                print(f"At {time_stamp}, the price of {symbol} was: ${price:.2f}")
                last_price = price
    else:
        print(f"No historical data available for {symbol} on {date} from market open to close.")

if __name__ == "__main__":
    import sys

    if len(sys.argv) == 4:
        symbol = sys.argv[1].upper()
        historical = sys.argv[2].strip().lower() == 'yes'
        date = sys.argv[3].strip()
    else:
        symbol = input("Enter a stock symbol: ").upper()
        historical = input("Do you want to check historical data? (yes/no): ").strip().lower() == 'yes'

        if historical:
            date = input("Enter the date to check (YYYY-MM-DD): ").strip()
        else:
            date = None

    if historical:
        track_historical_prices(symbol, date)
    else:
        if is_market_open(datetime.now(pytz.timezone('US/Eastern'))):
            track_price(symbol)
        else:
            print("The market is currently closed.")

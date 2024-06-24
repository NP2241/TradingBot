import yfinance as yf
from datetime import datetime, timedelta
import pytz

def get_current_price(symbol):
    try:
        stock = yf.Ticker(symbol)
        now = datetime.now(pytz.timezone('US/Eastern'))

        if is_market_open(now):
            data = stock.history(period='1d', interval='1m')
            if not data.empty:
                current_price = data['Close'].iloc[-1]
                return current_price
            else:
                return None
        else:
            data = stock.history(period='2d', interval='1d')
            if not data.empty:
                # Get the closing price of the most recent trading day
                closing_price = data['Close'].iloc[-1]
                return closing_price
            else:
                return None
    except Exception as e:
        return None

def is_market_open(current_time):
    # US market hours (9:30 AM to 4:00 PM EST)
    market_open_time = current_time.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close_time = current_time.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open_time <= current_time <= market_close_time

def main():
    symbol = input("Enter a stock symbol: ").upper()
    price = get_current_price(symbol)
    now = datetime.now(pytz.timezone('US/Eastern'))

    if price is not None:
        if is_market_open(now):
            print(f"The current price of {symbol} is: ${price:.2f}")
            print("The market is currently open.")
        else:
            print(f"The last closing price of {symbol} is: ${price:.2f}")
            print("The market is currently closed.")
    else:
        print("Invalid symbol or unable to fetch data. Please check the stock symbol and try again.")

if __name__ == "__main__":
    main()

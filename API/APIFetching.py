import yfinance as yf
from datetime import datetime, timedelta
import pytz

def get_current_price(symbol):
    try:
        stock = yf.Ticker(symbol)
        eastern = pytz.timezone('US/Eastern')
        now = datetime.now(eastern)

        if is_market_open(now):
            # Fetch the latest available data
            data = stock.history(period='1d', interval='1m')
            if not data.empty:
                current_price = data['Close'].iloc[-1]
                return current_price
            else:
                return None
        else:
            # Fetch daily data for the last day
            data = stock.history(period='5d', interval='1d')
            if not data.empty:
                # Ensure to get the closing price of the most recent trading day
                closing_price = data['Close'].iloc[-1]
                return closing_price
            else:
                return None
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None

def get_historical_prices(symbol, date):
    try:
        stock = yf.Ticker(symbol)
        eastern = pytz.timezone('US/Eastern')
        start_date = eastern.localize(datetime.strptime(date, '%Y-%m-%d')).replace(hour=9, minute=30, second=0)
        end_date = start_date + timedelta(hours=6, minutes=30)  # 9:30 AM + 6.5 hours = 4:00 PM
        data = stock.history(start=start_date, end=end_date, interval='1m')
        if not data.empty:
            return data['Close']
        else:
            return None
    except Exception as e:
        print(f"Error fetching historical data for {symbol} on {date}: {e}")
        return None

def is_market_open(current_time):
    # US market hours (9:30 AM to 4:00 PM EST)
    market_open_time = current_time.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close_time = current_time.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open_time <= current_time <= market_close_time

if __name__ == "__main__":
    symbol = input("Enter a stock symbol: ").upper()
    price = get_current_price(symbol)
    if price is not None:
        print(f"The current price of {symbol} is: ${price:.2f}")
    else:
        print("Invalid symbol or unable to fetch data. Please check the stock symbol and try again.")

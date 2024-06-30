import yfinance as yf
from datetime import datetime, timedelta

def get_historical_prices(symbol, date_str, interval='1m'):
    stock = yf.Ticker(symbol)
    start_date = date_str
    end_date = (datetime.strptime(date_str, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    try:
        data = stock.history(start=start_date, end=end_date, interval=interval)
        if data.empty:
            return None
        else:
            return data[['Close', 'Volume']]
    except Exception as e:
        print(f"Error fetching historical data for {symbol} on {date_str}: {e}")
        return None

def get_current_price_and_volume(symbol):
    stock = yf.Ticker(symbol)
    try:
        data = stock.history(period='1d', interval='1m')
        if not data.empty:
            latest_data = data.iloc[-1]
            return latest_data['Close'], latest_data['Volume']
        else:
            return None, None
    except Exception as e:
        print(f"Error fetching current price and volume for {symbol}: {e}")
        return None, None

def is_market_open():
    now = datetime.now()
    # US market hours (9:30 AM to 4:00 PM EST)
    market_open_time = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close_time = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open_time <= now <= market_close_time

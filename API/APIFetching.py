import requests # Odd intellij error, file works though
from datetime import datetime

API_KEY = 'MQQMS2DV63VD3YDK'

def get_current_price(symbol):
    url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={API_KEY}'
    response = requests.get(url)
    data = response.json()
    return data

def is_market_open():
    now = datetime.now()
    # US market hours
    market_open_time = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close_time = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open_time <= now <= market_close_time

def main():
    symbol = input("Enter a stock symbol: ").upper()
    data = get_current_price(symbol)
    if 'Global Quote' in data and '05. price' in data['Global Quote']:
        price = data['Global Quote']['05. price']
        latest_trading_day = data['Global Quote']['07. latest trading day']
        print(f"The current price of {symbol} is: ${price} (as of {latest_trading_day})")

        if is_market_open():
            print("The market is currently open.")
        else:
            print("The market is currently closed.")
    else:
        print("Invalid symbol.")

if __name__ == "__main__":
    main()

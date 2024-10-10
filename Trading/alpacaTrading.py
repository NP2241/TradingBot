import os
import json
import boto3
import yfinance as yf
from datetime import datetime, timedelta
import alpaca_trade_api as tradeapi  # Requires 'alpaca-trade-api' library

# Alpaca API keys and base URL
APCA_API_KEY_ID = os.getenv("APCA_API_KEY_ID")
APCA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")
APCA_API_BASE_URL = "https://paper-api.alpaca.markets"
alpaca_api = tradeapi.REST(APCA_API_KEY_ID, APCA_API_SECRET_KEY, base_url=APCA_API_BASE_URL)

# DynamoDB setup
dynamodb = boto3.resource('dynamodb')
DYNAMODB_TABLE_NAME = 'StockDataTable'
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

def get_alpaca_cash_balance():
    """
    Retrieves the current cash balance from the Alpaca account.
    :return: Current cash balance as a float
    """
    try:
        account = alpaca_api.get_account()
        return float(account.cash)
    except Exception as e:
        print(f"Error retrieving cash balance from Alpaca: {e}")
        return 0.0

def save_minute_data_to_dynamodb(symbol, price, volume, timestamp):
    """
    Save the minute-by-minute price and volume data to DynamoDB for a given symbol.
    :param symbol: Stock symbol (primary key)
    :param price: Stock price at the given timestamp
    :param volume: Stock trading volume at the given timestamp
    :param timestamp: Timestamp (sort key) in 'YYYY-MM-DD HH:MM' format
    """
    try:
        table.put_item(
            Item={
                'symbol': symbol,  # Partition key
                'timestamp': timestamp,  # Sort key
                'price': price,
                'volume': volume
            }
        )
        print(f"Successfully saved data for {symbol} at {timestamp} to DynamoDB.")
    except Exception as e:
        print(f"Error saving data to DynamoDB for {symbol}: {e}")

def load_14_day_data_from_dynamodb(symbol):
    """
    Load the last 14 days of minute-by-minute data from DynamoDB for a given symbol.
    :param symbol: Stock symbol (primary key)
    :return: List of prices over the last 14 days
    """
    try:
        # Calculate the start date for the 14-day window
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=14)
        start_timestamp = start_date.strftime('%Y-%m-%d %H:%M')
        end_timestamp = end_date.strftime('%Y-%m-%d %H:%M')

        # Query DynamoDB for all items between start_timestamp and end_timestamp
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('symbol').eq(symbol) &
                                   boto3.dynamodb.conditions.Key('timestamp').between(start_timestamp, end_timestamp)
        )

        # Extract and return the price values
        prices = [item['price'] for item in response.get('Items', [])]
        print(f"Loaded {len(prices)} data points for {symbol} from DynamoDB.")
        return prices
    except Exception as e:
        print(f"Error loading data from DynamoDB for {symbol}: {e}")
        return []

def update_data_with_yfinance(symbol, interval='1m'):
    """
    Update the stock data dictionary with the latest real-time minute-by-minute data from yfinance.
    :param symbol: Stock symbol
    :param interval: Time interval for yfinance data (default is '1m' for minute-by-minute)
    """
    # Fetch the most recent minute-by-minute data
    ticker = yf.Ticker(symbol)
    new_data = ticker.history(period="1d", interval=interval)
    if not new_data.empty:
        latest_minute = new_data.index[-1].strftime('%Y-%m-%d %H:%M')
        latest_price = new_data['Close'].iloc[-1]
        latest_volume = new_data['Volume'].iloc[-1]

        # Save the new data to DynamoDB
        save_minute_data_to_dynamodb(symbol, latest_price, latest_volume, latest_minute)

        print(f"Updated {symbol} data with yfinance. Latest price: {latest_price} at {latest_minute}")

def trade_with_alpaca(symbols, threshold=0.1):
    """
    Executes trading strategy using Alpaca API for real-time trading, similar to the simulate_trading method.
    """
    # Load initial stock data from DynamoDB
    stock_data = {symbol: {'prices': load_14_day_data_from_dynamodb(symbol), 'shares': 0, 'purchase_history': [], 'daily_profit': 0} for symbol in symbols}

    # Retrieve the current cash balance from Alpaca
    total_cash = get_alpaca_cash_balance()
    print(f"Current cash balance from Alpaca: ${total_cash:.2f}")

    combined_equity = total_cash

    # Update the stock data with the latest prices using yfinance
    for symbol in symbols:
        update_data_with_yfinance(symbol)

    # Calculate Bollinger Bands for each symbol
    initial_bands = {}
    for symbol in symbols:
        lower_band, weighted_moving_average, upper_band = calculate_weighted_bollinger_bands(stock_data[symbol]['prices'])
        initial_bands[symbol] = (lower_band, weighted_moving_average, upper_band)

    # Evaluate buy and sell opportunities
    for symbol in symbols:
        current_price = stock_data[symbol]['prices'][-1]
        lower_band, _, upper_band = initial_bands[symbol]

        # Adjust the threshold dynamically based on suggestions
        hour_of_day = datetime.utcnow().hour
        if hour_of_day < 11 or hour_of_day >= 15:  # Early morning or late afternoon
            dynamic_threshold = threshold * 1.5  # Increase threshold to allow wider bands
        else:
            dynamic_threshold = threshold  # Use default threshold during midday

        # Adjust threshold based on proximity to bands
        if current_price < lower_band:
            dynamic_threshold *= 1.1  # Increase threshold for buys
        elif current_price > upper_band:
            dynamic_threshold *= 1.1  # Increase threshold for sells

        # Calculate dynamic buy size based on distance from lower band
        distance_from_band = abs(current_price - lower_band) / (upper_band - lower_band)
        dynamic_buy_size = max(1, int((1 - distance_from_band) * (total_cash // current_price)))

        # Sell Logic
        if current_price >= (upper_band * (1 - dynamic_threshold / 100)) and stock_data[symbol]['shares'] > 0:
            shares_to_sell = stock_data[symbol]['shares']
            if shares_to_sell > 0:
                cash_gained = round(shares_to_sell * current_price, 2)
                stock_data[symbol]['shares'] = 0
                total_cash += cash_gained

                # Track performance and update state
                stock_data[symbol]['daily_profit'] += cash_gained
                stock_data[symbol]['purchase_history'] = []  # Reset purchase history

                print(f"Sold {shares_to_sell} shares of {symbol} at {current_price}. Total Cash: {total_cash}")

        # Buy Logic
        if current_price <= (lower_band * (1 + dynamic_threshold / 100)) and total_cash >= current_price:
            shares_to_buy = min(dynamic_buy_size, int(total_cash // current_price))
            if shares_to_buy > 0:
                stock_data[symbol]['shares'] += shares_to_buy
                cash_spent = round(shares_to_buy * current_price, 2)
                total_cash -= cash_spent
                stock_data[symbol]['purchase_history'].append((current_price, shares_to_buy))

                print(f"Bought {shares_to_buy} shares of {symbol} at {current_price}. Cash Remaining: {total_cash}")

if __name__ == "__main__":
    # Define symbols and execute the trading strategy
    symbols = ["AAPL", "AMZN", "NFLX", "GOOGL", "META"]
    trade_with_alpaca(symbols)

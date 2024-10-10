import os
import json
import boto3
import yfinance as yf
from datetime import datetime, timedelta
import alpaca_trade_api as tradeapi  # Ensure you have the `alpaca-trade-api` library installed

# Alpaca API keys and base URL
APCA_API_KEY_ID = os.getenv("APCA_API_KEY_ID")
APCA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")
APCA_API_BASE_URL = "https://paper-api.alpaca.markets/v2"  # Updated to the correct endpoint

# Initialize the Alpaca API client with the new base URL
alpaca_api = tradeapi.REST(APCA_API_KEY_ID, APCA_API_SECRET_KEY, base_url=APCA_API_BASE_URL)

# DynamoDB setup
dynamodb = boto3.resource('dynamodb')
DYNAMODB_TABLE_NAME = 'StockDataTable'
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

# Function to get Alpaca cash balance
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

# Function to save data to DynamoDB
def save_data_to_dynamodb(symbol, timestamp, data):
    """
    Save the dictionary data to DynamoDB for a given symbol and timestamp.
    :param symbol: Stock symbol (primary key)
    :param timestamp: The timestamp for the stock data (sort key)
    :param data: Dictionary to be saved as the item for the given symbol and timestamp
    """
    try:
        table.put_item(
            Item={
                'symbol': symbol,
                'timestamp': timestamp,
                'price': data['prices'][-1],  # Latest price
                'volume': data.get('volume', 0),  # Volume of the stock
                'shares': data['shares'],
                'purchase_history': json.dumps(data['purchase_history']),  # Serialize purchase history
                'daily_profit': data['daily_profit'],
                'winning_sells': data['winning_sells'],
                'losing_sells': data['losing_sells']
            }
        )
        print(f"Successfully saved data for {symbol} at {timestamp} to DynamoDB.")
    except Exception as e:
        print(f"Error saving data to DynamoDB for {symbol} at {timestamp}: {e}")


# Function to load data from DynamoDB
def load_data_from_dynamodb(symbol):
    """
    Load the dictionary data from DynamoDB for a given symbol.
    :param symbol: Stock symbol (primary key)
    :return: Loaded dictionary or an empty list if no data is found
    """
    try:
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('symbol').eq(symbol),
            ScanIndexForward=False,  # Get the latest item first
            Limit=1  # Get only the most recent data point
        )
        if response['Items']:
            item = response['Items'][0]
            item['purchase_history'] = json.loads(item['purchase_history'])  # Deserialize purchase history
            return item
        else:
            print(f"No data found for {symbol} in DynamoDB. Initializing new data store.")
            return {"prices": [], "shares": 0, "purchase_history": [], "daily_profit": 0, "winning_sells": 0, "losing_sells": 0}
    except Exception as e:
        print(f"Error loading data from DynamoDB for {symbol}: {e}")
        return {"prices": [], "shares": 0, "purchase_history": [], "daily_profit": 0, "winning_sells": 0, "losing_sells": 0}

# Function to update data with yfinance
def update_data_with_yfinance(symbol, stock_data, interval='1m'):
    """
    Update the stock data dictionary with the latest real-time minute-by-minute data from yfinance.
    :param symbol: Stock symbol
    :param stock_data: Dictionary storing historical and real-time data
    :param interval: Time interval for yfinance data (default is '1m' for minute-by-minute)
    """
    # Fetch the most recent minute-by-minute data
    ticker = yf.Ticker(symbol)
    new_data = ticker.history(period="1d", interval=interval)
    if not new_data.empty:
        latest_minute = new_data.index[-1].strftime('%Y-%m-%d %H:%M:%S')
        latest_price = new_data['Close'].iloc[-1]

        # Append the new data and maintain only the last 14 days of minute-by-minute data
        stock_data['prices'].append(latest_price)
        stock_data['prices'] = stock_data['prices'][-1440 * 14:]  # Keep last 14 days of minute data
        print(f"Updated {symbol} data with yfinance. Latest price: {latest_price} at {latest_minute}")

# Calculate Bollinger Bands function (Add this function as a placeholder if it's missing)
def calculate_weighted_bollinger_bands(prices, window=14):
    if len(prices) < window:
        return None, None, None
    weights = [i + 1 for i in range(window)]
    weighted_moving_average = sum([p * w for p, w in zip(prices[-window:], weights)]) / sum(weights)
    weighted_std_dev = (sum(w * (p - weighted_moving_average) ** 2 for p, w in zip(prices[-window:], weights)) / sum(weights)) ** 0.5
    upper_band = weighted_moving_average + (weighted_std_dev * 2)
    lower_band = weighted_moving_average - (weighted_std_dev * 2)
    return lower_band, weighted_moving_average, upper_band

# Function to handle trading with Alpaca
def trade_with_alpaca(symbols, threshold=0.1):
    """
    Executes trading strategy using Alpaca API for real-time trading, similar to the simulate_trading method.
    """
    # Load initial stock data from DynamoDB
    stock_data = {symbol: load_data_from_dynamodb(symbol) for symbol in symbols}

    # Retrieve the current cash balance from Alpaca
    total_cash = get_alpaca_cash_balance()
    print(f"Current cash balance from Alpaca: ${total_cash:.2f}")

    combined_equity = total_cash

    # Update the stock data with the latest prices using yfinance
    for symbol in symbols:
        update_data_with_yfinance(symbol, stock_data[symbol])

    # Calculate Bollinger Bands for each symbol
    initial_bands = {}
    for symbol in symbols:
        lower_band, weighted_moving_average, upper_band = calculate_weighted_bollinger_bands(stock_data[symbol]['prices'])
        initial_bands[symbol] = (lower_band, weighted_moving_average, upper_band)

    # Evaluate buy and sell opportunities
    for symbol in symbols:
        current_price = stock_data[symbol]['prices'][-1]
        lower_band, _, upper_band = initial_bands[symbol]
        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

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

        # Save updated stock data to DynamoDB with timestamp
        save_data_to_dynamodb(symbol, timestamp, stock_data[symbol])

if __name__ == "__main__":
    # Define symbols and execute the trading strategy
    symbols = ["AAPL", "AMZN", "NFLX", "GOOGL", "META"]
    trade_with_alpaca(symbols)

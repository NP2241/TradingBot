import os
import sys
import sqlite3
import time
import subprocess
from datetime import datetime, timedelta
import holidays

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../DataAnalysis')))

from stockAnalysis import calculate_buy_index, get_db_path, database_exists, create_database, check_db_populated, calculate_stock_analysis
from stockMetrics import calculate_bollinger_bands

def create_simulation_database(symbol, start_date, end_date, interval):
    script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../DatabaseSetup/setupDatabase.py'))
    command = [sys.executable, script_path, symbol, "yes", start_date, interval, end_date]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

def create_database(symbol, start_date, end_date, interval):
    script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../DatabaseSetup/setupDatabase.py'))
    command = [sys.executable, script_path, symbol, "yes", start_date, interval, end_date]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

def check_table_exists(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stock_prices'")
    result = c.fetchone()
    conn.close()
    return result is not None

def clear_trade_data_file(trade_data_file):
    if os.path.exists(trade_data_file):
        os.remove(trade_data_file)

def is_market_closed(date):
    # Check if the date is a weekend
    if date.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        return True

    # Check if the date is a US market holiday
    us_holidays = holidays.US()
    if date in us_holidays:
        return True

    return False

def initialize_trade_data_file(trade_data_file):
    os.makedirs(os.path.dirname(trade_data_file), exist_ok=True)

    conn = sqlite3.connect(trade_data_file)
    c = conn.cursor()

    # Create table with the expected columns
    c.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            date TEXT,
            cash REAL,
            shares INTEGER,
            equity REAL
        )
    ''')

    conn.commit()
    conn.close()

def write_trade_to_db(trade_data_file, current_date, cash, shares, equity):
    conn = sqlite3.connect(trade_data_file)
    c = conn.cursor()

    # Insert the values into the trades table
    c.execute("INSERT INTO trades (date, cash, shares, equity) VALUES (?, ?, ?, ?)",
              (current_date, cash, shares, equity))

    conn.commit()
    conn.close()

def get_last_trade_data(trade_data_file):
    conn = sqlite3.connect(trade_data_file)
    c = conn.cursor()

    c.execute("SELECT date, cash, shares, equity FROM trades ORDER BY date DESC LIMIT 1")
    last_trade = c.fetchone()
    conn.close()

    if last_trade:
        return last_trade[1], last_trade[2], last_trade[3]  # cash, shares, equity
    else:
        return None  # If no data is present

def print_trade_data_file(trade_data_file):
    if os.path.exists(trade_data_file):
        with open(trade_data_file, 'r') as f:
            contents = f.readlines()
        print(f"Contents of trade file:")
        for line in contents:
            print(line.strip())
    else:
        print(f"{trade_data_file} does not exist.")

def simulate_trading(symbol, start_date, end_date, interval, simulate_start_date, simulate_end_date, threshold, initial_cash):
    # Ensure the database for the range exists
    db_path = get_db_path(symbol, start_date, end_date, interval)
    if not database_exists(db_path):
        print(f"Database for {symbol} does not exist. Creating database...")
        create_database(symbol, start_date, end_date, interval)

        # Indicator states
        indicator_states = ['   ', '.  ', '.. ', '...']
        indicator_index = 0

        while not (database_exists(db_path) and check_db_populated(db_path)):
            sys.stdout.write(f"\rWaiting for database to be populated{indicator_states[indicator_index]}")
            sys.stdout.flush()
            time.sleep(1)
            indicator_index = (indicator_index + 1) % len(indicator_states)

        # Clear the waiting message
        sys.stdout.write("\rDatabase populated.                              \n")

    volatility_index, metrics = calculate_stock_analysis(db_path)
    if volatility_index is not None and metrics is not None:
        current_price = metrics['moving_average_value']  # Assuming the current price is the latest moving average
        buy_index = calculate_buy_index(volatility_index, metrics, current_price)
    else:
        print("No data available to calculate the stock analysis.")
        return

    lower_band = min(metrics['lower_band'], metrics['upper_band'])
    upper_band = max(metrics['lower_band'], metrics['upper_band'])

    # Create the simulation date range database if it doesn't exist
    simulate_db_path = get_db_path(symbol, simulate_start_date, simulate_end_date, interval)
    if not database_exists(simulate_db_path):
        print(f"Creating database for {symbol} from {simulate_start_date} to {simulate_end_date}...")
        create_simulation_database(symbol, simulate_start_date, simulate_end_date, interval)

    while not (database_exists(simulate_db_path) and check_table_exists(simulate_db_path)):
        print("Waiting for simulation database to be populated...")
        time.sleep(1)

    trade_data_file = os.path.join(os.path.dirname(__file__), '../data/tradeData/trades.db')
    clear_trade_data_file(trade_data_file)
    initialize_trade_data_file(trade_data_file)

    # Check if there's previous data in the trades.db file
    last_trade_data = get_last_trade_data(trade_data_file)
    if last_trade_data:
        cash, shares, equity = last_trade_data
    else:
        cash = initial_cash  # Starting cash in USD
        shares = 0

    # Iterate through each simulation day
    current_date = simulate_start_date
    prev_closing_equity = cash

    while current_date <= simulate_end_date:
        current_date_dt = datetime.strptime(current_date, '%Y-%m-%d')

        # Check if the market is closed on this date
        if is_market_closed(current_date_dt):
            current_date = (current_date_dt + timedelta(days=1)).strftime('%Y-%m-%d')
            continue

        # Read the data from the simulation date range database
        conn = sqlite3.connect(simulate_db_path)
        c = conn.cursor()
        c.execute("SELECT stock_price, volume, price_date, price_time FROM stock_prices WHERE price_date = ?", (current_date,))
        stock_data = c.fetchall()
        conn.close()

        # If no stock data is available and it's not a market closure day, skip to the next day
        if not stock_data:
            print(f"No data available for {symbol} on {current_date}.")
            current_date = (current_date_dt + timedelta(days=1)).strftime('%Y-%m-%d')
            continue

        for price, volume, date, time_ in stock_data:
            # Recalculate Bollinger Bands using the last 20 data points before any trade
            recent_prices = [data[0] for data in stock_data if datetime.strptime(data[2], '%Y-%m-%d') <= datetime.strptime(date, '%Y-%m-%d')][-20:]
            if len(recent_prices) >= 20:
                lower_band, moving_average, upper_band = calculate_bollinger_bands(recent_prices)

            if price <= lower_band * (1 + (threshold / 100)):  # Buy condition within the specified percentage of the lower band
                if cash >= price:  # Check if there is enough money to buy one share
                    shares += 1  # Buy only one share
                    cash -= price  # Deduct the cost of one share from cash

                    # Recalculate Bollinger Bands after the buy using the most recent 20 data points
                    recent_prices = [data[0] for data in stock_data if datetime.strptime(data[2], '%Y-%m-%d') <= datetime.strptime(date, '%Y-%m-%d')][-20:]
                    if len(recent_prices) >= 20:
                        lower_band, moving_average, upper_band = calculate_bollinger_bands(recent_prices)

            elif price >= upper_band * (1 - (threshold / 100)) and shares > 0:  # Sell condition within the specified percentage of the upper band
                cash += shares * price
                shares = 0

                # Recalculate Bollinger Bands after the sell using the most recent 20 data points
                recent_prices = [data[0] for data in stock_data if datetime.strptime(data[2], '%Y-%m-%d') <= datetime.strptime(date, '%Y-%m-%d')][-20:]
                if len(recent_prices) >= 20:
                    lower_band, moving_average, upper_band = calculate_bollinger_bands(recent_prices)

        ending_price = stock_data[-1][0]  # Closing price on the simulation date
        equity = cash + (shares * ending_price)
        returns_percentage = ((equity - prev_closing_equity) / prev_closing_equity) * 100
        prev_closing_equity = equity

        write_trade_to_db(trade_data_file, current_date, cash, shares, equity)

        current_date = (current_date_dt + timedelta(days=1)).strftime('%Y-%m-%d')

        # Recalculate the Bollinger Bands
        volatility_index, metrics = calculate_stock_analysis(db_path)
        #if volatility_index is not None and metrics is not None:
        #    lower_band = min(metrics['lower_band'], metrics['upper_band'])
        #    upper_band = max(metrics['lower_band'], metrics['upper_band'])

    total_equity = cash + (shares * ending_price)
    total_returns = ((total_equity - initial_cash) / initial_cash) * 100

    print(f"Final Equity: {total_equity}")
    print(f"Total Percentage Returns: {total_returns}%")
if len(sys.argv) != 9:
    print("Usage: python tradeSimulator.py <symbol> <start_date> <end_date> <interval> <simulate_start_date> <simulate_end_date> <threshold> <initial_cash>")
    sys.exit(1)

symbol = sys.argv[1].upper()
start_date = sys.argv[2].strip()
end_date = sys.argv[3].strip()
interval = sys.argv[4].strip()
simulate_start_date = sys.argv[5].strip()
simulate_end_date = sys.argv[6].strip()
threshold = float(sys.argv[7].strip())
initial_cash = float(sys.argv[8].strip())

simulate_trading(symbol, start_date, end_date, interval, simulate_start_date, simulate_end_date, threshold, initial_cash)
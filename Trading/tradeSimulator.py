import os
import sys
import sqlite3
import time
import subprocess
from datetime import datetime, timedelta
import holidays
import numpy as np

# Import necessary functions and modules from other scripts
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../DataAnalysis')))
from stockAnalysis import calculate_buy_index, get_db_path, database_exists, create_database, check_db_populated, calculate_stock_analysis
from stockMetrics import calculate_bollinger_bands, calculate_volatility_index, calculate_stock_metrics

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
    if date.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        return True

    us_holidays = holidays.US()
    if date in us_holidays:
        return True

    return False

def initialize_equity_file(equity_file):
    os.makedirs(os.path.dirname(equity_file), exist_ok=True)

    conn = sqlite3.connect(equity_file)
    c = conn.cursor()

    c.execute('''
        CREATE TABLE IF NOT EXISTS equity (
            date TEXT,
            cash REAL,
            shares INTEGER,
            equity REAL
        )
    ''')

    conn.commit()
    conn.close()

def write_equity_to_db(equity_file, current_date, cash, shares, equity):
    conn = sqlite3.connect(equity_file)
    c = conn.cursor()

    c.execute("INSERT INTO equity (date, cash, shares, equity) VALUES (?, ?, ?, ?)",
              (current_date, cash, shares, equity))

    conn.commit()
    conn.close()

def write_trade_to_db(trades_file, date, action, shares, price, profit):
    conn = sqlite3.connect(trades_file)
    c = conn.cursor()

    c.execute(f"INSERT INTO {symbol}_trades (date, action, shares, price, profit) VALUES (?, ?, ?, ?, ?)",
              (date, action, shares, price, profit))

    conn.commit()
    conn.close()

def get_last_trade_data(equity_file):
    conn = sqlite3.connect(equity_file)
    c = conn.cursor()

    c.execute("SELECT date, cash, shares, equity FROM equity ORDER BY date DESC LIMIT 1")
    last_trade = c.fetchone()
    conn.close()

    if last_trade:
        return last_trade[1], last_trade[2], last_trade[3]  # cash, shares, equity
    else:
        return None  # If no data is present



# Function to calculate weighted average for given prices and weights
def calculate_weighted_average(prices, weights):
    """
    Calculate the weighted average of the given prices using the provided weights.
    """
    return np.dot(prices, weights) / sum(weights)

# Function to calculate weighted Bollinger Bands
def calculate_weighted_bollinger_bands(prices, window=14):
    """
    Calculate the weighted Bollinger Bands using the last `window` days of prices.
    More weight is given to the most recent days.
    """
    if len(prices) < window:
        return None, None, None  # Not enough data points

    # Create weights such that recent days have higher weights
    weights = np.arange(1, window + 1)  # [1, 2, 3, ..., window]

    # Calculate the weighted moving average
    weighted_moving_average = calculate_weighted_average(prices[-window:], weights)

    # Calculate the standard deviation
    weighted_std_dev = np.sqrt(np.dot(weights, (prices[-window:] - weighted_moving_average) ** 2) / sum(weights))

    # Calculate the upper and lower bands
    upper_band = weighted_moving_average + (weighted_std_dev * 2)
    lower_band = weighted_moving_average - (weighted_std_dev * 2)

    return lower_band, weighted_moving_average, upper_band


def initialize_trade_summary_file(trades_file):
    """
    Initializes the trade summary file with columns: date, profit, winning_sells, and losing_sells.
    """
    os.makedirs(os.path.dirname(trades_file), exist_ok=True)

    conn = sqlite3.connect(trades_file)
    c = conn.cursor()

    # Create the new table structure with the correct column names
    c.execute(f'''
        CREATE TABLE IF NOT EXISTS {symbol}_trades (
            date TEXT,
            profit REAL,
            winning_sells REAL,
            losing_sells REAL
        )
    ''')

    conn.commit()
    conn.close()

def write_daily_summary_to_db(trades_file, date, daily_profit, winning_sells, losing_sells):
    """
    Writes the daily summary for trading activity into the trades table.
    The summary includes date, profit, winning sells, and losing sells.
    """
    conn = sqlite3.connect(trades_file)
    c = conn.cursor()

    # Insert the date, profit, winning sells, and losing sells into the table
    c.execute(f"INSERT INTO {symbol}_trades (date, profit, winning_sells, losing_sells) VALUES (?, ?, ?, ?)",
              (date, daily_profit, winning_sells, losing_sells))

    conn.commit()
    conn.close()

def simulate_trading(symbol, start_date, end_date, interval, simulate_start_date, simulate_end_date, threshold, initial_cash):
    # Step 1: Create a single database covering from start_date to simulate_end_date
    full_db_path = get_db_path(symbol, start_date, simulate_end_date, interval)
    if not database_exists(full_db_path):
        print(f"Database for {symbol} does not exist. Creating database from {start_date} to {simulate_end_date}...")
        create_database(symbol, start_date, simulate_end_date, interval)

        indicator_states = ['   ', '.  ', '.. ', '...']
        indicator_index = 0

        while not (database_exists(full_db_path) and check_db_populated(full_db_path)):
            sys.stdout.write(f"\rWaiting for database to be populated{indicator_states[indicator_index]}")
            sys.stdout.flush()
            time.sleep(1)
            indicator_index = (indicator_index + 1) % len(indicator_states)

        sys.stdout.write("\rDatabase populated.                              \n")

    # Step 2: Calculate initial bands using historical data up to the day before the simulation starts
    historical_db_conn = sqlite3.connect(full_db_path)
    historical_cursor = historical_db_conn.cursor()
    historical_cursor.execute("SELECT stock_price FROM stock_prices WHERE price_date BETWEEN ? AND ? ORDER BY price_date, price_time",
                              (start_date, simulate_start_date))
    historical_prices = [row[0] for row in historical_cursor.fetchall()]
    historical_db_conn.close()

    if len(historical_prices) < 14:  # Ensure we have at least 14 days of data
        print("Not enough historical data to calculate initial bands.")
        return

    # Calculate initial bands using historical data with weighted average
    lower_band, weighted_moving_average, upper_band = calculate_weighted_bollinger_bands(historical_prices, window=14)

    # Step 3: Simulate trading using minute-by-minute data from the current day onwards
    simulate_db_conn = sqlite3.connect(full_db_path)
    simulate_cursor = simulate_db_conn.cursor()

    # Set the new file names for equity and trades tracking
    equity_file = os.path.join(os.path.dirname(__file__), '../data/tradeData/equity.db')
    trades_file = os.path.join(os.path.dirname(__file__), '../data/tradeData/trades.db')

    # Clear and initialize the equity and trades files
    clear_trade_data_file(equity_file)
    initialize_equity_file(equity_file)
    clear_trade_data_file(trades_file)
    initialize_trade_summary_file(trades_file)

    last_trade_data = get_last_trade_data(equity_file)
    if last_trade_data:
        cash, shares, equity = last_trade_data
    else:
        cash = initial_cash
        shares = 0

    # Initialize variables to track buy/sell cycles
    purchase_history = []  # Track (price, quantity) for each buy trade
    total_trades = 0  # Track total number of trades made
    daily_profit = 0  # Track profit for the day

    current_date = simulate_start_date
    prev_closing_equity = cash
    missing_dates = []  # Track missing dates

    # Variables to track daily and cumulative performance
    winning_sells = 0  # Track total value of profitable sells for the day
    losing_sells = 0  # Track total value of unprofitable sells for the day

    while current_date <= simulate_end_date:
        current_date_dt = datetime.strptime(current_date, '%Y-%m-%d')

        if is_market_closed(current_date_dt):
            current_date = (current_date_dt + timedelta(days=1)).strftime('%Y-%m-%d')
            continue

        simulate_cursor.execute("SELECT stock_price, volume, price_date, price_time FROM stock_prices WHERE price_date = ? ORDER BY price_time",
                                (current_date,))
        stock_data = simulate_cursor.fetchall()

        if not stock_data:
            # Collect missing date
            missing_dates.append(current_date_dt)
            current_date = (current_date_dt + timedelta(days=1)).strftime('%Y-%m-%d')
            continue

        # Reset daily statistics
        daily_profit = 0
        winning_sells = 0
        losing_sells = 0

        # Track purchase details for profit calculation
        for price, volume, date, time_ in stock_data:
            # Check buy/sell decision before updating bands with the new minute's price
            if price <= lower_band and cash >= price:
                # Buy logic
                shares_to_buy = 1  # Buy only 1 share at a time
                shares += shares_to_buy
                cash_spent = shares_to_buy * price
                cash -= cash_spent

                # Track the purchase in history
                purchase_history.append((price, shares_to_buy))

                total_trades += 1  # Increment trade count for buy

            elif price >= upper_band and shares > 0:
                # Sell logic
                cash_gained = shares * price

                # Calculate profit based on cumulative purchase history
                total_buy_cost = sum([qty * p for p, qty in purchase_history])  # Sum of all buy costs
                profit = cash_gained - total_buy_cost  # Calculate profit

                # Update cash and clear shares and purchase history
                cash += cash_gained
                purchase_history.clear()  # Clear purchase history after sell
                shares = 0

                daily_profit += profit  # Update daily profit

                # Update winning/losing sells based on profit
                if profit > 0:
                    winning_sells += profit  # Add to winning sells
                else:
                    losing_sells += profit  # Add to losing sells

                total_trades += 1  # Increment trade count for sell

            # Now update bands using the new price for the next minute
            historical_prices.append(price)  # Include this minute's price in historical prices
            lower_band, weighted_moving_average, upper_band = calculate_weighted_bollinger_bands(historical_prices[-14:], window=14)  # Recalculate with new price

        # Write a single summary entry for the day in `trades.db`
        write_daily_summary_to_db(trades_file, current_date, daily_profit, winning_sells, losing_sells)

        # Write end-of-day equity data
        ending_price = stock_data[-1][0]
        equity = cash + (shares * ending_price)
        prev_closing_equity = equity

        write_equity_to_db(equity_file, current_date, cash, shares, equity)

        current_date = (current_date_dt + timedelta(days=1)).strftime('%Y-%m-%d')

    print(f"Total Trades Executed: {total_trades}")

    total_equity = cash + (shares * ending_price)
    total_returns = ((total_equity - initial_cash) / initial_cash) * 100

    print(f"Final Equity: {total_equity}")
    print(f"Total Percentage Returns: {total_returns}%")


if len(sys.argv) != 10:
    print("Usage: python tradeSimulator.py <symbol> <start_date> <end_date> <interval> <simulate_start_date> <simulate_end_date> <threshold> <initial_cash> <initial_period_length>")
    sys.exit(1)

symbol = sys.argv[1].upper()
start_date = sys.argv[2].strip()
end_date = sys.argv[3].strip()
interval = sys.argv[4].strip()
simulate_start_date = sys.argv[5].strip()
simulate_end_date = sys.argv[6].strip()
threshold = float(sys.argv[7].strip())
initial_cash = float(sys.argv[8].strip())
initial_period_length = int(sys.argv[9].strip())

simulate_trading(symbol, start_date, end_date, interval, simulate_start_date, simulate_end_date, threshold, initial_cash)

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
    """
    Initializes the equity file with columns: date, cash, and equity.
    """
    os.makedirs(os.path.dirname(equity_file), exist_ok=True)

    conn = sqlite3.connect(equity_file)
    c = conn.cursor()

    # Create the new table structure without 'shares' column
    c.execute('''
        CREATE TABLE IF NOT EXISTS equity (
            date TEXT,
            cash REAL,
            equity REAL
        )
    ''')

    conn.commit()
    conn.close()

def write_equity_to_db(equity_file, current_date, cash, equity):
    """
    Writes the equity data for each day into the equity database.
    Excludes the 'shares' column.
    """
    conn = sqlite3.connect(equity_file)
    c = conn.cursor()

    # Remove shares from insertion statement
    c.execute("INSERT INTO equity (date, cash, equity) VALUES (?, ?, ?)",
              (current_date, cash, equity))

    conn.commit()
    conn.close()


def write_trade_to_db(trades_file, symbol, date, action, shares, price, profit):
    """
    Writes a trade entry into the database.
    :param trades_file: Path to the trades database file.
    :param symbol: The stock symbol for which the trade is being recorded.
    :param date: The date of the trade.
    :param action: Either 'BUY' or 'SELL'.
    :param shares: Number of shares traded.
    :param price: Price at which the trade was executed.
    :param profit: The profit for a sell action or 0 for a buy action.
    """
    conn = sqlite3.connect(trades_file)
    c = conn.cursor()

    # Ensure the table is created if not exists
    c.execute(f'''
        CREATE TABLE IF NOT EXISTS {symbol}_trades (
            date TEXT,
            action TEXT,
            shares INTEGER,
            price REAL,
            profit REAL
        )
    ''')

    # Insert the trade details into the table
    c.execute(f"INSERT INTO {symbol}_trades (date, action, shares, price, profit) VALUES (?, ?, ?, ?, ?)",
              (date, action, shares, price, profit))

    conn.commit()
    conn.close()


def get_last_trade_data(equity_file):
    conn = sqlite3.connect(equity_file)
    c = conn.cursor()

    c.execute("SELECT date, cash, equity FROM equity ORDER BY date DESC LIMIT 1")
    last_trade = c.fetchone()
    conn.close()

    if last_trade:
        return last_trade[1], last_trade[2]  # cash, equity
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

def get_historical_prices(db_path, start_date, simulate_start_date):
    """
    Retrieves historical prices from the database between the start_date and simulate_start_date.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT stock_price FROM stock_prices WHERE price_date BETWEEN ? AND ? ORDER BY price_date, price_time",
        (start_date, simulate_start_date),
    )
    historical_prices = [row[0] for row in cursor.fetchall()]
    conn.close()
    return historical_prices

def initialize_trade_summary_file(trades_file, symbol):
    """
    Initializes the trade summary file for a given stock symbol with columns: date, daily_profit, winning_sells, losing_sells, and daily_success_percent.
    If the table already exists, it will be dropped and recreated to ensure correct schema.
    """
    os.makedirs(os.path.dirname(trades_file), exist_ok=True)

    conn = sqlite3.connect(trades_file)
    c = conn.cursor()

    # Drop the existing table if it exists to ensure correct schema
    c.execute(f"DROP TABLE IF EXISTS {symbol}_trades")

    # Create the new table structure with the correct column names
    c.execute(f'''
        CREATE TABLE IF NOT EXISTS {symbol}_trades (
            date TEXT,
            daily_profit REAL,
            winning_sells REAL,
            losing_sells REAL,
            daily_success_percent REAL
        )
    ''')

    conn.commit()
    conn.close()

def write_daily_summary_to_db(trades_file, symbol, date, daily_profit, winning_sells, losing_sells, daily_success_percent):
    """
    Writes the daily summary for trading activity into the trades table.
    The summary includes date, daily profit, total winning sells, total losing sells, and daily success percent.
    """
    conn = sqlite3.connect(trades_file)
    c = conn.cursor()

    # Insert the new columns for date, daily_profit, winning_sells, losing_sells, and daily success percentage
    c.execute(f"INSERT INTO {symbol}_trades (date, daily_profit, winning_sells, losing_sells, daily_success_percent) VALUES (?, ?, ?, ?, ?)",
              (date, daily_profit, winning_sells, losing_sells, daily_success_percent))

    conn.commit()
    conn.close()

def simulate_trading(symbols, start_date, end_date, interval, simulate_start_date, simulate_end_date, threshold, initial_cash):
    # Initialize shared cash pool and starting equity
    total_cash = initial_cash  # Shared cash across all stocks
    starting_equity = initial_cash
    combined_equity = initial_cash

    # Initialize tracking variables for each stock
    stock_data = {symbol: {'shares': 0, 'purchase_history': [], 'daily_profit': 0, 'winning_sells': 0, 'losing_sells': 0} for symbol in symbols}
    trade_data = {symbol: {'total_trades': 0} for symbol in symbols}

    # Create tables for each stock in trades.db and initialize the equity file
    trades_file = os.path.join(os.path.dirname(__file__), '../data/tradeData/trades.db')
    equity_file = os.path.join(os.path.dirname(__file__), '../data/tradeData/equity.db')
    clear_trade_data_file(trades_file)
    clear_trade_data_file(equity_file)

    # Initialize trade summary tables for each stock and the equity file
    for symbol in symbols:
        initialize_trade_summary_file(trades_file, symbol)
    initialize_equity_file(equity_file)

    # Create a single database for each stock and calculate initial bands
    initial_bands = {}
    for symbol in symbols:
        full_db_path = get_db_path(symbol, start_date, simulate_end_date, interval)
        if not database_exists(full_db_path):
            print(f"Database for {symbol} does not exist. Creating database from {start_date} to {simulate_end_date}...")
            create_database(symbol, start_date, simulate_end_date, interval)

            # Wait for database to populate
            indicator_states = ['   ', '.  ', '.. ', '...']
            indicator_index = 0
            while not (database_exists(full_db_path) and check_db_populated(full_db_path)):
                sys.stdout.write(f"\rWaiting for {symbol} database to be populated{indicator_states[indicator_index]}")
                sys.stdout.flush()
                time.sleep(1)
                indicator_index = (indicator_index + 1) % len(indicator_states)

            sys.stdout.write(f"\rDatabase for {symbol} populated.                              \n")

        # Calculate initial bands using historical data up to the simulation start date for each symbol
        historical_prices = get_historical_prices(full_db_path, start_date, simulate_start_date)
        if len(historical_prices) < 14:  # Ensure we have at least 14 days of data for Bollinger Bands
            print(f"Not enough historical data to calculate initial bands for {symbol}.")
            return
        lower_band, weighted_moving_average, upper_band = calculate_weighted_bollinger_bands(historical_prices, window=14)
        initial_bands[symbol] = (lower_band, weighted_moving_average, upper_band)

    # Simulate trading for each minute using minute-by-minute data from the current day onwards for all symbols
    simulate_db_conn = {symbol: sqlite3.connect(get_db_path(symbol, start_date, simulate_end_date, interval)) for symbol in symbols}
    simulate_cursor = {symbol: simulate_db_conn[symbol].cursor() for symbol in symbols}

    current_date = simulate_start_date
    while current_date <= simulate_end_date:
        current_date_dt = datetime.strptime(current_date, '%Y-%m-%d')

        if is_market_closed(current_date_dt):
            current_date = (current_date_dt + timedelta(days=1)).strftime('%Y-%m-%d')
            continue

        # Reset daily profit and performance for each symbol at the start of the day
        for symbol in symbols:
            stock_data[symbol]['daily_profit'] = 0
            stock_data[symbol]['winning_sells'] = 0
            stock_data[symbol]['losing_sells'] = 0

        # Fetch data for each symbol
        stock_prices_per_minute = {}
        for symbol in symbols:
            simulate_cursor[symbol].execute("SELECT stock_price, volume, price_date, price_time FROM stock_prices WHERE price_date = ? ORDER BY price_time",
                                            (current_date,))
            stock_data_for_symbol = simulate_cursor[symbol].fetchall()
            if not stock_data_for_symbol:
                continue
            stock_prices_per_minute[symbol] = stock_data_for_symbol

        # **Step 1**: Sell phase - Evaluate sell opportunities for all stocks first
        for symbol, stock_info in stock_prices_per_minute.items():
            for price, volume, date, time_ in stock_info:
                lower_band, _, upper_band = initial_bands[symbol]

                # Calculate dynamic profit margin based on cash levels
                min_profit_margin = 0.1 / 100  # 0.1% minimum profit margin
                max_profit_margin = 2 / 100  # 2% maximum profit margin
                cash_ratio = total_cash / initial_cash  # Calculate the ratio of current cash to initial cash

                # Calculate the dynamic profit threshold
                dynamic_profit_margin = min_profit_margin + (max_profit_margin - min_profit_margin) * cash_ratio
                if dynamic_profit_margin < min_profit_margin:
                    dynamic_profit_margin = min_profit_margin
                elif dynamic_profit_margin > max_profit_margin:
                    dynamic_profit_margin = max_profit_margin

                # Sell decision: when the price is above or equal to the upper band and we have shares to sell
                if price >= upper_band and stock_data[symbol]['shares'] > 0:
                    shares_to_sell = stock_data[symbol]['shares']
                    cash_gained = shares_to_sell * price
                    total_buy_cost = sum([p * q for p, q in stock_data[symbol]['purchase_history']])

                    # Calculate the realized profit based on the initial purchase history
                    profit = cash_gained - total_buy_cost

                    # Only sell if the profit exceeds the dynamically calculated profit margin
                    if profit >= total_buy_cost * dynamic_profit_margin:
                        total_cash += cash_gained
                        stock_data[symbol]['shares'] = 0
                        stock_data[symbol]['purchase_history'] = []  # Reset purchase history after selling
                        stock_data[symbol]['daily_profit'] += profit

                        # Update winning/losing sells based on profit
                        if profit > 0:
                            stock_data[symbol]['winning_sells'] += profit
                        else:
                            stock_data[symbol]['losing_sells'] += profit

                        trade_data[symbol]['total_trades'] += 1

                # Update bands for the next minute using the new price
                historical_prices.append(price)
                lower_band, _, upper_band = calculate_weighted_bollinger_bands(historical_prices[-14:], window=14)
                initial_bands[symbol] = (lower_band, _, upper_band)

        # **Step 2**: Buy phase - Evaluate buy opportunities only after all sells are done
        for symbol, stock_info in stock_prices_per_minute.items():
            for price, volume, date, time_ in stock_info:
                lower_band, _, upper_band = initial_bands[symbol]

                # Buy decision: when the price is below or equal to the lower band and we have enough cash available
                if price <= lower_band and total_cash >= price:
                    shares_to_buy = int(total_cash // price)
                    if shares_to_buy > 0:
                        stock_data[symbol]['shares'] += shares_to_buy
                        cash_spent = shares_to_buy * price
                        total_cash -= cash_spent
                        stock_data[symbol]['purchase_history'].append((price, shares_to_buy))
                        trade_data[symbol]['total_trades'] += 1

                # Update bands for the next minute using the new price
                historical_prices.append(price)
                lower_band, _, upper_band = calculate_weighted_bollinger_bands(historical_prices[-14:], window=14)
                initial_bands[symbol] = (lower_band, _, upper_band)

        # Calculate combined end-of-day equity for all stocks
        combined_equity = total_cash
        for symbol in symbols:
            if stock_data[symbol]['shares'] > 0:
                closing_price = stock_prices_per_minute.get(symbol, [(None, None, None, None)])[-1][0]
                if closing_price:
                    combined_equity += stock_data[symbol]['shares'] * closing_price

        # Write end-of-day summary entry for each symbol
        for symbol in symbols:
            daily_profit = stock_data[symbol]['daily_profit']
            winning_sells = stock_data[symbol]['winning_sells']
            losing_sells = stock_data[symbol]['losing_sells']

            total_sells_value = winning_sells + abs(losing_sells)
            daily_success_percent = (winning_sells - abs(losing_sells)) / total_sells_value * 100 if total_sells_value > 0 else 0

            write_daily_summary_to_db(trades_file, symbol, current_date, daily_profit, winning_sells, losing_sells, daily_success_percent)

        # Write combined equity into `equity.db` at the end of the day
        write_equity_to_db(equity_file, current_date, total_cash, combined_equity)

        current_date = (current_date_dt + timedelta(days=1)).strftime('%Y-%m-%d')

    # Final report
    print(f"Total Trades Executed: {sum(trade_data[symbol]['total_trades'] for symbol in symbols)}")

    # Display average daily success percentage and total profit for each symbol
    conn = sqlite3.connect(trades_file)
    c = conn.cursor()
    overall_success_percentages = []
    overall_profit = 0
    for symbol in symbols:
        c.execute(f"SELECT AVG(daily_success_percent) FROM {symbol}_trades")
        avg_success_percentage = c.fetchone()[0] or 0.0
        overall_success_percentages.append(avg_success_percentage)

        # Calculate total profit for each symbol
        c.execute(f"SELECT SUM(daily_profit) FROM {symbol}_trades")
        total_profit = c.fetchone()[0] or 0.0
        overall_profit += total_profit
        print(f"Average Daily Success Percentage for {symbol}: {avg_success_percentage:.2f}%, Total Profit: {total_profit:.2f}")

    # Calculate and display overall results
    overall_avg_success_percentage = sum(overall_success_percentages) / len(overall_success_percentages)
    print(f"Overall Average Daily Success Percentage: {overall_avg_success_percentage:.2f}%")
    print(f"Overall Starting Equity: {starting_equity}")
    print(f"Overall Final Equity: {combined_equity:.2f}")
    print(f"Overall Percentage Returns: {((combined_equity - starting_equity) / starting_equity) * 100:.2f}%")
    print(f"Overall Total Profit: {combined_equity - starting_equity:.2f}")
    conn.close()

# Parsing and handling multiple symbols correctly
if len(sys.argv) != 10:
    print("Usage: python tradeSimulator.py <symbols> <start_date> <end_date> <interval> <simulate_start_date> <simulate_end_date> <threshold> <initial_cash> <initial_period_length>")
    sys.exit(1)

# Parse symbols as a list
symbols = sys.argv[1].upper().split(",")  # Split the symbols string into a list
start_date = sys.argv[2].strip()
end_date = sys.argv[3].strip()
interval = sys.argv[4].strip()
simulate_start_date = sys.argv[5].strip()
simulate_end_date = sys.argv[6].strip()
threshold = float(sys.argv[7].strip())
initial_cash = float(sys.argv[8].strip())
initial_period_length = int(sys.argv[9].strip())

simulate_trading(symbols, start_date, end_date, interval, simulate_start_date, simulate_end_date, threshold, initial_cash)

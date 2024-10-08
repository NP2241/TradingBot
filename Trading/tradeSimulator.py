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

def initialize_equity_file(equity_file):
    """
    Initializes the equity file with columns: date, cash, equity, buys, and sells.
    """
    os.makedirs(os.path.dirname(equity_file), exist_ok=True)

    conn = sqlite3.connect(equity_file)
    c = conn.cursor()

    # Create the new table structure with 'buys' and 'sells' columns
    c.execute('''
        CREATE TABLE IF NOT EXISTS equity (
            date TEXT,
            cash REAL,
            equity REAL,
            buys INTEGER,
            sells INTEGER
        )
    ''')

    conn.commit()
    conn.close()

def write_equity_to_db(equity_file, current_date, cash, equity, daily_buys, daily_sells):
    """
    Writes the equity data for each day into the equity database.
    Includes 'buys' and 'sells' columns.
    """
    conn = sqlite3.connect(equity_file)
    c = conn.cursor()

    # Insert the data into the equity table, including 'buys' and 'sells'
    c.execute("INSERT INTO equity (date, cash, equity, buys, sells) VALUES (?, ?, ?, ?, ?)",
              (current_date, cash, equity, daily_buys, daily_sells))

    conn.commit()
    conn.close()

def initialize_trade_summary_file(trades_file, symbol):
    """
    Initialize a trade summary table for each stock symbol in the trades file.
    Adds the new columns 'buys' and 'sells' to track daily buys and sells.
    """
    conn = sqlite3.connect(trades_file)
    c = conn.cursor()

    # Create a table for the specific stock symbol with new 'buys' and 'sells' columns
    c.execute(f'''
        CREATE TABLE IF NOT EXISTS {symbol}_trades (
            date TEXT,
            daily_profit REAL,
            winning_sells REAL,
            losing_sells REAL,
            daily_success_percent REAL,
            buys INTEGER,
            sells INTEGER
        )
    ''')
    conn.commit()
    conn.close()

def write_daily_summary_to_db(trades_file, symbol, date, daily_profit, winning_sells, losing_sells, daily_success_percent, daily_buys, daily_sells):
    """
    Write daily summary data into the trades file for each stock symbol, including buys and sells.
    """
    conn = sqlite3.connect(trades_file)
    c = conn.cursor()

    # Insert daily summary for the stock, including the new buys and sells columns
    c.execute(f'''
        INSERT INTO {symbol}_trades (date, daily_profit, winning_sells, losing_sells, daily_success_percent, buys, sells)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (date, daily_profit, winning_sells, losing_sells, daily_success_percent, daily_buys, daily_sells))

    conn.commit()
    conn.close()
    print(f"Written daily summary for {symbol} on {date} into trades.db")  # Debug print statement

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
            stock_data[symbol]['winning_sells'] = 0  # Reset winning sells count for the day
            stock_data[symbol]['losing_sells'] = 0  # Reset losing sells count for the day

        # Track total buys and sells across all symbols for the day
        daily_buys = 0
        daily_sells = 0

        # Track buys and sells for each individual stock
        stock_daily_buys = {symbol: 0 for symbol in symbols}
        stock_daily_sells = {symbol: 0 for symbol in symbols}

        # Fetch data for each symbol
        stock_prices_per_minute = {}
        for symbol in symbols:
            simulate_cursor[symbol].execute(
                "SELECT stock_price, volume, price_date, price_time FROM stock_prices WHERE price_date = ? ORDER BY price_time",
                (current_date,))
            stock_data_for_symbol = simulate_cursor[symbol].fetchall()
            if not stock_data_for_symbol:
                continue
            stock_prices_per_minute[symbol] = stock_data_for_symbol

        # Store available cash at the beginning of the minute to avoid using cash from current-minute sells
        cash_at_minute_start = total_cash

        # Track the best buy opportunity within the current minute
        best_buy_symbol = None
        best_buy_price = float('inf')

        # Evaluate buy and sell opportunities for each stock in the same loop
        for symbol, stock_info in stock_prices_per_minute.items():
            for price, volume, date, time_ in stock_info:
                lower_band, weighted_moving_average, upper_band = initial_bands[symbol]

                # Calculate dynamic profit/loss tolerance based on cash levels
                min_loss_tolerance = -3 / 100  # 3% loss tolerance
                min_profit_margin = 0.05 / 100  # 0.05% minimum profit margin

                # --- SELL LOGIC ---
                # Sell decision: when the price is above or equal to the upper band
                if price >= upper_band and stock_data[symbol]['shares'] > 0:
                    shares_to_sell = stock_data[symbol]['shares']
                    if shares_to_sell > 0:
                        cash_gained = shares_to_sell * price
                        total_buy_cost = sum([p * q for p, q in stock_data[symbol]['purchase_history']])

                        # Calculate the realized profit or loss based on the initial purchase history
                        profit_or_loss = cash_gained - total_buy_cost

                        # Check if the sell is either profitable or meets the loss tolerance requirement
                        if profit_or_loss >= total_buy_cost * min_profit_margin or profit_or_loss >= total_buy_cost * min_loss_tolerance:
                            total_cash += cash_gained
                            stock_data[symbol]['shares'] = 0
                            stock_data[symbol]['purchase_history'] = []  # Reset purchase history after selling
                            stock_data[symbol]['daily_profit'] += profit_or_loss

                            # Track cash gained or lost for winning or losing sells
                            if profit_or_loss > 0:
                                stock_data[symbol]['winning_sells'] += profit_or_loss
                            else:
                                stock_data[symbol]['losing_sells'] += abs(profit_or_loss)

                            # Increment daily sells for both the total and individual stock
                            daily_sells += shares_to_sell
                            stock_daily_sells[symbol] += shares_to_sell

                            # Record the total trades executed for the stock
                            trade_data[symbol]['total_trades'] += 1

                # --- BUY LOGIC ---
                # Identify the best buy opportunity for the current minute
                if price <= lower_band and cash_at_minute_start >= price:
                    # Track the best buy based on the lowest price
                    if price < best_buy_price:
                        best_buy_symbol = symbol
                        best_buy_price = price

                # Update bands for the next minute using the new price
                historical_prices.append(price)
                lower_band, _, upper_band = calculate_weighted_bollinger_bands(historical_prices[-14:], window=14)
                initial_bands[symbol] = (lower_band, _, upper_band)

        # Execute the best buy for the minute (only if there is a best buy)
        if best_buy_symbol is not None and best_buy_price <= cash_at_minute_start:
            shares_to_buy = int(cash_at_minute_start // best_buy_price)
            if shares_to_buy > 0:
                stock_data[best_buy_symbol]['shares'] += shares_to_buy
                cash_spent = shares_to_buy * best_buy_price
                total_cash -= cash_spent
                stock_data[best_buy_symbol]['purchase_history'].append((best_buy_price, shares_to_buy))

                # Increment daily buys for both the total and individual stock
                daily_buys += shares_to_buy
                stock_daily_buys[best_buy_symbol] += shares_to_buy

                # Record the total trades executed for the stock
                trade_data[best_buy_symbol]['total_trades'] += 1

        # Calculate combined end-of-day equity for all stocks
        combined_equity = total_cash
        for symbol in symbols:
            if stock_data[symbol]['shares'] > 0:
                closing_price = stock_prices_per_minute.get(symbol, [(None, None, None, None)])[-1][0]
                if closing_price:
                    combined_equity += stock_data[symbol]['shares'] * closing_price

        # Write daily summary for each stock into `trades.db`
        for symbol in symbols:
            daily_profit = stock_data[symbol]['daily_profit']
            winning_sells = stock_data[symbol]['winning_sells']  # Winning sells for this stock on this day
            losing_sells = stock_data[symbol]['losing_sells']    # Losing sells for this stock on this day

            # Calculate the daily success percentage using only today's data
            total_sells_value = winning_sells + losing_sells
            daily_success_percent = (winning_sells - losing_sells) / total_sells_value * 100 if total_sells_value > 0 else 0

            # Write the daily summary into the trades database for this specific stock, including buys and sells
            write_daily_summary_to_db(trades_file, symbol, current_date, daily_profit, winning_sells, losing_sells, daily_success_percent, stock_daily_buys[symbol], stock_daily_sells[symbol])

        # Write combined equity into `equity.db` at the end of the day, including daily buys and sells
        write_equity_to_db(equity_file, current_date, total_cash, combined_equity, daily_buys, daily_sells)

        # Move to the next day
        current_date = (current_date_dt + timedelta(days=1)).strftime('%Y-%m-%d')

    # Final report
    total_trades_executed = sum(trade_data[symbol]['total_trades'] for symbol in symbols)
    print(f"Total Trades Executed: {total_trades_executed:,}")

    # Display average daily success percentage and total profit for each symbol
    conn = sqlite3.connect(trades_file)
    c = conn.cursor()
    overall_success_percentages = []
    overall_profit = 0
    for symbol in symbols:
        c.execute(f"SELECT AVG(daily_success_percent) FROM {symbol}_trades WHERE daily_profit != 0 OR winning_sells != 0 OR losing_sells != 0")
        avg_success_percentage = c.fetchone()[0] or 0.0
        overall_success_percentages.append(avg_success_percentage)

        # Calculate total profit for each symbol
        c.execute(f"SELECT SUM(daily_profit) FROM {symbol}_trades WHERE daily_profit != 0 OR winning_sells != 0 OR losing_sells != 0")
        total_profit = c.fetchone()[0] or 0.0
        overall_profit += total_profit
        print(f"Average Daily Success Percentage for {symbol}: {avg_success_percentage:,.2f}%, Total Profit: {total_profit:,.2f}")

    # Calculate and display overall results
    overall_avg_success_percentage = sum(overall_success_percentages) / len(overall_success_percentages)
    print(f"Overall Average Daily Success Percentage: {overall_avg_success_percentage:,.2f}%")
    print(f"Overall Starting Equity: {starting_equity:,.2f}")
    print(f"Overall Final Equity: {combined_equity:,.2f}")
    overall_percentage_returns = ((combined_equity - starting_equity) / starting_equity) * 100
    print(f"Overall Percentage Returns: {overall_percentage_returns:,.2f}%")
    print(f"Overall Total Profit: {combined_equity - starting_equity:,.2f}")
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

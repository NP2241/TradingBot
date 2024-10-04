import os
import sys
import sqlite3
import time
import subprocess
from datetime import datetime, timedelta
import holidays

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

def initialize_trade_data_file(trade_data_file):
    os.makedirs(os.path.dirname(trade_data_file), exist_ok=True)

    conn = sqlite3.connect(trade_data_file)
    c = conn.cursor()

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

    # Step 2: Perform initial calculations using data from start_date to end_date
    initial_db_conn = sqlite3.connect(full_db_path)
    initial_cursor = initial_db_conn.cursor()
    initial_cursor.execute("SELECT stock_price, volume FROM stock_prices WHERE price_date BETWEEN ? AND ?",
                           (start_date, end_date))
    initial_data = initial_cursor.fetchall()
    initial_db_conn.close()

    if initial_data:
        prices = [row[0] for row in initial_data]
        volumes = [row[1] for row in initial_data]
        volatility_index = calculate_volatility_index(prices, volumes)
        metrics = calculate_stock_metrics(prices, volumes)
        if volatility_index is not None and metrics is not None:
            current_price = metrics['moving_average_value']
            buy_index = calculate_buy_index(volatility_index, metrics, current_price)
        else:
            print("No data available to calculate the initial stock analysis.")
            return
    else:
        print(f"No initial data available for {symbol} from {start_date} to {end_date}.")
        return

    lower_band = min(metrics['lower_band'], metrics['upper_band'])
    upper_band = max(metrics['lower_band'], metrics['upper_band'])

    # Step 3: Simulate trading using data from simulate_start_date to simulate_end_date
    simulate_db_conn = sqlite3.connect(full_db_path)
    simulate_cursor = simulate_db_conn.cursor()

    trade_data_file = os.path.join(os.path.dirname(__file__), '../data/tradeData/trades.db')
    clear_trade_data_file(trade_data_file)
    initialize_trade_data_file(trade_data_file)

    last_trade_data = get_last_trade_data(trade_data_file)
    if last_trade_data:
        cash, shares, equity = last_trade_data
    else:
        cash = initial_cash
        shares = 0

    current_date = simulate_start_date
    prev_closing_equity = cash
    missing_dates = []  # Track missing dates

    while current_date <= simulate_end_date:
        current_date_dt = datetime.strptime(current_date, '%Y-%m-%d')

        if is_market_closed(current_date_dt):
            current_date = (current_date_dt + timedelta(days=1)).strftime('%Y-%m-%d')
            continue

        simulate_cursor.execute("SELECT stock_price, volume, price_date, price_time FROM stock_prices WHERE price_date = ?",
                                (current_date,))
        stock_data = simulate_cursor.fetchall()

        if not stock_data:
            # Collect missing date
            missing_dates.append(current_date_dt)
            current_date = (current_date_dt + timedelta(days=1)).strftime('%Y-%m-%d')
            continue

        # Trading logic: buy/sell decisions based on price
        for price, volume, date, time_ in stock_data:
            if price <= lower_band * (1 + (threshold / 100)):
                shares_to_buy = cash // price
                if shares_to_buy > 0:
                    shares += shares_to_buy
                    cash -= shares_to_buy * price
            elif price >= upper_band * (1 - (threshold / 100)) and shares > 0:
                cash += shares * price
                shares = 0

        ending_price = stock_data[-1][0]
        equity = cash + (shares * ending_price)
        returns_percentage = ((equity - prev_closing_equity) / prev_closing_equity) * 100
        prev_closing_equity = equity

        write_trade_to_db(trade_data_file, current_date, cash, shares, equity)

        current_date = (current_date_dt + timedelta(days=1)).strftime('%Y-%m-%d')

        # Update metrics for the next day
        volatility_index, metrics = calculate_stock_analysis(full_db_path)
        if volatility_index is not None and metrics is not None:
            lower_band = min(metrics['lower_band'], metrics['upper_band'])
            upper_band = max(metrics['lower_band'], metrics['upper_band'])

    # Print consolidated ranges for missing data
    if missing_dates:
        start_date = missing_dates[0]
        prev_date = start_date

        for i in range(1, len(missing_dates)):
            current_date = missing_dates[i]
            if (current_date - prev_date).days == 1:
                # Consecutive date, continue the range
                prev_date = current_date
            else:
                # Print the range and start a new one
                if start_date == prev_date:
                    print(f"No data available for {symbol} on {start_date.strftime('%Y-%m-%d')}.")
                else:
                    print(f"No data available for {symbol} from {start_date.strftime('%Y-%m-%d')} to {prev_date.strftime('%Y-%m-%d')}.")
                start_date = current_date
                prev_date = current_date

        # Print the last range
        if start_date == prev_date:
            print(f"No data available for {symbol} on {start_date.strftime('%Y-%m-%d')}.")
        else:
            print(f"No data available for {symbol} from {start_date.strftime('%Y-%m-%d')} to {prev_date.strftime('%Y-%m-%d')}.")

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

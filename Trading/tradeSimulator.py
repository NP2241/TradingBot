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

# This function creates the historical database
def create_simulation_database(symbol, start_date, end_date):
    script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../DatabaseSetup/historicalDatabase.py'))
    command = [sys.executable, script_path, symbol, start_date, end_date]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode == 0:
        print(f"Database for {symbol} created from {start_date} to {end_date}.")
    else:
        print(f"Error creating database for {symbol} from {start_date} to {end_date}: {result.stderr}")

# Ensure the historical database exists
def ensure_database_exists(symbol, start_date, end_date):
    db_path = get_db_path(symbol, start_date, end_date, '1m')
    if not database_exists(db_path):
        print(f"Database for {symbol} does not exist. Creating it from {start_date} to {end_date}...")
        create_simulation_database(symbol, start_date, end_date)

        # Display waiting status on a single line, updated continuously
        indicator_states = ['|', '/', '-', '\\']
        indicator_index = 0
        while not (database_exists(db_path) and check_db_populated(db_path)):
            sys.stdout.write(f"\rWaiting for database to be populated {indicator_states[indicator_index]}")
            sys.stdout.flush()
            time.sleep(1)
            indicator_index = (indicator_index + 1) % len(indicator_states)

        # Clear the waiting message
        sys.stdout.write("\rDatabase populated successfully.                           \n")
    return db_path

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
    if date.weekday() >= 5:  # Weekend
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

def simulate_trading(symbol, start_date, end_date, interval, simulate_start_date, simulate_end_date, threshold, initial_cash):
    db_path = ensure_database_exists(symbol, start_date, simulate_end_date)

    # Perform analysis on data from start_date to end_date
    volatility_index, metrics = calculate_stock_analysis(db_path)
    if volatility_index is not None and metrics is not None:
        current_price = metrics['moving_average_value']
        buy_index = calculate_buy_index(volatility_index, metrics, current_price)
    else:
        print("No data available to calculate stock analysis.")
        return

    lower_band = min(metrics['lower_band'], metrics['upper_band'])
    upper_band = max(metrics['lower_band'], metrics['upper_band'])

    simulate_db_path = get_db_path(symbol, simulate_start_date, simulate_end_date, interval)
    if not database_exists(simulate_db_path):
        print(f"Creating simulation database for {symbol} from {simulate_start_date} to {simulate_end_date}...")
        create_simulation_database(symbol, simulate_start_date, simulate_end_date)

    while not (database_exists(simulate_db_path) and check_table_exists(simulate_db_path)):
        sys.stdout.write(f"\rWaiting for simulation database to be populated...")
        sys.stdout.flush()
        time.sleep(1)

    trade_data_file = os.path.join(os.path.dirname(__file__), '../data/tradeData/trades.db')
    clear_trade_data_file(trade_data_file)
    initialize_trade_data_file(trade_data_file)

    last_trade_data = get_last_trade_data(trade_data_file)
    cash, shares = last_trade_data if last_trade_data else (initial_cash, 0)
    prev_closing_equity = cash

    current_date = simulate_start_date
    while current_date <= simulate_end_date:
        current_date_dt = datetime.strptime(current_date, '%Y-%m-%d')

        if is_market_closed(current_date_dt):
            current_date = (current_date_dt + timedelta(days=1)).strftime('%Y-%m-%d')
            continue

        conn = sqlite3.connect(simulate_db_path)
        c = conn.cursor()
        c.execute("SELECT stock_price, volume, price_date, price_time FROM stock_prices WHERE price_date = ?", (current_date,))
        stock_data = c.fetchall()
        conn.close()

        if not stock_data:
            current_date = (current_date_dt + timedelta(days=1)).strftime('%Y-%m-%d')
            continue

        for price, volume, date, time_ in stock_data:
            recent_prices = [data[0] for data in stock_data][-20:]
            if len(recent_prices) >= 20:
                lower_band, moving_average, upper_band = calculate_bollinger_bands(recent_prices)

            if price <= lower_band * (1 + (threshold / 100)) and cash >= price:
                shares += 1
                cash -= price

            elif price >= upper_band * (1 - (threshold / 100)) and shares > 0:
                cash += shares * price
                shares = 0

        equity = cash + (shares * price)
        prev_closing_equity = equity
        write_trade_to_db(trade_data_file, current_date, cash, shares, equity)

        sys.stdout.write(f"\rSimulating trading for {current_date}...    ")
        sys.stdout.flush()

        current_date = (current_date_dt + timedelta(days=1)).strftime('%Y-%m-%d')

    total_equity = cash + (shares * price)
    total_returns = ((total_equity - initial_cash) / initial_cash) * 100

    print(f"\nFinal Equity: {total_equity}")
    print(f"Total Percentage Returns: {total_returns}%")

# Main program entry point
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

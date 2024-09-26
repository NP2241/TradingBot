import os
import sys
import sqlite3
import time
from datetime import datetime, timedelta
import holidays

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../DataAnalysis')))

from stockAnalysis import calculate_buy_index, database_exists, calculate_stock_analysis
from stockMetrics import calculate_bollinger_bands

# Check if a database for the symbol exists (ignoring date ranges)
def find_existing_symbol_db(symbol):
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data'))
    for file in os.listdir(data_dir):
        if file.startswith(symbol) and file.endswith('.db'):
            return os.path.join(data_dir, file)
    return None

# Find the actual start and end dates in the database
def get_db_start_end_dates(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Find the minimum (start) and maximum (end) price_day in the database
    c.execute("SELECT MIN(price_day), MAX(price_day) FROM stock_prices")
    start_date, end_date = c.fetchone()

    conn.close()

    return start_date, end_date

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

def simulate_trading(symbol, start_date, end_date, simulate_start_date, simulate_end_date, threshold, initial_cash):
    # Check if a database for the symbol exists
    db_path = find_existing_symbol_db(symbol)
    if not db_path:
        print(f"No existing database for {symbol}. Exiting.")
        sys.exit(1)

    print(f"Using database for {symbol}: {db_path}")

    # Check that the table exists in the database
    if not check_table_exists(db_path):
        print(f"No valid stock_prices table found in database {db_path}. Exiting.")
        sys.exit(1)

    # Find the actual start_date and end_date in the database
    db_start_date, db_end_date = get_db_start_end_dates(db_path)
    print(f"Database contains data from {db_start_date} to {db_end_date}")

    # Ensure the start_date and end_date are within the actual data range
    if start_date < db_start_date:
        print(f"Analysis start date is earlier than available data. Adjusting to {db_start_date}")
        start_date = db_start_date

    if end_date > db_end_date:
        print(f"Analysis end date is later than available data. Adjusting to {db_end_date}")
        end_date = db_end_date

    # Ensure the simulation start and end dates are within the actual data range
    if simulate_start_date < db_start_date:
        print(f"Simulation start date is earlier than available data. Adjusting to {db_start_date}")
        simulate_start_date = db_start_date

    if simulate_end_date > db_end_date:
        print(f"Simulation end date is later than available data. Adjusting to {db_end_date}")
        simulate_end_date = db_end_date

    # Perform analysis on data from start_date to end_date (initial calculations)
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT stock_price FROM stock_prices WHERE price_day BETWEEN ? AND ?", (start_date, end_date))
    analysis_data = c.fetchall()
    conn.close()

    if analysis_data:
        recent_prices = [row[0] for row in analysis_data]
        if len(recent_prices) >= 20:
            lower_band, moving_average, upper_band = calculate_bollinger_bands(recent_prices)
        else:
            print("Not enough data to calculate Bollinger Bands.")
            return
    else:
        print(f"No data available for analysis between {start_date} and {end_date}")
        return

    print(f"Bollinger Bands: Lower = {lower_band}, Moving Average = {moving_average}, Upper = {upper_band}")

    # Start simulation
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

        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("SELECT stock_price, volume, price_day, price_time FROM stock_prices WHERE price_day = ?", (current_date,))
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
if len(sys.argv) == 9:
    symbol = sys.argv[1].upper()
    start_date = sys.argv[2].strip()
    end_date = sys.argv[3].strip()
    interval = sys.argv[4].strip()
    simulate_start_date = sys.argv[5].strip()
    simulate_end_date = sys.argv[6].strip()
    threshold = float(sys.argv[7].strip())
    initial_cash = float(sys.argv[8].strip())

    simulate_trading(symbol, start_date, end_date, simulate_start_date, simulate_end_date, threshold, initial_cash)
else:
    print("Usage: python tradeSimulator.py <symbol> <start_date> <end_date> <interval> <simulate_start_date> <simulate_end_date> <threshold> <initial_cash>")

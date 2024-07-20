import os
import sys
import sqlite3
import time
import subprocess
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../DataAnalysis')))

from stockAnalysis import calculate_buy_index, get_db_path, database_exists, create_database, check_db_populated, calculate_stock_analysis
from stockMetrics import calculate_bollinger_bands

def create_simulation_database(symbol, start_date, end_date, interval):
    script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../DatabaseSetup/setupDatabase.py'))
    command = [sys.executable, script_path, symbol, "yes", start_date, interval, end_date]
    print(f"Running command to create range database: {' '.join(command)}")
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

def initialize_trade_data_file(trade_data_file, start_date, initial_cash):
    if not os.path.exists(trade_data_file):
        with open(trade_data_file, 'w') as f:
            f.write("date,cash,shares,equity\n")
            f.write(f"{start_date},{initial_cash},0,{initial_cash}\n")
        print(f"Trade data file initialized with starting values for {start_date}")

def write_trade_data(trade_data_file, date, cash, shares, equity):
    with open(trade_data_file, 'a') as f:
        f.write(f"{date},{cash},{shares},{equity}\n")
    print(f"Trade data written: {date},{cash},{shares},{equity}")  # Debug print statement

def read_last_trade_data(trade_data_file):
    with open(trade_data_file, 'r') as f:
        lines = f.readlines()
        last_line = lines[-1].strip()
        _, cash, shares, equity = last_line.split(',')
        return float(cash), int(shares), float(equity)

def simulate_trading(symbol, start_date, end_date, interval, simulate_start_date, simulate_end_date, threshold, initial_cash):
    # Ensure the database for the range exists
    db_path = get_db_path(symbol, start_date, end_date, interval)
    db_name = os.path.basename(db_path)
    if not database_exists(db_path):
        print(f"Database for {symbol} does not exist. Creating database...")
        create_database(symbol, start_date, end_date, interval)

        while not (database_exists(db_path) and check_db_populated(db_path)):
            print("Waiting for database to be populated...")
            time.sleep(1)

    print(f"Database {db_name} exists and is populated.")
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
    simulate_db_name = os.path.basename(simulate_db_path)
    if not database_exists(simulate_db_path):
        print(f"Creating database for {symbol} from {simulate_start_date} to {simulate_end_date}...")
        create_simulation_database(symbol, simulate_start_date, simulate_end_date, interval)

    while not (database_exists(simulate_db_path) and check_table_exists(simulate_db_path)):
        print("Waiting for simulation database to be populated...")
        time.sleep(1)

    trade_data_file = os.path.join(os.path.dirname(__file__), '../data/tradeData/trades.db')
    clear_trade_data_file(trade_data_file)
    initialize_trade_data_file(trade_data_file, simulate_start_date, initial_cash)

    cash, shares, prev_closing_equity = read_last_trade_data(trade_data_file)

    # Iterate through each simulation day
    current_date = simulate_start_date

    while current_date <= simulate_end_date:
        # Read the data from the simulation date range database
        conn = sqlite3.connect(simulate_db_path)
        c = conn.cursor()
        c.execute("SELECT stock_price, volume, price_date, price_time FROM stock_prices WHERE price_date = ?", (current_date,))
        stock_data = c.fetchall()
        conn.close()

        if not stock_data:
            print(f"No data available for {symbol} on {current_date} even after creation.")
            return

        for price, volume, date, time_ in stock_data:
            if price <= lower_band * (1 + (threshold / 100)):  # Buy condition within the specified percentage of the lower band
                shares_to_buy = cash // price
                if shares_to_buy > 0:
                    shares += shares_to_buy
                    cash -= shares_to_buy * price
            elif price >= upper_band * (1 - (threshold / 100)) and shares > 0:  # Sell condition within the specified percentage of the upper band
                cash += shares * price
                shares = 0

        ending_price = stock_data[-1][0]  # Closing price on the simulation date
        equity = cash + (shares * ending_price)
        returns_percentage = ((equity - prev_closing_equity) / prev_closing_equity) * 100
        prev_closing_equity = equity

        write_trade_data(trade_data_file, current_date, cash, shares, equity)

        current_date = (datetime.strptime(current_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

        # Recalculate the Bollinger Bands
        volatility_index, metrics = calculate_stock_analysis(db_path)
        if volatility_index is not None and metrics is not None:
            lower_band = min(metrics['lower_band'], metrics['upper_band'])
            upper_band = max(metrics['lower_band'], metrics['upper_band'])

    total_equity = cash + (shares * ending_price)
    total_returns = ((total_equity - initial_cash) / initial_cash) * 100

    print(f"Final Equity: {total_equity}")
    print(f"Total Percentage Returns: {total_returns}%")

if __name__ == "__main__":
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

import os
import sys
import sqlite3
import time
import subprocess
import shutil
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../DataAnalysis')))

from stockAnalysis import calculate_buy_index, get_db_path, database_exists, create_database, check_db_populated, calculate_stock_analysis
from stockMetrics import calculate_bollinger_bands

def create_single_day_database(symbol, date, interval):
    script_path = os.path.join(os.path.dirname(__file__), '../DatabaseSetup/setupDatabase.py')
    command = [sys.executable, script_path, symbol, "yes", date, interval]
    subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

def check_table_exists(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stock_prices'")
    result = c.fetchone()
    conn.close()
    return result is not None

def load_trading_state(trade_data_path):
    if not os.path.exists(trade_data_path):
        return 10000, 0  # Default starting cash and shares

    conn = sqlite3.connect(trade_data_path)
    c = conn.cursor()
    c.execute("SELECT cash, shares FROM trading_state ORDER BY id DESC LIMIT 1")
    result = c.fetchone()
    conn.close()

    if result:
        return result
    else:
        return 10000, 0  # Default starting cash and shares

def save_trading_state(trade_data_path, cash, shares, symbol):
    conn = sqlite3.connect(trade_data_path)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trading_state (id INTEGER PRIMARY KEY, cash REAL, shares REAL, symbol TEXT)''')
    c.execute("DELETE FROM trading_state")  # Clear previous entries
    c.execute("INSERT INTO trading_state (cash, shares, symbol) VALUES (?, ?, ?)", (cash, shares, symbol))
    conn.commit()
    conn.close()

def clear_trade_data_folder(trade_data_folder):
    if os.path.exists(trade_data_folder):
        for filename in os.listdir(trade_data_folder):
            file_path = os.path.join(trade_data_folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')

def simulate_trading(symbol, start_date, end_date, interval, simulate_start_date, simulate_end_date, threshold):
    # Clear the trade data folder
    trade_data_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data/tradeData'))
    os.makedirs(trade_data_folder, exist_ok=True)
    clear_trade_data_folder(trade_data_folder)

    # Ensure the database for the range exists
    db_path = get_db_path(symbol, start_date, end_date, interval)
    if not database_exists(db_path):
        print(f"Database for {symbol} does not exist. Creating database...")
        create_database(symbol, start_date, end_date, interval)

        while not (database_exists(db_path) and check_db_populated(db_path)):
            time.sleep(0.1)

    volatility_index, metrics = calculate_stock_analysis(db_path)
    if volatility_index is not None and metrics is not None:
        current_price = metrics['moving_average_value']  # Assuming the current price is the latest moving average
        buy_index = calculate_buy_index(volatility_index, metrics, current_price)
    else:
        print("No data available to calculate the stock analysis.")
        return

    lower_band = min(metrics['lower_band'], metrics['upper_band'])
    upper_band = max(metrics['lower_band'], metrics['upper_band'])

    current_date = simulate_start_date

    # Load initial trading state
    cash, shares = load_trading_state(os.path.join(trade_data_folder, f"{simulate_start_date}_trades.db"))

    starting_cash = cash  # Track initial starting cash
    ending_price = 0

    while current_date <= simulate_end_date:
        trade_data_path = os.path.join(trade_data_folder, f"{current_date}_trades.db")

        # Create the single day database if it doesn't exist
        single_day_db_path = get_db_path(symbol, current_date, interval=interval)
        if not database_exists(single_day_db_path):
            print(f"Creating single-day database for {symbol} on {current_date}...")
            create_single_day_database(symbol, current_date, interval)

        while not (database_exists(single_day_db_path) and check_table_exists(single_day_db_path)):
            time.sleep(0.1)

        # Read the data from the single day database
        conn = sqlite3.connect(single_day_db_path)
        c = conn.cursor()
        c.execute("SELECT stock_price, volume, price_date, price_time FROM stock_prices WHERE price_date = ?", (current_date,))
        stock_data = c.fetchall()
        conn.close()

        if not stock_data:
            print(f"No data available for {symbol} on {current_date} even after creation.")
            return

        # Track starting equity for the day
        start_equity = cash + (shares * stock_data[0][0])
        print(f"Starting equity for {current_date}: {start_equity}")

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
        returns_percentage = ((equity - start_equity) / start_equity) * 100

        # Print the one-line output for the day
        print(f"Date: {current_date}, Daily Percentage Returns: {returns_percentage:.2f}%")

        # Save trading state at the end of the day
        save_trading_state(trade_data_path, cash, shares, symbol)

        # Move to the next day
        current_date = (datetime.strptime(current_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    # Final calculation of equity and returns after the last simulation day
    final_equity = cash + (shares * ending_price)
    total_returns_percentage = ((final_equity - starting_cash) / starting_cash) * 100

    print(f"Final Equity: {final_equity}")
    print(f"Total Percentage Returns: {total_returns_percentage}%")

if __name__ == "__main__":
    if len(sys.argv) != 8:
        print("Usage: python tradeSimulator.py <symbol> <start_date> <end_date> <interval> <simulate_start_date> <simulate_end_date> <threshold>")
        sys.exit(1)

    symbol = sys.argv[1].upper()
    start_date = sys.argv[2].strip()
    end_date = sys.argv[3].strip()
    interval = sys.argv[4].strip()
    simulate_start_date = sys.argv[5].strip()
    simulate_end_date = sys.argv[6].strip()
    threshold = float(sys.argv[7].strip())

    simulate_trading(symbol, start_date, end_date, interval, simulate_start_date, simulate_end_date, threshold)

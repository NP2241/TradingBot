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

def create_range_database(symbol, start_date, end_date, interval):
    script_path = os.path.join(os.path.dirname(__file__), '../DatabaseSetup/setupDatabase.py')
    command = [sys.executable, script_path, symbol, "yes", start_date, interval, end_date]
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
    c.execute("SELECT cash, shares FROM trading_state ORDER BY date DESC LIMIT 1")
    result = c.fetchone()
    conn.close()

    if result:
        return result
    else:
        return 10000, 0  # Default starting cash and shares

def save_trading_state(trade_data_path, date, cash, shares, equity):
    conn = sqlite3.connect(trade_data_path)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trading_state (date TEXT, cash REAL, shares REAL, equity REAL)''')
    c.execute("INSERT INTO trading_state (date, cash, shares, equity) VALUES (?, ?, ?, ?)", (date, cash, shares, equity))
    conn.commit()
    conn.close()

def clear_trade_data_file(trade_data_file):
    if os.path.exists(trade_data_file):
        os.remove(trade_data_file)

def calculate_bollinger_bands_from_db(db_path, symbol, end_date, window_size=20, num_std_dev=2):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT stock_price FROM stock_prices WHERE price_date <= ? ORDER BY price_date DESC, price_time DESC LIMIT ?", (end_date, window_size))
    prices = [row[0] for row in c.fetchall()]
    conn.close()

    if len(prices) < window_size:
        return None, None, None

    moving_average = sum(prices) / window_size
    std_dev = (sum([(price - moving_average) ** 2 for price in prices]) / window_size) ** 0.5
    upper_band = moving_average + (num_std_dev * std_dev)
    lower_band = moving_average - (num_std_dev * std_dev)

    return lower_band, moving_average, upper_band

def simulate_trading(symbol, start_date, end_date, interval, simulate_start_date, simulate_end_date, threshold):
    # Clear the trade data file
    trade_data_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data/tradeData/trade_data.db'))
    os.makedirs(os.path.dirname(trade_data_file), exist_ok=True)
    clear_trade_data_file(trade_data_file)

    # Ensure the database for the simulation range exists
    simulate_db_path = get_db_path(symbol, simulate_start_date, simulate_end_date, interval)
    if not database_exists(simulate_db_path):
        print(f"Database for {symbol} does not exist. Creating database...")
        create_range_database(symbol, simulate_start_date, simulate_end_date, interval)

        while not (database_exists(simulate_db_path) and check_db_populated(simulate_db_path)):
            time.sleep(0.1)

    current_date = simulate_start_date

    starting_cash = 10000  # Track initial starting cash
    ending_price = 0

    prev_closing_equity = None

    while current_date <= simulate_end_date:
        # For the first day, load the initial trading state
        if current_date == simulate_start_date:
            cash, shares = load_trading_state(trade_data_file)
        else:
            # For subsequent days, load the state from the previous day's entry in the trade data file
            conn = sqlite3.connect(trade_data_file)
            c = conn.cursor()
            c.execute("SELECT cash, shares, equity FROM trading_state ORDER BY date DESC LIMIT 1")
            previous_state = c.fetchone()
            conn.close()
            if previous_state:
                cash, shares, prev_closing_equity = previous_state
            else:
                cash, shares = 10000, 0
                prev_closing_equity = None

        # Calculate metrics based on the range database
        lower_band, moving_average, upper_band = calculate_bollinger_bands_from_db(simulate_db_path, symbol, current_date)
        if lower_band is None or upper_band is None:
            print("Not enough data to calculate Bollinger Bands.")
            return

        # Read the data from the range database for the current day
        conn = sqlite3.connect(simulate_db_path)
        c = conn.cursor()
        c.execute("SELECT stock_price, volume, price_date, price_time FROM stock_prices WHERE price_date = ?", (current_date,))
        stock_data = c.fetchall()
        conn.close()

        if not stock_data:
            print(f"No data available for {symbol} on {current_date} even after creation.")
            return

        # Track starting equity for the day
        start_equity = cash + (shares * stock_data[0][0])
        if prev_closing_equity is None:
            prev_closing_equity = start_equity

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
        ending_equity = cash + (shares * ending_price)

        if prev_closing_equity is not None:
            returns_percentage = ((ending_equity - prev_closing_equity) / prev_closing_equity) * 100
        else:
            returns_percentage = ((ending_equity - starting_cash) / starting_cash) * 100

        # Save trading state at the end of the day
        save_trading_state(trade_data_file, current_date, cash, shares, ending_equity)

        # Update prev_closing_equity for the next day
        prev_closing_equity = ending_equity

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

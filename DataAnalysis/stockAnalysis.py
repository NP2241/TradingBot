import os
import sqlite3
import subprocess
import sys
import statistics
import numpy as np

def database_exists(db_path):
    return os.path.exists(db_path)

def create_database(symbol, start_date, end_date, interval):
    script_path = os.path.join(os.path.dirname(__file__), '../DatabaseSetup/setupDatabase.py')
    command = [sys.executable, script_path, symbol, "yes", start_date, interval, end_date]
    subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

def get_db_path(symbol, start_date, end_date, interval):
    db_filename = f"{symbol}_{start_date.replace('-', '.')}_{end_date.replace('-', '.')}_{interval.replace(' ', '').replace(':', '').replace('-', '')}.db"
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data', db_filename))
    return db_path

def calculate_bollinger_bands(prices, window_size=20, num_std_dev=2):
    rolling_mean = np.mean(prices[-window_size:])
    rolling_std = np.std(prices[-window_size:])
    upper_band = rolling_mean + (rolling_std * num_std_dev)
    lower_band = rolling_mean - (rolling_std * num_std_dev)
    return rolling_mean, upper_band, lower_band

def calculate_stock_analysis(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT stock_price FROM stock_prices")
    prices = [row[0] for row in c.fetchall()]
    conn.close()

    if prices:
        # Calculate metrics
        std_dev = statistics.stdev(prices)
        historical_volatility = np.std(np.diff(np.log(prices))) * np.sqrt(len(prices))
        atr = np.mean([max(prices[i] - prices[i-1], abs(prices[i] - prices[i-1]), abs(prices[i-1] - prices[i])) for i in range(1, len(prices))])
        cv = std_dev / statistics.mean(prices) * 100

        # Calculate Bollinger Bands
        _, upper_band, lower_band = calculate_bollinger_bands(prices)
        bollinger_band_width = upper_band - lower_band

        # Weight the metrics
        weights = {
            'std_dev': 0.2,  # 20%
            'historical_volatility': 0.2,  # 20%
            'atr': 0.2,  # 20%
            'cv': 0.1,  # 10%
            'bollinger_band_width': 0.3  # 30%
        }

        # Custom volatility index (weighted average)
        volatility_index = (
                weights['std_dev'] * std_dev +
                weights['historical_volatility'] * historical_volatility +
                weights['atr'] * atr +
                weights['cv'] * cv +
                weights['bollinger_band_width'] * bollinger_band_width
        )

        print(f"The custom volatility index is: {volatility_index}")
    else:
        print("No data available to calculate the stock analysis.")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python stockAnalysis.py <symbol> <start_date> <end_date> <interval>")
        sys.exit(1)

    symbol = sys.argv[1].upper()
    start_date = sys.argv[2].strip()
    end_date = sys.argv[3].strip()
    interval = sys.argv[4].strip()

    db_path = get_db_path(symbol, start_date, end_date, interval)

    if not database_exists(db_path):
        create_database(symbol, start_date, end_date, interval)

    calculate_stock_analysis(db_path)

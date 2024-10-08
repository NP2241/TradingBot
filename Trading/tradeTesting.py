import subprocess
import os

def run_trade_simulator():
    symbols = ["AAPL", "AMZN", "NFLX", "GOOGL", "META"]  # List of symbols
    start_date = "2022-10-30"
    end_date = "2023-01-01"
    interval = "1m"
    simulate_start_date = "2023-01-02"
    simulate_end_date = "2024-10-01"
    threshold = 1  # 1% threshold within Bollinger Bands to trigger sells and buys
    initial_cash = 10000  # Initial cash amount
    initial_period_length = 60  # Period for initial parameter calculation

    # Convert symbols list to a single string to pass as an argument
    symbols_str = ",".join(symbols)

    script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'tradeSimulator.py'))
    command = [
        "/usr/local/bin/python3",
        script_path,
        symbols_str,  # Pass symbols as a single argument
        start_date,
        end_date,
        interval,
        simulate_start_date,
        simulate_end_date,
        str(threshold),
        str(initial_cash),
        str(initial_period_length)
    ]
    subprocess.run(command, check=True)

if __name__ == "__main__":
    run_trade_simulator()

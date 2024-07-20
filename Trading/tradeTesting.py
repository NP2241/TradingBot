import subprocess
import os

def run_trade_simulator():
    symbol = "AAPL"
    start_date = "2024-07-02"
    end_date = "2024-07-09"
    interval = "1m"
    simulate_start_date = "2024-07-10"
    simulate_end_date = "2024-07-12"
    threshold = 1  # 1% threshold
    initial_cash = 10000  # Initial cash amount

    script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'tradeSimulator.py'))
    command = [
        "/usr/local/bin/python3",
        script_path,
        symbol,
        start_date,
        end_date,
        interval,
        simulate_start_date,
        simulate_end_date,
        str(threshold),
        str(initial_cash)
    ]
    subprocess.run(command, check=True)

if __name__ == "__main__":
    run_trade_simulator()

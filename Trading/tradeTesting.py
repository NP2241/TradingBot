import subprocess
import os

def run_trade_simulator():
    symbol = "SPY"
    start_date = "2018-12-01"
    end_date = "2019-01-01"
    interval = "1m"
    simulate_start_date = "2019-01-02"
    simulate_end_date = "2024-09-24"
    threshold = 1  # 1% threshold within bollinger bands to trigger sells and buys
    initial_cash = 100000  # Initial cash amount

    script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'tradeSimulator.py'))
    command = [
        "python3",
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

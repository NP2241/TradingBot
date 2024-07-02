import subprocess
import os

def run_trade_simulator():
    symbol = "AAPL"
    start_date = "2024-06-17"
    end_date = "2024-06-24"
    interval = "1m"
    simulate_start_date = "2024-06-25"
    simulate_end_date = "2024-06-26"
    threshold = 1  # 1% threshold

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
        str(threshold)
    ]
    subprocess.run(command, check=True)

if __name__ == "__main__":
    run_trade_simulator()

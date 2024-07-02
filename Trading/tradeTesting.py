import subprocess
import os

def run_trade_simulator():
    script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'tradeSimulator.py'))
    command = [
        "/usr/local/bin/python3",
        script_path,
        "AAPL",
        "2024-06-17",
        "2024-06-24",
        "1m",
        "2024-06-25"
    ]
    subprocess.run(command, check=True)

if __name__ == "__main__":
    run_trade_simulator()

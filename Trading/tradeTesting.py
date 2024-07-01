import subprocess
import sys

def run_trade_simulator():
    command = [sys.executable, "tradeSimulator.py", "AAPL", "2024-06-17", "2024-06-21", "1m"]
    subprocess.run(command, check=True)

if __name__ == "__main__":
    run_trade_simulator()

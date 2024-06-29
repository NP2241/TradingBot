import subprocess
import os
import sys

def get_script_dir():
    return os.path.dirname(os.path.realpath(__file__))

def run_analysis_tester():
    script_dir = get_script_dir()
    stock_analysis_path = os.path.join(script_dir, "stockAnalysis.py")

    symbol = "AAPL"
    start_date = "2024-06-17"
    end_date = "2024-06-24"
    interval = "1h"

    command = [sys.executable, stock_analysis_path, symbol, start_date, end_date, interval]

    subprocess.run(command)

if __name__ == "__main__":
    run_analysis_tester()

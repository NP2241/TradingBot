import subprocess
import sys
import os

def get_script_dir():
    return os.path.dirname(os.path.realpath(__file__))

def main():
    if len(sys.argv) < 5:
        print("Usage: python stockAnalysis.py <symbol> <start_date> <end_date> <interval>")
        sys.exit(1)

    symbol = sys.argv[1]
    start_date = sys.argv[2]
    end_date = sys.argv[3]
    interval = sys.argv[4]

    project_root = os.path.abspath(os.path.join(get_script_dir(), ".."))
    setup_database_path = os.path.join(project_root, "DatabaseSetup", "setupDatabase.py")

    command = [
        sys.executable, setup_database_path, symbol, "yes", start_date, interval, end_date
    ]

    print(f"Running command: {' '.join(command)}")
    subprocess.run(command)

if __name__ == "__main__":
    main()

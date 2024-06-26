import subprocess
import sys
import os

def get_script_dir():
    return os.path.dirname(os.path.realpath(__file__))

def run_setup_database_single_day(print_all=False):
    # Full path to the Python interpreter
    python_path = sys.executable  # Use the current Python executable

    # Define the path to setup_database.py
    script_dir = get_script_dir()
    setup_database_path = os.path.join(script_dir, "setupDatabase.py")

    # Define the command and arguments for a single day with hourly interval
    command = [python_path, setup_database_path, "AAPL", "yes", "2024-06-24"]

    print(f"Running command: {' '.join(command)} in directory: {script_dir}")

    # Run the setupDatabase.py script with the specified arguments and capture the output
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Split the output into lines
    output_lines = result.stdout.split('\n')
    error_lines = result.stderr.split('\n')

    print("Testing setupDatabase.py for single day (2024-06-24) with hourly data:")
    if print_all:
        for line in output_lines:
            print(line)
        for line in error_lines:
            print(line)
    else:
        for line in output_lines[:2]:
            print(line)
        if len(output_lines) > 5:
            print("...")  # Indicate that the output is truncated
        for line in output_lines[-3:]:
            print(line)

        for line in error_lines[:2]:
            print(line)
        if len(error_lines) > 5:
            print("...")  # Indicate that the output is truncated
        for line in error_lines[-3:]:
            print(line)

    # Print the database path from the output
    for line in output_lines:
        if "Database saved at:" in line:
            print(line)

def run_setup_database_date_range(print_all=False):
    # Full path to the Python interpreter
    python_path = sys.executable  # Use the current Python executable

    # Define the path to setup_database.py
    script_dir = get_script_dir()
    setup_database_path = os.path.join(script_dir, "setupDatabase.py")

    # Define the command and arguments for a date range with hourly interval
    command = [python_path, setup_database_path, "AAPL", "yes", "2024-06-21", "2024-06-24"]

    print(f"Running command: {' '.join(command)} in directory: {script_dir}")

    # Run the setupDatabase.py script with the specified arguments and capture the output
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Split the output into lines
    output_lines = result.stdout.split('\n')
    error_lines = result.stderr.split('\n')

    print("\nTesting setupDatabase.py for date range (2024-06-21 to 2024-06-24) with hourly data:")
    if print_all:
        for line in output_lines:
            print(line)
        for line in error_lines:
            print(line)
    else:
        for line in output_lines[:2]:
            print(line)
        if len(output_lines) > 5:
            print("...")  # Indicate that the output is truncated
        for line in output_lines[-3:]:
            print(line)

        for line in error_lines[:2]:
            print(line)
        if len(error_lines) > 5:
            print("...")  # Indicate that the output is truncated
        for line in error_lines[-3:]:
            print(line)

    # Print the database path from the output
    for line in output_lines:
        if "Database saved at:" in line:
            print(line)

if __name__ == "__main__":
    # Check for command line arguments
    if len(sys.argv) > 1 and sys.argv[1].lower() == 'all':
        run_setup_database_single_day(print_all=True)
        run_setup_database_date_range(print_all=True)
    else:
        run_setup_database_single_day()
        run_setup_database_date_range()

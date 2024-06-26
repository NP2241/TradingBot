import subprocess

def run_price_tracker_single_day(print_all=False):
    # Full path to the Python interpreter
    python_path = "/usr/local/bin/python3"

    # Define the command and arguments for a single day
    command = [python_path, "priceTracker.py", "AAPL", "yes", "2024-06-24"]

    # Run the priceTracker.py script with the specified arguments and capture the output
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Split the output into lines
    output_lines = result.stdout.split('\n')

    print("Testing single day (2024-06-24):")
    if print_all:
        for line in output_lines:
            print(line)
    else:
        for line in output_lines[:2]:
            print(line)
        if len(output_lines) > 5:
            print("...")  # Indicate that the output is truncated
        for line in output_lines[-3:]:
            print(line)

def run_price_tracker_date_range(print_all=False):
    # Full path to the Python interpreter
    python_path = "/usr/local/bin/python3"

    # Define the command and arguments for a date range
    command = [python_path, "priceTracker.py", "AAPL", "yes", "2024-06-21", "2024-06-24"]

    # Run the priceTracker.py script with the specified arguments and capture the output
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Split the output into lines
    output_lines = result.stdout.split('\n')

    print("\nTesting date range (2024-06-21 to 2024-06-24):")
    if print_all:
        for line in output_lines:
            print(line)
    else:
        for line in output_lines[:2]:
            print(line)
        if len(output_lines) > 5:
            print("...")  # Indicate that the output is truncated
        for line in output_lines[-3:]:
            print(line)

if __name__ == "__main__":
    import sys

    # Check for command line arguments
    if len(sys.argv) > 1 and sys.argv[1].lower() == 'all':
        run_price_tracker_single_day(print_all=True)
        run_price_tracker_date_range(print_all=True)
    else:
        run_price_tracker_single_day()
        run_price_tracker_date_range()

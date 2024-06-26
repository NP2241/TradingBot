import subprocess

def run_price_tracker(print_all=False): # Change print_all to conditionally crop the output
    # Full path to the Python interpreter
    python_path = "/usr/local/bin/python3"

    # Define the command and arguments
    command = [python_path, "priceTracker.py", "AAPL", "yes", "2024-06-24"]

    # Run the priceTracker.py script with the specified arguments and capture the output
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Split the output into lines
    output_lines = result.stdout.split('\n')

    if print_all:
        for line in output_lines:
            print(line)
    else:
        for line in output_lines[:20]:
            print(line)

if __name__ == "__main__":
    import sys

    # Check for command line arguments
    if len(sys.argv) > 1 and sys.argv[1].lower() == 'all':
        run_price_tracker(print_all=True)
    else:
        run_price_tracker()

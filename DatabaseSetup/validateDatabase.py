import os
import sys
import sqlite3
from datetime import datetime, timedelta
import holidays
from dateutil.relativedelta import relativedelta

# Set up US holidays
us_holidays = holidays.US()

# Function to check if a given date is a trading day
def is_trading_day(date):
    """
    Returns True if the given date is a trading day (not a weekend or US holiday).
    """
    return date.weekday() < 5 and date not in us_holidays

# Function to get the list of distinct trading days from the database
def get_dates_from_db(db_path):
    """
    Retrieve the list of distinct trading days from the database.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Query to get all distinct price_date entries from the stock_prices table
    c.execute("SELECT DISTINCT price_date FROM stock_prices ORDER BY price_date ASC")
    dates = c.fetchall()

    conn.close()

    # Convert the tuples into a list of datetime.date objects
    return [datetime.strptime(row[0], '%Y-%m-%d').date() for row in dates]

# Function to calculate the expected number of trading days between two dates
def calculate_expected_trading_days(start_date, end_date):
    """
    Calculate the number of expected trading days (excluding weekends and holidays) between two dates.
    """
    current_date = start_date
    expected_trading_days = 0

    while current_date <= end_date:
        if is_trading_day(current_date):
            expected_trading_days += 1
        current_date += timedelta(days=1)

    return expected_trading_days

# Function to get the duration in months and days between two dates
def get_duration(start, end):
    """
    Get the duration between two dates in months and days.
    """
    delta = relativedelta(end, start)
    months = delta.months
    days = delta.days
    return months, days

# Function to check for exact duplicate entries in the database
def find_exact_duplicates(db_path):
    """
    Check the database for any exact duplicate entries.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Query to find exact duplicate rows based on all fields except primary key
    c.execute("""
        SELECT stock_name, stock_price, volume, price_time, price_date, COUNT(*) as count 
        FROM stock_prices 
        GROUP BY stock_name, stock_price, volume, price_time, price_date 
        HAVING count > 1
    """)
    duplicates = c.fetchall()

    conn.close()

    return duplicates

# Function to validate the database for missing and duplicate data
def validate_database(db_name):
    """
    Validate the database for missing and exact duplicate trading days.
    """
    # Construct the full path to the database file assuming it's in the data/ folder
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', db_name))

    if not os.path.exists(db_path):
        print(f"Database file not found: {db_name}")
        return

    # Fetch distinct trading days from the database
    dates = get_dates_from_db(db_path)

    if not dates:
        print("No data found in the database.")
        return

    # Define the start and end dates of the data in the database
    start_date = dates[0]
    end_date = dates[-1]
    print(f"First data entry: {start_date}")
    print(f"Last data entry: {end_date}")

    missing_ranges = []
    current_date = start_date

    # Loop through the dates and identify any gaps larger than 1 trading day
    for i in range(1, len(dates)):
        next_date = dates[i]
        day_diff = (next_date - current_date).days

        # Check for gaps larger than 1 day
        if day_diff > 1:
            # Identify the missing trading days in the gap
            missing_days = []
            for j in range(1, day_diff):
                potential_missing_date = current_date + timedelta(days=j)
                if is_trading_day(potential_missing_date):
                    missing_days.append(potential_missing_date)

            # If we found any missing trading days, add them to the missing ranges
            if missing_days:
                print(f"Found missing days: {[day.strftime('%Y-%m-%d') for day in missing_days]}")  # Print missing days
                missing_ranges.append((current_date, next_date))

        current_date = next_date

    # Check for exact duplicate entries
    duplicates = find_exact_duplicates(db_path)
    if duplicates:
        print("\nExact duplicate data found for the following entries:")
        for stock_name, stock_price, volume, price_time, price_date, count in duplicates:
            print(f" - {stock_name} on {price_date} at {price_time} with price {stock_price} and volume {volume}: {count} entries")
    else:
        print("\nNo exact duplicate data found.")

    # Calculate expected vs. actual trading days
    expected_trading_days = calculate_expected_trading_days(start_date, end_date)
    actual_trading_days = len(dates)

    print(f"\nExpected trading days between {start_date} and {end_date}: {expected_trading_days}")
    print(f"Actual trading days in database: {actual_trading_days}")

    coverage_percentage = (actual_trading_days / expected_trading_days) * 100

    # Print out the missing ranges with months and days
    if missing_ranges:
        print("\nMissing data found in the following ranges (excluding weekends and holidays):")
        for start, end in missing_ranges:
            months, days = get_duration(start, end)
            print(f"Gap between {start} and {end} (Duration: {months} months, {days} days)")
    else:
        print("\nNo missing data found.")

    print(f"\nData coverage: {coverage_percentage:.2f}%")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python validate_database.py <database_name>")
        sys.exit(1)

    db_name = sys.argv[1]  # Only the database file name, not the full path
    validate_database(db_name)

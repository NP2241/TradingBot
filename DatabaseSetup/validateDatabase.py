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
    if date.weekday() >= 5 or date in us_holidays:  # Weekend or US holiday
        return False
    return True

# Function to get the list of dates from the database
def get_dates_from_db(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Query to get all distinct price_day entries from the stock_prices table
    c.execute("SELECT DISTINCT price_day FROM stock_prices ORDER BY price_day ASC")
    dates = c.fetchall()

    conn.close()

    # Convert the tuples into a list of datetime.date objects
    return [datetime.strptime(row[0], '%Y-%m-%d').date() for row in dates]

# Function to calculate the expected number of trading days
def calculate_expected_trading_days(start_date, end_date):
    current_date = start_date
    expected_trading_days = 0

    while current_date <= end_date:
        if is_trading_day(current_date):
            expected_trading_days += 1
        current_date += timedelta(days=1)

    return expected_trading_days

# Function to get the duration in months and days
def get_duration(start, end):
    delta = relativedelta(end, start)
    months = delta.months
    days = delta.days
    return months, days

# Function to validate the database for missing data
def validate_database(db_name):
    # Construct the full path to the database file assuming it's in the data/ folder
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', db_name))

    if not os.path.exists(db_path):
        print(f"Database file not found: {db_name}")
        return

    dates = get_dates_from_db(db_path)

    if not dates:
        print("No data found in the database.")
        return

    start_date = dates[0]
    end_date = dates[-1]
    print(f"First data entry: {start_date}")
    print(f"Last data entry: {end_date}")

    missing_ranges = []
    current_date = start_date

    # Loop through all dates and find missing trading days
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

            if missing_days:
                missing_ranges.append((current_date, next_date))

        current_date = next_date

    # Calculate expected vs actual trading days
    expected_trading_days = calculate_expected_trading_days(start_date, end_date)
    actual_trading_days = len(dates)

    coverage_percentage = (actual_trading_days / expected_trading_days) * 100

    # Print out the missing ranges with months and days
    if missing_ranges:
        print("Missing data found in the following ranges (excluding weekends and holidays):")
        for start, end in missing_ranges:
            months, days = get_duration(start, end)
            print(f"Gap between {start} and {end} (Duration: {months} months, {days} days)")
    else:
        print("No missing data found.")

    print(f"\nExpected trading days: {expected_trading_days}")
    print(f"Actual trading days in database: {actual_trading_days}")
    print(f"Data coverage: {coverage_percentage:.2f}%")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python validate_database.py <database_name>")
        sys.exit(1)

    db_name = sys.argv[1]  # Only the database file name, not the full path
    validate_database(db_name)

#!/usr/bin/env python3
"""
Simple test to verify the monthly detection logic in the actual context
"""
from datetime import datetime, timedelta
import json

def load_config():
    """Load configuration from file JSON."""
    with open("config/config.json", 'r') as f:
        return json.load(f)

def test_monthly_detection():
    config = load_config()
    
    # Get dates from config
    config_start_date = config['twitter'].get('start_date')
    config_end_date = config['twitter'].get('end_date')

    if config_start_date and config_end_date:
        start_date_obj = datetime.strptime(config_start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(config_end_date, '%Y-%m-%d')
    
    print(f"Config start date: {start_date_obj.strftime('%Y-%m-%d')}")
    print(f"Config end date: {end_date_obj.strftime('%Y-%m-%d')}")

    # Calculate the number of days to process
    date_range = end_date_obj - start_date_obj
    total_days = date_range.days + 1

    print(f"Total days: {total_days}")

    # Check if the range is monthly - either by duration (>31 days) or by being full calendar month
    start_of_month = start_date_obj.replace(day=1)
    end_of_month = (start_of_month.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    is_full_month = (start_date_obj.day == 1 and end_date_obj.date() == end_of_month.date())
    
    # Additional check: if both dates are in the same month and cover most/all days of the month
    same_month = (start_date_obj.month == end_date_obj.month and start_date_obj.year == end_date_obj.year)
    days_in_month = (start_date_obj.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    days_in_target_month = (days_in_month.day)
    is_most_of_month = (same_month and total_days >= days_in_target_month * 0.75)  # If covers 75%+ of the month

    print(f"Is full month: {is_full_month}")
    print(f"Same month: {same_month}")
    print(f"Days in target month: {days_in_target_month}")
    print(f"Is most of month: {is_most_of_month}")
    print(f"Should be treated as monthly: {total_days > 31 or is_full_month or is_most_of_month}")
    
    if total_days > 31 or is_full_month or is_most_of_month:
        print("\n>>> This SHOULD be treated as MONTHLY scraping! <<<")
    else:
        print("\n>>> This will be treated as DAILY scraping! <<<")

if __name__ == "__main__":
    test_monthly_detection()
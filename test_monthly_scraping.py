#!/usr/bin/env python3
"""
Test script to verify monthly scraping functionality
"""

from resilient_etl import run_etl
from datetime import datetime

def test_monthly_scraping():
    print("Testing monthly scraping functionality...")
    
    # Test monthly scraping with 32 days (which should trigger monthly processing)
    print("\n1. Testing with monthly range (September 1-30, 2024)...")
    try:
        run_etl("2024-09-01", "2024-09-30")
        print("✅ Monthly scraping test completed successfully")
    except Exception as e:
        print(f"❌ Monthly scraping test failed: {e}")
    
    print("\n2. Testing with daily range (just a few days)...")
    try:
        run_etl("2024-09-25", "2024-09-27")  # Just 3 days
        print("✅ Daily scraping test completed successfully")
    except Exception as e:
        print(f"❌ Daily scraping test failed: {e}")
    
    print("\nTesting completed!")

if __name__ == "__main__":
    test_monthly_scraping()
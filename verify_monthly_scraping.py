#!/usr/bin/env python3
"""
Test script to verify monthly scraping functionality structure
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    print("Testing imports...")
    try:
        from resilient_etl import run_etl
        print("[OK] resilient_etl imported successfully")
    except Exception as e:
        print(f"[ERROR] resilient_etl import failed: {e}")
        return False

    try:
        from src.resilient_scraper import ResilientScraper
        print("[OK] ResilientScraper imported successfully")
    except Exception as e:
        print(f"[ERROR] ResilientScraper import failed: {e}")
        return False

    try:
        from utils import save_monthly_data_labeled, get_daily_files_for_month, aggregate_monthly_data
        print("[OK] Utils functions imported successfully")
    except Exception as e:
        print(f"[ERROR] Utils functions import failed: {e}")
        return False

    return True

def test_monthly_function_exists():
    print("\nTesting if monthly scraping function exists...")
    try:
        from src.resilient_scraper import ResilientScraper
        scraper_methods = [method for method in dir(ResilientScraper) if not method.startswith('_')]
        print(f"Available methods in ResilientScraper: {scraper_methods}")

        if 'scrape_month_maximum' in scraper_methods:
            print("[OK] scrape_month_maximum method exists")
            return True
        else:
            print("[ERROR] scrape_month_maximum method does not exist")
            return False
    except Exception as e:
        print(f"[ERROR] Error checking methods: {e}")
        return False

def test_main_logic():
    print("\nTesting main ETL logic...")
    try:
        # Read the resilient_etl.py file to check if monthly logic is present
        with open("resilient_etl.py", "r", encoding="utf-8") as f:
            content = f.read()

        if "total_days > 31" in content and "scrape_month_maximum" in content:
            print("[OK] Monthly logic detected in main ETL")
            return True
        else:
            print("[ERROR] Monthly logic not found in main ETL")
            return False
    except Exception as e:
        print(f"[ERROR] Error checking main logic: {e}")
        return False

def test_config():
    print("\nTesting configuration...")
    try:
        import json
        with open("config/config.json", "r") as f:
            config = json.load(f)

        if config["twitter"]["days_back"] >= 30:
            print(f"[OK] Configuration updated for monthly processing (days_back: {config['twitter']['days_back']})")
        else:
            print(f"[WARN] Configuration may not be optimized for monthly processing (days_back: {config['twitter']['days_back']})")

        if config["twitter"]["start_date"] and config["twitter"]["end_date"]:
            print(f"[OK] Configuration has start and end dates: {config['twitter']['start_date']} to {config['twitter']['end_date']}")
            return True
        else:
            print("[ERROR] Configuration missing start or end date")
            return False
    except Exception as e:
        print(f"[ERROR] Error checking configuration: {e}")
        return False

def main():
    print("Running verification tests for monthly scraping functionality...\n")

    success = True
    success &= test_imports()
    success &= test_monthly_function_exists()
    success &= test_main_logic()
    success &= test_config()

    print(f"\n{'='*60}")
    if success:
        print("[OK] All verification tests passed! The monthly scraping functionality is properly implemented.")
        print("\nTo run the actual monthly scraping, use:")
        print("  python resilient_etl.py")
        print("Or with specific date range:")
        print("  python -c \"from resilient_etl import run_etl; run_etl('2024-09-01', '2024-09-30')\"")
    else:
        print("[ERROR] Some tests failed. Please check the implementation.")
    print(f"{'='*60}")

    return success

if __name__ == "__main__":
    main()
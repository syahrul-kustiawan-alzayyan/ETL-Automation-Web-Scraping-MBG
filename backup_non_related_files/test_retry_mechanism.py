#!/usr/bin/env python3
"""
Test script untuk menguji mekanisme retry pada scraper
"""

import json
import logging
from datetime import datetime
import undetected_chromedriver as uc
from src.resilient_scraper import ResilientScraper
from utils import DailyCollectionManager

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/test_retry.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TestRetry")

def test_retry_mechanism():
    """Fungsi untuk menguji mekanisme retry"""
    logger.info("Memulai test mekanisme retry")
    
    # Muat konfigurasi
    with open('config/config.json', 'r') as f:
        config = json.load(f)
    
    # Setup driver
    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-extensions')
    options.add_argument('--profile-directory=Default')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    options.add_argument('--disable-web-security')
    options.add_argument('--allow-running-insecure-content')

    driver = uc.Chrome(options=options, version_main=None)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    try:
        # Setup collection manager
        collection_manager = DailyCollectionManager(config)
        
        # Buat scraper
        scraper = ResilientScraper(driver, config, collection_manager)
        
        # Inject cookies
        scraper.inject_cookies()
        
        # Navigate to search
        target_date = datetime.now().date()
        scraper.navigate_to_search(target_date)
        
        # Test deteksi "Something went wrong"
        print("Testing deteksi 'Something went wrong'...")
        has_error = scraper.detect_something_went_wrong()
        print(f"Apakah ada pesan error? {has_error}")
        
        # Test retry mechanism
        print("Testing retry mechanism...")
        retry_success = scraper.handle_retry_mechanism(max_retries=3)
        print(f"Apakah retry berhasil? {retry_success}")
        
        logger.info("Test selesai")
        
    finally:
        driver.quit()

if __name__ == "__main__":
    test_retry_mechanism()
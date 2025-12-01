"""
max_tweets_etl.py
File utama untuk ETL dengan fokus maksimum tweet per hari
"""

import json
import logging
import time
from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# Pustaka Eksternal
import undetected_chromedriver as uc

# Load config
CONFIG_FILE = "config/config.json"

def load_config():
    """Memuat konfigurasi dari file JSON."""
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

CONFIG = load_config()

# Setup Logging
logging.basicConfig(
    level=getattr(logging, CONFIG['logging']['level']),
    format=CONFIG['logging']['format'],
    handlers=[
        logging.FileHandler(CONFIG['logging']['file']),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("SummaryApp")

def setup_driver():
    """Mengkonfigurasi Undetected Chromedriver."""
    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-extensions')
    options.add_argument('--profile-directory=Default')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    options.add_argument('--disable-web-security')
    options.add_argument('--allow-running-insecure-content')

    logger.info("Memulai Chromium Driver...")
    driver = uc.Chrome(options=options, version_main=None)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def init_db():
    """Menginisialisasi koneksi MongoDB."""
    try:
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))

        from utils import DailyCollectionManager
        client = MongoClient(CONFIG['database']['mongo_uri'])
        collection_manager = DailyCollectionManager(CONFIG)
        logger.info("Koneksi MongoDB berhasil diinisialisasi.")
        return client, collection_manager
    except PyMongoError as e:
        logger.critical(f"Gagal terhubung ke MongoDB: {e}")
        exit(1)

def run_etl():
    client, collection_manager = init_db()
    driver = setup_driver()

    try:
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))

        from src.super_efficient_scraper import SuperEfficientScraper

        # Inisialisasi scraper super efisien
        scraper = SuperEfficientScraper(driver, CONFIG, collection_manager)

        # Login
        scraper.inject_cookies()

        # Tentukan rentang tanggal
        days_back = CONFIG['twitter']['days_back']
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        logger.info(f"Memproses tweet dari {start_date.strftime('%Y-%m-%d')} hingga {end_date.strftime('%Y-%m-%d')}")

        # Loop harian
        current_date = start_date.date()
        end_date_date = end_date.date()

        total_all_days = 0
        while current_date <= end_date_date:
            logger.info(f"Memulai scraping maksimum untuk {current_date}")
            daily_count = scraper.scrape_day_maximum(current_date)
            total_all_days += daily_count
            logger.info(f"Selesai untuk {current_date}, total hari ini: {daily_count}")

            # Pindah ke hari berikutnya
            current_date += timedelta(days=1)

        logger.info(f"ETL selesai. Total keseluruhan: {total_all_days} tweet")

    except KeyboardInterrupt:
        logger.info("Proses ETL dihentikan secara manual.")
    except Exception as e:
        logger.error(f"Error saat menjalankan ETL: {e}")
    finally:
        client.close()
        driver.quit()

if __name__ == "__main__":
    logger.info("Memulai proses ETL maksimum tweet untuk analisis sentimen MBG...")
    run_etl()
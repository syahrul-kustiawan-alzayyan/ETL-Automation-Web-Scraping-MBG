"""
json_only_etl.py
File ETL untuk scraping langsung ke JSON tanpa database
"""

import json
import logging
import time
import os
from datetime import datetime, timedelta
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("JSONOnlyETL")


def load_config():
    """Memuat konfigurasi dari file JSON."""
    with open('config/config.json', 'r') as f:
        return json.load(f)


def create_output_directories():
    """Membuat direktori output jika belum ada."""
    os.makedirs('data', exist_ok=True)
    os.makedirs('logs', exist_ok=True)


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


def process_tweets_with_cleaning_and_labeling(input_file_path, output_file_path):
    """Proses file JSON dengan cleaning dan labeling."""
    from utils import apply_data_cleaning, apply_sentiment_labeling

    logger.info(f"Memulai proses cleaning dan labeling untuk: {input_file_path}")

    # Baca data dari file JSON
    with open(input_file_path, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)

    # Lakukan cleaning
    cleaned_data = apply_data_cleaning(raw_data)

    # Lakukan labeling
    labeled_data = apply_sentiment_labeling(cleaned_data)

    # Simpan hasil ke file labeled
    with open(output_file_path, 'w', encoding='utf-8') as f:
        json.dump(labeled_data, f, ensure_ascii=False, indent=2, default=str)

    logger.info(f"Proses cleaning dan labeling selesai. Output: {output_file_path}")
    return len(labeled_data)


def run_json_only_etl(start_date=None, end_date=None):
    """Fungsi utama ETL yang menyimpan langsung ke JSON."""
    config = load_config()
    create_output_directories()

    # Setup driver
    driver = setup_driver()

    try:
        # Import scraper yang tidak menggunakan database
        from src.json_only_scraper import JSONOnlyScraper

        # Inisialisasi scraper JSON-only
        scraper = JSONOnlyScraper(driver, config)

        # Login
        scraper.inject_cookies()

        # Tentukan rentang tanggal
        if start_date is None or end_date is None:
            # Periksa apakah ada tanggal spesifik di konfigurasi
            config_start_date = config['twitter'].get('start_date')
            config_end_date = config['twitter'].get('end_date')

            if config_start_date and config_end_date:
                # Gunakan tanggal dari konfigurasi
                start_date_obj = datetime.strptime(config_start_date, '%Y-%m-%d')
                end_date_obj = datetime.strptime(config_end_date, '%Y-%m-%d')
            else:
                # Gunakan konfigurasi default jika tidak ada tanggal spesifik
                days_back = config['twitter']['days_back']
                end_date_obj = datetime.now()
                start_date_obj = end_date_obj - timedelta(days=days_back)
        else:
            # Gunakan tanggal yang ditentukan sebagai parameter
            if isinstance(start_date, str):
                start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
            else:
                start_date_obj = start_date

            if isinstance(end_date, str):
                end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
            else:
                end_date_obj = end_date

        logger.info(f"Memproses tweet dari {start_date_obj.strftime('%Y-%m-%d')} hingga {end_date_obj.strftime('%Y-%m-%d')}")

        # Loop harian
        current_date = start_date_obj.date()
        end_date_date = end_date_obj.date()

        total_all_days = 0
        while current_date <= end_date_date:
            logger.info(f"Memulai scraping untuk {current_date}")

            # Kumpulkan data tweet untuk tanggal ini
            daily_count = scraper.scrape_day_maximum(current_date)
            total_all_days += daily_count
            logger.info(f"Selesai scraping untuk {current_date}, total hari ini: {daily_count}")

            if daily_count > 0:
                # Proses cleaning dan labeling untuk file harian
                raw_file_path = f"data/mbg_sentiment_db.tweets_{current_date.strftime('%Y-%m-%d')}.json"
                labeled_file_path = f"data/mbg_sentiment_db.tweets_{current_date.strftime('%Y-%m-%d')}_labeled.json"

                if os.path.exists(raw_file_path):
                    processed_count = process_tweets_with_cleaning_and_labeling(raw_file_path, labeled_file_path)
                    logger.info(f"Selesai proses cleaning dan labeling untuk {current_date}, diproses: {processed_count}")

            # Pindah ke hari berikutnya
            current_date += timedelta(days=1)

        logger.info(f"ETL JSON-Only selesai. Total keseluruhan: {total_all_days} tweet")

        # Lakukan agregasi bulanan jika diperlukan
        from utils import aggregate_monthly_data_if_needed
        # Periksa setiap bulan dalam rentang
        current_date = start_date_obj.date()
        while current_date <= end_date_date:
            aggregate_monthly_data_if_needed(current_date)
            current_date += timedelta(days=1)

    except Exception as e:
        logger.error(f"Error saat menjalankan ETL: {e}")
    finally:
        driver.quit()


if __name__ == "__main__":
    logger.info("Memulai proses ETL JSON-Only untuk analisis sentimen MBG...")
    logger.info("Catatan: Script ini menyimpan data langsung ke file JSON, bukan ke database")

    # Untuk mengumpulkan data bulan September 2024:
    # run_json_only_etl("2024-09-01", "2024-09-30")

    # Untuk mengumpulkan data bulan Oktober 2024:
    # run_json_only_etl("2024-10-01", "2024-10-31")

    # Jalankan dengan konfigurasi dari config.json
    run_json_only_etl()
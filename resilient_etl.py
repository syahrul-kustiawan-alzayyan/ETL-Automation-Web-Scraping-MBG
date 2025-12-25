"""
resilient_etl.py
File ETL yang lebih tangguh, bisa melanjutkan dari titik terakhir jika terputus

Module ini bertanggung jawab untuk:
- Mengelola pipeline ETL (Extract, Transform, Load) untuk data tweet
- Menyediakan mekanisme ketahanan saat proses scraping terputus
- Mengintegrasikan scraping, pembersihan data, dan labeling sentimen
- Mengelola penyimpanan data ke MongoDB dan file JSON
"""

import json
import logging
import time
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError
import os
import random

# Pustaka Eksternal
import undetected_chromedriver as uc
from tqdm import tqdm

# Define path to configuration file
CONFIG_FILE = "config/config.json"

def load_config():
    """
    Memuat konfigurasi dari file JSON.

    Returns:
        dict: Konfigurasi aplikasi dari file JSON
    """
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

# Load configuration at module initialization
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
logger = logging.getLogger("ResilientETL")

def setup_driver():
    """
    Mengkonfigurasi Undetected Chromedriver.

    Fungsi ini mengatur opsi Chrome untuk menghindari deteksi otomasi,
    termasuk mengubah user agent dan menyembunyikan sifat otomatisasi.

    Returns:
        uc.Chrome: Instance dari Chrome driver yang tidak terdeteksi sebagai otomasi
    """
    # Set up Chrome options to avoid detection and improve loading performance
    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')  # Bypass OS security model
    options.add_argument('--disable-dev-shm-usage')  # Overcome limited resource problems
    options.add_argument('--disable-blink-features=AutomationControlled')  # Hide automation
    options.add_argument('--disable-extensions')  # Disable extensions that might cause issues
    options.add_argument('--profile-directory=Default')  # Use default profile
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.109 Safari/537.36')  # Updated user agent
    options.add_argument('--disable-web-security')  # Disable web security for broader access
    options.add_argument('--allow-running-insecure-content')  # Allow mixed content
    options.add_argument('--disable-features=VizDisplayCompositor')  # May help with loading speed
    options.add_argument('--disable-gpu-sandbox')  # May improve performance
    options.add_argument('--disable-ipc-flooding-protection')  # May prevent hanging
    options.add_argument('--disable-background-timer-throttling')  # Prevent timer throttling in background tabs
    options.add_argument('--disable-renderer-backgrounding')  # Prevent background renderer throttling
    options.add_argument('--disable-backgrounding-occluded-windows')  # Prevent backgrounding occluded windows
    # Additional options to make headless mode less detectable
    # Note: undetected_chromedriver handles excludeSwitches and useAutomationExtension internally
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-features=VizDisplayCompositor")
    options.add_argument("--disable-ipc-flooding-protection")

    # Add headless mode based on config
    if CONFIG['scraper'].get('use_headless', False):
        options.add_argument('--headless=new')  # Use new headless mode (Chrome 109+)
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-web-security')
        options.add_argument('--allow-running-insecure-content')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-features=VizDisplayCompositor')
        logger.info("Menggunakan mode headless (tanpa tampilan)")
    else:
        logger.info("Menggunakan mode visible (dengan tampilan)")

    # Set browser preferences for faster loading
    prefs = {
        "profile.default_content_setting_values.notifications": 2,  # Disable notifications
        "profile.default_content_settings.popups": 0,  # Disable popups
        "profile.managed_default_content_settings.images": 2,  # Disable images for faster loading (set to 2 to block, 1 to allow)
        "profile.default_content_setting_values.cookies": 1,  # Allow cookies
        "profile.default_content_setting_values.javascript": 1,  # Allow JavaScript
        "profile.default_content_setting_values.plugins": 1,  # Allow plugins
        "profile.default_content_setting_values.notifications": 2,  # Block notifications
        "profile.default_content_setting_values.geolocation": 2,  # Block geolocation
        "profile.default_content_setting_values.media_stream_mic": 2,  # Block microphone
        "profile.default_content_setting_values.media_stream_camera": 2,  # Block camera
        "profile.default_content_settings.popups": 0,  # Disable popups
        "profile.managed_default_content_settings.images": 1,  # Actually keep images enabled as they may be needed for Twitter functionality
    }
    options.add_experimental_option("prefs", prefs)

    logger.info("Memulai Chromium Driver...")
    # Initialize undetected Chrome driver with stealth settings
    # Note: undetected_chromedriver handles automation extension internally
    # Pass the suppress_welcome flag and other settings directly to uc.Chrome
    driver = uc.Chrome(options=options, version_main=None,
                      suppress_welcome=True,
                      no_sandbox=True)

    # Execute multiple scripts to remove webdriver properties and enhance stealth
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
    driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['id-ID', 'id', 'en-US', 'en']})")
    driver.execute_script("const originalQuery = window.navigator.permissions.query; window.navigator.permissions.query = (parameters) => { if (parameters.name === 'notifications') { return Promise.resolve({state: 'denied'}); } return originalQuery(parameters); }")

    # Additional stealth for headless detection
    if CONFIG['scraper'].get('use_headless', False):
        try:
            driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                "source": """
                    Object.defineProperty(navigator, 'maxTouchPoints', {
                        get: () => 1
                    });
                    Object.defineProperty(navigator, 'doNotTrack', {
                        get: () => '1'
                    });
                """
            })
        except:
            # If CDP command fails, continue without it
            pass

    return driver

def init_db():
    """
    Menginisialisasi koneksi MongoDB dan manajer koleksi harian.

    Fungsi ini membuat koneksi ke MongoDB dan menginisialisasi
    DailyCollectionManager untuk mengelola koleksi berdasarkan tanggal.

    Returns:
        tuple: Pasangan (client MongoDB, instance DailyCollectionManager)
    """
    try:
        import sys
        # Add current directory to Python path to import local modules
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))

        from utils import DailyCollectionManager
        # Create MongoDB client with URI from config
        client = MongoClient(CONFIG['database']['mongo_uri'])
        # Initialize collection manager with configuration
        collection_manager = DailyCollectionManager(CONFIG)
        logger.info("Koneksi MongoDB berhasil diinisialisasi.")
        return client, collection_manager
    except PyMongoError as e:
        logger.critical(f"Gagal terhubung ke MongoDB: {e}")
        exit(1)

def run_etl(start_date=None, end_date=None, continue_from_last=True):
    """
    Fungsi utama untuk menjalankan ETL yang tangguh.

    Fungsi ini mengatur keseluruhan pipeline ETL untuk mengumpulkan,
    membersihkan, melabeli, dan menyimpan data tweet dari X/Twitter.

    Args:
        start_date (str or datetime, optional): Tanggal awal untuk scraping (format: YYYY-MM-DD)
        end_date (str or datetime, optional): Tanggal akhir untuk scraping (format: YYYY-MM-DD)
        continue_from_last (bool): Jika True, melanjutkan dari data yang sudah ada

    Returns:
        None: Fungsi ini menjalankan proses ETL secara keseluruhan
    """
    # Initialize database and driver connections
    client, collection_manager = init_db()
    driver = setup_driver()

    try:
        import sys
        # Add current directory to Python path to import local modules
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))

        from src.resilient_scraper import ResilientScraper

        # Initialize the resilient scraper with driver, config, and collection manager
        scraper = ResilientScraper(driver, CONFIG, collection_manager)

        # Inject cookies to authenticate the session
        scraper.inject_cookies()

        # Determine date range for processing
        if start_date is None or end_date is None:
            # Check if there are specific dates in the configuration
            config_start_date = CONFIG['twitter'].get('start_date')
            config_end_date = CONFIG['twitter'].get('end_date')

            if config_start_date and config_end_date:
                # Use dates from configuration
                start_date_obj = datetime.strptime(config_start_date, '%Y-%m-%d')
                end_date_obj = datetime.strptime(config_end_date, '%Y-%m-%d')
            else:
                # Use default configuration if no specific dates
                days_back = CONFIG['twitter']['days_back']
                end_date_obj = datetime.now()
                start_date_obj = end_date_obj - timedelta(days=days_back)
        else:
            # Use dates specified as parameters
            if isinstance(start_date, str):
                start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
            else:
                start_date_obj = start_date

            if isinstance(end_date, str):
                end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
            else:
                end_date_obj = end_date

        logger.info(f"Memproses tweet dari {start_date_obj.strftime('%Y-%m-%d')} hingga {end_date_obj.strftime('%Y-%m-%d')}")

        # Calculate the number of days to process
        date_range = end_date_obj.date() - start_date_obj.date()
        total_days = date_range.days + 1

        # Print ETL process header with information
        print(f"\n{'='*70}")
        print(f"PROSES ETL DIMULAI")
        print(f"Rentang Tanggal: {start_date_obj.strftime('%Y-%m-%d')} s/d {end_date_obj.strftime('%Y-%m-%d')}")
        print(f"Jumlah Hari: {total_days}")
        print(f"{'='*70}")

        # Loop through each day in the date range - now modified to handle monthly
        current_date = start_date_obj.date()
        end_date_date = end_date_obj.date()

        # Keep track of total tweets collected across the month
        total_all_days = 0

        # Check if daily processing is enabled
        daily_processing_enabled = CONFIG['twitter'].get('daily_processing', False)

        # Check if the range is monthly - either by duration (>31 days) or by being full calendar month
        start_of_month = start_date_obj.replace(day=1)
        end_of_month = (start_of_month.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        is_full_month = (start_date_obj.day == 1 and end_date_obj.date() == end_of_month.date())

        # Additional check: if both dates are in the same month and cover most/all days of the month
        same_month = (start_date_obj.month == end_date_obj.month and
                      start_date_obj.year == end_date_obj.year)
        days_in_month = (start_date_obj.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        days_in_target_month = (days_in_month.day)
        is_most_of_month = (same_month and total_days >= days_in_target_month * 0.75)  # If covers 75%+ of the month

        # Check if the range is monthly (more than 31 days OR full/complete month) or daily
        # Conditions:
        # 1. More than 31 days (covers multiple months)
        # 2. Full calendar month (starts on day 1 and ends on last day of month)
        # 3. Most of a month (75% or more of days in a single month)
        # 4. Daily processing flag is set (process daily but store monthly)
        if daily_processing_enabled or total_days > 31 or is_full_month or is_most_of_month:  # Treat as monthly or daily with monthly storage
            if daily_processing_enabled:
                print(f"\n[HYBRID] Processing daily with monthly storage: {start_date_obj.strftime('%Y-%m')} (periode: {start_date_obj.strftime('%Y-%m-%d')} - {end_date_obj.strftime('%Y-%m-%d')})")
            else:
                print(f"\n[MONTHLY] Memproses data secara BULANAN: {start_date_obj.strftime('%Y-%m')} (periode: {start_date_obj.strftime('%Y-%m-%d')} - {end_date_obj.strftime('%Y-%m-%d')})")

            try:
                # Handle daily processing with monthly storage
                if daily_processing_enabled:
                    # Process each day in the range individually
                    current_date = start_date_obj.date()
                    end_date_date = end_date_obj.date()

                    total_all_days = 0

                    # Progress bar for the overall process
                    overall_day_pbar = tqdm(total=total_days, desc="Overall Daily Progress", position=0, leave=True)

                    while current_date <= end_date_date:
                        logger.info(f"Memulai scraping dan processing harian untuk {current_date}")
                        print(f"\n[DAILY] Memproses hari: {current_date.strftime('%A, %d %B %Y')}")

                        try:
                            # Get the collection for the start date (monthly collection)
                            # But we'll be scraping for just this day
                            monthly_collection, collection_name = collection_manager.get_collection_by_date(start_date_obj)  # Use start date for monthly collection

                            # Check if there's already data for this date (if wanting to continue from last point)
                            if continue_from_last:
                                # Check for tweets created on this specific date
                                existing_count = monthly_collection.count_documents({
                                    "metadata.created_at": {
                                        "$gte": datetime.combine(current_date, datetime.min.time()),
                                        "$lt": datetime.combine(current_date + timedelta(days=1), datetime.min.time())
                                    }
                                })
                                if existing_count > 0:
                                    logger.info(f"Sudah ada {existing_count} data untuk {current_date}, lewati atau proses ulang?")
                                    print(f"  [COUNT] Sudah ada {existing_count} tweet di database untuk {current_date}")
                                    # Skip to next date
                                    overall_day_pbar.update(1)
                                    current_date += timedelta(days=1)
                                    continue

                            # Scrape tweets for the current day specifically
                            daily_count = scraper.scrape_day_maximum(current_date)
                            total_all_days += daily_count
                            logger.info(f"Selesai scraping untuk {current_date}, total hari ini: {daily_count}")

                            if daily_count > 0:
                                print(f"  [SUCCESS] Scraping selesai: {daily_count} tweet baru untuk {current_date}")

                                # Get only the data for this specific day from the monthly collection
                                daily_tweets_data = list(monthly_collection.find({
                                    "metadata.created_at": {
                                        "$gte": datetime.combine(current_date, datetime.min.time()),
                                        "$lt": datetime.combine(current_date + timedelta(days=1), datetime.min.time())
                                    }
                                }))

                                # Process cleaning and labeling for this day's data
                                logger.info(f"Memulai proses cleaning dan labeling untuk {current_date}")
                                print(f"  [PROCESS] Memproses cleaning dan labeling untuk {current_date}...")
                                from utils import apply_data_cleaning, apply_sentiment_labeling

                                # Perform cleaning and labeling
                                cleaned_data = apply_data_cleaning(daily_tweets_data)
                                labeled_data = apply_sentiment_labeling(cleaned_data)

                                # Update monthly collection with processed data for this day
                                if labeled_data:
                                    bulk_operations = []
                                    for labeled_tweet in labeled_data:
                                        bulk_operations.append(
                                            UpdateOne(
                                                {"_id": labeled_tweet["_id"]},
                                                {"$set": labeled_tweet}
                                            )
                                        )

                                    if bulk_operations:
                                        # Write bulk updates to MongoDB
                                        monthly_collection.bulk_write(bulk_operations, ordered=False)
                                        logger.info(f"Berhasil update {len(bulk_operations)} tweet di MongoDB untuk {current_date}")

                                logger.info(f"Selesai proses cleaning dan labeling untuk {current_date}, diproses: {len(labeled_data)} tweet")
                                print(f"  [CLEAN] Cleaning dan labeling selesai: {len(labeled_data)} tweet diproses")

                                # Since we're storing all in monthly format, we don't save daily JSON files
                                # But we could still save monthly data periodically
                                if current_date.day == 15 or current_date == end_date_date:  # Mid-month or end of month
                                    # Save to monthly JSON file periodically
                                    from utils import save_monthly_data_labeled
                                    output_path = save_monthly_data_labeled(list(monthly_collection.find({})), start_date_obj, current_date)

                                    if output_path:
                                        logger.info(f"Data labeled sementara bulanan disimpan ke: {output_path}")
                                        print(f"  [SAVE] Data labeled sementara bulanan disimpan: {output_path}")

                            else:
                                print(f"  [ERROR] Tidak ada tweet ditemukan untuk {current_date}")
                                # Jika tidak ada tweet ditemukan, lanjutkan ke tanggal berikutnya
                                logger.info(f"Lanjut ke tanggal berikutnya...")
                                print(f"  [NEXT] Melanjutkan ke tanggal berikutnya...")

                        except Exception as daily_error:
                            logger.error(f"Error saat memproses {current_date}: {daily_error}")

                            # Update progress bar even if error occurs
                            overall_day_pbar.update(1)

                            # If error is related to browser connection, restart browser
                            if "koneksi browser terputus" in str(daily_error).lower() or "connection" in str(daily_error).lower():
                                logger.error("Koneksi browser terputus, mencoba restart browser...")
                                print(f"  [WARNING] Koneksi browser terputus, restart browser...")
                                try:
                                    # Close old driver if it still exists
                                    driver.quit()
                                except:
                                    pass  # Ignore if driver no longer exists

                                # Create new driver
                                driver = setup_driver()
                                logger.info("Browser berhasil direstart, melanjutkan proses...")
                                print(f"  [RESTART] Browser berhasil direstart")

                                # Initialize scraper with new driver
                                scraper = ResilientScraper(driver, CONFIG, collection_manager)
                                scraper.inject_cookies()
                            else:
                                logger.info(f"Lanjut ke tanggal berikutnya...")
                                print(f"  [NEXT] Melanjutkan ke tanggal berikutnya...")
                                # Add additional delay if error occurs
                                time.sleep(30)

                        # Move to the next day
                        current_date += timedelta(days=1)

                        # Add random delay between days to avoid detection
                        if current_date <= end_date_date:  # Only if not the last day
                            min_delay = CONFIG.get('etl', {}).get('min_daily_delay', 5)
                            max_delay = CONFIG.get('etl', {}).get('max_daily_delay', 15)
                            jeda = random.randint(min_delay, max_delay)
                            logger.info(f"Jeda {jeda} detik sebelum memproses hari berikutnya")

                            # Show countdown
                            for i in range(jeda, 0, -1):
                                print(f"  [WAIT] Jeda {i} detik sebelum lanjut ke hari berikutnya...", end='\r')
                                time.sleep(1)
                            print(" " * 50, end='\r')  # Clear the countdown line

                        # Update progress bar
                        overall_day_pbar.update(1)

                    # Close the progress bar
                    overall_day_pbar.close()

                else:  # Original monthly processing
                    # Scrape tweets for the entire month
                    total_all_days = scraper.scrape_month_maximum(start_date_obj, end_date_obj)

                    logger.info(f"Selesai scraping untuk bulan {start_date_obj.strftime('%Y-%m')}, total: {total_all_days}")

                    if total_all_days > 0:
                        print(f"  [SUCCESS] Scraping bulan {start_date_obj.strftime('%Y-%m')} selesai: {total_all_days} tweet baru")

                        # For monthly processing, get data from the monthly collection
                        # The monthly scraping has already stored data in the collection for start_date
                        monthly_collection, _ = collection_manager.get_collection_by_date(start_date_obj)
                        monthly_tweets_data = list(monthly_collection.find({}))

                        # Process and save monthly data
                        logger.info(f"Memulai proses cleaning dan labeling untuk bulan {start_date_obj.strftime('%Y-%m')}")
                        print(f"  [PROCESS] Memproses cleaning dan labeling bulan {start_date_obj.strftime('%Y-%m')}...")
                        from utils import apply_data_cleaning, apply_sentiment_labeling

                        # Perform cleaning and labeling
                        cleaned_data = apply_data_cleaning(monthly_tweets_data)
                        labeled_data = apply_sentiment_labeling(cleaned_data)

                        # Update monthly collection with processed data
                        if labeled_data:
                            # Use the collection for the start date to store the month's data
                            monthly_collection, _ = collection_manager.get_collection_by_date(start_date_obj)
                            bulk_operations = []
                            for labeled_tweet in labeled_data:
                                bulk_operations.append(
                                    UpdateOne(
                                        {"_id": labeled_tweet["_id"]},
                                        {"$set": labeled_tweet}
                                    )
                                )

                            if bulk_operations:
                                # Write bulk updates to MongoDB
                                monthly_collection.bulk_write(bulk_operations, ordered=False)
                                logger.info(f"Berhasil update {len(bulk_operations)} tweet di MongoDB untuk bulan {start_date_obj.strftime('%Y-%m')}")

                        logger.info(f"Selesai proses cleaning dan labeling untuk bulan {start_date_obj.strftime('%Y-%m')}, diproses: {len(labeled_data)} tweet")
                        print(f"  [CLEAN] Cleaning dan labeling selesai: {len(labeled_data)} tweet diproses")

                        # Also save to labeled JSON file for the month using the utility function
                        from utils import save_monthly_data_labeled
                        output_path = save_monthly_data_labeled(labeled_data, start_date_obj, end_date_obj)

                        if output_path:
                            logger.info(f"Data labeled bulanan disimpan ke: {output_path}")
                            print(f"  [SAVE] Data labeled bulanan disimpan: {output_path}")
                        else:
                            logger.error("Gagal menyimpan data labeled bulanan")
                            print(f"  [ERROR] Gagal menyimpan data labeled bulanan")

                    else:
                        print(f"  [ERROR] Tidak ada tweet ditemukan untuk bulan ini")
            except Exception as monthly_error:
                logger.error(f"Error saat memproses bulan {start_date_obj.strftime('%Y-%m')}: {monthly_error}")

                # If error is related to browser connection, restart browser
                if "koneksi browser terputus" in str(monthly_error).lower() or "connection" in str(monthly_error).lower():
                    logger.error("Koneksi browser terputus, mencoba restart browser...")
                    print(f"  [WARNING] Koneksi browser terputus, restart browser...")
                    try:
                        # Close old driver if it still exists
                        driver.quit()
                    except:
                        pass  # Ignore if driver no longer exists

                    # Create new driver
                    driver = setup_driver()
                    logger.info("Browser berhasil direstart, melanjutkan proses...")
                    print(f"  [RESTART] Browser berhasil direstart")

                    # Initialize scraper with new driver
                    scraper = ResilientScraper(driver, CONFIG, collection_manager)
                    scraper.inject_cookies()
        else:
            # Original daily processing for shorter ranges
            # Progress bar for total days
            overall_day_pbar = tqdm(total=total_days, desc="Overall Progress", position=0, leave=True)

            # Process each day in the range
            while current_date <= end_date_date:
                logger.info(f"Memulai scraping untuk {current_date}")
                print(f"\n[DAILY] Memproses hari: {current_date.strftime('%A, %d %B %Y')}")

                try:
                    # Get the daily collection for the current date
                    collection, collection_name = collection_manager.get_collection_by_date(current_date)

                    # Check if there's already data for this date (if wanting to continue from last point)
                    if continue_from_last:
                        existing_count = collection.count_documents({})
                        if existing_count > 0:
                            logger.info(f"Sudah ada {existing_count} data untuk {current_date}, lewati atau proses ulang?")
                            print(f"  [COUNT] Sudah ada {existing_count} tweet di database")
                            # If wanting to skip already processed days, uncomment the following lines
                            # current_date += timedelta(days=1)
                            # continue

                    # Scrape tweets for the current day
                    daily_count = scraper.scrape_day_maximum(current_date)
                    total_all_days += daily_count
                    logger.info(f"Selesai scraping untuk {current_date}, total hari ini: {daily_count}")

                    if daily_count > 0:
                        print(f"  [SUCCESS] Scraping selesai: {daily_count} tweet baru")
                        # Get data from collection
                        tweets_data = list(collection.find({}))

                        # Process cleaning and labeling
                        logger.info(f"Memulai proses cleaning dan labeling untuk {current_date}")
                        print(f"  [PROCESS] Memproses cleaning dan labeling...")
                        from utils import apply_data_cleaning, apply_sentiment_labeling

                        # Perform cleaning and labeling
                        cleaned_data = apply_data_cleaning(tweets_data)
                        labeled_data = apply_sentiment_labeling(cleaned_data)

                        # Update collection with processed data
                        if labeled_data:
                            bulk_operations = []
                            for labeled_tweet in labeled_data:
                                bulk_operations.append(
                                    UpdateOne(
                                        {"_id": labeled_tweet["_id"]},
                                        {"$set": labeled_tweet}
                                    )
                                )

                            if bulk_operations:
                                # Write bulk updates to MongoDB
                                collection.bulk_write(bulk_operations, ordered=False)
                                logger.info(f"Berhasil update {len(bulk_operations)} tweet di MongoDB untuk {current_date}")

                        logger.info(f"Selesai proses cleaning dan labeling untuk {current_date}, diproses: {len(labeled_data)} tweet")
                        print(f"  [CLEAN] Cleaning dan labeling selesai: {len(labeled_data)} tweet diproses")

                        # Also save to labeled JSON file
                        os.makedirs('data', exist_ok=True)
                        output_path = f"data/mbg_sentiment_db.tweets_{current_date.strftime('%Y-%m-%d')}_labeled.json"

                        # Convert ObjectId to string for JSON
                        for doc in labeled_data:
                            if '_id' in doc and hasattr(doc['_id'], '__class__') and doc['_id'].__class__.__name__ == 'ObjectId':
                                doc['_id'] = str(doc['_id'])

                        # Write labeled data to JSON file
                        with open(output_path, 'w', encoding='utf-8') as f:
                            json.dump(labeled_data, f, ensure_ascii=False, indent=2, default=str)

                        logger.info(f"Data labeled harian disimpan ke: {output_path}")
                        print(f"  [SAVE] Data labeled disimpan: {output_path}")

                    else:
                        print(f"  [ERROR] Tidak ada tweet ditemukan")

                    # Perform monthly aggregation if needed (only for daily processing)
                    aggregate_monthly_data_if_needed(current_date)

                    # Update progress bar
                    overall_day_pbar.update(1)

                except Exception as daily_error:
                    logger.error(f"Error saat memproses {current_date}: {daily_error}")

                    # Update progress bar even if error occurs
                    overall_day_pbar.update(1)

                    # If error is related to browser connection, restart browser
                    if "koneksi browser terputus" in str(daily_error).lower() or "connection" in str(daily_error).lower():
                        logger.error("Koneksi browser terputus, mencoba restart browser...")
                        print(f"  [WARNING] Koneksi browser terputus, restart browser...")
                        try:
                            # Close old driver if it still exists
                            driver.quit()
                        except:
                            pass  # Ignore if driver no longer exists

                        # Create new driver
                        driver = setup_driver()
                        logger.info("Browser berhasil direstart, melanjutkan proses...")
                        print(f"  [RESTART] Browser berhasil direstart")

                        # Initialize scraper with new driver
                        scraper = ResilientScraper(driver, CONFIG, collection_manager)
                        scraper.inject_cookies()
                    else:
                        logger.info(f"Lanjut ke tanggal berikutnya...")
                        print(f"  [NEXT] Melanjutkan ke tanggal berikutnya...")
                        # Add additional delay if error occurs
                        time.sleep(30)

                # Move to the next day
                current_date += timedelta(days=1)

                # Add random delay between days to avoid detection
                if current_date <= end_date_date:  # Only if not the last day
                    min_delay = CONFIG.get('etl', {}).get('min_daily_delay', 5)
                    max_delay = CONFIG.get('etl', {}).get('max_daily_delay', 15)
                    jeda = random.randint(min_delay, max_delay)
                    logger.info(f"Jeda {jeda} detik sebelum memproses hari berikutnya")

                    # Show countdown
                    for i in range(jeda, 0, -1):
                        print(f"  [WAIT] Jeda {i} detik sebelum lanjut ke hari berikutnya...", end='\r')
                        time.sleep(1)
                    print(" " * 50, end='\r')  # Clear the countdown line

            # Close the progress bar
            overall_day_pbar.close()

        print(f"\n{'='*70}")
        print(f"PROSES ETL SELESAI")
        print(f"Total Tweet Terkumpul: {total_all_days}")
        print(f"Rentang Tanggal: {start_date_obj.strftime('%Y-%m-%d')} s/d {end_date_obj.strftime('%Y-%m-%d')}")
        print(f"{'='*70}")

        logger.info(f"ETL selesai. Total keseluruhan: {total_all_days} tweet")

    except KeyboardInterrupt:
        logger.info("Proses ETL dihentikan secara manual.")
    except Exception as e:
        logger.error(f"Error saat menjalankan ETL: {e}")
    finally:
        # Ensure connections are properly closed
        client.close()
        driver.quit()


def aggregate_monthly_data_if_needed(target_date):
    """
    Cek apakah bulan ini sudah selesai dan perlu digabungkan.

    Fungsi ini memeriksa apakah semua data harian untuk bulan tertentu
    sudah terkumpul dan kemudian membuat file agregasi bulanan jika belum ada.

    Args:
        target_date (datetime.date): Tanggal yang akan diperiksa untuk agregasi bulanan
    """
    from utils import get_daily_files_for_month, aggregate_monthly_data

    year = target_date.year
    month = target_date.month

    # Get all daily files for this month
    daily_files = get_daily_files_for_month("data/", year, month)

    if not daily_files:
        logger.info(f"Tidak ada file harian ditemukan untuk {year}-{month:02d}")
        return

    # Monthly output file name
    monthly_output_path = f"data/mbg_sentiment_db.tweets_{year}-{month:02d}_labeled.json"

    # Check if monthly file already exists
    if os.path.exists(monthly_output_path):
        logger.info(f"File bulanan {monthly_output_path} sudah ada, dilewati")
        return

    # Perform aggregation
    success = aggregate_monthly_data(daily_files, monthly_output_path)

    if success:
        logger.info(f"Agregasi bulanan untuk {year}-{month:02d} berhasil")
        logger.info(f"File agregat disimpan di: {monthly_output_path}")
    else:
        logger.error(f"Gagal melakukan agregasi bulanan untuk {year}-{month:02d}")


def process_existing_data_for_date(date_obj, client=None, collection_manager=None):
    """
    Fungsi untuk memproses ulang data yang sudah ada di database untuk tanggal tertentu.

    Fungsi ini mengambil data yang sudah ada di MongoDB untuk tanggal spesifik
    dan menjalankan proses cleaning dan labeling sentimen lagi jika diperlukan.

    Args:
        date_obj (datetime.date): Tanggal yang akan diproses
        client (MongoClient, optional): Koneksi MongoDB (jika sudah ada)
        collection_manager (DailyCollectionManager, optional): Manager koleksi harian (jika sudah ada)
    """
    should_close_client = False
    # Initialize database connection if not provided
    if client is None or collection_manager is None:
        client, collection_manager = init_db()
        should_close_client = True

    try:
        # Get the daily collection for the date
        collection, collection_name = collection_manager.get_collection_by_date(date_obj)

        logger.info(f"Memproses ulang data dari collection: {collection_name}")

        # Fetch all data from the collection
        tweets_data = list(collection.find({}))

        if len(tweets_data) > 0:
            # Import functions from utils
            from utils import apply_data_cleaning, apply_sentiment_labeling

            # Perform cleaning
            cleaned_data = apply_data_cleaning(tweets_data)

            # Perform labeling
            labeled_data = apply_sentiment_labeling(cleaned_data)

            # Update collection with processed data
            if labeled_data:
                bulk_operations = []
                for labeled_tweet in labeled_data:
                    bulk_operations.append(
                        UpdateOne(
                            {"_id": labeled_tweet["_id"]},
                            {"$set": labeled_tweet}
                        )
                    )

                if bulk_operations:
                    # Bulk write the updates to MongoDB
                    collection.bulk_write(bulk_operations, ordered=False)
                    logger.info(f"Berhasil update {len(bulk_operations)} tweet di MongoDB untuk {date_obj.strftime('%Y-%m-%d')}")

            logger.info(f"Selesai memproses ulang data untuk {date_obj.strftime('%Y-%m-%d')}, diproses: {len(labeled_data)} tweet")

            # Also save to labeled JSON file
            os.makedirs('data', exist_ok=True)
            output_path = f"data/mbg_sentiment_db.tweets_{date_obj.strftime('%Y-%m-%d')}_labeled.json"

            # Convert ObjectId to string for JSON
            for doc in labeled_data:
                if '_id' in doc and hasattr(doc['_id'], '__class__') and doc['_id'].__class__.__name__ == 'ObjectId':
                    doc['_id'] = str(doc['_id'])

            # Write the labeled data to a JSON file
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(labeled_data, f, ensure_ascii=False, indent=2, default=str)

            logger.info(f"Data labeled harian disimpan ke: {output_path}")

            # Perform monthly aggregation if needed
            aggregate_monthly_data_if_needed(date_obj)
        else:
            logger.info(f"Tidak ada data untuk diproses pada {date_obj.strftime('%Y-%m-%d')}")

    except Exception as e:
        logger.error(f"Error saat memproses ulang data untuk {date_obj.strftime('%Y-%m-%d')}: {e}")
    finally:
        # Close client connection if it was initialized in this function
        if should_close_client:
            client.close()


def process_existing_data_range(start_date, end_date):
    """
    Fungsi untuk memproses ulang rentang data yang sudah ada di database.

    Fungsi ini memproses ulang data untuk seluruh rentang tanggal,
    menjalankan cleaning dan labeling sentimen pada data yang sudah ada.

    Args:
        start_date (str or datetime): Tanggal awal rentang (format: YYYY-MM-DD)
        end_date (str or datetime): Tanggal akhir rentang (format: YYYY-MM-DD)
    """
    # Convert string to datetime if needed
    if isinstance(start_date, str):
        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    else:
        start_date_obj = start_date

    if isinstance(end_date, str):
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        end_date_obj = end_date

    current_date = start_date_obj.date()
    end_date_date = end_date_obj.date()

    logger.info(f"Memproses ulang data dari {start_date_obj.strftime('%Y-%m-%d')} hingga {end_date_obj.strftime('%Y-%m-%d')}")

    # Initialize client and collection manager once at the beginning
    client, collection_manager = init_db()

    try:
        # Process each day in the range
        while current_date <= end_date_date:
            process_existing_data_for_date(current_date, client, collection_manager)
            current_date += timedelta(days=1)
    finally:
        # Close the database connection
        client.close()


if __name__ == "__main__":
    """
    Main execution block - Entry point untuk menjalankan ETL.

    Blok ini menampilkan header informasi, memulai logging,
    dan menjalankan fungsi ETL utama dengan konfigurasi default.
    """
    print(f"{'='*80}")
    print(f"           ETL AUTOMATION FOR MBG SENTIMENT ANALYSIS")
    print(f"                   (Makan Bergizi Gratis)")
    print(f"{'='*80}")
    print(f"[TOOLS] Menggunakan teknik scraping canggih untuk mendapatkan data dari X/Twitter")
    print(f"[SHIELD] Dilengkapi mekanisme anti-detection dan retry otomatis")
    print(f"[CHART] Proses akan mencakup scraping, cleaning, labeling, dan penyimpanan data")
    print(f"[CLOCK] Estimasi waktu tergantung pada jumlah tanggal dan kebijakan X/Twitter")
    print(f"{'='*80}")

    logger.info("Memulai proses ETL tangguh untuk analisis sentimen MBG...")

    # Contoh penggunaan:
    # 1. Untuk mengumpulkan data baru (default)
    # run_etl()

    # 2. Untuk mengumpulkan data dari rentang tanggal tertentu
    # run_etl("2024-09-01", "2024-09-30")  # September
    # atau
    # run_etl("2024-10-01", "2024-10-31")  # Oktober

    # 3. Untuk memproses ulang data yang sudah ada di database
    # process_existing_data_range("2024-09-01", "2024-09-30")  # September
    # atau
    # process_existing_data_range("2024-10-01", "2024-10-31")  # Oktober

    # Jalankan proses default - mengumpulkan data baru dari rentang tanggal konfigurasi
    run_etl()

    print(f"\n{'='*80}")
    print(f"           PROSES SELESAI - TERIMA KASIH TELAH MENUNGGU")
    print(f"{'='*80}")
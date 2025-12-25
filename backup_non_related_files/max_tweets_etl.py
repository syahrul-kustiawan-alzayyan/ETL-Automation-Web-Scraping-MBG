"""
max_tweets_etl.py
File utama untuk ETL dengan fokus maksimum tweet per hari
"""

import json
import logging
import time
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne
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

def run_etl(start_date=None, end_date=None):
    """
    Fungsi utama untuk menjalankan ETL.
    Jika start_date dan end_date tidak ditentukan, gunakan konfigurasi dari file config.json.
    """
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
        if start_date is None or end_date is None:
            # Periksa apakah ada tanggal spesifik di konfigurasi
            config_start_date = CONFIG['twitter'].get('start_date')
            config_end_date = CONFIG['twitter'].get('end_date')
            
            if config_start_date and config_end_date:
                # Gunakan tanggal dari konfigurasi
                start_date_obj = datetime.strptime(config_start_date, '%Y-%m-%d')
                end_date_obj = datetime.strptime(config_end_date, '%Y-%m-%d')
            else:
                # Gunakan konfigurasi default jika tidak ada tanggal spesifik
                days_back = CONFIG['twitter']['days_back']
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
            
            # Dapatkan collection harian
            collection, collection_name = collection_manager.get_collection_by_date(current_date)
            
            # Scraping tweet untuk hari ini
            daily_count = scraper.scrape_day_maximum(current_date)
            total_all_days += daily_count
            logger.info(f"Selesai scraping untuk {current_date}, total hari ini: {daily_count}")
            
            if daily_count > 0:
                # Ambil data dari collection
                tweets_data = list(collection.find({}))
                
                # Proses cleaning dan labeling
                logger.info(f"Memulai proses cleaning dan labeling untuk {current_date}")
                from utils import apply_data_cleaning, apply_sentiment_labeling
                
                # Lakukan cleaning dan labeling
                cleaned_data = apply_data_cleaning(tweets_data)
                labeled_data = apply_sentiment_labeling(cleaned_data)
                
                # Update collection dengan data yang telah diproses
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
                        collection.bulk_write(bulk_operations, ordered=False)
                        logger.info(f"Berhasil update {len(bulk_operations)} tweet di MongoDB untuk {current_date}")
                
                logger.info(f"Selesai proses cleaning dan labeling untuk {current_date}, diproses: {len(labeled_data)} tweet")
                
                # Simpan juga ke file JSON labeled
                import os
                os.makedirs('data', exist_ok=True)
                output_path = f"data/mbg_sentiment_db.tweets_{current_date.strftime('%Y-%m-%d')}_labeled.json"
                
                # Konversi ObjectId ke string untuk JSON
                for doc in labeled_data:
                    if '_id' in doc and hasattr(doc['_id'], '__class__') and doc['_id'].__class__.__name__ == 'ObjectId':
                        doc['_id'] = str(doc['_id'])
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(labeled_data, f, ensure_ascii=False, indent=2, default=str)
                
                logger.info(f"Data labeled harian disimpan ke: {output_path}")
            
            # Lakukan agregasi bulanan jika diperlukan
            aggregate_monthly_data_if_needed(current_date)

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


def export_collection_to_json(collection, output_path):
    """Export collection ke file JSON."""
    try:
        # Konversi collection ke list
        data = list(collection.find({}))
        
        # Ubah ObjectId menjadi string agar bisa disimpan ke JSON
        for doc in data:
            if '_id' in doc and hasattr(doc['_id'], '__class__') and doc['_id'].__class__.__name__ == 'ObjectId':
                doc['_id'] = str(doc['_id'])
        
        # Simpan ke file JSON
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"Data collection disimpan ke {output_path}")
        return len(data)
    except Exception as e:
        logger.error(f"Error saat export collection: {e}")
        return 0


def aggregate_monthly_data_if_needed(target_date):
    """Cek apakah bulan ini sudah selesai dan perlu digabungkan."""
    from utils import get_daily_files_for_month, aggregate_monthly_data
    
    year = target_date.year
    month = target_date.month
    
    # Dapatkan semua file harian untuk bulan ini
    daily_files = get_daily_files_for_month("data/", year, month)
    
    if not daily_files:
        logger.info(f"Tidak ada file harian ditemukan untuk {year}-{month:02d}")
        return
    
    # Nama file output bulanan
    monthly_output_path = f"data/mbg_sentiment_db.tweets_{year}-{month:02d}_labeled.json"
    
    # Cek apakah file bulanan sudah ada
    if os.path.exists(monthly_output_path):
        logger.info(f"File bulanan {monthly_output_path} sudah ada, dilewati")
        return
    
    # Lakukan agregasi
    success = aggregate_monthly_data(daily_files, monthly_output_path)
    
    if success:
        logger.info(f"Agregasi bulanan untuk {year}-{month:02d} berhasil")
        logger.info(f"File agregat disimpan di: {monthly_output_path}")
    else:
        logger.error(f"Gagal melakukan agregasi bulanan untuk {year}-{month:02d}")


def process_existing_data_for_date(date_obj, client=None, collection_manager=None):
    """
    Fungsi untuk memproses ulang data yang sudah ada di database untuk tanggal tertentu
    """
    should_close_client = False
    if client is None or collection_manager is None:
        client, collection_manager = init_db()
        should_close_client = True
    
    try:
        # Dapatkan collection harian
        collection, collection_name = collection_manager.get_collection_by_date(date_obj)
        
        logger.info(f"Memproses ulang data dari collection: {collection_name}")
        
        # Ambil semua data dari collection
        tweets_data = list(collection.find({}))
        
        if len(tweets_data) > 0:
            # Import fungsi dari utils
            from utils import apply_data_cleaning, apply_sentiment_labeling
            
            # Lakukan cleaning
            cleaned_data = apply_data_cleaning(tweets_data)
            
            # Lakukan labeling
            labeled_data = apply_sentiment_labeling(cleaned_data)
            
            # Update collection dengan data yang telah diproses
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
                    collection.bulk_write(bulk_operations, ordered=False)
                    logger.info(f"Berhasil update {len(bulk_operations)} tweet di MongoDB untuk {date_obj.strftime('%Y-%m-%d')}")
            
            logger.info(f"Selesai memproses ulang data untuk {date_obj.strftime('%Y-%m-%d')}, diproses: {len(labeled_data)} tweet")
            
            # Simpan juga ke file JSON labeled
            import os
            os.makedirs('data', exist_ok=True)
            output_path = f"data/mbg_sentiment_db.tweets_{date_obj.strftime('%Y-%m-%d')}_labeled.json"
            
            # Konversi ObjectId ke string untuk JSON
            for doc in labeled_data:
                if '_id' in doc and hasattr(doc['_id'], '__class__') and doc['_id'].__class__.__name__ == 'ObjectId':
                    doc['_id'] = str(doc['_id'])
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(labeled_data, f, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"Data labeled harian disimpan ke: {output_path}")
            
            # Lakukan agregasi bulanan jika diperlukan
            aggregate_monthly_data_if_needed(date_obj)
        else:
            logger.info(f"Tidak ada data untuk diproses pada {date_obj.strftime('%Y-%m-%d')}")
            
    except Exception as e:
        logger.error(f"Error saat memproses ulang data untuk {date_obj.strftime('%Y-%m-%d')}: {e}")
    finally:
        if should_close_client:
            client.close()


def process_existing_data_range(start_date, end_date):
    """
    Fungsi untuk memproses ulang rentang data yang sudah ada di database
    """
    # Konversi string ke datetime jika perlu
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
    
    # Inisialisasi client dan collection manager sekali di awal
    client, collection_manager = init_db()
    
    try:
        while current_date <= end_date_date:
            process_existing_data_for_date(current_date, client, collection_manager)
            current_date += timedelta(days=1)
    finally:
        client.close()


if __name__ == "__main__":
    logger.info("Memulai proses ETL maksimum tweet untuk analisis sentimen MBG...")
    
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
    
    # Jalankan proses default
    run_etl()
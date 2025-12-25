"""
fix_unprocessed_monthly_tweets.py
Skrip untuk mendeteksi dan memproses data tweet yang belum diproses di koleksi monthly_tweets_20251001

Fungsi ini menemukan data dalam collection monthly_tweets_20251001 yang belum melalui
proses data cleaning, sentiment classification, dan deteksi lokasi seperti pada
proses harian, kemudian melakukan processing terhadap data-data tersebut.
"""

import json
import logging
from datetime import datetime
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError
import os
import sys
from tqdm import tqdm

# Tambahkan path untuk mengimpor modul lokal
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import fungsi-fungsi dari utils
from utils import (
    apply_data_cleaning,
    apply_sentiment_labeling,
    update_tweet_locations,
    load_indonesian_locations
)

def load_config():
    """
    Memuat konfigurasi dari file JSON.

    Returns:
        dict: Konfigurasi aplikasi dari file JSON
    """
    config_file = "config/config.json"
    with open(config_file, 'r') as f:
        return json.load(f)

# Load configuration
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
logger = logging.getLogger("FixUnprocessedTweets")

def init_db():
    """
    Menginisialisasi koneksi MongoDB ke database dan collection yang ditentukan.

    Returns:
        tuple: Pasangan (client MongoDB, database, collection)
    """
    try:
        # Create MongoDB client with URI from config
        client = MongoClient(CONFIG['database']['mongo_uri'])
        db = client[CONFIG['database']['db_name']]

        # Gunakan collection monthly_tweets_20251001 sesuai permintaan
        collection_name = "monthly_tweets_20251001"
        collection = db[collection_name]

        logger.info(f"Koneksi MongoDB berhasil diinisialisasi ke {CONFIG['database']['db_name']}.{collection_name}.")
        return client, db, collection
    except PyMongoError as e:
        logger.critical(f"Gagal terhubung ke MongoDB: {e}")
        exit(1)

def detect_unprocessed_tweets(collection):
    """
    Mendeteksi tweet-tweet yang belum diproses dalam suatu collection.

    Tweet dianggap belum diproses jika:
    1. Tidak memiliki field 'content.clean_text' (hasil cleaning)
    2. Tidak memiliki field 'sentiment_analysis.label' (hasil sentiment classification)
    3. Tidak memiliki field 'location.province' atau 'location.city' (hasil deteksi lokasi)

    Args:
        collection: MongoDB collection object

    Returns:
        list: Daftar tweet yang belum diproses
    """
    logger.info("Mendeteksi tweet-tweet yang belum diproses...")

    # Query untuk mencari tweet yang belum diproses
    # Kriteria: tidak memiliki clean_text, label sentimen, atau informasi lokasi
    unprocessed_query = {
        "$or": [
            # Belum dilakukan cleaning (tidak ada clean_text)
            {"content.clean_text": {"$exists": False}},
            # Atau belum dilakukan sentiment analysis
            {"sentiment_analysis.label": {"$exists": False}},
            # Atau belum dilakukan deteksi lokasi
            {
                "$or": [
                    {"location": {"$exists": False}},
                    {"location.province": {"$exists": False}},
                    {"location.city": {"$exists": False}}
                ]
            }
        ]
    }

    # Hitung jumlah total dokumen
    total_docs = collection.count_documents({})
    unprocessed_docs = list(collection.find(unprocessed_query))

    logger.info(f"Ditemukan {len(unprocessed_docs)} dari {total_docs} tweet yang belum diproses.")

    return unprocessed_docs

def process_all_unprocessed_tweets(collection, unprocessed_tweets):
    """
    Memproses semua tweet yang belum diproses dalam urutan yang benar:
    1. Cleaning
    2. Sentiment analysis
    3. Location detection

    Args:
        collection: MongoDB collection object
        unprocessed_tweets: List tweet yang belum diproses
    """
    if not unprocessed_tweets:
        logger.info("Tidak ada tweet yang perlu diproses.")
        return 0

    logger.info(f"Memproses {len(unprocessed_tweets)} tweet yang belum diproses...")

    # Proses dalam batch untuk efisiensi
    batch_size = 50
    processed_count = 0

    for i in tqdm(range(0, len(unprocessed_tweets), batch_size), desc="Processing Batches"):
        batch = unprocessed_tweets[i:i + batch_size]

        # Langkah 1: Lakukan cleaning pada batch
        cleaned_batch = apply_data_cleaning(batch)

        # Langkah 2: Lakukan sentiment analysis pada hasil cleaning
        sentiment_batch = apply_sentiment_labeling(cleaned_batch)

        # Langkah 3: Lakukan deteksi lokasi pada hasil sentiment
        location_batch = update_tweet_locations(sentiment_batch)

        # Langkah 4: Update dokumen-dokumen yang telah diproses ke database
        bulk_operations = []
        for original_tweet, processed_tweet in zip(batch, location_batch):
            # Hapus _id dari data yang akan diupdate
            update_data = {k: v for k, v in processed_tweet.items() if k != '_id'}
            bulk_operations.append(
                UpdateOne(
                    {"_id": original_tweet["_id"]},
                    {"$set": update_data}
                )
            )

        # Jalankan operasi bulk update
        if bulk_operations:
            try:
                result = collection.bulk_write(bulk_operations, ordered=False)
                processed_count += result.modified_count
                logger.info(f"Batch {i//batch_size + 1}: {result.modified_count} tweet berhasil diperbarui")
            except PyMongoError as e:
                logger.error(f"Error saat memperbarui batch {i//batch_size + 1}: {e}")

    logger.info(f"Total {processed_count} tweet telah diproses dan diperbarui.")
    return processed_count

def main():
    """
    Fungsi utama untuk menjalankan skrip deteksi dan pemrosesan tweet yang belum diproses.
    """
    print("="*70)
    print("SKRIP PERBAIKAN DATA TWEET YANG BELUM DIPROSES")
    print("Collection: monthly_tweets_20251001")
    print("="*70)

    # Inisialisasi database
    client, db, collection = init_db()

    try:
        # Deteksi tweet yang belum diproses
        print("🔍 Memeriksa collection untuk tweet yang belum diproses...")
        unprocessed_tweets = detect_unprocessed_tweets(collection)

        if not unprocessed_tweets:
            print("✅ Tidak ditemukan tweet yang belum diproses.")
            print("Proses selesai.")
            return

        # Tampilkan ringkasan
        print(f"🔍 Ditemukan {len(unprocessed_tweets)} tweet yang belum diproses:")

        # Analisis tambahan: hitung berapa banyak yang perlu berbagai jenis processing
        needs_cleaning = sum(1 for tweet in unprocessed_tweets if "content.clean_text" not in tweet or tweet.get("content", {}).get("clean_text") is None)
        needs_sentiment = sum(1 for tweet in unprocessed_tweets if "sentiment_analysis.label" not in tweet or tweet.get("sentiment_analysis", {}).get("label") is None)
        needs_location = sum(1 for tweet in unprocessed_tweets
                           if "location" not in tweet or
                              ("province" not in tweet.get("location", {}) and
                               "city" not in tweet.get("location", {})) or
                              (tweet.get("location", {}).get("province") is None and
                               tweet.get("location", {}).get("city") is None))

        print(f"   - {needs_cleaning} tweet perlu cleaning")
        print(f"   - {needs_sentiment} tweet perlu sentiment classification")
        print(f"   - {needs_location} tweet perlu deteksi lokasi")

        # Konfirmasi sebelum memproses
        confirmation = input("\nLanjutkan proses pembaruan? (y/N): ")
        if confirmation.lower() != 'y':
            print("Proses dibatalkan oleh pengguna.")
            return

        # Proses dan update tweet dalam urutan yang benar
        processed_count = process_all_unprocessed_tweets(collection, unprocessed_tweets)

        print(f"\n✅ Proses perbaikan data selesai!")
        print(f"   {processed_count} tweet telah diproses dan diperbarui di database.")

        # Verifikasi: cek apakah masih ada tweet yang belum diproses
        remaining_unprocessed = detect_unprocessed_tweets(collection)
        if len(remaining_unprocessed) == 0:
            print("   ✅ Semua tweet telah diproses sepenuhnya!")
        else:
            print(f"   ⚠️ Masih ada {len(remaining_unprocessed)} tweet yang belum sepenuhnya diproses.")

    except KeyboardInterrupt:
        logger.info("Proses dihentikan oleh pengguna.")
        print("\n❌ Proses dihentikan oleh pengguna.")
    except Exception as e:
        logger.error(f"Terjadi error saat menjalankan skrip: {e}")
        print(f"\n❌ Terjadi error saat menjalankan skrip: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Tutup koneksi database
        client.close()
        logger.info("Koneksi database ditutup.")

if __name__ == "__main__":
    main()
"""
src/utils/daily_collection_manager.py
Modul untuk mengelola koleksi MongoDB berdasarkan tanggal
"""

import logging
from datetime import datetime, timedelta, date
from pymongo import MongoClient
from pymongo.errors import PyMongoError

logger = logging.getLogger("SummaryApp")

class DailyCollectionManager:
    def __init__(self, config):
        self.config = config
        self.client = MongoClient(config['database']['mongo_uri'])
        self.db = self.client[config['database']['db_name']]
        
    def get_collection_by_date(self, date_obj):
        """Mendapatkan nama koleksi berdasarkan tanggal."""
        if isinstance(date_obj, str):
            # Jika string, asumsikan format YYYY-MM-DD dan ubah ke datetime
            date_obj = datetime.strptime(date_obj, '%Y-%m-%d')
        elif isinstance(date_obj, date) and not isinstance(date_obj, datetime):
            # Jika date object (bukan datetime), ubah ke datetime
            date_obj = datetime.combine(date_obj, datetime.min.time())
        elif isinstance(date_obj, datetime):
            # Jika datetime, gunakan langsung
            date_obj = date_obj
        else:
            # Jika jenis lain, asumsikan format timestamp
            date_obj = datetime.fromtimestamp(date_obj)

        # Format tanggal sesuai konfigurasi
        date_str = date_obj.strftime('%Y%m%d')  # Format: YYYYMMDD
        collection_name = f"{self.config['database']['collection_prefix']}{date_str}"

        # Dapatkan koleksi dari database
        collection = self.db[collection_name]

        # Buat index jika belum ada
        self._ensure_indexes(collection)

        return collection, collection_name
    
    def _ensure_indexes(self, collection):
        """Membuat index standar untuk koleksi."""
        try:
            # Membuat index untuk performa kueri
            collection.create_index("metadata.created_at")
            collection.create_index("metadata.location")
            collection.create_index([("content.clean_text", "text")])
            
            logger.debug(f"Index berhasil dibuat untuk koleksi: {collection.name}")
        except PyMongoError as e:
            logger.error(f"Gagal membuat index untuk koleksi {collection.name}: {e}")
    
    def get_all_daily_collections(self, start_date, end_date):
        """Mendapatkan semua koleksi harian dalam rentang tanggal."""
        collections = []
        # Pastikan start_date dan end_date adalah datetime object
        if isinstance(start_date, date) and not isinstance(start_date, datetime):
            start_date = datetime.combine(start_date, datetime.min.time())
        if isinstance(end_date, date) and not isinstance(end_date, datetime):
            end_date = datetime.combine(end_date, datetime.min.time())

        current_date = start_date.date() if isinstance(start_date, datetime) else start_date
        end_date_obj = end_date.date() if isinstance(end_date, datetime) else end_date

        while current_date <= end_date_obj:
            collection, name = self.get_collection_by_date(current_date)
            collections.append((collection, name, current_date))
            current_date += timedelta(days=1)

        return collections

    def get_collection_names_in_range(self, start_date, end_date):
        """Mendapatkan nama-nama koleksi dalam rentang tanggal."""
        collection_names = []
        # Pastikan start_date dan end_date adalah datetime object
        if isinstance(start_date, date) and not isinstance(start_date, datetime):
            start_date = datetime.combine(start_date, datetime.min.time())
        if isinstance(end_date, date) and not isinstance(end_date, datetime):
            end_date = datetime.combine(end_date, datetime.min.time())

        current_date = start_date.date() if isinstance(start_date, datetime) else start_date
        end_date_obj = end_date.date() if isinstance(end_date, datetime) else end_date

        while current_date <= end_date_obj:
            _, name = self.get_collection_by_date(current_date)
            collection_names.append(name)
            current_date += timedelta(days=1)

        return collection_names
"""
Utils module for ETL Automation - MBG Sentiment
Contains utility functions for data cleaning, sentiment labeling, and monthly aggregation

Module ini menyediakan berbagai fungsi utilitas penting untuk pipeline ETL,
termasuk pembersihan teks, analisis sentimen, agregasi data, dan manajemen koleksi MongoDB.
"""

import logging
import re
import json
import pandas as pd
from datetime import datetime
import os
from typing import List, Dict, Tuple, Any

# Import for sentiment analysis
try:
    from transformers import AutoTokenizer, AutoModelForSequenceClassification
    from transformers import pipeline
    import torch
except ImportError:
    print("Transformers library not installed. Install with 'pip install transformers torch'")

logger = logging.getLogger(__name__)


def clean_tweet_text(text: str) -> str:
    """
    Clean tweet text by removing URLs, mentions, hashtags, and extra whitespaces
    """
    if not isinstance(text, str):
        return ""

    # Remove URLs
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)

    # Remove user mentions (@username)
    text = re.sub(r'@\w+', '[MENTION]', text)

    # Remove hashtags (#hashtag)
    text = re.sub(r'#\w+', '[HASHTAG]', text)

    # Remove extra whitespaces and newlines
    text = re.sub(r'\s+', ' ', text)

    # Remove leading and trailing spaces
    text = text.strip()

    return text.lower()


def initialize_sentiment_classifier():
    """
    Initialize the Indonesian sentiment classification model
    """
    try:
        model_name = "w11wo/indonesian-roberta-base-sentiment-classifier"
        tokenizer = AutoTokenizer.from_pretrained(model_name)
        model = AutoModelForSequenceClassification.from_pretrained(model_name)

        # Create a sentiment analysis pipeline
        sentiment_pipeline = pipeline(
            "sentiment-analysis",
            model=model,
            tokenizer=tokenizer,
            device=0 if torch.cuda.is_available() else -1  # Use GPU if available
        )

        return sentiment_pipeline
    except Exception as e:
        logger.error(f"Error initializing sentiment classifier: {e}")
        return None


def classify_sentiment(text: str, sentiment_pipeline) -> Tuple[str, float]:
    """
    Classify sentiment for a single text using the sentiment pipeline
    """
    try:
        if not text or not isinstance(text, str):
            return 'NEUTRAL', 0.0

        # Truncate text if too long (some models have token limits)
        if len(text) > 512:
            text = text[:512]

        result = sentiment_pipeline(text)[0]
        return result['label'], result['score']
    except Exception as e:
        logger.error(f"Error processing text: {text[:50] if text else 'None'}..., Error: {str(e)}")
        return 'NEUTRAL', 0.0


def apply_data_cleaning(raw_data: List[Dict]) -> List[Dict]:
    """
    Apply data cleaning to the raw tweet data
    """
    logger.info(f"Starting data cleaning for {len(raw_data)} tweets...")

    cleaned_data = []
    for tweet in raw_data:
        # Copy the original tweet data
        cleaned_tweet = tweet.copy()

        # Clean the text content
        original_text = tweet.get('content', {}).get('text', '')
        cleaned_text = clean_tweet_text(original_text)

        # Add cleaned text to content
        if 'content' not in cleaned_tweet:
            cleaned_tweet['content'] = {}
        cleaned_tweet['content']['clean_text'] = cleaned_text

        # Update location information if it's missing or needs enhancement
        # This will apply location detection if original location is null
        if not tweet.get('location') or tweet.get('location') is None or tweet.get('location') == '':
            # Extract text and author information for location detection
            # The text might be at different levels depending on the source
            text_content = tweet.get('content', {}).get('text', '')
            if not text_content:  # If not in content.text, try directly
                text_content = tweet.get('text', '')
            author_name = tweet.get('author_name', '')
            author_handle = tweet.get('author_handle', '')

            # Detect location from text and author
            location_data = detect_location_from_text(text_content, author_name)

            # Create structured location information
            location_info = {
                "province": location_data["province"],
                "city": location_data["city"],
                "detected_from": "text_analysis",
                "original_location": None
            }

            # Update location in metadata
            if 'metadata' not in cleaned_tweet:
                cleaned_tweet['metadata'] = {}
            cleaned_tweet['metadata']['location'] = location_info

            # Also update root level location if needed
            cleaned_tweet['location'] = location_info

        # Set processing status for cleaning
        if 'processing_status' not in cleaned_tweet:
            cleaned_tweet['processing_status'] = {}
        cleaned_tweet['processing_status']['cleaning_completed'] = True
        cleaned_tweet['processing_status']['cleaning_timestamp'] = datetime.now().isoformat()

        cleaned_data.append(cleaned_tweet)

    logger.info(f"Data cleaning completed for {len(cleaned_data)} tweets")
    return cleaned_data


def apply_sentiment_labeling(raw_data: List[Dict], batch_size: int = 50) -> List[Dict]:
    """
    Apply sentiment labeling to the tweet data
    """
    logger.info(f"Starting sentiment labeling for {len(raw_data)} tweets...")

    # Initialize sentiment classifier
    sentiment_pipeline = initialize_sentiment_classifier()
    if not sentiment_pipeline:
        logger.error("Failed to initialize sentiment classifier")
        return raw_data

    labeled_data = []

    # Process in batches to manage memory usage
    for i in range(0, len(raw_data), batch_size):
        batch = raw_data[i:i + batch_size]
        logger.info(f"Processing batch {i // batch_size + 1}/{(len(raw_data) - 1) // batch_size + 1}")

        for tweet in batch:
            # Copy the original tweet data
            labeled_tweet = tweet.copy()

            # Get the cleaned text for sentiment analysis
            text_to_analyze = tweet.get('content', {}).get('clean_text', '')

            # Classify sentiment
            label, score = classify_sentiment(text_to_analyze, sentiment_pipeline)

            # Add sentiment analysis results
            if 'sentiment_analysis' not in labeled_tweet:
                labeled_tweet['sentiment_analysis'] = {}
            labeled_tweet['sentiment_analysis']['label'] = label
            labeled_tweet['sentiment_analysis']['confidence_score'] = float(score)

            # Update processing status
            if 'processing_status' not in labeled_tweet:
                labeled_tweet['processing_status'] = {}
            labeled_tweet['processing_status']['sentiment_analyzed'] = True
            labeled_tweet['processing_status']['sentiment_analysis_timestamp'] = datetime.now().isoformat()

            labeled_data.append(labeled_tweet)

    logger.info(f"Sentiment labeling completed for {len(labeled_data)} tweets")
    return labeled_data


def flatten_tweet_data(tweet: Dict) -> Dict:
    """
    Flatten a single tweet dictionary for DataFrame conversion
    """
    flat_tweet = {
        '_id': tweet.get('_id'),
        'text': tweet.get('content', {}).get('text', ''),
        'clean_text': tweet.get('content', {}).get('clean_text', ''),
        'author_handle': tweet.get('metadata', {}).get('author_handle', ''),
        'created_at': tweet.get('metadata', {}).get('created_at', {}).get('$date', ''),
        'tweet_url': tweet.get('metadata', {}).get('tweet_url', ''),
        'reply_count': tweet.get('metrics', {}).get('reply_count', 0),
        'retweet_count': tweet.get('metrics', {}).get('retweet_count', 0),
        'like_count': tweet.get('metrics', {}).get('like_count', 0),
        'sentiment_label': tweet.get('sentiment_analysis', {}).get('label', ''),
        'sentiment_confidence': tweet.get('sentiment_analysis', {}).get('confidence_score', 0.0),
        'sentiment_analyzed': tweet.get('processing_status', {}).get('sentiment_analyzed', False)
    }
    return flat_tweet


def aggregate_monthly_data(daily_files: List[str], output_path: str) -> bool:
    """
    Aggregate daily data files into a monthly file
    """
    logger.info(f"Starting monthly aggregation for {len(daily_files)} daily files...")

    all_data = []

    for file_path in daily_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                daily_data = json.load(f)
                all_data.extend(daily_data)
            logger.info(f"Loaded {len(daily_data)} tweets from {file_path}")
        except Exception as e:
            logger.error(f"Error loading daily file {file_path}: {e}")
            continue

    # Save aggregated data to monthly file
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)

        logger.info(f"Monthly aggregation completed. Total tweets: {len(all_data)}")
        logger.info(f"Aggregated data saved to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error saving aggregated data to {output_path}: {e}")
        return False


def save_monthly_data_labeled(monthly_data: List[Dict], start_date: datetime, end_date: datetime) -> str:
    """
    Save monthly data to a labeled JSON file
    """
    logger.info(f"Saving monthly labeled data for period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    # Create output path for monthly file
    os.makedirs('data', exist_ok=True)
    output_path = f"data/mbg_sentiment_db.tweets_{start_date.strftime('%Y-%m')}_labeled.json"

    # Convert ObjectId to string for JSON
    for doc in monthly_data:
        if '_id' in doc and hasattr(doc['_id'], '__class__') and doc['_id'].__class__.__name__ == 'ObjectId':
            doc['_id'] = str(doc['_id'])

    # Write labeled data to JSON file
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(monthly_data, f, ensure_ascii=False, indent=2, default=str)

        logger.info(f"Monthly labeled data saved to: {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Error saving monthly labeled data to {output_path}: {e}")
        return ""


def get_daily_files_for_month(month_dir: str, year: int, month: int) -> List[str]:
    """
    Get all daily files for a specific month
    """
    import re
    daily_files = []

    # Regular expression to match daily files for a specific month
    pattern = re.compile(f"mbg_sentiment_db\\.tweets_{year}-{month:02d}-\\d{{2}}_labeled\\.json$")

    for filename in os.listdir(month_dir):
        if pattern.match(filename):
            daily_files.append(os.path.join(month_dir, filename))

    return sorted(daily_files)


def load_indonesian_locations():
    """
    Load Indonesian provinces and cities from the JSON configuration file
    """
    try:
        with open("config/indonesia_locations.json", "r", encoding="utf-8") as f:
            locations = json.load(f)
        return locations
    except FileNotFoundError:
        logger.warning("indonesia_locations.json file not found. Creating default structure...")
        # Default minimal structure if file doesn't exist
        default_locations = {
            "DKI Jakarta": ["Jakarta Selatan", "Jakarta Pusat", "Jakarta Barat", "Jakarta Utara", "Jakarta Timur"],
            "Jawa Barat": ["Bandung", "Bekasi", "Depok", "Cimahi", "Tasikmalaya"],
            "Jawa Tengah": ["Semarang", "Solo", "Yogyakarta", "Magelang", "Surakarta"],
            "Jawa Timur": ["Surabaya", "Malang", "Sidoarjo", "Madiun", "Kediri"],
            "Banten": ["Tangerang", "Serang", "Cilegon", "Tangerang Selatan"]
        }
        # Create directory if it doesn't exist
        os.makedirs("config", exist_ok=True)

        # Save default structure
        with open("config/indonesia_locations.json", "w", encoding="utf-8") as f:
            json.dump(default_locations, f, ensure_ascii=False, indent=2)
        return default_locations
    except json.JSONDecodeError:
        logger.error("Error reading indonesia_locations.json file. Using default structure.")
        return {
            "DKI Jakarta": ["Jakarta Selatan", "Jakarta Pusat", "Jakarta Barat", "Jakarta Utara", "Jakarta Timur"],
            "Jawa Barat": ["Bandung", "Bekasi", "Depok", "Cimahi", "Tasikmalaya"],
            "Jawa Tengah": ["Semarang", "Solo", "Yogyakarta", "Magelang", "Surakarta"],
            "Jawa Timur": ["Surabaya", "Malang", "Sidoarjo", "Madiun", "Kediri"],
            "Banten": ["Tangerang", "Serang", "Cilegon", "Tangerang Selatan"]
        }


def detect_location_from_text(text, author_name=None):
    """
    Detect Indonesian province and city from text content and optionally author name
    Returns a dictionary with detected province and city if found
    """
    if not text:
        return {"province": None, "city": None}

    # Load location data
    indonesian_locations = load_indonesian_locations()

    # Prepare text for matching - convert to lowercase and handle variations
    text_lower = text.lower()

    # Add author name to search if provided
    if author_name:
        text_lower += " " + author_name.lower()

    detected_province = None
    detected_city = None

    # Prepare text by adding spaces around common location indicators for better matching
    text_for_matching = text_lower
    # Replace common location separators with spaces for better word boundary matching
    for separator in ['-', '/', '\\', '|', '_', ',', ';', '.']:
        text_for_matching = text_for_matching.replace(separator, ' ')

    # First, try to find cities by checking all kabupaten/kota with multiple matching strategies
    for province, cities in indonesian_locations.items():
        for city in cities:
            # Case 1: Exact word boundary match
            city_lower = city.lower()
            if re.search(r'\b' + re.escape(city_lower) + r'\b', text_for_matching):
                detected_city = city
                detected_province = province
                break

            # Case 2: Partial match with common variations (e.g., "Jakarta" in "Jakarta Selatan")
            # Split the city name and check if any part appears in the text
            city_parts = city_lower.split()
            for part in city_parts:
                if len(part) > 2 and re.search(r'\b' + re.escape(part) + r'\b', text_for_matching):
                    # Verify this is likely the right city by checking if other parts might also be present
                    detected_city = city
                    detected_province = province
                    break

            # Case 3: Common location abbreviations and forms
            abbreviations = {
                'jaksel': 'jakarta selatan',
                'jaktim': 'jakarta timur',
                'jakbar': 'jakarta barat',
                'jakut': 'jakarta utara',
                'jakselpusat': 'jakarta pusat',
                'sby': 'surabaya',
                'bdg': 'bandung',
                'smg': 'semarang',
                'ygy': 'yogyakarta'
            }
            for abbrev, full_name in abbreviations.items():
                if abbrev in text_lower and province in full_name:
                    detected_city = city
                    detected_province = province
                    break

        if detected_city:
            break

    # If no city detected but we want to look for province names
    if not detected_city:
        for province, cities in indonesian_locations.items():
            province_lower = province.lower()
            # Check for province name in the text with word boundaries
            if re.search(r'\b' + re.escape(province_lower) + r'\b', text_for_matching):
                detected_province = province
                break

            # Additional check: common province abbreviations
            province_variations = [
                province_lower.replace(' ', ''),
                province_lower.replace('dki ', ''),
                province_lower.replace('di ', ''),
                province_lower.replace('provinsi ', ''),
                province_lower.replace('nusa tenggara', 'nt').replace('barat', 'b'),
                province_lower.replace('nusa tenggara', 'nt').replace('timur', 't'),
                province_lower.replace('kalimantan', 'kalt'),
                province_lower.replace('sulawesi', 'sul'),
                province_lower.replace('maluku', 'mal')
            ]

            for variation in province_variations:
                if variation and re.search(r'\b' + re.escape(variation) + r'\b', text_for_matching):
                    detected_province = province
                    break

            if detected_province:
                break

    return {
        "province": detected_province,
        "city": detected_city
    }


def detect_location_fuzzy(text, author_name=None, threshold=0.7):
    """
    Detect Indonesian province and city using fuzzy matching for when exact matches fail
    """
    try:
        from fuzzywuzzy import fuzz
    except ImportError:
        # If fuzzywuzzy is not available, fall back to exact matching
        return detect_location_from_text(text, author_name)

    if not text:
        return {"province": None, "city": None}

    # Load location data
    indonesian_locations = load_indonesian_locations()

    # Prepare text for matching
    text_lower = text.lower()
    if author_name:
        text_lower += " " + author_name.lower()

    detected_province = None
    detected_city = None
    best_score = 0

    # Check all cities first for fuzzy matches
    for province, cities in indonesian_locations.items():
        for city in cities:
            # Calculate similarity score
            score_city = fuzz.partial_ratio(city.lower(), text_lower)
            score_text = fuzz.partial_ratio(text_lower, city.lower())
            final_score = max(score_city, score_text)

            if final_score > best_score and final_score >= threshold * 100:
                best_score = final_score
                detected_city = city
                detected_province = province

    # If no good city match, try province names
    if not detected_city and best_score < threshold * 100:
        for province, cities in indonesian_locations.items():
            score_province = fuzz.partial_ratio(province.lower(), text_lower)
            score_text = fuzz.partial_ratio(text_lower, province.lower())
            final_score = max(score_province, score_text)

            if final_score > best_score and final_score >= threshold * 100:
                best_score = final_score
                detected_province = province

    return {
        "province": detected_province,
        "city": detected_city
    }


def update_tweet_locations(tweet_data_list):
    """
    Update a list of tweet data with detected locations
    """
    updated_tweets = []
    for tweet in tweet_data_list:
        # Get text content to search for locations
        text = tweet.get('content', {}).get('text', '')
        author_name = tweet.get('author_name', '')
        author_handle = tweet.get('author_handle', '')

        # Combine author information for location detection
        all_text = text + " " + author_name + " " + author_handle

        # Detect location from the combined text using exact matching first
        # (fuzzy matching is reserved for post-processing due to performance considerations)
        location_data = detect_location_from_text(text, author_name)

        # Update tweet with location information
        updated_tweet = tweet.copy()

        # Create location field structure
        location_info = {
            "province": location_data["province"],
            "city": location_data["city"],
            "detected_from": "text" if location_data["province"] or location_data["city"] else "none"
        }

        # Update the location field in the tweet metadata
        if 'metadata' not in updated_tweet:
            updated_tweet['metadata'] = {}

        # If original location was null, replace with our detection
        original_location = tweet.get('location') or tweet.get('metadata', {}).get('location')
        if not original_location or original_location is None:
            updated_tweet['metadata']['location'] = location_info
        else:
            # If location already exists, we keep the original but add our detected location as well
            updated_tweet['metadata']['location'] = {
                "original": original_location,
                "detected": location_info
            }

        # For consistency, also add to root level if needed
        if 'location' not in updated_tweet:
            updated_tweet['location'] = location_info
        elif updated_tweet.get('location') is None or updated_tweet.get('location') == '':
            updated_tweet['location'] = location_info

        updated_tweets.append(updated_tweet)

    return updated_tweets


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
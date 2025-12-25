"""
src/resilient_scraper.py
Scraper yang lebih tahan lama dan bisa melanjutkan dari titik terakhir jika terputus

Module ini menyediakan kelas ResilientScraper yang dirancang untuk
mengumpulkan data tweet dari X/Twitter dengan ketahanan tinggi terhadap
koneksi terputus dan mekanisme retry otomatis.
"""

import time
import logging
import json
import re
import random
from datetime import datetime, timedelta, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import dateutil.parser
from pymongo import UpdateOne
from pymongo.errors import PyMongoError
from tqdm import tqdm

# Initialize logger for scraper module
logger = logging.getLogger("SummaryApp")

class ResilientScraper:
    """
    Kelas scraper tangguh yang dirancang untuk mengumpulkan data tweet dari X/Twitter.

    Kelas ini menyediakan mekanisme ketahanan terhadap koneksi terputus,
    deteksi elemen yang gagal dimuat, dan penanganan kesalahan otomatis.
    """
    def __init__(self, driver, config, collection_manager):
        """
        Inisialisasi scraper tangguh.

        Args:
            driver: Instance Chrome driver untuk scraping
            config (dict): Konfigurasi aplikasi dari file JSON
            collection_manager: Instance DailyCollectionManager untuk mengelola koleksi MongoDB
        """
        self.driver = driver
        self.config = config
        self.collection_manager = collection_manager

        # Scraping configuration
        self.max_tweets = config['twitter']['max_tweets']
        self.scroll_pause_min = config['scraper'].get('scroll_min_pause', 1.0)
        self.scroll_pause_max = config['scraper'].get('scroll_max_pause', 3.0)
        self.max_scrolls = 100000  # Very high number to allow extended scraping (effectively unlimited)

        # Sets to prevent duplication within one session
        self.processed_tweet_ids = set()  # Track processed tweet IDs to avoid duplicates
        self.processed_texts = set()      # Track processed text content to avoid duplicates

    def inject_cookies(self):
        """
        Menyuntikkan cookie sesi dari file JSON.

        Fungsi ini membaca file cookie dari konfigurasi dan menyuntikannya
        ke dalam sesi browser untuk mengotentikasi pengguna tanpa perlu login manual.
        """
        try:
            logger.info("Navigasi awal ke x.com...")
            # Navigate to x.com to establish base domain context
            self.driver.get("https://x.com")
            time.sleep(5)  # Additional time to ensure page loads

            # Load cookies from JSON file
            with open(self.config['twitter']['cookies_file'], 'r') as f:
                cookies = json.load(f)

            logger.info(f"Memuat {len(cookies)} cookie...")
            # Inject each cookie into the browser session
            for cookie in cookies:
                cookie_dict = {
                    'name': cookie.get('name'),           # Cookie name
                    'value': cookie.get('value'),         # Cookie value
                    'domain': cookie.get('domain', '.x.com'),  # Domain for the cookie
                    'path': cookie.get('path', '/'),       # Path for the cookie
                    'secure': cookie.get('secure', True),  # Whether cookie should be secure
                    'httpOnly': cookie.get('httpOnly', False),  # Whether cookie is HTTP only
                    'sameSite': 'Lax'                      # SameSite attribute
                }
                try:
                    self.driver.add_cookie(cookie_dict)
                except:
                    continue  # Skip problematic cookies

            logger.info("Cookie disuntikkan. Refresh halaman...")
            # Refresh the page to apply cookies
            self.driver.refresh()
            time.sleep(8)  # Wait longer after refresh for cookies to take effect

            # Check if the URL contains login, indicating session might be invalid
            if "login" in self.driver.current_url.lower():
                logger.warning("Sesi mungkin tidak valid.")
            else:
                logger.info("Login via cookie berhasil.")

        except FileNotFoundError:
            logger.error(f"File {self.config['twitter']['cookies_file']} tidak ditemukan!")
            exit(1)
        except Exception as e:
            logger.error(f"Error saat injeksi cookie: {e}")
            exit(1)

    def _generate_extended_keywords(self):
        """
        Menghasilkan variasi kata kunci terkait MBG untuk menemukan lebih banyak tweet.

        Fungsi ini menghasilkan berbagai variasi dari kata kunci utama
        untuk meningkatkan peluang menemukan tweet terkait MBG.

        Returns:
            list: Daftar semua variasi kata kunci untuk pencarian
        """
        # Base keywords related to MBG (Makanan Bergizi Gratis)
        base_keywords = [
            "Makan Bergizi Gratis", "MBG", "Program Makan Bergizi",
            "Makan Gratis", "Program MBG", "Kementerian PPPA",
            "Pangan Anak", "Nutrisi Anak", "Makanan Gratis Anak"
        ]

        # Variations of spelling and abbreviations
        variations = []
        for keyword in base_keywords:
            variations.append(keyword)
            # Add variations with spaces removed
            variations.append(keyword.replace(" ", ""))
            # Add variations with different capitalization
            variations.append(keyword.lower())
            variations.append(keyword.upper())

        # Add related keywords
        related_keywords = [
            "gizi", "anak", "makan", "gratis", "sehat", "nutrisi",
            "kakak asuh", "anak asuh", "kementerian", "PPPA", "pembelajaran",
            "pendidikan", "kesehatan", "makan siang", "kakak asuh", "program"
        ]

        return variations + related_keywords

    def build_search_query(self, target_date, additional_keywords=None):
        """
        Membangun query pencarian canggih untuk mendapatkan lebih banyak tweet.

        Fungsi ini menggabungkan query dasar dari konfigurasi dengan variasi
        kata kunci tambahan dan rentang tanggal untuk memaksimalkan hasil pencarian.

        Args:
            target_date (datetime.date): Tanggal target untuk pencarian tweet
            additional_keywords (list, optional): Kata kunci tambahan untuk pencarian

        Returns:
            str: Query pencarian lengkap dengan rentang tanggal
        """
        # Get base query from configuration (query_1) and its variations
        base_query = self.config['twitter'].get('query_1', 'Makan Bergizi Gratis OR MBG lang:id')

        # Format date range
        since_date = target_date.strftime('%Y-%m-%d')
        until_date = (target_date + timedelta(days=1)).strftime('%Y-%m-%d')

        # Build query with OR to capture more tweets
        keyword_variations = self._generate_extended_keywords()

        # Create OR query for keyword variations (only take first 5 to avoid too long query)
        or_keywords = " OR ".join([f'"{kw}"' for kw in keyword_variations[:5]])

        # Combine base query with variations
        full_query = f"({base_query}) OR ({or_keywords})"

        # Add date range to query
        search_query = f"{full_query} since:{since_date} until:{until_date}"

        return search_query

    def build_monthly_queries(self, start_date, end_date):
        """
        Membangun query pencarian untuk mencakup seluruh rentang bulan.

        Fungsi ini menggabungkan query dasar dari konfigurasi dengan rentang tanggal bulanan
        untuk mendapatkan tweet dari seluruh bulan sekaligus.

        Args:
            start_date (datetime.date): Tanggal awal rentang bulan
            end_date (datetime.date): Tanggal akhir rentang bulan

        Returns:
            list: Daftar query pencarian untuk rentang bulan
        """
        # Format date range for monthly queries
        since_date = start_date.strftime('%Y-%m-%d')
        until_date = end_date.strftime('%Y-%m-%d')  # Include the full end date

        queries = []

        # Use query_1 to query_5 from configuration with monthly date range
        for i in range(1, 6):
            query_key = f'query_{i}'
            if query_key in self.config['twitter']:
                query_value = self.config['twitter'][query_key]
                queries.append(f"{query_value} since:{since_date} until:{until_date}")

        # If no queries are configured, use default monthly queries
        if not queries:
            base_query = self.config['twitter'].get('query_1', 'Makan Bergizi Gratis OR MBG lang:id')
            queries = [
                f"{base_query} since:{since_date} until:{until_date}",
                f"('Makan Bergizi Gratis' OR 'MBG' OR 'makan gratis') since:{since_date} until:{until_date}",
                f"('gizi anak' OR 'makanan gratis' OR 'MBG') since:{since_date} until:{until_date}",
                f"('makan gratis' OR 'program makan' OR 'makan bersama') lang:id since:{since_date} until:{until_date}",
                f"({base_query}) (Jakarta OR Surabaya OR Bandung OR Medan OR Makassar OR Palembang OR Semarang OR Yogyakarta) since:{since_date} until:{until_date}"
            ]

        return queries

    def build_alternative_queries(self, target_date):
        """
        Membangun beberapa query alternatif untuk mendapatkan lebih banyak tweet.

        Fungsi ini membuat berbagai variasi query pencarian untuk meningkatkan
        jumlah tweet yang ditemukan dalam satu pencarian.

        Args:
            target_date (datetime.date): Tanggal target untuk pencarian tweet

        Returns:
            list: Daftar query pencarian alternatif
        """
        # Format date range for queries
        since_date = target_date.strftime('%Y-%m-%d')
        until_date = (target_date + timedelta(days=1)).strftime('%Y-%m-%d')

        queries = []

        # Use query_1 to query_5 from configuration
        for i in range(1, 6):
            query_key = f'query_{i}'
            if query_key in self.config['twitter']:
                query_value = self.config['twitter'][query_key]
                queries.append(f"{query_value} since:{since_date} until:{until_date}")

        # If no queries are configured, use default queries
        if not queries:
            base_query = self.config['twitter'].get('query_1', 'Makan Bergizi Gratis OR MBG lang:id')
            queries = [
                f"{base_query} since:{since_date} until:{until_date}",
                f"('Makan Bergizi Gratis' OR 'MBG' OR 'makan gratis') since:{since_date} until:{until_date}",
                f"('gizi anak' OR 'makanan gratis' OR 'MBG') since:{since_date} until:{until_date}",
                f"('makan gratis' OR 'program makan' OR 'makan bersama') lang:id since:{since_date} until:{until_date}",
                f"({base_query}) (Jakarta OR Surabaya OR Bandung OR Medan OR Makassar OR Palembang OR Semarang OR Yogyakarta) since:{since_date} until:{until_date}"
            ]

        return queries

    def navigate_to_search(self, target_date):
        """
        Navigasi ke halaman pencarian dengan query maksimum.

        Fungsi ini membuka halaman pencarian X/Twitter dengan query yang dibangun
        dan menunggu elemen tweet muncul sebelum kembali.

        Args:
            target_date (datetime.date): Tanggal target untuk pencarian tweet
        """
        # Build search query for the target date
        search_query = self.build_search_query(target_date)

        # Format search URL
        encoded_query = search_query.replace(' ', '%20').replace(':', '%3A').replace(',', '%2C')
        search_url = f"https://x.com/search?q={encoded_query}&src=typed_query&f=live"

        logger.info(f"Mengakses URL Pencarian: {search_url}")
        # Navigate to search page
        self.driver.get(search_url)

        # Wait for tweet elements to appear or timeout
        try:
            # Wait a few seconds before looking for elements to ensure page loads
            time.sleep(3)
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'article[data-testid="tweet"]'))
            )
        except:
            logger.warning("Tidak menemukan elemen tweet, coba alternatif...")
            # Try finding tweet elements with alternative selector
            try:
                time.sleep(5)
                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="cellInnerDiv"]'))
                )
            except:
                logger.warning("Tetap tidak menemukan elemen tweet")

        # Add additional delay to ensure page is fully ready
        time.sleep(2)

    def extract_tweets_advanced(self):
        """
        Ekstrak tweet dengan pendekatan lebih cepat dan efisien.

        Fungsi ini mengambil elemen tweet dari halaman dan mengekstrak
        data tweet menggunakan pendekatan cepat dengan Selenium langsung.

        Returns:
            list: Daftar tweet yang diekstrak dari halaman
        """
        try:
            # Kurangi waktu tunggu untuk kecepatan
            time.sleep(0.5)  # Dikurangi dari 2 detik

            # Deteksi masalah dengan cara yang lebih cepat
            if self.detect_something_went_wrong():
                logger.warning("Menemukan pesan error, mencoba mekanisme retry...")
                print("  [WARNING] Menemukan pesan error, mencoba retry...")

                retry_success = self.handle_retry_mechanism(max_retries=self.config['scraper'].get('max_retry_attempts', 10))

                if not retry_success:
                    logger.warning("Mekanisme retry gagal...")
                    print("  [RETRY] Retry gagal...")
                    time.sleep(2)  # Dikurangi dari 5
                else:
                    logger.info("Retry berhasil...")
                    print("  [SUCCESS] Retry berhasil...")
                    time.sleep(1)  # Dikurangi

            # Gunakan selector yang lebih spesifik dan cepat
            tweet_elements = self.driver.find_elements(By.CSS_SELECTOR, 'article[data-testid="tweet"]')

            # Jika tidak ditemukan, coba alternatif dengan waktu minimal
            if len(tweet_elements) == 0:
                tweet_elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-testid="cellInnerDiv"] article')

            # Jika tetap tidak ada, kembalikan kosong
            if len(tweet_elements) == 0:
                return []

            tweets = []
            processed_on_page = 0
            max_per_page = 50  # Meningkatkan dari 20 untuk lebih banyak data per halaman

            for element in tweet_elements:
                try:
                    # Proses langsung dengan Selenium untuk kecepatan (hindari BeautifulSoup untuk ekstraksi awal)
                    tweet_data = self._extract_tweet_data_fast_simple(element)

                    if tweet_data and tweet_data.get('_id') not in self.processed_tweet_ids:
                        # Filter duplikat dengan hash teks
                        text_hash = hash(tweet_data['text'].strip().lower())
                        if text_hash not in self.processed_texts:
                            tweets.append(tweet_data)
                            self.processed_tweet_ids.add(tweet_data['_id'])
                            self.processed_texts.add(text_hash)
                            processed_on_page += 1

                            # Meningkatkan jumlah maksimum per halaman
                            if processed_on_page >= max_per_page:
                                break
                except Exception as e:
                    # Gunakan level debug untuk menghindari logging berlebihan
                    logger.debug(f"Error memproses elemen: {str(e)[:50]}...")
                    continue

            if tweets:
                print(f"  [SUCCESS] Berhasil mengekstrak {len(tweets)} tweet baru")
            return tweets
        except Exception as e:
            logger.error(f"Error dalam extract_tweets_advanced: {str(e)}")
            return []

    def _extract_tweet_data_fast_simple(self, element):
        """
        Ekstrak data tweet dengan pendekatan sangat cepat menggunakan Selenium langsung.

        Args:
            element: Elemen Selenium dari tweet

        Returns:
            dict or None: Data tweet yang diekstrak atau None jika gagal
        """
        try:
            # Ambil teks tweet langsung dari elemen
            try:
                text_element = element.find_element(By.CSS_SELECTOR, 'div[data-testid="tweetText"]')
                text = text_element.text if text_element else ""
            except:
                # Coba selector alternatif
                try:
                    text_elements = element.find_elements(By.CSS_SELECTOR, 'div[lang]')
                    text = " ".join([el.text for el in text_elements]) if text_elements else ""
                except:
                    text = ""

            if not text or len(text.strip()) < 5:
                return None

            # Ambil URL tweet dan ID
            try:
                link_elements = element.find_elements(By.CSS_SELECTOR, 'a[href*="/status/"]')
                tweet_url = ""
                tweet_id = ""

                for link in link_elements:
                    href = link.get_attribute('href')
                    if '/status/' in href and 'photo' not in href.lower() and 'video' not in href.lower():
                        parts = href.split('/')
                        if 'status' in parts:
                            idx = parts.index('status')
                            if len(parts) > idx + 1:
                                tweet_id = parts[idx + 1]
                                tweet_url = f"https://x.com{href}" if not href.startswith('http') else href
                                break
            except:
                tweet_url = ""
                tweet_id = ""

            if not tweet_id:
                return None

            # Ambil nama penulis
            try:
                author_elements = element.find_elements(By.CSS_SELECTOR, 'div[data-testid="User-Names"] span:first-child')
                author_name = author_elements[0].text if author_elements else ""
            except:
                author_name = ""

            # Ambil handle penulis
            try:
                handle_elements = element.find_elements(By.CSS_SELECTOR, 'span[data-testid="User-Names"] a')
                author_handle = handle_elements[0].text if handle_elements else ""
                if not author_handle:
                    # Alternatif untuk handle
                    handle_elements = element.find_elements(By.CSS_SELECTOR, 'a[href^="/"]')
                    if handle_elements:
                        href = handle_elements[0].get_attribute('href')
                        if href:
                            author_handle = href.split('/')[1]
            except:
                author_handle = ""

            # Ambil waktu
            try:
                time_element = element.find_element(By.CSS_SELECTOR, 'time')
                datetime_attr = time_element.get_attribute('datetime')
                if datetime_attr:
                    created_at = dateutil.parser.isoparse(datetime_attr)
                else:
                    created_at = datetime.utcnow()
            except:
                created_at = datetime.utcnow()

            # Ambil metrik (reply, retweet, like)
            metrics = {'reply_count': 0, 'retweet_count': 0, 'like_count': 0}

            try:
                # Mencari elemen metrik dengan pendekatan yang lebih efisien
                button_elements = element.find_elements(By.CSS_SELECTOR, 'div[role="group"] div[role="button"], button[data-testid*="Button"]')

                for btn in button_elements:
                    btn_text = btn.text.lower()
                    if 'reply' in btn_text or 'balas' in btn_text:
                        numbers = re.findall(r'\d+', btn_text)
                        if numbers:
                            metrics['reply_count'] = int(numbers[0]) if numbers[0] else 0
                    elif 'retweet' in btn_text or 'retwit' in btn_text:
                        numbers = re.findall(r'\d+', btn_text)
                        if numbers:
                            metrics['retweet_count'] = int(numbers[0]) if numbers[0] else 0
                    elif 'like' in btn_text or 'suka' in btn_text:
                        numbers = re.findall(r'\d+', btn_text)
                        if numbers:
                            metrics['like_count'] = int(numbers[0]) if numbers[0] else 0
            except:
                pass  # Tetap gunakan default jika metrik gagal diambil

            # Ambil lokasi
            try:
                location_elements = element.find_elements(By.CSS_SELECTOR, 'span[data-testid="UserLocation"]')
                location_text = location_elements[0].text if location_elements else None
            except:
                location_text = None

            # Kembalikan data yang diekstrak
            return {
                '_id': tweet_id,
                'text': text.strip(),
                'created_at': created_at,
                'tweet_url': tweet_url,
                'author_handle': author_handle,
                'author_name': author_name,
                'location': location_text,
                'metrics': metrics
            }

        except Exception as e:
            logger.debug(f"Error dalam _extract_tweet_data_fast_simple: {str(e)[:50]}...")
            return None

    def _extract_tweet_data_fast(self, inner_html, element):
        """
        Ekstrak data tweet dengan pendekatan super cepat.

        Fungsi ini menguraikan HTML tweet untuk mengekstrak informasi penting
        seperti teks tweet, ID, URL, penulis, waktu, dan metrik.

        Args:
            inner_html (str): HTML dari elemen tweet
            element: Elemen Selenium dari tweet

        Returns:
            dict or None: Data tweet yang diekstrak atau None jika gagal
        """
        try:
            # Parse HTML to get important information
            soup = BeautifulSoup(inner_html, 'html.parser')

            # Get tweet text
            text_divs = soup.find_all('div', {'data-testid': 'tweetText'})
            text = ""
            if text_divs:
                text = text_divs[0].get_text(separator=" ", strip=True)
            else:
                # Try alternative
                text_divs = soup.find_all('div', {'dir': 'auto'})
                if text_divs:
                    text = text_divs[0].get_text(separator=" ", strip=True)

            if not text or len(text) < 5:  # Filter out very short text
                return None

            # Extract URL and tweet ID from href
            links = soup.find_all('a', href=True)
            tweet_id = None
            tweet_url = ""
            author_handle = ""

            for link in links:
                href = link['href']
                if '/status/' in href and 'photo' not in href.lower():
                    parts = href.split('/')
                    if 'status' in parts:
                        idx = parts.index('status')
                        if len(parts) > idx + 1:
                            tweet_id = parts[idx + 1]
                            tweet_url = f"https://x.com{href}"
                            if idx > 0:
                                author_handle = parts[idx - 1]
                            break

            if not tweet_id:
                return None

            # Get time from time element if available
            try:
                time_element = element.find_element(By.CSS_SELECTOR, 'time') if element.find_elements(By.CSS_SELECTOR, 'time') else None
                if time_element:
                    datetime_attr = time_element.get_attribute('datetime')
                    if datetime_attr:
                        try:
                            # Parse ISO datetime string to datetime object
                            created_at = dateutil.parser.isoparse(datetime_attr)
                        except:
                            created_at = datetime.utcnow()
                    else:
                        created_at = datetime.utcnow()
                else:
                    created_at = datetime.utcnow()
            except:
                created_at = datetime.utcnow()

            # Get author name from element
            try:
                author_name = ""
                author_divs = element.find_elements(By.CSS_SELECTOR, 'div[data-testid="User-Names"]')
                if author_divs:
                    spans = author_divs[0].find_elements(By.CSS_SELECTOR, 'span')
                    if spans:
                        author_name = spans[0].text
            except:
                author_name = ""

            # Get location from UI element first (this is often null/empty)
            try:
                location_text = None
                location_spans = element.find_elements(By.CSS_SELECTOR, 'span[data-testid="UserLocation"]')
                if location_spans:
                    location_text = location_spans[0].text
            except:
                location_text = None

            # If location is null or empty, try to detect location from text content and author name
            if not location_text or location_text.strip() == '':
                try:
                    # Import detection functions (only when needed to avoid loading delays)
                    from utils import detect_location_from_text

                    # Use only the basic exact matching function to prevent loading delays
                    # Fuzzy matching can be computationally expensive and cause loading issues
                    location_detection = detect_location_from_text(text, author_name)

                    # Format location as a structured object if found
                    if location_detection.get('province') or location_detection.get('city'):
                        location_text = {
                            "province": location_detection['province'],
                            "city": location_detection['city'],
                            "detected_from": "text_analysis",
                            "original_location": None
                        }
                    else:
                        # No location detected from content
                        location_text = {
                            "province": None,
                            "city": None,
                            "detected_from": "none",
                            "original_location": None
                        }
                except Exception as e:
                    logger.error(f"Error in location detection: {e}")
                    # Fallback to simple null if module load fails
                    location_text = {
                        "province": None,
                        "city": None,
                        "detected_from": "error",
                        "original_location": None
                    }
            else:
                # Location was available from UI, keep it but add structure
                location_text = {
                    "province": None,
                    "city": None,
                    "detected_from": "ui_element",
                    "original_location": location_text
                }

            # Basic metrics
            metrics = {
                'reply_count': 0,      # Number of replies to the tweet
                'retweet_count': 0,    # Number of retweets
                'like_count': 0        # Number of likes
            }

            # Get metrics from button text
            try:
                buttons = element.find_elements(By.CSS_SELECTOR, 'div[role="button"] span, button span')
                for btn in buttons:
                    btn_text = btn.text.lower()
                    if 'reply' in btn_text or 'balas' in btn_text:
                        numbers = re.findall(r'\d+', btn_text)
                        if numbers:
                            metrics['reply_count'] = int(numbers[0])
                    elif 'retweet' in btn_text or 'retwit' in btn_text:
                        numbers = re.findall(r'\d+', btn_text)
                        if numbers:
                            metrics['retweet_count'] = int(numbers[0])
                    elif 'like' in btn_text or 'suka' in btn_text:
                        numbers = re.findall(r'\d+', btn_text)
                        if numbers:
                            metrics['like_count'] = int(numbers[0])
            except:
                # If failed to get metrics, keep default values
                pass

            # Return extracted tweet data
            return {
                '_id': tweet_id,           # Unique tweet ID
                'text': text,              # Text content of the tweet
                'created_at': created_at,  # Timestamp when tweet was created
                'tweet_url': tweet_url,    # URL to the tweet
                'author_handle': author_handle,  # Twitter handle of the author
                'author_name': author_name,      # Display name of the author
                'location': location_text,       # Location of the tweet if available
                'metrics': metrics               # Engagement metrics (replies, retweets, likes)
            }

        except:
            return None

    def detect_something_went_wrong(self):
        """Deteksi apakah muncul pesan 'Something went wrong' dan tombol retry."""
        try:
            # Cari elemen yang mengandung pesan "Something went wrong" atau frasa serupa
            error_messages = self.driver.find_elements(By.CSS_SELECTOR,
                'div[role="status"], div[role="alert"], div[aria-live="polite"], '
                'div[role="dialog"], div[aria-label], span, div, p, h1, h2, h3, h4, h5, h6, article')

            for element in error_messages:
                try:
                    text = element.text.lower()
                    if any(phrase in text for phrase in ['something went wrong', 'something went', 'went wrong',
                                                       'load failed', 'failed to load', 'try again',
                                                       'refresh', 'reload', 'error occurred',
                                                       'gagal memuat', 'muat ulang']):
                        logger.info(f"Menemukan pesan kesalahan: '{element.text[:50]}...'")
                        return True
                except:
                    continue

            # Cari tombol retry secara spesifik
            retry_buttons = self.driver.find_elements(By.CSS_SELECTOR,
                '[data-testid*="retry" i], button[aria-label*="retry" i], '
                'button[aria-label*="Try again" i], button[aria-label*="Refresh" i], '
                'button[aria-label*="Reload" i], button[aria-label*="Muat ulang" i]')

            # Cari juga elemen yang mungkin berisi pesan error
            error_elements = self.driver.find_elements(By.CSS_SELECTOR,
                '[data-testid="app-bar"], [data-testid="cellInnerDiv"] div[role="article"]')

            for element in error_elements:
                try:
                    inner_text = element.text.lower()
                    if 'something went wrong' in inner_text or 'error' in inner_text:
                        logger.info(f"Menemukan elemen error: '{element.text[:50]}...'")
                        return True
                except:
                    continue

            if len(retry_buttons) > 0:
                logger.info(f"Menemukan {len(retry_buttons)} tombol retry atau elemen terkait")
                return True

            return False
        except:
            return False

    def _click_retry_button(self):
        """
        Klik tombol retry jika ditemukan.

        Fungsi ini mencoba berbagai selektor untuk menemukan tombol retry
        dan mengkliknya jika ditemukan, atau menyegarkan halaman jika tidak ditemukan.

        Returns:
            bool: True jika tindakan berhasil (tombol diklik atau halaman direfresh)
        """
        try:
            # Try multiple selectors for retry buttons
            selectors = [
                '[data-testid="Retry"]',
                '[data-testid="retry"]',
                '[data-testid*="Retry" i]',
                '[data-testid*="retry" i]',
                'button[aria-label*="retry" i]',
                'button[aria-label*="Try again" i]',
                'button[aria-label*="Refresh" i]',
                'button[aria-label*="Reload" i]',
                'button[aria-label*="Muat ulang" i]',
                'div[role="button"]',
                'button[type="submit"]',
                'button'
            ]

            # Search for buttons with various selectors
            for selector in selectors:
                try:
                    retry_buttons = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for button in retry_buttons:
                        try:
                            # Check button text
                            button_text = button.text.lower()
                            # Check aria-label or other attributes
                            aria_label = button.get_attribute('aria-label')
                            data_testid = button.get_attribute('data-testid')

                            # Check if button matches retry criteria
                            text_match = any(phrase in button_text for phrase in ['retry', 'try again', 'refresh', 'reconnect', 'muat ulang', 'reload', 'coba'])
                            aria_match = aria_label and any(phrase in aria_label.lower() for phrase in ['retry', 'try again', 'refresh', 'reload'])
                            testid_match = data_testid and any(phrase in data_testid.lower() for phrase in ['retry', 'refresh', 'reload'])

                            # Click button if it matches criteria and is visible/enabled
                            if (text_match or aria_match or testid_match) and button.is_displayed() and button.is_enabled():
                                logger.info(f"Menemukan dan mengklik tombol retry: '{button.text or aria_label or data_testid}'")
                                button.click()
                                time.sleep(3)  # Wait briefly after clicking
                                return True
                        except:
                            continue  # Move to next button
                except:
                    continue  # Move to next selector

            # If no retry buttons found, DON'T refresh/scroll to beginning - just return False
            # This allows the system to continue from the current scroll position
            logger.info("Tidak menemukan tombol retry yang sesuai, tidak melakukan refresh")
            return False  # Return False to indicate no retry action was taken

        except Exception as e:
            logger.error(f"Error saat mengklik tombol retry: {e}")
            # If can't click button, try to refresh the page
            try:
                self.driver.refresh()
                time.sleep(5)
                return True
            except:
                return False

    def handle_retry_mechanism(self, max_retries=10):
        """
        Handle retry mechanism when 'Something went wrong' is detected.

        Fungsi ini mencoba beberapa kali untuk menangani error 'Something went wrong'
        di halaman X/Twitter dengan mencoba klik tombol retry atau refresh halaman.

        Args:
            max_retries (int): Jumlah maksimal percobaan retry

        Returns:
            bool: True jika retry berhasil dan ada tweet baru, False jika tidak
        """
        logger.info(f"Memulai mekanisme retry, maksimal {max_retries} percobaan...")
        # Get initial count of tweets before retry
        initial_tweet_count = len(self.driver.find_elements(By.CSS_SELECTOR, 'article[data-testid="tweet"]'))

        retry_attempts = 0
        # Loop until max_retries is reached
        while retry_attempts < max_retries:
            retry_attempts += 1
            logger.info(f"Percobaan retry {retry_attempts}/{max_retries}")

            if self.detect_something_went_wrong():
                logger.info(f"Coba klik tombol retry (percobaan {retry_attempts})...")

                if self._click_retry_button():
                    # Wait briefly for content to reload
                    time.sleep(3)

                    # Wait until content may be loaded
                    try:
                        # Wait until there are more tweets than initially
                        WebDriverWait(self.driver, 10).until(
                            lambda driver: len(driver.find_elements(By.CSS_SELECTOR, 'article[data-testid="tweet"]')) > initial_tweet_count
                        )
                        final_tweet_count = len(self.driver.find_elements(By.CSS_SELECTOR, 'article[data-testid="tweet"]'))
                        logger.info(f"Berhasil! Jumlah tweet sekarang: {final_tweet_count}, sebelumnya: {initial_tweet_count}")
                        if final_tweet_count > initial_tweet_count:
                            return True
                    except:
                        # If no change after waiting, continue to next attempt
                        final_tweet_count = len(self.driver.find_elements(By.CSS_SELECTOR, 'article[data-testid="tweet"]'))
                        if final_tweet_count > initial_tweet_count:
                            logger.info(f"Berhasil! Jumlah tweet sekarang: {final_tweet_count}, sebelumnya: {initial_tweet_count}")
                            return True
                        else:
                            logger.info(f"Tidak ada perubahan tweet setelah retry {retry_attempts}")

                        # Wait longer between attempts
                        time.sleep(random.uniform(5, 10))
                else:
                    # No retry button found - DON'T refresh page, exit retry mechanism and continue scraping
                    logger.info(f"Tidak ada tombol retry ditemukan, kembali ke proses scraping normal")
                    logger.info("Lanjutkan scraping dari posisi sekarang tanpa refresh halaman")
                    break  # Exit the retry loop and continue with main scraping process
            else:
                logger.info("Tidak ada pesan 'Something went wrong' terdeteksi")
                # Check if there are new tweets after a few seconds
                final_tweet_count = len(self.driver.find_elements(By.CSS_SELECTOR, 'article[data-testid="tweet"]'))
                if final_tweet_count > initial_tweet_count:
                    logger.info(f"Berhasil! Jumlah tweet sekarang: {final_tweet_count}, sebelumnya: {initial_tweet_count}")
                    return True
                break  # Exit if no error detected and no new tweets

        logger.warning(f"Mencapai batas maksimal retry ({max_retries}), tidak ada perubahan konten")
        return False

    def clean_text(self, text):
        """
        Membersihkan teks sederhana.

        Fungsi ini membersihkan teks tweet dari URL, mention, dan hashtag
        untuk keperluan analisis lebih lanjut.

        Args:
            text (str): Teks tweet yang akan dibersihkan

        Returns:
            str: Teks yang telah dibersihkan
        """
        # Remove URLs and replace with placeholder
        text = re.sub(r'http\S+|www\S+|https\S+', '[LINK]', text, flags=re.MULTILINE)
        # Remove mentions and replace with placeholder
        text = re.sub(r'@\w+', '[MENTION]', text)
        # Remove hashtag symbols but keep the text
        text = re.sub(r'#(\w+)', r'\1', text)
        # Normalize whitespace
        text = ' '.join(text.split())
        return text.strip()

    def process_and_save_tweets(self, tweets, collection):
        """
        Proses dan simpan tweet ke collection secara efisien.

        Fungsi ini mentransformasi data tweet ke format yang sesuai
        dan menyimpannya ke MongoDB collection dengan pendekatan bulk.

        Args:
            tweets (list): Daftar tweet yang akan diproses
            collection: MongoDB collection untuk menyimpan data

        Returns:
            int: Jumlah tweet yang berhasil disimpan
        """
        if not tweets:
            return 0

        # Transform tweets to the required format
        transformed_tweets = []
        for tweet_data in tweets:
            try:
                # Clean the text content
                clean_content = self.clean_text(tweet_data['text'])

                # Format tweet data in a structured way for storage
                transformed_tweet = {
                    "_id": tweet_data.get('_id', ''),  # Unique tweet ID
                    "content": {
                        "text": tweet_data['text'],      # Original tweet text
                        "clean_text": clean_content.lower()  # Cleaned and lowercased text
                    },
                    "metadata": {
                        "author_name": tweet_data.get('author_name', ''),  # Display name of author
                        "author_handle": tweet_data.get('author_handle', ''),  # Twitter handle
                        "created_at": tweet_data['created_at'],  # When tweet was created
                        "scraped_at": datetime.utcnow(),         # When it was scraped
                        "location": tweet_data.get('location', None),  # Location if available
                        "tweet_url": tweet_data.get('tweet_url', '')   # URL to the tweet
                    },
                    "metrics": tweet_data['metrics'],  # Engagement metrics
                    "processing_status": {
                        "sentiment_analyzed": False    # Flag for sentiment analysis status
                    }
                }

                transformed_tweets.append(transformed_tweet)

            except:
                continue  # Skip malformed tweets

        # Save transformed tweets to MongoDB
        if transformed_tweets:
            try:
                # Prepare bulk operations
                bulk_ops = [
                    UpdateOne(
                        {"_id": tweet["_id"]},  # Match by tweet ID
                        {"$set": tweet},        # Update with new data
                        upsert=True             # Create if doesn't exist
                    ) for tweet in transformed_tweets
                ]

                # Execute bulk write operation
                result = collection.bulk_write(bulk_ops)
                logger.info(f"Berhasil menyimpan {len(bulk_ops)} tweet ke collection")
                return len(bulk_ops)
            except PyMongoError as e:
                logger.error(f"Error menyimpan ke MongoDB: {e}")
                # Save one by one if bulk fails
                success_count = 0
                for tweet in transformed_tweets:
                    try:
                        # Attempt to save each tweet individually
                        collection.update_one(
                            {"_id": tweet["_id"]},
                            {"$set": tweet},
                            upsert=True
                        )
                        success_count += 1
                    except:
                        continue  # Skip failed tweets
                return success_count

        return 0


    def detect_rate_limiting(self):
        """
        Deteksi apakah kita kena rate limiting berdasarkan kondisi halaman.

        Fungsi ini memeriksa berbagai indikator bahwa X/Twitter
        mungkin telah membatasi permintaan kita karena terlalu cepat.

        Returns:
            bool: True jika mendeteksi rate limiting, False jika tidak
        """
        try:
            # Check if URL changes to page indicating problems
            current_url = self.driver.current_url
            if any(pattern in current_url.lower() for pattern in ['unusual', 'rate', 'limit', 'access', 'safety', 'verify', 'challenge']):
                return True

            # Check if special pages appear indicating rate limiting or verification
            limit_elements = self.driver.find_elements(By.CSS_SELECTOR,
                'div[aria-label*="Suspicious" i], div[aria-label*="Verify" i], '
                'div[aria-label*="Access" i], div[aria-label*="Rate" i], '
                'div[aria-label*="limit" i]')

            # Detect specific messages indicating rate limiting
            warning_texts = self.driver.find_elements(By.CSS_SELECTOR,
                'span, div, p, h1, h2, h3, h4, h5, h6')

            for element in warning_texts:
                text = element.text.lower()
                if any(phrase in text for phrase in ['rate limit', 'too many requests', 'try again later',
                                                   'unusual activity', 'verify it\'s really you',
                                                   'please try again', 'access denied', 'blocked']):
                    return True

            # Only check error elements if needed, as this can slow things down
            error_elements = self.driver.find_elements(By.CSS_SELECTOR,
                'div[role="alert"], div[aria-label="Error"], .error, [data-testid="error"]')

            return len(limit_elements) > 0 or len(error_elements) > 0
        except:
            return False

    def exponential_backoff(self, attempt, max_backoff=45):
        """
        Implementasi exponential backoff untuk menghindari rate limiting.

        Fungsi ini membuat jeda yang meningkat secara eksponensial
        antar percobaan untuk menghindari pembatasan dari X/Twitter.

        Args:
            attempt (int): Nomor percobaan saat ini
            max_backoff (int): Waktu maksimum jeda dalam detik
        """
        import math
        # Calculate backoff time with exponential function
        # Use base backoff time from config, default to 8 seconds
        base_backoff = self.config['scraper'].get('base_backoff', 8)
        # Calculate backoff time: base * (1.5 ^ attempt) + random jitter
        # Use 1.5 as multiplier to reduce backoff time compared to original 2.0
        backoff_time = min(base_backoff * (1.5 ** attempt) + random.uniform(0, 1), max_backoff)
        logger.info(f"Melakukan backoff selama {backoff_time:.2f} detik (attempt {attempt})")
        time.sleep(backoff_time)

    def scrape_day_maximum(self, target_date):
        """Scrape maksimum tweet untuk satu hari menggunakan multi-query."""
        print(f"\n{'='*60}")
        print(f"MEMULAI SCRAPING TWEET UNTUK TANGGAL: {target_date.strftime('%Y-%m-%d')}")
        print(f"{'='*60}")

        logger.info(f"Memulai scraping maksimum untuk: {target_date.strftime('%Y-%m-%d')}")

        # Dapatkan collection - gunakan collection tanggal awal bulan untuk daily processing dengan monthly storage
        # atau collection harian biasa untuk processing biasa
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

        # Load config to check if daily processing is enabled
        try:
            with open("config/config.json", 'r') as f:
                config = json.load(f)
            daily_processing_enabled = config['twitter'].get('daily_processing', False)
        except:
            daily_processing_enabled = False

        if daily_processing_enabled:
            # For daily processing with monthly storage, use the month collection (first day of month)
            month_collection_date = target_date.replace(day=1)
            collection, collection_name = self.collection_manager.get_collection_by_date(month_collection_date)
            logger.info(f"Menggunakan collection bulanan: {collection_name} untuk menyimpan data harian {target_date.strftime('%Y-%m-%d')}")
        else:
            # Normal processing - use daily collection
            collection, collection_name = self.collection_manager.get_collection_by_date(target_date)
        existing_count = collection.count_documents({})
        print(f"Tweet yang sudah ada di database: {existing_count}")

        total_scraped = 0
        max_tweets = self.max_tweets  # Ambil nilai dari instance

        # Dapatkan semua query alternatif
        alternative_queries = self.build_alternative_queries(target_date)
        total_queries = len(alternative_queries)

        logger.info(f"Menggunakan {total_queries} query alternatif untuk memaksimalkan hasil")
        print(f"Jumlah query yang akan digunakan: {total_queries}")
        print(f"Target maksimal tweet: {max_tweets}")
        print(f"-" * 60)

        # Membuat progress bar untuk query
        query_pbar = tqdm(total=total_queries, desc="Query Progress", position=0, leave=False)
        overall_pbar = tqdm(total=max_tweets, desc="Total Tweet Progress", position=1, leave=True,
                           bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]')

        for i, query in enumerate(alternative_queries):
            query_name = f"Query-{i+1}"
            logger.info(f"Menjalankan {query_name}: {query[:100]}...")
            print(f"\n{query_name}: {query[:80]}...")

            # Inisialisasi variabel query_scraped di awal loop agar selalu terdefinisi
            query_scraped = 0

            # Coba beberapa kali jika terkena rate limit
            max_retries = self.config['scraper'].get('max_retries', 3)
            retry_count = 0

            while retry_count <= max_retries:
                try:
                    # Navigasi ke pencarian dengan query saat ini
                    encoded_query = query.replace(' ', '%20').replace(':', '%3A').replace(',', '%2C')
                    search_url = f"https://x.com/search?q={encoded_query}&src=typed_query&f=live"

                    logger.debug(f"URL Query {i+1}: {search_url}")

                    # Coba navigate dan tangani error
                    try:
                        # Set page load timeout to prevent hanging
                        self.driver.set_page_load_timeout(30)  # 30 second timeout for page loading

                        self.driver.get(search_url)

                        # Tunggu beberapa detik sebelum mengecek elemen untuk memberi waktu loading
                        time.sleep(3)  # Reduce from 5 back to 3 but with better detection

                        # Deteksi error segera setelah navigasi selesai
                        if self.detect_something_went_wrong():
                            logger.warning(f"Menemukan pesan error segera setelah navigasi untuk query {i+1}, mencoba retry mekanisme...")
                            print(f"  [WARNING] Menemukan pesan error segera setelah navigasi, mencoba retry...")

                            # Coba retry mekanisme segera setelah deteksi error
                            retry_success = self.handle_retry_mechanism(max_retries=self.config['scraper'].get('max_retry_attempts', 10))
                            if retry_success:
                                logger.info("Retry berhasil setelah deteksi awal error")
                                print(f"  [SUCCESS] Retry berhasil setelah deteksi awal error...")
                            else:
                                logger.warning("Retry gagal setelah deteksi awal error")
                                print(f"  [ERROR] Retry gagal setelah deteksi awal error...")

                        # Tunggu elemen tweet muncul dengan pendekatan lebih agresif dan timeout
                        try:
                            # Gunakan pendekatan dengan polling interval yang lebih agresif
                            WebDriverWait(self.driver, 20, poll_frequency=1).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, 'article[data-testid="tweet"]'))
                            )
                        except:
                            # Coba selector alternatif yang mungkin muncul di halaman kosong atau error
                            try:
                                # Tunggu dengan pendekatan polling yang lebih cepat
                                element_found = False
                                for _ in range(20):  # Coba hingga 20 kali dengan jeda 1 detik
                                    time.sleep(1)
                                    tweet_elements = self.driver.find_elements(By.CSS_SELECTOR, 'article[data-testid="tweet"]')
                                    alternative_elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-testid="cellInnerDiv"] article')

                                    if len(tweet_elements) > 0 or len(alternative_elements) > 0:
                                        element_found = True
                                        break

                                    # Cek apakah ada pesan error
                                    if self.detect_something_went_wrong():
                                        break

                                if not element_found:
                                    # Cek apakah ada pesan error sekarang
                                    if self.detect_something_went_wrong():
                                        logger.warning(f"Menemukan pesan error setelah pengecekan lanjutan untuk query {i+1}")
                                        print(f"  [ERROR] Menemukan pesan error setelah pengecekan lanjutan")

                                        # Coba retry mekanisme
                                        retry_success = self.handle_retry_mechanism(max_retries=self.config['scraper'].get('max_retry_attempts', 10))
                                        if retry_success:
                                            logger.info("Retry berhasil setelah deteksi error lanjutan")
                                            print(f"  [SUCCESS] Retry berhasil setelah deteksi error lanjutan...")

                                            # Tunggu lebih lama setelah retry
                                            time.sleep(8)

                                            # Pastikan tweet muncul setelah retry dengan polling lebih agresif
                                            for _ in range(15):  # Coba hingga 15 kali
                                                time.sleep(1)
                                                tweet_elements = self.driver.find_elements(By.CSS_SELECTOR, 'article[data-testid="tweet"]')
                                                if len(tweet_elements) > 0:
                                                    break

                                            if len(tweet_elements) == 0:
                                                logger.warning("Tetap tidak ada tweet setelah retry")
                                                print(f"  [ERROR] Tetap tidak ada tweet setelah retry")
                                                break
                                        else:
                                            logger.warning("Retry gagal setelah deteksi error lanjutan")
                                            print(f"  [ERROR] Retry gagal setelah deteksi error lanjutan...")
                                            break

                            except Exception as e:
                                logger.warning(f"Error saat menunggu elemen: {str(e)}")
                                logger.warning(f"Query {i+1} tidak menghasilkan tweet setelah pengecekan tambahan, lanjut ke query berikutnya")
                                print(f"  [ERROR] Tidak ada tweet ditemukan di query ini setelah pengecekan tambahan, lanjut ke query berikutnya")
                                break  # Keluar dari retry loop dan lanjut ke query berikutnya
                    except Exception as nav_error:
                        logger.error(f"Error navigasi untuk query {i+1}: {nav_error}")

                        # Jika error terkait dengan koneksi terputus ke driver
                        if "connection" in str(nav_error).lower() or "session" in str(nav_error).lower() or "no connection could be made" in str(nav_error).lower():
                            logger.error("Koneksi ke browser terputus. Mencoba restart browser...")
                            try:
                                self.driver.quit()  # Tutup driver lama
                                # Tunggu sebentar sebelum membuat yang baru
                                time.sleep(5)
                                # Dalam konteks ini kita tidak bisa membuat driver baru karena tidak ada akses ke setup_driver
                                # Jadi kita hanya bisa keluar dari fungsi ini dan membiarkan error menyebar
                                raise Exception("Koneksi browser terputus, memerlukan restart") from nav_error
                            except:
                                raise Exception("Koneksi browser terputus, memerlukan restart") from nav_error

                        # Cek rate limiting hanya jika error terkait dengan itu
                        elif "rate" in str(nav_error).lower() or "limit" in str(nav_error).lower() or self.detect_rate_limiting():
                            logger.warning(f"Terkena rate limiting saat navigasi query {i+1}, retry {retry_count+1}/{max_retries}")
                            retry_count += 1
                            if retry_count <= max_retries:
                                self.exponential_backoff(retry_count)
                                continue  # Coba lagi dengan query yang sama
                            else:
                                logger.warning(f"Mencapai maksimum retry untuk query {i+1}, lanjut ke query berikutnya")
                                break  # Jika sudah max retry, lanjut ke query berikutnya
                        else:
                            retry_count += 1
                            if retry_count <= max_retries:
                                self.exponential_backoff(retry_count)
                                continue  # Coba lagi dengan query yang sama
                            else:
                                logger.warning(f"Mencapai maksimum retry untuk query {i+1}, lanjut ke query berikutnya")
                                break  # Jika sudah max retry, lanjut ke query berikutnya

                    # Reset counter untuk query ini
                    consecutive_no_new = 0
                    max_consecutive_no_new = 50  # Increased for better tolerance of no new content
                    scroll_count = 0
                    query_scraped = 0  # Jumlah tweet dari query saat ini
                    previous_total = 0

                    # Counter untuk melacak berapa kali tidak ada data baru
                    no_new_data_counter = 0
                    max_no_new_data = 20  # Increased to allow more iterations without new data before switching query

                    # Counter untuk memantau kapan perlu melakukan data processing
                    last_processing_scroll = 0
                    tweets_since_last_processing = 0
                    max_tweets_between_processing = self.config['scraper'].get('max_tweets_between_processing', 100)

                    # Progress bar untuk query saat ini
                    query_tweet_pbar = tqdm(total=min(10000, max_tweets), desc=f"Query-{i+1} Progress", position=2, leave=False,
                                            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}, {rate_fmt}]')

                    # Lakukan scraping untuk query saat ini - using infinite scroll with configurable limits
                    rate_limit_check_counter = 0  # Counter untuk membatasi pengecekan rate limiting
                    scroll_pbar = tqdm(total=100000, desc=f"Scroll Progress", position=3, leave=False)  # Very high number (effectively infinite)

                    while total_scraped < max_tweets and consecutive_no_new < max_consecutive_no_new:
                        try:
                            # Cek rate limiting hanya setiap 5 scroll untuk mengurangi overhead
                            rate_limit_check_counter += 1
                            if rate_limit_check_counter >= 5:
                                if self.detect_rate_limiting():
                                    logger.warning(f"Terkena rate limiting saat scraping Query-{i+1}, retry {retry_count+1}/{max_retries}")
                                    print(f"  ⚠️  Terkena rate limiting saat scraping, retry {retry_count+1}/{max_retries}...")
                                    retry_count += 1
                                    self.exponential_backoff(retry_count)
                                    break  # Keluar dari loop scraping
                                rate_limit_check_counter = 0  # Reset counter

                            # Ambil tweet dari halaman saat ini
                            tweets = self.extract_tweets_advanced()

                            if tweets:
                                # Simpan tweet
                                saved_count = self.process_and_save_tweets(tweets, collection)
                                if saved_count > 0:
                                    total_scraped += saved_count
                                    query_scraped += saved_count
                                    tweets_since_last_processing += saved_count

                                    # Update progress bar
                                    overall_pbar.update(saved_count)
                                    query_tweet_pbar.update(saved_count)

                                    if saved_count > 0:
                                        logger.info(f"Query {i+1} - Total terkumpul: {total_scraped} tweet ({saved_count} baru)")
                                        print(f"    [INBOX] Berhasil mengumpulkan {saved_count} tweet baru (total: {total_scraped})")
                                        consecutive_no_new = 0  # Reset jika ada data baru
                                        no_new_data_counter = 0  # Reset counter tidak ada data baru
                                else:
                                    consecutive_no_new += 1
                                    no_new_data_counter += 1  # Tambah counter tidak ada data baru
                            else:
                                consecutive_no_new += 1  # Tidak ada tweet ditemukan
                                no_new_data_counter += 1  # Tambah counter tidak ada data baru

                            # Jika tidak ada data baru selama max_no_new_data kali, lanjut ke query berikutnya
                            if no_new_data_counter >= max_no_new_data:
                                logger.info(f"Tidak ada data baru selama {max_no_new_data} iterasi berturut-turut, lanjut ke query berikutnya")
                                print(f"  [REFRESH] Tidak ada data baru selama {max_no_new_data} iterasi, lanjut ke query berikutnya...")
                                break

                            # Scroll ke bawah untuk memuat lebih banyak data
                            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

                            # Tunggu sebentar agar konten dimuat - TAPI HANYA JIKA ADA DATA BARU
                            # Jika tidak ada data baru, kurangi jeda untuk kecepatan lebih tinggi
                            if query_scraped > previous_total:
                                # Ada data baru, gunakan jeda normal untuk memastikan konten termuat
                                base_pause = random.uniform(self.scroll_pause_min/2, self.scroll_pause_max/2)
                            else:
                                # Tidak ada data baru, gunakan jeda minimal untuk kecepatan
                                base_pause = random.uniform(0.1, 0.3)  # Jeda sangat cepat saat tidak ada data baru

                            time.sleep(base_pause)

                            scroll_count += 1
                            scroll_pbar.update(1)

                            # Tampilkan info secara berkala
                            if scroll_count % 20 == 0:
                                print(f"    [CHART] Status: {scroll_count} scroll, {query_scraped} tweet dari Query-{i+1}")

                            # Tambahkan deteksi halaman tidak berubah untuk menghindari loop tak terbatas
                            if query_scraped == previous_total and scroll_count > 5:
                                consecutive_no_new += 1

                                # Cek apakah muncul pesan "Something went wrong" ketika tidak ada perubahan konten
                                if self.detect_something_went_wrong():
                                    logger.warning(f"Menemukan pesan 'Something went wrong' saat tidak ada perubahan konten (scroll {scroll_count})")
                                    print(f"    [WARNING] Menemukan pesan 'Something went wrong', mencoba retry...")

                                    # Lakukan retry mekanisme
                                    retry_success = self.handle_retry_mechanism(max_retries=self.config['scraper'].get('max_retry_attempts', 10))
                                    if retry_success:
                                        logger.info("Retry berhasil, melanjutkan scraping...")
                                        print(f"    [SUCCESS] Retry berhasil, melanjutkan scraping...")
                                        consecutive_no_new = 0  # Reset karena ada perubahan konten
                                        previous_total = len(self.driver.find_elements(By.CSS_SELECTOR, 'article[data-testid="tweet"]'))
                                    else:
                                        logger.warning("Retry gagal setelah beberapa percobaan")
                                        print(f"    [ERROR] Retry gagal, melanjutkan...")
                            previous_total = query_scraped

                            # Refresh halaman setiap 100 scroll untuk mencegah halaman menjadi stale
                            if scroll_count > 0 and scroll_count % 100 == 0:
                                logger.info(f"Melakukan refresh halaman setelah {scroll_count} scroll untuk mencegah halaman menjadi stale...")
                                print(f"    [REFRESH] Refresh halaman setelah {scroll_count} scroll...")

                                # Simpan posisi scroll sebelum refresh
                                scroll_position = scroll_count

                                # Refresh halaman
                                self.driver.refresh()

                                # Tunggu sebentar agar halaman dimuat kembali
                                time.sleep(5)

                                # Coba kembali ke posisi sebelumnya atau lanjutkan scraping
                                try:
                                    # Tunggu elemen tweet muncul kembali
                                    WebDriverWait(self.driver, 10).until(
                                        EC.presence_of_element_located((By.CSS_SELECTOR, 'article[data-testid="tweet"]'))
                                    )
                                    logger.info(f"Halaman berhasil di-refresh, melanjutkan scraping dari posisi {scroll_position}")
                                    print(f"    [SUCCESS] Halaman berhasil di-refresh, melanjutkan scraping...")
                                except:
                                    logger.warning("Gagal memuat ulang elemen tweet setelah refresh")
                                    print(f"    [WARNING] Memuat ulang elemen tweet setelah refresh...")

                            # Hentikan scraping jika sudah tidak ada tweet baru dalam jumlah scroll tertentu
                            # Ini untuk mendeteksi jika hari saat ini sudah tidak menghasilkan tweet lagi
                            # Jika mode daily processing diaktifkan, beralih ke hari berikutnya setelah 3 scroll tanpa data baru
                            import sys
                            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

                            try:
                                with open("config/config.json", 'r') as f:
                                    config = json.load(f)
                                daily_processing_enabled = config['twitter'].get('daily_processing', False)
                            except:
                                daily_processing_enabled = False

                            if daily_processing_enabled and consecutive_no_new > 3:  # Jika dalam mode daily processing dan tidak ada data baru dalam 3 scroll
                                logger.info(f"Tidak ada tweet baru dalam {consecutive_no_new} iterasi scroll, beralih ke hari berikutnya...")
                                print(f"  [SWITCH] Tidak ada tweet baru, beralih ke hari berikutnya...")
                                break  # Keluar dari loop scraping untuk hari ini dan lanjutkan ke hari berikutnya
                            # For monthly processing, we can allow more consecutive no-new scrolls before breaking
                            elif not daily_processing_enabled and consecutive_no_new > 50:  # For monthly processing, allow up to 50 consecutive no-new scrolls
                                logger.info(f"Tidak ada tweet baru dalam {consecutive_no_new} iterasi scroll...")
                                print(f"  [INFO] Tidak ada tweet baru dalam {consecutive_no_new} scroll...")
                                # Continue to next query but don't necessarily break the entire process

                            # Jika sudah mencapai maksimum, berhenti
                            if total_scraped >= max_tweets:
                                print(f"    [TARGET] Target maksimum {max_tweets} tweet telah tercapai!")
                                break

                            # Tambahkan jeda tambahan setiap 20 scroll untuk menghindari kelelahan sistem
                            if scroll_count % 20 == 0:
                                # Kurangi jeda jika tidak ada data baru ditemukan
                                if consecutive_no_new > 10:  # Jika lebih dari 10 scroll tanpa data baru
                                    jeda = random.randint(3, 8)  # Jeda lebih pendek
                                else:
                                    jeda = random.randint(5, 10)  # Jeda normal
                                logger.info(f"Jeda {jeda} detik setiap 20 scroll (setelah {scroll_count} scroll)")
                                time.sleep(jeda)

                        except Exception as e:
                            logger.error(f"Error dalam loop scraping: {e}")

                            # Cek apakah error terkait dengan koneksi terputus ke driver
                            if "connection" in str(e).lower() or "session" in str(e).lower() or "no connection could be made" in str(e).lower():
                                logger.error("Koneksi ke browser terputus dalam loop scraping. Menghentikan proses...")
                                raise Exception("Koneksi browser terputus, memerlukan restart") from e

                            # Cek apakah error karena rate limiting
                            if self.detect_rate_limiting():
                                logger.warning(f"Terkena rate limiting atau halaman verifikasi dalam loop scraping, retry {retry_count+1}/{max_retries}")
                                print(f"  [WARNING] Terkena rate limiting, retry {retry_count+1}/{max_retries}...")
                                retry_count += 1
                                if retry_count <= max_retries:
                                    self.exponential_backoff(retry_count)
                                    # Coba refresh halaman setelah backoff
                                    try:
                                        self.driver.refresh()
                                        time.sleep(5)  # Kurangi dari 10 menjadi 5
                                    except:
                                        logger.warning("Gagal refresh halaman setelah rate limiting detected")
                                    # Lanjut ke retry berikutnya
                                    break  # Keluar dari loop scraping dan lanjut ke retry
                                else:
                                    logger.warning(f"Mencapai maksimum retry, lanjut ke query berikutnya")
                                    break  # Jika sudah max retry, lanjut ke query berikutnya
                            else:
                                consecutive_no_new += 1  # Anggap sebagai kegagalan
                                no_new_data_counter += 1  # Tambah counter tidak ada data baru

                                # Jika tidak ada data baru selama max_no_new_data kali, lanjut ke query berikutnya
                                if no_new_data_counter >= max_no_new_data:
                                    logger.info(f"Tidak ada data baru selama {max_no_new_data} iterasi berturut-turut, lanjut ke query berikutnya")
                                    print(f"  [REFRESH] Tidak ada data baru selama {max_no_new_data} iterasi, lanjut ke query berikutnya...")
                                    break

                                # Coba refresh halaman jika terjadi error
                                try:
                                    self.driver.refresh()
                                    time.sleep(3)  # Kurangi dari 10 menjadi 3
                                    WebDriverWait(self.driver, 8).until(  # Kurangi dari 15 menjadi 8
                                        EC.presence_of_element_located((By.CSS_SELECTOR, 'article[data-testid="tweet"]'))
                                    )
                                except:
                                    logger.warning("Tidak bisa refresh halaman, lanjut ke query berikutnya")
                                    break

                    # Hentikan progress bar scroll dan query
                    scroll_pbar.close()
                    query_tweet_pbar.close()

                    # Info tambahan setelah menyelesaikan query
                    if query_scraped > 0:
                        print(f"  [CHART_UP] Query-{i+1} berhasil mengumpulkan {query_scraped} tweet")
                    else:
                        print(f"  [CHART_DOWN] Query-{i+1} tidak menghasilkan tweet baru")

                    # Jika berhasil tanpa rate limiting, keluar dari retry loop
                    break
                except Exception as e:
                    logger.error(f"Error dalam query {i+1}: {e}")

                    # Cek apakah error terkait dengan koneksi terputus ke driver
                    if "connection" in str(e).lower() or "session" in str(e).lower() or "no connection could be made" in str(e).lower():
                        logger.error("Koneksi ke browser terputus saat eksekusi query. Menghentikan proses...")
                        raise Exception("Koneksi browser terputus, memerlukan restart") from e

                    # Cek apakah error karena rate limiting dengan fungsi deteksi khusus
                    if "rate" in str(e).lower() or "limit" in str(e).lower() or self.detect_rate_limiting():
                        logger.warning(f"Terkena rate limiting atau halaman verifikasi pada query {i+1}, retry {retry_count+1}/{max_retries}")
                        retry_count += 1
                        if retry_count <= max_retries:
                            self.exponential_backoff(retry_count)
                            try:
                                self.driver.refresh()
                                time.sleep(5)  # Kurangi dari 10 menjadi 5
                            except:
                                logger.warning("Gagal refresh halaman setelah rate limiting detected")
                        else:
                            logger.warning(f"Mencapai maksimum retry untuk query {i+1}, lanjut ke query berikutnya")
                            break  # Jika sudah max retry, lanjut ke query berikutnya
                    else:
                        break  # Jika error bukan karena rate limiting, lanjut ke query berikutnya

            # Update query progress
            query_pbar.update(1)

            logger.info(f"Query {i+1} selesai, dapatkan {query_scraped} tweet")
            print(f"  [SUCCESS] Query-{i+1} selesai, dapatkan {query_scraped} tweet")

            # Jika sudah mencapai maksimum, berhenti
            if total_scraped >= self.max_tweets:
                logger.info(f"Mencapai batas maksimum {self.max_tweets} tweet")
                print(f"\n[TARGET] Target maksimum {self.max_tweets} tweet telah tercapai!")
                break

            # Tambahkan jeda acak antar query untuk menghindari pembatasan
            jeda = random.randint(10, 20)  # Kurangi dari 20-40 menjadi 10-20
            logger.info(f"Jeda {jeda} detik antar query")
            print(f"  [WAIT] Jeda {jeda} detik sebelum query berikutnya...")
            time.sleep(jeda)

        # Tutup semua progress bar
        query_pbar.close()
        overall_pbar.close()

        print(f"\n{'='*60}")
        print(f"SELESAI SCRAPING UNTUK {target_date.strftime('%Y-%m-%d')}")
        print(f"Total tweet terkumpul: {total_scraped}")
        print(f"{'='*60}")

        logger.info(f"Selesai scraping untuk {target_date.strftime('%Y-%m-%d')}, total: {total_scraped} tweet")
        return total_scraped

    def scrape_month_maximum(self, start_date, end_date):
        """Scrape maksimum tweet untuk satu bulan menggunakan multi-query."""
        print(f"\n{'='*60}")
        print(f"MEMULAI SCRAPING TWEET UNTUK BULAN: {start_date.strftime('%Y-%m')} (dari {start_date.strftime('%Y-%m-%d')} hingga {end_date.strftime('%Y-%m-%d')})")
        print(f"{'='*60}")

        logger.info(f"Memulai scraping maksimum untuk bulan: {start_date.strftime('%Y-%m')}")

        # Dapatkan collection untuk bulan ini (akan menggunakan awal bulan untuk nama koleksi)
        # Load config to check if daily processing is enabled
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

        try:
            with open("config/config.json", 'r') as f:
                config = json.load(f)
            daily_processing_enabled = config['twitter'].get('daily_processing', False)
        except:
            daily_processing_enabled = False

        collection, collection_name = self.collection_manager.get_collection_by_date(start_date)
        existing_count = collection.count_documents({})
        print(f"Tweet yang sudah ada di database: {existing_count}")

        total_scraped = 0
        max_tweets = self.max_tweets  # Ambil nilai dari instance

        # Gunakan query bulanan daripada loop harian untuk efisiensi
        logger.info(f"Memproses rentang tanggal: {start_date.strftime('%Y-%m-%d')} hingga {end_date.strftime('%Y-%m-%d')}")

        # Dapatkan collection untuk bulan ini (akan menggunakan awal bulan untuk nama koleksi)
        collection, collection_name = self.collection_manager.get_collection_by_date(start_date)

        # Dapatkan query bulanan untuk seluruh rentang bulan
        monthly_queries = self.build_monthly_queries(start_date, end_date)
        total_queries = len(monthly_queries)

        logger.info(f"Menggunakan {total_queries} query bulanan untuk rentang: {start_date.strftime('%Y-%m-%d')} hingga {end_date.strftime('%Y-%m-%d')}")
        print(f"Jumlah query bulanan yang akan digunakan: {total_queries}")
        print(f"Rentang waktu: {start_date.strftime('%Y-%m-%d')} hingga {end_date.strftime('%Y-%m-%d')}")

        # Progress bar untuk total query bulanan
        query_pbar = tqdm(total=total_queries, desc="Monthly Query Progress", position=0, leave=True)

        for i, query in enumerate(monthly_queries):
            query_name = f"Monthly Query-{i+1}"
            logger.info(f"Menjalankan {query_name}: {query[:100]}...")
            print(f"\n{query_name}: {query[:80]}...")

            # Inisialisasi variabel query_scraped di awal loop agar selalu terdefinisi
            query_scraped = 0

            # Coba beberapa kali jika terkena rate limit
            max_retries = self.config['scraper'].get('max_retries', 3)
            retry_count = 0

            while retry_count <= max_retries:
                try:
                    # Navigasi ke pencarian dengan query saat ini
                    encoded_query = query.replace(' ', '%20').replace(':', '%3A').replace(',', '%2C')
                    search_url = f"https://x.com/search?q={encoded_query}&src=typed_query&f=live"

                    logger.debug(f"URL Query {i+1} untuk rentang bulan: {search_url}")

                    # Coba navigate dan tangani error
                    try:
                        # Set page load timeout to prevent hanging
                        self.driver.set_page_load_timeout(30)  # 30 second timeout for page loading

                        self.driver.get(search_url)

                        # Tunggu beberapa detik sebelum mengecek elemen untuk memberi waktu loading
                        time.sleep(3)  # Reduce from 5 back to 3 but with better detection

                        # Deteksi error segera setelah navigasi selesai
                        if self.detect_something_went_wrong():
                            logger.warning(f"Menemukan pesan error segera setelah navigasi untuk query {i+1}, mencoba retry mekanisme...")
                            print(f"  [WARNING] Menemukan pesan error segera setelah navigasi, mencoba retry...")

                            # Coba retry mekanisme segera setelah deteksi error
                            retry_success = self.handle_retry_mechanism(max_retries=self.config['scraper'].get('max_retry_attempts', 10))
                            if retry_success:
                                logger.info("Retry berhasil setelah deteksi awal error")
                                print(f"  [SUCCESS] Retry berhasil setelah deteksi awal error...")
                            else:
                                logger.warning("Retry gagal setelah deteksi awal error")
                                print(f"  [ERROR] Retry gagal setelah deteksi awal error...")

                        # Tunggu elemen tweet muncul dengan pendekatan lebih agresif dan timeout
                        try:
                            # Gunakan pendekatan dengan polling interval yang lebih agresif
                            WebDriverWait(self.driver, 20, poll_frequency=1).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, 'article[data-testid="tweet"]'))
                            )
                        except:
                            # Coba selector alternatif yang mungkin muncul di halaman kosong atau error
                            try:
                                # Tunggu dengan pendekatan polling yang lebih cepat
                                element_found = False
                                for _ in range(20):  # Coba hingga 20 kali dengan jeda 1 detik
                                    time.sleep(1)
                                    tweet_elements = self.driver.find_elements(By.CSS_SELECTOR, 'article[data-testid="tweet"]')
                                    alternative_elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-testid="cellInnerDiv"] article')

                                    if len(tweet_elements) > 0 or len(alternative_elements) > 0:
                                        element_found = True
                                        break

                                    # Cek apakah ada pesan error
                                    if self.detect_something_went_wrong():
                                        break

                                if not element_found:
                                    # Cek apakah ada pesan error sekarang
                                    if self.detect_something_went_wrong():
                                        logger.warning(f"Menemukan pesan error setelah pengecekan lanjutan untuk query {i+1}")
                                        print(f"  [ERROR] Menemukan pesan error setelah pengecekan lanjutan")

                                        # Coba retry mekanisme
                                        retry_success = self.handle_retry_mechanism(max_retries=self.config['scraper'].get('max_retry_attempts', 10))
                                        if retry_success:
                                            logger.info("Retry berhasil setelah deteksi error lanjutan")
                                            print(f"  [SUCCESS] Retry berhasil setelah deteksi error lanjutan...")

                                            # Tunggu lebih lama setelah retry
                                            time.sleep(8)

                                            # Pastikan tweet muncul setelah retry dengan polling lebih agresif
                                            for _ in range(15):  # Coba hingga 15 kali
                                                time.sleep(1)
                                                tweet_elements = self.driver.find_elements(By.CSS_SELECTOR, 'article[data-testid="tweet"]')
                                                if len(tweet_elements) > 0:
                                                    break

                                            if len(tweet_elements) == 0:
                                                logger.warning("Tetap tidak ada tweet setelah retry")
                                                print(f"  [ERROR] Tetap tidak ada tweet setelah retry")
                                                break
                                        else:
                                            logger.warning("Retry gagal setelah deteksi error lanjutan")
                                            print(f"  [ERROR] Retry gagal setelah deteksi error lanjutan...")
                                            break

                            except Exception as e:
                                logger.warning(f"Error saat menunggu elemen: {str(e)}")
                                logger.warning(f"Query {i+1} tidak menghasilkan tweet setelah pengecekan tambahan, lanjut ke query berikutnya")
                                print(f"  [ERROR] Tidak ada tweet ditemukan di query ini setelah pengecekan tambahan, lanjut ke query berikutnya")
                                break  # Keluar dari retry loop dan lanjut ke query berikutnya
                    except Exception as nav_error:
                        logger.error(f"Error navigasi untuk query {i+1}: {nav_error}")

                        # Jika error terkait dengan koneksi terputus ke driver
                        if "connection" in str(nav_error).lower() or "session" in str(nav_error).lower() or "no connection could be made" in str(nav_error).lower():
                            logger.error("Koneksi ke browser terputus. Mencoba restart browser...")
                            try:
                                self.driver.quit()  # Tutup driver lama
                                # Tunggu sebentar sebelum membuat yang baru
                                time.sleep(5)
                                # Dalam konteks ini kita tidak bisa membuat driver baru karena tidak ada akses ke setup_driver
                                # Jadi kita hanya bisa keluar dari fungsi ini dan membiarkan error menyebar
                                raise Exception("Koneksi browser terputus, memerlukan restart") from nav_error
                            except:
                                raise Exception("Koneksi browser terputus, memerlukan restart") from nav_error

                        # Cek rate limiting hanya jika error terkait dengan itu
                        elif "rate" in str(nav_error).lower() or "limit" in str(nav_error).lower() or self.detect_rate_limiting():
                            logger.warning(f"Terkena rate limiting saat navigasi query {i+1}, retry {retry_count+1}/{max_retries}")
                            retry_count += 1
                            if retry_count <= max_retries:
                                self.exponential_backoff(retry_count)
                                continue  # Coba lagi dengan query yang sama
                            else:
                                logger.warning(f"Mencapai maksimum retry untuk query {i+1}, lanjut ke query berikutnya")
                                break  # Jika sudah max retry, lanjut ke query berikutnya
                        else:
                            retry_count += 1
                            if retry_count <= max_retries:
                                self.exponential_backoff(retry_count)
                                continue  # Coba lagi dengan query yang sama
                            else:
                                logger.warning(f"Mencapai maksimum retry untuk query {i+1}, lanjut ke query berikutnya")
                                break  # Jika sudah max retry, lanjut ke query berikutnya

                    # Reset counter untuk query ini
                    consecutive_no_new = 0
                    max_consecutive_no_new = 50  # Increased for better tolerance of no new content
                    scroll_count = 0
                    query_scraped = 0  # Jumlah tweet dari query saat ini
                    previous_total = 0

                    # Counter untuk melacak berapa kali tidak ada data baru
                    no_new_data_counter = 0
                    max_no_new_data = 20  # Increased to allow more iterations without new data before switching query

                    # Counter untuk memantau kapan perlu melakukan data processing
                    last_processing_scroll = 0
                    tweets_since_last_processing = 0
                    max_tweets_between_processing = self.config['scraper'].get('max_tweets_between_processing', 100)

                    # Progress bar untuk query saat ini
                    query_tweet_pbar = tqdm(total=min(10000, max_tweets), desc=f"{query_name} Progress", position=1, leave=False,
                                            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}, {rate_fmt}]')

                    # Lakukan scraping untuk query saat ini - using infinite scroll with configurable limits
                    rate_limit_check_counter = 0  # Counter untuk membatasi pengecekan rate limiting
                    scroll_pbar = tqdm(total=100000, desc=f"Scroll Progress", position=2, leave=False)  # Very high number (effectively infinite)

                    while total_scraped < max_tweets and consecutive_no_new < max_consecutive_no_new:
                        try:
                            # Cek rate limiting hanya setiap 5 scroll untuk mengurangi overhead
                            rate_limit_check_counter += 1
                            if rate_limit_check_counter >= 5:
                                if self.detect_rate_limiting():
                                    logger.warning(f"Terkena rate limiting saat scraping {query_name}, retry {retry_count+1}/{max_retries}")
                                    print(f"  ⚠️  Terkena rate limiting saat scraping, retry {retry_count+1}/{max_retries}...")
                                    retry_count += 1
                                    self.exponential_backoff(retry_count)
                                    break  # Keluar dari loop scraping
                                rate_limit_check_counter = 0  # Reset counter

                            # Ambil tweet dari halaman saat ini
                            tweets = self.extract_tweets_advanced()

                            if tweets:
                                # Simpan tweet ke collection bulanan
                                saved_count = self.process_and_save_tweets(tweets, collection)
                                if saved_count > 0:
                                    total_scraped += saved_count
                                    query_scraped += saved_count
                                    tweets_since_last_processing += saved_count

                                    # Update progress bar
                                    query_tweet_pbar.update(saved_count)

                                    if saved_count > 0:
                                        logger.info(f"{query_name} - Total terkumpul untuk bulan ini: {total_scraped} tweet (total bulan: {total_scraped})")
                                        print(f"    [INBOX] Berhasil mengumpulkan {saved_count} tweet baru (total bulan: {total_scraped})")
                                        consecutive_no_new = 0  # Reset jika ada data baru
                                        no_new_data_counter = 0  # Reset counter tidak ada data baru
                                else:
                                    consecutive_no_new += 1
                                    no_new_data_counter += 1  # Tambah counter tidak ada data baru
                            else:
                                consecutive_no_new += 1  # Tidak ada tweet ditemukan
                                no_new_data_counter += 1  # Tambah counter tidak ada data baru

                            # Jika tidak ada data baru selama max_no_new_data kali, lanjut ke query berikutnya
                            if no_new_data_counter >= max_no_new_data:
                                logger.info(f"Tidak ada data baru selama {max_no_new_data} iterasi berturut-turut, lanjut ke query berikutnya")
                                print(f"  [REFRESH] Tidak ada data baru selama {max_no_new_data} iterasi, lanjut ke query berikutnya...")
                                break

                            # Scroll ke bawah untuk memuat lebih banyak data
                            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

                            # Tunggu sebentar agar konten dimuat - TAPI HANYA JIKA ADA DATA BARU
                            # Jika tidak ada data baru, kurangi jeda untuk kecepatan lebih tinggi
                            if query_scraped > previous_total:
                                # Ada data baru, gunakan jeda normal untuk memastikan konten termuat
                                base_pause = random.uniform(self.scroll_pause_min/2, self.scroll_pause_max/2)
                            else:
                                # Tidak ada data baru, gunakan jeda minimal untuk kecepatan
                                base_pause = random.uniform(0.1, 0.3)  # Jeda sangat cepat saat tidak ada data baru

                            time.sleep(base_pause)

                            scroll_count += 1
                            scroll_pbar.update(1)

                            # Tampilkan info secara berkala
                            if scroll_count % 20 == 0:
                                print(f"    [CHART] Status: {scroll_count} scroll, {query_scraped} tweet dari {query_name}")

                            # Tambahkan deteksi halaman tidak berubah untuk menghindari loop tak terbatas
                            if query_scraped == previous_total and scroll_count > 5:
                                consecutive_no_new += 1

                                # Cek apakah muncul pesan "Something went wrong" ketika tidak ada perubahan konten
                                if self.detect_something_went_wrong():
                                    logger.warning(f"Menemukan pesan 'Something went wrong' saat tidak ada perubahan konten (scroll {scroll_count})")
                                    print(f"    [WARNING] Menemukan pesan 'Something went wrong', mencoba retry...")

                                    # Lakukan retry mekanisme
                                    retry_success = self.handle_retry_mechanism(max_retries=self.config['scraper'].get('max_retry_attempts', 10))
                                    if retry_success:
                                        logger.info("Retry berhasil, melanjutkan scraping...")
                                        print(f"    [SUCCESS] Retry berhasil, melanjutkan scraping...")
                                        consecutive_no_new = 0  # Reset karena ada perubahan konten
                                        previous_total = len(self.driver.find_elements(By.CSS_SELECTOR, 'article[data-testid="tweet"]'))
                                    else:
                                        logger.warning("Retry gagal setelah beberapa percobaan")
                                        print(f"    [ERROR] Retry gagal, melanjutkan...")
                            previous_total = query_scraped

                            # Refresh halaman setiap 100 scroll untuk mencegah halaman menjadi stale
                            if scroll_count > 0 and scroll_count % 100 == 0:
                                logger.info(f"Melakukan refresh halaman setelah {scroll_count} scroll untuk mencegah halaman menjadi stale...")
                                print(f"    [REFRESH] Refresh halaman setelah {scroll_count} scroll...")

                                # Simpan posisi scroll sebelum refresh
                                scroll_position = scroll_count

                                # Refresh halaman
                                self.driver.refresh()

                                # Tunggu sebentar agar halaman dimuat kembali
                                time.sleep(5)

                                # Coba kembali ke posisi sebelumnya atau lanjutkan scraping
                                try:
                                    # Tunggu elemen tweet muncul kembali
                                    WebDriverWait(self.driver, 10).until(
                                        EC.presence_of_element_located((By.CSS_SELECTOR, 'article[data-testid="tweet"]'))
                                    )
                                    logger.info(f"Halaman berhasil di-refresh, melanjutkan scraping dari posisi {scroll_position}")
                                    print(f"    [SUCCESS] Halaman berhasil di-refresh, melanjutkan scraping...")
                                except:
                                    logger.warning("Gagal memuat ulang elemen tweet setelah refresh")
                                    print(f"    [WARNING] Memuat ulang elemen tweet setelah refresh...")

                            # Hentikan scraping jika sudah tidak ada tweet baru dalam jumlah scroll tertentu
                            # Ini untuk mendeteksi jika hari saat ini sudah tidak menghasilkan tweet lagi
                            # Jika mode daily processing diaktifkan, beralih ke hari berikutnya setelah 3 scroll tanpa data baru
                            import sys
                            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

                            try:
                                with open("config/config.json", 'r') as f:
                                    config = json.load(f)
                                daily_processing_enabled = config['twitter'].get('daily_processing', False)
                            except:
                                daily_processing_enabled = False

                            if daily_processing_enabled and consecutive_no_new > 3:  # Jika dalam mode daily processing dan tidak ada data baru dalam 3 scroll
                                logger.info(f"Tidak ada tweet baru dalam {consecutive_no_new} iterasi scroll, beralih ke hari berikutnya...")
                                print(f"  [SWITCH] Tidak ada tweet baru, beralih ke hari berikutnya...")
                                break  # Keluar dari loop scraping untuk hari ini dan lanjutkan ke hari berikutnya
                            # For monthly processing, we can allow more consecutive no-new scrolls before breaking
                            elif not daily_processing_enabled and consecutive_no_new > 50:  # For monthly processing, allow up to 50 consecutive no-new scrolls
                                logger.info(f"Tidak ada tweet baru dalam {consecutive_no_new} iterasi scroll...")
                                print(f"  [INFO] Tidak ada tweet baru dalam {consecutive_no_new} scroll...")
                                # Continue to next query but don't necessarily break the entire process

                            # Jika sudah mencapai maksimum, berhenti
                            if total_scraped >= max_tweets:
                                print(f"    [TARGET] Target maksimum {max_tweets} tweet telah tercapai untuk bulan!")
                                break

                            # Tambahkan jeda tambahan setiap 20 scroll untuk menghindari kelelahan sistem
                            if scroll_count % 20 == 0:
                                # Kurangi jeda jika tidak ada data baru ditemukan
                                if consecutive_no_new > 10:  # Jika lebih dari 10 scroll tanpa data baru
                                    jeda = random.randint(3, 8)  # Jeda lebih pendek
                                else:
                                    jeda = random.randint(5, 10)  # Jeda normal
                                logger.info(f"Jeda {jeda} detik setiap 20 scroll (setelah {scroll_count} scroll)")
                                time.sleep(jeda)

                        except Exception as e:
                            logger.error(f"Error dalam loop scraping: {e}")

                            # Cek apakah error terkait dengan koneksi terputus ke driver
                            if "connection" in str(e).lower() or "session" in str(e).lower() or "no connection could be made" in str(e).lower():
                                logger.error("Koneksi ke browser terputus dalam loop scraping. Menghentikan proses...")
                                raise Exception("Koneksi browser terputus, memerlukan restart") from e

                            # Cek apakah error karena rate limiting
                            if self.detect_rate_limiting():
                                logger.warning(f"Terkena rate limiting atau halaman verifikasi dalam loop scraping, retry {retry_count+1}/{max_retries}")
                                print(f"  [WARNING] Terkena rate limiting, retry {retry_count+1}/{max_retries}...")
                                retry_count += 1
                                if retry_count <= max_retries:
                                    self.exponential_backoff(retry_count)
                                    # Coba refresh halaman setelah backoff
                                    try:
                                        self.driver.refresh()
                                        time.sleep(5)  # Kurangi dari 10 menjadi 5
                                    except:
                                        logger.warning("Gagal refresh halaman setelah rate limiting detected")
                                    # Lanjut ke retry berikutnya
                                    break  # Keluar dari loop scraping dan lanjut ke retry
                                else:
                                    logger.warning(f"Mencapai maksimum retry, lanjut ke query berikutnya")
                                    break  # Jika sudah max retry, lanjut ke query berikutnya
                            else:
                                consecutive_no_new += 1  # Anggap sebagai kegagalan
                                no_new_data_counter += 1  # Tambah counter tidak ada data baru

                                # Jika tidak ada data baru selama max_no_new_data kali, lanjut ke query berikutnya
                                if no_new_data_counter >= max_no_new_data:
                                    logger.info(f"Tidak ada data baru selama {max_no_new_data} iterasi berturut-turut, lanjut ke query berikutnya")
                                    print(f"  [REFRESH] Tidak ada data baru selama {max_no_new_data} iterasi, lanjut ke query berikutnya...")
                                    break

                                # Coba refresh halaman jika terjadi error
                                try:
                                    self.driver.refresh()
                                    time.sleep(3)  # Kurangi dari 10 menjadi 3
                                    WebDriverWait(self.driver, 8).until(  # Kurangi dari 15 menjadi 8
                                        EC.presence_of_element_located((By.CSS_SELECTOR, 'article[data-testid="tweet"]'))
                                    )
                                except:
                                    logger.warning("Tidak bisa refresh halaman, lanjut ke query berikutnya")
                                    break

                    # Hentikan progress bar scroll dan query
                    scroll_pbar.close()
                    query_tweet_pbar.close()

                    # Info tambahan setelah menyelesaikan query
                    if query_scraped > 0:
                        print(f"  [CHART_UP] {query_name} berhasil mengumpulkan {query_scraped} tweet")
                    else:
                        print(f"  [CHART_DOWN] {query_name} tidak menghasilkan tweet baru")

                    # Jika berhasil tanpa rate limiting, keluar dari retry loop
                    break
                except Exception as e:
                    logger.error(f"Error dalam {query_name}: {e}")

                    # Cek apakah error terkait dengan koneksi terputus ke driver
                    if "connection" in str(e).lower() or "session" in str(e).lower() or "no connection could be made" in str(e).lower():
                        logger.error("Koneksi ke browser terputus saat eksekusi query. Menghentikan proses...")
                        raise Exception("Koneksi browser terputus, memerlukan restart") from e

                    # Cek apakah error karena rate limiting dengan fungsi deteksi khusus
                    if "rate" in str(e).lower() or "limit" in str(e).lower() or self.detect_rate_limiting():
                        logger.warning(f"Terkena rate limiting atau halaman verifikasi pada {query_name}, retry {retry_count+1}/{max_retries}")
                        retry_count += 1
                        if retry_count <= max_retries:
                            self.exponential_backoff(retry_count)
                            try:
                                self.driver.refresh()
                                time.sleep(5)  # Kurangi dari 10 menjadi 5
                            except:
                                logger.warning("Gagal refresh halaman setelah rate limiting detected")
                        else:
                            logger.warning(f"Mencapai maksimum retry untuk {query_name}, lanjut ke query berikutnya")
                            break  # Jika sudah max retry, lanjut ke query berikutnya
                    else:
                        break  # Jika error bukan karena rate limiting, lanjut ke query berikutnya

            # Update query progress
            query_pbar.update(1)

            logger.info(f"{query_name} selesai, dapatkan {query_scraped} tweet")
            print(f"  [SUCCESS] {query_name} selesai, dapatkan {query_scraped} tweet")

            # Jika sudah mencapai maksimum, berhenti
            if total_scraped >= self.max_tweets:
                logger.info(f"Mencapai batas maksimum {self.max_tweets} tweet untuk bulan")
                print(f"\n[TARGET] Target maksimum {self.max_tweets} tweet telah tercapai untuk bulan!")
                break

            # Tambahkan jeda acak antar query untuk menghindari pembatasan
            jeda = random.randint(10, 20)  # Kurangi dari 20-40 menjadi 10-20
            logger.info(f"Jeda {jeda} detik antar query")
            print(f"  [WAIT] Jeda {jeda} detik sebelum query berikutnya...")
            time.sleep(jeda)

        # Tutup progress bar
        query_pbar.close()

        print(f"\n{'='*60}")
        print(f"SELESAI SCRAPING UNTUK BULAN: {start_date.strftime('%Y-%m')}")
        print(f"Total tweet terkumpul untuk bulan: {total_scraped}")
        print(f"{'='*60}")

        logger.info(f"Selesai scraping untuk bulan {start_date.strftime('%Y-%m')}, total: {total_scraped} tweet")
        return total_scraped
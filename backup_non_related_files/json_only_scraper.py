"""
src/json_only_scraper.py
Scraper untuk mendapatkan tweet langsung ke file JSON tanpa database
"""

import time
import logging
import json
import re
from datetime import datetime, timedelta, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import dateutil.parser
import os
from typing import List, Dict

logger = logging.getLogger("SummaryApp")

class JSONOnlyScraper:
    def __init__(self, driver, config):
        self.driver = driver
        self.config = config

        # Konfigurasi scraping maksimum
        self.max_tweets = config['twitter']['max_tweets']
        self.scroll_pause = config['scraper'].get('scroll_min_pause', 0.5)
        self.max_scrolls = 200

        # Set untuk mencegah duplikasi dalam sekali sesi
        self.processed_tweet_ids = set()
        self.processed_texts = set()

    def inject_cookies(self):
        """Menyuntikkan cookie sesi dari file JSON."""
        try:
            logger.info("Navigasi awal ke x.com...")
            self.driver.get("https://x.com")
            time.sleep(3)

            with open(self.config['twitter']['cookies_file'], 'r') as f:
                cookies = json.load(f)

            logger.info(f"Memuat {len(cookies)} cookie...")
            for cookie in cookies:
                cookie_dict = {
                    'name': cookie.get('name'),
                    'value': cookie.get('value'),
                    'domain': cookie.get('domain', '.x.com'),
                    'path': cookie.get('path', '/'),
                    'secure': cookie.get('secure', True),
                    'httpOnly': cookie.get('httpOnly', False),
                    'sameSite': 'Lax'
                }
                try:
                    self.driver.add_cookie(cookie_dict)
                except:
                    continue  # Lewati cookie yang bermasalah

            logger.info("Cookie disuntikkan. Refresh halaman...")
            self.driver.refresh()
            time.sleep(5)

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
        """Menghasilkan variasi kata kunci terkait MBG untuk menemukan lebih banyak tweet."""
        base_keywords = [
            "Makan Bergizi Gratis", "MBG", "Program Makan Bergizi",
            "Makan Gratis", "Program MBG", "Kementerian PPPA",
            "Pangan Anak", "Nutrisi Anak", "Makanan Gratis Anak"
        ]

        # Variasi ejaan dan singkatan
        variations = []
        for keyword in base_keywords:
            variations.append(keyword)
            # Tambahkan variasi dengan spasi diganti
            variations.append(keyword.replace(" ", ""))
            # Tambahkan variasi dengan huruf kapital berbeda
            variations.append(keyword.lower())
            variations.append(keyword.upper())

        # Tambahkan kata kunci terkait
        related_keywords = [
            "gizi", "anak", "makan", "gratis", "sehat", "nutrisi",
            "kakak asuh", "anak asuh", "kementerian", "PPPA", "pembelajaran",
            "pendidikan", "kesehatan", "makan siang", "kakak asuh", "program"
        ]

        return variations + related_keywords

    def build_search_query(self, target_date, additional_keywords=None):
        """Membangun query pencarian canggih untuk mendapatkan lebih banyak tweet."""
        # Ambil kata kunci dasar dari query_1 dan variasinya
        base_query = self.config['twitter'].get('query_1', 'Makan Bergizi Gratis OR MBG lang:id')

        # Format tanggal
        since_date = target_date.strftime('%Y-%m-%d')
        until_date = (target_date + timedelta(days=1)).strftime('%Y-%m-%d')

        # Bangun query dengan OR untuk menangkap lebih banyak tweet
        keyword_variations = self._generate_extended_keywords()

        # Buat query OR untuk variasi kata kunci
        or_keywords = " OR ".join([f'"{kw}"' for kw in keyword_variations[:10]])  # Ambil 10 pertama untuk menghindari query terlalu panjang

        # Gabungkan base query dengan variasi
        full_query = f"({base_query}) OR ({or_keywords})"

        # Tambahkan rentang tanggal
        search_query = f"{full_query} since:{since_date} until:{until_date}"

        return search_query

    def build_alternative_queries(self, target_date):
        """Membangun beberapa query alternatif untuk mendapatkan lebih banyak tweet."""
        since_date = target_date.strftime('%Y-%m-%d')
        until_date = (target_date + timedelta(days=1)).strftime('%Y-%m-%d')

        queries = []

        # Gunakan query_1 sampai query_5 dari konfigurasi
        for i in range(1, 6):
            query_key = f'query_{i}'
            if query_key in self.config['twitter']:
                query_value = self.config['twitter'][query_key]
                queries.append(f"{query_value} since:{since_date} until:{until_date}")

        # Jika tidak ada query yang dikonfigurasi, gunakan default
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
        """Navigasi ke halaman pencarian dengan query maksimum."""
        search_query = self.build_search_query(target_date)

        # Format URL pencarian
        encoded_query = search_query.replace(' ', '%20').replace(':', '%3A').replace(',', '%2C')
        search_url = f"https://x.com/search?q={encoded_query}&src=typed_query&f=live"

        logger.info(f"Mengakses URL Pencarian: {search_url}")
        self.driver.get(search_url)

        # Tunggu elemen tweet muncul atau timeout
        try:
            # Tunggu beberapa saat sebelum mencari elemen untuk memastikan halaman dimuat
            time.sleep(3)
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'article[data-testid="tweet"]'))
            )
        except:
            logger.warning("Tidak menemukan elemen tweet, coba alternatif...")
            # Coba temukan elemen tweet dengan selektor lain
            try:
                time.sleep(5)
                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="cellInnerDiv"]'))
                )
            except:
                logger.warning("Tetap tidak menemukan elemen tweet")

        # Tambahkan jeda tambahan agar halaman benar-benar siap
        time.sleep(2)

    def extract_tweets_advanced(self):
        """Ekstrak tweet dengan pendekatan super cepat dan efisien."""
        try:
            # Tunggu sebentar agar konten benar-benar dimuat
            time.sleep(2)

            # Tambahkan pengecekan apakah halaman sedang dalam proses loading
            loading_elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-testid="loading"]')
            if len(loading_elements) > 0:
                logger.info("Halaman sedang loading, tunggu beberapa detik...")
                time.sleep(10)

            # Ambil semua artikel yang mungkin berisi tweet - tambahkan lebih banyak selektor
            tweet_elements = self.driver.find_elements(By.CSS_SELECTOR,
                'article[data-testid="tweet"], [data-testid="cellInnerDiv"] article, div[role="article"], [data-testid="tweet"]')

            # Tambahkan selektor alternatif jika tidak ditemukan
            if len(tweet_elements) == 0:
                tweet_elements = self.driver.find_elements(By.CSS_SELECTOR,
                    'div[data-testid="cellInnerDiv"] div[role="article"]')

            tweets = []
            processed_on_page = 0  # Jumlah yang diproses di halaman ini

            for element in tweet_elements:
                try:
                    # Ambil outerHTML langsung untuk kecepatan maksimum
                    outer_html = element.get_attribute('outerHTML')
                    if not outer_html:
                        continue

                    # Ekstrak informasi tweet dari HTML
                    tweet_data = self._extract_tweet_data_fast(outer_html, element)
                    if tweet_data and tweet_data.get('_id') not in self.processed_tweet_ids:
                        # Tambahkan filter lanjutan untuk mencegah duplikat
                        text_hash = hash(tweet_data['text'].strip().lower())
                        if text_hash not in self.processed_texts:
                            tweets.append(tweet_data)
                            self.processed_tweet_ids.add(tweet_data['_id'])
                            self.processed_texts.add(text_hash)
                            processed_on_page += 1

                            # Batasi jumlah yang diproses per halaman untuk kecepatan
                            if processed_on_page >= 20:  # Kurangi jumlah agar lebih ringan
                                break
                except Exception as e:
                    logger.debug(f"Error memproses elemen: {str(e)}")
                    continue  # Lewati elemen yang bermasalah

            return tweets
        except Exception as e:
            logger.error(f"Error dalam extract_tweets_advanced: {str(e)}")
            return []  # Kembalikan list kosong jika gagal

    def _extract_tweet_data_fast(self, inner_html, element):
        """Ekstrak data tweet dengan pendekatan super cepat."""
        try:
            # Parse sebagian HTML untuk mendapatkan info penting
            soup = BeautifulSoup(inner_html, 'html.parser')

            # Ambil teks tweet
            text_divs = soup.find_all('div', {'data-testid': 'tweetText'})
            text = ""
            if text_divs:
                text = text_divs[0].get_text(separator=" ", strip=True)
            else:
                # Coba alternatif
                text_divs = soup.find_all('div', {'dir': 'auto'})
                if text_divs:
                    text = text_divs[0].get_text(separator=" ", strip=True)

            if not text or len(text) < 5:  # Filter teks terlalu pendek
                return None

            # Ekstrak URL dan ID tweet dari href
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

            # Ambil waktu dari elemen waktu jika tersedia
            time_element = element.find_element(By.CSS_SELECTOR, 'time') if element.find_elements(By.CSS_SELECTOR, 'time') else None
            if time_element:
                datetime_attr = time_element.get_attribute('datetime')
                if datetime_attr:
                    try:
                        created_at = dateutil.parser.isoparse(datetime_attr)
                    except:
                        created_at = datetime.utcnow()
                else:
                    created_at = datetime.utcnow()
            else:
                created_at = datetime.utcnow()

            # Ambil nama penulis dari elemen
            author_name = ""
            author_divs = element.find_elements(By.CSS_SELECTOR, 'div[data-testid="User-Names"]')
            if author_divs:
                spans = author_divs[0].find_elements(By.CSS_SELECTOR, 'span')
                if spans:
                    author_name = spans[0].text

            # Ambil lokasi
            location_text = None
            location_spans = element.find_elements(By.CSS_SELECTOR, 'span[data-testid="UserLocation"]')
            if location_spans:
                location_text = location_spans[0].text

            # Metrik dasar
            metrics = {
                'reply_count': 0,
                'retweet_count': 0,
                'like_count': 0
            }

            # Ambil metrik dari teks tombol
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

            return {
                '_id': tweet_id,
                'text': text,
                'created_at': created_at,
                'tweet_url': tweet_url,
                'author_handle': author_handle,
                'author_name': author_name,
                'location': location_text,
                'metrics': metrics
            }

        except:
            return None

    def clean_text(self, text):
        """Membersihkan teks sederhana."""
        text = re.sub(r'http\\S+|www\\S+|https\\S+', '[LINK]', text, flags=re.MULTILINE)
        text = re.sub(r'@\\w+', '[MENTION]', text)
        text = re.sub(r'#(\\w+)', r'\\1', text)
        text = ' '.join(text.split())
        return text.strip()

    def process_tweets(self, tweets) -> List[Dict]:
        """Proses tweet tanpa menyimpan ke database."""
        if not tweets:
            return []

        transformed_tweets = []
        for tweet_data in tweets:
            try:
                clean_content = self.clean_text(tweet_data['text'])

                transformed_tweet = {
                    "_id": tweet_data.get('_id', ''),
                    "content": {
                        "text": tweet_data['text'],
                        "clean_text": clean_content.lower()
                    },
                    "metadata": {
                        "author_name": tweet_data.get('author_name', ''),
                        "author_handle": tweet_data.get('author_handle', ''),
                        "created_at": tweet_data['created_at'],
                        "scraped_at": datetime.utcnow().isoformat(),
                        "location": tweet_data.get('location', None),
                        "tweet_url": tweet_data.get('tweet_url', '')
                    },
                    "metrics": tweet_data['metrics'],
                    "processing_status": {
                        "sentiment_analyzed": False
                    }
                }

                transformed_tweets.append(transformed_tweet)

            except Exception as e:
                logger.error(f"Error memproses tweet: {e}")
                continue

        return transformed_tweets

    def save_daily_tweets(self, tweets: List[Dict], target_date_str: str):
        """Simpan tweet ke file JSON harian."""
        filename = f"data/mbg_sentiment_db.tweets_{target_date_str}.json"
        
        # Buat direktori data jika belum ada
        os.makedirs('data', exist_ok=True)
        
        # Ubah ObjectId menjadi string agar bisa disimpan ke JSON
        for doc in tweets:
            if '_id' in doc and hasattr(doc['_id'], '__class__') and doc['_id'].__class__.__name__ == 'ObjectId':
                doc['_id'] = str(doc['_id'])
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(tweets, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"Data harian disimpan ke {filename}")
        return len(tweets)

    def scrape_day_maximum(self, target_date):
        """Scrape maksimum tweet untuk satu hari menggunakan multi-query."""
        logger.info(f"Memulai scraping maksimum untuk: {target_date.strftime('%Y-%m-%d')}")

        total_scraped = 0
        all_tweets = []  # Kumpulkan semua tweet untuk disimpan di akhir

        # Dapatkan semua query alternatif
        alternative_queries = self.build_alternative_queries(target_date)

        logger.info(f"Menggunakan {len(alternative_queries)} query alternatif untuk memaksimalkan hasil")

        for i, query in enumerate(alternative_queries):
            logger.info(f"Menjalankan query {i+1}/{len(alternative_queries)}: {query[:100]}...")

            # Navigasi ke pencarian dengan query saat ini
            encoded_query = query.replace(' ', '%20').replace(':', '%3A').replace(',', '%2C')
            search_url = f"https://x.com/search?q={encoded_query}&src=typed_query&f=live"

            logger.debug(f"URL Query {i+1}: {search_url}")
            self.driver.get(search_url)

            # Tunggu elemen tweet muncul
            try:
                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'article[data-testid="tweet"]'))
                )
            except:
                logger.warning(f"Query {i+1} tidak menghasilkan tweet, lanjut ke query berikutnya")
                continue  # Lanjut ke query berikutnya

            # Reset counter untuk query ini
            consecutive_no_new = 0
            max_consecutive_no_new = 8  # Turunkan sedikit untuk efisiensi
            scroll_count = 0
            query_scraped = 0  # Jumlah tweet dari query saat ini
            previous_total = 0

            # Lakukan scraping untuk query saat ini
            while total_scraped < self.max_tweets and scroll_count < 50 and consecutive_no_new < max_consecutive_no_new:
                # Ambil tweet dari halaman saat ini
                tweets = self.extract_tweets_advanced()

                if tweets:
                    # Proses tweet
                    processed_tweets = self.process_tweets(tweets)
                    all_tweets.extend(processed_tweets)
                    query_scraped += len(processed_tweets)
                    total_scraped += len(processed_tweets)

                    if len(processed_tweets) > 0:
                        logger.info(f"Query {i+1} - Total terkumpul: {total_scraped} tweet ({len(processed_tweets)} baru)")
                        consecutive_no_new = 0  # Reset jika ada data baru
                    else:
                        consecutive_no_new += 1
                else:
                    consecutive_no_new += 1  # Tidak ada tweet ditemukan

                # Scroll ke bawah untuk memuat lebih banyak data
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

                # Tunggu sebentar agar konten dimuat
                time.sleep(self.scroll_pause)

                scroll_count += 1

                # Tambahkan deteksi halaman tidak berubah untuk menghindari loop tak terbatas
                if query_scraped == previous_total and scroll_count > 3:
                    consecutive_no_new += 1
                previous_total = query_scraped

                # Jika sudah mencapai maksimum, berhenti
                if total_scraped >= self.max_tweets:
                    break

            logger.info(f"Query {i+1} selesai, dapatkan {query_scraped} tweet")

            # Jika sudah mencapai maksimum, berhenti
            if total_scraped >= self.max_tweets:
                logger.info(f"Mencapai batas maksimum {self.max_tweets} tweet")
                break

        # Simpan semua tweet ke file JSON setelah selesai scraping
        if all_tweets:
            daily_count = self.save_daily_tweets(all_tweets, target_date.strftime('%Y-%m-%d'))
            logger.info(f"Selesai scraping untuk {target_date.strftime('%Y-%m-%d')}, total: {daily_count} tweet disimpan")
        else:
            logger.info(f"Selesai scraping untuk {target_date.strftime('%Y-%m-%d')}, total: 0 tweet ditemukan")

        return len(all_tweets)
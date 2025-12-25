# ETL Automation for MBG Sentiment Analysis

Repository ini berisi kode untuk mengotomasi proses ETL (Extract, Transform, Load) data dari Twitter/X untuk analisis sentimen terkait program Makan Bergizi Gratis (MBG).

## Deskripsi

Proyek ini menggunakan teknik scraping canggih untuk mendapatkan data tweet dari Twitter/X, kemudian melakukan proses pembersihan data dan pelabelan sentimen. Sistem dirancang untuk bersifat tangguh (resilient) dengan kemampuan melanjutkan proses dari titik terakhir jika terputus.

## Fitur Utama

- **Resilient ETL**: Dapat melanjutkan proses dari titik terakhir jika terputus
- **Scraping Anti-Detection**: Menggunakan teknik anti-detection untuk menghindari pemblokiran
- **Data Cleaning**: Membersihkan data mentah dari Twitter
- **Sentiment Labeling**: Melabeli sentimen dari tweet (positif, negatif, netral)
- **Daily & Monthly Aggregation**: Mengumpulkan data harian dan bulanan
- **MongoDB Integration**: Menyimpan data ke database MongoDB

## Struktur Proyek

```
ETL-Automation-MBG-Sentiment/
├── config/
│   ├── config.json                 # Konfigurasi sistem
│   └── indonesia_locations.json    # Konfigurasi lokasi
├── data/                           # Direktori output data
├── logs/                           # Log aktivitas sistem
├── src/
│   └── resilient_scraper.py        # Scraper utama
├── requirements.txt                # Daftar dependensi
├── resilient_etl.py                # File utama ETL
├── twitter_cookies.json            # Cookie login Twitter
├── utils.py                        # Fungsi-fungsi utilitas
└── backup_non_related_files/       # Backup file-file yang tidak terkait
```

## Persiapan dan Instalasi

### 1. Instalasi Python dan Git

Pastikan Anda memiliki Python 3.8+ dan Git terinstal di sistem Anda. Jika belum, Anda bisa mengunduhnya dari:

- **Python**: https://www.python.org/downloads/
- **Git**: https://git-scm.com/downloads

### 2. Clone Repository

1. Buka Command Prompt atau Git Bash
2. Clone repository ini:
   ```bash
   git clone <URL_REPOSITORY>
   cd ETL-Automation-MBG-Sentiment
   ```

### 3. Instalasi Dependencies

1. Instal dependensi yang dibutuhkan:
   ```bash
   pip install -r requirements.txt
   ```

   Jika Anda menggunakan environment virtual (disarankan):
   ```bash
   # Membuat environment virtual
   python -m venv venv
   # Aktifkan environment (Windows)
   venv\Scripts\activate
   # Instal dependensi
   pip install -r requirements.txt
   ```

### 4. Instalasi Chrome Driver

**undetected_chromedriver** akan secara otomatis mengunduh dan menginstal ChromeDriver yang sesuai. Namun, pastikan Anda memiliki Google Chrome terinstal di sistem Anda:

1. Unduh dan instal Google Chrome terbaru dari: https://www.google.com/chrome/
2. Verifikasi instalasi Chrome dengan membukanya
3. Saat pertama kali menjalankan script, **undetected_chromedriver** akan secara otomatis mengelola ChromeDriver untuk Anda

Jika Anda mengalami masalah, Anda juga bisa menginstal ChromeDriver secara manual:
1. Cek versi Google Chrome Anda
2. Download ChromeDriver yang sesuai dari: https://chromedriver.chromium.org/
3. Simpan di folder PATH atau folder proyek

### 5. Setup Database MongoDB

#### Pilihan 1: MongoDB Atlas (Disarankan - Online)
1. Kunjungi: https://www.mongodb.com/atlas
2. Buat akun gratis atau login jika sudah memiliki akun
3. Buat cluster baru dengan spesifikasi gratis
4. Dalam cluster, buat database user dengan username dan password
5. Dapatkan connection string: Klik "Connect" → "Connect your application" → Copy connection string
6. Ganti `<password>` dalam connection string dengan password user yang Anda buat
7. Masukkan connection string ke file `config/config.json` di bagian `database.mongo_uri`

#### Pilihan 2: MongoDB Lokal
1. Download dan instal MongoDB Community Server: https://www.mongodb.com/try/download/community
2. Jalankan MongoDB service di sistem Anda
3. Gunakan connection string: `mongodb://localhost:27017` atau sesuaikan port jika berbeda
4. Masukkan ke file `config/config.json` di bagian `database.mongo_uri`

### 6. Konfigurasi Cookie Twitter/X

Untuk dapat mengakses data Twitter/X, Anda memerlukan cookie otentikasi. Ada beberapa cara:

#### Cara 1: Menggunakan Browser Extension (Disarankan)
1. Instal browser extension "Get Cookies.txt" di Chrome/Edge
2. Login ke https://x.com dengan akun Twitter/X Anda
3. Ekstrak cookies dan simpan sebagai file `twitter_cookies.json`
4. File ini harus berisi objek array dengan format:
   ```json
   [
     {"name": "auth_token", "value": "your_auth_token_here"},
     {"name": "ct0", "value": "your_ct0_cookie_here"},
     {"name": "guest_id", "value": "your_guest_id_here"},
     {"name": "kdt", "value": "your_kdt_cookie_here"},
     {"name": "twid", "value": "your_twid_cookie_here"}
   ]
   ```

#### Cara 2: Manual (Memerlukan pengetahuan teknis)
1. Login ke https://x.com
2. Buka Developer Tools (F12)
3. Pergi ke tab "Application" atau "Storage"
4. Cari "Cookies" → "https://x.com"
5. Cari cookie yang diperlukan (auth_token, ct0, ds_t, guest_id, kdt, twid)
6. Tambahkan ke file `twitter_cookies.json`

**Catatan Penting**: Cookie akan kadaluarsa secara berkala, jadi Anda perlu memperbaruinya secara rutin.

## Konfigurasi

File `config/config.json` berisi beberapa bagian penting:
- `database`: Konfigurasi koneksi MongoDB
- `twitter`: Parameter untuk scraping (rentang waktu, hashtag, dll)
- `logging`: Konfigurasi logging
- `sentiment`: Parameter untuk analisis sentimen
- `scraper`: Parameter untuk proses scraping
- `etl`: Parameter untuk proses ETL

Contoh konfigurasi:
```json
{
  "database": {
    "mongo_uri": "mongodb+srv://username:password@cluster.mongodb.net/",
    "db_name": "mbg_sentiment_db",
    "collection_prefix": "tweets_"
  },
  "twitter": {
    "cookies_file": "twitter_cookies.json",
    "max_tweets": 1000,
    "days_back": 30,
    "query_1": "Makan Bergizi Gratis OR MBG lang:id",
    "query_2": "Makan Gratis OR MBG lang:id",
    "start_date": "2024-09-01",
    "end_date": "2024-09-30"
  },
  "scraper": {
    "scroll_min_pause": 1.0,
    "scroll_max_pause": 3.0,
    "max_retries": 3,
    "base_backoff": 8
  }
}
```

## Penggunaan

### Menjalankan ETL Utama

Jalankan ETL utama untuk mengumpulkan data dari rentang tanggal tertentu:
```bash
python resilient_etl.py
```

### Menjalankan dengan parameter khusus

Anda juga dapat menjalankan dengan tanggal spesifik:
```bash
# Jalankan dari kode, edit resilient_etl.py
# Di bagian akhir file, uncomment dan sesuaikan:
run_etl("2024-09-01", "2024-09-30")  # September
# atau
process_existing_data_range("2024-09-01", "2024-09-30")  # Proses ulang data yang sudah ada
```

### Proses yang Dilakukan

1. **Extract**: Mengumpulkan tweet dari Twitter/X berdasarkan kueri dan rentang tanggal
2. **Transform**: Membersihkan data dan menganalisis sentimen
3. **Load**: Menyimpan data ke MongoDB dan file JSON harian & bulanan

### Output yang Dihasilkan

- Data harian disimpan di folder `data/` dengan format: `mbg_sentiment_db.tweets_YYYY-MM-DD_labeled.json`
- Data bulanan disimpan di folder `data/` dengan format: `mbg_sentiment_db.tweets_YYYY-MM_labeled.json`
- Log aktivitas disimpan di folder `logs/`
- Data juga disimpan ke MongoDB sesuai konfigurasi

## Fungsi Utama

- `run_etl()`: Fungsi utama untuk menjalankan proses ETL dari awal atau melanjutkan dari titik terakhir
- `process_existing_data_range()`: Untuk memproses ulang data yang sudah ada di database
- `run_etl(start_date, end_date)`: Menjalankan ETL dengan rentang tanggal tertentu
- `run_etl(start_date, end_date, continue_from_last=False)`: Menjalankan ETL dengan opsi untuk mengabaikan data yang sudah ada

## Teknologi yang Digunakan

- **Python**: Bahasa pemrograman utama
- **Selenium** dengan **undetected_chromedriver**: Otomasi browser untuk scraping
- **MongoDB**: Database NoSQL untuk penyimpanan data
- **BeautifulSoup**: Parsing HTML untuk ekstraksi data
- **Transformers** (HuggingFace): Untuk analisis sentimen dalam bahasa Indonesia
- **Tqdm**: Progress bar untuk visualisasi proses
- **Regex**: Pemrosesan teks dan validasi
- **Pymongo**: Driver MongoDB untuk Python

## Troubleshooting

### 1. Error saat scraping
- Pastikan cookie masih valid
- Periksa koneksi internet
- Coba kurangi kecepatan scraping di konfigurasi

### 2. Error koneksi database
- Pastikan string koneksi MongoDB benar
- Pastikan firewall tidak memblokir koneksi
- Cek apakah MongoDB service berjalan (untuk lokal)

### 3. Rate limiting
- Sistem sudah memiliki exponential backoff, tapi jika masih terjadi:
- Tambahkan delay di konfigurasi
- Kurangi jumlah maksimum tweet per hari

### 4. Memory issues
- Kurangi batch size di fungsi-fungsi proses
- Proses lebih sedikit hari dalam satu eksekusi

## Kontribusi

Silakan buat pull request jika ingin berkontribusi pada proyek ini.

## Lisensi

[MIT License]
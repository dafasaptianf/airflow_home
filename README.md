
# Airflow ETL Pipeline - Lion Parcel

## Overview
Proyek ini bertujuan untuk membangun pipeline **ETL (Extract, Transform, Load)** menggunakan **Apache Airflow** yang berjalan sepenuhnya di dalam **Docker**.

Database Architecture
Baik source_db maupun warehouse_db dijalankan di dalam container PostgreSQL yang sama melalui layanan postgres pada docker-compose.yaml.
Airflow mengakses kedua database tersebut menggunakan dua koneksi terpisah:

```bash
postgres_docker_src → untuk database sumber (source_db)
```
```bash
postgres_docker_wh → untuk database tujuan (warehouse_db)
```

Dengan pendekatan ini, seluruh pipeline ETL (Airflow, PostgreSQL) berjalan sepenuhnya di dalam ekosistem Docker, tanpa ketergantungan pada instalasi lokal.

### Fitur Utama
1.  Ekstraksi dan konsolidasi data **JSON** menjadi satu tabel terstruktur di PostgreSQL.
2.  ETL dari **source_db** ke **warehouse_db** menggunakan metode **soft delete**.
3.  Pipeline berjalan otomatis menggunakan **Airflow scheduler** setiap awal bulan atau setiap jam.
4.  Semua komponen utama — Airflow, PostgreSQL — dijalankan melalui **Docker Compose**.

---

## Environment Setup

###  Clone Repository

```
git clone [https://github.com/dafasaptianf/airflow_home.git](https://github.com/dafasaptianf/airflow_home.git)

````

### 2️. Build Custom Airflow Image

Pastikan file `Dockerfile` dan `requirements.txt` sudah tersedia di root folder (`airflow_home`).

```bash
docker build -t airflow-custom:latest .
```

### 3️. Jalankan Docker Compose

```bash
docker compose up -d
```

### 4️. Cek Service Status

```bash
docker ps
```

Pastikan container berikut sudah berjalan:

  * `airflow_home-airflow-apiserver-1`
  * `airflow_home-airflow-scheduler-1`
  * `airflow_home-airflow-worker-1`
  * `airflow_home-postgres-1`
  * `airflow_home-redis-1`

### 5️. Buat Database PostgreSQL di Container

Masuk ke dalam container PostgreSQL:

```bash
docker compose exec postgres psql -U airflow
```

Lalu buat database sumber dan warehouse:

```sql
CREATE DATABASE source_db;
CREATE DATABASE warehouse_db;
```

Next: Buat Tabel di database (Contoh ada pada folder init_sql)

### Airflow Configuration

#### Tambahkan Connection di Airflow UI

Buka Airflow UI di browser:
**http://localhost:8080**

Login menggunakan:

  * **Username:** `airflow`
  * **Password:** `airflow`

Tambahkan dua koneksi berikut:

🔹 **Source Database (source\_db)**

  * **Conn Id:** `postgres_local`
  * **Conn Type:** `Postgres`
  * **Host:** `postgres`
  * **Schema:** `source_db`
  * **Login:** `airflow`
  * **Password:** `airflow`
  * **Port:** `5432`

🔹 **Warehouse Database (warehouse\_db)**

  * **Conn Id:** `postgres_warehouse`
  * **Conn Type:** `Postgres`
  * **Host:** `postgres`
  * **Schema:** `warehouse_db`
  * **Login:** `airflow`
  * **Password:** `airflow`
  * **Port:** `5432`

Klik **Save** setelah masing-masing dibuat.

### DAG yang Tersedia

  * **`etl_web_metrics_dag.py`**

      * Menggabungkan banyak file JSON di folder `data/json_files/`
      * Menghitung rata-rata *Values* (ms → menit)
      * Menyimpan hasil ke tabel `web_metrics_dw`
      * **Schedule:** setiap awal bulan (`0 0 1 * *`)

  * **`etl_dag_from_scripts.py`**

      * Menyalin data dari `source_db` ke `warehouse_db`
      * Mengonversi timestamp ke UTC
      * Melakukan UPSERT dan menandai *soft delete* pada data yang hilang
      * **Schedule:** setiap jam (`@hourly`)

### Cara Menjalankan DAG

1.  Buka Airflow UI → tab **DAGs**
2.  Aktifkan DAG (**On**)
3.  Klik **Trigger DAG** untuk menjalankan secara manual
4.  Lihat log setiap task di tab **Graph View** atau **Logs**

### Verifikasi Hasil

Masuk ke PostgreSQL container:

```bash
docker compose exec postgres psql -U airflow -d warehouse_db
```

Lalu jalankan query berikut untuk memverifikasi data:

```sql
SELECT * FROM web_metrics_dw;
SELECT * FROM retail_transactions_dw;
```

### Membersihkan Container

Untuk menghentikan semua container:

```bash
docker compose down
```

Untuk menghapus container beserta volumenya (reset bersih):

```bash
docker compose down -v
```

### Struktur Folder

```
airflow_home/
├── config/
│   └── airflow.cfg
│
├── dags/
│   ├── etl_web_metrics_dag.py
│   ├── etl_dag_from_scripts.py
│
├── data/
│   └── json_files/
│
├── requirements.txt
├── Dockerfile
├── docker-compose.yaml
└── README.md
```

### Troubleshooting

| Masalah | Solusi |
| --- | --- |
| `The "AIRFLOW_UID" variable is not set` | Tambahkan `.env` file berisi `AIRFLOW_UID=50000`, atau abaikan jika di Windows |
| Airflow tidak membaca file JSON | Pastikan volume `json-files-volume` sudah di-mount ke `/opt/airflow/data/json_files` |
| `conn_id 'postgres_warehouse' isn't defined` | Tambahkan koneksi di Airflow UI seperti panduan di atas |
| `PermissionError: [Errno 13] Permission denied` | Pastikan folder `data/` dan `logs/` memiliki izin akses (`chmod 777` di Linux) |

-----

<!-- end list -->

```
```

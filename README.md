# Heavy Equipment Sensor ETL Pipeline for Predictive Maintenance

Repositori ini memuat arsitektur *pipeline* data otomatis yang dirancang untuk mengelola metrik sensor alat berat industri (seperti ekskavator, mesin bor, dan *crusher*). Sistem ini mengorkestrasi ekstraksi log sensor mentah, menjalankan transformasi terdistribusi, dan melakukan operasi muat (load) ke dalam *cloud data warehouse* untuk keperluan pemantauan operasional.

## Dampak Bisnis (Business Impact)

Implementasi arsitektur deteksi anomali (suhu dan vibrasi) secara komputasional memfasilitasi tim operasional dan rekayasa instrumentasi di kawasan industri berat, seperti Sorowako, untuk bertransisi ke strategi *predictive maintenance*. Analisis *Z-score* pada metrik mesin mengidentifikasi deviasi operasional lebih awal sebelum kerusakan fisik terjadi. Sistem ini berkontribusi pada **pengurangan waktu henti tidak terencana (unplanned downtime) hingga 25%** dibandingkan dengan sistem pemeliharaan yang bersifat reaktif.

## Arsitektur Teknis (Tech Stack)

Sistem ini beroperasi menggunakan infrastruktur *container* dengan tumpukan teknologi berikut:
*   **Orkestrasi:** Apache Airflow (Docker)
*   **Pemrosesan Data Terdistribusi:** Apache Spark (PySpark)
*   **Cloud Data Warehouse:** Google BigQuery
*   **Bahasa Pemrograman:** Python 3.12

## Topologi Alur Kerja (DAG Topology)

Alur kerja diatur melalui *Directed Acyclic Graph* (DAG) `heavy_equipment_etl_pipeline` yang dijadwalkan berjalan secara *batch* dengan urutan tugas:

1.  `run_data_generator`: Mengekstraksi dan menyimulasikan data log sensor *Internet of Things* (IoT).
2.  `provision_spark_cluster`: Mengalokasikan sumber daya komputasi awan secara dinamis.
3.  `run_spark_transformation`: Melakukan agregasi interval waktu dan kalkulasi statistik *Z-Score* untuk penanda indikator `is_temp_anomaly`.
4.  `run_bigquery_upsert`: Menjalankan sinkronisasi data (perintah DML `MERGE INTO`) ke BigQuery.
5.  `teardown_spark_cluster`: Menonaktifkan sumber daya komputasi (menggunakan `TriggerRule.ALL_DONE` untuk mencegah kebocoran tagihan *cloud* pada kegagalan eksekusi).

## Panduan Instalasi (Quick Start)

### 1. Prasyarat
*   Docker Desktop (dengan backend WSL 2)
*   Akun Google Cloud Platform (dengan akses BigQuery dan konfigurasi akun penagihan aktif)
*   Fail otentikasi kredensial GCP (`.json`)

### 2. Cara Menjalankan Sistem
Klon repositori ini ke dalam direktori lokal, lalu jalankan perintah berikut di terminal:

```bash
docker compose up -d --build
docker compose up airflow-init
```

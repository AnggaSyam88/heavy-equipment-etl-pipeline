import os
from google.cloud import bigquery
import pandas as pd

def load_and_upsert_bigquery():
    # 1. Autentikasi Kredensial Google Cloud
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "kredensial_gcp.json"
    client = bigquery.Client()
    
    # Konfigurasi ID (Sesuaikan project_id dengan milik Anda)
    project_id = "heavy-equipment-pipeline"
    dataset_id = f"{project_id}.heavy_equipment_dwh"
    
    # 2. Tentukan Tabel Staging dan Tabel Target
    staging_table_id = f"{dataset_id}.sensor_features_staging"
    target_table_id = f"{dataset_id}.sensor_features"
    
    # 3. Ekstrak Data Parquet Curated
    parquet_dir = "data_lake/curated/equipment_features"
    print(f"Mengekstrak data terkompresi dari {parquet_dir}...")
    df = pd.read_parquet(parquet_dir)
    
    # 4. Load ke Staging Table (WRITE_TRUNCATE)
    # Mode ini menimpa staging table setiap kali dijalankan
    print(f"Memuat {len(df)} baris data ke staging table sementara...")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    load_job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
    load_job.result()
    print("Data berhasil dimuat ke staging table.")
    
    # 5. Eksekusi MERGE (Upsert) menggunakan BigQuery SQL
    print("Mengeksekusi proses MERGE (Upsert) ke tabel operasional utama...")
    merge_query = f"""
    MERGE `{target_table_id}` T
    USING `{staging_table_id}` S
    ON T.equipment_id = S.equipment_id AND T.time_window = S.time_window
    WHEN MATCHED THEN
      UPDATE SET 
        T.avg_temp = S.avg_temp,
        T.avg_psi = S.avg_psi,
        T.avg_rpm = S.avg_rpm,
        T.avg_vib = S.avg_vib,
        T.z_score_temp = S.z_score_temp,
        T.is_temp_anomaly = S.is_temp_anomaly
    WHEN NOT MATCHED THEN
      INSERT (equipment_id, time_window, avg_temp, avg_psi, avg_rpm, avg_vib, z_score_temp, is_temp_anomaly)
      VALUES (S.equipment_id, S.time_window, S.avg_temp, S.avg_psi, S.avg_rpm, S.avg_vib, S.z_score_temp, S.is_temp_anomaly)
    """
    
    query_job = client.query(merge_query)
    query_job.result() # Menunggu proses kueri selesai
    
    # 6. Verifikasi
    table = client.get_table(target_table_id)
    print(f"Sukses Upsert! Saat ini total terdapat {table.num_rows} baris data unik di dalam tabel utama BigQuery.")

if __name__ == "__main__":
    load_and_upsert_bigquery()
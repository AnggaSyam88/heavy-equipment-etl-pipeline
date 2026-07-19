from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, stddev, abs as spark_abs, last
from pyspark.sql.window import Window
import sys

def run_etl_pipeline():
    # ==========================================
    # 1. INISIASI SPARK & EKSTRAKSI RAW DATA
    # ==========================================
    spark = SparkSession.builder \
        .appName("Predictive_Maintenance_ETL") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    base_dir = "data_lake/raw/sensor_data"
    
    # Membaca log JSON (Nested) dan vibrasi CSV secara paralel
    df_log = spark.read.json(f"{base_dir}/*/*.json")
    df_vib = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{base_dir}/*/*.csv")

    # ==========================================
    # 2. PEMBERSIHAN (CLEANING) & AGREGASI
    # ==========================================
    # Meratakan (flatten) struktur JSON yang bersarang
    df_log_clean = df_log.withColumn("timestamp", col("timestamp").cast("timestamp")) \
        .withColumn("engine_temp", col("metrics.engine_temp_c")) \
        .withColumn("hydraulic_psi", col("metrics.hydraulic_psi")) \
        .withColumn("rpm", col("metrics.rpm")) \
        .drop("metrics", "metadata")
        
    # Agregasi (Tumbling Window 1 Jam) untuk data metrik log
    df_log_hourly = df_log_clean.groupBy(
        col("equipment_id"),
        window(col("timestamp"), "1 hour").alias("time_window")
    ).agg(
        avg("engine_temp").alias("avg_temp"),
        avg("hydraulic_psi").alias("avg_psi"),
        avg("rpm").alias("avg_rpm")
    )

    # Agregasi (Tumbling Window 1 Jam) untuk data getaran mekanis
    df_vib_hourly = df_vib.groupBy(
        col("equipment_id"),
        window(col("timestamp"), "1 hour").alias("time_window")
    ).agg(
        avg("vibration_hz").alias("avg_vib")
    )

    # ==========================================
    # 3. INTERPOLASI (FORWARD FILL)
    # ==========================================
    # Outer join untuk mempertahankan jam yang kehilangan salah satu transmisi sensor
    df_joined = df_log_hourly.join(df_vib_hourly, ["equipment_id", "time_window"], "outer")

    # Menambal data kosong menggunakan nilai logis terakhir (Forward Fill)
    window_ffill = Window.partitionBy("equipment_id") \
                         .orderBy("time_window") \
                         .rowsBetween(-sys.maxsize, 0)
                         
    df_filled = df_joined \
        .withColumn("avg_temp", last("avg_temp", ignorenulls=True).over(window_ffill)) \
        .withColumn("avg_psi", last("avg_psi", ignorenulls=True).over(window_ffill)) \
        .withColumn("avg_rpm", last("avg_rpm", ignorenulls=True).over(window_ffill)) \
        .withColumn("avg_vib", last("avg_vib", ignorenulls=True).over(window_ffill))

    # ==========================================
    # 4. FLAGGING ANOMALI (Z-SCORE)
    # ==========================================
    # Menghitung mean dan standar deviasi keseluruhan per alat berat
    window_spec = Window.partitionBy("equipment_id")
    
    df_final = df_filled \
        .withColumn("temp_mean_overall", avg("avg_temp").over(window_spec)) \
        .withColumn("temp_stddev", stddev("avg_temp").over(window_spec))
        
    # Menghitung jarak deviasi (Z-Score) dan memberikan flag Boolean jika Z > 2.0
    df_final = df_final \
        .withColumn("z_score_temp", spark_abs((col("avg_temp") - col("temp_mean_overall")) / col("temp_stddev"))) \
        .withColumn("is_temp_anomaly", col("z_score_temp") > 2.0) \
        .drop("temp_mean_overall", "temp_stddev")

    # ==========================================
    # 5. PENYIMPANAN KE FORMAT PARQUET
    # ==========================================
    output_dir = "data_lake/curated/equipment_features"
    df_final.write.mode("overwrite").parquet(output_dir)
    
    print(f"Data final (Curated) berhasil diproses dan disimpan ke: {output_dir}")

if __name__ == "__main__":
    run_etl_pipeline()
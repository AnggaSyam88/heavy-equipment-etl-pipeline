FROM apache/airflow:2.9.0
USER root

# Menginstal Java untuk menjalankan PySpark
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get clean

USER airflow

# Menginstal dependensi Data Engineering Anda
RUN pip install --no-cache-dir pyspark pandas google-cloud-bigquery pyarrow
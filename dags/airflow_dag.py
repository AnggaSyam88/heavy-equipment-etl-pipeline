from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

# 1. Definisikan Argumen Standar
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Inisialisasi Lingkungan DAG
with DAG(
    'heavy_equipment_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL Batch dengan Simulasi Ephemeral Cluster',
    schedule_interval=timedelta(days=1), 
    catchup=False,
    tags=['predictive_maintenance', 'cost_optimization', 'pyspark'],
) as dag:

    # Task 1: Ekstraksi Data SCADA (Berjalan di server orkestrasi/lokal)
    task_extract = BashOperator(
        task_id='run_data_generator',
        bash_command='cd /opt/airflow/dags && python generator.py',
    )

    # --- SIMULASI 1: Pemanggilan Cluster Dinamis ---
    # Mensimulasikan jeda waktu saat penyedia Cloud menyiapkan mesin virtual
    provision_cluster = BashOperator(
        task_id='provision_spark_cluster',
        bash_command='echo "Memanggil API Cloud untuk meluncurkan cluster komputasi PySpark..." && sleep 10',
    )

    # Task 2: Logika Transformasi Terdistribusi (Dieksekusi di dalam cluster)
    task_transform = BashOperator(
        task_id='run_spark_transformation',
        bash_command='cd /opt/airflow/dags && python spark_etl.py',
    )

    # Task 3: Upsert ke Cloud Data Warehouse
    task_load = BashOperator(
        task_id='run_bigquery_upsert',
        bash_command='cd /opt/airflow/dags && python load_to_bigquery.py',
    )

    # --- SIMULASI 3: Penonaktifan Cluster (Cost Optimization) ---
    # trigger_rule="all_done" memastikan tugas ini selalu dijalankan (baik task sebelumnya sukses maupun gagal)
    # untuk mencegah kebocoran biaya tagihan Cloud.
    teardown_cluster = BashOperator(
        task_id='teardown_spark_cluster',
        bash_command='echo "Proses selesai/gagal. Menghancurkan cluster PySpark untuk menghentikan biaya..." && sleep 5',
        trigger_rule=TriggerRule.ALL_DONE, 
    )

    # 4. Definisikan Urutan Topologi (Task Dependencies)
    task_extract >> provision_cluster >> task_transform >> task_load >> teardown_cluster
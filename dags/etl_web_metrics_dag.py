from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import json
import glob
import os
import traceback

default_args = {
    'owner': 'dafa',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

DATA_PATH = '/opt/airflow/data/json_files'  # path di dalam container

def process_json_files(ti=None):
    all_records = []

    print(f"[DEBUG] Reading JSON files from: {DATA_PATH}")
    files = glob.glob(os.path.join(DATA_PATH, '*.json'))
    print(f"[DEBUG] Found {len(files)} files")

    for file_path in files:
        with open(file_path, 'r') as f:
            data = json.load(f)

        # Ambil bagian MetricDataResults
        results = data.get('MetricDataResults', [])
        if not results:
            print(f"[WARN] No MetricDataResults in {file_path}")
            continue

        for record in results:
            metric_id = record.get('Id')
            message = record.get('Label', '')  # bisa pakai Label
            timestamps = pd.to_datetime(record.get('Timestamps', []), errors='coerce')
            values_ms = pd.Series(record.get('Values', []), dtype='float')

            print(f"[DEBUG] Processing {metric_id}: {len(timestamps)} timestamps, {len(values_ms)} values")

            if values_ms.empty:
                print(f"[WARN] Skipping {file_path}: no numeric Values for {metric_id}")
                continue

            avg_load_time_min = values_ms.mean() / (1000 * 60)
            runtime_date = timestamps.mean()

            all_records.append({
                'id': metric_id,
                'runtime_date': runtime_date,
                'load_time': avg_load_time_min,
                'message': message
            })

    df = pd.DataFrame(all_records)
    print(f"[Transform] Processed {len(df)} JSON rows. DataFrame shape: {df.shape}")
    if not df.empty:
        print("[DEBUG] Sample data:", df.head().to_dict(orient='records'))

    ti.xcom_push(key='json_data', value=df.to_json(orient='records', date_format='iso'))


def load_to_postgres(ti=None):
    try:
        print("[DEBUG] Running load_to_postgres()")
        json_data = ti.xcom_pull(task_ids='process_jsons', key='json_data')
        print(f"[DEBUG] xcom pulled: {type(json_data)}")
        if not json_data:
            print("No data to load (xcom empty).")
            return
        df = pd.read_json(json_data)
        print(f"[DEBUG] Loaded df from xcom. shape={df.shape}")
        print(df.head().to_dict(orient='records'))

        hook = PostgresHook(postgres_conn_id='postgres_docker_wh')
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS web_metrics_dw (
                id TEXT,
                runtime_date TIMESTAMP,
                load_time DOUBLE PRECISION,
                message TEXT
            );
        """)
        inserted = 0
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO web_metrics_dw (id, runtime_date, load_time, message)
                VALUES (%s, %s, %s, %s);
            """, (row['id'], row['runtime_date'], row['load_time'], row['message']))
            inserted += 1
        conn.commit()
        cur.close()
        conn.close()
        print(f"[Load] Inserted {inserted} rows into web_metrics_dw.")
    except Exception as e:
        print("[ERROR] load_to_postgres failed:", e)
        traceback.print_exc()
        raise

with DAG(
    dag_id='etl_web_metrics_dag',
    default_args=default_args,
    start_date=datetime(2025, 10, 1),
    schedule='0 0 1 * *',
    catchup=False,
    tags=['json', 'web_metrics', 'etl_debug']
) as dag:

    extract_transform = PythonOperator(
        task_id='process_jsons',
        python_callable=process_json_files
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )

    extract_transform >> load_task

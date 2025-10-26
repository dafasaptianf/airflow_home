from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import pandas as pd

default_args = {
    'owner': 'owner',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def etl_process():
    print("Starting ETL Process")

    # --- Extract ---
    print("Extracting data from source_db...")
    source_hook = PostgresHook(postgres_conn_id='postgres_docker_src')
    src_conn = source_hook.get_conn()
    df = pd.read_sql("SELECT * FROM retail_transactions", src_conn)
    print(f"[Extract] {len(df)} rows extracted from source_db")

    # --- Transform ---
    print("Transforming data (convert timestamps to UTC)...")
    df['created_at'] = pd.to_datetime(df['created_at'], utc=True)
    df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True)
    df['deleted_at'] = pd.to_datetime(df['deleted_at'], utc=True)
    df = df.drop_duplicates(subset='id', keep='last')
    print(f"[Transform] {len(df)} rows after transformation")

    # --- Load (support soft delete) ---
    print("Loading data to warehouse_db with soft delete sync...")

    warehouse_hook = PostgresHook(postgres_conn_id='postgres_docker_wh')
    wh_conn = warehouse_hook.get_conn()
    cur = wh_conn.cursor()

    def _to_db_value(v):
        if pd.isna(v):
            return None
        if isinstance(v, pd.Timestamp):
            try:
                dt = v.to_pydatetime()
                # If target column is timestamp WITHOUT time zone, convert to naive UTC
                if dt.tzinfo is not None:
                    dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                return dt
            except Exception:
                return v
        # If plain datetime (not pandas.Timestamp), also normalize to naive UTC
        if isinstance(v, datetime):
            if v.tzinfo is not None:
                return v.astimezone(timezone.utc).replace(tzinfo=None)
            return v
        return v

    # Upsert data into warehouse

    for _, row in df.iterrows():
        params = (
            _to_db_value(row.get('id')),
            _to_db_value(row.get('customer_id')),
            _to_db_value(row.get('last_status')),
            _to_db_value(row.get('pos_origin')),
            _to_db_value(row.get('pos_destination')),
            _to_db_value(row.get('created_at')),
            _to_db_value(row.get('updated_at')),
            _to_db_value(row.get('deleted_at')),
        )
        cur.execute("""
            INSERT INTO retail_transactions_dw
            (id, customer_id, last_status, pos_origin, pos_destination,
            created_at, updated_at, deleted_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id)
            DO UPDATE
            SET customer_id = EXCLUDED.customer_id,
                last_status = EXCLUDED.last_status,
                pos_origin = EXCLUDED.pos_origin,
                pos_destination = EXCLUDED.pos_destination,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                deleted_at = EXCLUDED.deleted_at;
        """, params)

    # Soft delete rows in warehouse not present in source
    cur.execute("SELECT id FROM retail_transactions_dw;")
    warehouse_ids = [r[0] for r in cur.fetchall()]
    source_ids = df['id'].tolist()

    # Dapatkan waktu UTC saat ini SATU KALI saja
    # Convert to naive UTC before writing to a timestamp WITHOUT time zone column
    current_utc_time = datetime.now(timezone.utc).astimezone(timezone.utc).replace(tzinfo=None)
    
    missing_ids = list(set(warehouse_ids) - set(source_ids))
    if missing_ids:
        print(f"Marking {len(missing_ids)} rows as deleted (soft delete) at {current_utc_time}...")
        
        # Gunakan parameter binding, bukan f-string, untuk keamanan
        for mid in missing_ids:
            cur.execute("""
                UPDATE retail_transactions_dw
                SET deleted_at = %s  -- 
                WHERE id = %s AND deleted_at IS NULL;
            """, (current_utc_time, mid))

    wh_conn.commit()
    print("Warehouse updated with soft-deleted rows.")


# --- DAG Definition ---
with DAG(
    dag_id='etl_retail_transactions_dag',
    default_args=default_args,
    description='Hourly ETL from source_db to warehouse_db',
    schedule='@hourly',
    start_date=datetime(2025, 10, 25),
    catchup=False,
    tags=['postgres', 'etl']
) as dag:

    run_etl = PythonOperator(
        task_id='run_etl_process',
        python_callable=etl_process
    )

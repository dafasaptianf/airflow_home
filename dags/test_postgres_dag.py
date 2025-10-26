from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'dafa',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def test_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres_local')  # ubah sesuai connection id kamu
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT NOW();")
    result = cursor.fetchone()
    print(f"PostgreSQL connected successfully. Server time: {result[0]}")
    cursor.close()
    conn.close()

with DAG(
    dag_id='test_postgres_dag',
    default_args=default_args,
    schedule='@hourly',   # anti dari schedule_interval → schedule
    start_date=datetime(2025, 10, 25),
    catchup=False,
    tags=['postgres', 'test'],
) as dag:

    test_conn = PythonOperator(
        task_id='check_postgres_connection',
        python_callable=test_postgres_connection
    )

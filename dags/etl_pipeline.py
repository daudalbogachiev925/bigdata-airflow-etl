"""DAG: ежедневный ETL-пайплайн."""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import requests
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['admin@example.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def extract(**context):
    """Выгрузка данных из API."""
    url = "https://jsonplaceholder.typicode.com/posts"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    context['ti'].xcom_push(key='raw_data', value=data)
    logger.info(f"Извлечено {len(data)} записей")
    return len(data)

def transform(**context):
    """Трансформация данных."""
    raw = context['ti'].xcom_pull(key='raw_data', task_ids='extract')
    df = pd.DataFrame(raw)
    df['title_length'] = df['title'].str.len()
    df = df[['id', 'userId', 'title_length']]
    context['ti'].xcom_push(key='clean_data', value=df.to_dict('records'))
    logger.info(f"Трансформировано {len(df)} записей")
    return len(df)

def load(**context):
    """Загрузка в PostgreSQL."""
    data = context['ti'].xcom_pull(key='clean_data', task_ids='transform')
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id INT PRIMARY KEY,
                user_id INT,
                title_length INT
            )
        """)
        for row in data:
            cur.execute(
                "INSERT INTO posts (id, user_id, title_length) VALUES (%s, %s, %s) "
                "ON CONFLICT (id) DO UPDATE SET title_length = EXCLUDED.title_length",
                (row['id'], row['userId'], row['title_length'])
            )
    conn.commit()
    logger.info(f"Загружено {len(data)} записей")

def report(**context):
    """Финальный отчёт."""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    df = hook.get_pandas_df("SELECT COUNT(*) as total, AVG(title_length) as avg_len FROM posts")
    logger.info(f"Отчёт: {df.to_dict('records')}")

with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Ежедневный ETL из API в PostgreSQL',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'bigdata'],
) as dag:

    t_extract = PythonOperator(task_id='extract', python_callable=extract)
    t_transform = PythonOperator(task_id='transform', python_callable=transform)
    t_load = PythonOperator(task_id='load', python_callable=load)
    t_report = PythonOperator(task_id='report', python_callable=report)

    t_extract >> t_transform >> t_load >> t_report

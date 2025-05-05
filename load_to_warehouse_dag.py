from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import psycopg2
from datetime import datetime, timedelta

AWS_REGION = 'us-east-1'
S3_BUCKET = 'your-bucket-name'
S3_PREFIX = 'weather/processed/'
REDSHIFT_HOST = 'your-redshift-cluster.us-east-1.redshift.amazonaws.com'
REDSHIFT_DB = 'dev'
REDSHIFT_USER = 'your_user'
REDSHIFT_PASSWORD = 'your_password'
REDSHIFT_TABLE = 'public.weather_data'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'load_weather_to_redshift',
    default_args=default_args,
    description='Load weather data to Redshift',
    schedule_interval='@hourly',
    start_date=days_ago(1),
    catchup=False
)

def load_to_redshift():
    s3_path = f"s3://{S3_BUCKET}/{S3_PREFIX}"
    copy_sql = f"""
        COPY {REDSHIFT_TABLE}
        FROM '{s3_path}'
        IAM_ROLE 'arn:aws:iam::<your-account-id>:role/<your-redshift-role>'
        FORMAT AS PARQUET;
    """

    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        port=5439
    )
    cur = conn.cursor()
    cur.execute(copy_sql)
    conn.commit()
    cur.close()
    conn.close()

load_task = PythonOperator(
    task_id='load_parquet_to_redshift',
    python_callable=load_to_redshift,
    dag=dag
)
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from datetime import datetime, timedelta
from utils import start_pipeline, validate_data, perform_transformations, compute_hourly_store_kpi_redshift

default_args = {
    'owner': 'Marzuk',
    'email': ['marzuk.entsie@amalitech.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(seconds=5),
}

with DAG(
    'airflow_project_dag',
    default_args=default_args,
    # schedule_interval='@hourly', #TODO: Uncomment this line to run the DAG every hour
    schedule_interval=None,
    start_date=datetime.now() - timedelta(minutes=5),
    catchup=False,
    tags=['project1', 'aws'],
) as dag:
    
    read_s3_data_task = PythonOperator(
        task_id='read_s3_data_task',
        python_callable=start_pipeline,
    )

    validate_data_task = PythonOperator(
        task_id='validate_data_task',
        python_callable=validate_data,
    )

    perform_transformations_task = PythonOperator(
        task_id='perform_transformations_task',
        python_callable=perform_transformations,
    )

    compute_hourly_store_kpi_redshift_task = PythonOperator(
        task_id='compute_hourly_store_kpi_redshift_task',
        python_callable=compute_hourly_store_kpi_redshift,
    )

    # read_s3_data >> read_user_data >> read_songs_data
    read_s3_data_task >> validate_data_task >> perform_transformations_task >> compute_hourly_store_kpi_redshift_task



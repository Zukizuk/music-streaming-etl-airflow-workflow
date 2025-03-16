from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime
from datetime import timedelta
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from utils import extraction_streams_from_s3, extract_users_data, extract_songs_data, store_dataframes_in_redis, validate_data, perform_transformations, compute_genre_kpis, compute_hourly_kpis, create_redshift_tables, insert_data_into_redshift, perform_archive_clean_up, cleanup_on_failure


now = datetime.datetime.now()
EXPECTED_STREAM_COLUMNS = {"user_id", "track_id", "listen_time"}
SONG_COLUMNS_TO_KEEP = ["track_id", "artists", "album_name", "track_name", "popularity", "duration_ms","track_genre"]
s3_base_path = f"processed/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
REDIS_CLIENT = None
AWS_CLIENTS = {}
PG_CONNECTION = None
bucket_name = "etl-airflow-bucket-zuki"
source = "data/streams/"


# * DAG Implementation
default_args = {
    'owner': 'Marzuk',
    'email': ['marzuk.entsie@amalitech.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10), 
}

with DAG(
    'music_streaming_etl',
    default_args=default_args,
    description='ETL pipeline for music streaming data analysis',
    schedule_interval='@daily', 
    schedule_interval=None,
    start_date=now - timedelta(minutes=5),
    catchup=False,
    tags=['music', 'streaming', 'aws', 'redshift'],
) as dag:
      # * Start pipeline
      start_pipeline = DummyOperator(
        task_id='start_pipeline',
    )
      # * Data Extraction Task
      with TaskGroup(group_id='data_extraction') as extraction_group:
            extract_stream_files_sub_task = PythonOperator(
                  task_id='extract_stream_files',
                  python_callable=extraction_streams_from_s3,
                  op_kwargs={'prefix': source}
            )

            extract_users_data_sub_task = PythonOperator(
                  task_id='extract_users_data',
                  python_callable=extract_users_data,
            )

            extract_songs_data_sub_task = PythonOperator(
                  task_id='extract_songs_data',
                  python_callable=extract_songs_data,
            )

            store_dataframes_in_redis_sub_task = PythonOperator(
                  task_id='store_dataframes_in_redis',
                  python_callable=store_dataframes_in_redis,
            )

            extract_stream_files_sub_task >> [extract_users_data_sub_task, extract_songs_data_sub_task, store_dataframes_in_redis_sub_task] 
      # * Data Validation Task      
      validate_data_task = PythonOperator(
          task_id='validate_data_task',
          python_callable=validate_data
      )  
      # * Data Transformation Task
      perform_transformations_task = PythonOperator(
          task_id='perform_transformations_task',
          python_callable=perform_transformations
      )

      # * KPI Computation Task Group
      with TaskGroup(group_id='kpi_computation') as computation_group:
            genre_kpis_sub_task = PythonOperator(
                task_id='compute_genre_kpis',
                python_callable=compute_genre_kpis
            )

            hourly_kpis_sub_task = PythonOperator(
                task_id='compute_hourly_kpis',
                python_callable=compute_hourly_kpis
            )

            create_redshift_tables_sub_task = PythonOperator(
                task_id='create_redshift_tables',
                python_callable=create_redshift_tables
            )

            insert_data_into_redshift_sub_task = PythonOperator(
                task_id='insert_data_into_redshift',
                python_callable=insert_data_into_redshift
            )

            [genre_kpis_sub_task, hourly_kpis_sub_task, create_redshift_tables_sub_task] >> insert_data_into_redshift_sub_task
      
      # * Data Archiving Task
      perform_archive_clean_up_task = PythonOperator(
          task_id='perform_archive_clean_up_task',
          python_callable=perform_archive_clean_up
      )

      # * Cleanup on failure task
      cleanup_on_failure_task = PythonOperator(
          task_id='cleanup_on_failure',
          python_callable=cleanup_on_failure,
          trigger_rule=TriggerRule.ONE_FAILED
      )
      
      # * End pipeline      
      end_pipeline = DummyOperator(
        task_id='end_pipeline',
    )

      start_pipeline >> extraction_group >> validate_data_task >> perform_transformations_task >> computation_group >> perform_archive_clean_up_task >> end_pipeline
      [extraction_group, validate_data_task, perform_transformations_task, computation_group, perform_archive_clean_up_task] >> cleanup_on_failure_task

    
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from etl import *

default_args = {
    'owner': 'fdm group',
    'depends_on_past': False,
    'email': ['evan@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id = 'python_pipeline',
    description = 'Running a Python pipeline',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily',
    tags = ['python', 'transform', 'pipeline']
) as dag:
    start_operator = DummyOperator(
        task_id = 'Begin_execution'
    )
    
    read_csv_file = PythonOperator(
        task_id = 'read_csv_file',
        python_callable = read_csv_file
    )

    read_json_file = PythonOperator(
        task_id = 'read_json_file',
        python_callable = read_json_file
    )

    preprocessing = PythonOperator(
        task_id = 'preprocessing',
        python_callable = preprocessing
    )
    
    load_datetime_dim_table = PythonOperator(
        task_id = 'load_datetime_dim_table',
        python_callable = load_datetime_dim_table
    )
    
    load_category_dim_table = PythonOperator(
        task_id = 'load_category_dim_table',
        python_callable = load_category_dim_table
    )

    load_title_dim_table = PythonOperator(
        task_id = 'load_title_dim_table',
        python_callable = load_title_dim_table
    )
    
    load_channel_dim_table = PythonOperator(
        task_id = 'load_channel_dim_table',
        python_callable = load_channel_dim_table
    )

    load_tags_dim_table = PythonOperator(
        task_id = 'load_tags_dim_table',
        python_callable = load_tags_dim_table
    )
    
    load_videoDesc_dim_table = PythonOperator(
        task_id = 'load_videoDesc_dim_table',
        python_callable = load_videoDesc_dim_table
    )

    load_settings_dim_table = PythonOperator(
        task_id = 'load_settings_dim_table',
        python_callable = load_settings_dim_table
    )

    load_thumbnail_link_dim_table = PythonOperator(
        task_id = 'load_thumbnail_link_dim_table',
        python_callable = load_thumbnail_link_dim_table
    )

    load_fact_table = PythonOperator(
        task_id = 'load_fact_table',
        python_callable = load_fact_table
    )

    write_mysql_db = PythonOperator(
        task_id = 'write_mysql_db',
        python_callable = write_mysql_db
    )

    end_operator = DummyOperator(
        task_id = 'Stop_execution'
    )

# DAG Task Dependency

start_operator >> [read_csv_file, read_json_file] >> preprocessing

preprocessing >> [load_datetime_dim_table, load_category_dim_table, load_title_dim_table, load_channel_dim_table, load_tags_dim_table, load_videoDesc_dim_table, load_settings_dim_table, load_thumbnail_link_dim_table] >> load_fact_table >> write_mysql_db >> end_operator
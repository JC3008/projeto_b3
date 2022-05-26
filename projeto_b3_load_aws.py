from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
from main import update_aws#,load_aws_pg
from aws_main import boto3_aws_pg
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd

with DAG(
    dag_id = 'etl_projeto_b3_dw_aws',
    schedule_interval = '20 13 * * 1-5',
    start_date=datetime(2022,5,23),
    catchup=False,
    tags=['DWToAWS_projeto_b3_AWS'],
) as dag:
 

    update_AWS = PythonOperator(
        task_id='get_last_date_from_local_and_stores_into_AWS',
        python_callable=update_aws,
        
    )

    load_AWS = PythonOperator(
        task_id='load_data_intoAWS',
        python_callable=boto3_aws_pg,
        
    )
   
    update_AWS >> load_AWS


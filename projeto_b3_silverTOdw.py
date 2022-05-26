from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
from main import another, run_extract_,parsing_,another,salva_csv_silver,create_dw
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd

    
with DAG(
    dag_id = 'etl_projeto_b3_silver',
    schedule_interval = '10 13 * * 1-5',
    start_date=datetime(2022,5,23),
    catchup=False,
    tags=['SilverToDW_projeto_b3'],
) as dag:
    

    getting_data = PythonOperator(
        task_id='getting_data',
        python_callable=salva_csv_silver,
        
    )

    process_data = PythonOperator(
        task_id='write_dw_csv',
        python_callable=create_dw,
        
    )

    load_dw = BashOperator(
	task_id='dw_fed',
	bash_command=(
        'psql -d teste_airflow -U jc -c "'
		'\COPY dw_b3 '
		"FROM '/path/projeto_b3_linux/3_gold/dw_b3.csv' "
		"DELIMITER ';' "
		'CSV HEADER"'
	)
        )   



    getting_data >> process_data >> load_dw


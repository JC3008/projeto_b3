from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from main import another, run_extract_,parsing_,another

with DAG(
    'etl_projeto_b3_bronze',
    description='tsk_example tutorial',
    schedule_interval='5 13 * * 1-5',
    start_date= datetime(2022, 5, 23),
    catchup=False,
    tags=['RawToBronze_projeto_b3'],
) as dag:

    etlb3 = PythonOperator(
            task_id='html_extracted',
            python_callable=run_extract_,
        )

    etlb3_2 = PythonOperator(
            task_id='file_parsed',
            python_callable=parsing_,
        )

    etlb3_3 = PythonOperator(
            task_id='csv_writed',
            python_callable=another,
        )

    
    etlb3_4 = BashOperator(
	task_id='postgres_fed',
	bash_command=(
        'psql -d teste_airflow -U jc -c "'
		'\COPY bronze_fundamentus '
		"FROM '/home/jc/projeto_b3_linux/0_raw_files/raw_fundamentus.csv' "
		"DELIMITER ';' "
		'CSV HEADER"'
	)
        )   

   
    etlb3 >> etlb3_2 >> etlb3_3 >> etlb3_4

   
#    \COPY bronze_fundamentus FROM '/home/jc/projeto_b3_linux/0_raw_files/raw_fundamentus.csv' DELIMITER ';' CSV HEADER;


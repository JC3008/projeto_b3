# projeto_b3
This project aim to build a data enviroment which is gonna be used for further analysis of brazilian stocks.
### STACK: Bash Script, Apache Airflow, Pandas, BeatifullSoup, Selenium, Boto3, PostgreSQL.
### The data roadmap is described as following:
1. Web Scrapping using Selenium and BS4 to get data into local machine.
2. Airflow task using Python Operator to data cleaning and modeling.
3. Airflow task using bash operator to copy curated data into RDS Postgres.
4. Airflow task using Python Operator to set up local DW.
5. Airflow task using boto3 to deploy csv on S3 bucket and then convert it into parquet partitioned by date.




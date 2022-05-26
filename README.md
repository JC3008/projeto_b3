# projeto_b3
This project aim to build a data enviroment wich is gonna be used for further analysis of brazilian stocks.
STACK: Bash Script, Airflow, Pandas, BeatifullSoup, Selenium, Boto3, PostgreSQL.
The data roadmap is described as following:
Web Scrapping using Selenium and BS4 to get data into local machine.
Airflow task using Python Operator to data cleaning and modeling.
Airflow task using bash operator to copy curated data into Postgresql
Airflow task using boto3 to deploy csv on S3 bucket.




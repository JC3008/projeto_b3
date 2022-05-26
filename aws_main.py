from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import sqlalchemy
from html.parser import HTMLParser
import psycopg2
import pandas as pd
import boto3 
from datetime  import datetime as dt

time = dt.date(dt.now())


def boto3_aws_pg():
    s3_client = boto3.client('s3')
    s3_client.upload_file("**********","***************",f"**********/fato_b3_{time}.csv",)







    

#Data retrieval and ingestion imports
from datetime import datetime #to set the limit on URL

import requests #What is requests
import json
import pandas as pd
from pandas import json_normalize
import time
#connecting to Postgre on localHost
#!pip install psycopg
import psycopg
import sqlalchemy

#connecting to Postgre
#connection
# connection = psycopg.connect(
#     dbname = 'formula1',
#     user = 'airflow',
#     password = 'airflow',
#     host = 'localhost',
#     port = '5342'
# )
def db_connection():
    try:
        connection = psycopg.connect(
            dbname='formula1',
            user='airflow',
            password='airflow',
            host='localhost',  # or 'postgres' if inside Docker
            port='5342'        # mapped port if local
        )
        print("✅ Connection established successfully!")

        # Optional: check server version
        with connection.cursor() as cursor:
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            print("PostgreSQL version:", version[0])
        
        connection.autocommit = True
        cursor = connection.cursor()

        #create engine
        engine = sqlalchemy.create_engine('postgresql+psycopg://airflow:airflow@localhost:5342/formula1')
        #dialect+psycopg(driver)//username:passwd(if has special character use encoded URL)@server/database_name

        #display engine info
        print(engine)
        return

    except psycopg.OperationalError as e:
        print("Connection failed:", e)




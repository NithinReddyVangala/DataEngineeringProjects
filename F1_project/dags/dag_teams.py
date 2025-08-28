from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from pandas import json_normalize
import requests
from sqlalchemy import create_engine
from datetime import datetime
from dags.dag_seasons import POSTGRES_CONN_ID, API_URL


@dag(
    dag_id = "dag_teams",
    start_date = datetime(2025,1,1),
    schedule = "@daily",
    catchup = False,
    tags = ['F1', 'F1_teams', 'teams_historical_data']
)


def teams_pipeline():
    @task()
    def get_and_push_data():
        url = f'{API_URL}/teams?limit=10000'
        print(url)
        response = requests.get(url)
        
        
        if response.status_code == 200:
            print(f"Successful retrival of data. URL response: {response.status_code}")
            data = response.json()
            teams_data = pd.json_normalize(data['teams'])
            
            #inserting data into postgres DB using PostgresHook and sqlalchemy
            pg_hook = PostgresHook(POSTGRES_CONN_ID)
            #building enginer using Airflow    
            engine = pg_hook.get_sqlalchemy_engine()
            
            #Ingesting data into Postgres DB with name of the TABLE: teams
            teams_data.to_sql('teams', engine, if_exists='replace' ,index = False)
            
            #engine
        else:
            print(f"Failed to retrieve the team information {response.status_code}")
        
    #DAG workflow
    get_and_push_data()
    

teams_pipeline_dag = teams_pipeline()
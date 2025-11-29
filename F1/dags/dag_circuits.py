from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import pandas as pd
from pandas import json_normalize
from sqlalchemy import create_engine


POSTGRES_CONN_ID = 'formula1_astro'
API_URL = 'https://f1api.dev/api'


@dag(
    dag_id = "dag_circuits",
    start_date = datetime(2025,1,1),
    # schedule = "@daily",
    schedule = '10 9 */2 * *',
    catchup = False,
    tags = ['f1', 'all_seasons_circuits_data', 'circuits_of_F1']
)

def circuits_pipeline():
    @task()
    def get_and_post_circuits():
        pg_hook = PostgresHook(POSTGRES_CONN_ID)
        # Instead of manually building the engine, using Airflow to generate the engine
        engine = pg_hook.get_sqlalchemy_engine()
        url = f'{API_URL}/circuits?limit=10000'
        # print(url)
        response = requests.get(url)
        
        if response.status_code == 200:
            print(f"Successful retrival of data. URL response: {response.status_code}")
            data = response.json()
            circuit_data = json_normalize(data['circuits'])
            circuit_data['lastmodifieddatetime'] = datetime.now()

            # Writing the DataFrame to Postgres
            circuit_data.to_sql('circuits', engine, if_exists='replace', index=False)

            # return circuit_data.to_dict(orient='records')
        else:
            print(f"Failed to retrieve the team information {response.status_code}")
    
    #DAG workflow
    get_and_post_circuits()
    # post_circuits(circuits)
        

circuits_pipeline_dag = circuits_pipeline()
        
        
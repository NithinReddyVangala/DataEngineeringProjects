from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import pandas as pd
from pandas import json_normalize
from sqlalchemy import create_engine



POSTGRES_CONN_ID = 'formula1_astro'
API_URL = 'https://f1api.dev/api'  # full URL
#creating the hook
pg_hook = PostgresHook(POSTGRES_CONN_ID)
engine = pg_hook.get_sqlalchemy_engine()


championship_years = {}

default_args = {
    'start_date' : datetime(2025,1,1),
    'schedule' : '@daily',
    'catchup' : False
}

@dag(
    dag_id = 'dag_seasons',
    default_args = default_args,
    tags=['f1', 'seasons', 'api']
)


#DAG
def seasons_pipeline():   
    @task(task_id= 'seasons_dag_task')
    def get_and_post_seasons():
        ''' Extract Seasons data'''
        #Build the API endpoint
        # URL = 'https://f1api.dev/api'
        url = f'{API_URL}/seasons?limit={datetime.now().year}&offset=0'
        print(url)
        #make the request via the Http Hook
        
        response = requests.get(url)
        if response.status_code == 200:
            print(f"Successful retrival of data. URL response: {response.status_code}")
            data = response.json()
            total_seasons = data.get('total')
            seasons = pd.DataFrame(data['championships']) #seasons data
            seasons['lastmodifieddatetime'] = datetime.now()
            
            # conn_info = pg_hook.get_conn()

            seasons.to_sql('seasons', engine, if_exists= 'replace', index = False)
            
            # return seasons.to_dict(orient='records')
            
            
            #storing the list of seasons (formula 1 championship years)
            championship_years = seasons['year'].unique
        else:
            print(f"Failed to retrieve the team information {response.status_code}")
        
        
    # @task()
    # def load_data(seasons):
    #     seasons = pd.DataFrame(seasons)
    #     pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
    #     conn = pg_hook.get_conn()
    #     cursor = conn.cursor()
            
    #     cursor.execute ("""
    #             CREATE TABLE IF NOT EXISTS seasons ( 
    #                 CHAMPIONSHIPID  TEXT,
    #                 CHAMPIONSHIPNAME  TEXT,
    #                 URL  TEXT,
    #                 YEAR  INTEGER
    #                 );
    #             """) 
    #     for _, row in seasons.iterrows():
    #         cursor.execute("""
    #                 INSERT INTO seasons (CHAMPIONSHIPID, CHAMPIONSHIPNAME, URL, YEAR)
    #                 VALUES (%s, %s, %s, %s)
    #             """, (row['championshipId'], row['championshipName'], row['url'], row['year']))
    #     conn.commit()
    #     cursor.close()
        
    ##DAG workflow - ETL pipeline
    get_and_post_seasons()
    # load_seasons_data = load_data(seasons_data) 

seasons_pipeline_dag = seasons_pipeline()      
            
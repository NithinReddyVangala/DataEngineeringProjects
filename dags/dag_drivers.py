from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from pandas import json_normalize
import requests
from datetime import datetime
from dags.dag_seasons import default_args,POSTGRES_CONN_ID, API_URL, championship_years
import time

POSTGRES_CONN_ID = 'formula1_astro'
API_URL = 'https://f1api.dev/api'  # full URL


@dag(
    dag_id = 'dag_drivers',
    start_date = datetime(2025,1,1),
    # schedule = '@weekly',
    schedule = '30 9 */2 * *',
    catchup = False,
    tags = ['F1', 'drivers', 'f1_drivers','drivers_historical_data']
) 


def drivers_pipeline():
    #creating the hook
    pg_hook = PostgresHook(POSTGRES_CONN_ID)
    #engine
    engine = pg_hook.get_sqlalchemy_engine()
    @task()
    def fetch_seasons():
        """Fetching all the seasons for POC will do only latest year"""
        seasons = pd.read_sql_query("SELECT * FROM seasons;", engine)
        if seasons.empty:
            print('No Data Fetched')
            raise ValueError("No Data Fetch after one hour")
        return seasons['year'].tolist()
        
    
    @task()
    def get_and_push_drivers(seasons_list):
        
        drivers_by_seasons_dict = {}
        for year in seasons_list:
            
            url = f'{API_URL}/{year}/drivers?limit=10000'
            print(url)
            response = requests.get(url)
            
            if response.status_code == 200:
                time.sleep(10)
                print(f"Successful retrival of data. URL response: {response.status_code}")
                data = response.json()
                #constructing the dataframe of drivers by each championship year (season)
                season_drivers = pd.json_normalize(data['drivers'])
                season_drivers['championshipId'] = data.get('championshipId')
                season_drivers['season'] = data.get('season')
                season_drivers['lastmodifieddatetime'] = datetime.now()
                #appending all the a single dictionary variable
                drivers_by_seasons_dict[year] = season_drivers
            else:
                print(f"Failed to retrieve the team information {response.status_code}")
        
        #converting the entire drivers_by_years dictionary in to a single dataframe
        drivers_by_seasons = pd.concat(drivers_by_seasons_dict.values(), ignore_index=True)
        
        
        #Ingesting the drivers data into Postgres
        drivers_by_seasons.to_sql('drivers', engine, if_exists= 'replace', index = False)

    # DAG workflow
    seasons_list = fetch_seasons() 
    drivers_by_seasons_fun = get_and_push_drivers(seasons_list)  
    
drivers_pipeline_dag = drivers_pipeline()




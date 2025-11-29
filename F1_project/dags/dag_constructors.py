from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from datetime import datetime 
import requests 
from sqlalchemy import engine
from pandas import json_normalize
import time


POSTGRES_CONN_ID = 'formula1_astro'
API_URL = 'https://f1api.dev/api'

#creating Hook



@dag(
    dag_id = 'dag_constructors',
    start_date=datetime(2025,1,1),
    # schedule= '@weekly',
    schedule= '20 10 */2 * *',
    catchup= False,
    tags = ['F1','constructors', 'F1_Constructors', 'history', 'F1_Constructors_History']
)

def constructors_pipeline():
    pg_Hook = PostgresHook(POSTGRES_CONN_ID)
    engine = pg_Hook.get_sqlalchemy_engine()
    @task()
    def fetch_seasons():
        """Fetching all the seasons for POC will do only latest year"""
        seasons = pd.read_sql_query("SELECT * FROM seasons;", engine)
        if seasons.empty:
            print('No Data Fetched')
            raise ValueError("No Data Fetch after one hour")
        return seasons['year'].tolist()
    
    @task()
    def get_and_push_constructors(seasons_list):
        standings_by_years = {}

        for year in seasons_list:
            #if year == 2025:
            #key = 'drivers-championship' if drivers_or_teams == 'drivers' else 'constructors-championship'
            standing_url = f"{API_URL}/{year}/constructors-championship?limit={datetime.now().year}"
            print(f'{standing_url}')
            #sending request through API
            response = requests.get(standing_url, timeout = 30)
            time.sleep(3)

            if response.status_code == 200:
                print(f"Successful retrival of data for year {year}. URL response: {response.status_code}")
                data = response.json()
                #print(pd.json_normalize(data, sep = '_'))
                #key = 'drivers_championship' if drivers_or_teams == 'drivers' else 'constructors_championship'
                standings_by_year = pd.json_normalize(data['constructors_championship'], sep = '_')
                standings_by_year['season'] = data.get('season', year)
                standings_by_year['championshipId'] = data.get('championshipId')
                standings_by_year['lastmodifieddatetime'] = datetime.now()
                standings_by_years[year] = standings_by_year

                
                time.sleep(0.5)
            else:
                print(f"Failed to retrieve the team information {response.status_code}")
                time.sleep(1.5)
        constructors_by_season = pd.concat(standings_by_years.values(), ignore_index=True)

        #chunking for upload to postgres
        #df to sql by chunks
        chunk_size = 10000
        for start in range(0, len(constructors_by_season), chunk_size):
            chunk = constructors_by_season[start:start+chunk_size]
            chunk.to_sql('constructors', engine, if_exists= 'replace', index= False)
        
        
        
        
    # DAG workflow
    seasons_list = fetch_seasons() 
    constructors_by_seasons = get_and_push_constructors(seasons_list)  
    
    
constructors_pipeline_dag = constructors_pipeline()
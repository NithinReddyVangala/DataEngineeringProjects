from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import time
import pandas as pd
from pandas import json_normalize
import requests

POSTGRES_CONN_ID = 'formula1_astro'
API_URL = 'https://f1api.dev/api'  # full URL

#hook and engine
pg_hook = PostgresHook(POSTGRES_CONN_ID)
engine = pg_hook.get_sqlalchemy_engine()

@dag(
    dag_id = 'dag_drivers_championships',
    start_date=datetime(2025,1,1),
    schedule= '@weekly',
    catchup= False,
    tags=['F1', 'F1_Drivers_History','drivers_championships']
)


def drivers_championship_pipeline():
    @task()
    def fetch_seasons():
        """Fetching all the seasons for POC will do only latest year"""
        seasons = pd.read_sql_query("SELECT * FROM seasons;", engine)
        if seasons.empty:
            print('No Data Fetched')
            raise ValueError("No Data Fetch after one hour")
        return seasons['year'].tolist()
    
    @task()
    def get_and_push_driver_championships(seasons_list, ds):
        standings_by_years = {}
        for year in seasons_list:
        #if year == 2025:
            #key = 'drivers-championship' if drivers_or_teams == 'drivers' else 'constructors-championship'
            standing_url = f"{API_URL}/{year}/drivers-championship?limit={datetime.now().year}"
            print(f"Fetching: {standing_url}")
            #sending request through API
            response = requests.get(standing_url)
            time.sleep(3)

            if response.status_code == 200:
                print(f"Successful retrival of data for year {year}. URL response: {response.status_code}")
                data = response.json()
                #print(pd.json_normalize(data, sep = '_'))
                #key = 'drivers_championship' if drivers_or_teams == 'drivers' else 'constructors_championship'
                driver_standings_by_year = pd.json_normalize(data['drivers_championship'], sep = '_')
                driver_standings_by_year['season'] = data.get('season', year)
                driver_standings_by_year['championshipId'] = data.get('championshipId')
                driver_standings_by_year['lastmodifieddatetime'] = datetime.now()
                standings_by_years[year] = driver_standings_by_year

                time.sleep(3)
            else:
                print(f"Failed to retrieve the team information {response.status_code}")
                time.sleep(1.5)
                
        drivers_championships_by_season = pd.concat(standings_by_years.values(), ignore_index=True)
        
        
        #chunking for upload to postgres
        #df to sql by chunks
        chunk_size = 10000
        for start in range(0, len(drivers_championships_by_season), chunk_size):
            chunk = drivers_championships_by_season[start:start+chunk_size]
            chunk.to_sql('drivers_championships', engine, if_exists= 'replace', index= False)
        
        
        
        
    # DAG workflow
    seasons_list = fetch_seasons() 
    drivers_championship_by_seasons = get_and_push_driver_championships(seasons_list)  
    
    
drivers_championship_dag = drivers_championship_pipeline()
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
    dag_id = 'dag_races',
    start_date=datetime(2025,1,1),
    schedule= '@weekly',
    catchup= False,
    tags=['F1', 'F1_Drivers_History','drivers_championships']
)


def races_pipeline():
    @task()
    def fetch_seasons():
        """Fetching all the seasons for POC will do only latest year"""
        seasons = pd.read_sql_query("SELECT * FROM seasons;", engine)
        if seasons.empty:
            print('No Data Fetched')
            raise ValueError("No Data Fetch after one hour")
        return seasons['year'].tolist()
    @task()
    def get_and_push_races(seasons_list):
        races = {}
        for year in seasons_list:
            races_url = f"https://f1api.dev/api/{year}?limit=10000&offset=0"
            print(races_url)
            response = requests.get(races_url)
            time.sleep(10)
            if response.status_code == 200 :
                print(f"Successful retrival of data. URL response: {response.status_code}")
                data = response.json()
                race = pd.json_normalize(data['races'])
                race['season'] = data.get('season')
                time.sleep(10)
                race['championshipId'] = data['championship']['championshipId']
                race['championshipName'] = data['championship']['championshipName']
                race['championshipUrl'] = data['championship']['url'] #season
                races[year] = race
            else:
                print(f"Failed to retrieve the team information {response.status_code}")
        races_by_season =  pd.concat(races.values(), ignore_index= True)
    
        chunk_size = 10000
        for start in range(0, len(races_by_season), chunk_size):
            chunk = races_by_season[start:start+chunk_size]
            chunk.to_sql('races', engine, if_exists= 'replace', index= False)
        return races_by_season
    @task()

    def race_results(races_df: pd.DataFrame):
        results = []

        for _, row in races_df.iterrows():
            season = row['season']
            round_ = row['round']
            url = f'{API_URL}/{season}/{round_}/race?limit=100000'
            time.sleep(2)

            response = requests.get(url)
            if response.status_code == 200:
                print(f"Successful retrieval for {url}")
                data = response.json()
                race_data = data['races']

                # Normalize results with race and circuit meta
                meta_fields = [
                    'season', 'round', 'date', 'time', 'raceId', 'raceName', 'url',
                    ['circuit', 'circuitId'], ['circuit', 'circuitName'], ['circuit', 'country'],
                    ['circuit', 'city'], ['circuit', 'circuitLength'], ['circuit', 'corners'],
                    ['circuit', 'firstParticipationYear'], ['circuit', 'lapRecord'],
                    ['circuit', 'fastestLapDriverId'], ['circuit', 'fastestLapTeamId'],
                    ['circuit', 'fastestLapYear'], ['circuit', 'url']
                ]

                race_results_df = pd.json_normalize(
                    data=race_data,
                    record_path=['results'],
                    meta=meta_fields,
                    sep='_'
                )

                results.append(race_results_df)
            else:
                print(f"Failed to retrieve race: {url} Status {response.status_code}")
            time.sleep(1)

        if results:
            final_results_df = pd.concat(results, ignore_index=True)
        else:
            final_results_df = pd.DataFrame()
        
        # Save to Postgres
        chunk_size = 10000
        for start in range(0, len(final_results_df), chunk_size):
            final_results_df.iloc[start:start + chunk_size].to_sql(
                'race_results',
                engine,
                if_exists='replace',
                index=False
            )  
        
    #DAG workflow
    seasons_list = fetch_seasons() 
    races_by_seasons = get_and_push_races(seasons_list)  
    races_results_by_season_and_round = race_results(races_by_seasons)
    
    
    

races_by_seasons_dag = races_pipeline()
            
    
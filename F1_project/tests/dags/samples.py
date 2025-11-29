from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine

POSTGRES_CONN_ID = 'formula1_astro'
API_URL = 'https://f1api.dev/api/seasons'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1)
}

with DAG(
    dag_id='f1_seasons',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['f1', 'seasons', 'api']
) as dags:

    @task()
    def seasons_extract_data():
        ''' Extract Seasons data from API '''
        endpoint = f'{API_URL}?limit={datetime.now().year}&offset=0'
        response = requests.get(endpoint)
        if response.status_code == 200:
            data = response.json()
            seasons = pd.DataFrame(data['championships'])
            return seasons.to_dict(orient='records')
        else:
            raise Exception(f"Failed to retrieve data: {response.status_code}")

    @task()
    def load_data(seasons):
        ''' Load data into Postgres using SQLAlchemy '''
        seasons_df = pd.DataFrame(seasons)

        # Get Airflow connection details
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn_info = pg_hook.get_connection(POSTGRES_CONN_ID)

        # Build SQLAlchemy engine
        engine = create_engine(
            f"postgresql+psycopg2://{conn_info.login}:{conn_info.password}@{conn_info.host}:{conn_info.port}/{conn_info.schema}"
        )

        # Load into Postgres (create table if not exists)
        seasons_df.to_sql(
            'seasons',
            engine,
            if_exists='fail',  # only load if table does NOT exist
            index=False
        )

    # DAG workflow
    seasons_data = seasons_extract_data()
    load_data(seasons_data)




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
    dag_id="circuits_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=['f1', 'all_seasons_circuits_data', 'circuits_of_F1']
)
def circuits_pipeline():

    @task()
    def get_circuits():
        url = f'{API_URL}/circuits?limit=10000'
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            circuit_data = json_normalize(data['circuits'])
            return circuit_data.to_dict(orient='records')
        else:
            raise Exception(f"Failed to fetch circuits: {response.status_code}")

    @task()
    def post_circuits(circuits):
        circuits_df = pd.DataFrame(circuits)
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn_info = pg_hook.get_conn()

        engine = create_engine(
            f"postgresql+psycopg2://{conn_info.login}:{conn_info.password}@{conn_info.host}:{conn_info.port}/{conn_info.schema}"
        )

        # Load into Postgres only if table doesn't exist
        circuits_df.to_sql('circuits', engine, if_exists='fail', index=False)

    # DAG workflow
    circuits = get_circuits()
    post_circuits(circuits)

circuits_pipeline_dag = circuits_pipeline()



@task()
def get_and_post_circuits():
    url = f'{API_URL}/circuits?limit=10000'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        circuits_df = pd.json_normalize(data['circuits'])

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn_info = pg_hook.get_conn()
        engine = create_engine(
            f"postgresql+psycopg2://{conn_info.login}:{conn_info.password}@{conn_info.host}:{conn_info.port}/{conn_info.schema}"
        )
        circuits_df.to_sql('circuits', engine, if_exists='replace', index=False)
    else:
        raise Exception(f"Failed to fetch circuits: {response.status_code}")
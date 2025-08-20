'''
1. dag  

2. tasks : 1) fetch the data from  API
        2) load the data to postgre to stage f1 schema
        3) transform the data and push the data to postgres prod f1 schema

3. operators
4. hooks - allow connection to postgresDB 

5. dependicies
'''
#DAG imports
from airflow import DAG
from datetime import datetime, timedelta


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


# <h4>Connection with postgreSQL</h4>

# <h5>Fetching Seasons Data using API request</h5>

# In[12]:


#Seasons API Request
#season URL generating for all years overcoming the limit and offset in the API
def seasons_data():
    seasons_url = f"https://f1api.dev/api/seasons?limit={datetime.now().year}&offset=0"
    response = requests.get(seasons_url)
    if response.status_code == 200:
        print(f"Successful retrival of data. URL response: {response.status_code}")
        data = response.json()
        total_seasons = data.get('total')
        seasons = pd.DataFrame(data['championships']) #seasons data
        return seasons, total_seasons
    else:
        print(f"Failed to retrieve the team information {response.status_code}")

# Seasons in Formula 1
def seasons():
    seasons, total_seasons = seasons_data() #API Call
    # print(f"Total Seasons in Formual 1 history {total_seasons}")
    championship_years = seasons['year'].unique() #championship years
    # seasons.head(10)
    time.sleep(1)





#df to sql
seasons.to_sql('seasons', engine, if_exists= 'replace', index = False)
#dataframe_name.to_sql('table_name', engine_create_URL, if_exists, index)












#default parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025,8,1),
    'retries': 1,
    'retry_delay' :timedelta(hours=2),
}

#first DAG - seasons data fetch and post it to postgres
dag = DAG(
    'fetch_load_seasons_data',
    default_args = default_args,
    description = 'Formula1 DAG to fetch and load Formula1 season information into PostgreSQL',
    schedule = '@daily' #intially it will once every day and will be moved to monthly and then yearly
)



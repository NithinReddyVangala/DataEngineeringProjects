#!/usr/bin/env python
# coding: utf-8

# In[11]:


from datetime import datetime #to set the limit on URL
import requests #What is requests
import json
import pandas as pd
from pandas import json_normalize
import time

#connecting to Postgre on localHost
#!pip install psycopg
from postgres_engine import db_connection


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


# <h5>
# Seasons in Formula 1</h5>

# In[13]:


# current_year = datetime.now().year
#print(current_year)
seasons, total_seasons = seasons_data() #API Call
print(f"Total Seasons in Formual 1 history {total_seasons}")
championship_years = seasons['year'].unique() #championship years
seasons.head(10)
time.sleep(1)



#All the drivers and teams over time period 
#We will get drivers by year because drivers might switch teams or can retire after or in between a season

def get_drivers():
  drivers_year = {}
  for year in championship_years:
    # if year == 2025:
    drivers_api = f"https://f1api.dev/api/{year}/drivers?limit=10000"
    print(drivers_api)
    response = requests.get(drivers_api)
    if response.status_code == 200:
      time.sleep(10)
      print(f"Successful retrival of data. URL response: {response.status_code}")
      data = response.json()
      drivers_data = pd.json_normalize(data['drivers'])
      drivers_data['championshipId'] = data.get('championshipId')
      drivers_data['season'] = data.get('season')
        
      drivers_year[year] = drivers_data
    else:
        print(f"Failed to retrieve the team information {response.status_code}")
  
  return pd.concat(drivers_year.values(), ignore_index=True)

def get_teams():
  drivers_api = f"https://f1api.dev/api/teams?limit=10000"
  print(drivers_api)
  time.sleep(10)
  response = requests.get(drivers_api)
  if response.status_code == 200:
    print(f"Successful retrival of data. URL response: {response.status_code}")
    data = response.json()
    return pd.json_normalize(data['teams'])
  else:
        print(f"Failed to retrieve the team information {response.status_code}")


# In[17]:


#time.sleep(60)


# In[18]:


#Seasons API Request
#season URL generating for all years overcoming the limit and offset in the API
def races_data(races_current):
    races = {}
    #current_races = {}

    if  not races_current:
        for year in championship_years:
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
        return pd.concat(races.values(), ignore_index= True)
    else:
        races_url = f"https://f1api.dev/api/current?limit=10000&offset=0"
        time.sleep(10)
        response = requests.get(races_url)
        if response.status_code == 200:
            print(f"Successful retrival of data. URL response: {response.status_code}")
            data = response.json()
            current_races = pd.json_normalize(data['races'])
            current_races['season'] = data.get('season')
            current_races['championshipId'] = data['championship']['championshipId']
            current_races['championshipName'] = data['championship']['championshipName']
            current_races['championshipUrl'] = data['championship']['url'] #season
            time.sleep(10)
            return current_races


# In[19]:


time.sleep(60)


# In[20]:


#Drivers or Constructors Standing for each

def standings_data(URL,limit,drivers_or_teams = 'constructors'):
    standings_by_years = {}


    for year in championship_years:
        #if year == 2025:
        key = 'drivers-championship' if drivers_or_teams == 'drivers' else 'constructors-championship'
        standing_url = f"{URL}/{year}/{key}?limit={limit}"
        print(f"Fetching: {standing_url}")
        #sending request through API
        response = requests.get(standing_url)
        time.sleep(3)

        if response.status_code == 200:
            print(f"Successful retrival of data for year {year}. URL response: {response.status_code}")
            data = response.json()
            #print(pd.json_normalize(data, sep = '_'))
            key = 'drivers_championship' if drivers_or_teams == 'drivers' else 'constructors_championship'
            standings_by_year = pd.json_normalize(data[key], sep = '_')
            standings_by_year['season'] = data.get('season', year)
            standings_by_year['championshipId'] = data.get('championshipId')
            standings_by_years[year] = standings_by_year


            time.sleep(3)
        else:
            print(f"Failed to retrieve the team information {response.status_code}")
            time.sleep(1.5)
    return pd.concat(standings_by_years.values(), ignore_index=True)


# In[21]:


#drivers 
drivers = get_drivers()
time.sleep(60)


# In[22]:


#teams
teams = get_teams()
time.sleep(60)


# In[23]:


#circuits
circuits = get_circuits() 
time.sleep(60)


# In[25]:


#races
races = races_data(False) #needs to be re run for 1990 years
time.sleep(60)


# In[ ]:


#current season races
races_current = races_data(True)
time.sleep(60)


# In[ ]:


#Race results for each race since first seasos
def race_results(races_year_rounds):
    results = []
    results_df_rows = pd.DataFrame(columns=['URL', 'response_status'])
    for _, row in races_year_rounds.iterrows():
        
        season =  2025 # row['season']
        round = row['round']
        time.sleep(10)
        url = f"https://f1api.dev/api/{season}/{round}/race?limit=100000"
        if row['season'] != 2025:
            break
        
        response = requests.get(url)
        # Create a temporary DataFrame for the new row
        new_row = pd.DataFrame([{'URL': url, 'response_status': response.status_code}])
            
            # Concatenate it to the main DataFrame
        results_df_rows = pd.concat([results_df_rows, new_row], ignore_index=True)
            
                
        if response.status_code == 200:
            print(f"Successful retrival of data. URL response: {response.status_code}")
            data = response.json()
            time.sleep(2)
            result = pd.json_normalize(data['races'], sep= '.')
            result['season'] = data.get('season')
            result['round'] = data.get('round')
            results.append(result)
            #time.sleep(10)
        else:
            print(f"Failed to retrieve the team information {response.status_code}") 
        time.sleep(1)
    return pd.concat(results, ignore_index= True), results_df_rows


# In[29]:


#race results
races_year_rounds = races[['season','round']]
race_results_df, URL_status_results = race_results(races_year_rounds)
URL_status_results


# In[30]:


race_results_df.to_csv('raceresults.csv', index=False)


# In[31]:


race_results_df


# In[ ]:


URL = 'https://f1api.dev/api'
#Constructor Standing
constructors_standings_all_years = standings_data(URL,10000,'constructors')
time.sleep(60)


# In[32]:


race_results_df2 = race_results_df
race_results_df2


# In[33]:


import ast

def safe_parse_results(x):
    """Safely parse results string to list/dict"""
    if isinstance(x, list):
        return x
    try:
        return ast.literal_eval(x)
    except (ValueError, SyntaxError) as e:
        print(f"Error parsing result: {e}")
        print(f"Problematic data: {x[:100]}...")  # Show first 100 chars
        return None

# Clean and process results data
race_results_df2['results'] = race_results_df2['results'].apply(safe_parse_results)

# Remove any rows where parsing failed
race_results_df2 = race_results_df2[race_results_df2['results'].notna()]

# Now safely flatten the results
try:
    # Explode the results column
    race_results_flat = race_results_df2.explode('results').reset_index(drop=True)
    
    # Normalize the nested dictionaries
    results_normalized = pd.json_normalize(
        race_results_flat['results'].tolist(),
        sep='_'
    )
    
    # Combine with original data
    race_results_flat = pd.concat(
        [race_results_flat.drop(['results'], axis=1), results_normalized],
        axis=1
    )
    
    print(f"Successfully flattened {len(race_results_flat)} race results")
    race_results_flat.head()
    
except Exception as e:
    print(f"Error flattening results: {e}")
    print("\nSample of problematic data:")
    print(race_results_df2['results'].head())


# In[34]:


race_results_flat.dtypes


# In[35]:


race_results_flat.columns.values[2] = 'race_time_race'    # or whatever index is correct
race_results_flat.columns.values[22] = 'race_time_driver'


# In[ ]:


#Driver Standing
URL = 'https://f1api.dev/api'
driver_standing_all_years = standings_data(URL,10000,'drivers')
time.sleep(60)


# In[ ]:


results_df = race_results['results'].apply(pd.Series)


# Convert 'info' column (dicts) to separate columns
# info_df = df['info'].apply(pd.Series)

# Merge back with the original DataFrame (optional)
race_results = pd.concat([race_results.drop(columns=['results']), results_df], axis=1)
results_df


# In[ ]:


race_results


# CREATE TABLES IN POSTGRE SQL USING GENERATED SCRIPTS
# 
# <p>Used sqlAlchemy to upload data to postgres. But just wanted to know how to do it</p>

# In[39]:


#connecting to Postgre
#connection
connection = psycopg.connect(
    dbname = 'formula1',
    user = 'postgres',
    password = 'Vsnkr@02',
    host = 'localhost',
    port = '5432'
)

connection.autocommit = True
cursor = connection.cursor()


# In[40]:


def pandas_to_sql_dtype_mapping(dataframe):
    headers_dic = {col: str(dtype) for col, dtype in dataframe.dtypes.items()}
    #print(headers_dic)


    for key in headers_dic:
        if headers_dic[key] == 'object' or headers_dic == 'string' or headers_dic[key] == 'category':
            headers_dic[key] = 'TEXT'
        if headers_dic[key] == 'int64'  or headers_dic[key] == 'int32':
            headers_dic[key] = 'INTEGER'
        if headers_dic[key] == 'float64' or headers_dic[key] == 'float32':
            headers_dic[key] = 'DOUBLE PRECISION'
        if headers_dic[key] == 'bool':
            headers_dic[key] = 'BOOLEAN'
        if headers_dic == 'datetime64[ns]':
            headers_dic[key] = 'TIMESTAMP'
        if headers_dic[key] == 'timedelta[ns]':
            headers_dic[key] = 'INTERVAL'
        else:
            headers_dic[key] == 'TEXT'
    print(headers_dic)


    return headers_dic


def generate_sql_schema(table_name,dataframe):
    table_headers_dic =pandas_to_sql_dtype_mapping(dataframe)
    columns = []
    for key, value in table_headers_dic.items():
        columns.append(f'{key.upper()}  {value.upper()}')
    #print(columns)

    columns_ddl = ",\n".join(columns)
    #print(columns_ddl)

    # create table and add the varible names to it to make a complete SQL table statement
    #table_name = 'SEASONS'
    table_create = f"CREATE TABLE IF NOT EXISTS {table_name} ( \n{columns_ddl}\n);"
    return table_create

# seasons.info()
def  generate_table(sql_create_script,table_name):
    try:
        cursor.execute(sql_create_script)
        return f"Table '{table_name}' has been created on {datetime.now()}"
    except Exception as e:
        connection.rollback()  # 💥 Required to reset the transaction state
        print("Error executing SQL:", e)
        return f"Table '{table_name}' has not been created. Please verify the Query"

#open a cursor and creating a table in SQL
table_name = 'seasons'

#generating SQL statement with  generate_sql_schema function
season_table_script = generate_sql_schema(table_name, seasons)
print(season_table_script)
print(generate_table(season_table_script, table_name))




#driver_standings_all_years.head(5)
table_name = 'driver_standing_all_years'
#driver_standings_table_headers = driver_standing_all_years.columns
driver_standing_sql_table = generate_sql_schema(table_name, driver_standing_all_years)
print(driver_standing_sql_table)

#creating table
print(generate_table(driver_standing_sql_table,table_name))


# <h3>Ingesting Data into Postgres</h3>

# In[41]:


#create engine
engine = sqlalchemy.create_engine('postgresql+psycopg://postgres:Vsnkr%4002@localhost/formula1')
#dialect+psycopg(driver)//username:passwd(if has special character use encoded URL)@server/database_name

#display engine info
print(engine)



# In[42]:


#df to sql
seasons.to_sql('seasons', engine, if_exists= 'replace', index = False)
#dataframe_name.to_sql('table_name', engine_create_URL, if_exists, index)


# In[43]:


chunk_size = 10000
for start in range(0, len(races), chunk_size):
    chunk = races[start:start+chunk_size]
    chunk.to_sql('races', engine, if_exists= 'replace', index= False)


# In[44]:


teams.to_sql('teams', engine, if_exists= 'replace', index = False)


# In[45]:


drivers.to_sql('drivers', engine, if_exists= 'replace', index = False)


# In[ ]:


races_current.to_sql('races_current', engine, if_exists= 'replace', index = False)


# In[46]:


circuits.to_sql('circuits', engine, if_exists= 'replace', index = False)


# In[ ]:


#df to sql by chunks
chunk_size = 10000
for start in range(0, len(driver_standing_all_years), chunk_size):
    chunk = driver_standing_all_years[start:start+chunk_size]
    chunk.to_sql('driver_standing_all_years', engine, if_exists= 'replace', index= False)


# In[ ]:


#df to sql by chunks
chunk_size = 10000
for start in range(0, len(constructors_standings_all_years), chunk_size):
    chunk = constructors_standings_all_years[start:start+chunk_size]
    chunk.to_sql('constructors_standings_all_years', engine, if_exists= 'replace', index= False)


# In[47]:


chunk_size = 10000
for start in range(0, len(race_results_flat), chunk_size):
    chunk = race_results_flat[start:start+chunk_size]
    chunk.to_sql('race_results', engine, if_exists= 'replace', index= False)


# Running select statements to check if the queries have been created in postgres through SQLAlchemy

# In[48]:


#cursor.execute("SELECT * FROM driver_standing_all_years")
df = pd.read_sql_query("SELECT * FROM race_results", engine)
df


# In[ ]:


connection.commit()
connection.close()


# In[ ]:





# In[ ]:





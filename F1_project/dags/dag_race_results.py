from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import time
import pandas as pd
from pandas import json_normalize
import requests


POSTGRES_CONN_ID = 'formula1_astro'
API_URL = 'https://f1api.dev/api'  # full URL




@dag(
    dag_id = 'dag_prod_schema',
    start_date= datetime(2025,1,1),
    schedule= '0 16 */2 * *',
    catchup = False,
    tags = ['F1', 'PROD SCHEMA']    
)

def f1_prod_schema():

    @task(task_id= 'task_create_schema')
    def create_schema():
        #hook and engine
        pg_hook = PostgresHook(POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        with engine.begin() as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS PROD_F1;")
            result = conn.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name='PROD_F1';")
            schema_exists = result.fetchone()
            print("Schema created or exists:", schema_exists)
            
    @task(task_id = 'f1_seasons_task')
    def create_and_insert_seasons():
        #hook and engine
        pg_hook = PostgresHook(POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        with engine.begin() as conn:
            #Creating the table
            conn.execute('''
                    CREATE TABLE IF NOT EXISTS PROD_F1.SEASONS (
                        season INT,
                        championship_id TEXT PRIMARY KEY,   -- primary key
                        championship_name TEXT,
                        url TEXT,
                        last_modified_datetime TIMESTAMP DEFAULT NOW()
                    );
                ''')
            #Inserting the table
            conn.execute('''INSERT INTO PROD_F1.SEASONS (season, championship_id,championship_name, url,last_modified_datetime)
                         SELECT "year", 
                                "championshipId" ,
	                            "championshipName",
	                            "url",
                                "lastmodifieddatetime"
                         FROM PUBLIC.SEASONS
                         ON CONFLICT (championship_id)
                         DO UPDATE SET
                            championship_id = EXCLUDED.championship_id,
                            championship_name = EXCLUDED.championship_name,
                            url = EXCLUDED.url,
                            last_modified_datetime = EXCLUDED.last_modified_datetime;
                            ''')
            seasons = pd.read_sql_query("SELECT * FROM PROD_F1.SEASONS;", engine)
            print(seasons.head(5))
        
        #CIRCUITS HISTORY
    @task(task_id = 'f1_circuits_tasks')
    def create_and_insert_circuits():
        #hook and engine
        pg_hook = PostgresHook(POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        with engine.begin() as conn:
                #Creating the table circuits 
            conn.execute('''
                    CREATE TABLE IF NOT EXISTS PROD_F1.CIRCUITS(
                        id TEXT PRIMARY KEY,
                        name TEXT,
                        country TEXT,
                        city TEXT,
                        length FLOAT,
                        laprecord TEXT,
                        first_participation_year INT,
                        last_appearance_year INT,
                        corners FLOAT,
                        fastest_lap_driver_id TEXT,
                        fastest_lap_team_id TEXT,
                        fastest_lap_year FLOAT,
                        url TEXT,
                        lastmodifieddatetime TIMESTAMP
                        );
                    ''')
                 #Inserting the table
            conn.execute('''
                    INSERT INTO PROD_F1.CIRCUITS(
						id,
                        name,
                        country,
                        city,
                        length,
                        laprecord,
                        first_participation_year,
                        last_appearance_year,
                        corners,
                        fastest_lap_driver_id,
                        fastest_lap_team_id,
                        fastest_lap_year,
                        url,
                        lastmodifieddatetime)
                    SELECT 
                        "circuitId" ,
                        "circuitName"  ,
                        "country" ,
                        "city",
                        "circuitLength",
                        "lapRecord",
                        "firstParticipationYear",
                        null,
                        "numberOfCorners" ,
                        "fastestLapDriverId",
                        "fastestLapTeamId",
                        "fastestLapYear",
                        "url",
                        "lastmodifieddatetime"
                        FROM PUBLIC.CIRCUITS
                        ON CONFLICT (id)
                    DO UPDATE SET
                        id = EXCLUDED.id,
                        name = EXCLUDED.name,
                        country = EXCLUDED.country,
                        city = EXCLUDED.city,
                        length = EXCLUDED.length,
                        laprecord = EXCLUDED.laprecord,
                        first_participation_year = EXCLUDED.first_participation_year,
                        last_appearance_year = EXCLUDED.last_appearance_year,
                        corners = EXCLUDED.corners,
                        fastest_lap_driver_id = EXCLUDED.fastest_lap_driver_id,
                        fastest_lap_team_id = EXCLUDED.fastest_lap_team_id,
                        fastest_lap_year = EXCLUDED.fastest_lap_year,
                        url = EXCLUDED.url,
                        lastmodifieddatetime = EXCLUDED.lastmodifieddatetime;
                    ''')
            circuits = pd.read_sql_query("SELECT * FROM PROD_F1.CIRCUITS;", engine)
            print(circuits.head(5))
                
        
        
    
#DRIVERS HISTORY
    @task(task_id = 'f1_drivers_task')
    def create_and_insert_drivers():
        #hook and engine
        pg_hook = PostgresHook(POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        with engine.begin() as conn:
            #Creating the table
            conn.execute('''
                --DROP TABLE IF EXISTS PROD_F1.DRIVERS;
                CREATE TABLE IF NOT EXISTS PROD_F1.DRIVERS (
                    driver_id TEXT,
                    driver_name TEXT,
                    driver_number FLOAT,
                    short_name TEXT,
                    team_id TEXT,
                    team_name TEXT,
                    championship_id TEXT,
                    season INT,
	                driver_nationality TEXT,
	                driver_birthday DATE,
                    URL TEXT,
                    driver_season_id  TEXT PRIMARY KEY
                    );
                ''')
                #Inserting the table
            conn.execute(''' 
                INSERT INTO PROD_F1.DRIVERS (
                    driver_id, 
                    driver_name,
                    driver_number,
                    short_name,
                    team_id,
                    team_name,
                    championship_id,
                    season,
	                driver_nationality,
	                driver_birthday,
                    URL,
                    driver_season_id )
                SELECT 
                    "driverId",
                    "name"||' '||"surname",
                    "number",
                    "shortName",
                    d."teamId",
                    t."teamName",
                    "championshipId",
                    "season",
                    "nationality",
                    case 
		                when "birthday" ~ '^\d{4}-\d{2}-\d{2}$' then TO_DATE("birthday", 'YYYY-MM-DD')
		                when "birthday" ~ '^\d{2}/\d{2}/\d{4}' then TO_DATE("birthday", 'DD/MM/YYYYY')
		                else NULL
                	end,
                    d."url",
                    "driverId" || '_' || season::TEXT 
                    FROM PUBLIC.drivers d
                    LEFT JOIN PUBLIC.teams t on d."teamId" = t."teamId"
                    ON CONFLICT (driver_season_id)
                    DO UPDATE SET
                        driver_name = EXCLUDED.driver_name,
                        driver_number = EXCLUDED.driver_number,
                        short_name = EXCLUDED.short_name,
                        team_id = EXCLUDED.team_id,
                        team_name = EXCLUDED.team_name,
                        championship_id = EXCLUDED.championship_id,
                        season = EXCLUDED.season,
                        driver_nationality = EXCLUDED.driver_nationality,
                        driver_birthday = EXCLUDED.driver_birthday,
                        url = EXCLUDED.url;
                        ''')
            drivers = pd.read_sql_query("SELECT * FROM PROD_F1.DRIVERS;", engine)
            print(drivers.head(5))   
        
    #DRIVERS CHAMPIONSHIP HISTORY
    @task(task_id= 'f1_drivers_championship_standings_task')
    def create_and_insert_drivers_championship_standings():
        #hook and engine
        pg_hook = PostgresHook(POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        with engine.begin() as conn:
            conn.execute('''
                        CREATE TABLE IF NOT EXISTS PROD_F1.DRIVER_CHAMPIONSHIP_STANDINGS (
                            championship_id TEXT,
                            season INT,
                            classification_id INT,
                            driver_id TEXT,
                            driver_name TEXT,
                            driver_number INT,
                            points FLOAT,
                            wins INT,
                            position INT,
                            team_id TEXT,
                            team_name TEXT,
                            driver_season_id TEXT,
                            lastmodifieddatetime TIMESTAMP,
                            PRIMARY KEY (driver_season_id, classification_id)
                        );
                    ''')
            conn.execute('''
                        INSERT INTO prod_f1.DRIVER_CHAMPIONSHIP_STANDINGS (
                            championship_id,
                            season,
                            classification_id,
                            driver_id,
                            driver_name,
                            driver_number,
                            points,
                            wins,
                            position,
                            team_id,
                            team_name,
                            driver_season_id,
                            lastmodifieddatetime
                            )
                            SELECT
                                "championshipId",
                                "season",
                                "classificationId",
                                "driverId",
                                CONCAT("driver_name", ' ', "driver_surname") AS driver_name,
                                "driver_number",
                                "points",
                                COALESCE("wins", 0) AS wins,
                                "position",
                                "teamId",
                                "team_teamName" AS team_name,
                                "driverId" || '_' || season::TEXT AS driver_season_id,
                                "lastmodifieddatetime"
                            FROM public.drivers_championships
                            ON CONFLICT (driver_season_id, classification_id)
                            DO UPDATE SET
                                championship_id = EXCLUDED.championship_id,
                                season = EXCLUDED.season,
                                classification_id = EXCLUDED.classification_id,
                                driver_id = EXCLUDED.driver_id,
                                driver_name = EXCLUDED.driver_name,
                                driver_number = EXCLUDED.driver_number,
                                points = EXCLUDED.points,
                                wins = EXCLUDED.wins,
                                position = EXCLUDED.position,
                                team_id = EXCLUDED.team_id,
                                team_name = EXCLUDED.team_name,
                                lastmodifieddatetime = EXCLUDED.lastmodifieddatetime,
                                driver_season_id = EXCLUDED.driver_season_id;
                            ''')
            result = conn.execute("SELECT * FROM PROD_F1.DRIVER_CHAMPIONSHIP_STANDINGS;")
            table_exists = result.fetchone()
            print("Schema created or exists:",  table_exists)
                
     #CONSTRUCTORS CHAMPIONSHIP HISTORY
    @task(task_id= 'f1_constructors_championship_standings_task')
    def create_and_insert_constructors_championship_standings():
        #hook and engine
        pg_hook = PostgresHook(POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        with engine.begin() as conn:
            conn.execute('''
                        CREATE TABLE IF NOT EXISTS PROD_F1.CONSTRUCTOR_CHAMPIONSHIP_STANDINGS (
                            classification_id INT,
                            season INT,
                            championship_id TEXT,
                            team_id TEXT,
                            team_name TEXT,
                            team_season_id TEXT,
                            points FLOAT,
                            wins INT,
                            position INT,                    
                            lastmodifieddatetime TIMESTAMP,
                            PRIMARY KEY (team_season_id, classification_id)
                        );
                    ''')
            conn.execute('''
                        INSERT INTO prod_f1.CONSTRUCTOR_CHAMPIONSHIP_STANDINGS (
                            classification_id,
                            season,
                            championship_id,
                            team_id,
                            team_name,
                            team_season_id,
                            points,
                            wins,
                            position,
                            lastmodifieddatetime
                            )
                            SELECT
                                "classificationId",
                                "season",
                                "championshipId",
                                "teamId",
                                "team_teamName",
                                "teamId" || '_' || season::TEXT AS team_season_id,
                                "points",
                                COALESCE("wins", 0) AS wins,
                                "position",
                                "lastmodifieddatetime"
                            FROM public.constructors
                            ON CONFLICT (team_season_id)
                            DO UPDATE SET
                                classification_id = EXCLUDED.classification_id,
                                season = EXCLUDED.season,
                                championship_id = EXCLUDED.championship_id,
                                team_id = EXCLUDED.team_id,
                                team_name = EXCLUDED.team_name,
                                --team_season_id = EXCLUDED.team_season_id,
                                points = EXCLUDED.points,
                                wins = EXCLUDED.wins,
                                position = EXCLUDED.position,
                                lastmodifieddatetime = EXCLUDED.lastmodifieddatetime;
                            ''')
            result = conn.execute("SELECT * FROM PROD_F1.CONSTRUCTOR_CHAMPIONSHIP_STANDINGS;")
            table_exists = result.fetchone()
            print("Schema created or exists:",  table_exists)
            
    @task(task_id = 'f1_teams_task')
    def create_and_insert_teams():
        #hook and engine
        pg_hook = PostgresHook(POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        with engine.begin() as conn:
            #create table
            conn.execute('''
                    create table if not exists PROD_F1.TEAMS(
                        team_id TEXT primary KEY,
                        team_name TEXT,
                        team_nationality TEXT,
                        team_first_appearance FLOAT,
                        team_last_appearance FLOAT,
                        team_status TEXT,
                        constructors_championships FLOAT,
                        drivers_championships FLOAT,
                        url TEXT,
                        last_modified_datetime timestamp
                    );
                ''')
            conn.execute('''
                    WITH team_appearance_rank AS (
                        SELECT 
                            "teamId",  
                            season, 
                            RANK() OVER (
                                PARTITION BY "teamId" 
                                ORDER BY season DESC
                            ) AS team_last_appearance
                        FROM 
                            constructors
                        WHERE season <> EXTRACT('Year' FROM CURRENT_DATE)
                    ),
                    teams_current AS (
                        SELECT DISTINCT "teamId"
                        FROM constructors
                        WHERE season = EXTRACT('Year' FROM CURRENT_DATE)
                    ),
                    team_last_appearance AS (
                        SELECT  
                        tar."teamId" as teamId,
                        tar.season as season,
                        tar.team_last_appearance,
                        tc."teamId"	as tc_teamid
                    FROM team_appearance_rank tar
                    LEFT JOIN teams_current tc 
                        ON tar."teamId" = tc."teamId"
                    WHERE tc."teamId" IS NULL  -- Exclude teams active in 2025
                        AND tar.team_last_appearance = 1
                    ),
                    team_first_season as 
                    (
                    select "teamId", min(season) as first_season
                    from constructors
                    GROUP BY "teamId"
                    )
                    insert into PROD_F1.TEAMS(
                        team_id,
                        team_name,
                        team_nationality,
                        team_first_appearance,
                        team_last_appearance,
                        team_status,
                        constructors_championships,
                        drivers_championships,
                        url,
                        last_modified_datetime
                    )
                    SELECT
                        t."teamId",
                        t."teamName",
                        t."teamNationality",
                        CASE WHEN t."firstAppeareance" is null then (select first_season from team_first_season tfs where t."teamId" = tfs."teamId") 
                        ELSE  t."firstAppeareance" END,
                        tla."season" as team_last_appearance,
                        CASE WHEN t."teamId" in (select "teamId" from teams_current) then 'Active'
                            ELSE 'Inactive' END,
                        COALESCE("constructorsChampionships",0),
                        COALESCE("driversChampionships",0),
                        "url",
                        t."lastmodifieddatetime" 
                    FROM PUBLIC.teams t
                    left join team_last_appearance tla on t."teamId"  = tla.teamId
                    ON CONFLICT (team_id)
                    DO UPDATE SET
                        team_name = EXCLUDED.team_name,
                        team_nationality = EXCLUDED.team_nationality,
                        team_first_appearance = EXCLUDED.team_first_appearance,
                        team_last_appearance = EXCLUDED.team_last_appearance,
                        team_status = EXCLUDED.team_status,
                        constructors_championships = EXCLUDED.constructors_championships,
                        drivers_championships = EXCLUDED.drivers_championships,
                        url = EXCLUDED.url,
                        last_modified_datetime = EXCLUDED.last_modified_datetime
                        ;
                    ''')
            
            result = conn.execute("SELECT * FROM PROD_F1.TEAMS;")
            table_exists = result.fetchone()
            print("Table created or exists:",  table_exists)
            
            
    @task(task_id = 'f1_races_task')
    def create_and_insert_races():
         #hook and engine
        pg_hook = PostgresHook(POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        with engine.begin() as conn:
            #create table
            conn.execute(''' 
                    CREATE TABLE IF NOT EXISTS PROD_F1.RACES(
                        championship_id TEXT,
                        race_id text,
                        race_name text,
                        laps int,
                        round int,
                        corners float,
                        circuit_id text,
                        circuit_name text,
                        scheduled_race_date date,
                        scheduled_race_time time,
                        scheduled_qualy_date date,
                        scheduled_qualy_time time,
                        fastest_lap text,
                        fastest_lap_driver_id text,
                        race_winner_driver_id text,
                        race_winner_driver_name text,
                        team_id text,
                        team_name TEXT,
                        last_modified_datetime TIMESTAMP
                    );
                         ''')
            conn.execute(''' 
                    insert into PROD_F1.RACES(championship_id,
                        race_id,
                        race_name,
                        laps,
                        round,
                        corners,
                        circuit_id,
                        circuit_name,
                        scheduled_race_date,
                        scheduled_race_time,
                        scheduled_qualy_date,
                        scheduled_qualy_time,
                        fastest_lap,
                        fastest_lap_driver_id,
                        race_winner_driver_id,
                        race_winner_driver_name,
                        team_id,
                        team_name,
                        last_modified_datetime)
                    SELECT
                        "championshipId",
                        "raceId",
                        "raceName",
                        "laps",
                        "round",
                        "circuit_corners",
                        "circuit_circuitId",
                        "circuit_circuitName",
                        CAST("schedule_race_date" AS DATE),
                        CAST("schedule_race_time" AS TIME),
                        CAST("schedule_qualy_date" AS DATE),
                        CAST("schedule_qualy_time" AS TIME),
                        "fast_lap_fast_lap",
                        "fast_lap_fast_lap_driver_id",
                        "winner_driverId",
                        CONCAT("winner_name", ' ', "winner_surname"),
                        "teamWinner_teamId",
                        "teamWinner_teamName",
                        "lastmodifieddatetime"
                    FROM PUBLIC.races
                    ON CONFLICT (race_id, championship_id)
                  DO UPDATE set
                  		championship_id = EXCLUDED.championship_id,
                        race_id = EXCLUDED.race_id,
                        race_name = EXCLUDED.race_name,
                        laps = EXCLUDED.laps,
                        round = EXCLUDED.round,
                        corners = EXCLUDED.corners,
                        circuit_id = EXCLUDED.circuit_id,
                        circuit_name = EXCLUDED.circuit_name,
                        scheduled_race_date = EXCLUDED.scheduled_race_date,
						scheduled_race_time = EXCLUDED.scheduled_race_time,
						scheduled_qualy_date = EXCLUDED.scheduled_qualy_date,
						scheduled_qualy_time = EXCLUDED.scheduled_qualy_time,
						fastest_lap = EXCLUDED.fastest_lap,
						fastest_lap_driver_id = EXCLUDED.fastest_lap_driver_id,
						race_winner_driver_id = EXCLUDED.race_winner_driver_id,
						race_winner_driver_name = EXCLUDED.race_winner_driver_name,
						team_id = EXCLUDED.team_id,
						team_name = EXCLUDED.team_name,
                        last_modified_datetime = EXCLUDED.last_modified_datetime
                        ;
                         ''')
        
            result = conn.execute("SELECT * FROM PROD_F1.RACES;")
            table_exists = result.fetchone()
            print("Table created or exists:",  table_exists)
            
    @task(task_id='f1_season_races_results_task')
    def create_and_insert_season_race_results():
        # hook and engine
        pg_hook = PostgresHook(POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        
        with engine.begin() as conn:
            # create table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS PROD_F1.SEASON_RACE_RESULTS(
                    round TEXT,
                    race_date DATE,
                    race_time TIME,
                    race_id VARCHAR(255),
                    race_name TEXT,
                    circuit_id VARCHAR(255),
                    circuit_name TEXT,
                    circuit_length TEXT,
                    circuit_corners SMALLINT,
                    season INT,
                    driver_position TEXT,
                    points SMALLINT,
                    grid_position TEXT,
                    driver_race_time TEXT,
                    driver_fast_lap TEXT,
                    retired TEXT,
                    driver_id VARCHAR(255),
                    driver_number SMALLINT,
                    driver_name TEXT,
                    team_id TEXT,
                    team_name TEXT,
                    last_modified_datetime TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (race_id, driver_id)
                );
            ''')
            
            conn.execute('''
                    INSERT INTO PROD_F1.SEASON_RACE_RESULTS (
                        round, race_date, race_time, race_id, race_name,
                        circuit_id, circuit_name, circuit_length, circuit_corners, season,
                        driver_position, points, grid_position, driver_race_time, driver_fast_lap,
                        retired, driver_id, driver_number, driver_name, team_id, team_name, last_modified_datetime
                    )
                SELECT DISTINCT ON ("result_driver_driverId", "race_raceId", "result_team_teamId")
                    "race_round",
                    CAST("race_date" AS DATE),
                    CAST("race_race_time" AS TIME),
                    "race_raceId",
                    "race_raceName",
                    "race_circuit_circuitId",
                    "race_circuit_circuitName",
                    "race_circuit_circuitLength",
                    "race_circuit_corners",
                    "race_season",
                    "result_position",
                    "result_points",
                    "result_grid",
                    "result_time",
                    "result_fastLap",
                    "result_retired",
                    "result_driver_driverId",
                    CAST("result_driver_number" AS SMALLINT),
                    "result_driver_name",
                    "result_team_teamId",
                    "result_team_teamName",
                    NOW()
                FROM PUBLIC.RACE_RESULTS
                ON CONFLICT (driver_id,race_id, team_id)
                DO UPDATE SET
                    round = EXCLUDED.round,
                    race_date = EXCLUDED.race_date,
                    race_time = EXCLUDED.race_time,
                    race_name = EXCLUDED.race_name,
                    circuit_id = EXCLUDED.circuit_id,
                    circuit_name = EXCLUDED.circuit_name,
                    circuit_length = EXCLUDED.circuit_length,
                    circuit_corners = EXCLUDED.circuit_corners,
                    season = EXCLUDED.season,
                    driver_position = EXCLUDED.driver_position,
                    points = EXCLUDED.points,
                    grid_position = EXCLUDED.grid_position,
                    driver_race_time = EXCLUDED.driver_race_time,
                    driver_fast_lap = EXCLUDED.driver_fast_lap,
                    retired = EXCLUDED.retired,
                    driver_number = EXCLUDED.driver_number,
                    driver_name = EXCLUDED.driver_name,
                    team_id = EXCLUDED.team_id,
                    team_name = EXCLUDED.team_name,
                    last_modified_datetime = NOW();
            ''')

            result = conn.execute("SELECT * FROM PROD_F1.SEASON_RACE_RESULTS LIMIT 1;")
            table_exists = result.fetchone()
            print("Table created or exists:", bool(table_exists))


              
    task_f1_schema = create_schema()
    task_seasons = create_and_insert_seasons()
    task_circuits = create_and_insert_circuits()
    task_drivers = create_and_insert_drivers()
    task_drivers_championship_standings = create_and_insert_drivers_championship_standings()
    task_constructors_championship_standings =create_and_insert_constructors_championship_standings()
    task_teams = create_and_insert_teams()
    task_races = create_and_insert_races()
    task_season_race_results= create_and_insert_season_race_results()
    #task flow
    task_f1_schema >> task_seasons >> task_circuits >> task_drivers >> task_drivers_championship_standings >> task_constructors_championship_standings >> task_teams >> task_races >> task_season_race_results 
f1_prod =  f1_prod_schema()


--PROD SCHEMA CREATION FOR DATA; PUBLIC SCHEMA WILL BE USED FOR STAGING
CREATE SCHEMA IF NOT EXISTS PROD;

--Create DRIVERS STANDINGS Table with data for each year

--DROP TABLE IF EXISTS PROD.DRIVERS_STANDINGS;

CREATE TABLE IF NOT EXISTS PROD.DRIVERS_STANDINGS AS
SELECT
	"championshipId" as championship_id,
	"season" as season,
	"classificationId" as classification_id,
	"driverId" as driver_id,
	CONCAT("driver_name", ' ', "driver_surname") as driver_name,
	"driver_number" as driver_number,
	"points" as points,
	COALESCE("wins", 0) AS wins,
	"position" as position,
	"teamId" as team_id,
	"team_teamName" as team_name
FROM public.driver_standings_all_years;


SELECT *
FROM PROD.DRIVERS_STANDINGS;

--CREATE CONSTRUCTORS STANDINGS Table with data for each year

--DROP TABLE IF EXISTS PROD.CONSTRUCTORS_STANDINGS;

CREATE TABLE IF NOT EXISTS PROD.CONSTRUCTORS_STANDINGS AS
SELECT
	"championshipId" as championship_id,
	"season" as season,
	"classificationId" as classification_id,
	"teamId" as team_id,
	"team_teamName" as team_name,
	"position" as position,
	"points" as points,
	COALESCE("wins", 0) AS wins
FROM PUBLIC.constructors_standings_all_years;


SELECT *
FROM PROD.CONSTRUCTORS_STANDINGS;


select *
from public.seasons;

--SEASONS TABLE CREATION
-- DROP TABLE IF EXISTS PROD.SEASONS
CREATE TABLE IF NOT EXISTS PROD.SEASONS AS
SELECT
	"championshipId" as championship_id,
	"year" as season,
	"championshipName" as championship_name,
	"url" as URL
FROM PUBLIC.SEASONS;

SELECT *
FROM PROD.SEASONS;


--RACES BY SEASON TABLE CREATION

-- DROP TABLE IF EXISTS PROD.RACES
CREATE TABLE IF NOT EXISTS PROD.RACES AS
SELECT
	"championshipId" as championship_id,
	"raceId" as race_id,
	"raceName" as race_name,
	"laps" as laps,
	"round" as round,
	"circuit.corners" as corners,
	"circuit.circuitId" as circuit_id,
	"circuit.circuitName" as circuit_name,
	CAST("schedule.race.date" AS DATE) AS scheduled_race_date,
	CAST("schedule.race.time" AS TIME) AS scheduled_race_time,
	CAST("schedule.qualy.date" AS DATE) AS scheduled_qualy_date,
	CAST("schedule.qualy.time" AS TIME) AS scheduled_qualy_time,
	"fast_lap.fast_lap" as fastest_lap,
	"fast_lap.fast_lap_driver_id" as fastest_lap_driver_id,
	"winner.driverId" as race_winner_id,
	CONCAT("winner.name", ' ', "winner.surname") as race_winner_name,
	"teamWinner.teamId" as team_id,
	"teamWinner.teamName" as team_name
FROM PUBLIC.races;
	
SELECT *
FROM PROD.RACES;




-- CREATING TEAMS AND DRIVERS TABLES

-- DROP TABLE IF EXISTS PROD.DRIVERS

CREATE TABLE IF NOT EXISTS PROD.DRIVERS AS
SELECT 
	"driverId" as driver_id,
	CONCAT("name", ' ', "surname") as driver_name,
	"number" as driver_number,
	"shortName" as short_name,
	d."teamId" as team_id,
	"teamName" as driver_team_name,
	"championshipId" as championship_id,
	"season" as season,
	"nationality" as driver_nationality,
	"birthday" as driver_birthday,
	d."url" as url
FROM PUBLIC.drivers d
LEFT JOIN PUBLIC.teams t on d."teamId" = t."teamId"

SELECT *
FROM PROD.DRIVERS;


-- DROP TABLE IF EXISTS PROD.TEAMS


CREATE TABLE IF NOT EXISTS PROD.TEAMS AS 
WITH team_appearance_rank AS (
    SELECT 
        "teamId",  
        season, 
        RANK() OVER (
            PARTITION BY "teamId" 
            ORDER BY season DESC
        ) AS team_last_appearance
    FROM 
        constructors_standings_all_years
    WHERE season <> EXTRACT('Year' FROM CURRENT_DATE)
),
teams_current AS (
    SELECT DISTINCT "teamId"
    FROM constructors_standings_all_years
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
from constructors_standings_all_years
GROUP BY "teamId"
)
SELECT
	t."teamId" as team_id,
	t."teamName" as driver_team_name,
	t."teamNationality" as team_nationality,
	CASE WHEN t."firstAppeareance" is null then (select first_season from team_first_season tfs where t."teamId" = tfs."teamId") 
	ELSE  t."firstAppeareance" END AS team_first_appearance,
	tla."season" as team_last_appearance,
	CASE WHEN t."teamId" in (select "teamId" from teams_current) then 'Active'
		ELSE 'Inactive' END as team_status,
	COALESCE("constructorsChampionships",0) as constructors_championships,
	COALESCE("driversChampionships",0) as drivers_championships,
	"url" as url	
FROM PUBLIC.teams t
left join team_last_appearance tla on t."teamId"  = tla.teamId



SELECT *
FROM PROD.TEAMS;




-- DROP TABLE IF EXISTS PROD.CIRCUITS

CREATE TABLE IF NOT EXISTS PROD.CIRCUITS AS 
WITH circuit_appearance_rank as (
SELECT 
	circuit_id,
	circuit_name,
	season,
	RANK() OVER(Partition by circuit_id order by season desc) as season_rank
FROM PROD.RACES r
LEFT JOIN PROD.SEASONS s on r.championship_id = s.championship_id
and season <> EXTRACT('Year' FROM CURRENT_DATE)
),
circuit_first_appearance AS (
SELECT
	circuit_id,
	max(season)
from circuit_appearance_rank
GROUP BY circuit_id
),
current_season_circuits as (
SELECT circuit_id
FROM PROD.RACES r
LEFT JOIN PROD.SEASONS s on r.championship_id = s.championship_id
WHERE season = EXTRACT('Year' FROM CURRENT_DATE)),
circuit_last_appearance as (
SELECT car.*
from circuit_appearance_rank car
LEFT JOIN current_season_circuits csc on car.circuit_id = csc.circuit_id
WHERE car.season_rank = 1
)
SELECT 
"circuitId" as circuit_id,
"circuitName" as circuit_name,
"country" as circuit_country,
"city" as circuit_city,
"lapRecord" as circuit_laprecord,
CASE WHEN "firstParticipationYear" is null THEN (SELECT season from circuit_first_appearance cfa where c."circuitId" = cfa.circuit_id )
ELSE "firstParticipationYear" END AS first_participation_year,
"season" as last_appearance_year,
"numberOfCorners" as circuit_corners,
"fastestLapDriverId" as fastest_lap_driver_id,
"fastestLapTeamId" as fatest_lap_team_id,
"fastestLapYear" as fatest_lap_year
FROM PUBLIC.CIRCUITS c
JOIN  circuit_last_appearance cla on c."circuitId" = cla.circuit_id;


select *
from PROD.CIRCUITS;

select *
from public.race_results;

-- DROP TABLE IF EXISTS PROD.CURRENT_SEASON_RACE_RESULTS



CREATE TABLE IF NOT EXISTS PROD.CURRENT_SEASON_RACE_RESULTS(
	round 	text,
	date 	date,
	race_time_race time,
	race_id varchar(255),
	race_name text,
	circuit_id varchar(255),
	circuit_name text,
	circuit_length text,
	circuit_corners smallint,
	season int,
	driver_position text,
	points smallint,
	grid_position text,
	driver_race_time text,
	driver_fast_lap time,
	retired text,
	driver_id varchar(255),
	driver_number smallint,
	driver_name text,
	team_id text,
	team_name text
);

INSERT INTO PROD.CURRENT_SEASON_RACE_RESULTS (
    round,
    date,
    race_time_race,
    race_id,
    race_name,
    circuit_id,
    circuit_name,
    circuit_length,
    circuit_corners,
    season,
    driver_position,
    points,
    grid_position,
    driver_race_time,
    driver_fast_lap,
    retired,
    driver_id,
    driver_number,
    driver_name,
    team_id,
    team_name
)
SELECT 
    round, 
    CAST(date AS date),
    CAST(race_time_race AS time), 
    "raceId",
    "raceName", 
    "circuit.circuitId",
    "circuit.circuitName",
    "circuit.circuitLength",
    "circuit.corners",
    season,   -- You used bigint before, but your table column is 'season date'
    position, 
    points, 
    grid,
    race_time_driver,
    cast("fastLap" as time),
    "retired",
    "driver_driverId", 
    CAST("driver_number" AS smallint),  -- Optional, if driver_number is stored as text
    "driver_name",
    "team_teamId",
    "team_teamName"
FROM PUBLIC.RACE_RESULTS;



select *
from PROD.CURRENT_SEASON_RACE_RESULTS


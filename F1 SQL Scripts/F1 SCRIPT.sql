ALTER TABLE driver_standings_all_years_stage RENAME TO  driver_standings_all_years;
ALTER TABLE constructors_standings_all_years_stage RENAME TO constructors_standings_all_years;
ALTER TABLE drivers_stage  RENAME TO  drivers;
ALTER TABLE teams_stage RENAME TO  teams;
ALTER TABLE races_stage RENAME TO  races;
ALTER TABLE seasons_stage RENAME TO  seasons;


select *
from constructors_standings_all_years;


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

select *
from public.races;

--RACES BY SEASON TABLE CREATION

-- DROP TABLE IF EXISTS PROD.RACES
CREATE TABLE IF NOT EXISTS PROD.RACES AS
SELECT
	"championshipId" as championship_id,
	"raceId" as race_id,
	"raceName" as race_name,
	"laps" as laps,
	"round" as round,
	"circuit.circuitId" as circuit_id,
	"circuit.circuitName" as circuit_name,
	CAST("schedule.race.date" AS DATE) AS scheduled_race_date,
	CAST("schedule.race.time" AS TIME) AS scheduled_race_time,
	CAST("schedule.qualy.date" AS DATE) AS scheduled_qualy_date,
	CAST("schedule.qualy.time" AS TIME) AS scheduled_qualy_time,
	"fast_lap.fast_lap" as fast_lap,
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
FROM PROD.TEAMS
where team_status = 'Active';


select *
from prod.races






	









 
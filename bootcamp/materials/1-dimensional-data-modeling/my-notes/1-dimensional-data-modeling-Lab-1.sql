SELECT * FROM player_seasons
WHERE player_name = 'A.C. Green';



CREATE TYPE season_stats AS (
							season INTEGER,
							gp INTEGER, 
							pts REAL, 
							reb REAL, 
							ast REAL
							)
							
CREATE TYPE scoring_class AS ENUM(
								'star',
								'good',
								'average',
								'bad'
								)

CREATE TABLE players (
		player_name TEXT, 
		height TEXT, 
		college TEXT,
		country TEXT,
		draft_year TEXT, 
		draft_round TEXT,
		draft_number TEXT,
		season_stats season_stats [],
		scoring_class scoring_class,
		years_since_last_season INTEGER,
		current_season INTEGER,
		PRIMARY KEY (player_name, current_season)
	)

-- Seed Query 1st create it and then you insert into the create table (chnage the years to add subsequent years to table)
INSERT INTO players
WITH yesterday AS (
		SELECT * 
		  FROM players 
		 WHERE current_season = 2001
	),
	today AS (
	    SELECT * 
		  FROM player_seasons
		 WHERE season = 2002
	)
SELECT 
	   COALESCE(t.player_name, y.player_name) AS player_name, -- prevents null columns and duplicated columns
	   COALESCE(t.height, y.height) AS height,
	   COALESCE(t.college, y.college) AS college,
	   COALESCE(t.country, y.country) AS country,
	   COALESCE(t.draft_year, y.draft_year) AS draft_year,
	   COALESCE(t.draft_round, y.draft_round) AS draft_round,
	   COALESCE(t.draft_number, y.draft_number) AS draft_number,
	       CASE WHEN y.season_stats is NULL -- new player for season (t) 
		   	    THEN ARRAY [ROW(
							t.season,
							t.gp,
							t.pts,
							t.reb,
							t.ast
						)::season_stats] -- :: casts it as type season_stats
			    WHEN t.season IS NOT NULL -- player both before (y) and now (t)
				THEN y.season_stats || ARRAY [ROW(
							t.season,
							t.gp,
							t.pts,
							t.reb,
							t.ast
						)::season_stats]
				ELSE y.season_stats -- retired players without new seasons (y)
		      END AS season_stats,
		   CASE WHEN t.season IS NOT NULL THEN -- scoring_class based on last played season aka today or :: scoring_class
			   		CASE WHEN t.pts > 20 then 'star'
					     WHEN t.pts >15 THEN 'good'
						 WHEN t.pts > 10 THEN 'average'
						 ELSE 'bad'
					 END ::scoring_class
				ELSE y.scoring_class
		      END AS scoring_class,
		   CASE WHEN t.season IS NOT NULL THEN 0
		        ELSE y.years_since_last_season + 1
			  END AS years_since_last_season,
	   COALESCE(t.season, y.current_season + 1) as current_season
  FROM today as T 
  FULL OUTER JOIN yesterday as Y
    ON t.player_name = y.player_name;

-- You essentially populate players with yesterday and current year by inserting into the table with varying years

-- Looking at one player
SELECT *
  FROM players 
 WHERE player_name = 'Michael Jordan';
   
-- Looking at one player in current_season
SELECT *
  FROM players 
 WHERE current_season = 2001
   AND player_name = 'Michael Jordan';

-- Note you can explode it out again via unnested (note need to take the current year only! )
SELECT player_name,
	   UNNEST (season_stats)::season_stats AS season_stats
  FROM players 
 WHERE current_season = 2001
   AND player_name = 'Michael Jordan';

-- exploded - back to old schema in sorted oder!! 
WITH unnested as (
	SELECT player_name,
		   UNNEST (season_stats)::season_stats AS season_stats
	  FROM players 
	 WHERE current_season = 2001
)
SELECT player_name,
	   (season_stats::season_stats).*
  FROM unnested

-- Deletes the table! 
-- DROP TABLE players


-- Analytics of biggest improvement since 1st season
SELECT player_name,
	   (season_stats[1]::season_stats).pts as first_season,
	   (season_stats[CARDINALITY(season_stats)]::season_stats).pts as latest_season
  FROM players
 WHERE current_season = 2001

-- Using division last season/first season -> biggest improvement higher #
SELECT player_name,
	   ((season_stats[CARDINALITY(season_stats)]::season_stats).pts)/
	   CASE WHEN (season_stats[1]::season_stats).pts = 0 
	   		THEN 1 
			ELSE (season_stats[1]::season_stats).pts
		END AS improvement
  FROM players
 WHERE current_season = 2001
 ORDER BY improvement DESC

-- Dimensional data modeling 
-- incrementally builds up history
-- can run a fast query with historical analysis without shuffle, groupby etc

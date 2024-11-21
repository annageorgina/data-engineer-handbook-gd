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

-- adding is_active column to table from LAB-1
CREATE TABLE players (
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    draft_round TEXT,
    draft_number TEXT,
    seasons season_stats[],
    scorer_class scoring_class,
    years_since_last_active INTEGER,
    is_active BOOLEAN,
    current_season INTEGER,
    PRIMARY KEY (player_name, current_season)
);

-- instead of running sequentially the queries you generate a loop through all of the seasons 
INSERT INTO players (
    player_name,
    height,
    college,
    country,
    draft_year,
    draft_round,
    draft_number,
    seasons,
    scorer_class,
    years_since_last_active,
    is_active,
    current_season
)
WITH 
years AS (
    SELECT *
    FROM GENERATE_SERIES(1996, 2022) AS season
), 
p AS (
    SELECT
        player_name,
        MIN(season) AS first_season
    FROM player_seasons
    GROUP BY player_name
), 
players_and_seasons AS (
    SELECT *
    FROM p
    JOIN years y
        ON p.first_season <= y.season
), 
windowed AS (
    SELECT
        pas.player_name,
        pas.season,
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN ps.season IS NOT NULL
                        THEN ROW(
                            ps.season,
                            ps.gp,
                            ps.pts,
                            ps.reb,
                            ps.ast
                        )::season_stats
                END)
            OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
            NULL
        ) AS seasons
    FROM players_and_seasons pas
    LEFT JOIN player_seasons ps
        ON pas.player_name = ps.player_name
        AND pas.season = ps.season
    ORDER BY pas.player_name, pas.season
), 
static AS (
    SELECT
        player_name,
        MAX(height) AS height,
        MAX(college) AS college,
        MAX(country) AS country,
        MAX(draft_year) AS draft_year,
        MAX(draft_round) AS draft_round,
        MAX(draft_number) AS draft_number
    FROM player_seasons
    GROUP BY player_name
)
SELECT
    w.player_name,
    s.height,
    s.college,
    s.country,
    s.draft_year,
    s.draft_round,
    s.draft_number,
    seasons AS season_stats,
    CASE
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'average'
        ELSE 'bad'
    END::scoring_class AS scorer_class,
    w.season - (seasons[CARDINALITY(seasons)]::season_stats).season AS years_since_last_active,
    ((seasons[CARDINALITY(seasons)]::season_stats).season = w.season)::BOOLEAN AS is_active,
    w.season AS current_season
FROM windowed w
JOIN static s
    ON w.player_name = s.player_name;

-- rename column 
ALTER TABLE players
RENAME COLUMN scorer_class TO scoring_class;



DROP TABLE players_scd
-- creating players scd table where we have their scoring class in the seasons 
CREATE TABLE players_scd (
	 player_name TEXT, 
	 scoring_class scoring_class, 
	 is_active BOOLEAN, 
	 start_season INTEGER, 
	 end_season INTEGER, 
	 current_season INTEGER, 
	 PRIMARY KEY(player_name, start_season)
)

-- we want to see the streak how long they were in a current dimension (scoring class) aka using window funtions
SELECT 
		player_name,
		current_season,
		scoring_class, 
		is_active,
		LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) as previous_scoring_class,
		LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) as previous_is_active
FROM players

-- creating indicator that it chnaged with CTE 

WITH with_previous AS (
	SELECT 
			player_name,
			current_season,
			scoring_class, 
			is_active,
			LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) as previous_scoring_class,
			LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) as previous_is_active
	 FROM players
)
	SELECT *,
			   CASE 
			   		WHEN scoring_class <> previous_scoring_class THEN 1 
					ELSE 0
				END AS scoring_class_change_indicator,
				CASE 
			   		WHEN is_active <> previous_is_active THEN 1 
					ELSE 0
				END AS is_active_change_indicator
		 FROM with_previous
)


-- combining the change indicators into one change_indicator
INSERT INTO players_scd 
WITH with_previous AS (
	SELECT 
			player_name,
			current_season,
			scoring_class, 
			is_active,
			LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) as previous_scoring_class,
			LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) as previous_is_active
	 FROM players
	 WHERE current_season <= 2021
),
with_indicators AS (
	SELECT *,
		   CASE 
				WHEN scoring_class <> previous_scoring_class THEN 1 
				WHEN is_active <> previous_is_active THEN 1
				ELSE 0
			END AS change_indicator
		 FROM with_previous

), 
with_streaks AS (
	SELECT *,
	   SUM(change_indicator)
	   	  OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
  FROM with_indicators
)
SELECT player_name,
	   scoring_class,
	   is_active,
	   MIN(current_season) AS start_seaon,
	   MAX(current_season) AS end_season,
	   2021 AS current_season
 FROM with_streaks
GROUP BY player_name, streak_identifier, is_active, scoring_class
ORDER BY player_name, streak_identifier

-- expensive parts of the abover query 
 -- partititons & window functions on entire dataset 
 -- only groupby at the end so data is only "shrunk" at the end of the query

-- NOTE every day/year you would have the change the current_season and crunch the full dataset again!!
--  need to think about scale (millions ok probably but billions not) 

SELECT * FROM players_scd;

-- create a type for the array 
CREATE TYPE scd_type AS (
	scoring_class scoring_class,
	is_active BOOLEAN, 
	start_season INTEGER,
	end_season INTEGER
)

WITH 
	last_season_scd AS (
		SELECT * 
		 FROM players_scd 
		 WHERE current_season = 2021
		 AND end_season = 2021
	),
	historical_scd AS ( 			-- this does not change moving forwards because its all in the past! 
		SELECT * 
		FROM players_scd
		WHERE current_season = 2021
		 AND end_season < 2021
	),
	this_season_scd AS (
		SELECT * 
		 FROM players 				-- players not players_scd!! 
		WHERE current_season = 2022
	),
	unchanged_records AS (
		SELECT ts.player_name, 
			   ts.scoring_class,
			   ts.is_active,
			   ls.start_season, 
			   ts.current_season as end_season
		 FROM this_season_scd AS ts
		 JOIN last_season_scd AS ls
		  	  ON ls.player_name = ts.player_name 
		WHERE ts.scoring_class = ls.scoring_class 
		  AND ts.is_active = ls.is_active
	),
	changed_records AS (
		SELECT ts.player_name, 
			   UNNEST(ARRAY[
					ROW(
						ls.scoring_class,
						ls.is_active,
						ls.start_season,
						ls.end_season
					)::scd_type,
					ROW(
						ts.scoring_class,
						ts.is_active,
						ts.current_season,
						ts.current_season
					)::scd_type
			   ]) as records
		 FROM this_season_scd AS ts
		 LEFT JOIN last_season_scd AS ls
		  	  ON ls.player_name = ts.player_name 
		WHERE (ts.scoring_class <> ls.scoring_class  -- changed records
		  OR ts.is_active <> ls.is_active) 
	),
	unnested_changed_records AS (
		SELECT player_name, 
			   (records::scd_type).scoring_class,
			   (records::scd_type).is_active,
			   (records::scd_type).start_season,
			   (records::scd_type).end_season
	    FROM changed_records
	)
	SELECT * FROM unnested_changed_records
	

	SELECT ts.player_name, 
		   ts.scoring_class,
		   ts.is_active,
		   ls.scoring_class,
		   ls.is_active
	 FROM this_season_scd AS ts
	 LEFT JOIN last_season_scd AS ls
	  	  ON ls.player_name = ts.player_name

	   

	


	


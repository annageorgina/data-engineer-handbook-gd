-- 1-dimensional-data-modelling-Lab-3
SELECT * 
  FROM actor_films 
 LIMIT 100;

-- Which players play together 
-- Which players play together



CREATE TYPE vertex_type 
	AS ENUM ('player', 'team', 'game');

-- This deletes the type vertex_type 
-- DROP TYPE vertex_type CASCADE; 

CREATE TABLE vertices (
	identifier TEXT, 
	type vertex_type, --ENUM
	properties JSON, -- map in postgres
	PRIMARY KEY (identifier, type)
)

-- plays_against -> player connected to player on different teams
-- shares_team -> player connected to player play on same team
CREATE TYPE edge_type 
    AS ENUM ('plays_against', 'shares_team', 'plays_in', 'plays_on' );

CREATE TABLE edges (
	subject_identifier TEXT, 
	subject_type vertex_type, 
	object_identifier TEXT,
	object_type vertex_type,
	edge_type edge_type, 
	properties JSON,
	PRIMARY KEY (subject_identifier,
	             subject_type,
				 object_identifier,
				 object_type,
				 edge_type)
);

-- Create game as a vertex type (do select and then add to ertices table) 
INSERT INTO vertices
SELECT 
	   game_id AS identifier,
	   'game'::vertex_type AS type,
	   json_build_object(
			'pts_home', pts_home,
			'pts_away', pts_away,
			'winning_team', CASE WHEN home_team_wins = 1 THEN home_team_id ELSE visitor_team_id END
	   ) as properties
  FROM games;

SELECT * FROM vertices
 WHERE type = 'game'

-- add aggregate stats into the table 
INSERT INTO vertices
WITH players_agg AS (
	SELECT player_id AS identifier,
		   MAX(player_name) AS player_name,
		   COUNT(1) AS number_of_games, 
		   SUM(pts) AS total_points,
		   ARRAY_AGG(DISTINCT team_id) AS teams
	FROM game_details
	GROUP BY player_id
)
SELECT identifier,
	   'player'::vertex_type,
	   json_build_object(
			'player_name', player_name,
			'number_of_games', number_of_games,
			'total_points', total_points, 
			'teams', teams
	   )
From players_agg

-- check it was added
SELECT * FROM vertices
 WHERE type = 'player';

-- NOTE: teams has duplicated rows so we use teams deduped to only select the first entry for the team_id 

INSERT INTO vertices
WITH teams_deduped AS (
	SELECT *, 
		   ROW_NUMBER() OVER(PARTITION BY team_id) AS row_num
	  FROM teams
)
	SELECT team_id AS identifier,
		   'team'::vertex_type AS type,
		   json_build_object(
				'abbreviation', abbreviation,
				'nickname', nickname,
				'city', city,
				'arena', arena,
				'year_founded', yearfounded
		   )
	  FROM teams_deduped
	 WHERE row_num = 1;

-- check it was added
SELECT * FROM vertices
 WHERE type = 'team';


-- Now have all of the vertices!! to see the # in each type
SELECT type,
	   COUNT(1)
  FROM vertices
 GROUP BY type


-- Now adding to the edges table! 
-- Note we have deuplicated rows in game_details 
SELECT player_id, 
       game_id, 
       COUNT(1) AS tes
  FROM game_details 
 GROUP BY player_id, game_id
 HAVING COUNT(1) > 1;

INSERT INTO edges
WITH deduped AS (
		SELECT *,
			   ROW_NUMBER() OVER(PARTITION BY player_id, game_id) AS row_num
		 FROM game_details
)
SELECT 
		player_id AS subject_identifier,
		'player'::vertex_type as subject_type,
		game_id AS object_identifier, 
		'game'::vertex_type AS object_type, 
		'plays_in'::edge_type AS edge_type,
		json_build_object(
			'start_position', start_position,
			'pts',pts,
			'team_id', team_id,
			'team_abbreviation', team_abbreviation
		) as properties
FROM deduped
WHERE row_num = 1

SELECT *
  FROM vertices v

SELECT *
  FROM edges e 

SELECT *
  FROM vertices v 
  JOIN edges e
    ON e.subject_identifier = v.identifier
   AND e.subject_type = v.type

-- NOTE: need to cast the pts as an integer otherwise just take it as a str
SELECT v.properties->>'player_name',
	   MAX(CAST(e.properties->>'pts' AS INTEGER))
  FROM vertices v 
  JOIN edges e
    ON e.subject_identifier = v.identifier
   AND e.subject_type = v.type
 GROUP BY 1
 ORDER BY 2 DESC

-- creating edge of plays_against, plays_on
SELECT 
		player_id AS subject_identifier,
		'player'::vertex_type as subject_type,
		game_id AS object_identifier, 
		'game'::vertex_type AS object_type, 
		'plays_in'::edge_type AS edge_type,
		json_build_object(
			'start_position', start_position,
			'pts',pts,
			'team_id', team_id,
			'team_abbreviation', team_abbreviation
		) as properties
FROM deduped
WHERE row_num = 1
-- Need to create 2 edges wher x plays against y and another one with y plays_against x
-- ALSO includes the plays_on when plays in the same team (see team abbreviation CASE)

INSERT INTO edges
WITH deduped AS (
		SELECT *,
			   ROW_NUMBER() OVER(PARTITION BY player_id, game_id) AS row_num
		 FROM game_details
),
	filtered AS (
		SELECT * 
		  FROM deduped
		 WHERE row_num = 1
	),
	aggregated AS (
	 SELECT f1.player_id AS subject_player_id,
		   f2.player_id AS object_player_id,
		   CASE WHEN f1.team_abbreviation = f2.team_abbreviation 
		   		THEN 'shares_team'::edge_type
				ELSE 'plays_against'::edge_type
		    END AS edge_type,
			COUNT(1) as num_games,
			MAX(f1.player_name) AS subject_player_name,
			MAX(f2.player_name) AS object_player_name,
			SUM(f1.pts) AS subject_points,
			SUM(f2.pts) AS object_points
	  FROM filtered  AS f1
	  JOIN filtered  AS f2
	    ON f1.game_id = f2.game_id
	   AND f1.player_name <> f2.player_name
	 WHERE f1.player_name > f2.player_name
	 GROUP BY 
	 	   f1.player_id,
		   f2.player_id,
		   CASE WHEN f1.team_abbreviation = f2.team_abbreviation 
		   		THEN 'shares_team'::edge_type
				ELSE 'plays_against'::edge_type
		    END
	)
	SELECT subject_player_id AS subject_identifier,
		   'player'::vertex_type AS subject_type, 
		   object_player_id AS object_identifier, 
		   'player'::vertex_type AS object_type,
		   edge_type AS edge_type,
		   json_build_object(
				'num_games', num_games,
				'subject_points', subject_points,
				'object_points', object_points
		   )
	  FROM aggregated
	
-- SUbject & object doesn't matter in this case since double sided edge (could keep all or only pick 1 set of edges)
-- use the WHERE f1.player_name > f2.player_name --> this will only select 1 row for each 'pair' of equal but opposite edges

	   
SELECT v.properties->>'player_name' AS player_name,
       e.object_identifier AS object_identifier,
	   CAST(v.properties->>'number_of_games' AS REAL)/
	   CASE WHEN CAST(v.properties->>'total_points' AS REAL) = 0 
	        THEN 1 
		    ELSE CAST(v.properties->>'total_points' AS REAL)
	    END AS career_av ,
	   e.properties->>'subject_points' AS subject_points,
	   e.properties->>'num_games' AS num_games
  FROM vertices v 
  JOIN edges e 
    ON v.identifier = e.subject_identifier
   AND v.type = e.subject_type 
 WHERE e.object_type = 'player'::vertex_type







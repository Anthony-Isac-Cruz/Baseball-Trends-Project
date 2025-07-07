/* Step 1: Replace original table with a new one and check for null values*/

CREATE OR REPLACE TABLE `your_project.your_dataset.baseball_schedules` AS
SELECT * FROM `bigquery-public-data.baseball.schedules`;

SELECT
  COUNT(*) AS total_rows,
  COUNTIF(gameId IS NULL) AS null_gameId,
  COUNTIF(gameNumber IS NULL) AS null_gameNumber,
  COUNTIF(duration_minutes IS NULL) AS null_duration_minutes,
  COUNTIF(attendance IS NULL) AS null_attendance,
  COUNTIF(startTime IS NULL) AS null_startTime,
  COUNTIF(status IS NULL) AS null_status
FROM `your_project.your_dataset.baseball_schedules`;



/* Step 2: Data Cleaning & Type Normalization */

CREATE OR REPLACE TABLE `your_project.your_dataset.cleaned_baseball_schedules` AS
WITH cleaned AS (
  SELECT
    gameId,
    gameNumber,
    SAFE_CAST(seasonId AS INT64) AS seasonId,
    year,
    type,
    dayNight,
    SAFE_CAST(duration_minutes AS INT64) AS duration_minutes,
    homeTeamId,
    homeTeamName,
    awayTeamId,
    awayTeamName,
    startTime,
    SAFE_CAST(attendance AS INT64) AS attendance,
    status,
    created
  FROM `your_project.your_dataset.baseball_schedules`
  WHERE gameId IS NOT NULL AND startTime IS NOT NULL
)
SELECT * FROM cleaned;

-- Add helper columns

CREATE OR REPLACE TABLE `your_project.your_dataset.enriched_baseball_schedules` AS
WITH enriched AS (
  SELECT *,
    EXTRACT(DAYOFWEEK FROM startTime) AS day_of_week,
    EXTRACT(MONTH FROM startTime) AS game_month,
    FORMAT_DATE('%A', DATE(startTime)) AS day_name,
    CASE 
      WHEN EXTRACT(DAYOFWEEK FROM startTime) IN (1, 7) THEN 'Weekend'
      ELSE 'Weekday'
    END AS game_day_type,
    CASE 
      WHEN attendance IS NULL THEN 0 
      ELSE attendance 
    END AS attendance_filled
  FROM `your_project.your_dataset.cleaned_baseball_schedules`
)
SELECT * FROM enriched;


/* Step 3: Adding MLB Table values */


CREATE OR REPLACE TABLE `baseballtrends.DSI.enriched_baseball_schedules_v2` AS
WITH EnrichedCombined AS (
  SELECT 
    A.*,  -- include all original columns
    C.City AS HomeTeamCity,
    B.City AS AwayTeamCity,
    C.Division AS HomeDivision,
    B.Division AS AwayDivision,
    CASE 
      WHEN C.Division LIKE '%NL%' THEN 'National'
      WHEN C.Division LIKE '%AL%' THEN 'American'
    END AS HomeConference,
    CASE 
      WHEN B.Division LIKE '%NL%' THEN 'National'
      WHEN B.Division LIKE '%AL%' THEN 'American'
    END AS AwayConference
  FROM `baseballtrends.DSI.enriched_baseball_schedules_source` A 
  LEFT JOIN `baseballtrends.DSI.MLB_City` C 
    ON C.Team_Name = A.homeTeamName
  LEFT JOIN `baseballtrends.DSI.MLB_City` B 
    ON B.Team_Name = A.awayTeamName
)
SELECT * FROM EnrichedCombined;


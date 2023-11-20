WITH
  winsLosses AS(
  SELECT
    DISTINCT gameId,
    homeTeamName,
    awayTeamName,
    homeFinalRuns,
    awayFinalRuns,
    CASE
      WHEN homeFinalRuns > awayFinalRuns THEN 1
    ELSE
    0
  END
    AS homeWins,
    CASE
      WHEN homeFinalRuns < awayFinalRuns THEN 1
    ELSE
    0
  END
    AS homeLosses,
    CASE
      WHEN awayFinalRuns > homeFinalRuns THEN 1
    ELSE
    0
  END
    AS awayWins,
    CASE
      WHEN awayFinalRuns < homeFinalRuns THEN 1
    ELSE
    0
  END
    AS awayLosses
  FROM
    `ca-qulijing-edu.data_task.csv_from_gcs`
  WHERE
    homeTeamName NOT IN ('American League')
    AND awayTeamName NOT IN ('National League') )
SELECT
  CURRENT_DATE() AS standings_date,
  teamName,
  SUM(wins) AS wins,
  SUM(losses) AS losses,
  SUM(wins) / (SUM(wins) + SUM(losses)) AS winning_percentage,
  DENSE_RANK() OVER (ORDER BY (SUM(wins) / (SUM(wins) + SUM(losses))) DESC) AS standings
FROM (
  SELECT
    homeTeamName AS teamName,
    homeWins AS wins,
    homeLosses AS losses
  FROM
    winsLosses
  UNION ALL
  SELECT
    awayTeamName AS teamName,
    awayWins AS wins,
    awayLosses AS losses
  FROM
    winsLosses ) AS unionResult
GROUP BY
  teamName
ORDER BY
  standings;
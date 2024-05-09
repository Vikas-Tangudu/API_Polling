CREATE OR REPLACE VIEW
  `{{params.project_id}}.{{params.dataset}}.stats` AS
SELECT
  _sync_unix_timestamp,
  DATETIME(TIMESTAMP_SECONDS(_sync_unix_timestamp),
    "America/New_York") AS _sync,
  DATE(CAST(year AS int64),CAST(month AS int64),CAST(day AS int64)) AS date_utc,
  CAST( year AS int64) AS year,
  CAST( month AS int64) AS month,
  CAST( day AS int64) AS day,
  weekday,
  CAST( sent AS int64) AS sent,
  CAST( delivered AS int64) AS delivered,
  CAST( opened AS int64) AS opened,
  CAST( responded AS int64) AS responded,
  CAST( promoters AS int64) AS promoters,
  CAST( passives AS int64) AS passives,
  CAST( detractors AS int64) AS detractors,
  CAST( nps AS float64) AS nps,
  CAST( comments AS int64) AS comments,
  CAST( comment_length AS int64) AS comment_length,
  CAST( comment_responded_percent AS float64) AS comment_responded_percent,
  CAST( surveys_responded_percent AS float64) AS surveys_responded_percent,
  CAST( surveys_delivered_responded_percent AS float64) AS surveys_delivered_responded_percent,
  CAST( surveys_opened_responded_percent AS float64) AS surveys_opened_responded_percent
FROM
  `{{params.project_id}}.{{params.dataset}}._raw_stats`
ORDER BY
  date_utc DESC
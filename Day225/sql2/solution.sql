WITH ordered AS (
  SELECT 
      log_date,
      LAG(log_date) OVER (ORDER BY log_date) AS prev_date
  FROM service_log_361
),
gaps AS (
  SELECT 
      date(prev_date, '+1 day') AS start_date,
      date(log_date, '-1 day') AS end_date,
      JULIANDAY(log_date) - JULIANDAY(prev_date) - 1 AS days
  FROM ordered
  WHERE prev_date IS NOT NULL
    AND JULIANDAY(log_date) - JULIANDAY(prev_date) > 1
)
SELECT *
FROM gaps
ORDER BY start_date;
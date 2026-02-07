WITH cte AS (
    SELECT 
        *,
        LAG(timestamp) OVER (PARTITION BY cust_id ORDER BY timestamp) AS prev
    FROM cust_tracking
)
SELECT 
    cust_id,
    SUM(timestamp - prev) / 3600.0 AS total_hours
FROM cte
WHERE state = 0
GROUP BY cust_id
ORDER BY cust_id;
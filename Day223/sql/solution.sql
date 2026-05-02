WITH cte AS (
    SELECT *,
           LAG(event_timestamp) OVER (
               PARTITION BY user_id ORDER BY event_timestamp
           ) AS prev_ts
    FROM user_events_583
),
cte2 as(
SELECT *,
CASE 
    WHEN prev_ts IS NULL OR 
         (strftime('%s', event_timestamp) - strftime('%s', prev_ts)) > 1800
    THEN 1 
    ELSE 0 
END AS flag
FROM cte
)
  ,cte3 as(
select *,sum(flag) over(partition by user_id order by event_timestamp) as SESSION
from cte2
),
cte4 as (
select user_id, session, count(*) as n_events,(strftime('%s', max(event_timestamp)) - (strftime('%s', min(event_timestamp)))) /60 as duration
from cte3
group by 1,2
)
select *, 
case when n_events=1 then 'Yes' 
else 'No' end as bounce
from cte4
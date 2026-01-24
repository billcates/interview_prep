-- Write your PostgreSQL query statement below
with cte as(
    select player_id,min(event_date) as event_Date
    from activity
    group by 1
)
select round(sum(case when b.event_date is not null then 1 else 0 end)*(1.0)/(count(*)),2) as fraction
from cte a
left join activity b on a.player_id=b.player_id
and (a.event_date + INTERVAL '1 day')=b.event_date

--simpler way

SELECT COUNT(DISTINCT player_id)
FROM (
    SELECT 
        player_id,
        event_date,
        event_date - LAG(event_date) 
            OVER (PARTITION BY player_id ORDER BY event_date) AS diff
    FROM activity
) t
WHERE diff = INTERVAL '1 day';

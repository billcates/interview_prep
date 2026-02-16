with cte as(
select *,sum(session) over(partition by user_id order by record_date,rk)as session_id
from (
select *, case when (record_date-lag(record_date) over(partition by user_id order by record_date))=1 then 0 else 1 end as session,
row_number() over(partition by user_id order by record_date) as rk
from sf_events)
)
select user_id
from cte
group by user_id,session_id having count(1)>=3

--range between

SELECT DISTINCT user_id
FROM (
    SELECT
        user_id,
        record_date,
        COUNT(*) OVER (
            PARTITION BY user_id
            ORDER BY record_date
            RANGE BETWEEN INTERVAL '2 days' PRECEDING AND CURRENT ROW
        ) AS cnt
    FROM (
        SELECT DISTINCT user_id, record_date
        FROM sf_events
    ) d
) t
WHERE cnt >= 3;

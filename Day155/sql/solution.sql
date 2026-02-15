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
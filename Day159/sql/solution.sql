with cte as(
select 
*,
case when (watch_date-lag(watch_date,1) over(partition by user_id order by watch_date))=1 then 0 else 1 end as session
from user_watch_activity
),
cte2 as(
select *,sum(session) over(partition by user_id order by watch_date)  as session_id
from cte
),
cte3 as(
select *,rank() over(partition by user_id order by ct desc) as rk 
from(
select user_id, count(*) as ct
from cte2
group by user_id, session_id
)
)
select user_id,ct from cte3
where rk=1
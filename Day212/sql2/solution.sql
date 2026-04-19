with cte as(
select *, lag(event_time) over(partition by user_id order by event_time) as prev_time,
event_time - lag(event_time) over(partition by user_id order by event_time) as time_diff
from user_events
),
cte2 as(
select *, case when time_diff> interval '30 minutes' then 1 when time_diff is NULL then 1 else 0 end  as flag
from cte
),
cte3 as(
select *,sum(flag) over(partition by user_id order by event_time) as session_id
from cte2
)
select user_id,session_id, min(event_time) as min_time,max(event_time) as max_time,count(1) as ct_events
,max(event_time)-min(event_time) as duration
from cte3
group by 1,2
with cte as(
select *,extract(month from time_id) as mn 
from fact_events
)
select client_id, mn as month,count(distinct user_id) as users_num
from cte
group by 1,2
order by 1,2
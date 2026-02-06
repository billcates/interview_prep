with cte as(
select user_id,
(sum( case when event_type in  ('video call received', 'video call sent', 'voice call received', 'voice call sent') then 1 else 0 end)*100)/count(*) as pct
from fact_events
group by 1
)
select client_id
from fact_events
where user_id in (select user_id from cte where pct>50)
group by client_id
order by count(1) desc
limit 1
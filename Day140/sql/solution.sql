-- Write your PostgreSQL query statement below
with cte as(
select 
    machine_id, 
    process_id, 
    sum(case when activity_type='end' then timestamp end )- sum(case when activity_type='start' then timestamp end ) as ts
from activity
group by 1,2
)
select 
    machine_id,
    round(avg(ts)::numeric,3) as processing_time
from cte
group by 1
order by 1
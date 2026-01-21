-- Write your PostgreSQL query statement below
with cte as(
select a.user_id,b.time_stamp,b.action
from signups a
left join confirmations b on a.user_id=b.user_id
)
select user_id,round( (sum(case when action='confirmed' then 1 else 0 end))*(1.0)/(count(*)),2) as confirmation_rate
from cte
group by 1
-- Write your PostgreSQL query statement below
select activity_date as day,count(distinct user_id) as active_users
from activity
where activity_date<='2019-07-27'::date
and '2019-07-27'::date - activity_date<30
group by 1
order by 1
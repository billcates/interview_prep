with cte as(
select *,rank() over(partition by user_id order by t_minutes desc) as rk
from (
    select user_id,genre,sum(watch_minutes) as t_minutes 
    from watch_Events
    group by 1,2
)
)
select user_id,genre,t_minutes from cte where rk=1
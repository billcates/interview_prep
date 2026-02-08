with cte as(
select distinct user_id,date_visited
from user_streaks
where date_visited<='2022-08-10'
),
cte2 as(
select user_id,date_visited, case when date_visited-lag(date_visited) over(partition by user_id order by date_visited)=1 then 0 else 1 end as flag
from cte
),
cte3 as(
select *,sum(flag) over(partition by user_id order by date_visited) as streak_id
from cte2
),cte4 as(
select user_id,streak_id,count(*) as ct
from cte3
group by 1,2
),
cte5 as(
select *,dense_rank() over(order by ct desc) as rk
from cte4
)
select user_id,ct as streak_length
from cte5
where rk<=3
with cte as(
select *,
case when match_result='W' and
lag(match_result) over(partition by player_id order by match_date)='W' then 0 else 1 end as new_group
from players_results
),
cte2 as(
select *,sum(new_group) over(partition by player_id order by match_date) as grp
from cte
),
cte3 as(
select player_id,grp, count(1) as ct
from cte2
group by 1,2
)
select player_id,ct as longest_win_streak
from (
select *, rank() over( order by ct desc) as rk
from cte3
)
where rk=1
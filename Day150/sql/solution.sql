with cte as(
select video_id,count(1) as ct
from user_flags a
join flag_review b on a.flag_id=b.flag_id
group by 1
),
cte2 as(
select video_id, rank() over(order by ct desc) rk
from cte
)
select d.video_id,sum(case when reviewed_by_yt=True then 1 else 0 end) as num_flags_reviewed_by_yt
from user_flags d
join flag_review c on c.flag_id=d.flag_id
join cte2 e on d.video_id=e.video_id and e.rk=1
group by 1
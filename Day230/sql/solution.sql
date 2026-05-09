with cte as(
SELECT speaker_name,room_name,dense_rank() over(order by speaker_name) as r_s,
dense_rank() over(order by room_name) as r
from conference_slots_645
)
select speaker_name,room_name
from cte
where r_s=r
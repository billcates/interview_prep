with cte as(
select sum(center_north) as center_north,
sum(center_south) as center_south,
sum(center_east) as center_east,
sum(center_west) as center_west
from product_matrix_385
)
,cte2 as(
select 'North' as centre,center_north as ct
from cte 
union all
select 'South',center_south
from cte 
union all 
select 'West',center_west
from cte 
union all
select 'East',center_east
from cte 
)
select *, rank() over(order by ct desc) as rk
from cte2
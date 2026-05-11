with cte as(
select *, 
max(case when region is not null then row_id else 0 end ) over(order by row_id) as flag
from delivery_log_851
)
select b.region,b.city,a.package_code
from cte a 
join delivery_log_851 b on a.flag=b.row_id
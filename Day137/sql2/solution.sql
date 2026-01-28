-- Write your PostgreSQL query statement below
with cte as(
    select lat,lon,sum(pid) as pid
    from insurance
    group by 1,2 having count(1)=1
),
cte2 as(
    select distinct a.*
    from insurance a
    join insurance b on a.tiv_2015=b.tiv_2015 and a.pid<>b.pid
)
select ROUND(SUM(tiv_2016)::numeric, 2) AS tiv_2016
from cte2 a
join cte b on a.pid=b.pid
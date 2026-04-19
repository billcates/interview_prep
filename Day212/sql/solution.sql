with cte as(
SELECt a.employee_id, sum(case when b.query_id is not NULL then 1 else 0 end ) as unique_queries
FROM employees a 
left join queries b on a.employee_id=b.employee_id
and query_starttime BETWEen '07-01-2023' and '10-01-2023'
group by 1
)
select unique_queries,count(1)
from cte
group by 1 order by 1

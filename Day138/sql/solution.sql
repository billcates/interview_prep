-- Write your PostgreSQL query statement below
with cte as(
select a.*,b.name as department,dense_rank() over(partition by a.departmentId order by salary desc) as rk
from employee a
join department b on a.departmentId=b.id
)
select department,name as employee, salary
from cte where rk<=3
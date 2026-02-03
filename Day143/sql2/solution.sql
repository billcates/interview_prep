-- Write your PostgreSQL query statement below
with cte as(
select
employee_id,count(distinct department_id) as ct
from employee
group by 1
)
select a.employee_id,b.department_id
from cte a
join employee b on a.employee_id=b.employee_id
where (ct=1) or(primary_flag='Y')
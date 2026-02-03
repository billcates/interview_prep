-- Write your PostgreSQL query statement below
with cte as(
    select reports_to as employee_id,count(*) as reports_count,round(avg(age),0) as average_age
    from employees
    where reports_to is not NULL
    group by 1
)
select a.employee_id,b.name,a.reports_count,a.average_age
from cte a 
join employees b on a.employee_id=b.employee_id
order by 1
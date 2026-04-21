with cte as(
select *,dense_rank() over(partition by department_id order by salary desc) rk
from employee
)
select d.department_name,c.name,c.salary 
from cte c 
join department d on c.department_id=d.department_id
where rk<=3
order by 1,3 desc,2
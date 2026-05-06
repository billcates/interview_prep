with cte as(
select 
*,
JULIANDAY(hire_date)-JULIANDAY((lag(hire_date) over(order by hire_date))) as day_diff
from employees
)
select first_name,last_name,day_diff
from cte
where day_diff >300
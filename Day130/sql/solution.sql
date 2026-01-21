select salary
from employee
where salary not in(select max(salary) from employee)
order by salary desc
limit 1;
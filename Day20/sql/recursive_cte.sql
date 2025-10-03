with recursive cte as(
    select 
        emp_id,
        emp_name,
        emp_name::text as manager_chain 
    from employees
    where manager_id is NULL

union ALL

    select 
        a.emp_id,
        a.emp_name, 
        CONCAT(b.manager_chain, '->', a.emp_name) AS manager_chain
    from employees A
    join cte b on a.manager_id=b.emp_id
)
select * from cte


----part 2

WITH RECURSIVE hierarchy AS (
SELECT 
    emp_id,
    emp_name,
    manager_id,
    salary,
    emp_id::text as path
FROM employees
where manager_id is NULL

    UNION ALL    

SELECT 
    e.emp_id,
    e.emp_name,
    e.manager_id,
    e.salary,
    h.path::text ||'-'|| e.emp_id::text as path
FROM employees e
JOIN hierarchy h ON e.manager_id = h.emp_id
)
SELECT 
    parent.emp_name,
    parent.emp_id,
    sum(child.salary) as total_salary
from HIERARCHY parent
join HIERARCHY child
on child.path like parent.path || '%'
GROUP by 1,2
having count(child.emp_id)>1

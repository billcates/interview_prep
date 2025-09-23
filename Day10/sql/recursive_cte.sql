with RECURSIVE cte as(
  select 
    manager_id,
    'ceo' as manager_name,
    emp_id,
    emp_name,
     0 as level 
  from employees
  where manager_id is NULL
union all
  select 
    b.emp_id as manager_id,
    cast (b.emp_name as text) as manager_name,
    a.emp_id,
    a.emp_name,
    b.level+1 
  from employees a 
  join cte B 
  on a.manager_id=b.emp_id
 )
 select * from cte
 order by emp_id
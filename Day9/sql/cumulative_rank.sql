with cte as (
  select *,
  sum(sale_amount) over(partition by employee_id,region order by sale_date) as cumulative_sales
  from employee_sales
)
select *,
	rank() over(partition by region order by cumulative_sales desc) as running_rank
from cte
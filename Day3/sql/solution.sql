with cte as(
select region,employee_id, sum(sale_amount) as total_sales
from employee_sales
WHERE sale_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
group by region,employee_id
),
cte2 as(
select *, rank() over(partition by region order by total_sales desc) as rn
from cte
)
select * from cte2 where rn<=3
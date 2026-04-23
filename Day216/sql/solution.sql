with cte as (
SELECT *,rank() over(partition by p.category_name order by sales_quantity desc,rating desc) rk
FROM products p 
join product_sales ps on p.product_id=ps.product_id
)
select category_name,product_name
from cte 
where rk=1
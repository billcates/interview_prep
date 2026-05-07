with cte as(
select order_id,order_date,sum(freight) over(order by order_id) as s
from orders
)
select * from cte 
where s BETWEEN 10000 and 11000

with cte as(
select store_id,sum(amount) as total_amount
from store_sales_655
group by 1
order by 2 desc
)
select b.store_name,b.region,a.total_amount,
rank() over(partition by region order by total_amount desc) as rk
from cte a 
join stores_655 b on a.store_id=b.store_id
order by total_amount desc
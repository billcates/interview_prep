with cte as(
select *, lag(unit_price,1,0)over(partition by product_name order by effective_date) as prev_price
from price_history_659
)
select product_name,effective_date,unit_price, prev_price, unit_price-prev_price as diff,
case when prev_price>unit_price then "Decrease"
when prev_price=unit_price then "Unchanged"
else "Increase" end as direction
from cte
where prev_price!=0
order by 1,2
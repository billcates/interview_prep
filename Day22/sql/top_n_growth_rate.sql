with cte as(
    select *,
        AVG(revenue) over(partition by product_id order by event_date rows BETWEEN 6 PRECEDING and current row) as current_7d_avg,
        avg(revenue) over(partition by product_id order by event_date rows BETWEEN 13 PRECEDING and 7 preceding) as prev_7d_avg
    from product_sales
),
cte2 as(
    select 
        product_id,
        event_date,
        round(current_7d_avg,2),
        round(prev_7d_avg,2),
        round((current_7d_avg-prev_7d_avg)/(COALESCE(prev_7d_avg)),2) as growth_rate
    from cte
)
select * 
from cte2
WHERE event_date = (SELECT MAX(event_date) FROM product_sales)
and growth_rate is not null and growth_rate>0
order by growth_rate desc limit 3
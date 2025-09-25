with cte as(
    select *,to_char(date_trunc('MONTH',order_date),'yyyy-MM') as month
    from orders
)
select 
    customer_id,
    order_date,
    amount, 
    sum(amount) over(partition by customer_id, month order by order_date) as running_monthly_sum,
    PERCENT_RANK() over(partition by customer_id, month order by amount) as percentile_rank
from cte
order by customer_id,order_date
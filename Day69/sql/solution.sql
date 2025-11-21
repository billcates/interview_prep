with cte as(
    select 
        to_char(date_trunc('month',order_date),'yyyyMM') as _month,
        sum(amount) as monthly_amount
    from orders
    group by 1
    order by 1
),
cte2 as(
    select 
        _month,
        monthly_amount,
        lag(monthly_amount,1) over(order by _month) as prev_month_amount
    from cte
)
select 
    *,
    monthly_amount-prev_month_amount as revenue_diff
from cte2
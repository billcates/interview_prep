with cte as(
select 
    user_id,
    date_trunc('month',order_date) as _month,
    sum(order_amount) as monthly_total
    from orders
    group by user_id,date_trunc('month',order_date)
),
cte2 as(
select 
    user_id,
    to_char(_month,'YYYY-MM') as month,
    monthly_total, 
    lag(monthly_total,1) over(partition by user_id order by _month) as previous_month_total
from cte
)
select *, 
    case when previous_month_total is NULL or previous_month_total=0 then NULL
    else round((((monthly_total-previous_month_total)/previous_month_total)*100),2)
    end as growth_percent
from cte2;
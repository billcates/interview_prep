with cte as (
    select *,
    to_char(order_date,'YYYY-mm') as _month
    from orders
),
cte2 as(
    select user_id,_month,round(avg(order_amount),2) as spend
    from cte
    group by 1,2
),
cte3 as (
    select *,
        lag(_month) over (partition by user_id order by _month) as prev_month,
        lag(spend) over (partition by user_id order by _month) as prev_avg_spend,
        (spend- lag(spend) over (partition by user_id order by _month)) as increase,
        sum(spend) over (partition by user_id) as total_user_spend
    from cte2
),
cte4 as (
    select *,
    row_number() over(partition by user_id order by increase desc) as rn 
    from cte3
)
select user_id,_month,prev_month,prev_Avg_spend,spend as avg_spend,increase
from  cte4
where rn=1 
order by increase desc, total_user_spend DESC
limit 3
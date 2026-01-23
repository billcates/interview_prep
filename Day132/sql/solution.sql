-- Write your PostgreSQL query statement below
with cte as(
    select *, row_number() over(partition by customer_id order by order_date) as rn
    from delivery
)
select 
round((sum(case when order_date=customer_pref_delivery_date then 1 else 0 end)*(100.0))/(count(*)),2) as immediate_percentage 
from cte 
where rn=1
with cte as(
    select 
        a.order_id,
        a.product_id as a,
        b.product_id as b 
    from order_items a
    join order_items b on a.order_id=b.order_id and a.product_id<b.product_id
)
select a,b, count(1) as affinity
from cte
group by 1,2
having count(1)>1
with cte as(
    select *, row_number() over(partition by customer_id order by order_date desc) as rn
    from orders
)
select order_id,customer_id,order_date,order_amount
from cte where rn<=2

--Follow up:
-- To be certain in the ordering of recent two orders even on ties, ordering by both order_date desc and order_id desc
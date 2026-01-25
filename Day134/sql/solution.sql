-- Write your PostgreSQL query statement below
with cte as(
    select product_id,new_price,row_number() over(partition by product_id order by change_date desc) rk
    from products
    where change_date<='2019-08-16'
    ),
cte2 as(
    select b.product_id, a.new_price as price
    from (select distinct product_id from products) b
    left join cte a on a.product_id=b.product_id
    where (a.rk=1 or a.rk is null)
)
select product_id,coalesce(price,10) as price
from cte2


--simpler way

select distinct product_id, 10 as price from Products where product_id not in(select distinct product_id from Products where change_date <='2019-08-16' )
union 
select product_id, new_price as price from Products where (product_id,change_date) in (select product_id , max(change_date) as date from Products where change_date <='2019-08-16' group by product_id)
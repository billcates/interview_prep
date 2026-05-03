with cte as(
select a.product_id as p1,b.product_id as p2,count(1) as n
from basket_order_items_265 a 
join basket_order_items_265 b on a.order_id=b.order_id and a.product_id<b.product_id
group by 1,2
),
cte2 as(
  select product_id,count(distinct order_id) as total_orders
  from basket_order_items_265
  group by 1
)
select b.product_name,c.product_name,a.n, round((a.n*100.0)/(select count(distinct order_id) from basket_order_items_265),1) as s,
round((a.n*100.0)/d.total_orders,1) as c1,
round((a.n*100.0)/e.total_orders,1) as c2
from cte a
left join basket_products_265 b on a.p1=b.product_id
left join basket_products_265 c on a.p2=c.product_id
left join cte2 d on a.p1=d.product_id
left join cte2 e on a.p2=e.product_id
where a.n>2
order by 3 desc,4 desc
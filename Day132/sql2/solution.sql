-- Write your PostgreSQL query statement below
with cte as(
    select product_id,min(year) as year
    from sales
    group by 1
)
select a.product_id,a.year as first_year, a.quantity, a.price
from sales a
join cte b on a.product_id=b.product_id and a.year=b.year
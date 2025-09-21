with cte as (
    select 
        a.*,
        b.category
    from sales a
    join products B
    on a.product_id=b.product_id
    where sale_date >= cast('2025-03-01' as TIMESTAMP) - INTERVAL '90 days'
),
cte2 as(
    select 
        customer_id,
        category,
        sum(amount) as total_sales_amount,
        count(DISTINCT product_id) as unique_products
    from cte
    group by 1,2
)
select *,
rank() over(partition by category order by total_sales_amount desc) as rank_in_category
from cte2
select sale_month,
sum(case when category='Electronics' then revenue else 0 end) as Electronics,
sum(case when category='Clothing' then revenue else 0 end) as Clothing,
sum(case when category='Home' then revenue else 0 end) as Home,
sum(revenue) as total_revenue
from monthly_sales_365
group by 1
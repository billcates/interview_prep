with RECURSIVE cte as(
select sale_date as dt 
from sales_sparse_752
where sale_date=(select min(sale_date) from sales_sparse_752)

union all 

select date(dt,'+1 day')
from cte 
where dt<(select max(sale_date) from sales_sparse_752)
)
select a.dt,coalesce(b.revenue,0) as revenue
from cte a
left join sales_sparse_752 b on a.dt=b.sale_Date
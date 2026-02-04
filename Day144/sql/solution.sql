-- Write your PostgreSQL query statement below
select sell_date,count(*) as num_sold,string_agg(distinct product,',' Order by product) as products
from activities
group by 1
order by 1
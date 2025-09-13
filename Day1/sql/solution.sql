--join results

| 1           | 2024-01-01 || 1           | 100    |
| 1           | 2024-01-01 || 1           | 200    |
| 1           | 2024-01-15 || 1           | 100    |
| 1           | 2024-01-15 || 1           | 200    |
| 2           | 2024-01-20 || 2           | 150    |

duplicate records would be formed, leading to the wrong calculation of sum amount


--proposed solution
--simplest way

select customer_id,sum(amount) as total_payments
from payments
group by customer_id

--assuming some other details need to be fetched like cusotmer_name,cte can be used

with cte as (
    select customer_id,sum(amount) as total_payments
    from payments
    group by customer_id
)
select * from employee a
join cte c 
on a.customer_id=c.customer_id

--Rule of thumb => always perform aggregation before joins
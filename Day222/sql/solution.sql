with cte as(select account_id,txn_date,sum(amount) as total
from transactions_660
group by 1,2
),
  cte2 as(
select *,
avg(total)over(partition by account_id order by txn_date rows between 6 preceding and current row) as rolling_sum
from cte
),
  cte3 as(
select *, case when total>=3*rolling_sum then 100.0 *(total/rolling_sum) else 0 end as spike
from cte2
)
select * from cte3 where spike >0
order by spike desc
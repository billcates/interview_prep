with cte as(
select extract(month from invoicedate) as month,stockcode,description, sum(unitprice * quantity) as total_paid
from online_retail
where invoiceno not like 'C%'
group by 1,2,3
order by 1
),
cte2 as(
select *,rank() over(partition by month order by total_paid desc) as rk
from cte
)
select month,description,total_paid
from cte2 where rk=1
with cte as(
SELECT *,
count(*) over (partition by user_id order by filing_date range between interval '3 years' preceding and current row) as ct
FROM filed_taxes
where product like '%TurboTax%'
order by filing_id
)
select distinct user_id
from cte where ct>=3
order by 1
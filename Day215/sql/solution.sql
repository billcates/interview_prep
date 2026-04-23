with cte as(
SELECT ticker,to_char(date_trunc('MONTH',date),'Mon-YYYY') as mth,open,
rank() over(partition by ticker order by open desc) rk,
rank() over(partition by ticker order by open) rk_low
FROM stock_prices
)
select ticker, 
max(case when rk=1 then mth else NULL end) as highest_mth,
max(case when rk=1 then open else NULL end) as highest_open,
max(case when rk_low=1 then mth else NULL end) as lowest_mth,
max(case when rk_low=1 then open else NULL end) as lowest_open
from cte 
group by 1
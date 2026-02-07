select * 
from fraud_score a
where fraud_score>=(
select percentile_cont(0.95) within group(order by fraud_score)
from fraud_score b
where a.state=b.state
)
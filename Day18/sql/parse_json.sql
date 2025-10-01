with cte as(
select user_id,event_type,
	metadata ->> 'page' as page,
    metadata ->> 'campaign' as campaign,
    COALESCE(metadata ->> 'price','0')::integer as price 
from event_logs
)
SELECT campaign,
    count(DISTINCT user_id) as distinct_users,
    sum(price) as total_price,
    round(
        COUNT(DISTINCT CASE WHEN event_type='purchase' THEN user_id END)::decimal/
        COUNT(DISTINCT CASE WHEN event_type='click' THEN user_id END)::decimal
        *100
    ,2) as conversion_rate
from cte
group by campaign
order by 3 desc
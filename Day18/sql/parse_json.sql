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
        (sum(case when event_type='purchase' then 1 else 0 end))::decimal(5,2)/
        (sum(case when event_type='click' then 1 else 0 end))*(100.0)
    ,2) as conversion_rate
from cte
group by campaign
order by 3 desc
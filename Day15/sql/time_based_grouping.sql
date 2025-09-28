with cte as(
    select a.*,b.signup_date,b.plan_type, 
        DATE_TRUNC('hour', event_time) + 
        INTERVAL '15 minutes' * FLOOR(EXTRACT(minute FROM event_time) / 15) as bucket_start_time
    from app_events a
    join subscriptions b 
    on a.user_id=b.user_id
 ),
cte2 as(
    select user_id,feature,bucket_start_time, plan_type,
    count(*) as usage_count
    from cte
    group by 1 ,2,3 ,4
    order by 1,2
),
cte3 as(
    select *, 
    lead(bucket_start_time,1) over(partition by user_id,feature order by bucket_start_time) as next_time
    from cte2
)
select 
    user_id,
    feature,
    bucket_start_time,
    usage_count,
    case when next_time = bucket_start_time + interval '15 minutes' then 1 else 0 end as returned_bucket,
    plan_type
from cte3
with cte as(
    select *, 
        case when source='mobile' then 1 
        when source='web' then 2 
        when source='email' then 3 
        end as priority,
        to_char(date_trunc('day',event_time),'yyyy-MM-dd') as event_date
    from user_events
),
cte2 as(
    select *,
        row_number()over(partition by user_id,event_type,event_date order by priority,event_time) as rn 
    from cte
)
select 
    user_id,
    event_date,
    event_type,
    source,
    event_time
from cte2 
where rn=1
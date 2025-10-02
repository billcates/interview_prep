with cte as(
    select 
        *,
        case 
            when (activity_date - (
                lag(activity_date) over(partition by user_id order by activity_date)))<= 1 
            then 0 else 1 end as new_session
    from user_activity
),
cte2 as (
    select *,sum(new_session) over(partition by user_id order by activity_date) as session_id
    from cte
)
select 
    user_id,
    min(activity_date) as start_date,
    max(activity_date) as end_date,
    max(activity_date)-min(activity_date)+1 as number_of_days
from cte2 
group by user_id,session_id
order by 1,2
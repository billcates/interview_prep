with cte as(
    select user_id,
    page_url,
    login_time,
    case when  (lag(login_time,1) over (partition by user_id order by login_time) is NULL) or 
    ((login_time - lag(login_time,1) over (partition by user_id order by login_time)) > INTERVAL '30 minutes') then 1
    else 0 
    end as session_ct
),
cte2 as (
    select 
    user_id,
    page_url,
    login_time,
    sum(session_ct) over(partition by user_id order by login_time) as session_id
)
select * from cte2
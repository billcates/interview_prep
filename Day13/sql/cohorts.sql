with cte as(
    select *,
    to_char(date_trunc('Month',signup_date),'YYYY-mm') as month,
    1 as cohort_0,
    case when plan_end is NUll Or ((date_trunc('Month',signup_date) + INTERVAL '1 Month')<plan_end) then 1 else 0 end as cohort_1,
    case when plan_end is NUll OR ((date_trunc('Month',signup_date) + INTERVAL '2 Month')<plan_end) then 1 else 0 end as cohort_2
    from customers
),
cte2 as(
    select month, sum(cohort_0)as offset_0,sum(cohort_1) as offset_1, sum(cohort_2) as offset_2
    from cte
    group by 1
    order by 1
),
cte3 as(
    select month, 0 as month_offset, offset_0 as active_users
    from cte2
    union ALL
    select month, 1 as month_offset, offset_1 as active_users
    from cte2
    union ALL
    select month, 2 as month_offset, offset_2 as active_users
    from cte2
)
select *
from cte3
order by month
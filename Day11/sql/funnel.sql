with cte as
(
    select *,
     lag(event_time,1) over(partition by user_id order by event_time) as previous_time
    from website_events
),
cte2 as(
    select *, 
        case when event_time-previous_time <= interval '30 minutes' then 0 else 1 end as FLAG
    from cte
),
cte3 as(
    select user_id,event_time,event_type,previous_time,
        sum(flag) over(partition by user_id order by event_time) as session_id
    from cte2
),
funnel AS (
    SELECT
        user_id,
        session_id,
        MIN(CASE WHEN event_type = 'view_product' THEN event_time END) AS first_view_time,
        MIN(CASE WHEN event_type = 'add_to_cart' THEN event_time END) AS first_add_to_cart_time,
        MIN(CASE WHEN event_type = 'checkout' THEN event_time END) AS first_purchase_time
    FROM cte3
    GROUP BY user_id, session_id
)
SELECT DISTINCT ON (user_id)
    user_id,
    first_view_time,
    first_add_to_cart_time,
    first_purchase_time
FROM funnel
WHERE first_purchase_time - first_view_time <= INTERVAL '30 minutes'
ORDER BY user_id, first_view_time;
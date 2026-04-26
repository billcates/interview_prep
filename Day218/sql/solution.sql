with cte as(
select *,
date_trunc('week',event_date) as week
from marketing_touches
),
cte2 as(
select *,
case when week-lag(week) over(partition by contact_id order by event_date) = interval '7 days'
then 1 
else 0
end as prev,
case when lead(week) over(partition by contact_id order by event_date)-week = interval '7 days' 
then 1 
else 0 
end as nxt
from cte
)
select b.email
from cte2 a
join crm_contacts b on a.contact_id=b.contact_id
where prev=1 and nxt=1
and a.contact_id in (
select distinct contact_id from marketing_touches where event_type='trial_request'
)
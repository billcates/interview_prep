with cte as(
    select 
        from_user,
        count(1) as total_emails
    from google_gmail_emails
    group by 1
)
select 
from_user as user_id, 
total_emails, 
row_number() over(order by total_emails desc, from_user) as activity_rank
from cte;
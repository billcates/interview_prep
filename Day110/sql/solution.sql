with cte as(
    select 
        a.*,
        b.entry_date as next_date
    from premium_accounts_by_day a
    left join premium_accounts_by_day b on a.account_id=b.account_id
    and a.final_price>0 and b.final_price>0
    and a.entry_date+ INTERVAL '7 days'=b.entry_date
    where a.final_price>0
)
select 
    entry_date, 
    count(account_id) as premium_paid_accounts,
    count(next_date) as premium_paid_accounts_after_7d
from cte
group by  1 order by 1 
limit 7
with cte as(
select account_id,case when count(b_user)>0 then count(distinct user_id) *(100.0)/(count(distinct b_user)) else 0 end as r
from (
select a.*,b.user_id as b_user
from sf_events a
left join sf_events b on a.account_id=b.account_id and a.user_id=b.user_id
and extract(year from b.record_date)>2020
where extract(month from a.record_date)=12 
and extract(year from a.record_date)=2020
)
group by 1
),
cte2 as(
select account_id,case when count(b_user)>0 then(count(distinct user_id) *(100.0))/(count(distinct b_user)) else 0 end as r
from(
select a.*,b.user_id as b_user
from sf_events a
left join sf_events b on a.account_id=b.account_id and a.user_id=b.user_id
and extract(year from b.record_date)>=2021
and extract(month from b.record_date)>1
where extract(month from a.record_date)=1
and extract(year from a.record_date)=2021
)
group by 1
)
select cte.account_id,cte2.r/cte.r as retention
from cte
join cte2 on cte.account_id=cte2.account_id
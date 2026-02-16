with cte as(
select to_char(created_at,'MM-YYYY') as month,user_id,sum(number_of_comments) as total_comments 
from fb_comments_count
where to_char(created_at,'MM-YYYY') in ('12-2019','01-2020')
group by 1,2
),cte2 as(
select *,dense_rank() over(order by dec desc) as prev_rank,
dense_rank() over(order by jan desc) as cur_rank
from (
select b.country,
sum(case when month='12-2019' then total_comments else 0 end) as dec,
sum(case when month='01-2020' then total_comments else 0 end) as jan
from cte
join fb_active_users b on cte.user_id=b.user_id
group by 1
)
)
select country
from cte2
where cur_rank<prev_rank
-- Write your PostgreSQL query statement below
with cte as(
    (
        select requester_id as id ,count(1) as num
        from requestaccepted
        group by 1
    )
    union all
    (
        select accepter_id as id,count(1) as num
        from requestaccepted
        group by 1
    )
)
select id,sum(num) as num
from cte
group by 1
order by 2 desc limit 1
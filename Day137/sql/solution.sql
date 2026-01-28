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

--optimized

SELECT id, COUNT(*) AS num
FROM (
    SELECT requester_id AS id FROM requestaccepted
    UNION ALL
    SELECT accepter_id  AS id FROM requestaccepted
) t
GROUP BY id
ORDER BY num DESC
LIMIT 1;

-- Write your PostgreSQL query statement below
with cte as(
select *, sum(weight) over(order by turn) s
from queue
)
select person_name
from cte
where s<=1000
order by s desc limit 1
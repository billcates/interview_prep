-- Write your PostgreSQL query statement below
with cte as(
select a.customer_id
from visits a
left join transactions b on a.visit_id=b.visit_id
where b.visit_id is NULL
)
select customer_id, count(1) as count_no_trans
from cte
group by 1
-- Write your PostgreSQL query statement below
with cte as(
    select managerId
    from employee
    group by 1 having count(1)>=5
)
select name
from employee a
join cte b on a.id=b.managerid
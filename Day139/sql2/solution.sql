-- Write your PostgreSQL query statement below

select a.id
from weather a
join weather b on a.recordDate - interval '1 day'=b.recorddate
where a.temperature>b.temperature
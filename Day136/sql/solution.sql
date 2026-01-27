-- Write your PostgreSQL query statement below
(select b.name as results
from movierating a
join users b on a.user_id=b.user_id
group by 1
order by count(1) desc, 1 
limit 1)

union all

(select title
from movierating a
join movies b on a.movie_id=b.movie_id
and extract(month from a.created_at)=2
and extract(year from a.created_at)=2020
group by 1
order by avg(rating) desc
limit 1)
with cte as(
select city,unnest(amenities::text[]) 
from airbnb_search_details
),
cte2 as(
select city,count(1) as ct,rank() over(order by count(*) desc) as rk
from cte
group by 1
)
select city
from cte2 where rk=1
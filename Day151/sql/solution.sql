with recursive cte as(
select 0 as left_page_number, null as title
union all 
select left_page_number + 1,b.title
from cte
left join cookbook_titles b on cte.left_page_number+1=b.page_number
where left_page_number <(select max(page_number) from cookbook_titles)
),
cte2 as(
select *, lead(title,1) over(order by left_page_number) as right_title
from cte
)
select left_page_number,title as left_title,right_title 
from cte2
where left_page_number%2=0 and (title is not NULL or right_title is not NULL)
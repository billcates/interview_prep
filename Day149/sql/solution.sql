with cte as(
select name, top_genre,count(*)
from oscar_nominees a
join nominee_information b on a.nominee=b.name and a.winner=True
group by 1,2
order by 3 desc,1 desc
)
select top_genre 
from cte
limit 1
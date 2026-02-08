with cte as(
    select * ,
        rank() over(partition by actor_name order by release_date desc) as rk,
        count(*)over(partition by actor_name) as ct
    from actor_rating_shift
),cte2 as(
    select 
        actor_name, 
        avg(case when rk!=1 and ct >1 then film_rating end) as avg_rating,
        sum(case when rk=1 and ct>1 then film_rating end) as latest_rating,
        round((sum(case when rk=1 and ct>1 then film_rating end)-
        avg(case when rk!=1 and ct >1 then film_rating end))::numeric,2) as rating_difference
    from cte
    where ct!=1
    group by 1

    union all
    
    select actor_name,film_rating, film_rating, 0
    from cte where ct=1
)
select * from cte2
order by 1
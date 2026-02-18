with genre_stats as (
    select actor_name,
           genre,
           count(*) as ct,
           max(movie_rating) as mx,
           avg(movie_rating) as ag
    from top_actors_rating
    group by actor_name, genre
),
best_genres as (
    select *,
           rank() over (
               partition by actor_name
               order by ct desc, mx desc
           ) as rk
    from genre_stats
)
select *
from (
    select actor_name,
           genre,
           ag as avg_rating,
           dense_rank() over(order by ag desc) as actor_rank
    from best_genres
    where rk = 1
) t
where actor_rank <= 3;

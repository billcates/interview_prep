-- Write your PostgreSQL query statement below
with cte as(
    select visited_on,sum(amount) as amount
    from customer 
    group by 1
    order by 1
),
cte2 as(
    select 
        visited_on,
        sum(amount) over(order by visited_on rows between 6 preceding and current row) as amount,
        round(avg(amount) over(order by visited_on rows between 6 preceding and current row),2) as average_amount,
        lag(amount,6) over(order by visited_on) as f
    from cte
)
select visited_on,amount,average_amount
from cte2
where f is not NULL
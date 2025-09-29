with cte as(
    select * , 
    sum(case when movement_type='in' then quantity else -quantity end) 
    over(partition by product_id order by movement_date) as running_sum
    from inventory_movements
),
cte2 as(
    select *,
    case when running_sum <=0 then 1 else 0 end as FLAG,
    lag(running_sum,1) over (PARTITIOn by product_id order by movement_date) as prev_quantity
    from cte
),
cte3 as(
select *, 
    case when prev_quantity >0 and running_sum<=0 then movement_date end as stock_out_start,
    case when prev_quantity<=0 and running_sum>0 then movement_date end as stock_out_end
from cte2
),
cte4 as(
    select *,
    lead(stock_out_end) over(partition by product_id order by movement_date) as stock_out_end_date
    from cte3
    where (stock_out_start is not null) or stock_out_end is not NULL
)
select 
    product_id,
    stock_out_start as stock_out_start_date,
    stock_out_end_date,
    (stock_out_end_date-stock_out_start) as days_out_of_stock
from cte4
where stock_out_start is not null
with cte as(
    select 
        employee_id,
        job_id,
        start_date,
        case when end_date is null then  '2099-12-31'::DATE else end_date end as end_date_new
    from employee_jobs
)
select * 
from cte A
join cte b
on a.employee_id=b.employee_id
and  a.job_id!=b.job_id
and a.start_date<=b.end_date_new and a.start_date>=b.start_date
select category, count(*) as accounts_count
from (
  select
    case
      when income < 20000 then 'Low Salary'
      when income between 20000 and 50000 then 'Average Salary'
      else 'High Salary'
    end as category
  from accounts
) t
group by category
order by accounts_count desc;

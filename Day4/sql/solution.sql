select 
    user_id, 
    page_url as current_page_url,
    lag(page_url,1,NULL) over(partition by user_id order by visit_time) as previous_page_url,
    visit_time
from page_visits
order by user_id,visit_time;
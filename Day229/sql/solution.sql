select name
from restaurants_623
where restaurant_id=(
select count(*) as ct 
from restaurants_623)/2
or restaurant_id=((
  select count(*) as ct 
from restaurants_623
)/2)+1
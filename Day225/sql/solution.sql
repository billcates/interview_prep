select group_concat(candidate_name, ', ') as names
from( select candidate_name 
  from candidates_582
  order by candidate_id)
select student_id 
from sat_scores
where sat_writing=(
SELECT percentile_cont(0.5) within group (order by sat_writing) as writing_percentile 
FROM sat_scores)
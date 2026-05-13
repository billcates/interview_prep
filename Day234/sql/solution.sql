select student_name,exam_date,subject,score,
(score*100.0)/max_score as pct,
round(avg(score) over(partition by student_name order by exam_date) ,1)as avg
from exam_results_628
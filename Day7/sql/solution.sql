select 
    emp_id,
    to_char(attendance_date,'YYYY-MM') as month,
    count(1) as total_working_days_in_month,
    sum(case when attendednce_date_status='Present' then 1 else 0 end) as total_present_days,
    sum(case when attendednce_date_status='Absent' then 1 else 0 end) as total_absent_days
from employee_attendance
group by to_char(attendance_date,'YYYY-MM'),emp_id
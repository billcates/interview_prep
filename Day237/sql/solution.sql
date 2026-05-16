WITH ranked AS (
    SELECT department, staff_name, salary,
        DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rnk
    FROM staff_salaries_531
)
SELECT department AS "Department", staff_name AS "StaffName", salary AS "Salary", rnk AS "Rank"
FROM ranked
WHERE rnk = 2
ORDER BY department
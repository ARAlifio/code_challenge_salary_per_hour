-- Insert calculated salary per hour data into the 'salary_per_hour' table
INSERT INTO salary_per_hour (year, month, branch_id, salary_per_hour)

SELECT 
    YEAR(t.date) AS year,
    MONTH(t.date) AS month,
    e.branch_id,
    SUM(e.salary) / SUM(TIMESTAMPDIFF(HOUR, t.checkin, t.checkout)) AS salary_per_hour  -- SUM / total working hours from check-in and check-out

FROM employees e
JOIN timesheets t ON e.employee_id = t.employee_id

-- Filter out employees who resigned before the timesheet date
WHERE (e.resign_date IS NULL OR e.resign_date >= t.date)

-- Group by year, month, and branch to get the data per branch and month
GROUP BY year, month, branch_id

-- If a duplicate entry for the combination of year, month, and branch_id exists, update the salary_per_hour value
ON DUPLICATE KEY UPDATE 
    salary_per_hour = salary_per_hour;

-- Create db
CREATE DATABASE salary_analysis;

-- Use db
USE salary_analysis;

CREATE TABLE IF NOT EXISTS employees (
    employee_id INT PRIMARY KEY,
    branch_id INT,
    salary INT,
    join_date DATE,
    resign_date DATE
);

CREATE TABLE IF NOT EXISTS timesheets (
    timesheet_id INT PRIMARY KEY,
    employee_id INT,
    date DATE,
    checkin TIME,
    checkout TIME
);

-- Monthly salary per hour, ON DUPLICATE ensure only latest run of each month is recorded
CREATE TABLE IF NOT EXISTS salary_per_hour (
    branch_id INT NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    salary_per_hour DECIMAL(10, 2),
    PRIMARY KEY (branch_id, year, month)
);

-- Only latest run per branch
CREATE OR REPLACE VIEW latest_salary_per_hour AS
SELECT 
    sp.branch_id,
    sp.year,
    sp.month,
    sp.salary_per_hour
FROM 
    salary_per_hour sp
    INNER JOIN (
        SELECT 
            branch_id, 
            MAX(CONCAT(year, LPAD(month, 2, '0'))) AS latest_period
        FROM 
            salary_per_hour
        GROUP BY 
            branch_id
    ) latest
    ON sp.branch_id = latest.branch_id 
    AND CONCAT(sp.year, LPAD(sp.month, 2, '0')) = latest.latest_period;
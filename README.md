# code_challenge_salary_per_hour

This repository contains a Python ETL script (`csv_etl.py`) and SQL schema files (`schema.sql`, `salary_per_hour.sql`) that calculate salary per hour for employees based on timesheet and employee data, and then insert the results into a MySQL database. The solution is optimized for incremental updates using the `ON DUPLICATE KEY UPDATE` method in MySQL.

## Prerequisites

Before running the Python script or applying the SQL schema, ensure you install the Python dependencies using `pip`:

```bash
pip install pandas sqlalchemy pymysql
```

## Files Included

1.  **`csv_etl.py`**: The Python script that performs the ETL process.
2.  **`schema.sql`**: SQL file that contains the schema for creating the `salary_per_hour` table.
3.  **`salary_per_hour.sql`**: SQL file for defining the calculation logic for inserting or updating salary per hour.

## Database Setup

1.  **Create the Database**: Use the provided `schema.sql` to create the required table in your MySQL database.
    
	```bash
	 mysql -u <username> -p < database_name < schema.sql
	```
	This will create a table called `salary_per_hour` with a composite primary key of `branch_id`, `year`, and `month`.
    
2.  **Inserting/Updating Salary Data**: The `salary_per_hour.sql` file includes an example query using the `ON DUPLICATE KEY UPDATE` statement to insert or update the salary per hour in the table.
    ```bash
    mysql -u <username> -p < database_name < salary_per_hour.sql
    ``` 
    

##  Python ETL Script

### 1. **CSV Files**:

You should have two CSV files:

-   **`employees.csv`**: Contains employee details such as `employee_id`, `branch_id`, and `salary`.
-   **`timesheets.csv`**: Contains timesheet entries with check-in and check-out times.

Place these files in the appropriate directory where the `csv_etl.py` script can read them.

### 2. **Database Configuration**:

In the `csv_etl.py` script, configure your MySQL database credentials:

`DATABASE_URI = 'mysql+pymysql://username:password@localhost/salary_analysis'` 

Replace `username`, `password`, and `payroll_analysis` with your actual database credentials.

### 3. **Run the Script**:

Execute the Python script to load the data, perform the salary per hour calculation, and upsert the results into the database:


`python csv_etl.py` 

## How It Works

1.  **Data Loading**:
    
    -   The `csv_etl.py` script loads employee and timesheet data from CSV files using `pandas`.
2.  **Data Transformation**:
    
    -   It calculates the total working hours for each employee based on check-in and check-out times.
    -   It groups the data by `branch_id`, `year`, and `month` to calculate the salary per hour.
3.  **Upserting into MySQL**:
    
    -   The script uses the `ON DUPLICATE KEY UPDATE` method to insert or update the salary per hour in the MySQL database for each `branch_id`, `year`, and `month`.

## Error Handling and Debugging

-   **SQL Errors**: Any SQL-related errors (such as connection issues or invalid queries) will be logged to the console.
-   **Logging**: Errors during the upsert process will be logged, and you can increase the logging level for more detailed output.
-   **Data Issues**: Invalid or missing data (e.g., missing check-in/check-out times) will be handled gracefully, and those rows will be skipped.
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

DATABASE_URI = 'mysql+pymysql://aralifio:Vwaffe123@localhost/salary_analysis'
engine = create_engine(DATABASE_URI)

try:
    employees_df = pd.read_csv("C:/Users/poisa/Downloads/employees.csv")
    timesheets_df = pd.read_csv("C:/Users/poisa/Downloads/timesheets.csv")
    logging.info("CSV files loaded successfully.")
except Exception as e:
    logging.error("Error loading CSV files: %s", e)
    raise

timesheets_df['date'] = pd.to_datetime(timesheets_df['date'])
employees_df['resign_date'] = pd.to_datetime(employees_df['resign_date'], errors='coerce')

# Convert check-in and check-out columns to datetime.time
timesheets_df['checkin'] = pd.to_datetime(timesheets_df['checkin'], format='%H:%M:%S', errors='coerce').dt.time
timesheets_df['checkout'] = pd.to_datetime(timesheets_df['checkout'], format='%H:%M:%S', errors='coerce').dt.time

# Remove rows with missing check-in or check-out values
# Other option can be imputed with default values (e.g. 08:00:00 checkin and 17:00:00 checkout) or appending checkout with current execution time to calculate actual worked time
timesheets_df = timesheets_df.dropna(subset=['checkin', 'checkout'])

# Merge employees and timesheets data
merged_df = pd.merge(timesheets_df, employees_df, on='employee_id')

# Filter out employees who resigned before the timesheet date
merged_df = merged_df[(merged_df['resign_date'].isna()) | (merged_df['resign_date'] >= merged_df['date'])]

# Concatenate date with check-in and check-out times
merged_df['checkin_datetime'] = pd.to_datetime(
    merged_df['date'].astype(str) + ' ' + merged_df['checkin'].astype(str), 
    errors='coerce'
)

merged_df['checkout_datetime'] = pd.to_datetime(
    merged_df['date'].astype(str) + ' ' + merged_df['checkout'].astype(str), 
    errors='coerce'
)

# Remove rows with invalid datetime values
# Didn't find any NULL date and NULL checkin/checkout, although just in case
merged_df = merged_df.dropna(subset=['checkin_datetime', 'checkout_datetime'])

# Calculate working hours for each record
merged_df['working_hours'] = (merged_df['checkout_datetime'] - merged_df['checkin_datetime']).dt.total_seconds() / 3600

# Extract year and month for grouping
merged_df['year'] = merged_df['date'].dt.year
merged_df['month'] = merged_df['date'].dt.month

# Calculate total salary per hour for each key (branch_id, year, month)
salary_per_hour_df = merged_df.groupby(['branch_id', 'year', 'month'], as_index=False).apply(
    lambda x: pd.Series({
        'salary_per_hour': x['salary'].sum() / x['working_hours'].sum()
    })
).reset_index(drop=True)

# Upsert
# Lots of workaround for local mysql via sqlalchemy, in cloud data warehouse environment should be able to be more elegant 
def upsert_salary_per_hour(df, connection):
    with connection.connect() as conn:
        for index, row in df.iterrows():
            insert_query = text("""
            INSERT INTO salary_per_hour (branch_id, year, month, salary_per_hour)
            VALUES (:branch_id, :year, :month, :salary_per_hour)
            ON DUPLICATE KEY UPDATE 
                salary_per_hour = :salary_per_hour;
            """)
            params = {
                'branch_id': int(row['branch_id']),
                'year': int(row['year']),
                'month': int(row['month']),
                'salary_per_hour': float(row['salary_per_hour'])
            }
            
            logging.debug(f"Upserting with params: {params}")
            
            try:
                conn.execute(insert_query, params)
                logging.info(f"Upsert successful for branch_id: {params['branch_id']}, year: {params['year']}, month: {params['month']}")
            except Exception as e:
                logging.error("Error while upserting data for branch_id: %(branch_id)s, year: %(year)s, month: %(month)s. Error: %s. Params: %s", 
                              params['branch_id'], params['year'], params['month'], e, params)

try:
    upsert_salary_per_hour(salary_per_hour_df, engine)
    logging.info("Data successfully upserted into salary_per_hour table.")
except Exception as e:
    logging.error("Error while upserting data into salary_per_hour table: %s", e)
    raise
import pandas as pd
from google.cloud import bigquery

# BigQuery configuration
PROJECT_ID = "intrepid-hour-272417"
DATASET_ID = "kenya_employee_analytics"
TABLE_ID = "gold_employee_detail"

# Create BigQuery client
client = bigquery.Client(project=PROJECT_ID)

# Query the Gold table
query = f"""
SELECT *
FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
"""

# Pull BigQuery data into Pandas
df = client.query(query).to_dataframe()

# Confirm the data
print("Rows:", len(df))
print("Columns:", df.columns.tolist())

print("\nFirst 5 rows:")
print(df.head())


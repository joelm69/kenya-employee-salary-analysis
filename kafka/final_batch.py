import json
import tempfile
import os

from kafka import KafkaConsumer
from google.cloud import bigquery


PROJECT_ID = "intrepid-hour-272417"
TABLE_ID = "intrepid-hour-272417.kenya_employee_analytics.raw_employee_events"

consumer = KafkaConsumer(
    "employee-events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="bigquery-employee-batch-ingestion",
    enable_auto_commit=False
)

client = bigquery.Client(project=PROJECT_ID)

batch = []

print("Reading remaining Kafka events...")


for message in consumer:

    employee = json.loads(message.value.decode("utf-8"))

    row = {
        "employee_id": employee["employee_id"],
        "full_name": employee["full_name"],
        "gender": employee["gender"],
        "department": employee["department"],
        "job_title": employee["job_title"],
        "county": employee["county"],
        "hire_date": employee["hire_date"],
        "salary_kes": int(employee["salary_kes"]),
        "age": int(employee["age"]),
        "performance_rating": int(employee["performance_rating"]),
        "ingested_at": employee.get(
            "ingested_at",
            None
        )
    }

    batch.append(row)

    print(f"Received {employee['employee_id']} ({len(batch)}/20)")

    if len(batch) == 20:
        break


with tempfile.NamedTemporaryFile(
    mode="w",
    suffix=".jsonl",
    delete=False
) as file:

    for row in batch:
        file.write(json.dumps(row) + "\n")

    file_path = file.name


job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
)


with open(file_path, "rb") as file:

    job = client.load_table_from_file(
        file,
        TABLE_ID,
        job_config=job_config
    )

job.result()

consumer.commit()

os.remove(file_path)

print(f"Successfully loaded {len(batch)} remaining events.")
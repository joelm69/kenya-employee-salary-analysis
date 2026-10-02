import argparse
import json
import os
import tempfile
from datetime import datetime, timezone

from kafka import KafkaConsumer
from google.cloud import bigquery


PROJECT_ID = "intrepid-hour-272417"
DATASET_ID = "kenya_employee_analytics"
TABLE_ID = "raw_employee_events"


# Allow the batch size to be changed from the command line
parser = argparse.ArgumentParser()

parser.add_argument(
    "--batch-size",
    type=int,
    default=50,
    help="Number of Kafka events to load into BigQuery per batch"
)

args = parser.parse_args()

BATCH_SIZE = args.batch_size


consumer = KafkaConsumer(
    "employee-events-test",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="bigquery-employee-batch-ingestion",
    enable_auto_commit=False
)

client = bigquery.Client(project=PROJECT_ID)

table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


print("Connected to Kafka and BigQuery.")
print(f"Collecting {BATCH_SIZE} events per batch...")


batch = []


def load_batch(rows):
    """
    Load a batch of employee events into BigQuery
    using a temporary JSONL file and a BigQuery load job.
    """

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".jsonl",
        delete=False
    ) as file:

        for row in rows:
            file.write(json.dumps(row) + "\n")

        file_path = file.name

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    try:

        with open(file_path, "rb") as file:

            job = client.load_table_from_file(
                file,
                table_ref,
                job_config=job_config
            )

        job.result()

        print(
            f"Loaded {len(rows)} events into BigQuery."
        )

        return True

    except Exception as error:

        print(
            f"BigQuery load failed: {error}"
        )

        return False

    finally:

        os.remove(file_path)


def flush_batch():
    """
    Load and commit any remaining events.
    """

    global batch

    if not batch:
        return True

    success = load_batch(batch)

    if success:

        consumer.commit()

        print(
            f"Final batch of {len(batch)} events committed."
        )

        batch = []

        return True

    else:

        print(
            "Final batch failed. "
            "Kafka events were not committed."
        )

        return False


try:

    for message in consumer:

        employee = json.loads(
            message.value.decode("utf-8")
        )

        # Check whether this is the end-of-stream event
        if employee.get("event_type") == "END_OF_STREAM":

            print("END_OF_STREAM received.")

            flush_batch()

            break

        row = {
            "event_id": employee["event_id"],
            "employee_id": employee["employee_id"],
            "full_name": employee["full_name"],
            "gender": employee["gender"],
            "department": employee["department"],
            "job_title": employee["job_title"],
            "county": employee["county"],
            "hire_date": employee["hire_date"],
            "salary_kes": int(employee["salary_kes"]),
            "age": int(employee["age"]),
            "performance_rating": int(
                employee["performance_rating"]
            ),
            "ingested_at": datetime.now(
                timezone.utc
            ).isoformat()
        }

        batch.append(row)

        print(
            f"Received employee "
            f"{employee['employee_id']} "
            f"({len(batch)}/{BATCH_SIZE})"
        )

        if len(batch) >= BATCH_SIZE:

            success = load_batch(batch)

            if success:

                consumer.commit()

                batch = []

            else:

                print(
                    "Batch not committed. "
                    "Kafka events will be retried."
                )

                break


except KeyboardInterrupt:

    print("\nConsumer stopped by user.")


finally:

    consumer.close()

    print("Kafka consumer closed.")
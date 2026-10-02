import csv
import json
import time
import uuid

from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="localhost:9092"
)


with open("../employees_cleaned.csv", "r") as file:

    reader = csv.DictReader(file)

    for employee in reader:

        # Generate a unique ID for this event
        employee["event_id"] = str(uuid.uuid4())

        event = json.dumps(employee)

        producer.send(
            "employee-events-test",
            event.encode("utf-8")
        )

        producer.flush()

        print(
            f"Sent employee {employee['employee_id']} "
            f"| event_id: {employee['event_id']}"
        )

        time.sleep(1)


# Tell the consumer that the file has finished
end_event = json.dumps({
    "event_type": "END_OF_STREAM"
})

producer.send(
    "employee-events-test",
    end_event.encode("utf-8")
)

producer.flush()

print("Sent END_OF_STREAM event.")


producer.close()

print("Finished streaming employees.")
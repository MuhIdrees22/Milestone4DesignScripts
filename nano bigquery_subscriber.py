import json
from google.cloud import pubsub_v1
from google.cloud import bigquery

project_id = "pure-wall-451118-g4"
topic_name = "smart-meter"
subscription_name = "bq-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

bq_client = bigquery.Client()
table_id = f"{project_id}.dataset.smart_meter_table"

def callback(message):
    data = json.loads(message.data.decode("utf-8"))

    errors = bq_client.insert_rows_json(table_id, [data])
    if not errors:
        print("Inserted into BigQuery:", data)
    else:
        print("BigQuery error:", errors)

    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print("BigQuery subscriber running...")
streaming_pull_future.result()

import json
from google.cloud import pubsub_v1

project_id = "pure-wall-451118-g4"
topic_name = "smart-meter"
subscription_name = "filter-sub"

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

subscription_path = subscriber.subscription_path(project_id, subscription_name)
topic_path = publisher.topic_path(project_id, topic_name)

def callback(message):
    data = json.loads(message.data.decode("utf-8"))

    if None not in data.values():
        publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
        print("Passed filter:", data)
    else:
        print("Filtered out:", data)

    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print("FilterReading running...")
streaming_pull_future.result()

import json
from google.cloud import pubsub_v1

project_id = "pure-wall-451118-g4"
topic_name = "smart-meter"
subscription_name = "convert-sub"

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

subscription_path = subscriber.subscription_path(project_id, subscription_name)
topic_path = publisher.topic_path(project_id, topic_name)

def callback(message):
    data = json.loads(message.data.decode("utf-8"))

    data["pressure"] = data["pressure"] / 6.895
    data["temperature"] = data["temperature"] * 1.8 + 32

    publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
    print("Converted:", data)

    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print("ConvertReading running...")
streaming_pull_future.result()

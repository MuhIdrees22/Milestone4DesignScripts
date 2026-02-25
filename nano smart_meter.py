import json
import random
import time
from google.cloud import pubsub_v1

project_id = "pure-wall-451118-g4"
topic_name = "smart-meter"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

while True:
    reading = {
        "temperature": random.choice([20, 25, None, 30]),
        "pressure": random.choice([100, 110, None, 120])
    }

    publisher.publish(topic_path, json.dumps(reading).encode("utf-8"))
    print("Sent:", reading)

    time.sleep(2)

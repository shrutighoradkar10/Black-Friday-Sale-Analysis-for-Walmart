import json
import time
import random
import os
from google.cloud import pubsub_v1
from datetime import datetime, timedelta

credentials_path=r"C:\Users\Shruti Ghoradkar\Downloads\single-brace-410112-85c8ed6d4a24.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=credentials_path

# Initialize the Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()


# Project and Topic details
project_id = "single-brace-410112"
topic_name = "Inventory-Updates-Stream"
topic_path = publisher.topic_path(project_id, topic_name)



# Callback function to handle the publishing results.
def callback(future):
    try:
        # Get the message_id after publishing.
        message_id = future.result()
        print(f"Published message with ID: {message_id}")
    except Exception as e:
        print(f"Error publishing message: {e}")


def generate_mock_data():
    product_id = ["P500", "P501", "P502", "P503", "P504","P505","P506","P507","P508","P509","P5010","P5011","P5012","P809"]
    store_id = ["W100","W101","W102","W104","W105","W106","W107","W108","W109"]
    start_date = datetime(2023, 10, 1)
    end_date = datetime(2024, 2, 3)
    random_timestamp = start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))
    return {
        "product_id": random.choice(product_id),
        "timestamp": random_timestamp.strftime("%Y-%m-%d %H:%M:%S")              ,
        "quantity_change": random.randint(-4,4),
        "store_id":random.choice(store_id)
    }


while True:
    # random_timestamp = start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))
    data = generate_mock_data()
    json_data = json.dumps(data).encode('utf-8')

    try:
        future = publisher.publish(topic_path, data=json_data)
        future.add_done_callback(callback)
        future.result()
    except Exception as e:
        print(f"Exception encountered: {e}")

    time.sleep(1)  # Wait for 2 seconds


  
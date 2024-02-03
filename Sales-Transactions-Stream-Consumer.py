import base64
from google.cloud import bigquery
import json
import functions_framework

# Initialize BigQuery client
client = bigquery.Client()

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    # Print out the data from Pub/Sub, to prove that it worked
    pubsub_data = base64.b64decode(cloud_event.data["message"]["data"])
    data_dict = json.loads(pubsub_data)
    print(data_dict)

    transaction_id = data_dict.get("transaction_id")
    product_id = data_dict.get("product_id")
    timestamp = data_dict.get("timestamp")
    quantity = int(data_dict.get("quantity"))  # Convert to int
    unit_price = data_dict.get("unit_price")
    store_id = data_dict.get("store_id")

    # Create a BigQuery SQL parameter list
    query_params = [
        bigquery.ScalarQueryParameter("transaction_id", "STRING", transaction_id),
        bigquery.ScalarQueryParameter("product_id", "STRING", product_id),
        bigquery.ScalarQueryParameter("timestamp", "STRING", timestamp),
        bigquery.ScalarQueryParameter("initial_quantity", "INT64", quantity),
        bigquery.ScalarQueryParameter("updated_quantity", "INT64", None),
        bigquery.ScalarQueryParameter("store_id", "STRING", store_id),
    ]

    # Insert data into BigQuery table with ? placeholders
    QUERY = (
        "INSERT INTO `single-brace-410112.Sales_Data.Fact_Sales_table` "
        "(transaction_id, product_id, timestamp, initial_quantity, updated_quantity, store_id) "
        "VALUES (@transaction_id, @product_id, @timestamp, @initial_quantity, @updated_quantity, @store_id)"
    )

    # Run the query
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    query_job = client.query(QUERY, job_config=job_config)
    rows = query_job.result()  # Waits for the query to finish
    print(f"Inserted  rows into BigQuery table.")

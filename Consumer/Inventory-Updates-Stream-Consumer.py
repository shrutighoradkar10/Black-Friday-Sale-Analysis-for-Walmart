import base64
import json
import functions_framework
from google.cloud import bigquery
from google.cloud import pubsub_v1
dlq_topic_path='projects/single-brace-410112/topics/DLQ_for_Sales_Data'

client = bigquery.Client()  # Initialize BigQuery client

publisher = pubsub_v1.PublisherClient()  # Initialize the Pub/Sub publisher client

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    pubsub_data = base64.b64decode(cloud_event.data["message"]["data"])
    data_dict = json.loads(pubsub_data)
    print(data_dict)     # Print out the data from Pub/Sub, to prove that it worked

    product_id = data_dict.get("product_id")
    timestamp = data_dict.get("timestamp")
    quantity_change = int(data_dict.get("quantity_change"))
    store_id = data_dict.get("store_id")

    # Create a BigQuery SQL parameter list
    query_params = [
        bigquery.ScalarQueryParameter("product_id", "STRING", product_id),
        bigquery.ScalarQueryParameter("timestamp", "STRING", timestamp),
        bigquery.ScalarQueryParameter("quantity_change", "INT64", quantity_change),
        bigquery.ScalarQueryParameter("store_id", "STRING", store_id)
    ]
    # Check if product_id exists in the BigQuery table
    query = "SELECT product_id FROM `single-brace-410112.Sales_Data.Fact_Sales_table` WHERE product_id =  @product_id"
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    query_job = client.query(query, job_config=job_config)
    result = query_job.result()
    if len(list(result)) > 0: # if product_id found
        update_query = """
                UPDATE `single-brace-410112.Sales_Data.Fact_Sales_table` SET  
                updated_quantity = CASE WHEN (initial_quantity + @quantity_change) < 0 THEN 0    
                          ELSE initial_quantity + @quantity_change
                     END
        WHERE product_id = @product_id AND store_id = @store_id;

                    
                """
        job_config = bigquery.QueryJobConfig(query_parameters=query_params)
        query_jobb = client.query(update_query, job_config=job_config)
        rows = query_jobb.result()  # Waits for the query to finish
        print(f"Updated data in BigQuery ->.",data_dict)
    else:  # if Product_id not found
        # pubsub_data_bytes = json.dumps(data_dict).encode('utf-8')
        publisher.publish(dlq_topic_path, data=pubsub_data)
        print("Data thrown in dlq because Product_id not found -> ", data_dict)




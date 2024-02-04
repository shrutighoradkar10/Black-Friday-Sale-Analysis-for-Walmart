# Black-Friday-Sales-Analysis-for-Walmart
Real-Time Sales and  Inventory Management

## Technologies used in the solution:

| S.No | TECH                                   |
|------|----------------------------------------|
| 1    | Python                                 |
| 2    | GCP Pub/Sub                            |
| 3    | Event Driven GCP Cloud Functions       |
| 4    | BigQuery                               |
| 5    | Looker Studio                          |

## ARCHITECTURE

![Pipline_Architecture_Diagram](https://github.com/shrutighoradkar10/Black-Friday-Sale-Analysis-for-Walmart/assets/75423631/724b4ebb-0ebe-434d-87ed-0c890a5dd559)

Explanation:

1. 2 Python mock generator will produce mock data in 3 different Pub/Sub topics.
2. Cloud Functions will act like consumers as soon as the messages arrived in Pub/Sub topics function will get triggered.
3. Consumer 1 is responsible for inserting data in BigQuery fact table with updated_quantity initially as None.
4. Consumer 2 is responsible  for updating Quantity  where the product_id and store_id is same
5.  



# fabric_cost_monitor.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as spark_sum
import requests
import pandas as pd

# Azure credentials (assumed to be loaded securely)
TENANT_ID = "your-tenant-id"
CLIENT_ID = "your-client-id"
CLIENT_SECRET = "your-client-secret"
SUBSCRIPTION_ID = "your-subscription-id"

# Authenticate and get bearer token for Azure REST API
def get_azure_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "resource": "https://management.azure.com/"
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(url, data=payload, headers=headers)
    response.raise_for_status()
    return response.json()["access_token"]

# Fetch cost usage data from Azure Cost Management API
def fetch_usage_data():
    token = get_azure_token()
    url = f"https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}/providers/Microsoft.CostManagement/query?api-version=2023-03-01"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    query_body = {
        "type": "ActualCost",
        "timeframe": "MonthToDate",
        "dataset": {
            "granularity": "Daily",
            "aggregation": {
                "totalCost": {
                    "name": "PreTaxCost",
                    "function": "Sum"
                }
            },
            "grouping": [
                {"type": "Dimension", "name": "ResourceGroup"},
                {"type": "Dimension", "name": "ServiceName"}
            ]
        }
    }
    response = requests.post(url, headers=headers, json=query_body)
    response.raise_for_status()
    # The API returns rows under properties.rows
    return response.json()["properties"]["rows"]

# Start Spark session
spark = SparkSession.builder.appName("FabricCostMonitoring").getOrCreate()

raw_data = fetch_usage_data()

# If data is returned, process and display
if raw_data:
    # The Azure Cost Management API returns rows as lists; define column names accordingly
    columns = ["ResourceGroup", "ServiceName", "Date", "Cost"]
    pdf = pd.DataFrame(raw_data, columns=columns)
    df = spark.createDataFrame(pdf)

    df = df.withColumn("date", to_date(col("Date"))) \
           .withColumnRenamed("ResourceGroup", "workspace") \
           .withColumnRenamed("ServiceName", "service") \
           .withColumnRenamed("Cost", "cost") \
           .select("workspace", "service", "date", "cost")

    df.createOrReplaceTempView("fabric_costs")

    cost_summary = df.groupBy("workspace", "service").agg(spark_sum("cost").alias("total_cost"))
    cost_summary.show()
else:
    print("No data to process.")

# Stop Spark session
spark.stop()
# Fabric Cost Monitor

This project provides a PySpark-based script (`fabric_cost_monitor.py`) to monitor and summarize Azure Fabric workspace costs using the Azure Cost Management API. It fetches daily cost data, processes it with Spark, and outputs a summary of costs by workspace and service.

## Features
- Authenticates with Azure using service principal credentials
- Fetches cost usage data from Azure Cost Management API
- Processes and aggregates cost data using PySpark
- Outputs a summary of total costs by workspace and service

## Prerequisites
- Python 3.7+
- [PySpark](https://spark.apache.org/docs/latest/api/python/)
- [pandas](https://pandas.pydata.org/)
- [requests](https://docs.python-requests.org/)
- Azure subscription with Cost Management API access

## Setup
1. **Clone the repository:**
   ```bash
   git clone <your-repo-url>
   cd <your-repo-directory>
   ```
2. **Install dependencies:**
   ```bash
   pip install pyspark pandas requests
   ```
3. **Configure Azure credentials:**
   - Edit `fabric_cost_monitor.py` and set the following variables with your Azure service principal and subscription details:
     - `TENANT_ID`
     - `CLIENT_ID`
     - `CLIENT_SECRET`
     - `SUBSCRIPTION_ID`

## Usage
Run the script using Python:
```bash
python fabric_cost_monitor.py
```

The script will:
- Authenticate with Azure
- Fetch cost usage data for the current month (granularity: daily)
- Process and aggregate the data using PySpark
- Display a summary of total costs by workspace and service

## Output Example
```
+----------+--------------------+----------+
| workspace|             service|total_cost|
+----------+--------------------+----------+
|   my-rg  | Azure SQL Database |   123.45 |
|   my-rg  | Azure Storage      |    67.89 |
+----------+--------------------+----------+
```

## Notes
- Ensure your service principal has permission to access the Cost Management API for the specified subscription.
- The script processes only the current month's data (MonthToDate).
- For production use, consider securing credentials and handling errors more robustly.

## License
MIT License 
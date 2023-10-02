from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago

job_config = {
    "source": "<data source connection related fields>",
    "granularity": "daily",
    "kpis": [
        {
            "name": "Users count",
            "sql": "approx_count_distinct(fullVisitorId, 0.03)"
        },
        {
            "name": "Bounce count",
            "sql": "sum(totals.bounces)"
        },
        {
            "name": "Conversion rate",
            "sql": "(sum(totals.transactions)/count(*))*100"
        }
    ],
    "dimensions": [
        "geoNetwork.country",
        "device.browser",
        "device.deviceCategory",
        "device.operatingSystem"
    ],
    "depth": 2
}

with DAG('databricks_dag',
  start_date = days_ago(2),
  schedule_interval = None,
  default_args = job_config
  ) as dag:

  opr_run_now = DatabricksRunNowOperator(
    task_id = 'run_now',
    databricks_conn_id = 'databricks_default',
    job_id = '466727288980718'
  )
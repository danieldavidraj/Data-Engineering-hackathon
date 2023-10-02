from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

notebook_params = {
    "source": '{ \
        "start_date": "25/07/2017", \
        "end_date": "30/07/2017" \
    }',
    "granularity": "daily",
    "kpis": '[ \
        { \
            "name": "Users count", \
            "sql": "approx_count_distinct(fullVisitorId, 0.03)" \
        }, \
        { \
            "name": "Bounce count", \
            "sql": "sum(totals.bounces)" \
        }, \
        { \
            "name": "Conversion rate", \
            "sql": "(sum(totals.transactions)/count(*))*100" \
        } \
    ]',
    "dimensions": '[ \
        "geoNetwork.country", \
        "device.browser", \
        "device.deviceCategory", \
        "device.operatingSystem" \
    ]',
    "depth": "2"
}

with DAG(
    'databricks_dag',
    start_date=datetime(2021, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "email_on_failure": False,
        "email_on_retry": False,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:

    opr_run_now = DatabricksRunNowOperator(
        task_id = 'run_now',
        databricks_conn_id = 'databricks_default',
        job_id = '510006363154351',
        notebook_params = notebook_params,
    )
FROM apache/airflow:2.7.1
RUN pip install apache-airflow==${AIRFLOW_VERSION}
RUN pip install apache-airflow-providers-databricks
RUN pip install "apache-airflow[databricks, celery, password]"
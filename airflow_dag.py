from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 

notebook_task = {
    'notebook_path': '/Repos/matthew_marquis@btinternet.com/pinterest-data-pipeline/db_data_querying'
}

notebook_params = {
    "Variable":5
}

default_args = {
    'owner': 'Matthew',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG('databricks_dag',
    start_date=datetime(2023, 4, 23),
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_args
    ) as dag:


    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    opr_submit_run
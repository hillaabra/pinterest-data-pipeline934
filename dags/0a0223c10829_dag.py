from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta


#Define params for Submit Run Operator
notebook_task = {
    'notebook_path': '/Users/hilla.abramov@gmail.com/batch_processing_pipeline',
}


#Define params for Run Now Operator
notebook_params = {
    'Variable': 5
}


default_args = {
    'owner': 'Hilla',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


with DAG('0a0223c10829_dag',
    # should be a datetime format
    start_date=datetime(2023, 12, 17),
    # check out possible intervals, should be a string
    schedule_interval='0 0 * * *', # cron expression for daily - to be run once a day at midnight
    catchup=False,
    default_args=default_args
    ) as dag:

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        # the connection we set-up previously
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    opr_submit_run
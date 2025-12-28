# orchestration/dataflow_dag.py
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.utils.dates import days_ago

with DAG(
    'daily_etl_orchestration',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    launch_template = DataflowTemplatedJobStartOperator(
        task_id='execute_etl_template',
        template='gs://gcp_poc1/production/etl_poc1.json',
        location='us-central1',
        parameters={
            'input_file': 'gs://gcp_poc1/input/sample_transactions.csv',
            'output_table': 'gcp_poc1:gcp_poc1.transformed_transactions'
        }
    )

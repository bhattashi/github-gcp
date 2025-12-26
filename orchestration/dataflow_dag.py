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
        template_gcs_path='gs://your-bucket/templates/etl_v1.json',
        location='us-central1',
        parameters={
            'input_file': 'gs://your-bucket/input/data_{{ ds }}.csv',
            'output_table': 'your-project:dataset.table',
            'error_output_file': 'gs://your-bucket/errors/{{ ds }}/err.txt'
        }
    )

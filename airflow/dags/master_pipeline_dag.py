from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'gavi_architect',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'client_analytics_master_pipeline',
    default_args=default_args,
    description='End-to-End ingestion, validation, and dbt transformation',
    schedule_interval='0 6 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['production', 'tier-1', 'client-analytics'],
) as dag:

    start_ingestion = EmptyOperator(task_id='start_ingestion')
    
    apis = ['stripe', 'hubspot', 'mailchimp', 'zendesk', 'ga4', 'shopify', 'intercom', 'custom_crm']
    ingest_tasks = []
    
    for api in apis:
        task = EmptyOperator(task_id=f'extract_{api}_api')
        ingest_tasks.append(task)

    validate_contracts = EmptyOperator(task_id='enforce_data_contracts_yaml')
    load_to_warehouse = EmptyOperator(task_id='load_s3_to_bigquery_raw')
    dbt_staging = EmptyOperator(task_id='dbt_run_staging_models')
    dbt_marts = EmptyOperator(task_id='dbt_run_business_marts')
    dbt_test = EmptyOperator(task_id='dbt_test_schema_assertions')
    slack_success_alert = EmptyOperator(task_id='slack_executive_summary')

    start_ingestion >> ingest_tasks >> validate_contracts >> load_to_warehouse
    load_to_warehouse >> dbt_staging >> dbt_marts >> dbt_test >> slack_success_alert

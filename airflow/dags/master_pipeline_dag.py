"""
Master pipeline DAG.

Runs daily at 06:00 UTC. Orchestrates the full ELT pipeline:
    extract → validate → load → transform (dbt) → notify

Retry logic: 2 retries with 5-minute delay.
Failure callback: Slack alert fires on any task failure.
SLA: pipeline must complete within 90 minutes of scheduled start.
"""

import logging
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from alerts.slack_notifier import alert_on_failure, alert_on_success

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# DAG default args
# ------------------------------------------------------------------

default_args = {
    "owner": "garvpreet",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
    "email_on_failure": False,
    "email_on_retry": False,
}

# ------------------------------------------------------------------
# Task callables
# ------------------------------------------------------------------

def extract_all_apis(**context) -> None:
    """
    Pull data from all 8 SaaS connectors concurrently.
    Pushes source_records to XCom for the load task.
    """
    from ingestion.connectors.stripe import StripeConnector
    from ingestion.connectors.hubspot import HubSpotConnector
    from ingestion.utils.async_extractor import extract_all_sources_sync

    execution_date = context["ds"]  # "YYYY-MM-DD" from Airflow

    connectors = [
        StripeConnector(),
        HubSpotConnector(),
        # ShopifyConnector(), GA4Connector(), etc. — add as you build them
    ]

    source_records = extract_all_sources_sync(connectors, date=execution_date)

    total = sum(len(v) for v in source_records.items())
    logger.info("Extracted %d total records from %d sources", total, len(source_records))

    # Push to XCom so load task can pick it up
    context["ti"].xcom_push(key="source_records", value=source_records)
    context["ti"].xcom_push(key="extraction_date", value=execution_date)


def validate_contracts(**context) -> None:
    """
    Run lightweight schema validation before loading to BQ.
    Prevents obviously malformed data from reaching the warehouse.
    Full contract enforcement happens in dbt after load.
    """
    from ingestion.utils.schema_validator import validate_all

    source_records = context["ti"].xcom_pull(key="source_records", task_ids="extract_all_apis")

    validation_errors = validate_all(source_records)
    if validation_errors:
        error_summary = "\n".join(
            f"  [{src}] {err}" for src, err in validation_errors.items()
        )
        raise ValueError(f"Pre-load validation failed:\n{error_summary}")

    logger.info("All pre-load validations passed.")


def load_to_bigquery(**context) -> None:
    """Load validated records into BigQuery raw dataset."""
    from ingestion.loaders.bigquery_loader import BigQueryLoader

    source_records = context["ti"].xcom_pull(key="source_records", task_ids="extract_all_apis")
    extraction_date = context["ti"].xcom_pull(key="extraction_date", task_ids="extract_all_apis")

    loader = BigQueryLoader()
    rows_loaded = loader.load_all(source_records, partition_date=extraction_date)

    for source, count in rows_loaded.items():
        logger.info("[%s] Loaded %d rows", source, count)

    total = sum(rows_loaded.values())
    logger.info("Load complete — %d total rows across %d sources", total, len(rows_loaded))
    context["ti"].xcom_push(key="rows_loaded", value=rows_loaded)


def run_dbt(**context) -> None:
    """
    Run dbt build — executes all models and tests.
    Runs in the dbt/ subdirectory of the project.
    """
    import os

    dbt_project_dir = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "dbt")

    commands = [
        ["dbt", "deps"],
        ["dbt", "build", "--target", "prod"],  # runs models + tests in one command
    ]

    for cmd in commands:
        logger.info("Running: %s", " ".join(cmd))
        result = subprocess.run(
            cmd,
            cwd=dbt_project_dir,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            logger.error("dbt stderr:\n%s", result.stderr)
            raise RuntimeError(f"dbt command failed: {' '.join(cmd)}\n{result.stderr}")
        logger.info("dbt stdout:\n%s", result.stdout)

    logger.info("dbt build complete — all models and tests passed.")


def notify_success(**context) -> None:
    """Send Slack success notification with pipeline summary."""
    rows_loaded = context["ti"].xcom_pull(key="rows_loaded", task_ids="load_to_bigquery")
    execution_date = context["ds"]
    alert_on_success(context, rows_loaded=rows_loaded, execution_date=execution_date)


# ------------------------------------------------------------------
# DAG definition
# ------------------------------------------------------------------

with DAG(
    dag_id="master_client_pipeline",
    default_args=default_args,
    description="Daily ELT: 8 SaaS APIs → BigQuery → dbt → Streamlit dashboard",
    schedule_interval="0 6 * * *",   # 06:00 UTC daily
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["production", "client", "elt"],
    doc_md="""
## Master Client Pipeline

Runs every day at 06:00 UTC.

**Flow:** Extract (8 APIs) → Validate schemas → Load to BigQuery → dbt build → Slack notify

**On failure:** Slack alert fires immediately via `on_failure_callback`.

**Retry policy:** 2 retries, 5-minute delay between attempts.

See `docs/client_runbook.md` for troubleshooting steps.
    """,
) as dag:

    t_extract = PythonOperator(
        task_id="extract_all_apis",
        python_callable=extract_all_apis,
        sla=timedelta(minutes=30),
    )

    t_validate = PythonOperator(
        task_id="validate_contracts",
        python_callable=validate_contracts,
        sla=timedelta(minutes=35),
    )

    t_load = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
        sla=timedelta(minutes=55),
    )

    t_transform = PythonOperator(
        task_id="run_dbt",
        python_callable=run_dbt,
        sla=timedelta(minutes=85),
    )

    t_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    # Task dependency chain
    t_extract >> t_validate >> t_load >> t_transform >> t_notify

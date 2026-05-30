# Client Operations Runbook

## Overview
This platform runs autonomously at 6:00 AM UTC daily. It requires zero manual intervention under normal operating conditions. This guide is for the executive team to understand system health and handle exceptions.

## 🟢 Daily Health Checks
1. **Executive Dashboard:** Open your Streamlit dashboard URL. Data should reflect yesterday's close-of-business metrics.
2. **Pipeline Status:** You will receive a green `SUCCESS` notification in the `#data-alerts` Slack channel by 6:05 AM UTC.

## 🔴 Troubleshooting (When Slack says FAILED)

If an API changes its data structure (e.g., Stripe deprecates a field), our **Data Contracts** will intentionally fail the pipeline to prevent corrupted data from reaching your board reports.

**What you need to do:**
1. Check the Slack alert. It will specify which API (e.g., `extract_hubspot_api`) failed.
2. Forward that alert to your data engineering contact.
3. The dashboard will continue to serve yesterday's safe, validated data until the contract is patched.

## ➕ Adding a New Data Source
This architecture is modular. To add a new source (e.g., Salesforce):
1. A new Python connector class must be added to `ingestion/connectors/`.
2. A new Data Contract YAML file must be defined in `dbt/contracts/`.
3. The source must be appended to the `apis` list in `airflow/dags/master_pipeline_dag.py`.

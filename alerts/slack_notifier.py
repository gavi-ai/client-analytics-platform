import os
import logging
import requests

logger = logging.getLogger(__name__)

def send_slack_alert(context, status="failed"):
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    
    color = "#FF0000" if status == "failed" else "#36a64f"
    
    payload = {
        "attachments": [{
            "color": color,
            "title": f"Airflow Pipeline {status.upper()}",
            "text": f"*DAG:* {dag_id}\n*Task:* {task_id}"
        }]
    }
    
    try:
        requests.post(webhook_url, json=payload)
    except Exception as e:
        logger.error(f"Failed to send Slack alert: {e}")

def alert_on_failure(context):
    send_slack_alert(context, status="failed")

def alert_on_success(context):
    send_slack_alert(context, status="success")

"""
Slack notifier.

Provides Airflow callback functions for pipeline success and failure alerts.
Uses Slack Incoming Webhooks — free, no app installation required.

Setup:
    1. Go to api.slack.com/apps → Create App → Incoming Webhooks
    2. Add webhook to any channel (create a #pipeline-alerts channel)
    3. Set SLACK_WEBHOOK_URL in your .env

Env var required: SLACK_WEBHOOK_URL
"""

import logging
import os
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")


def _post_to_slack(payload: dict) -> bool:
    """Send a payload dict to the Slack webhook. Returns True on success."""
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set — skipping Slack notification.")
        return False

    try:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        return True
    except requests.RequestException as exc:
        logger.error("Failed to send Slack notification: %s", exc)
        return False


def alert_on_failure(context: dict) -> None:
    """
    Airflow failure callback.
    Attach to DAG default_args as on_failure_callback.

    Fires when any task in the DAG fails after all retries.
    """
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = str(context.get("execution_date", "unknown"))
    exception = context.get("exception", "unknown error")
    log_url = context.get("task_instance").log_url

    payload = {
        "attachments": [
            {
                "color": "#E24B4A",   # red
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": (
                                f":x: *Pipeline Failed*\n"
                                f"*DAG:* `{dag_id}`\n"
                                f"*Task:* `{task_id}`\n"
                                f"*Date:* {execution_date}\n"
                                f"*Error:* {str(exception)[:200]}\n"
                                f"<{log_url}|View logs>"
                            ),
                        },
                    }
                ],
            }
        ]
    }

    sent = _post_to_slack(payload)
    if sent:
        logger.info("Slack failure alert sent for task %s/%s", dag_id, task_id)


def alert_on_success(
    context: dict,
    rows_loaded: dict[str, int] | None = None,
    execution_date: str | None = None,
) -> None:
    """
    Success notification. Call from the final notify task.

    Shows a summary of rows loaded per source.
    """
    dag_id = context.get("dag").dag_id
    run_date = execution_date or str(context.get("ds", "unknown"))

    if rows_loaded:
        summary_lines = "\n".join(
            f"  • {source}: {count:,} rows" for source, count in rows_loaded.items()
        )
        total = sum(rows_loaded.values())
        rows_summary = f"*Rows loaded:*\n{summary_lines}\n*Total:* {total:,}"
    else:
        rows_summary = "_Row counts unavailable_"

    payload = {
        "attachments": [
            {
                "color": "#1D9E75",   # green
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": (
                                f":white_check_mark: *Pipeline Complete*\n"
                                f"*DAG:* `{dag_id}`\n"
                                f"*Date:* {run_date}\n"
                                f"{rows_summary}\n"
                                f"Dashboard refreshed — data is current as of {run_date}."
                            ),
                        },
                    }
                ],
            }
        ]
    }

    sent = _post_to_slack(payload)
    if sent:
        logger.info("Slack success alert sent for DAG %s on %s", dag_id, run_date)


def send_custom_alert(message: str, color: str = "#185FA5") -> None:
    """
    Send a freeform alert to Slack. Useful for anomaly detection.

    Args:
        message: markdown-formatted message string
        color:   hex color for the attachment sidebar
    """
    payload = {
        "attachments": [
            {
                "color": color,
                "blocks": [
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": message},
                    }
                ],
            }
        ]
    }
    _post_to_slack(payload)

"""
Alert Notifications

Handles sending alerts via various channels (Slack, Email, etc.)
"""

import os
import requests
from typing import Optional, Dict, List, Any
from src.utils.logger import get_logger
from src.utils.config import get_config

logger = get_logger(__name__)


def send_slack_alert(message: str, severity: str = "INFO") -> bool:
    """
    Send alert to Slack.
    
    Args:
        message: Alert message
        severity: Alert severity level
        
    Returns:
        bool: True if sent successfully
    """
    config = get_config()
    
    if not config['monitoring']['alerts']['slack']['enabled']:
        logger.info("Slack alerts disabled, skipping")
        return False
    
    webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    
    if not webhook_url:
        logger.warning("SLACK_WEBHOOK_URL not configured")
        return False
    
    # Severity emoji mapping
    emoji_map = {
        'CRITICAL': ':rotating_light:',
        'HIGH': ':warning:',
        'MEDIUM': ':information_source:',
        'LOW': ':white_check_mark:',
        'INFO': ':bell:'
    }
    
    emoji = emoji_map.get(severity, ':bell:')
    
    payload = {
        'text': f"{emoji} *{severity}* Alert\n{message}"
    }
    
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info("Slack alert sent successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to send Slack alert: {str(e)}")
        return False


def send_failure_alert(dag_id: str, task_id: str, execution_date: str, 
                      error: str, logs_url: Optional[str] = None) -> None:
    """
    Send failure alert.
    
    Args:
        dag_id: DAG identifier
        task_id: Task identifier
        execution_date: Execution date
        error: Error message
        logs_url: URL to logs
    """
    logger.error(f"Pipeline failure: {dag_id}.{task_id}")
    
    message = f"""
*Pipeline Failure*

*DAG*: {dag_id}
*Task*: {task_id}
*Execution Date*: {execution_date}
*Error*: {error}
"""
    
    if logs_url:
        message += f"\n*Logs*: {logs_url}"
    
    # Send to Slack
    send_slack_alert(message, severity="CRITICAL")
    
    # In production, also:
    # - Send email to on-call
    # - Create PagerDuty incident
    # - Log to incident management system


def send_success_notification(dag_id: str, execution_date: str, message: str) -> None:
    """
    Send success notification.
    
    Args:
        dag_id: DAG identifier
        execution_date: Execution date
        message: Success message
    """
    logger.info(f"Pipeline success: {dag_id}")
    
    notification = f"""
*Pipeline Success* :white_check_mark:

*DAG*: {dag_id}
*Execution Date*: {execution_date}
*Message*: {message}
"""
    
    # Only send success notifications for important pipelines
    # or on specific schedules (e.g., daily summary)
    send_slack_alert(notification, severity="INFO")


def send_data_quality_alert(
    layer: str,
    failed_checks: List[str],
    metrics: Dict[str, Any]
) -> None:
    """
    Send data quality alert.
    
    Args:
        layer: Data layer (bronze, silver, gold)
        failed_checks: List of failed checks
        metrics: Quality metrics
    """
    logger.warning(f"Data quality issues in {layer} layer")
    
    checks_str = "\n".join([f"• {check}" for check in failed_checks])
    
    message = f"""
*Data Quality Alert*

*Layer*: {layer}
*Failed Checks*:
{checks_str}

*Metrics*: {metrics}
"""
    
    send_slack_alert(message, severity="HIGH")

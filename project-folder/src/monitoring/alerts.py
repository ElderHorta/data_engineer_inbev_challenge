from typing import Dict, List, Any
from src.utils.logger import get_logger
from src.utils.config import get_config

logger = get_logger(__name__)


def send_slack_alert(message: str, severity: str = "INFO") -> bool:
    """
    Send alert to Slack using Airflow's SlackWebhookHook.
    
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
    
    emoji_map = {
        'CRITICAL': ':rotating_light:',
        'HIGH': ':warning:',
        'MEDIUM': ':information_source:',
        'LOW': ':white_check_mark:',
        'INFO': ':bell:'
    }
    
    emoji = emoji_map.get(severity, ':bell:')
    formatted_message = f"{emoji} *{severity}* Alert\n{message}"
    
    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        hook = SlackWebhookHook(slack_webhook_conn_id='slack_webhook')
        hook.send(text=formatted_message)
        logger.info("Slack alert sent successfully via SlackWebhookHook")
        return True
    except ImportError:
        logger.warning("Slack provider not installed, skipping alert")
        return False
    except Exception as e:
        logger.error(f"Failed to send Slack alert: {str(e)}")
        return False


def send_failure_alert(context: Dict[str, Any]) -> None:
    """
    Airflow DAG-level failure callback.
    
    Called automatically by Airflow when DAG run fails.
    Extracts context info and sends Slack alert.
    
    Args:
        context: Airflow context dictionary containing:
            - dag_run: DAGRun object
            - dag: DAG object
            - exception: Exception that caused failure (if available)
    """
    dag_run = context.get('dag_run')
    dag_id = dag_run.dag_id if dag_run else 'unknown'
    execution_date = str(dag_run.execution_date) if dag_run else 'unknown'
    exception = context.get('exception', context.get('reason', 'Unknown error'))
    
    logger.error(f"Pipeline failure: {dag_id} on {execution_date}")
    
    message = f"""
*Pipeline Failure* :rotating_light:

*DAG*: {dag_id}
*Execution Date*: {execution_date}
*Error*: {exception}
"""
    
    send_slack_alert(message, severity="CRITICAL")


def send_success_notification(context: Dict[str, Any]) -> None:
    """
    Airflow DAG-level success callback.
    
    Called automatically by Airflow when DAG run completes successfully.
    
    Args:
        context: Airflow context dictionary containing:
            - dag_run: DAGRun object
            - dag: DAG object
    """
    dag_run = context.get('dag_run')
    dag_id = dag_run.dag_id if dag_run else 'unknown'
    execution_date = str(dag_run.execution_date) if dag_run else 'unknown'
    
    logger.info(f"Pipeline success: {dag_id} on {execution_date}")
    
    message = f"""
*Pipeline Success* :white_check_mark:

*DAG*: {dag_id}
*Execution Date*: {execution_date}
*Status*: All tasks completed successfully
"""
    
    send_slack_alert(message, severity="INFO")


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

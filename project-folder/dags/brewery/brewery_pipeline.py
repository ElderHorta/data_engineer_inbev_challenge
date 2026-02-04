from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import sys
sys.path.insert(0, '/opt/airflow')
from src.monitoring.alerts import send_failure_alert, send_success_notification
from src.pipelines.brewery.brewery_tasks import (
    check_brewery_api_health,
    extract_brewery_db_api,
    validate_brewery_bronze,
    transform_brewery_to_silver,
    validate_brewery_silver,
    aggregate_brewery_to_gold,
    validate_brewery_gold,
    collect_metrics,
)


default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-engineering@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

dag = DAG(
    'brewery_pipeline',
    default_args=default_args,
    description='Data pipeline getting data from Open Brewery DB API',
    schedule_interval='0 2 * * *',
    start_date=datetime(2026, 1, 20),
    catchup=False,
    max_active_runs=1,
    tags=['brewery', 'medallion', 'data-lake', 'api'],
    doc_md=__doc__,
    on_failure_callback=send_failure_alert,
    on_success_callback=send_success_notification,
)

with dag:
    
    with TaskGroup('bronze_layer', tooltip='Bronze Layer Processing') as bronze_group:
        
        check_api = PythonOperator(
            task_id='check_api_health',
            python_callable=check_brewery_api_health,
        )
        
        extract = PythonOperator(
            task_id='extract_brewery_db_api',
            python_callable=extract_brewery_db_api,
        )
        
        validate_bronze_task = PythonOperator(
            task_id='validate_bronze',
            python_callable=validate_brewery_bronze,
        )
        
        check_api >> extract >> validate_bronze_task
    
    with TaskGroup('silver_layer', tooltip='Silver Layer Processing') as silver_group:
        
        transform_silver = PythonOperator(
            task_id='transform_to_silver',
            python_callable=transform_brewery_to_silver,
        )
        
        validate_silver_task = PythonOperator(
            task_id='validate_silver',
            python_callable=validate_brewery_silver,
        )
        
        transform_silver >> validate_silver_task
    
    with TaskGroup('gold_layer', tooltip='Gold Layer Processing') as gold_group:
        
        aggregate_gold = PythonOperator(
            task_id='aggregate_to_gold',
            python_callable=aggregate_brewery_to_gold,
        )
        
        validate_gold_task = PythonOperator(
            task_id='validate_gold',
            python_callable=validate_brewery_gold,
        )
        
        publish_metrics = PythonOperator(
            task_id='collect_and_publish_metrics',
            python_callable=collect_metrics,
        )
        
        aggregate_gold >> validate_gold_task >> publish_metrics
    
    bronze_group >> silver_group >> gold_group

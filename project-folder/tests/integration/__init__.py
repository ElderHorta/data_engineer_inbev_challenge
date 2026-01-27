"""
Integration tests package for the brewery data pipeline.

This package contains end-to-end tests that validate the complete
data flow through Bronze → Silver → Gold layers with real Spark
sessions and Delta Lake operations.

Run these tests in Docker:
    docker-compose exec airflow-webserver pytest tests/integration/ -v
"""

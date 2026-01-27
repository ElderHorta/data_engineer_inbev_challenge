"""
Spark Session Management

Provides configured Spark session for the application.
"""

import os
from pyspark.sql import SparkSession
from src.utils.logger import get_logger

logger = get_logger(__name__)

_spark_session = None


def get_spark_session(app_name: str = "brewery_pipeline") -> SparkSession:
    """
    Get or create Spark session.
    
    Args:
        app_name: Application name
        
    Returns:
        Configured SparkSession
    """
    global _spark_session
    
    if _spark_session is not None:
        return _spark_session
    
    logger.info(f"Creating Spark session: {app_name}")
    
    # Build Spark session
    # Delta Lake 3.0.0 is compatible with PySpark 3.5.x
    # See: https://docs.delta.io/latest/releases.html
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g")) \
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "2g")) \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.jars.packages", 
                "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", 
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", 
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    # Set master (local for development, can be overridden)
    master = os.getenv("SPARK_MASTER", "local[*]")
    builder = builder.master(master)
    
    _spark_session = builder.getOrCreate()
    
    # Set log level
    _spark_session.sparkContext.setLogLevel("WARN")
    
    logger.info("Spark session created successfully")
    
    return _spark_session


def stop_spark_session() -> None:
    """Stop the Spark session."""
    global _spark_session
    
    if _spark_session is not None:
        logger.info("Stopping Spark session")
        _spark_session.stop()
        _spark_session = None

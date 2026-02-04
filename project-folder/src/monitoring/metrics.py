"""
Metrics Collection

Collects and publishes pipeline metrics for monitoring.
"""

import json
from datetime import datetime
from typing import Dict, Optional
from src.utils.logger import get_logger
from src.utils.config import get_config

logger = get_logger(__name__)


class MetricsCollector:
    """
    Collects and publishes pipeline metrics.
    
    Uses dependency injection for testability and configuration-driven paths.
    """
    
    def __init__(self, config: Optional[Dict] = None, metrics_log_path: Optional[str] = None):
        """
        Initialize metrics collector with dependency injection.
        
        Args:
            config: Configuration dictionary (if None, loads from default)
            metrics_log_path: Path for metrics log file (if None, uses config or default)
            
        Why dependency injection:
        - Enables unit testing with mocked config
        - Configurable paths for different environments
        """
        self.config = config if config is not None else get_config()
        
        # Get metrics path from config, parameter, or use default
        if metrics_log_path:
            self.metrics_log_path = metrics_log_path
        else:
            monitoring_config = self.config.get('monitoring', {})
            self.metrics_log_path = monitoring_config.get('metrics_log_path', '/logs/metrics.jsonl')
    
    def publish(self, metrics: Dict) -> None:
        """
        Publish metrics.
        
        Args:
            metrics: Dictionary of metrics to publish
        """
        logger.info("Publishing pipeline metrics")
        
        # Add timestamp
        metrics['timestamp'] = datetime.now().isoformat()
        
        # Log metrics in structured format
        logger.info("Pipeline metrics", extra={'metrics': metrics})
        
        # Write to metrics log file (append mode)
        try:
            with open(self.metrics_log_path, 'a') as f:
                f.write(json.dumps(metrics) + '\n')
        except Exception as e:
            logger.warning(f"Failed to write metrics to file: {str(e)}")
        
        # In production, you would also:
        # - Send to Prometheus
        # - Send to CloudWatch/Cloud Monitoring
        # - Send to DataDog
        # - Store in metrics database
        
        logger.info("Metrics published successfully")

"""
Metric History - Time-series analytics and trend detection
==========================================================

Tracks metrics over time and provides trend analysis for understanding
how the network is evolving. Useful for detecting gradual changes,
planning capacity, and historical reporting.

Tracked Metrics
---------------
    Network-wide:
        - node_count: Total nodes in network
        - edge_count: Total validated edges
        - health_score: Overall network health
        - resilience: Network resilience score
        - avg_centrality: Average node centrality
        
    Per-node:
        - centrality: Betweenness centrality over time
        - degree: Connection count over time
        - traffic: Packet volume through node
        - activity: Active hours per period

Trend Detection
---------------
    The module calculates trends using linear regression over
    the historical data points:
    
        - increasing: Positive slope, metric is growing
        - decreasing: Negative slope, metric is declining  
        - stable: Near-zero slope, metric is constant
        - volatile: High variance, metric is fluctuating

Storage
-------
    Metrics are stored in the metric_history table with:
        - timestamp: When the metric was recorded
        - metric_type: Type of metric (e.g., 'health_score')
        - node_hash: Node hash (NULL for network-wide metrics)
        - value: The metric value

API Endpoints
-------------
    GET /api/analytics/trends?metric=health_score&days=7
        Returns trend analysis for a network-wide metric.
        
    GET /api/analytics/trends?metric=centrality&node=0xAB&days=7
        Returns trend analysis for a node-specific metric.

See Also
--------
    - worker.py: Periodically records metrics
    - network_health.py: Provides health score to record
    - graph.py: Provides centrality scores
"""

import logging
import math
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Union
from enum import Enum

from .db import AnalyticsDB, ensure_connection, DBConnection

logger = logging.getLogger("Analytics.MetricHistory")


class MetricType(str, Enum):
    """Types of tracked metrics."""
    # Network-wide metrics
    NODE_COUNT = "node_count"
    EDGE_COUNT = "edge_count"
    HEALTH_SCORE = "health_score"
    RESILIENCE = "resilience"
    AVG_CENTRALITY = "avg_centrality"
    CONNECTIVITY = "connectivity"
    REDUNDANCY = "redundancy"
    PACKET_RATE = "packet_rate"
    
    # Per-node metrics
    CENTRALITY = "centrality"
    DEGREE = "degree"
    TRAFFIC = "traffic"
    ACTIVITY = "activity"


class TrendDirection(str, Enum):
    """Direction of a trend."""
    INCREASING = "increasing"
    DECREASING = "decreasing"
    STABLE = "stable"
    VOLATILE = "volatile"
    UNKNOWN = "unknown"


# Thresholds for trend classification
TREND_SLOPE_THRESHOLD = 0.01  # Slope below this is "stable"
VOLATILITY_THRESHOLD = 0.3    # Coefficient of variation above this is "volatile"


@dataclass
class DataPoint:
    """A single metric data point."""
    timestamp: float
    value: float
    
    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "value": round(self.value, 4),
        }


@dataclass
class TrendAnalysis:
    """Result of trend analysis on a metric."""
    metric: str
    node_hash: Optional[str]
    direction: TrendDirection
    
    # Statistical measures
    slope: float = 0.0           # Rate of change per day
    intercept: float = 0.0       # Y-intercept of trend line
    r_squared: float = 0.0       # Goodness of fit (0-1)
    
    # Summary stats
    current_value: float = 0.0
    min_value: float = 0.0
    max_value: float = 0.0
    avg_value: float = 0.0
    std_dev: float = 0.0
    
    # Change metrics
    change_absolute: float = 0.0  # Absolute change over period
    change_percent: float = 0.0   # Percentage change
    
    # Data
    data_points: List[DataPoint] = field(default_factory=list)
    point_count: int = 0
    period_days: int = 7
    
    def to_dict(self) -> dict:
        return {
            "metric": self.metric,
            "nodeHash": self.node_hash,
            "direction": self.direction.value,
            "slope": round(self.slope, 6),
            "intercept": round(self.intercept, 4),
            "rSquared": round(self.r_squared, 4),
            "currentValue": round(self.current_value, 4),
            "minValue": round(self.min_value, 4),
            "maxValue": round(self.max_value, 4),
            "avgValue": round(self.avg_value, 4),
            "stdDev": round(self.std_dev, 4),
            "changeAbsolute": round(self.change_absolute, 4),
            "changePercent": round(self.change_percent, 2),
            "dataPoints": [p.to_dict() for p in self.data_points],
            "pointCount": self.point_count,
            "periodDays": self.period_days,
        }


def _linear_regression(x: List[float], y: List[float]) -> Tuple[float, float, float]:
    """
    Simple linear regression.
    
    Returns:
        Tuple of (slope, intercept, r_squared)
    """
    n = len(x)
    if n < 2:
        return 0.0, y[0] if y else 0.0, 0.0
    
    sum_x = sum(x)
    sum_y = sum(y)
    sum_xy = sum(xi * yi for xi, yi in zip(x, y))
    sum_x2 = sum(xi * xi for xi in x)
    sum_y2 = sum(yi * yi for yi in y)
    
    denom = n * sum_x2 - sum_x * sum_x
    if denom == 0:
        return 0.0, sum_y / n, 0.0
    
    slope = (n * sum_xy - sum_x * sum_y) / denom
    intercept = (sum_y - slope * sum_x) / n
    
    # Calculate R-squared
    ss_tot = sum_y2 - (sum_y * sum_y) / n
    ss_res = sum((yi - (slope * xi + intercept)) ** 2 for xi, yi in zip(x, y))
    
    if ss_tot == 0:
        r_squared = 1.0 if ss_res == 0 else 0.0
    else:
        r_squared = 1 - (ss_res / ss_tot)
    
    return slope, intercept, max(0, r_squared)


def _determine_trend_direction(
    slope: float,
    std_dev: float,
    avg_value: float,
) -> TrendDirection:
    """Determine trend direction based on slope and volatility."""
    # Calculate coefficient of variation (normalized volatility)
    if avg_value != 0:
        cv = std_dev / abs(avg_value)
    else:
        cv = 0
    
    # Check for volatility first
    if cv > VOLATILITY_THRESHOLD:
        return TrendDirection.VOLATILE
    
    # Normalize slope by average value for comparison
    if avg_value != 0:
        normalized_slope = slope / abs(avg_value)
    else:
        normalized_slope = slope
    
    if abs(normalized_slope) < TREND_SLOPE_THRESHOLD:
        return TrendDirection.STABLE
    elif normalized_slope > 0:
        return TrendDirection.INCREASING
    else:
        return TrendDirection.DECREASING


def record_metric(
    conn_or_db: DBConnection,
    metric_type: Union[MetricType, str],
    value: float,
    node_hash: Optional[str] = None,
    timestamp: Optional[float] = None,
) -> bool:
    """
    Record a metric value to the history table.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        metric_type: Type of metric being recorded
        value: The metric value
        node_hash: Node hash for per-node metrics (None for network-wide)
        timestamp: Recording timestamp (defaults to now)
        
    Returns:
        True if recorded successfully
    """
    if timestamp is None:
        timestamp = time.time()
    
    metric_name = metric_type.value if isinstance(metric_type, MetricType) else metric_type
    
    with ensure_connection(conn_or_db) as conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO metric_history (timestamp, metric_type, node_hash, value)
                VALUES (?, ?, ?, ?)
            """, (timestamp, metric_name, node_hash, value))
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to record metric {metric_name}: {e}")
            return False


def record_metrics_batch(
    conn_or_db: DBConnection,
    metrics: List[Tuple[Union[MetricType, str], float, Optional[str]]],
    timestamp: Optional[float] = None,
) -> int:
    """
    Record multiple metrics in a single transaction.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        metrics: List of (metric_type, value, node_hash) tuples
        timestamp: Recording timestamp (defaults to now)
        
    Returns:
        Number of metrics recorded
    """
    if timestamp is None:
        timestamp = time.time()
    
    recorded = 0
    
    with ensure_connection(conn_or_db) as conn:
        cursor = conn.cursor()
        
        for metric_type, value, node_hash in metrics:
            try:
                metric_name = metric_type.value if isinstance(metric_type, MetricType) else metric_type
                cursor.execute("""
                    INSERT INTO metric_history (timestamp, metric_type, node_hash, value)
                    VALUES (?, ?, ?, ?)
                """, (timestamp, metric_name, node_hash, value))
                recorded += 1
            except Exception as e:
                logger.error(f"Failed to record metric: {e}")
        
        conn.commit()
    
    return recorded


def get_metric_history(
    conn_or_db: DBConnection,
    metric_type: Union[MetricType, str],
    node_hash: Optional[str] = None,
    days: int = 7,
) -> List[DataPoint]:
    """
    Get historical data points for a metric.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        metric_type: Type of metric to retrieve
        node_hash: Node hash for per-node metrics
        days: Number of days of history to retrieve
        
    Returns:
        List of DataPoint objects, oldest first
    """
    metric_name = metric_type.value if isinstance(metric_type, MetricType) else metric_type
    cutoff = time.time() - (days * 24 * 3600)
    
    with ensure_connection(conn_or_db) as conn:
        cursor = conn.cursor()
        
        if node_hash:
            cursor.execute("""
                SELECT timestamp, value
                FROM metric_history
                WHERE metric_type = ? AND node_hash = ? AND timestamp > ?
                ORDER BY timestamp ASC
            """, (metric_name, node_hash, cutoff))
        else:
            cursor.execute("""
                SELECT timestamp, value
                FROM metric_history
                WHERE metric_type = ? AND node_hash IS NULL AND timestamp > ?
                ORDER BY timestamp ASC
            """, (metric_name, cutoff))
        
        return [DataPoint(timestamp=row[0], value=row[1]) for row in cursor.fetchall()]


def analyze_trend(
    conn_or_db: DBConnection,
    metric_type: Union[MetricType, str],
    node_hash: Optional[str] = None,
    days: int = 7,
) -> TrendAnalysis:
    """
    Analyze trend for a metric over time.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        metric_type: Type of metric to analyze
        node_hash: Node hash for per-node metrics
        days: Number of days to analyze
        
    Returns:
        TrendAnalysis with slope, direction, and statistics
    """
    metric_name = metric_type.value if isinstance(metric_type, MetricType) else metric_type
    
    data_points = get_metric_history(conn_or_db, metric_type, node_hash, days)
    
    result = TrendAnalysis(
        metric=metric_name,
        node_hash=node_hash,
        direction=TrendDirection.UNKNOWN,
        data_points=data_points,
        point_count=len(data_points),
        period_days=days,
    )
    
    if not data_points:
        return result
    
    values = [p.value for p in data_points]
    
    # Calculate summary statistics
    result.current_value = values[-1]
    result.min_value = min(values)
    result.max_value = max(values)
    result.avg_value = sum(values) / len(values)
    
    if len(values) > 1:
        variance = sum((v - result.avg_value) ** 2 for v in values) / len(values)
        result.std_dev = math.sqrt(variance)
    
    # Calculate change
    result.change_absolute = values[-1] - values[0]
    if values[0] != 0:
        result.change_percent = (result.change_absolute / values[0]) * 100
    
    # Perform linear regression
    if len(data_points) >= 2:
        # Normalize timestamps to days from start
        start_time = data_points[0].timestamp
        x = [(p.timestamp - start_time) / 86400 for p in data_points]  # Days
        y = values
        
        result.slope, result.intercept, result.r_squared = _linear_regression(x, y)
        
        # Determine direction
        result.direction = _determine_trend_direction(
            result.slope,
            result.std_dev,
            result.avg_value,
        )
    elif len(data_points) == 1:
        result.direction = TrendDirection.STABLE
    
    return result


def get_all_metric_trends(
    conn_or_db: DBConnection,
    days: int = 7,
) -> Dict[str, TrendAnalysis]:
    """
    Get trend analysis for all network-wide metrics.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        days: Number of days to analyze
        
    Returns:
        Dict mapping metric name to TrendAnalysis
    """
    network_metrics = [
        MetricType.NODE_COUNT,
        MetricType.EDGE_COUNT,
        MetricType.HEALTH_SCORE,
        MetricType.RESILIENCE,
        MetricType.AVG_CENTRALITY,
        MetricType.CONNECTIVITY,
        MetricType.REDUNDANCY,
        MetricType.PACKET_RATE,
    ]
    
    results = {}
    for metric in network_metrics:
        trend = analyze_trend(conn_or_db, metric, days=days)
        if trend.point_count > 0:
            results[metric.value] = trend
    
    return results


def get_node_trends(
    conn_or_db: DBConnection,
    node_hash: str,
    days: int = 7,
) -> Dict[str, TrendAnalysis]:
    """
    Get trend analysis for all metrics of a specific node.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        node_hash: Node hash to analyze
        days: Number of days to analyze
        
    Returns:
        Dict mapping metric name to TrendAnalysis
    """
    node_metrics = [
        MetricType.CENTRALITY,
        MetricType.DEGREE,
        MetricType.TRAFFIC,
        MetricType.ACTIVITY,
    ]
    
    results = {}
    for metric in node_metrics:
        trend = analyze_trend(conn_or_db, metric, node_hash=node_hash, days=days)
        if trend.point_count > 0:
            results[metric.value] = trend
    
    return results


def cleanup_old_metrics(
    conn_or_db: DBConnection,
    days: int = 30,
) -> int:
    """
    Remove metric history older than N days.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        days: Age threshold in days
        
    Returns:
        Number of records deleted
    """
    cutoff = time.time() - (days * 24 * 3600)
    
    with ensure_connection(conn_or_db) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM metric_history WHERE timestamp < ?", (cutoff,))
        deleted = cursor.rowcount
        conn.commit()
        
        if deleted > 0:
            logger.info(f"Cleaned up {deleted} old metric records")
        
        return deleted


def record_network_snapshot(
    conn_or_db: DBConnection,
    health_score: float,
    node_count: int,
    edge_count: int,
    resilience: float,
    connectivity: float,
    redundancy: float,
    packet_rate: float = 0.0,
    avg_centrality: float = 0.0,
) -> int:
    """
    Record a complete network metrics snapshot.
    
    Convenience function for recording all network-wide metrics at once.
    Called periodically by the worker.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        health_score: Overall network health (0-1)
        node_count: Total nodes in network
        edge_count: Total validated edges
        resilience: Network resilience score
        connectivity: Network connectivity score
        redundancy: Network redundancy score
        packet_rate: Packets per minute
        avg_centrality: Average node centrality
        
    Returns:
        Number of metrics recorded
    """
    metrics = [
        (MetricType.HEALTH_SCORE, health_score, None),
        (MetricType.NODE_COUNT, float(node_count), None),
        (MetricType.EDGE_COUNT, float(edge_count), None),
        (MetricType.RESILIENCE, resilience, None),
        (MetricType.CONNECTIVITY, connectivity, None),
        (MetricType.REDUNDANCY, redundancy, None),
        (MetricType.PACKET_RATE, packet_rate, None),
        (MetricType.AVG_CENTRALITY, avg_centrality, None),
    ]
    
    return record_metrics_batch(conn_or_db, metrics)


def record_node_snapshot(
    conn_or_db: DBConnection,
    node_hash: str,
    centrality: float,
    degree: int,
    traffic: int = 0,
    activity: float = 0.0,
) -> int:
    """
    Record metrics snapshot for a single node.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        node_hash: Node hash
        centrality: Betweenness centrality
        degree: Number of connections
        traffic: Packet volume through node
        activity: Activity score (0-1)
        
    Returns:
        Number of metrics recorded
    """
    metrics = [
        (MetricType.CENTRALITY, centrality, node_hash),
        (MetricType.DEGREE, float(degree), node_hash),
        (MetricType.TRAFFIC, float(traffic), node_hash),
        (MetricType.ACTIVITY, activity, node_hash),
    ]
    
    return record_metrics_batch(conn_or_db, metrics)

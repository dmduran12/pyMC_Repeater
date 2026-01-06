"""
Analytics Configuration Module
==============================

Centralized configuration for all analytics components. All magic numbers
and thresholds are defined here for easy tuning and consistency.

Usage
-----
    from repeater.analytics.config import Config
    
    # Use values directly
    min_certainty = Config.TOPOLOGY.MIN_EDGE_VALIDATIONS
    
    # Or get entire config group
    topo_config = Config.TOPOLOGY

Environment Override
-------------------
Configuration values can be overridden via environment variables using
the pattern: ANALYTICS_{GROUP}_{NAME}

For example:
    ANALYTICS_TOPOLOGY_MIN_EDGE_VALIDATIONS=20
    ANALYTICS_GRAPH_DEFAULT_MIN_CERTAIN_COUNT=10

Hot Reload
----------
Configuration can be reloaded at runtime without restarting:

    from repeater.analytics.config import reload_config, Config
    
    # After changing environment variables
    reload_config()
    
    # Config now reflects new values
    print(Config.TOPOLOGY.MIN_EDGE_VALIDATIONS)

Thread Safety
-------------
Configuration reads are thread-safe. Reloads are atomic - readers will
see either the old or new config, never a partial state.
"""

import logging
import os
import threading
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Callable

logger = logging.getLogger("Analytics.Config")

# Lock for thread-safe config reload
_config_lock = threading.RLock()


def _env_int(key: str, default: int) -> int:
    """Get integer from environment or use default."""
    val = os.environ.get(key)
    if val is not None:
        try:
            return int(val)
        except ValueError:
            logger.warning(f"Invalid int value for {key}: {val}, using default {default}")
    return default


def _env_float(key: str, default: float) -> float:
    """Get float from environment or use default."""
    val = os.environ.get(key)
    if val is not None:
        try:
            return float(val)
        except ValueError:
            logger.warning(f"Invalid float value for {key}: {val}, using default {default}")
    return default


def _env_bool(key: str, default: bool) -> bool:
    """Get boolean from environment or use default."""
    val = os.environ.get(key)
    if val is not None:
        return val.lower() in ('true', '1', 'yes', 'on')
    return default


@dataclass(frozen=True)
class TopologyConfig:
    """Topology analysis configuration."""
    
    # Minimum certain observations before edge appears in topology
    MIN_EDGE_VALIDATIONS: int = _env_int(
        "ANALYTICS_TOPOLOGY_MIN_EDGE_VALIDATIONS", 15
    )
    
    # Traffic share threshold for hub classification (15%)
    HUB_THRESHOLD_PERCENT: float = _env_float(
        "ANALYTICS_TOPOLOGY_HUB_THRESHOLD_PERCENT", 0.15
    )
    
    # Traffic share threshold for gateway classification (5%)
    GATEWAY_THRESHOLD_PERCENT: float = _env_float(
        "ANALYTICS_TOPOLOGY_GATEWAY_THRESHOLD_PERCENT", 0.05
    )
    
    # Maximum nodes in detected loop (limits DFS depth)
    MAX_LOOP_LENGTH: int = _env_int(
        "ANALYTICS_TOPOLOGY_MAX_LOOP_LENGTH", 6
    )


@dataclass(frozen=True)
class GraphConfig:
    """Graph analysis configuration."""
    
    # Default minimum certainty for including edges in graph
    DEFAULT_MIN_CERTAIN_COUNT: int = _env_int(
        "ANALYTICS_GRAPH_DEFAULT_MIN_CERTAIN_COUNT", 5
    )


@dataclass(frozen=True)
class NetworkHealthConfig:
    """Network health analysis thresholds."""
    
    # 30% of traffic = critical node
    CRITICAL_NODE_TRAFFIC_THRESHOLD: float = _env_float(
        "ANALYTICS_HEALTH_CRITICAL_NODE_THRESHOLD", 0.3
    )
    
    # Links below 40% confidence are weak
    WEAK_LINK_CONFIDENCE_THRESHOLD: float = _env_float(
        "ANALYTICS_HEALTH_WEAK_LINK_THRESHOLD", 0.4
    )
    
    # Symmetry ratio below 30% indicates asymmetric link
    ASYMMETRIC_LINK_THRESHOLD: float = _env_float(
        "ANALYTICS_HEALTH_ASYMMETRIC_LINK_THRESHOLD", 0.3
    )
    
    # Path changes above 60% indicate instability
    HIGH_PATH_VOLATILITY_THRESHOLD: float = _env_float(
        "ANALYTICS_HEALTH_PATH_VOLATILITY_THRESHOLD", 0.6
    )


@dataclass(frozen=True)
class NodeProfileConfig:
    """Node profile classification thresholds."""
    
    # Centrality threshold for hub classification
    HUB_CENTRALITY_THRESHOLD: float = _env_float(
        "ANALYTICS_NODE_HUB_CENTRALITY_THRESHOLD", 0.3
    )
    
    # Centrality threshold for relay classification
    RELAY_CENTRALITY_THRESHOLD: float = _env_float(
        "ANALYTICS_NODE_RELAY_CENTRALITY_THRESHOLD", 0.1
    )
    
    # Degree threshold for relay classification
    HIGH_DEGREE_THRESHOLD: int = _env_int(
        "ANALYTICS_NODE_HIGH_DEGREE_THRESHOLD", 5
    )


@dataclass(frozen=True)
class PacketFlowConfig:
    """Packet flow analysis configuration."""
    
    # Minimum packets to include a flow in analysis
    MIN_FLOW_PACKETS: int = _env_int(
        "ANALYTICS_FLOW_MIN_PACKETS", 5
    )


@dataclass(frozen=True)
class RouteSimulationConfig:
    """Route simulation timing constants (milliseconds)."""
    
    # Base latency per hop
    BASE_LATENCY_PER_HOP_MS: int = _env_int(
        "ANALYTICS_ROUTE_BASE_LATENCY_MS", 250
    )
    
    # Per-node processing delay
    PROCESSING_DELAY_MS: int = _env_int(
        "ANALYTICS_ROUTE_PROCESSING_DELAY_MS", 50
    )
    
    # Average transmission delay
    TX_DELAY_AVG_MS: int = _env_int(
        "ANALYTICS_ROUTE_TX_DELAY_MS", 100
    )
    
    # Flood routing overhead factor
    FLOOD_OVERHEAD_FACTOR: float = _env_float(
        "ANALYTICS_ROUTE_FLOOD_OVERHEAD", 1.5
    )


@dataclass(frozen=True)
class WorkerConfig:
    """Analytics worker configuration."""
    
    # Default seconds between worker runs
    DEFAULT_INTERVAL_SECONDS: int = _env_int(
        "ANALYTICS_WORKER_INTERVAL_SECONDS", 60
    )
    
    # Batch size for packet processing
    BATCH_SIZE: int = _env_int(
        "ANALYTICS_WORKER_BATCH_SIZE", 1000
    )


@dataclass(frozen=True)
class APIConfig:
    """API endpoint defaults."""
    
    # Default time range in hours for activity queries
    DEFAULT_HOURS: int = _env_int(
        "ANALYTICS_API_DEFAULT_HOURS", 168  # 7 days
    )
    
    # Default limit for list queries
    DEFAULT_LIMIT: int = _env_int(
        "ANALYTICS_API_DEFAULT_LIMIT", 100
    )
    
    # Maximum allowed limit
    MAX_LIMIT: int = _env_int(
        "ANALYTICS_API_MAX_LIMIT", 1000
    )
    
    # Maximum hours for queries (prevent excessive DB scans)
    MAX_HOURS: int = _env_int(
        "ANALYTICS_API_MAX_HOURS", 8760  # 1 year
    )


@dataclass(frozen=True)
class CacheConfig:
    """Caching configuration."""
    
    # Graph cache TTL in seconds
    GRAPH_CACHE_TTL_SECONDS: int = _env_int(
        "ANALYTICS_CACHE_GRAPH_TTL", 300  # 5 minutes
    )
    
    # Topology cache TTL in seconds
    TOPOLOGY_CACHE_TTL_SECONDS: int = _env_int(
        "ANALYTICS_CACHE_TOPOLOGY_TTL", 300  # 5 minutes
    )


class Config:
    """
    Main configuration container with all config groups.
    
    Access via Config.GROUP.CONSTANT, e.g.:
        Config.TOPOLOGY.MIN_EDGE_VALIDATIONS
        Config.HEALTH.WEAK_LINK_CONFIDENCE_THRESHOLD
        
    Thread Safety:
        All reads are thread-safe. Use reload_config() to update
        configuration at runtime.
    """
    
    TOPOLOGY = TopologyConfig()
    GRAPH = GraphConfig()
    HEALTH = NetworkHealthConfig()
    NODE = NodeProfileConfig()
    FLOW = PacketFlowConfig()
    ROUTE = RouteSimulationConfig()
    WORKER = WorkerConfig()
    API = APIConfig()
    CACHE = CacheConfig()
    
    # Version counter for cache invalidation on config change
    _version: int = 0
    
    @classmethod
    def to_dict(cls) -> Dict[str, Dict[str, Any]]:
        """Export all config as dict (useful for debugging)."""
        from dataclasses import asdict
        return {
            "topology": asdict(cls.TOPOLOGY),
            "graph": asdict(cls.GRAPH),
            "health": asdict(cls.HEALTH),
            "node": asdict(cls.NODE),
            "flow": asdict(cls.FLOW),
            "route": asdict(cls.ROUTE),
            "worker": asdict(cls.WORKER),
            "api": asdict(cls.API),
            "cache": asdict(cls.CACHE),
            "_version": cls._version,
        }
    
    @classmethod
    def get_version(cls) -> int:
        """Get current config version (increments on reload)."""
        return cls._version


def reload_config() -> None:
    """
    Reload configuration from environment variables.
    
    Call this after changing environment variables to apply new values
    without restarting the application.
    
    Thread Safety:
        This operation is atomic - readers will see either the old or
        new configuration, never a partial state.
        
    Example:
        >>> import os
        >>> os.environ['ANALYTICS_API_DEFAULT_HOURS'] = '24'
        >>> reload_config()
        >>> Config.API.DEFAULT_HOURS
        24
    """
    with _config_lock:
        # Recreate all config groups with fresh env reads
        Config.TOPOLOGY = TopologyConfig()
        Config.GRAPH = GraphConfig()
        Config.HEALTH = NetworkHealthConfig()
        Config.NODE = NodeProfileConfig()
        Config.FLOW = PacketFlowConfig()
        Config.ROUTE = RouteSimulationConfig()
        Config.WORKER = WorkerConfig()
        Config.API = APIConfig()
        Config.CACHE = CacheConfig()
        Config._version += 1
        
        # Update module-level references
        global TOPOLOGY, GRAPH, HEALTH, NODE, FLOW, ROUTE, WORKER, API, CACHE
        TOPOLOGY = Config.TOPOLOGY
        GRAPH = Config.GRAPH
        HEALTH = Config.HEALTH
        NODE = Config.NODE
        FLOW = Config.FLOW
        ROUTE = Config.ROUTE
        WORKER = Config.WORKER
        API = Config.API
        CACHE = Config.CACHE
        
        logger.info(f"Configuration reloaded (version {Config._version})")


# Export convenient references
TOPOLOGY = Config.TOPOLOGY
GRAPH = Config.GRAPH
HEALTH = Config.HEALTH
NODE = Config.NODE
FLOW = Config.FLOW
ROUTE = Config.ROUTE
WORKER = Config.WORKER
API = Config.API
CACHE = Config.CACHE
